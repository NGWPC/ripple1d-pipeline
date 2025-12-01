"""
Bridge processor module for masking depth library TIFs based on bridge locations.

This module processes depth grids to adjust water depth values where bridges
are located. The algorithm calculates Water Surface Elevation (WSE) and compares
it to bridge deck elevations to determine if water flows over or under bridges.

Processing Steps:
1. Query bridge tile index for intersecting bridges
2. If intersection: load depth, DEM, and bridge rasters
3. Align all rasters to depth grid
4. Calculate WSE = DEM_elevation + depth
5. Adjust depth based on bridge comparison:
   - If bridge > WSE (dry): set depth = 0
   - If bridge < WSE (submerged): set depth = WSE - bridge_elevation
"""

import logging
import multiprocessing
import os
import shutil
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from osgeo import gdal

from .extent_library import setup_gdal_environment

if TYPE_CHECKING:
    from ..setup.collection_data import CollectionData

# Enable GDAL exceptions
gdal.UseExceptions()

# S3 storage options for pandas
S3_STORAGE_OPTIONS = {"profile": "fimbucket"}


def load_bridge_index(bridge_index_path: str) -> pd.DataFrame:
    """
    Load the bridge tile index from S3 or local path.

    Args:
        bridge_index_path: Path to bridge index parquet (S3 or local)

    Returns:
        DataFrame with bridge index data
    """
    if bridge_index_path.startswith("s3://"):
        return pd.read_parquet(bridge_index_path, storage_options=S3_STORAGE_OPTIONS)
    return pd.read_parquet(bridge_index_path)


def get_raster_bounds(tif_path: Path) -> Tuple[float, float, float, float]:
    """
    Get the bounding box of a raster file.

    Args:
        tif_path: Path to the raster file

    Returns:
        Tuple of (xmin, ymin, xmax, ymax)
    """
    ds = gdal.Open(str(tif_path))
    gt = ds.GetGeoTransform()
    width = ds.RasterXSize
    height = ds.RasterYSize

    xmin = gt[0]
    xmax = gt[0] + width * gt[1]
    ymax = gt[3]
    ymin = gt[3] + height * gt[5]

    ds = None
    return (xmin, ymin, xmax, ymax)


def boxes_intersect(box1: Tuple[float, float, float, float], box2: Dict[str, float]) -> bool:
    """
    Check if two bounding boxes intersect.

    Args:
        box1: Tuple of (xmin, ymin, xmax, ymax)
        box2: Dict with keys xmin, ymin, xmax, ymax

    Returns:
        True if boxes intersect
    """
    return not (
        box1[2] < box2["xmin"]  # box1 xmax < box2 xmin
        or box1[0] > box2["xmax"]  # box1 xmin > box2 xmax
        or box1[3] < box2["ymin"]  # box1 ymax < box2 ymin
        or box1[1] > box2["ymax"]  # box1 ymin > box2 ymax
    )


def query_intersecting_bridges(
    depth_bounds: Tuple[float, float, float, float], bridge_index: pd.DataFrame
) -> pd.DataFrame:
    """
    Query the bridge index for bridges that intersect the depth raster bounds.

    Args:
        depth_bounds: Bounding box of depth raster (xmin, ymin, xmax, ymax)
        bridge_index: DataFrame with bridge tile index

    Returns:
        DataFrame of intersecting bridges
    """
    mask = bridge_index["geometry_bbox"].apply(lambda bbox: boxes_intersect(depth_bounds, bbox))
    return bridge_index[mask]


def align_raster_to_reference(
    src_path: str, ref_ds: gdal.Dataset, output_path: str, resampling: int = gdal.GRA_Bilinear
) -> Optional[str]:
    """
    Align a source raster to match a reference raster's grid.

    Args:
        src_path: Path to source raster (can be /vsis3/ path)
        ref_ds: Reference GDAL dataset to match
        output_path: Path for aligned output
        resampling: GDAL resampling algorithm

    Returns:
        Path to aligned raster or None if failed
    """
    ref_gt = ref_ds.GetGeoTransform()
    ref_proj = ref_ds.GetProjection()
    ref_width = ref_ds.RasterXSize
    ref_height = ref_ds.RasterYSize

    # Calculate output bounds
    xmin = ref_gt[0]
    ymax = ref_gt[3]
    xmax = xmin + ref_width * ref_gt[1]
    ymin = ymax + ref_height * ref_gt[5]

    warp_options = gdal.WarpOptions(
        format="GTiff",
        outputBounds=(xmin, ymin, xmax, ymax),
        xRes=abs(ref_gt[1]),
        yRes=abs(ref_gt[5]),
        dstSRS=ref_proj,
        resampleAlg=resampling,
        creationOptions=["COMPRESS=LZW"],
    )

    try:
        result = gdal.Warp(output_path, src_path, options=warp_options)
        if result is None:
            logging.error(f"Failed to warp {src_path}")
            return None
        result = None  # Close dataset
        return output_path
    except Exception as e:
        logging.error(f"Error aligning raster {src_path}: {e}")
        return None


def process_depth_with_bridges(
    depth_path: Path, dem_path: Path, bridge_paths: List[str], output_path: Path, temp_dir: Path
) -> bool:
    """
    Process a single depth TIF with bridge masking.

    Args:
        depth_path: Path to input depth raster
        dem_path: Path to DEM raster for this reach
        bridge_paths: List of bridge raster paths (can be /vsis3/)
        output_path: Path for output COG
        temp_dir: Temporary directory for intermediate files

    Returns:
        True if successful
    """
    try:
        # Open depth raster as reference
        depth_ds = gdal.Open(str(depth_path))
        if depth_ds is None:
            logging.error(f"Failed to open depth raster: {depth_path}")
            return False

        # Read depth data
        depth_band = depth_ds.GetRasterBand(1)
        depth_data = depth_band.ReadAsArray().astype(np.float32)
        depth_nodata = depth_band.GetNoDataValue()

        # Align and read DEM
        aligned_dem_path = str(temp_dir / "aligned_dem.tif")
        if not align_raster_to_reference(str(dem_path), depth_ds, aligned_dem_path):
            depth_ds = None
            return False

        dem_ds = gdal.Open(aligned_dem_path)
        dem_data = dem_ds.GetRasterBand(1).ReadAsArray().astype(np.float32)
        dem_ds = None

        # Process each bridge raster
        combined_bridge_mask = np.zeros(depth_data.shape, dtype=bool)
        combined_bridge_elev = np.full(depth_data.shape, np.nan, dtype=np.float32)

        for i, bridge_path in enumerate(bridge_paths):
            aligned_bridge_path = str(temp_dir / f"aligned_bridge_{i}.tif")
            if not align_raster_to_reference(bridge_path, depth_ds, aligned_bridge_path):
                logging.warning(f"Failed to align bridge raster: {bridge_path}")
                continue

            bridge_ds = gdal.Open(aligned_bridge_path)
            if bridge_ds is None:
                continue

            bridge_band = bridge_ds.GetRasterBand(1)
            bridge_data = bridge_band.ReadAsArray().astype(np.float32)
            bridge_nodata = bridge_band.GetNoDataValue()
            bridge_ds = None

            # Create mask for valid bridge pixels
            if bridge_nodata is not None:
                valid_bridge = ~np.isclose(bridge_data, bridge_nodata)
            else:
                valid_bridge = ~np.isnan(bridge_data)

            # Update combined bridge data
            combined_bridge_mask |= valid_bridge
            np.copyto(combined_bridge_elev, bridge_data, where=valid_bridge)

        # Apply bridge masking algorithm
        if np.any(combined_bridge_mask):
            # Calculate WSE where we have depth
            has_depth = depth_data > 0
            if depth_nodata is not None:
                has_depth &= ~np.isclose(depth_data, depth_nodata)

            # Only process pixels that have both bridge and depth
            process_mask = combined_bridge_mask & has_depth

            if np.any(process_mask):
                # Calculate Water Surface Elevation
                wse = dem_data + depth_data

                # Calculate difference: WSE - bridge_elevation
                delta = wse - combined_bridge_elev

                # Apply masking rules:
                # - If bridge > WSE (delta < 0): bridge is dry, set depth = 0
                # - If bridge < WSE (delta > 0): bridge submerged, depth = delta
                bridge_dry = process_mask & (delta < 0)
                bridge_submerged = process_mask & (delta >= 0)

                depth_data[bridge_dry] = 0
                depth_data[bridge_submerged] = delta[bridge_submerged]

        # Write output as COG
        driver = gdal.GetDriverByName("GTiff")
        temp_output = str(temp_dir / "temp_output.tif")

        out_ds = driver.Create(
            temp_output, depth_ds.RasterXSize, depth_ds.RasterYSize, 1, gdal.GDT_Float32, ["COMPRESS=LZW"]
        )
        out_ds.SetGeoTransform(depth_ds.GetGeoTransform())
        out_ds.SetProjection(depth_ds.GetProjection())

        out_band = out_ds.GetRasterBand(1)
        if depth_nodata is not None:
            out_band.SetNoDataValue(depth_nodata)
        out_band.WriteArray(depth_data)
        out_ds.FlushCache()
        out_ds = None
        depth_ds = None

        # Convert to COG
        output_path.parent.mkdir(parents=True, exist_ok=True)
        cog_options = gdal.TranslateOptions(format="COG", creationOptions=["COMPRESS=LZW"])
        gdal.Translate(str(output_path), temp_output, options=cog_options)

        return True

    except Exception as e:
        logging.error(f"Error processing {depth_path}: {e}", exc_info=True)
        return False


def copy_as_cog(src_path: Path, dest_path: Path) -> bool:
    """
    Copy a raster file to destination as COG format.

    Args:
        src_path: Source raster path
        dest_path: Destination path

    Returns:
        True if successful
    """
    try:
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        cog_options = gdal.TranslateOptions(format="COG", creationOptions=["COMPRESS=LZW"])
        result = gdal.Translate(str(dest_path), str(src_path), options=cog_options)
        return result is not None
    except Exception as e:
        logging.error(f"Error copying {src_path} to COG: {e}")
        return False


def get_dem_path_for_reach(reach_id: str, submodels_dir: Path) -> Optional[Path]:
    """
    Get the DEM path for a specific reach from the submodels directory.

    Args:
        reach_id: The reach ID
        submodels_dir: Path to submodels directory

    Returns:
        Path to DEM TIF or None if not found
    """
    terrain_dir = submodels_dir / reach_id / "Terrain"
    if not terrain_dir.exists():
        return None

    # Look for the DEM TIF file
    dem_files = list(terrain_dir.glob("*.seamless_3dep_dem_3m_5070.tif"))
    if dem_files:
        return dem_files[0]

    # Fallback: look for any .tif that's not .hdf
    tif_files = [f for f in terrain_dir.glob("*.tif") if ".hdf" not in f.name]
    if tif_files:
        return tif_files[0]

    return None


def bridge_worker(args: tuple) -> Tuple[str, bool]:
    """
    Worker function for processing a single depth TIF.

    Args:
        args: Tuple of (depth_path, dem_path, bridge_paths, output_path, temp_base_dir)

    Returns:
        Tuple of (depth_path_str, success)
    """
    depth_path, dem_path, bridge_paths, output_path, temp_base_dir = args

    import tempfile

    with tempfile.TemporaryDirectory(dir=temp_base_dir) as temp_dir:
        temp_path = Path(temp_dir)

        if bridge_paths:
            # Process with bridge masking
            success = process_depth_with_bridges(depth_path, dem_path, bridge_paths, output_path, temp_path)
        else:
            # No bridges - just copy as COG
            success = copy_as_cog(depth_path, output_path)

    return (str(depth_path), success)


def process_bridges(collection: "CollectionData", print_progress: bool = False) -> Dict[str, any]:
    """
    Apply bridge masking to depth library TIFs.

    This function reads the bridge tile index, processes each depth TIF
    to adjust water depths where bridges intersect, and outputs the
    results as Cloud Optimized GeoTIFFs.

    Args:
        collection: CollectionData object with configuration including:
            - library_dir: Path to depth library with TIF files
            - submodels_dir: Path to submodels with terrain data
            - bridge_tile_index_path: S3 path to bridge index parquet
        print_progress: Whether to display progress information

    Returns:
        Dict with processing statistics:
            - total: Total number of TIFs processed
            - success: Number of successful processes
            - failed: Number of failed processes
            - with_bridges: List of file paths that had bridges masked
            - without_bridges: List of file paths with no bridge intersections
    """
    library_dir = Path(collection.library_dir)
    submodels_dir = Path(collection.submodels_dir)
    bridge_index_path = collection.bridge_tile_index_path
    process_count = collection.config["execution"]["OPTIMUM_PARALLEL_PROCESS_COUNT"]

    setup_gdal_environment(collection)

    if print_progress:
        print(f"Loading bridge index from: {bridge_index_path}")

    # Load bridge index
    bridge_index = load_bridge_index(bridge_index_path)
    if print_progress:
        print(f"Loaded {len(bridge_index)} bridge records")

    # Rename library to temp
    library_temp_dir = library_dir.parent / "library_temp"
    if library_temp_dir.exists():
        shutil.rmtree(library_temp_dir)

    if print_progress:
        print(f"Renaming {library_dir} to {library_temp_dir}")
    shutil.move(str(library_dir), str(library_temp_dir))

    # Create new library directory
    library_dir.mkdir(parents=True, exist_ok=True)

    # Get all depth TIFs
    depth_tifs = list(library_temp_dir.rglob("*.tif"))
    if print_progress:
        print(f"Found {len(depth_tifs)} depth TIFs to process")

    # Prepare worker arguments and track which files have bridges
    worker_args = []
    files_with_bridges = []
    files_without_bridges = []
    for depth_path in depth_tifs:
        # Get reach ID from path structure: library_temp/{reach_id}/z_xxx/f_yyy.tif
        # The reach_id is the parent of the z_xxx folder
        reach_id = depth_path.parent.parent.name

        # Get DEM path for this reach
        dem_path = get_dem_path_for_reach(reach_id, submodels_dir)
        if dem_path is None:
            logging.warning(f"No DEM found for reach {reach_id}, skipping")
            continue

        # Get depth raster bounds and query for intersecting bridges
        try:
            depth_bounds = get_raster_bounds(depth_path)
            intersecting_bridges = query_intersecting_bridges(depth_bounds, bridge_index)
            bridge_paths = intersecting_bridges["location"].tolist() if len(intersecting_bridges) > 0 else []
        except Exception as e:
            logging.error(f"Error querying bridges for {depth_path}: {e}")
            bridge_paths = []

        # Track which files have bridges
        if bridge_paths:
            files_with_bridges.append(str(depth_path))
        else:
            files_without_bridges.append(str(depth_path))

        # Calculate output path (same relative structure)
        relative_path = depth_path.relative_to(library_temp_dir)
        output_path = library_dir / relative_path

        worker_args.append(
            (
                depth_path,
                dem_path,
                bridge_paths,
                output_path,
                str(library_dir.parent),  # temp base dir
            )
        )

    # Process with multiprocessing
    success_count = 0
    fail_count = 0

    with multiprocessing.Pool(process_count) as pool:
        for i, (path, success) in enumerate(pool.imap_unordered(bridge_worker, worker_args), 1):
            if success:
                success_count += 1
            else:
                fail_count += 1
                logging.error(f"Failed to process: {path}")

            if print_progress:
                sys.stdout.write(
                    f"\rProcessing: {i}/{len(worker_args)} (success: {success_count}, failed: {fail_count})"
                )
                sys.stdout.flush()

    if print_progress:
        sys.stdout.write("\n")
        print(f"Bridge processing complete: {success_count} succeeded, {fail_count} failed")

    # Clean up temp directory
    if fail_count == 0:
        if print_progress:
            print(f"Removing temporary directory: {library_temp_dir}")
        shutil.rmtree(library_temp_dir)
    else:
        logging.warning(f"Keeping {library_temp_dir} due to {fail_count} failures")
        if print_progress:
            print(f"WARNING: Keeping {library_temp_dir} due to failures")

    return {
        "total": len(worker_args),
        "success": success_count,
        "failed": fail_count,
        "with_bridges": files_with_bridges,
        "without_bridges": files_without_bridges,
    }
