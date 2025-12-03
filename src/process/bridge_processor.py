"""
Bridge processor module for masking depth library TIFs based on bridge locations.

This module processes depth grids to adjust water depth values where bridges
are located. The algorithm calculates Water Surface Elevation (WSE) and compares
it to bridge elevations to determine if bridges are submerged or not.
"""

import logging
import multiprocessing
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from osgeo import gdal
from shapely.geometry import box

from .extent_library import setup_gdal_environment

if TYPE_CHECKING:
    from ..setup.collection_data import CollectionData

# Enable GDAL exceptions
gdal.UseExceptions()


def get_raster_bounds(tif_path: Path) -> Tuple[float, float, float, float]:
    """
    Get the bounding box of a raster file.

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


def apply_bridge_mask(
    depth_path: Path,
    dem_path: Path,
    bridge_paths: List[str],
    output_path: Path,
    temp_dir: Path,
    bridge_elev_conv_factor: float,
) -> Tuple[bool, int]:
    """
    Process a single depth TIF with bridge masking.

    Args:
        depth_path: Path to input depth raster
        dem_path: Path to DEM raster for this reach
        bridge_paths: List of bridge raster paths (can be /vsis3/)
        output_path: Path for output COG
        temp_dir: Temporary directory for intermediate files
        bridge_elev_conv_factor: Conversion factor for bridge elevations to depth grid units

    Returns:
        Tuple of (success, pixels_modified)
    """
    try:
        # Using depth raster as reference
        depth_ds = gdal.Open(str(depth_path))
        if depth_ds is None:
            logging.error(f"Failed to open depth raster: {depth_path}")
            return (False, 0)

        depth_band = depth_ds.GetRasterBand(1)
        depth_data = depth_band.ReadAsArray().astype(np.float32)
        depth_nodata = depth_band.GetNoDataValue()

        aligned_dem_path = str(temp_dir / "aligned_dem.tif")
        if not align_raster_to_reference(str(dem_path), depth_ds, aligned_dem_path):
            depth_ds = None
            return (False, 0)

        dem_ds = gdal.Open(aligned_dem_path)
        dem_data = dem_ds.GetRasterBand(1).ReadAsArray().astype(np.float32)
        dem_ds = None

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

            valid_bridge = bridge_data != bridge_nodata

            # Convert bridge elevations to depth grid units
            bridge_data = bridge_data * bridge_elev_conv_factor

            # Update combined bridge data
            combined_bridge_mask |= valid_bridge
            np.copyto(combined_bridge_elev, bridge_data, where=valid_bridge)

        # bridge masking algorithm
        pixels_modified = 0
        if np.any(combined_bridge_mask):
            # Only Calculate WSE where we have valid depth
            has_valid_depth = depth_data != depth_nodata
            process_mask = combined_bridge_mask & has_valid_depth

            if np.any(process_mask):
                wse = dem_data + depth_data
                delta = wse - combined_bridge_elev

                # - If bridge > WSE (delta < 0): bridge is above water, set depth = nodata
                # - If bridge <= WSE (delta >= 0): bridge submerged, depth = WSE - bridge_elev
                bridge_above_water = process_mask & (delta < 0)
                bridge_submerged = process_mask & (delta >= 0)

                pixels_modified = int(np.sum(bridge_above_water) + np.sum(bridge_submerged))

                nodata_value = depth_nodata if depth_nodata is not None else -9999.0
                depth_data[bridge_above_water] = nodata_value
                # When bridge is submerged, depth is water height above the bridge deck
                depth_data[bridge_submerged] = delta[bridge_submerged]

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

        output_path.parent.mkdir(parents=True, exist_ok=True)
        cog_options = gdal.TranslateOptions(format="COG", creationOptions=["COMPRESS=LZW"])
        gdal.Translate(str(output_path), temp_output, options=cog_options)

        return (True, pixels_modified)

    except Exception as e:
        logging.error(f"Error processing {depth_path}: {e}", exc_info=True)
        return (False, 0)


def bridge_worker(args: tuple) -> Tuple[str, bool, int]:
    """
    Worker function for processing a single depth TIF.

    Args:
        args: Tuple of (depth_path, dem_path, bridge_paths, output_path, temp_base_dir, bridge_elev_conv_factor)

    Returns:
        Tuple of (depth_path_str, success, pixels_modified)
    """
    depth_path, dem_path, bridge_paths, output_path, temp_base_dir, bridge_elev_conv_factor = args

    with tempfile.TemporaryDirectory(dir=temp_base_dir) as temp_dir:
        temp_path = Path(temp_dir)

        if bridge_paths:
            success, pixels_modified = apply_bridge_mask(
                depth_path, dem_path, bridge_paths, output_path, temp_path, bridge_elev_conv_factor
            )
        else:
            # No bridges so just copy as COG
            output_path.parent.mkdir(parents=True, exist_ok=True)
            cog_options = gdal.TranslateOptions(format="COG", creationOptions=["COMPRESS=LZW"])
            result = gdal.Translate(str(output_path), str(depth_path), options=cog_options)
            success = result is not None
            pixels_modified = 0

    return (str(depth_path), success, pixels_modified)


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
            - with_bridges: List of file paths that had bridge intersections
            - without_bridges: List of file paths with no bridge intersections
            - modified: List of file paths where depth values were actually modified
    """
    library_dir = Path(collection.library_dir)
    submodels_dir = Path(collection.submodels_dir)
    bridge_index_path = collection.bridge_tile_index_path
    process_count = collection.config["execution"]["OPTIMUM_PARALLEL_PROCESS_COUNT"]
    bridge_elev_conv_factor = collection.config["bridge_processing"]["BRIDGE_ELEV_CONV_FACTOR"]

    setup_gdal_environment(collection)

    if print_progress:
        sys.stdout.write(f"Loading bridge index from: {bridge_index_path}\n")

    bridge_index = pd.read_parquet(bridge_index_path)
    if print_progress:
        sys.stdout.write(f"Loaded {len(bridge_index)} bridge records\n")

    library_temp_dir = library_dir.parent / "library_temp"
    if library_temp_dir.exists():
        shutil.rmtree(library_temp_dir)

    if print_progress:
        sys.stdout.write(f"Renaming {library_dir} to {library_temp_dir}\n")
    shutil.move(str(library_dir), str(library_temp_dir))

    library_dir.mkdir(parents=True, exist_ok=True)

    depth_tifs = list(library_temp_dir.rglob("*.tif"))
    if print_progress:
        sys.stdout.write(f"Found {len(depth_tifs)} depth TIFs to process\n")

    worker_args = []
    files_with_bridges = []
    files_without_bridges = []
    for depth_path in depth_tifs:
        # Get reach ID from path structure: library_temp/{reach_id}/z_xxx/f_yyy.tif
        # The reach_id is the parent of the z_xxx folder
        reach_id = depth_path.parent.parent.name

        terrain_dir = submodels_dir / reach_id / "Terrain"
        dem_files = list(terrain_dir.glob("*.seamless_3dep_dem_3m_5070.tif"))
        if not dem_files:
            raise FileNotFoundError(f"No DEM found for reach {reach_id} in {terrain_dir}")
        dem_path = dem_files[0]

        try:
            depth_bounds = get_raster_bounds(depth_path)
            raster_box = box(*depth_bounds)
            mask = bridge_index["geometry_bbox"].apply(
                lambda bbox: raster_box.intersects(box(bbox["xmin"], bbox["ymin"], bbox["xmax"], bbox["ymax"]))
            )
            bridge_paths = bridge_index[mask]["location"].tolist()
        except Exception as e:
            logging.error(f"Error querying bridges for {depth_path}: {e}")
            bridge_paths = []

        if bridge_paths:
            files_with_bridges.append(str(depth_path))
        else:
            files_without_bridges.append(str(depth_path))

        relative_path = depth_path.relative_to(library_temp_dir)
        output_path = library_dir / relative_path

        worker_args.append(
            (
                depth_path,
                dem_path,
                bridge_paths,
                output_path,
                str(library_dir.parent),  # temp base dir
                bridge_elev_conv_factor,
            )
        )

    success_count = 0
    fail_count = 0
    files_modified = []

    with multiprocessing.Pool(process_count) as pool:
        for i, (path, success, pixels_modified) in enumerate(pool.imap_unordered(bridge_worker, worker_args), 1):
            if success:
                success_count += 1
                if pixels_modified > 0:
                    files_modified.append(path)
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
        sys.stdout.write(f"Bridge processing complete: {success_count} succeeded, {fail_count} failed\n")

    if fail_count == 0:
        if print_progress:
            sys.stdout.write(f"Removing temporary directory: {library_temp_dir}\n")
        shutil.rmtree(library_temp_dir)
    else:
        logging.warning(f"Keeping {library_temp_dir} due to {fail_count} failures")
        if print_progress:
            sys.stdout.write(f"WARNING: Keeping {library_temp_dir} due to failures\n")

    return {
        "total": len(worker_args),
        "success": success_count,
        "failed": fail_count,
        "with_bridges": files_with_bridges,
        "without_bridges": files_without_bridges,
        "modified": files_modified,
    }
