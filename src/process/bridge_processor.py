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
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from osgeo import gdal
from shapely.geometry import box

from .extent_library import setup_gdal_environment

# Enable GDAL exceptions
gdal.UseExceptions()


def get_raster_bounds(tif_path: Path) -> Tuple[float, float, float, float]:
    """Get bounding box (xmin, ymin, xmax, ymax) of a raster file."""
    ds = gdal.Open(str(tif_path))
    gt, w, h = ds.GetGeoTransform(), ds.RasterXSize, ds.RasterYSize
    ds = None
    return gt[0], gt[3] + h * gt[5], gt[0] + w * gt[1], gt[3]


def align_raster_to_reference(
    src_path: str, ref_ds: gdal.Dataset, output_path: str, resampling: int = gdal.GRA_Bilinear
) -> Optional[str]:
    """Align a source raster to match a reference raster's grid."""
    gt, w, h = ref_ds.GetGeoTransform(), ref_ds.RasterXSize, ref_ds.RasterYSize
    warp_options = gdal.WarpOptions(
        format="GTiff",
        outputBounds=(gt[0], gt[3] + h * gt[5], gt[0] + w * gt[1], gt[3]),
        xRes=abs(gt[1]),
        yRes=abs(gt[5]),
        dstSRS=ref_ds.GetProjection(),
        resampleAlg=resampling,
        creationOptions=["COMPRESS=LZW"],
    )
    try:
        result = gdal.Warp(output_path, src_path, options=warp_options)
        if result is None:
            logging.error(f"Failed to warp {src_path}")
            return None
        result = None
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
    """Process a single depth TIF with bridge masking. Returns (success, pixels_modified)."""
    try:
        depth_ds = gdal.Open(str(depth_path))
        if depth_ds is None:
            logging.error(f"Failed to open depth raster: {depth_path}")
            return False, 0

        depth_band = depth_ds.GetRasterBand(1)
        depth_data = depth_band.ReadAsArray().astype(np.float32)
        depth_nodata = depth_band.GetNoDataValue()

        if not align_raster_to_reference(str(dem_path), depth_ds, str(temp_dir / "aligned_dem.tif")):
            depth_ds = None
            return False, 0

        dem_ds = gdal.Open(str(temp_dir / "aligned_dem.tif"))
        dem_data = dem_ds.GetRasterBand(1).ReadAsArray().astype(np.float32)
        dem_ds = None

        combined_mask = np.zeros(depth_data.shape, dtype=bool)
        combined_elev = np.full(depth_data.shape, np.nan, dtype=np.float32)

        for i, bridge_path in enumerate(bridge_paths):
            aligned_path = str(temp_dir / f"aligned_bridge_{i}.tif")
            if not align_raster_to_reference(bridge_path, depth_ds, aligned_path):
                logging.warning(f"Failed to align bridge raster: {bridge_path}")
                continue
            bridge_ds = gdal.Open(aligned_path)
            if bridge_ds is None:
                continue
            band = bridge_ds.GetRasterBand(1)
            data, nodata = band.ReadAsArray().astype(np.float32), band.GetNoDataValue()
            bridge_ds = None
            valid = data != nodata
            combined_mask |= valid
            np.copyto(combined_elev, data * bridge_elev_conv_factor, where=valid)

        # Apply masking algorithm
        pixels_modified = 0
        if np.any(combined_mask):
            process_mask = combined_mask & (depth_data != depth_nodata)
            if np.any(process_mask):
                delta = (dem_data + depth_data) - combined_elev
                above_water, submerged = process_mask & (delta < 0), process_mask & (delta >= 0)
                pixels_modified = int(np.sum(above_water) + np.sum(submerged))
                depth_data[above_water] = depth_nodata if depth_nodata is not None else -9999.0
                depth_data[submerged] = delta[submerged]

        temp_output = str(temp_dir / "temp_output.tif")
        out_ds = gdal.GetDriverByName("GTiff").Create(
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

        # Need to convert to cog with call to gdal.Translate because gdal's COG driver needs to read from an existing source
        output_path.parent.mkdir(parents=True, exist_ok=True)
        gdal.Translate(
            str(output_path), temp_output, options=gdal.TranslateOptions(format="COG", creationOptions=["COMPRESS=LZW"])
        )
        return True, pixels_modified

    except Exception as e:
        logging.error(f"Error processing {depth_path}: {e}", exc_info=True)
        return False, 0


def bridge_worker(args: tuple) -> Tuple[str, bool, int]:
    """Worker for processing a single depth TIF. Returns (path, success, pixels_modified)."""
    depth_path, dem_path, bridge_paths, output_path, temp_base_dir, bridge_elev_conv_factor = args
    with tempfile.TemporaryDirectory(dir=temp_base_dir) as temp_dir:
        if bridge_paths:
            success, pixels_modified = apply_bridge_mask(
                depth_path, dem_path, bridge_paths, output_path, Path(temp_dir), bridge_elev_conv_factor
            )
        else:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            result = gdal.Translate(
                str(output_path),
                str(depth_path),
                options=gdal.TranslateOptions(format="COG", creationOptions=["COMPRESS=LZW"]),
            )
            success, pixels_modified = result is not None, 0
    return str(depth_path), success, pixels_modified


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
    _log = lambda m: print_progress and sys.stdout.write(m)

    setup_gdal_environment(collection)

    _log(f"Loading bridge index from: {bridge_index_path}\n")
    bridge_index = pd.read_parquet(bridge_index_path)
    _log(f"Loaded {len(bridge_index)} bridge records\n")

    library_temp_dir = library_dir.parent / "library_temp"
    if library_temp_dir.exists():
        shutil.rmtree(library_temp_dir)
    _log(f"Renaming {library_dir} to {library_temp_dir}\n")
    shutil.move(str(library_dir), str(library_temp_dir))
    library_dir.mkdir(parents=True, exist_ok=True)

    depth_tifs = list(library_temp_dir.rglob("*.tif"))
    _log(f"Found {len(depth_tifs)} depth TIFs to process\n")

    worker_args, files_with_bridges, files_without_bridges = [], [], []
    for depth_path in depth_tifs:
        reach_id = depth_path.parent.parent.name
        dem_files = list((submodels_dir / reach_id / "Terrain").glob("*.seamless_3dep_dem_3m_5070.tif"))
        if not dem_files:
            raise FileNotFoundError(f"No DEM found for reach {reach_id}")
        try:
            raster_box = box(*get_raster_bounds(depth_path))
            mask = bridge_index["geometry_bbox"].apply(
                lambda bbox: raster_box.intersects(box(bbox["xmin"], bbox["ymin"], bbox["xmax"], bbox["ymax"]))
            )
            bridge_paths = bridge_index[mask]["location"].tolist()
        except Exception as e:
            logging.error(f"Error querying bridges for {depth_path}: {e}")
            bridge_paths = []

        (files_with_bridges if bridge_paths else files_without_bridges).append(str(depth_path))
        worker_args.append(
            (
                depth_path,
                dem_files[0],
                bridge_paths,
                library_dir / depth_path.relative_to(library_temp_dir),
                str(library_dir.parent),
                bridge_elev_conv_factor,
            )
        )

    success_count, fail_count, files_modified = 0, 0, []
    with multiprocessing.Pool(process_count) as pool:
        for i, (path, success, pixels_modified) in enumerate(pool.imap_unordered(bridge_worker, worker_args), 1):
            if success:
                success_count += 1
                if pixels_modified > 0:
                    files_modified.append(path)
            else:
                fail_count += 1
                logging.error(f"Failed to process: {path}")
            _log(f"\rProcessing: {i}/{len(worker_args)} (success: {success_count}, failed: {fail_count})")
            print_progress and sys.stdout.flush()

    _log(f"\nBridge processing complete: {success_count} succeeded, {fail_count} failed\n")
    if fail_count == 0:
        _log(f"Removing temporary directory: {library_temp_dir}\n")
        shutil.rmtree(library_temp_dir)
    else:
        logging.warning(f"Keeping {library_temp_dir} due to {fail_count} failures")
        _log(f"WARNING: Keeping {library_temp_dir} due to failures\n")

    return {
        "total": len(worker_args),
        "success": success_count,
        "failed": fail_count,
        "with_bridges": files_with_bridges,
        "without_bridges": files_without_bridges,
        "modified": files_modified,
    }
