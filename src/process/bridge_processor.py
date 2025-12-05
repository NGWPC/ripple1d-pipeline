"""
Bridge processor module for masking depth library TIFs based on bridge locations.

Uses GDAL/OGR command-line tools via subprocess for all operations. This has benefits of maintainability and also easier debugging if you have existing intermediate outputs before the subprocess call.
"""

import json
import logging
import multiprocessing
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Tuple

from ..setup.collection_data import CollectionData
from .extent_library import get_all_tif_paths, setup_gdal_environment


def run_cmd(cmd: List, description: str) -> subprocess.CompletedProcess:
    """Run a command and raise on failure. This packages the error handling pattern used several times in extent_library.py whenever subprocess.run is called there"""
    result = subprocess.run([str(c) for c in cmd], capture_output=True, text=True)
    if result.returncode != 0:
        logging.debug(f"{description} stdout: {result.stdout}")
        logging.error(f"{description} stderr: {result.stderr}")
        logging.debug(f"Command: {' '.join(str(c) for c in cmd)}")
        raise RuntimeError(f"{description} failed")
    return result


def get_raster_info(tif_path: Path) -> Tuple[Tuple[float, float, float, float], Tuple[float, float], float]:
    """Get bounds (xmin, ymin, xmax, ymax), resolution (xres, yres), and nodata value from a raster."""
    result = run_cmd(["gdalinfo", "-json", tif_path], f"gdalinfo {tif_path}")
    info = json.loads(result.stdout)
    corners = info["cornerCoordinates"]
    bounds = (corners["upperLeft"][0], corners["lowerRight"][1], corners["lowerRight"][0], corners["upperLeft"][1])
    # geoTransform: [originX, pixelWidth, 0, originY, 0, pixelHeight]
    gt = info["geoTransform"]
    res = (abs(gt[1]), abs(gt[5]))
    nodata = info["bands"][0].get("noDataValue", -9999.0)
    return bounds, res, nodata


def align_raster(
    src_path: Path,
    output_path: Path,
    bounds: Tuple[float, float, float, float],
    res: Tuple[float, float],
    nodata: float = None,
    target_crs: str = None,
) -> None:
    """Align a raster to the specified extent and resolution, outputting a VRT.

    If target_crs is provided, the source raster will be reprojected to that CRS.
    """
    xmin, ymin, xmax, ymax = bounds
    xres, yres = res
    cmd = [
        "gdalwarp",
        "-overwrite",
        "-of",
        "VRT",
        "-te",
        xmin,
        ymin,
        xmax,
        ymax,
        "-tr",
        xres,
        yres,
        "-r",
        "bilinear",
    ]
    if target_crs is not None:
        cmd.extend(["-t_srs", target_crs])
    if nodata is not None:
        cmd.extend(["-dstnodata", nodata])
    cmd.extend([src_path, output_path])
    run_cmd(cmd, f"gdalwarp align {src_path}")


def apply_bridge_mask(args: Tuple) -> Tuple[str, bool]:
    """
    Process a single depth TIF with bridge masking (worker function for multiprocessing).

    Expects pre-aligned DEM and bridge VRTs. Runs gdal_calc for the masking computation
    and overwrites the original file on success.
    """
    depth_path, aligned_dem, aligned_bridges, library_parent, conv_factor, depth_nodata = args
    depth_path = Path(depth_path)

    try:
        with tempfile.TemporaryDirectory(dir=library_parent) as temp_dir:
            temp_dir = Path(temp_dir)

            # Algorithm: delta = (DEM + depth) - bridge_elev * conv_factor
            #   - bridge above water (delta < 0): set to nodata
            #   - bridge submerged (delta >= 0): depth = delta
            #   - no bridge (C is nodata): keep original depth
            calc_expr = (
                f"numpy.where((C == -9999) | numpy.isnan(C), A, "
                f"numpy.where((B + A) - (C * {conv_factor}) < 0, {depth_nodata}, "
                f"(B + A) - (C * {conv_factor})))"
            )

            temp_output = temp_dir / "result.tif"
            run_cmd(
                [
                    "gdal_calc",
                    "-A",
                    depth_path,
                    "-B",
                    aligned_dem,
                    "-C",
                    aligned_bridges,
                    "--outfile",
                    temp_output,
                    f"--NoDataValue={depth_nodata}",
                    "--co=COMPRESS=LZW",
                    "--quiet",
                    f"--calc={calc_expr}",
                ],
                "gdal_calc",
            )

            # gdal_calc can't work with COG so need to convert here
            cog_output = temp_dir / "output.tif"
            run_cmd(["gdal_translate", "-of", "COG", "-co", "COMPRESS=LZW", temp_output, cog_output], "gdal_translate")
            shutil.move(str(cog_output), str(depth_path))

        return (str(depth_path), True)

    except Exception as e:
        logging.error(f"Error processing {depth_path}: {e}", exc_info=True)
        return (str(depth_path), False)


def process_bridges(collection: "CollectionData", print_progress: bool = False) -> Dict[str, any]:
    """
    Apply bridge masking to depth library TIFs in place.
    """
    if print_progress:
        logging.getLogger().setLevel(logging.INFO)

    library_dir = Path(collection.library_dir)
    submodels_dir = Path(collection.submodels_dir)
    bridge_index_path = collection.bridge_tile_index_path
    conv_factor = collection.config["bridge_processing"]["BRIDGE_ELEV_CONV_FACTOR"]

    setup_gdal_environment(collection)

    depth_tifs = get_all_tif_paths(library_dir)
    logging.info(f"Found {len(depth_tifs)} depth TIFs to process")

    reaches: Dict[str, List[Path]] = {}
    for tif in depth_tifs:
        reach_id = tif.parent.parent.name
        reaches.setdefault(reach_id, []).append(tif)

    success_count, fail_count, processed = 0, 0, 0
    files_with_bridges, files_without_bridges, files_modified = [], [], []

    for reach_id, reach_tifs in reaches.items():
        dem_path = submodels_dir / reach_id / "Terrain" / f"{reach_id}.seamless_3dep_dem_3m_5070.tif"
        if not dem_path.exists():
            raise FileNotFoundError(f"No DEM found for reach {reach_id}: {dem_path}")

        # The bridge query requires that all bridge tiles be in epsg 5070
        try:
            bounds, depth_res, depth_nodata = get_raster_info(reach_tifs[0])
            xmin, ymin, xmax, ymax = bounds
            result = run_cmd(
                [
                    "ogr2ogr",
                    "-f",
                    "CSV",
                    "-spat",
                    xmin,
                    ymin,
                    xmax,
                    ymax,
                    "-select",
                    "location",
                    "/vsistdout/",
                    bridge_index_path,
                ],
                "ogr2ogr bridge query",
            )
            lines = result.stdout.strip().split("\n")
            bridge_paths = lines[1:] if len(lines) > 1 else []
        except Exception as e:
            logging.error(f"Error querying bridges for reach {reach_id}: {e}")
            fail_count += len(reach_tifs)
            processed += len(reach_tifs)
            continue

        if not bridge_paths:
            # No bridges in this reach - nothing to do
            for depth_path in reach_tifs:
                files_without_bridges.append(str(depth_path))
                success_count += 1
                processed += 1
            logging.info(f"Reach {reach_id}: no bridges, skipped {len(reach_tifs)} TIFs")
            continue

        files_with_bridges.extend(str(p) for p in reach_tifs)

        with tempfile.TemporaryDirectory(dir=str(library_dir.parent)) as reach_temp_dir:
            reach_temp_dir = Path(reach_temp_dir)

            bridges_vrt = reach_temp_dir / "bridges.vrt"
            run_cmd(["gdalbuildvrt", bridges_vrt] + bridge_paths, "gdalbuildvrt")

            # Depth rasters are in EPSG:5070, reproject DEM and bridges to match
            target_crs = "EPSG:5070"

            aligned_dem = reach_temp_dir / "aligned_dem.vrt"
            align_raster(dem_path, aligned_dem, bounds, depth_res, nodata=depth_nodata, target_crs=target_crs)

            aligned_bridges = reach_temp_dir / "aligned_bridges.vrt"
            align_raster(bridges_vrt, aligned_bridges, bounds, depth_res, nodata=depth_nodata, target_crs=target_crs)

            worker_args = [
                (
                    str(depth_path),
                    str(aligned_dem),
                    str(aligned_bridges),
                    str(library_dir.parent),
                    conv_factor,
                    depth_nodata,
                )
                for depth_path in reach_tifs
            ]

            num_workers = collection.config["execution"]["OPTIMUM_PARALLEL_PROCESS_COUNT"]
            with multiprocessing.Pool(processes=num_workers) as pool:
                for depth_path, success in pool.imap_unordered(apply_bridge_mask, worker_args):
                    if success:
                        success_count += 1
                        files_modified.append(depth_path)
                    else:
                        fail_count += 1
                    processed += 1

            logging.info(f"Reach {reach_id}: processed {len(reach_tifs)} TIFs with {len(bridge_paths)} bridges")

    logging.info(f"Bridge processing complete: {success_count} succeeded, {fail_count} failed")

    return {
        "total": len(depth_tifs),
        "success": success_count,
        "failed": fail_count,
        "with_bridges": files_with_bridges,
        "without_bridges": files_without_bridges,
        "modified": files_modified,
    }
