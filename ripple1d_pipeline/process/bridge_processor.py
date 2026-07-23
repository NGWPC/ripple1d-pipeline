"""
Bridge processor module for masking depth library TIFs based on bridge locations.

Three strategies (controlled by bridge_processing.STRATEGY in config):
  - "current": per-TIF gdal_calc mask + COG overwrite (original approach)
  - "clearance": one clearance TIF per reach (3a-i)
  - "clearance_per_tile": one clearance TIF per bridge tile (3a-ii)

Clearance strategies produce files in library/<reach_id>/bridge_heights/
which downstream consumers (F2F, QGIS) use via VRT expression to apply masking on read.
"""

import json
import logging
import multiprocessing
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Tuple

from ..setup.collection_data import CollectionData
from .extent_library import get_all_tif_paths

logger = logging.getLogger(__name__)


def run_cmd(cmd: list, description: str) -> subprocess.CompletedProcess:
    result = subprocess.run([str(c) for c in cmd], capture_output=True, text=True)
    if result.returncode != 0:
        logger.debug(f"{description} stdout: {result.stdout}")
        logger.error(f"{description} stderr: {result.stderr}")
        logger.debug(f"Command: {' '.join(str(c) for c in cmd)}")
        raise RuntimeError(f"{description} failed")
    return result


def get_raster_info(
    tif_path: Path,
) -> Tuple[Tuple[float, float, float, float], Tuple[float, float], float]:
    """Get bounds (xmin, ymin, xmax, ymax), resolution (xres, yres), and nodata value from a raster."""
    result = run_cmd(["gdalinfo", "-json", tif_path], f"gdalinfo {tif_path}")
    info = json.loads(result.stdout)
    corners = info["cornerCoordinates"]
    bounds = (
        corners["upperLeft"][0],
        corners["lowerRight"][1],
        corners["lowerRight"][0],
        corners["upperLeft"][1],
    )
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
    resampling: str = "bilinear",
) -> None:
    """Align a raster to the specified extent and resolution, outputting a VRT."""
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
        resampling,
    ]
    if target_crs is not None:
        cmd.extend(["-t_srs", target_crs])
    if nodata is not None:
        cmd.extend(["-dstnodata", nodata])
    cmd.extend([src_path, output_path])
    run_cmd(cmd, f"gdalwarp align {src_path}")


def _query_bridges(bridge_index_path, xmin, ymin, xmax, ymax):
    """Query bridge index for tiles intersecting the given bounds."""
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
    return lines[1:] if len(lines) > 1 else []


# ---------------------------------------------------------------------------
# Strategy: "current" - per-TIF mask + COG overwrite
# ---------------------------------------------------------------------------
def apply_bridge_mask(args: Tuple) -> Tuple[str, bool]:
    """Process a single depth TIF with bridge masking (worker function for multiprocessing)."""
    (
        depth_path,
        aligned_dem,
        aligned_bridges,
        library_parent,
        conv_factor,
        depth_nodata,
        reach_id,
    ) = args
    depth_path = Path(depth_path)

    try:
        with tempfile.TemporaryDirectory(dir=library_parent, prefix=f"{reach_id}_") as temp_dir:
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
                    "--hideNoData",
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
            run_cmd(
                ["gdal_translate", "-of", "COG", "-co", "COMPRESS=LZW", temp_output, cog_output],
                "gdal_translate",
            )
            logger.debug(f"Finished processing {depth_path}, moving result to original location")
            shutil.move(str(cog_output), str(depth_path))
            logger.debug(f"Successfully processed {depth_path}")
        return (str(depth_path), True)

    except Exception as e:
        logger.exception(f"Error processing {depth_path}: {e}")
        return (str(depth_path), False)


def _process_reach_current(
    reach_id,
    reach_dir,
    dem_path,
    intersecting_bridge_paths,
    bounds,
    depth_res,
    depth_nodata,
    conv_factor,
    library_dir,
    num_workers,
):
    """Current strategy: mask each depth TIF in-place via multiprocessing."""
    t_start = time.perf_counter()
    reach_tifs = [p for p in get_all_tif_paths(reach_dir) if p.parent.name != "bridge_heights"]

    with tempfile.TemporaryDirectory(dir=str(library_dir.parent), prefix=f"{reach_id}_") as reach_temp_dir:
        reach_temp_dir = Path(reach_temp_dir)

        t = time.perf_counter()
        bridges_vrt = reach_temp_dir / "bridges.vrt"
        run_cmd(["gdalbuildvrt", bridges_vrt] + intersecting_bridge_paths, "gdalbuildvrt")
        logger.info(f"Reach {reach_id}: buildvrt done ({time.perf_counter() - t:.1f}s)")

        target_crs = "EPSG:5070"

        t = time.perf_counter()
        aligned_dem = reach_temp_dir / "aligned_dem.vrt"
        align_raster(dem_path, aligned_dem, bounds, depth_res, nodata=depth_nodata, target_crs=target_crs)
        logger.info(f"Reach {reach_id}: align DEM VRT done ({time.perf_counter() - t:.1f}s)")

        t = time.perf_counter()
        aligned_bridges = reach_temp_dir / "aligned_bridges.vrt"
        align_raster(bridges_vrt, aligned_bridges, bounds, depth_res, nodata=depth_nodata, target_crs=target_crs)
        logger.info(f"Reach {reach_id}: align bridges VRT done ({time.perf_counter() - t:.1f}s)")

        worker_args = [
            (
                str(depth_path),
                str(aligned_dem),
                str(aligned_bridges),
                str(library_dir.parent),
                conv_factor,
                depth_nodata,
                reach_id,
            )
            for depth_path in reach_tifs
        ]

        t = time.perf_counter()
        files_modified = []
        with multiprocessing.Pool(processes=num_workers) as pool:
            for depth_path, success in pool.imap_unordered(apply_bridge_mask, worker_args):
                if success:
                    files_modified.append(depth_path)
                else:
                    logger.error(f"Failed to process {depth_path}")
        dt_mask = time.perf_counter() - t
        logger.info(f"Reach {reach_id}: mask+COG {len(reach_tifs)} TIFs done ({dt_mask:.1f}s)")

    dt_total = time.perf_counter() - t_start
    logger.info(
        f"Reach {reach_id}: processed {len(reach_tifs)} TIFs with "
        f"{len(intersecting_bridge_paths)} bridges (total: {dt_total:.1f}s)"
    )
    return files_modified


# ---------------------------------------------------------------------------
# Strategy: "clearance" - one clearance TIF per reach
# ---------------------------------------------------------------------------
def _process_reach_clearance(
    reach_id, reach_dir, dem_path, intersecting_bridge_paths, bounds, depth_res, depth_nodata, conv_factor, library_dir
):
    """Clearance strategy: produce one bridge_heights/combined.tif per reach.

    clearance = (bridge_elev * conv_factor) - DEM
    Where no bridge: clearance = 0 (not nodata, so VRT expression keeps depth unchanged).
    """
    t_start = time.perf_counter()

    with tempfile.TemporaryDirectory(dir=str(library_dir.parent), prefix=f"{reach_id}_") as temp_dir:
        temp_dir = Path(temp_dir)

        t = time.perf_counter()
        bridges_vrt = temp_dir / "bridges.vrt"
        run_cmd(["gdalbuildvrt", bridges_vrt] + intersecting_bridge_paths, "gdalbuildvrt")
        logger.info(f"Reach {reach_id}: buildvrt done ({time.perf_counter() - t:.1f}s)")

        target_crs = "EPSG:5070"

        # Align DEM (bilinear is correct for continuous terrain)
        t = time.perf_counter()
        aligned_dem = temp_dir / "aligned_dem.vrt"
        align_raster(dem_path, aligned_dem, bounds, depth_res, nodata=depth_nodata, target_crs=target_crs)
        logger.info(f"Reach {reach_id}: align DEM VRT done ({time.perf_counter() - t:.1f}s)")

        # Align bridges with nearest-neighbor (preserves discrete elevation values)
        t = time.perf_counter()
        aligned_bridges = temp_dir / "aligned_bridges.vrt"
        align_raster(
            bridges_vrt,
            aligned_bridges,
            bounds,
            depth_res,
            nodata=depth_nodata,
            target_crs=target_crs,
            resampling="near",
        )
        logger.info(f"Reach {reach_id}: align bridges VRT done ({time.perf_counter() - t:.1f}s)")

        # Materialize aligned DEM and bridges (avoids lazy S3 re-fetch during gdal_calc)
        t = time.perf_counter()
        aligned_dem_tif = temp_dir / "aligned_dem.tif"
        run_cmd(["gdal_translate", "-q", aligned_dem, aligned_dem_tif], "materialize DEM")
        logger.info(f"Reach {reach_id}: materialize DEM done ({time.perf_counter() - t:.1f}s)")

        t = time.perf_counter()
        aligned_bridges_tif = temp_dir / "aligned_bridges.tif"
        run_cmd(["gdal_translate", "-q", aligned_bridges, aligned_bridges_tif], "materialize bridges")
        logger.info(f"Reach {reach_id}: materialize bridges done ({time.perf_counter() - t:.1f}s)")

        # Clearance = 0 means "no bridge" in the VRT expression ((B2 == 0) ? B1 : ...).
        # A real clearance of exactly 0 (bridge at ground level) is physically impossible.
        t = time.perf_counter()
        clearance_expr = f"numpy.where((C == -9999) | numpy.isnan(C), 0, C * {conv_factor} - B)"
        clearance_temp = temp_dir / "clearance.tif"
        run_cmd(
            [
                "gdal_calc",
                "--overwrite",
                "--hideNoData",
                "-B",
                aligned_dem_tif,
                "-C",
                aligned_bridges_tif,
                "--outfile",
                clearance_temp,
                "--NoDataValue=0",
                "--quiet",
                f"--calc={clearance_expr}",
            ],
            "gdal_calc clearance",
        )
        logger.info(f"Reach {reach_id}: gdal_calc clearance done ({time.perf_counter() - t:.1f}s)")

        t = time.perf_counter()
        bridge_heights_dir = reach_dir / "bridge_heights"
        bridge_heights_dir.mkdir(exist_ok=True)
        clearance_output = bridge_heights_dir / "combined.tif"
        run_cmd(
            ["gdal_translate", "-of", "GTiff", "-co", "COMPRESS=LZW", clearance_temp, clearance_output],
            "write clearance",
        )
        logger.info(f"Reach {reach_id}: write clearance TIF done ({time.perf_counter() - t:.1f}s)")

    dt = time.perf_counter() - t_start
    size_kb = clearance_output.stat().st_size / 1024
    logger.info(
        f"Reach {reach_id}: clearance TIF generated in {dt:.1f}s "
        f"({size_kb:.0f} KB, {len(intersecting_bridge_paths)} bridge tiles)"
    )
    return str(clearance_output)


# ---------------------------------------------------------------------------
# Strategy: "clearance_per_tile" - one clearance TIF per bridge tile
# ---------------------------------------------------------------------------
def _process_reach_clearance_per_tile(
    reach_id, reach_dir, dem_path, intersecting_bridge_paths, bounds, depth_res, depth_nodata, conv_factor, library_dir
):
    """Per-tile clearance strategy: one clearance TIF per bridge tile.

    Each output covers only the intersection of the bridge tile footprint with
    the reach, so downstream VRT/GTI can skip them via SkipNonContributingSources.
    """
    t_start = time.perf_counter()
    reach_xmin, reach_ymin, reach_xmax, reach_ymax = bounds
    target_crs = "EPSG:5070"
    clearance_outputs = []

    for bridge_path in intersecting_bridge_paths:
        # bridge_path is a GDAL virtual path (/vsis3/...); Path() would convert
        # forward slashes to backslashes on Windows, breaking GDAL.
        bridge_stem = bridge_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
        label = f"{reach_id}/tile_{bridge_stem}"

        try:
            bridge_bounds, _, _ = get_raster_info(bridge_path)
        except Exception as e:
            logger.error(f"{label}: failed to read bridge tile info: {e}")
            continue

        bxmin, bymin, bxmax, bymax = bridge_bounds
        ixmin = max(reach_xmin, bxmin)
        iymin = max(reach_ymin, bymin)
        ixmax = min(reach_xmax, bxmax)
        iymax = min(reach_ymax, bymax)

        if ixmin >= ixmax or iymin >= iymax:
            logger.warning(f"{label}: no spatial overlap with reach, skipping")
            continue

        tile_bounds = (ixmin, iymin, ixmax, iymax)

        with tempfile.TemporaryDirectory(dir=str(library_dir.parent), prefix=f"{reach_id}_{bridge_stem}_") as temp_dir:
            temp_dir = Path(temp_dir)

            t = time.perf_counter()
            aligned_dem = temp_dir / "aligned_dem.vrt"
            align_raster(dem_path, aligned_dem, tile_bounds, depth_res, nodata=depth_nodata, target_crs=target_crs)

            aligned_bridge = temp_dir / "aligned_bridge.vrt"
            align_raster(
                bridge_path,
                aligned_bridge,
                tile_bounds,
                depth_res,
                nodata=depth_nodata,
                target_crs=target_crs,
                resampling="near",
            )

            aligned_dem_tif = temp_dir / "aligned_dem.tif"
            run_cmd(["gdal_translate", "-q", aligned_dem, aligned_dem_tif], "materialize DEM")

            aligned_bridge_tif = temp_dir / "aligned_bridge.tif"
            run_cmd(["gdal_translate", "-q", aligned_bridge, aligned_bridge_tif], "materialize bridge")

            clearance_expr = f"numpy.where((C == -9999) | numpy.isnan(C), 0, C * {conv_factor} - B)"
            clearance_temp = temp_dir / "clearance.tif"
            run_cmd(
                [
                    "gdal_calc",
                    "--overwrite",
                    "--hideNoData",
                    "-B",
                    aligned_dem_tif,
                    "-C",
                    aligned_bridge_tif,
                    "--outfile",
                    clearance_temp,
                    "--NoDataValue=0",
                    "--quiet",
                    f"--calc={clearance_expr}",
                ],
                "gdal_calc clearance",
            )

            bridge_heights_dir = reach_dir / "bridge_heights"
            bridge_heights_dir.mkdir(exist_ok=True)
            clearance_output = bridge_heights_dir / f"{bridge_stem}.tif"
            run_cmd(
                ["gdal_translate", "-of", "GTiff", "-co", "COMPRESS=LZW", clearance_temp, clearance_output],
                "write clearance",
            )

            dt = time.perf_counter() - t
            size_kb = clearance_output.stat().st_size / 1024
            logger.info(f"{label}: clearance TIF done ({dt:.1f}s, {size_kb:.0f} KB)")
            clearance_outputs.append(str(clearance_output))

    dt_total = time.perf_counter() - t_start
    logger.info(f"Reach {reach_id}: {len(clearance_outputs)} per-tile clearance TIFs in {dt_total:.1f}s")
    return clearance_outputs


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def process_bridges(collection: "CollectionData") -> dict[str, any]:
    """Apply bridge masking to a collection's depth library."""
    library_dir = Path(collection.library_dir)
    submodels_dir = Path(collection.submodels_dir)
    bridge_index_path = collection.bridge_tile_index_path
    conv_factor = collection.config["bridge_processing"]["BRIDGE_ELEV_CONV_FACTOR"]
    strategy = collection.config["bridge_processing"].get("STRATEGY", "clearance")
    num_workers = collection.config["execution"]["OPTIMUM_PARALLEL_PROCESS_COUNT"]

    logger.info(f"Bridge processing strategy: {strategy}")

    reach_dirs = [d for d in library_dir.iterdir() if d.is_dir()]
    logger.info(f"Found {len(reach_dirs)} reaches to process")
    logger.info(f"Bridge index: {bridge_index_path}")

    t_total = time.perf_counter()
    reaches_with_bridges, reaches_without_bridges = [], []
    files_modified, clearance_files = [], []

    for reach_dir in reach_dirs:
        reach_id = reach_dir.name
        logger.debug(f"Processing reach {reach_id}")

        sample_reach_tif = next(
            (p for p in reach_dir.rglob("*.tif") if p.parent.name != "bridge_heights"), None
        )
        if sample_reach_tif is None:
            logger.warning(f"Reach {reach_id}: no TIF files found, skipping")
            continue
        dem_path = submodels_dir / reach_id / "Terrain" / f"{reach_id}.seamless_3dep_dem_3m_5070.tif"
        if not dem_path.exists():
            raise FileNotFoundError(f"No DEM found for reach {reach_id}: {dem_path}")

        # The bridge query requires that all bridge tiles be in epsg 5070
        try:
            bounds, depth_res, depth_nodata = get_raster_info(sample_reach_tif)
            xmin, ymin, xmax, ymax = bounds

            t_query = time.perf_counter()
            intersecting_bridge_paths = _query_bridges(bridge_index_path, xmin, ymin, xmax, ymax)
            dt_query = time.perf_counter() - t_query

            logger.debug(f"Reach {reach_id}: {len(intersecting_bridge_paths)} bridges (query: {dt_query:.3f}s)")
        except Exception as e:
            logger.exception(f"Error querying bridges for reach {reach_id}: {e}")
            continue

        if not intersecting_bridge_paths:
            reaches_without_bridges.append(reach_id)
            logger.info(f"Reach {reach_id}: no bridges, skipped")
            continue

        reaches_with_bridges.append(reach_id)

        if strategy == "current":
            # based on the cpu utilization, the num_workers maybe increased by x1.5, or x2 or even x3.
            modified = _process_reach_current(
                reach_id,
                reach_dir,
                dem_path,
                intersecting_bridge_paths,
                bounds,
                depth_res,
                depth_nodata,
                conv_factor,
                library_dir,
                num_workers * 2,
            )
            files_modified.extend(modified)

        elif strategy == "clearance":
            clearance_path = _process_reach_clearance(
                reach_id,
                reach_dir,
                dem_path,
                intersecting_bridge_paths,
                bounds,
                depth_res,
                depth_nodata,
                conv_factor,
                library_dir,
            )
            clearance_files.append(clearance_path)

        elif strategy == "clearance_per_tile":
            tile_paths = _process_reach_clearance_per_tile(
                reach_id,
                reach_dir,
                dem_path,
                intersecting_bridge_paths,
                bounds,
                depth_res,
                depth_nodata,
                conv_factor,
                library_dir,
            )
            clearance_files.extend(tile_paths)

    dt_total = time.perf_counter() - t_total
    logger.info(
        f"Bridge processing complete ({strategy}): "
        f"{len(reaches_with_bridges)} with bridges, "
        f"{len(reaches_without_bridges)} without, "
        f"total: {dt_total:.1f}s"
    )

    return {
        "strategy": strategy,
        "reaches_with_bridges": reaches_with_bridges,
        "reaches_without_bridges": reaches_without_bridges,
        "modified": files_modified,
        "clearance_files": clearance_files,
    }
