"""
Bridge processor module for masking depth library TIFs based on bridge locations.

Uses GDAL/OGR command-line tools via subprocess for all operations.
gdal_calc handles raster alignment automatically, eliminating need for explicit gdalwarp calls.
"""

import json
import logging
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Tuple

from .extent_library import setup_gdal_environment

if TYPE_CHECKING:
    from ..setup.collection_data import CollectionData


def run_cmd(cmd: List, description: str) -> subprocess.CompletedProcess:
    """Run a command and raise on failure. This packages the error handling pattern used several times in extent_library.py whenever subprocess.run is called there"""
    result = subprocess.run([str(c) for c in cmd], capture_output=True, text=True)
    if result.returncode != 0:
        logging.debug(f"{description} stdout: {result.stdout}")
        logging.error(f"{description} stderr: {result.stderr}")
        logging.debug(f"Command: {' '.join(str(c) for c in cmd)}")
        raise RuntimeError(f"{description} failed")
    return result


def get_raster_info(tif_path: Path) -> Tuple[Tuple[float, float, float, float], float]:
    """Get bounds (xmin, ymin, xmax, ymax) and nodata value from a raster."""
    result = run_cmd(["gdalinfo", "-json", tif_path], f"gdalinfo {tif_path}")
    info = json.loads(result.stdout)
    corners = info["cornerCoordinates"]
    bounds = (corners["upperLeft"][0], corners["lowerRight"][1], corners["lowerRight"][0], corners["upperLeft"][1])
    nodata = info["bands"][0].get("noDataValue", -9999.0)
    return bounds, nodata


def query_bridge_index(bridge_index_path: str, bounds: Tuple[float, float, float, float]) -> List[str]:
    """Query bridge index parquet for bridges intersecting the given bounds using OGR."""
    xmin, ymin, xmax, ymax = bounds

    # OGR flattens parquet structs: geometry_bbox.xmin, geometry_bbox.xmax, etc.
    sql = (
        f"SELECT location FROM bridge_index WHERE "
        f'"geometry_bbox.xmin" <= {xmax} AND "geometry_bbox.xmax" >= {xmin} AND '
        f'"geometry_bbox.ymin" <= {ymax} AND "geometry_bbox.ymax" >= {ymin}'
    )

    result = run_cmd(["ogr2ogr", "-f", "CSV", "/vsistdout/", bridge_index_path, "-sql", sql], "ogr2ogr bridge query")

    # Parse CSV output (first line is header "location", rest are paths)
    lines = result.stdout.strip().split("\n")
    return lines[1:] if len(lines) > 1 else []


def apply_bridge_mask(
    depth_path: Path,
    dem_path: Path,
    bridge_paths: List[str],
    output_path: Path,
    temp_dir: Path,
    conv_factor: float,
    depth_nodata: float,
) -> bool:
    """
    Process a single depth TIF with bridge masking.

    gdal_calc handles alignment automatically - uses first input's grid,
    resamples other inputs to match. No explicit gdalwarp needed.

    GDAL calls: 2-3 (gdalbuildvrt if multiple bridges, gdal_calc, gdal_translate)
    """
    try:
        # Merge bridges into single VRT (skip if only one)
        if len(bridge_paths) == 1:
            bridges_input = bridge_paths[0]
        else:
            bridges_vrt = temp_dir / "bridges.vrt"
            run_cmd(["gdalbuildvrt", bridges_vrt] + bridge_paths, "gdalbuildvrt")
            bridges_input = bridges_vrt

        # gdal_calc: A=depth (defines output grid), B=DEM, C=bridges
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
                dem_path,
                "-C",
                bridges_input,
                "--outfile",
                temp_output,
                f"--NoDataValue={depth_nodata}",
                "--co=COMPRESS=LZW",
                "--quiet",
                f"--calc={calc_expr}",
            ],
            "gdal_calc",
        )

        # Convert to COG
        output_path.parent.mkdir(parents=True, exist_ok=True)
        run_cmd(["gdal_translate", "-of", "COG", "-co", "COMPRESS=LZW", temp_output, output_path], "gdal_translate")

        return True

    except Exception as e:
        logging.error(f"Error processing {depth_path}: {e}", exc_info=True)
        return False


def process_bridges(collection: "CollectionData", print_progress: bool = False) -> Dict[str, any]:
    """
    Apply bridge masking to depth library TIFs.

    GDAL/OGR calls per TIF:
      - No bridges: 2 (gdalinfo, ogr2ogr) + file copy
      - With bridges: 5 (gdalinfo, ogr2ogr, gdalbuildvrt, gdal_calc, gdal_translate)
      - Single bridge: 4 (gdalinfo, ogr2ogr, gdal_calc, gdal_translate)
    """
    library_dir = Path(collection.library_dir)
    submodels_dir = Path(collection.submodels_dir)
    bridge_index_path = collection.bridge_tile_index_path
    conv_factor = collection.config["bridge_processing"]["BRIDGE_ELEV_CONV_FACTOR"]
    _log = lambda m: print_progress and sys.stdout.write(m)

    setup_gdal_environment(collection)

    # Rename library to temp, process into new library dir
    library_temp_dir = library_dir.parent / "library_temp"
    if library_temp_dir.exists():
        shutil.rmtree(library_temp_dir)
    _log(f"Renaming {library_dir} to {library_temp_dir}\n")
    shutil.move(str(library_dir), str(library_temp_dir))
    library_dir.mkdir(parents=True, exist_ok=True)

    depth_tifs = list(library_temp_dir.rglob("*.tif"))
    _log(f"Found {len(depth_tifs)} depth TIFs to process\n")

    success_count, fail_count = 0, 0
    files_with_bridges, files_without_bridges, files_modified = [], [], []

    for i, depth_path in enumerate(depth_tifs, 1):
        reach_id = depth_path.parent.parent.name
        dem_files = list((submodels_dir / reach_id / "Terrain").glob("*.seamless_3dep_dem_3m_5070.tif"))
        if not dem_files:
            raise FileNotFoundError(f"No DEM found for reach {reach_id}")

        # Get raster info and query bridges
        try:
            bounds, depth_nodata = get_raster_info(depth_path)
            bridge_paths = query_bridge_index(bridge_index_path, bounds)
        except Exception as e:
            logging.error(f"Error reading {depth_path}: {e}")
            fail_count += 1
            continue

        output_path = library_dir / depth_path.relative_to(library_temp_dir)

        if not bridge_paths:
            # No bridges - copy file (already COG)
            files_without_bridges.append(str(depth_path))
            output_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(depth_path, output_path)
            success_count += 1
        else:
            # Process with bridge masking
            files_with_bridges.append(str(depth_path))
            with tempfile.TemporaryDirectory(dir=str(library_dir.parent)) as temp_dir:
                if apply_bridge_mask(
                    depth_path, dem_files[0], bridge_paths, output_path, Path(temp_dir), conv_factor, depth_nodata
                ):
                    success_count += 1
                    files_modified.append(str(depth_path))
                else:
                    fail_count += 1

        _log(f"\rProcessing: {i}/{len(depth_tifs)} (success: {success_count}, failed: {fail_count})")
        print_progress and sys.stdout.flush()

    _log(f"\nBridge processing complete: {success_count} succeeded, {fail_count} failed\n")

    if fail_count == 0:
        shutil.rmtree(library_temp_dir)
    else:
        logging.warning(f"Keeping {library_temp_dir} due to {fail_count} failures")

    return {
        "total": len(depth_tifs),
        "success": success_count,
        "failed": fail_count,
        "with_bridges": files_with_bridges,
        "without_bridges": files_without_bridges,
        "modified": files_modified,
    }
