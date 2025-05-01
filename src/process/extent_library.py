"""Create Extent library from Depth library"""

import logging
import multiprocessing
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Type

from ..setup.collection_data import CollectionData


def setup_gdal_environment(collection):
    """
    Add GDAL binaries to the system PATH
    """
    GDAL_BINS_PATH = collection.config["flows2fim"]["GDAL_BINS_PATH"]
    GDAL_SCRIPTS_PATH = collection.config["flows2fim"]["GDAL_SCRIPTS_PATH"]

    if GDAL_BINS_PATH:
        # Add GDAL path to the system PATH
        os.environ["PATH"] = GDAL_BINS_PATH + os.pathsep + os.environ["PATH"]

    if GDAL_SCRIPTS_PATH:
        os.environ["PATH"] = GDAL_SCRIPTS_PATH + os.pathsep + os.environ["PATH"]


def create_mirrored_structure(src_dir, dest_dir):
    for root, dirs, files in os.walk(src_dir):
        for folder in dirs:
            src_path = os.path.join(root, folder)
            dest_path = src_path.replace(src_dir, dest_dir)
            if not os.path.exists(dest_path):
                os.makedirs(dest_path)


def create_extent_tif(tif_path, tmp_dir, dest_dir) -> None:
    tif_stem = Path(tif_path).stem
    tmp_tif = os.path.join(tmp_dir, f"tmp_{tif_stem}.tif")
    dest_tif = os.path.join(dest_dir, f"{tif_stem}.tif")

    if os.path.exists(dest_tif):
        logging.debug(f"Destination file {dest_tif} already exists. Skipping processing.")
        return

    gdal_calc_cmd = [
        "gdal_calc",
        "-A",
        tif_path,
        "--outfile",
        tmp_tif,
        '--calc="1*A"',
        "--type=Byte",
    ]

    result = subprocess.run(gdal_calc_cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logging.debug(f"gdal_calc stdout: {result.stdout}")
        logging.error(f"gdal_calc stderr: {result.stderr}")
        logging.debug(" ".join(gdal_calc_cmd))
        raise RuntimeError(f"gdal_calc failed for {tif_path}")

    if not os.path.exists(tmp_tif):
        logging.error(f"Temporary file {tmp_tif} not created!")
        raise FileNotFoundError(f"{tmp_tif} not created")

    # Translate to COG, COG format can't be created with gdal_calc
    gdal_translate_cmd = [
        "gdal_translate",
        "-of",
        "COG",
        tmp_tif,
        dest_tif,
    ]

    # Execute gdalwarp with output redirection
    result = subprocess.run(gdal_translate_cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logging.debug(f"gdal_translate stdout: {result.stdout}")
        logging.error(f"gdal_translate stderr: {result.stderr}")
        logging.debug(" ".join(gdal_calc_cmd))
        logging.debug(" ".join(gdal_translate_cmd))

        if os.path.exists(dest_tif):  # clean up
            os.remove(dest_tif)
        raise RuntimeError(f"gdal_translate failed for {tif_path}")


def create_domain_tif(tif_path, tmp_dir, gpkg_path, dest_dir) -> None:
    tmp_tif = os.path.join(tmp_dir, "tmp_domain.tif")
    dest_tif = os.path.join(dest_dir, "domain.tif")

    if os.path.exists(dest_tif):
        logging.debug(f"Destination file {dest_tif} already exists. Skipping processing.")
        return

    # create a temporary raster with same extents as all other to burn xs_concave_hull
    gdal_calc_cmd = [
        "gdal_calc",
        "-A",
        tif_path,
        "--outfile",
        tmp_tif,
        '--calc="0*A"',
        "--type=Byte",
    ]

    result = subprocess.run(gdal_calc_cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logging.debug(f"gdal_calc stdout: {result.stdout}")
        logging.error(f"gdal_calc stderr: {result.stderr}")
        logging.debug(" ".join(gdal_calc_cmd))
        raise RuntimeError(f"gdal_calc failed for domain from {tif_path}")

    if not os.path.exists(tmp_tif):
        logging.error(f"Temporary file {tmp_tif} not created!")
        raise FileNotFoundError(f"{tmp_tif} not created")

    # burn xs_concave_hull
    gdal_rasterize_cmd = ["gdal_rasterize", "-l", "XS_concave_hull", "-burn", 0, gpkg_path, tmp_tif]

    result = subprocess.run(gdal_rasterize_cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logging.debug(f"gdal_rasterize stdout: {result.stdout}")
        logging.error(f"gdal_rasterize stderr: {result.stderr}")
        logging.debug(" ".join(gdal_rasterize_cmd))
        raise RuntimeError(f"gdal_rasterize failed for domain {tmp_tif}")

    # Translate to COG, COG format can't be created with gdal_calc, or burn value into
    gdal_translate_cmd = [
        "gdal_translate",
        "-of",
        "COG",
        tmp_tif,
        dest_tif,
    ]

    result = subprocess.run(gdal_translate_cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logging.debug(f"gdal_translate stdout: {result.stdout}")
        logging.error(f"gdal_translate stderr: {result.stderr}")
        logging.debug(" ".join(gdal_calc_cmd))
        logging.debug(" ".join(gdal_rasterize_cmd))
        logging.debug(" ".join(gdal_translate_cmd))

        if os.path.exists(dest_tif):  # clean up
            os.remove(dest_tif)
        raise RuntimeError(f"gdal_translate failed for {dest_tif}")


def fim_worker(args):
    tif_path, library_dir, library_extent_dir = args
    try:
        dest_dir = Path(tif_path.replace(library_dir, library_extent_dir)).parent
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir)

        with tempfile.TemporaryDirectory() as tmp_dir:
            create_extent_tif(tif_path, tmp_dir, dest_dir)
    except Exception as e:
        logging.error(f"Error processing {tif_path}: {str(e)}")


def domain_worker(args):
    reach_id, tif_path, library_extent_dir, submodels_dir = args
    try:

        gpkg_path = os.path.join(submodels_dir, reach_id, f"{reach_id}.gpkg")

        if os.path.exists(gpkg_path):
            dest_dir = os.path.join(library_extent_dir, reach_id)
            if not os.path.exists(dest_dir):
                os.makedirs(dest_dir)

            with tempfile.TemporaryDirectory() as tmp_dir:
                create_domain_tif(tif_path, tmp_dir, gpkg_path, dest_dir)
        else:
            logging.error(f"No corresponding geopackage found for {tif_path}")

    except Exception as e:
        logging.error(f"Error processing domain: {str(e)}")


def get_all_tif_paths(src_dir):
    tif_paths = []
    for root, _, files in os.walk(src_dir):
        for file in files:
            if file.endswith(".tif"):
                tif_paths.append(os.path.join(root, file))
    return tif_paths


def get_reachid_tif_map(tif_paths):
    results = {}
    for tif_path in tif_paths:
        # Extract the reach ID from the file name
        reach_id = Path(tif_path).parents[1].name
        if reach_id not in results:
            results[reach_id] = tif_path
    return results


def create_extent_lib(collection: Type[CollectionData], print_progress=False):
    # Assign local variables from CollectionData Object
    library_dir = collection.library_dir
    extent_library_dir = collection.extent_library_dir
    submodels_dir = collection.submodels_dir
    OPTIMUM_PARALLEL_PROCESS_COUNT = collection.config["execution"]["OPTIMUM_PARALLEL_PROCESS_COUNT"]

    setup_gdal_environment(collection)
    create_mirrored_structure(library_dir, extent_library_dir)

    def update_progress():
        progress = int((processed_files / total_files) * 100)
        sys.stdout.write(f"\rProgress: [{ '#' * progress + '-' * (100 - progress)}] {progress}%")
        sys.stdout.flush()

    # create fims
    tif_paths = get_all_tif_paths(library_dir)
    total_files = len(tif_paths)
    processed_files = 0

    with multiprocessing.Pool(OPTIMUM_PARALLEL_PROCESS_COUNT) as pool:
        for _ in pool.imap_unordered(fim_worker, [(path, library_dir, extent_library_dir) for path in tif_paths]):
            processed_files += 1
            if print_progress:
                update_progress()
    if print_progress:
        sys.stdout.write("\n")

    # create domains
    reachid_tif_map = get_reachid_tif_map(tif_paths)
    total_files = len(reachid_tif_map)
    processed_files = 0

    with multiprocessing.Pool(OPTIMUM_PARALLEL_PROCESS_COUNT) as pool:
        for _ in pool.imap_unordered(
            domain_worker,
            [(reach_id, tif_path, extent_library_dir, submodels_dir) for reach_id, tif_path in reachid_tif_map.items()],
        ):
            processed_files += 1
            if print_progress:
                update_progress()
    if print_progress:
        sys.stdout.write("\n")
