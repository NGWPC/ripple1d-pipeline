import logging
import multiprocessing
import os
import subprocess
import sys
import tempfile
from pathlib import Path

from ..config import GDAL_BINS_PATH, GDAL_SCRIPTS_PATH, OPTIMUM_PARALLEL_PROCESS_COUNT


def setup_gdal_environment():
    """
    Add GDAL binaries to the system PATH
    """

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


def process_tif(tif_path, gpkg_path, tmp_dir, dest_dir):
    tif_stem = Path(tif_path).stem
    tmp_tif = os.path.join(tmp_dir, f"tmp_{tif_stem}.tif")
    dest_tif = os.path.join(dest_dir, f"{tif_stem}.tif")

    # Step 1: gdal_calc to create the binary mask
    gdal_calc_cmd = [
        "gdal_calc",
        "-A",
        tif_path,
        "--outfile",
        tmp_tif,
        "--calc=1*(A>0) + 0*(A<=0)",
        "--type=Byte",
        "--hideNoData",
    ]

    # Execute gdal_calc with output redirection
    subprocess.run(gdal_calc_cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    # Step 2: gdalwarp to crop based on the geopackage
    gdalwarp_cmd = [
        "gdalwarp",
        "-overwrite",
        "-cutline",
        gpkg_path,
        "-cl",
        "XS_concave_hull",
        "-crop_to_cutline",
        "-dstnodata",
        "255.0",
        "-co",
        "COMPRESS=DEFLATE",
        tmp_tif,
        dest_tif,
    ]

    # Execute gdalwarp with output redirection
    subprocess.run(gdalwarp_cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    if os.path.exists(tmp_tif):  # clean up
        os.remove(tmp_tif)


def find_gpkg(tif_path, submodels_dir):
    submodel_name = Path(tif_path).parts[-3]
    gpkg_path = os.path.join(submodels_dir, submodel_name, f"{submodel_name}.gpkg")
    return gpkg_path if os.path.exists(gpkg_path) else None


def worker(args):
    tif_path, library_dir, library_extent_dir, submodels_dir = args
    try:
        gpkg_path = find_gpkg(tif_path, submodels_dir)

        if gpkg_path:
            dest_dir = Path(tif_path.replace(library_dir, library_extent_dir)).parent
            if not os.path.exists(dest_dir):
                os.makedirs(dest_dir)

            with tempfile.TemporaryDirectory() as tmp_dir:
                process_tif(tif_path, gpkg_path, tmp_dir, dest_dir)
        else:
            logging.info(f"No corresponding geopackage found for {tif_path}", file=sys.stderr)
    except Exception as e:
        logging.info(f"Error processing {tif_path}: {str(e)}", file=sys.stderr)


def get_all_tif_paths(src_dir):
    tif_paths = []
    for root, _, files in os.walk(src_dir):
        for file in files:
            if file.endswith(".tif"):
                tif_paths.append(os.path.join(root, file))
    return tif_paths


def create_extent_lib(library_dir, library_extent_dir, submodels_dir, print_progress=False):
    setup_gdal_environment()
    create_mirrored_structure(library_dir, library_extent_dir)

    tif_paths = get_all_tif_paths(library_dir)

    total_files = len(tif_paths)
    processed_files = 0

    def update_progress():
        progress = int((processed_files / total_files) * 100)
        sys.stdout.write(f"\rProgress: [{ '#' * progress + '-' * (100 - progress)}] {progress}%")
        sys.stdout.flush()

    with multiprocessing.Pool(OPTIMUM_PARALLEL_PROCESS_COUNT) as pool:
        for _ in pool.imap_unordered(
            worker, [(path, library_dir, library_extent_dir, submodels_dir) for path in tif_paths]
        ):
            processed_files += 1
            if print_progress:
                update_progress()
    if print_progress:
        sys.stdout.write("\n")


if __name__ == "__main__":
    LIBRARY_DIR = r"D:\collections\ble_08020203_LowerStFrancis\library"
    SUBMODELS_DIR = r"D:\collections\ble_08020203_LowerStFrancis\submodels"
    LIBRARY_EXTENT_DIR = r"D:\collections\ble_08020203_LowerStFrancis\library_extent"

    create_extent_lib(LIBRARY_DIR, LIBRARY_EXTENT_DIR, SUBMODELS_DIR)
