"""This is work in progress"""

import multiprocessing
import os
import subprocess
from pathlib import Path

LIBRARY_DIR = "library"
SUBMODELS_DIR = "submodels"
LIBRARY_EXTENT_DIR = "library_extent"


# Create the mirror folder structure
def create_mirrored_structure(src_dir, dest_dir):
    for root, dirs, files in os.walk(src_dir):
        for folder in dirs:
            src_path = os.path.join(root, folder)
            dest_path = src_path.replace(src_dir, dest_dir)
            if not os.path.exists(dest_path):
                os.makedirs(dest_path)


# Process each tif file
def process_tif(tif_path, gpkg_path, tmp_dir, dest_dir):
    tif_stem = Path(tif_path).stem
    tmp_tif = os.path.join(tmp_dir, f"tmp_{tif_stem}.tif")
    dest_tif = os.path.join(dest_dir, f"{tif_stem}.tif")

    # Step 1: gdal_calc to create the binary mask
    gdal_calc_cmd = [
        "gdal_calc.py",
        "-A",
        tif_path,
        "--outfile",
        tmp_tif,
        "--calc=1*(A>0) + 0*(A<=0)",
        "--type=Byte",
        "--hideNoData",
    ]

    # execute gdal_calc
    subprocess.run(gdal_calc_cmd, check=True)

    # step 2: gdalwarp to crop based on the geopackage
    gdalwarp_cmd = [
        "gdalwarp",
        "-overwrite",
        "-of",
        "COG",
        "-cutline",
        gpkg_path,
        "-cl",
        "XS_concave_hull",
        "-crop_to_cutline",
        "-dstnodata",
        "255.0",
        tmp_tif,
        dest_tif,
    ]

    subprocess.run(gdalwarp_cmd, check=True)

    if os.path.exists(tmp_tif):  # clean up
        os.remove(tmp_tif)


def find_gpkg(tif_path):
    folder_name = Path(tif_path).parts[1]
    gpkg_path = os.path.join(SUBMODELS_DIR, folder_name, f"{folder_name}.gpkg")
    return gpkg_path if os.path.exists(gpkg_path) else None


def worker(tif_path):
    try:
        gpkg_path = find_gpkg(tif_path)  # Find the corresponding geopackage

        if gpkg_path:
            dest_dir = Path(tif_path.replace(LIBRARY_DIR, LIBRARY_EXTENT_DIR)).parent
            if not os.path.exists(dest_dir):
                os.makedirs(dest_dir)

            tmp_dir = Path(dest_dir).parent / "tmp"
            if not os.path.exists(tmp_dir):
                os.makedirs(tmp_dir)

            process_tif(tif_path, gpkg_path, tmp_dir, dest_dir)
        else:
            print(f"No corresponding geopackage found for {tif_path}")
    except Exception as e:
        print(f"Error processing {tif_path}: {str(e)}")


def get_all_tif_paths(src_dir):
    tif_paths = []
    for root, _, files in os.walk(src_dir):
        for file in files:
            if file.endswith(".tif"):
                tif_paths.append(os.path.join(root, file))
    return tif_paths


def main():
    create_mirrored_structure(LIBRARY_DIR, LIBRARY_EXTENT_DIR)

    tif_paths = get_all_tif_paths(LIBRARY_DIR)

    with multiprocessing.Pool() as pool:
        pool.map(worker, tif_paths)


if __name__ == "__main__":
    main()
