"""
Move all fims from the submodel folders to the FIM library location
"""

import os
import shutil


def move_fims_to_library(submodels_dir, library_dir):
    for submodel in os.listdir(submodels_dir):
        dirs = os.listdir(f"{submodels_dir}/{submodel}")
        if "fims" in dirs:
            source_path = os.path.join(submodels_dir, submodel, "fims")
            destination_path = os.path.join(library_dir, submodel)

            if os.path.exists(source_path):
                if not os.path.exists(destination_path):
                    shutil.copytree(source_path, destination_path)
                shutil.rmtree(source_path)

    print("All fims have been copied to the library directory.")


if __name__ == "__main__":
    submodels_dir = r"D:\Users\abdul.siddiqui\workbench\projects\test_production\submodels"
    library_dir = r"D:\Users\abdul.siddiqui\workbench\projects\test_production\library"

    move_fims_to_library(submodels_dir, library_dir)
