import logging
import os


def create_folders(root_dir) -> tuple:
    """
    Creates three folders inside the specified root directory
    """
    source_models_dir = os.path.join(root_dir, "source_models")
    submodels_dir = os.path.join(root_dir, "submodels")
    library_dir = os.path.join(root_dir, "library")

    os.makedirs(source_models_dir, exist_ok=True)
    os.makedirs(submodels_dir, exist_ok=True)
    os.makedirs(library_dir, exist_ok=True)

    logging.info(f"Folders created successfully inside {root_dir}")
    return source_models_dir, submodels_dir, library_dir


if __name__ == "__main__":
    root_dir = "data"  # Update this as necessary
    create_folders(root_dir)
