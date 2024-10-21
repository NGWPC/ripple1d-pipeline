import yaml
import os
from pathlib import Path

class CollectionData:
    """
    Load configuration file, assign filepaths, create folders within the collection's root directory.
    """
    def __init__(self, stac_collection_id, config_file='config.yaml'):
        self.stac_collection_id = stac_collection_id
        self.load_config(config_file)
        self.assign_paths()
        

    def load_config(self, config_file):
        try:
            with open(str(Path.cwd().parent / config_file), 'r') as file:
                self.config = yaml.safe_load(file)
        except FileNotFoundError:
            raise ValueError(f"File '{config_file}' not found. Ensure config.yaml is in parent dir (../scripts).")
        except yaml.YAMLError:
            raise ValueError("Invalid YAML configuration")


    def assign_paths(self):
        """ Assign filepaths to CollectionData object."""
        self.root_dir = os.path.join(self.config['paths']['COLLECTIONS_ROOT_DIR'], str(self.stac_collection_id))
        self.db_path = os.path.join(self.root_dir, "ripple.gpkg")
        self.merged_gpkg_path = os.path.join(self.root_dir, "source_models", "all_rivers.gpkg")
        self.source_models_dir = os.path.join(self.root_dir, "source_models")
        self.submodels_dir = os.path.join(self.root_dir, "submodels")
        self.library_dir = os.path.join(self.root_dir, "library")
        self.f2f_start_file = os.path.join(self.root_dir, "start_reaches.csv")

    def create_folders(self):
        """ Create folders for source models, submodels, and library."""

        os.makedirs(self.source_models_dir, exist_ok=True)
        os.makedirs(self.submodels_dir, exist_ok=True)
        os.makedirs(self.library_dir, exist_ok=True)

        print(f"Folders created successfully inside {self.root_dir}")

if __name__ == "__main__":
    RV = CollectionData("test")
    reveal_type(RV)