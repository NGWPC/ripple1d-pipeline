import yaml
import os
from pathlib import Path
from typing import List
import logging

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
            with open(str(Path.cwd() / "scripts" / config_file), 'r') as file:
                self.config = yaml.safe_load(file)
        except FileNotFoundError:
            raise ValueError(f"File '{config_file}' not found. Ensure config.yaml is in the scripts directory.")
        except yaml.YAMLError:
            raise ValueError("Invalid YAML configuration")

    #TODO - Assign ALL parameters from config file to Class to remove the need to use dot or bracket notation for
    # accessing values later?

    def assign_paths(self):
        """ Assign filepaths to CollectionData object."""
        self.root_dir = os.path.join(self.config['paths']['COLLECTIONS_ROOT_DIR'], str(self.stac_collection_id))
        self.db_path = os.path.join(self.root_dir, "ripple.gpkg")
        self.source_models_dir = os.path.join(self.root_dir, "source_models")
        self.merged_gpkg_path = os.path.join(self.root_dir, "source_models", "all_rivers.gpkg")
        self.submodels_dir = os.path.join(self.root_dir, "submodels")
        self.library_dir = os.path.join(self.root_dir, "library")
        self.extent_library_dir = os.path.join(self.root_dir, "library_extent")
        self.f2f_start_file = os.path.join(self.root_dir, "start_reaches.csv")
        self.error_report_path = os.path.join(self.root_dir, "error_report.xlsx")

    def create_folders(self):
        """ Create folders for source models, submodels, and library."""

        os.makedirs(self.source_models_dir, exist_ok=True)
        os.makedirs(self.submodels_dir, exist_ok=True)
        os.makedirs(self.library_dir, exist_ok=True)

    def get_models(self) -> List:
        models = []
        path = Path(self.source_models_dir)
        try:
            # Walk through the directory tree
            for root, dirs, files in os.walk(path):
                model = root.split("/")[-1]
                # The root directory is expressed as "source_models" from line above, 
                #  which is not an actual model, and must be ommitted
                if model != "source_models":
                    # Add all models pulled from the STAC Catalog
                    models.append(model)
            return models

        except Exception as e:
            logging.error(f"An error occurred: {e}.")
            logging.error(f"No models are available.")
            return []
