import logging
import os
from pathlib import Path
from typing import List

import yaml
from dotenv import load_dotenv


class CollectionData:
    """
    Load configuration file, assign filepaths, create folders within the collection's root directory.
    """

    def __init__(self, stac_collection_id, config_file="config.yaml"):
        self.stac_collection_id = stac_collection_id
        self.load_dotenv(".env")
        self.load_yaml(config_file)
        self.assign_paths()

    # TODO - Assign ALL parameters from config.yaml to attributes of CollectionData Class?
    def load_yaml(self, config_file):
        try:
            with open(str(Path.cwd() / "src" / config_file), "r") as file:
                self.config = yaml.safe_load(file)
        except FileNotFoundError:
            raise ValueError(f"File '{config_file}' not found. Ensure config.yaml is in the src directory.")
        except yaml.YAMLError:
            raise ValueError("Invalid YAML configuration")

    def load_dotenv(self, dotenv_file):
        try:
            load_dotenv(dotenv_file, override=True)
            self.RIPPLE1D_API_URL = os.getenv("RIPPLE1D_API_URL")
            self.STAC_URL = os.getenv("STAC_URL")
        except:
            raise ValueError("Invalid .env configuration")

    def assign_paths(self):
        """Assign filepaths to CollectionData object."""
        self.root_dir = os.path.join(self.config["paths"]["COLLECTIONS_ROOT_DIR"], str(self.stac_collection_id))
        self.db_path = os.path.join(self.root_dir, "ripple.gpkg")
        self.source_models_dir = os.path.join(self.root_dir, "source_models")
        self.source_models_gpkg_path = os.path.join(self.root_dir, "source_models", "source_models.gpkg")
        self.submodels_dir = os.path.join(self.root_dir, "submodels")
        self.library_dir = os.path.join(self.root_dir, "library")
        self.extent_library_dir = os.path.join(self.root_dir, "library_extent")
        self.f2f_start_file = os.path.join(self.root_dir, "start_reaches.csv")
        self.error_report_path = os.path.join(self.root_dir, "error_report.xlsx")

    def create_folders(self):
        """Create folders for source models, submodels, and library."""

        os.makedirs(self.source_models_dir, exist_ok=True)
        os.makedirs(self.submodels_dir, exist_ok=True)
        os.makedirs(self.library_dir, exist_ok=True)

        logging.info(f"Folders created successfully inside {self.root_dir}")

    def get_models(self) -> List:
        models = []
        path = Path(self.source_models_dir)
        try:
            for model in os.listdir(path):
                model_path = os.path.join(path, model)
                if os.path.isdir(model_path):
                    # Add all models pulled from the STAC Catalog
                    models.append(model)
            return models

        except Exception as e:
            logging.error(f"An error occurred: {e}.")
            logging.error(f"No models are available.")
            return []
            return []
