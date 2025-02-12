import logging
import os
from typing import List, Type

import boto3
import pystac_client
from dotenv import load_dotenv

from .collection_data import CollectionData


class STACImporter:
    """
    Methods to identify models from a STAC Collection, and download each model's gpkg files from AWS S3.
    """

    def __init__(self, collectiondata: Type[CollectionData]):
        self.db_path = collectiondata.db_path
        self.stac_collection = collectiondata.stac_collection_id
        self.stac_endpoint = collectiondata.STAC_URL
        self.source_models_dir = collectiondata.source_models_dir
        self.models_data = None
        self.model_ids = None
        self.get_aws_profile()

    def get_aws_profile(self):
        load_dotenv(".env", override=True)
        self.AWS_PROFILE = os.getenv("AWS_PROFILE")

    def get_models_from_stac(self) -> None:
        # TODO add filter
        """
        Retrieves GeoPackage file and conflation file paths for models in an STAC collection.
        Parameters:
        - self.stac_endpoint (str): The STAC API endpoint.
        - self.stac_collection (str): The name of the STAC collection.
        """
        client = pystac_client.Client.open(self.stac_endpoint)
        collection = client.get_collection(self.stac_collection)
        i = 0
        models_data = {}
        for item in collection.get_items():
            i += 1
            if item.properties["has_2d"]:
                logging.info(f"{item.id} skipping because it has 2d elements")
                continue
            gpkg_key = ""
            for _, asset in item.assets.items():
                if "ras-geometry-gpkg" in asset.roles:
                    gpkg_key = asset.href
                    break
            if gpkg_key:
                models_data[item.id] = {"gpkg": gpkg_key, "model_name": item.properties["model_name"]}

        logging.info(f"Total {i} models in this collection")
        logging.info(f"Total {len(models_data)} filtered models.")

        self.models_data = models_data

    def download_models_data(self) -> None:
        """
        Downloads GeoPackage for models to a local folder.

        Parameters:
        - self.models_data (dict): Dictionary containing model IDs and their file URLs.
        - self.source_models_dir (str): The local directory to store the downloaded models.
        """
        session = boto3.Session(profile_name=self.AWS_PROFILE)
        s3_client = session.client("s3")

        for id, data in self.models_data.items():
            try:
                model_dir = os.path.join(self.source_models_dir, id)
                os.makedirs(model_dir, exist_ok=True)

                # Download GeoPackage
                # local_gpkg_path = os.path.join(model_dir, f"{data["model_name"]}.gpkg") # 0.7.0
                local_gpkg_path = os.path.join(model_dir, f"{id}.gpkg")

                gpkg_url = data["gpkg"]
                bucket_name, key = gpkg_url.replace("s3://", "").split("/", 1)
                s3_client.download_file(bucket_name, key, local_gpkg_path)

                logging.info(f"Successfully downloaded files for {id}")
            except Exception as e:
                logging.info(f"Failed to download files for {id}: {e}")

    def get_model_ids(self) -> List[str]:
        self.model_ids = list(self.models_data.keys())
        return self.model_ids
