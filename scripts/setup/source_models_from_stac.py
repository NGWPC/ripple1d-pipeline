import os
import urllib.request

import boto3
import pystac_client

from ..config import AWS_PROFILE


def get_models_from_stac(stac_endpoint, stac_collection):
    # to do add filter
    """
    Retrieves GeoPackage file for models in an STAC collection.
    Parameters:
    - stac_endpoint (str): The STAC API endpoint.
    - stac_collection (str): The name of the STAC collection.
    Returns:
    - models_data (dict): Dictionary containing model IDs and their file URLs.
    """
    client = pystac_client.Client.open(stac_endpoint)
    collection = client.get_collection(stac_collection)
    i = 0
    models_data = {}
    for item in collection.get_items():
        i += 1
        gpkg_key = ""
        for _, asset in item.assets.items():
            if "ras-geometry-gpkg" in asset.roles:
                gpkg_key = asset.href
                break
        if gpkg_key:
            models_data[item.id] = {"gpkg": gpkg_key, "model_name": item.properties["model_name"]}

    print(f"Total {i} models in this collection")
    print(f"Total {len(models_data)} filtered models.")
    return models_data


def download_models_data(models_data, source_models_dir):
    """
    Downloads GeoPackage for models to a local folder.

    Parameters:
    - models_data (dict): Dictionary containing model IDs and their file URLs.
    - source_models_dir (str): The local directory to store the downloaded models.
    """
    session = boto3.Session(profile_name=AWS_PROFILE)
    s3_client = session.client("s3")

    for id, data in models_data.items():
        try:
            model_dir = os.path.join(source_models_dir, id)
            os.makedirs(model_dir, exist_ok=True)

            # Download GeoPackage
            local_gpkg_path = os.path.join(model_dir, f"{data["model_name"]}.gpkg")
            gpkg_url = data["gpkg"]
            bucket_name, key = gpkg_url.replace("s3://", "").split("/", 1)
            s3_client.download_file(bucket_name, key, local_gpkg_path)

            print(f"Successfully downloaded files for {id}")
        except Exception as e:
            print(f"Failed to download files for {id}: {e}")


if __name__ == "__main__":
    # Parameters
    stac_endpoint = "https://stac2.dewberryanalytics.com"  # STAC API endpoint
    stac_collection = "ripple_test_data"  # Collection name
    source_models_dir = "data/source_models"  # Local folder to store downloaded models
    # Step 1: Retrieve model data
    models_data = get_models_from_stac(stac_endpoint, stac_collection)

    # Step 2: Download model files
    download_models_data(models_data, source_models_dir)
