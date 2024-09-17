import os
import sqlite3
from typing import Dict

import geopandas as gpd
import pandas as pd


def load_river_table_from_gpkg(gpkg_path: str) -> gpd.GeoDataFrame:
    """
    Load the River table from a given GPKG file.

    Args:
        gpkg_path (str): Path to the GeoPackage file.

    Returns:
        GeoDataFrame: The loaded River table.
    """
    try:
        gdf = gpd.read_file(gpkg_path, columns=[], layer="River")
        return gdf
    except Exception as e:
        print(f"Error loading River table from {gpkg_path}: {e}")
        return None


def combine_river_tables(source_models_dir: str, models_data: Dict, output_gpkg_path: str) -> None:
    """
    Combine River tables from multiple GPKG files, add model_id field, and save to a new GPKG file.
    Ensures the final output is in CRS EPSG:5070.

    Args:
        source_models_dir (str): Directory containing the model folders with GPKG files.
        models_data (Dict):
        output_gpkg_path (str): Path to the output combined GPKG file.
    """
    river_gdfs = []
    target_crs = "EPSG:5070"  # Desired CRS for the combined output

    for model_id, model_data in models_data.items():
        gpkg_path = os.path.join(source_models_dir, model_id, f"{model_data["model_name"]}.gpkg")

        if not os.path.exists(gpkg_path):
            print(f"GPKG file not found: {gpkg_path}")
            continue

        river_gdf = load_river_table_from_gpkg(gpkg_path)

        if river_gdf is not None:
            if river_gdf.crs is None or river_gdf.crs.to_string() != target_crs:
                river_gdf = river_gdf.to_crs(target_crs)

            # Add the model_id field
            river_gdf["model_id"] = model_id

            # Collect the GeoDataFrame
            river_gdfs.append(river_gdf)

    if river_gdfs:
        # concatenate all GeoDataFrames at once
        combined_gdf = gpd.GeoDataFrame(pd.concat(river_gdfs, ignore_index=True))

        try:
            combined_gdf.to_file(output_gpkg_path, driver="GPKG", layer="River")
            print(f"Combined River tables saved to {output_gpkg_path}")
        except Exception as e:
            print(f"Error saving combined River table: {e}")
    else:
        print("No River tables were combined.")


if __name__ == "__main__":
    source_models_dir = "data/source_models"  # path to the directory containing model folders
    model_ids = ["Model_A", "Model_B", "Model_C"]  # list of model keys to combine
    output_gpkg_path = "output/combined_river.gpkg"  # output GPKG file path

    combine_river_tables(source_models_dir, model_ids, output_gpkg_path)
