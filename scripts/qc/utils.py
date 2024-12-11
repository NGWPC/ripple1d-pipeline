import logging
import os
import shutil
from typing import Type

import pandas as pd

from ..setup.collection_data import CollectionData


def write_failed_jobs_df_to_excel(df: pd.DataFrame, process_name: str, file_path: str) -> None:
    """
    Writes the DataFrame to an Excel file at the specified file path with the given process name as the sheet name.
    If the file or sheet already exists, it will add a new sheet or overwrite the existing one.

    Args:
        df: A pandas DataFrame containing the reach_id, error (err), and traceback (tb).
        process_name: The name of the process to be used as the sheet name in the Excel file.
        file_path: The path to the Excel file where the data will be saved.

    Returns:
        None
    """

    if os.path.exists(file_path):
        # Load the existing workbook
        with pd.ExcelWriter(file_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
            # Write the DataFrame to the specified sheet
            df.to_excel(writer, sheet_name=process_name, index=False)
    else:
        # Create a new Excel file and write the DataFrame to the specified sheet
        with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name=process_name, index=False)

    logging.info(f"Data written to {file_path} in sheet {process_name}.")


def copy_qc_map(collection: Type[CollectionData]):
    """Copy QGIS Template file inside a 'qc' folder in root_dir"""
    dest_location = os.path.join(collection.root_dir, "qc", "qc_map.qgs")
    os.makedirs(os.path.join(collection.root_dir, "qc"), exist_ok=True)
    shutil.copyfile(collection.config["qc"]["QC_TEMPLATE_QGIS_FILE"], dest_location)

    logging.info(f"QC map created at {dest_location}")
