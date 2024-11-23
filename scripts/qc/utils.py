import logging
import os
import shutil
import sqlite3
import time
from typing import List, Tuple

import pandas as pd
import requests
from openpyxl import load_workbook

from ..config import DB_CONN_TIMEOUT, QC_TEMPLATE_QGIS_FILE, RIPPLE1D_API_URL


def get_failed_jobs_df(failed_ids: List[Tuple[int, str, str]]) -> pd.DataFrame:
    """
    Sends a GET request to the API for each failed reach's job and returns a formatted table
    with reach_id, error message (err), and traceback (tb).

    Args:
        api_url: The base URL of the API (e.g., 'http://localhost:5000').
        failed_reaches: List of tuples (reach_id, job_id, status) for failed reaches.

    Returns:
        A pandas DataFrame containing the reach_id, error (err), and traceback (tb).
    """
    headers = {"Content-Type": "application/json"}
    results = []

    for id, job_id, _ in failed_ids:
        url = f"{RIPPLE1D_API_URL}/jobs/{job_id}?tb=true"

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            response_data = response.json()
            if response_data and response_data["result"]:
                err = response_data["result"].get("err", "No error message")
                tb = response_data["result"].get("tb", "No traceback")
            else:
                err = "No error message"
                tb = "No traceback"
            results.append((id, err, tb))
        else:
            results.append((id, f"Failed to get job status. Status code: {response.status_code}", ""))

    # Convert results to a pandas DataFrame for formatted output
    df = pd.DataFrame(results, columns=["id", "err", "tb"])
    return df


def get_all_job_ids_for_process(
    db_path: str, process_name: str, process_table: str = "processing"
) -> List[Tuple[int, str]]:
    """
    Retrieves all job IDs for the specified process from the processing table.

    Args:
        db_path: Path to the SQLite database.
        process_name: The name of the process (e.g., "create_fim_lib").

    Returns:
        List of tuples containing reach_id and job_id for the specified process.
    """

    conn = sqlite3.connect(db_path, timeout=DB_CONN_TIMEOUT)
    try:
        cursor = conn.cursor()

        query = f"""
            SELECT {"reach_id" if process_table == "processing" else "model_id"}, {process_name}_job_id
            FROM {process_table}
            WHERE {process_name}_job_id IS NOT NULL
        """
        cursor.execute(query)
        job_ids = cursor.fetchall()
    finally:
        conn.close()
    return job_ids


def poll_and_update_job_status(
    db_path: str,
    process_name: str,
    process_table: str = "processing",
):
    """
    Polls the API for the current status of each job for the given process and updates the status in the processing table.

    Args:
        db_path: Path to the SQLite database.
        process_name: The name of the process (e.g., "create_fim_lib").
        poll_interval: The time interval (in seconds) between API polling calls.
    """
    # Step 1: Get all job IDs for the process
    job_ids = get_all_job_ids_for_process(db_path, process_name, process_table)

    # Step 2: Poll the API and update the database
    headers = {"Content-Type": "application/json"}

    conn = sqlite3.connect(db_path, timeout=DB_CONN_TIMEOUT)
    try:
        cursor = conn.cursor()

        for entity, job_id in job_ids:
            if job_id:  # Ensure job_id exists
                url = f"{RIPPLE1D_API_URL}/jobs/{job_id}"
                try:
                    response = requests.get(url, headers=headers)

                    if response.status_code == 200:
                        response_data = response.json()
                        job_status = response_data.get("status", "unknown")

                        # Step 3: Update the processing table with the new status
                        cursor.execute(
                            f"""
                            UPDATE {process_table}
                            SET {process_name}_status = ?
                            WHERE {"reach_id" if process_table == "processing" else "model_id"} = ?;
                        """,
                            (job_status, entity),
                        )

                    else:
                        logging.info(
                            f"Failed to poll job {job_id} for reach {entity}. Status code: {response.status_code}"
                        )

                except requests.RequestException as e:
                    logging.info(f"Error polling job {job_id} for reach {entity}: {e}")

        conn.commit()
    finally:
        conn.close()


def get_reach_status_by_process(
    db_path: str, process_name: str, process_table: str = "processing"
) -> Tuple[List[Tuple[int, str, str]], List[Tuple[int, str, str]], List[Tuple[int, str, str]]]:
    """
    Retrieves successful, accepted, and failed reaches for a given process name
    by reading statuses from the processing table.

    Returns:
        successful: List of tuples (reach_id, job_id, status) for successful reaches.
        failed: List of tuples (reach_id, job_id, status) for failed reaches.
        accepted: List of tuples (reach_id, job_id, status) for accepted reaches.
    """
    conn = sqlite3.connect(db_path, timeout=DB_CONN_TIMEOUT)
    try:
        cursor = conn.cursor()

        # Build the dynamic SQL query for retrieving status and job IDs
        successful_query = f"""
            SELECT {"reach_id" if process_table == "processing" else "model_id"}, {process_name}_job_id, {process_name}_status
            FROM {process_table}
            WHERE {process_name}_status = 'successful'
        """
        accepted_query = f"""
            SELECT {"reach_id" if process_table == "processing" else "model_id"}, {process_name}_job_id, {process_name}_status
            FROM {process_table}
            WHERE {process_name}_status = 'accepted'
        """
        failed_query = f"""
            SELECT {"reach_id" if process_table == "processing" else "model_id"}, {process_name}_job_id, {process_name}_status
            FROM {process_table}
            WHERE {process_name}_status = 'failed'
        """

        # Retrieve successful reaches
        cursor.execute(successful_query)
        successful = cursor.fetchall()

        # Retrieve accepted reaches
        cursor.execute(accepted_query)
        accepted = cursor.fetchall()

        # Retrieve failed reaches
        cursor.execute(failed_query)
        failed = cursor.fetchall()

    finally:
        conn.close()

    return successful, failed, accepted


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


def copy_qc_map(root_dir: str):
    """Copy QGIS Template file inside a 'qc' folder in root_dir"""
    dest_location = os.path.join(root_dir, "qc", "qc_map.qgs")
    os.makedirs(os.path.join(root_dir, "qc"), exist_ok=True)
    shutil.copyfile(QC_TEMPLATE_QGIS_FILE, dest_location)

    logging.info(f"QC map created at {dest_location}")
