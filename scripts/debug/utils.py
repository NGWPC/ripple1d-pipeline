import os
import sqlite3
import time
from typing import List, Tuple

import pandas as pd
import requests
from openpyxl import load_workbook

from ..config import RIPPLE1D_API_URL


def get_failed_jobs_df(failed_reaches: List[Tuple[int, str, str]]) -> pd.DataFrame:
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

    for reach_id, job_id, _ in failed_reaches:
        url = f"{RIPPLE1D_API_URL}/jobs/{job_id}?tb=true"
        try:
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                response_data = response.json()
                err = response_data.get("result", {}).get("err", "No error message")
                tb = response_data.get("result", {}).get("tb", "No traceback")
                results.append((reach_id, err, tb))
            else:
                results.append((reach_id, f"Failed to get job status. Status code: {response.status_code}", ""))
        except requests.RequestException as e:
            results.append((reach_id, f"Error: {str(e)}", ""))

    # Convert results to a pandas DataFrame for formatted output
    df = pd.DataFrame(results, columns=["reach_id", "err", "tb"])
    return df


def get_all_job_ids_for_process(db_path: str, process_name: str) -> List[Tuple[int, str]]:
    """
    Retrieves all job IDs for the specified process from the processing table.

    Args:
        db_path: Path to the SQLite database.
        process_name: The name of the process (e.g., "create_fim_lib").

    Returns:
        List of tuples containing reach_id and job_id for the specified process.
    """

    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()

        query = f"""
            SELECT reach_id, {process_name}_job_id
            FROM processing
            WHERE {process_name}_job_id IS NOT NULL
        """
        cursor.execute(query)
        job_ids = cursor.fetchall()
    finally:
        conn.close()
    return job_ids


def poll_and_update_job_status(db_path: str, process_name: str, poll_interval: int = 0.1):
    """
    Polls the API for the current status of each job for the given process and updates the status in the processing table.

    Args:
        db_path: Path to the SQLite database.
        api_url: The base URL of the API (e.g., 'http://localhost:5000').
        process_name: The name of the process (e.g., "create_fim_lib").
        poll_interval: The time interval (in seconds) between API polling calls.
    """
    # Step 1: Get all job IDs for the process
    job_ids = get_all_job_ids_for_process(db_path, process_name)

    # Step 2: Poll the API and update the database
    headers = {"Content-Type": "application/json"}

    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()

        for reach_id, job_id in job_ids:
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
                            UPDATE processing
                            SET {process_name}_status = ?
                            WHERE reach_id = ?;
                        """,
                            (job_status, reach_id),
                        )

                    else:
                        print(f"Failed to poll job {job_id} for reach {reach_id}. Status code: {response.status_code}")

                except requests.RequestException as e:
                    print(f"Error polling job {job_id} for reach {reach_id}: {e}")

            # Optionally sleep between requests to avoid overloading the API
            time.sleep(poll_interval)

        conn.commit()
    finally:
        conn.close()


def get_reach_status_by_process(
    db_path: str, process_name: str
) -> Tuple[List[Tuple[int, str, str]], List[Tuple[int, str, str]], List[Tuple[int, str, str]]]:
    """
    Retrieves successful, accepted, and failed reaches for a given process name
    by reading statuses from the processing table.

    Returns:
        successful: List of tuples (reach_id, job_id, status) for successful reaches.
        accepted: List of tuples (reach_id, job_id, status) for accepted reaches.
        failed: List of tuples (reach_id, job_id, status) for failed reaches.
    """
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()

        # Build the dynamic SQL query for retrieving status and job IDs
        successful_query = f"""
            SELECT reach_id, {process_name}_job_id, {process_name}_status
            FROM processing
            WHERE {process_name}_status = 'successful'
        """
        accepted_query = f"""
            SELECT reach_id, {process_name}_job_id, {process_name}_status
            FROM processing
            WHERE {process_name}_status = 'accepted'
        """
        failed_query = f"""
            SELECT reach_id, {process_name}_job_id, {process_name}_status
            FROM processing
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

    print(f"Data written to {file_path} in sheet {process_name}.")
