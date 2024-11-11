import logging
import sqlite3
import time
from typing import List, Tuple

import requests

from ..config import DB_CONN_TIMEOUT, DEFAULT_POLL_WAIT, RIPPLE1D_API_URL


def update_models_table(model_job_ids: List[Tuple[int, str]], process_name: str, job_status: str, db_path: str) -> None:
    """
    Updates the models table with job_id and job_status for a given process.
    """
    conn = sqlite3.connect(db_path, timeout=DB_CONN_TIMEOUT)
    try:
        cursor = conn.cursor()
        cursor.executemany(
            f"""
            UPDATE models
            SET {process_name}_job_id = ?, {process_name}_status = '{job_status}'
            WHERE model_id = ?;
            """,
            [(model_job_id[1], model_job_id[0]) for model_job_id in model_job_ids],
        )
        conn.commit()
    finally:
        conn.close()


def update_processing_table(
    reach_job_ids: List[Tuple[int, str]], process_name: str, job_status: str, db_path: str
) -> None:
    """
    Updates the processing table with job_id and job_status for a given process.
    """
    conn = sqlite3.connect(db_path, timeout=DB_CONN_TIMEOUT)
    try:
        cursor = conn.cursor()
        cursor.executemany(
            f"""
            UPDATE processing
            SET {process_name}_job_id = ?, {process_name}_status = '{job_status}'
            WHERE reach_id = ?;
            """,
            [(reach_job_id[1], reach_job_id[0]) for reach_job_id in reach_job_ids],
        )
        conn.commit()
    finally:
        conn.close()


def datetime_to_epoch_utc(datetime_str):
    from datetime import datetime, timezone

    # Parse the string into a naive datetime object
    dt_naive = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")

    # Make the datetime object timezone-aware (UTC)
    dt_utc = dt_naive.replace(tzinfo=timezone.utc)

    # Convert to epoch time
    epoch_time = int(dt_utc.timestamp())

    return epoch_time


def get_job_update_time(job_id: str) -> str:
    """
    Get updated time of a job as string
    """
    url = f"{RIPPLE1D_API_URL}/jobs/{job_id}"

    response = requests.get(url)
    response.raise_for_status()
    job_update_time = response.json().get("updated")
    return job_update_time


def get_job_status(job_id: str) -> str:
    """
    Get status of a job from API
    """
    url = f"{RIPPLE1D_API_URL}/jobs/{job_id}"

    response = requests.get(url)
    response.raise_for_status()
    job_status = response.json().get("status")
    return job_status


def check_job_successful(job_id: str, poll_wait: int = DEFAULT_POLL_WAIT, timeout_minutes=90):
    """
    Wait for a job to finish and return ture or false based on success or failure
    timeout_minutes count start from the job last updated status
    """
    while True:
        status = get_job_status(job_id)
        if status == "successful":
            return True
        elif status == "failed":
            logging.error(f"{RIPPLE1D_API_URL}/jobs/{job_id}?tb=true job failed")
            return False
        elif status == "running":
            elapsed_time = time.time() - datetime_to_epoch_utc(get_job_update_time(job_id))
            if elapsed_time / 60 > timeout_minutes:
                logging.warning(f"{RIPPLE1D_API_URL}/jobs/{job_id} client timeout")
                return False
        time.sleep(poll_wait)


def wait_for_jobs(
    reach_job_ids: List[Tuple[int, str]], poll_wait: int = DEFAULT_POLL_WAIT, timeout_minutes=90
) -> Tuple[List[Tuple[int, str]], List[Tuple[int, str]]]:
    """
    Waits for jobs to finish and returns lists of successful, failed, and unknown status jobsjobs.
    """
    succeeded = []
    failed = []
    unknown = []

    i = 0
    for i in range(len(reach_job_ids)):
        while True:
            status = get_job_status(reach_job_ids[i][1])
            if status == "successful":
                succeeded.append((reach_job_ids[i][0], reach_job_ids[i][1], "successful"))
                break
            elif status == "failed":
                failed.append((reach_job_ids[i][0], reach_job_ids[i][1], "failed"))
                logging.error(f"{RIPPLE1D_API_URL}/jobs/{reach_job_ids[i][1]}?tb=true job failed")
                break
            elif status == "running":
                elapsed_time = time.time() - datetime_to_epoch_utc(get_job_update_time(reach_job_ids[i][1]))
                if elapsed_time / 60 > timeout_minutes:
                    logging.warning(f"{RIPPLE1D_API_URL}/jobs/{reach_job_ids[i][1]} client timeout")
                    unknown.append((reach_job_ids[i][0], reach_job_ids[i][1], "unknown"))
                    break
            time.sleep(poll_wait)

    return succeeded, failed, unknown
