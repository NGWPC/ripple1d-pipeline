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


def get_job_status(job_id: str) -> str:
    """
    Polls job status.
    """
    url = f"{RIPPLE1D_API_URL}/jobs/{job_id}"

    response = requests.get(url)
    response.raise_for_status()
    job_status = response.json().get("status")
    return job_status


def check_job_successful(job_id: str, poll_wait: int = DEFAULT_POLL_WAIT) -> bool:
    """
    Polls job status until it completes or fails.
    """
    url = f"{RIPPLE1D_API_URL}/jobs/{job_id}"

    while True:
        response = requests.get(url)
        response.raise_for_status()
        job_status = response.json().get("status")
        if job_status == "successful":
            return True
        elif job_status == "failed":
            print(f"Job {url}?tb=true failed.")
            return False
        time.sleep(poll_wait)


def wait_for_jobs(
    reach_job_ids: List[Tuple[int, str]], poll_wait: int = DEFAULT_POLL_WAIT, timeout_minutes=1500
) -> Tuple[List[Tuple[int, str]], List[Tuple[int, str]]]:
    """
    Waits for jobs to finish and returns lists of successful and failed jobs.
    """
    start_time = time.time()
    succeeded = []
    failed = []
    timedout = []

    i = 0
    while i < len(reach_job_ids):
        status = get_job_status(reach_job_ids[i][1])
        if status == "successful":
            succeeded.append((reach_job_ids[i][0], reach_job_ids[i][1], "successful"))
            i += 1
        elif status == "failed":
            failed.append((reach_job_ids[i][0], reach_job_ids[i][1], "failed"))
            print(f"{RIPPLE1D_API_URL}/jobs/{reach_job_ids[i][1]}?tb=true", "job failed")
            i += 1
        elif time.time() - start_time > timeout_minutes * 60:
            print(f"{RIPPLE1D_API_URL}/jobs/{reach_job_ids[i][1]}", "client timeout")
            timedout.append((reach_job_ids[i][0], reach_job_ids[i][1], "unknown"))
            i += 1

    return succeeded, failed
