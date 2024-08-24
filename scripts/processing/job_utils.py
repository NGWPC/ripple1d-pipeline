import sqlite3
import time

import requests


def update_processing_table(reach_job_ids, process_name, job_status, db_path):
    """Update the processing table with job_id and job_status."""

    conn = sqlite3.connect(db_path)
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
    conn.close()


def check_job_status(job_id: str, poll_wait=3) -> bool:
    """Poll job status until it completes or fails."""
    url = f"http://localhost/jobs/{job_id}"

    while True:
        response = requests.get(url)
        response.raise_for_status()
        job_status = response.json().get("status")
        if job_status == "successful":
            return True
        elif job_status == "failed":
            print(f"Job {url}?tb=true failed.")
            return False
        time.sleep(poll_wait)  # Wait for a few seconds before checking again


def wait_for_jobs(reach_job_ids, poll_wait=3):
    succeeded = []
    failed = []
    for reach_job_id in reach_job_ids:
        # although this will not give immidiate answers
        # but there will be eventual consistency and the load on API will be low
        if check_job_status(reach_job_id[1], poll_wait):
            succeeded.append(reach_job_id)
        else:
            failed.append(reach_job_id)

    return succeeded, failed
