import concurrent.futures
import json
import random
import sqlite3
from time import sleep
from typing import Tuple

import requests

from ..config import (
    API_LAUNCH_JOBS_RETRY_WAIT,
    PAYLOAD_TEMPLATES,
    RIPPLE1D_API_URL,
    RIPPLE1D_THREAD_COUNT,
)
from .ikwse_step import get_min_max_elevation
from .job_utils import update_processing_table, wait_for_jobs


def format_payload(template: dict, nwm_reach_id: int, submodels_dir: str, min_elev: float, max_elev: float) -> dict:
    """
    Formats a payload based on a given template and parameters.
    """
    payload = {}
    for key, value in template.items():
        if isinstance(value, str):
            payload[key] = value.format(
                nwm_reach_id=nwm_reach_id,
                submodels_directory=submodels_dir,
            )
        else:
            payload[key] = value

    payload["min_elevation"] = min_elev
    payload["max_elevation"] = max_elev
    return payload


def execute_request(nwm_reach_id: int, submodels_dir: str, downstream_id: int) -> Tuple[int, str, str]:
    """
    Executes an API request for a given process and returns the job ID and status.
    Retries upto 5 times
    """

    if downstream_id:
        min_elevation, max_elevation = get_min_max_elevation(downstream_id, submodels_dir, None, False, "")
        if min_elevation and max_elevation:

            for i in range(5):
                url = f"{RIPPLE1D_API_URL}/processes/run_known_wse/execution"
                payload = json.dumps(
                    format_payload(
                        PAYLOAD_TEMPLATES["run_known_wse"], nwm_reach_id, submodels_dir, min_elevation, max_elevation
                    )
                )
                headers = {"Content-Type": "application/json"}

                response = requests.post(url, headers=headers, data=payload)
                if response.status_code == 201:
                    job_id = response.json().get("jobID")
                    return nwm_reach_id, job_id, "accepted"
                elif response.status_code == 500:
                    print(f"Retrying. {nwm_reach_id}")
                else:
                    break
                sleep(i * API_LAUNCH_JOBS_RETRY_WAIT)
            print(f"Failed to accept {nwm_reach_id}, code: {response.status_code}, response: {response.text}")

    return nwm_reach_id, "", "not_accepted"


def execute_kwse_step(
    reach_data: list,
    db_path: str,
    submodels_dir: str,
    timeout_minutes: int = 60,
) -> Tuple[list, list, list]:
    """
    Executes a processing step concerning submodels/reach
    1. Request job for each id through API
    2. Wait for jobs to finish
    3. Update models table with final job status
    4. Return succeeded, failed, not_accepted, unknown status jobs
    """
    reach_job_id_statuses = []

    for reach_id, ds_id in reach_data:
        reach_job_id_status = execute_request(reach_id, submodels_dir, ds_id)
        reach_job_id_statuses.append(reach_job_id_status)

    accepted = [job for job in reach_job_id_statuses if job[2] == "accepted"]
    not_accepted = [job for job in reach_job_id_statuses if job[2] == "not_accepted"]

    update_processing_table(accepted, "run_known_wse", "accepted", db_path)
    print("Jobs submission complete. Waiting for jobs to finish...")

    succeeded, failed, unknown = wait_for_jobs(accepted, timeout_minutes=timeout_minutes)
    update_processing_table(succeeded, "run_known_wse", "successful", db_path)
    update_processing_table(failed, "run_known_wse", "failed", db_path)
    update_processing_table(unknown, "run_known_wse", "unknown", db_path)

    print(f"Successful: {len(succeeded)}")
    print(f"Failed: {len(failed)}")
    print(f"Not Accepted: {len(not_accepted)}")
    print(f"Unknown status: {len(unknown)}")

    return succeeded, failed, not_accepted, unknown


if __name__ == "__main__":
    reach_data = [
        (
            2820002,
            2820006,
        ),
        (
            2820006,
            2820012,
        ),
    ]
    db_path = "data/library.sqlite"
    submodels_dir = "data/submodels"
    execute_kwse_step(reach_data, db_path, submodels_dir)
