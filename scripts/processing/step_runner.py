import concurrent.futures
import json
import random
import sqlite3
from time import sleep
from typing import Tuple

import requests

from ..config import PAYLOAD_TEMPLATES, RIPPLE1D_API_URL
from .job_utils import update_processing_table, wait_for_jobs


def format_payload(
    template: dict, nwm_reach_id: int, model_key: str, source_model_dir: str, submodels_dir: str
) -> dict:
    """
    Formats a payload based on a given template and parameters.
    """
    payload = {}
    for key, value in template.items():
        if isinstance(value, str):
            payload[key] = value.format(
                nwm_reach_id=nwm_reach_id,
                model_key=model_key,
                source_model_directory=source_model_dir,
                submodels_directory=submodels_dir,
            )
        else:
            payload[key] = value
    return payload


def execute_request(
    nwm_reach_id: int, model_key: str, process_name: str, source_model_dir: str, submodels_dir: str
) -> Tuple[int, str, str]:
    """
    Executes an API request for a given process and returns the job ID and status.
    """
    sleep(random.uniform(1, 10))  # Sleep to avoid database locked error on Ripple API side
    url = f"{RIPPLE1D_API_URL}/processes/{process_name}/execution"
    payload = json.dumps(
        format_payload(PAYLOAD_TEMPLATES[process_name], nwm_reach_id, model_key, source_model_dir, submodels_dir)
    )
    headers = {"Content-Type": "application/json"}

    response = requests.post(url, headers=headers, data=payload)
    if response.status_code == 201:
        job_id = response.json().get("jobID")
        return nwm_reach_id, job_id, "accepted"
    else:
        print(f"Failed to accept {nwm_reach_id}, code: {response.status_code}, response: {response.text}")
        return nwm_reach_id, "", "not_accepted"


def execute_step(
    reach_data: list, process_name: str, db_path: str, source_model_dir: str, submodels_dir: str
) -> Tuple[list, list, list]:
    """
    Submits multiple reach processing jobs and waits for their completion.
    """
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(execute_request, reach[0], reach[2], process_name, source_model_dir, submodels_dir)
            for reach in reach_data
        ]
        reach_job_id_statuses = [
            future.result() for future in concurrent.futures.as_completed(futures) if future.result() is not None
        ]

    accepted = [job for job in reach_job_id_statuses if job[2] == "accepted"]
    not_accepted = [job for job in reach_job_id_statuses if job[2] == "not_accepted"]

    update_processing_table(accepted, process_name, "accepted", db_path)
    print("Jobs submission complete. Waiting for jobs to finish...")

    succeeded, failed = wait_for_jobs(accepted)
    update_processing_table(succeeded, process_name, "successful", db_path)
    update_processing_table(failed, process_name, "failed", db_path)

    print(f"Successful: {len(succeeded)}")
    print(f"Failed: {len(failed)}")
    print(f"Not Accepted: {len(not_accepted)}")

    return succeeded, failed, not_accepted


if __name__ == "__main__":
    reach_data = [(2820002, 2820006, "Baxter"), (2820006, 2820012, "Baxter")]
    process_name = "extract_submodel"
    db_path = "data/library.sqlite"
    source_model_dir = "data/source_models"
    submodels_dir = "data/submodels"
    execute_step(reach_data, process_name, db_path, source_model_dir, submodels_dir)
