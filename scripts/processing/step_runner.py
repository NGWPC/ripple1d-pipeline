import concurrent.futures
import csv
import json
import random
import sqlite3
from time import sleep

import requests

from .job_utils import update_processing_table, wait_for_jobs

payload_templates = {
    "extract_submodel": {
        "source_model_directory": "{source_model_directory}\\{model_key}",
        "submodel_directory": "{submodels_directory}\\{nwm_reach_id}",
        "nwm_id": "{nwm_reach_id}",
    },
    "create_ras_terrain": {
        "submodel_directory": "{submodels_directory}\\{nwm_reach_id}",
        "resolution": 3.0,
        "resolution_units": "Meters",
        "terrain_source_url": "s3://fimc-data/hand_fim/inputs/3dep_dems/1m_5070_lidar_tiles_fim60/fim_seamless_3dep_dem_1m_5070_fixed.vrt",
    },
    "create_model_run_normal_depth": {
        "submodel_directory": "{submodels_directory}\\{nwm_reach_id}",
        "plan_suffix": "ind",
        "num_of_discharges_for_initial_normal_depth_runs": 10,
        "ras_version": "631",
    },
    "run_incremental_normal_depth": {
        "submodel_directory": "{submodels_directory}\\{nwm_reach_id}",
        "plan_suffix": "nd",
        "depth_increment": 0.5,
        "ras_version": "631",
    },
}


def format_payload(template, nwm_reach_id, model_key, source_model_dir, submodels_dir):
    payload = {}
    for key, value in template.items():
        if type(value) != str:
            payload[key] = value
        else:
            payload[key] = value.format(
                nwm_reach_id=nwm_reach_id,
                model_key=model_key,
                source_model_directory=source_model_dir,
                submodels_directory=submodels_dir,
            )
    return payload


def execute_request(nwm_reach_id, model_key, process_name, source_model_dir, submodels_dir):
    sleep(random.uniform(1, 4))  # Sleep to avoid database locked error on Ripple API side
    url = f"http://localhost/processes/{process_name}/execution"
    payload = json.dumps(
        format_payload(payload_templates[process_name], nwm_reach_id, model_key, source_model_dir, submodels_dir)
    )
    headers = {"Content-Type": "application/json"}

    response = requests.post(url, headers=headers, data=payload)
    if response.status_code == 201:
        job_id = response.json().get("jobID")
        return (
            nwm_reach_id,
            job_id,
            "accepted",
        )
    else:
        print(f"Failed to process nwm_reach_id {nwm_reach_id}, status code: {response.status_code}")
        print(response.text)
        return (nwm_reach_id, "", "not_accepted")


def execute_step(reach_data, process_name, db_path, source_model_dir, submodels_dir):

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(execute_request, reach[0], reach[2], process_name, source_model_dir, submodels_dir)
            for reach in reach_data
        ]
        reach_job_id_statuses = [
            future.result() for future in concurrent.futures.as_completed(futures) if future.result() is not None
        ]

    accepted = [
        (reach_job_id[0], reach_job_id[1]) for reach_job_id in reach_job_id_statuses if reach_job_id[2] == "accepted"
    ]
    not_accepted = [
        (reach_job_id[0], reach_job_id[1])
        for reach_job_id in reach_job_id_statuses
        if reach_job_id[2] == "not_accepted"
    ]

    update_processing_table(accepted, process_name, "accepted", db_path)
    print("Jobs submission complete. Waiting for jobs to finish ...")

    succeeded, failed = wait_for_jobs(accepted)
    print("Jobs execution complete.")

    update_processing_table(succeeded, process_name, "successful", db_path)
    update_processing_table(failed, process_name, "failed", db_path)

    print("Successful:", len(succeeded))
    print("Failed:", len(failed))
    print("Not Accepted:", len(not_accepted))

    return succeeded, failed, not_accepted


if __name__ == "__main__":
    reach_data = [
        (2820002, 2820006, "Baxter"),
        (2820006, 2820012, "Baxter"),
    ]
    process_name = "extract_submodel"
    db_path = ""
    source_model_dir = ""
    submodels_dir = ""
    execute_step(reach_data, process_name, db_path, source_model_dir, submodels_dir)
