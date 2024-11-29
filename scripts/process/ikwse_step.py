import json
import logging
import os
import time
import traceback
from typing import List, Optional, Tuple, Type
import logging
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from threading import Lock
from typing import List, Optional, Tuple

import requests

from .job_client import JobClient
from ..setup.collection_data import CollectionData
from ..setup.database import Database

def get_min_max_elevation():
    pass

def process_reach(
    reach_id: int,
    downstream_id: Optional[int],
    collection : Type[CollectionData],
    database : Type[Database],
    job_client : Type[JobClient],
    task_queue: Queue,
    central_db_lock: Lock,
    use_central_db: bool,
    timeout_minutes: int = 30,
) -> None:
    """
    Process a single reach for KWSE.
    1. Find us min max elevation to use as boundary conditions
    2. Submit KWSE execution job to API and wait for it to finish
    3. Create FIM Library
    4. Load rating curves to central database
    5. Put upstream reaches in queue for later processing
    """

    DS_DEPTH_INCREMENT = collection.config['ripple_settings']['DS_DEPTH_INCREMENT']
    RAS_VERSION = collection.config['ripple_settings']['RAS_VERSION']
    RIPPLE1D_API_URL = collection.config['urls']['RIPPLE1D_API_URL']

    try:
        submodel_directory_path = os.path.join(collection.submodels_directory, str(reach_id))
        headers = {"Content-Type": "application/json"}
        valid_plans = ["nd"]

        if downstream_id:
            min_elevation, max_elevation = database.get_min_max_elevation(
                downstream_id, collection.submodels_directory, central_db_lock, use_central_db
            )
            if min_elevation and max_elevation:

                url = f"{RIPPLE1D_API_URL}/processes/run_known_wse/execution"
                payload = json.dumps(
                    {
                        "submodel_directory": submodel_directory_path,
                        "plan_suffix": "ikwse",
                        "min_elevation": min_elevation,
                        "max_elevation": max_elevation,
                        "depth_increment": DS_DEPTH_INCREMENT,
                        "ras_version": RAS_VERSION,
                        "write_depth_grids": False,
                    }
                )
                logging.info(f"<<<<<< payload for reach {reach_id}\n{payload}")

                # to do: launch job with retry
                response = requests.post(url, headers=headers, data=payload)
                response_json = response.json()
                job_id = response_json.get("jobID")
                if not job_id or not job_client.check_job_successful(job_id, timeout_minutes=timeout_minutes):
                    logging.info(f"KWSE run failed for {reach_id}, API job ID: {job_id}")
                    with central_db_lock:
                        database.update_processing_table([(reach_id, job_id)], "run_iknown_wse", "failed")
                else:
                    valid_plans = valid_plans + ["ikwse"]
                    with central_db_lock:
                        database.update_processing_table([(reach_id, job_id)], "run_iknown_wse", "successful")
            else:
                logging.info(f"Could not retrieve min/max elevation for reach_id: {downstream_id}")

        rc_db = f"{RIPPLE1D_API_URL}/processes/create_rating_curves_db/execution"
        rc_db_payload = json.dumps(
            {
                "submodel_directory": submodel_directory_path,
                "plans": valid_plans,
            }
        )
        response = requests.post(rc_db, headers=headers, data=rc_db_payload)
        rc_db_response_json = response.json()
        rc_db_job_id = rc_db_response_json.get("jobID")

        if not rc_db_job_id or not job_client.check_job_successful(rc_db_job_id, timeout_minutes=timeout_minutes):
            with central_db_lock:
                database.update_processing_table(
                    [(reach_id, rc_db_job_id)], "create_irating_curves_db", "failed"
                )
            
            upstream_reaches = database.get_upstream_reaches(reach_id, central_db_lock)
            for upstream_reach in upstream_reaches:
                task_queue.put((upstream_reach, None))
            return
        with central_db_lock:
            database.update_processing_table(
                [(reach_id, rc_db_job_id)], "create_irating_curves_db", "successful"
            )

        upstream_reaches = database.get_upstream_reaches(reach_id, central_db_lock)
        for upstream_reach in upstream_reaches:
            task_queue.put((upstream_reach, reach_id))

    except Exception as e:
        logging.info(f"Error processing reach {reach_id}: {str(e)}")
        traceback.print_exc()


def execute_ikwse_for_network(
    initial_reaches: List[Tuple[int, Optional[int]]],
    collection : Type[CollectionData],
    database : Type[Database],
    job_client : Type[JobClient],
    use_central_db: bool,
    timeout: int,
) -> None:
    """
    Start processing the network from the given list of initial reaches.
    """
    OPTIMUM_PARALLEL_PROCESS_COUNT = collection.config['execution']['OPTIMUM_PARALLEL_PROCESS_COUNT']

    task_queue = Queue()
    db_lock = Lock()
    for reach_pair in initial_reaches:
        task_queue.put(reach_pair)

    with ThreadPoolExecutor(max_workers=OPTIMUM_PARALLEL_PROCESS_COUNT) as executor:
        futures = []
        while not task_queue.empty() or futures:
            while not task_queue.empty():
                reach_id, downstream_id = task_queue.get()
                logging.info(f"Submitting task for reach {reach_id} with downstream {downstream_id}")
                future = executor.submit(
                    process_reach,
                    reach_id,
                    downstream_id,
                    collection,
                    database,
                    job_client,
                    task_queue,
                    db_lock,
                    use_central_db,
                    timeout,
                )

                futures.append(future)

            for future in futures.copy():
                if future.done():
                    futures.remove(future)

            time.sleep(1)
