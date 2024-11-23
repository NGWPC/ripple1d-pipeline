import json
import logging
import os
import sqlite3
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from threading import Lock
from typing import List, Optional, Tuple

import requests

from ..config import (
    DB_CONN_TIMEOUT,
    DS_DEPTH_INCREMENT,
    OPTIMUM_PARALLEL_PROCESS_COUNT,
    RIPPLE1D_API_URL,
)
# from .job_utils import check_job_successful, update_processing_table
from .load_rating_curves import load_rating_curve


def get_upstream_reaches(updated_to_id: int, db_path: str, db_lock: Lock) -> List[int]:
    """
    Fetch upstream reach IDs from the 'network' table.
    """
    with db_lock:
        conn = sqlite3.connect(db_path, timeout=DB_CONN_TIMEOUT)
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT reach_id FROM network WHERE updated_to_id = ?", (updated_to_id,))
            result = [row[0] for row in cursor.fetchall()]
        finally:
            conn.close()
        return result


def get_min_max_elevation(
    downstream_id: int, submodels_directory: str, db_lock: Lock, use_central_db: bool, central_db_path: str
) -> Tuple[Optional[float], Optional[float]]:
    """
    Fetch min and max upstream elevation for a reach
    If use_central_db is true central database is used
    """
    if use_central_db:
        if not os.path.exists(central_db_path):
            logging.info("central database not found")
            return None, None
        with db_lock:
            conn = sqlite3.connect(central_db_path, timeout=DB_CONN_TIMEOUT)
            try:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT MIN(us_wse), MAX(us_wse) FROM rating_curves WHERE reach_ID = ?", (downstream_id,)
                )
                min_elevation, max_elevation = cursor.fetchone()
            finally:
                conn.close()
            return min_elevation, max_elevation
    else:
        ds_submodel_db_path = os.path.join(submodels_directory, str(downstream_id), f"{downstream_id}.db")
        if not os.path.exists(ds_submodel_db_path):
            logging.info(f"Submodel database not found for reach_id: {downstream_id}")
            return None, None

        conn = sqlite3.connect(ds_submodel_db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT MIN(us_wse), MAX(us_wse) FROM rating_curves")
            min_elevation, max_elevation = cursor.fetchone()
        finally:
            conn.close()
        return min_elevation, max_elevation


def process_reach(
    reach_id: int,
    downstream_id: Optional[int],
    submodels_directory: str,
    task_queue: Queue,
    central_db_path: str,
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
    try:
        submodel_directory_path = os.path.join(submodels_directory, str(reach_id))
        headers = {"Content-Type": "application/json"}
        valid_plans = ["nd"]

        if downstream_id:
            min_elevation, max_elevation = get_min_max_elevation(
                downstream_id, submodels_directory, central_db_lock, use_central_db, central_db_path
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
                        "ras_version": "631",
                        "write_depth_grids": False,
                    }
                )
                logging.info(f"<<<<<< payload for reach {reach_id}\n{payload}")

                # to do: launch job with retry
                response = requests.post(url, headers=headers, data=payload)
                response_json = response.json()
                job_id = response_json.get("jobID")
                if not job_id or not check_job_successful(job_id, timeout_minutes=timeout_minutes):
                    logging.info(f"KWSE run failed for {reach_id}, API job ID: {job_id}")
                    with central_db_lock:
                        update_processing_table([(reach_id, job_id)], "run_iknown_wse", "failed", central_db_path)
                else:
                    valid_plans = valid_plans + ["ikwse"]
                    with central_db_lock:
                        update_processing_table([(reach_id, job_id)], "run_iknown_wse", "successful", central_db_path)
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

        if not rc_db_job_id or not check_job_successful(rc_db_job_id, timeout_minutes=timeout_minutes):
            with central_db_lock:
                update_processing_table(
                    [(reach_id, rc_db_job_id)], "create_irating_curves_db", "failed", central_db_path
                )
            upstream_reaches = get_upstream_reaches(reach_id, central_db_path, central_db_lock)
            for upstream_reach in upstream_reaches:
                task_queue.put((upstream_reach, None))
            return
        with central_db_lock:
            update_processing_table(
                [(reach_id, rc_db_job_id)], "create_irating_curves_db", "successful", central_db_path
            )

        upstream_reaches = get_upstream_reaches(reach_id, central_db_path, central_db_lock)
        for upstream_reach in upstream_reaches:
            task_queue.put((upstream_reach, reach_id))

    except Exception as e:
        logging.info(f"Error processing reach {reach_id}: {str(e)}")
        traceback.print_exc()


def execute_ikwse_for_network(
    initial_reaches: List[Tuple[int, Optional[int]]],
    submodels_directory: str,
    db_path: str,
    use_central_db: bool,
    timeout_minutes: int = 30,
) -> None:
    """
    Start processing the network from the given list of initial reaches.
    """
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
                    submodels_directory,
                    task_queue,
                    db_path,
                    db_lock,
                    use_central_db,
                    timeout_minutes,
                )
                futures.append(future)

            for future in futures.copy():
                if future.done():
                    futures.remove(future)

            time.sleep(1)


if __name__ == "__main__":
    submodels_directory = r"D:\Users\abdul.siddiqui\workbench\projects\production\submodels"
    db_path = r"D:\Users\abdul.siddiqui\workbench\projects\production\library.sqlite"
    library_directory = ""
    initial_reaches = [(10434118, None), (10434182, None)]
    execute_ikwse_for_network(initial_reaches, submodels_directory, db_path, False, library_directory, False)
