import json
import logging
import os
import requests
import sqlite3
import time
import traceback

from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from threading import Lock
from typing import List, Optional, Tuple, Type


from ..config import DB_CONN_TIMEOUT, RIPPLE1D_API_URL, RIPPLE1D_THREAD_COUNT, RAS_VERSION

from .job_client import JobClient
from ..setup.collection_data import CollectionData
from ..setup.database import Database

from .load_rating_curves import load_rating_curve

def process_single_reach_kwse(
    reach_id: int,
    downstream_id: Optional[int],
    database : Type[Database],
    job_client : Type[JobClient],
    submodels_directory: str,
    task_queue: Queue,
    central_db_path: str,
    central_db_lock: Lock,
    library_directory: str,
    use_central_db: bool,
    skip_if_lib_created: bool,
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
        # if skip_if_lib_created and not check_fim_lib_created(reach_id, central_db_path, central_db_lock):
        #     pass
        if skip_if_lib_created and not database.check_fim_lib_created(reach_id, central_db_lock):
            pass
        else:
            submodel_directory_path = os.path.join(submodels_directory, str(reach_id))
            headers = {"Content-Type": "application/json"}

            if downstream_id:
                # min_elevation, max_elevation = get_min_max_elevation(
                #     downstream_id, library_directory, central_db_lock, use_central_db, central_db_path
                # )
                min_elevation, max_elevation = database.get_min_max_elevation(
                    downstream_id, library_directory, central_db_lock, use_central_db
                )
                if min_elevation and max_elevation:

                    url = f"{RIPPLE1D_API_URL}/processes/run_known_wse/execution"
                    payload = json.dumps(
                        {
                            "submodel_directory": submodel_directory_path,
                            "plan_suffix": "kwse",
                            "min_elevation": min_elevation,
                            "max_elevation": max_elevation,
                            "depth_increment": 1,
                            "ras_version": RAS_VERSION,
                        }
                    )
                    logging.info(f"<<<<<< payload for reach {reach_id}\n{payload}")

                    # to do: launch job with retry
                    response = requests.post(url, headers=headers, data=payload)
                    response_json = response.json()
                    job_id = response_json.get("jobID")
                    if not job_id or not job_client.check_job_successful(job_id):
                        logging.info(f"KWSE run failed for {reach_id}, API job ID: {job_id}")
                        with central_db_lock:
                            database.update_processing_table([(reach_id, job_id)], "run_known_wse", "failed")
                    else:
                        with central_db_lock:
                            database.update_processing_table([(reach_id, job_id)], "run_known_wse", "successful")
                else:
                    logging.info(f"Could not retrieve min/max elevation for reach_id: {downstream_id}")

            fim_url = f"{RIPPLE1D_API_URL}/processes/create_fim_lib/execution"
            fim_payload = json.dumps(
                {
                    "submodel_directory": submodel_directory_path,
                    "plans": ["nd", "kwse"],
                    "resolution": 3,
                    "resolution_units": "Meters",
                    "library_directory": library_directory,
                    "cleanup": True,
                }
            )
            response = requests.post(fim_url, headers=headers, data=fim_payload)
            fim_response_json = response.json()
            fim_job_id = fim_response_json.get("jobID")

            sub_db_path = os.path.join(library_directory, str(reach_id), f"{reach_id}.db")
            if not fim_job_id or not job_client.check_job_successful(fim_job_id, 30):
                with central_db_lock:
                    database.update_processing_table([(reach_id, fim_job_id)], "create_fim_lib", "failed")
                    load_rating_curve(central_db_path, reach_id, sub_db_path)
                # upstream_reaches = get_upstream_reaches(reach_id, central_db_path, central_db_lock)
                upstream_reaches = database.get_upstream_reaches(reach_id, central_db_lock)
                for upstream_reach in upstream_reaches:
                    task_queue.put((upstream_reach, None))
                return

            with central_db_lock:
                database.update_processing_table([(reach_id, fim_job_id)], "create_fim_lib", "successful")
                load_rating_curve(central_db_path, reach_id, sub_db_path)

        # upstream_reaches = get_upstream_reaches(reach_id, central_db_path, central_db_lock)
        upstream_reaches = database.get_upstream_reaches(reach_id, central_db_lock)
        for upstream_reach in upstream_reaches:
            task_queue.put((upstream_reach, reach_id))

    except Exception as e:
        logging.info(f"Error processing reach {reach_id}: {str(e)}")
        traceback.print_exc()


def execute_kwse_for_network(
    initial_reaches: List[Tuple[int, Optional[int]]],
    collection : Type[CollectionData],
    database : Type[Database],
    job_client : Type[JobClient],
    use_central_db: bool = True,
    skip_if_lib_created: bool =False,
) -> None:
    """
    Start processing the network from the given list of initial reaches.
    """
    task_queue = Queue()
    db_lock = Lock()
    for reach_pair in initial_reaches:
        task_queue.put(reach_pair)

    with ThreadPoolExecutor(max_workers=RIPPLE1D_THREAD_COUNT) as executor:
        futures = []
        while not task_queue.empty() or futures:
            while not task_queue.empty():
                reach_id, downstream_id = task_queue.get()
                logging.info(f"Submitting task for reach {reach_id} with downstream {downstream_id}")
                future = executor.submit(
                    process_single_reach_kwse,
                    reach_id,
                    downstream_id,
                    database,
                    job_client,
                    collection.submodels_dir,
                    task_queue,
                    database.db_path,
                    db_lock,
                    collection.library_dir,
                    use_central_db,
                    skip_if_lib_created,
                )
                futures.append(future)

            for future in futures.copy():
                if future.done():
                    futures.remove(future)

            time.sleep(1)

# def get_upstream_reaches(updated_to_id: int, db_path: str, db_lock: Lock) -> List[int]:
#     """
#     Fetch upstream reach IDs from the 'network' table.
#     """
#     with db_lock:
#         conn = sqlite3.connect(db_path, timeout=DB_CONN_TIMEOUT)
#         try:
#             cursor = conn.cursor()
#             cursor.execute("SELECT reach_id FROM network WHERE updated_to_id = ?", (updated_to_id,))
#             result = [row[0] for row in cursor.fetchall()]
#         finally:
#             conn.close()
#         return result


# def check_fim_lib_created(reach_id: int, db_path: str, db_lock: Lock) -> bool:
#     """
#     Check if FIM library has been created for a reach.
#     """
#     with db_lock:
#         conn = sqlite3.connect(db_path, timeout=DB_CONN_TIMEOUT)
#         try:
#             cursor = conn.cursor()
#             cursor.execute("SELECT create_fim_lib_job_id FROM processing WHERE reach_id = ?", (reach_id,))
#             result = cursor.fetchone()
#         finally:
#             conn.close()

#         if result is None:
#             raise ValueError(f"No record found for reach_id {reach_id}")

#         return result[0] is not None


# def get_min_max_elevation(
#     downstream_id: int, library_directory: str, db_lock: Lock, use_central_db: bool, central_db_path: str
# ) -> Tuple[Optional[float], Optional[float]]:
#     """
#     Fetch min and max upstream elevation for a reach
#     If use_central_db is true central database is used
#     """
#     if use_central_db:
#         if not os.path.exists(central_db_path):
#             logging.info("central database not found")
#             return None, None
#         with db_lock:
#             conn = sqlite3.connect(central_db_path, timeout=DB_CONN_TIMEOUT)
#             try:
#                 cursor = conn.cursor()
#                 cursor.execute(
#                     "SELECT MIN(us_wse), MAX(us_wse) FROM rating_curves WHERE reach_ID = ?", (downstream_id,)
#                 )
#                 min_elevation, max_elevation = cursor.fetchone()
#             finally:
#                 conn.close()
#             return min_elevation, max_elevation
#     else:
#         ds_submodel_db_path = os.path.join(library_directory, str(downstream_id), f"{downstream_id}.db")
#         if not os.path.exists(ds_submodel_db_path):
#             logging.info(f"Submodel database not found for reach_id: {downstream_id}")
#             return None, None

#         conn = sqlite3.connect(ds_submodel_db_path)
#         try:
#             cursor = conn.cursor()
#             cursor.execute("SELECT MIN(us_wse), MAX(us_wse) FROM rating_curves")
#             min_elevation, max_elevation = cursor.fetchone()
#         finally:
#             conn.close()
#         return min_elevation, max_elevation


# if __name__ == "__main__":
#     submodels_directory = r"D:\Users\abdul.siddiqui\workbench\projects\production\submodels"
#     db_path = r"D:\Users\abdul.siddiqui\workbench\projects\production\library.sqlite"
#     library_directory = ""
#     initial_reaches = [(10434118, None), (10434182, None)]
#     execute_kwse_for_network(initial_reaches, submodels_directory, db_path, False, library_directory, False)
