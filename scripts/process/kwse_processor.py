import concurrent.futures
import logging
import json
import random
import requests

from time import sleep
from typing import Tuple, Type, List
from dataclasses import dataclass

from ..setup.collection_data import CollectionData
from ..setup.database import Database

from .batch_processor import BatchProcessor
from .job_client import JobClient

class KWSEProcessor(BatchProcessor):
        
    def __init__(self, collection: Type[CollectionData]):
        self.source_models_dir = collection.source_models_dir
        self.submodels_dir = collection.submodels_dir
        self.library_dir = collection.library_dir
        self.stop_on_error = collection.config['execution']['stop_on_error']
        self.RESOLUTION = collection.config['ripple_settings']['RESOLUTION']
        self.RESOLUTION_UNITS = collection.config['ripple_settings']['RESOLUTION_UNITS']
        self.TERRAIN_SOURCE_URL = collection.config['ripple_settings']['TERRAIN_SOURCE_URL']
        self.RAS_VERSION = collection.config['ripple_settings']['RAS_VERSION']
        self.RIPPLE1D_VERSION = collection.config['urls']['RIPPLE1D_VERSION']
        self.US_DEPTH_INCREMENT = collection.config['ripple_settings']['US_DEPTH_INCREMENT']
        self.DS_DEPTH_INCREMENT = collection.config['ripple_settings']['DS_DEPTH_INCREMENT']
        self.source_models_dir = collection.source_models_dir
        # self.payloads = collection.config['payload_templates']
        self.payloads = {
            "run_known_wse": {
                "submodel_directory": "{submodels_directory}\\{nwm_reach_id}",
                "plan_suffix": "kwse",
                "min_elevation": -9999,
                "max_elevation": -9999,
                "depth_increment": self.DS_DEPTH_INCREMENT,
                "ras_version": self.RAS_VERSION,
                "write_depth_grids": True,
            },
        }
        self.RIPPLE1D_API_URL = collection.config['urls']['RIPPLE1D_API_URL']
        self.API_LAUNCH_JOBS_RETRY_WAIT = collection.config['polling']['API_LAUNCH_JOBS_RETRY_WAIT']

    
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

    def execute_request(self, database : Type[Database], nwm_reach_id: int, submodels_dir: str, downstream_id: int) -> Tuple[int, str, str]:
        """
        Executes an API request for a given process and returns the job ID and status.
        Retries upto 5 times
        """

        if downstream_id:
            min_elevation, max_elevation = database.get_min_max_elevation(downstream_id, submodels_dir, None, False)
            if min_elevation and max_elevation:

                for i in range(5):
                    url = f"{self.RIPPLE1D_API_URL}/processes/run_known_wse/execution"
                    payload = json.dumps(
                        self.format_payload(
                            self.payloads["run_known_wse"], nwm_reach_id, submodels_dir, min_elevation, max_elevation
                        )
                    )
                    headers = {"Content-Type": "application/json"}

                    response = requests.post(url, headers=headers, data=payload)
                    if response.status_code == 201:
                        job_id = response.json().get("jobID")
                        return nwm_reach_id, job_id, "accepted"
                    elif response.status_code == 500:
                        logging.info(f"Retrying. {nwm_reach_id}")
                    else:
                        break
                    sleep(i * self.API_LAUNCH_JOBS_RETRY_WAIT)
                logging.info(f"Failed to accept {nwm_reach_id}, code: {response.status_code}, response: {response.text}")

        return nwm_reach_id, "", "not_accepted"

class KWSEStepProcessor(KWSEProcessor):
    """
    Inherits from the ReachProcessor class. This subclass sends API requests and 
    manages the each "step" for reach level processing in ripple1d. 
    Args:
        Collection object
        JobClient object
        Database object
    Processing:
        1. Request job for each id through API
        2. Wait for jobs to finish
        3. Update processing table with final job status
        4. Assign succeeded, failed, not_accepted, unknown status jobs
    Returns:
        None
    """
    def __init__(self, collection : Type[CollectionData], reach_data: List[Tuple[int, int, str]], job_client : Type[JobClient], database : Type[Database]):
        super().__init__(collection)
        self.job_client = job_client
        self.database = database
        self.reach_job_id_statuses = []
        self.accepted = None
        self.succeded = None
        self.failed = None
        self.not_accepted = None
        self.unknown = None
        self.reach_data = reach_data
        self.succesful_and_unknown_reaches = None

    def execute_process(self, process_name: str, timeout: int):

        for reach_id in self.reach_data:
            reach_job_id_status = self.execute_request(self.database, reach_id, process_name)
            self.reach_job_id_statuses.append(reach_job_id_status)

        self.accepted = [job for job in self.reach_job_id_statuses if job[2] == "accepted"]
        self.not_accepted = [job for job in self.reach_job_id_statuses if job[2] == "not_accepted"]

        self._update_db(self.accepted, process_name, "accepted")
        logging.info("Jobs submission complete. Waiting for jobs to finish...")

        self._wait_for_jobs(timeout)

        self._update_db(self.succeeded, process_name, "successful")
        self._update_db(self.failed, process_name, "failed")
        self._update_db(self.unknown, process_name, "unknown")

        logging.info(
            f"Successful: {len(self.succeeded)}\n"
            f"Failed: {len(self.failed)}\n"
            f"Not Accepted: {len(self.not_accepted)}\n"
            f"Unknown status: {len(self.unknown)}\n"
        )

        self._set_succesful_and_unknown_reaches_list()
        # return self.succeeded, self.failed, self.not_accepted, self.unknown

    def _update_db(self, status:str, process_name: str):
        
        if status == "accepted": 
            self.database.update_processing_table(self.accepted, process_name, "accepted")
        elif status == "succeeded":
            self.database.update_processing_table(self.succeeded, process_name, "successful")
        elif status == "failed":
            self.database.update_processing_table(self.failed, process_name, "failed")

    def _wait_for_jobs(self, timeout: int):
        self.succeeded, self.failed, self.unknown = self.job_client.wait_for_jobs(self.accepted, timeout_minutes=timeout)
    
    def _set_succesful_and_unknown_reaches_list(self):
        self.succesful_and_unknown_reaches = [reach[0] for reach in self.succeeded + self.unknown]
