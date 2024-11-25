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

class ReachProcessor(BatchProcessor):
        
    def __init__(self, collection: Type[CollectionData]):
        self.source_models_dir = collection.source_models_dir
        self.submodels_dir = collection.submodels_dir
        self.library_dir = collection.library_dir
        self.stop_on_error = collection.config['execution']['stop_on_error']
        self.RESOLUTION = collection.config['ripple_settings']['RESOLUTION']
        self.RESOLUTION_UNITS = collection.config['ripple_settings']['RESOLUTION_UNITS']
        self.TERRAIN_SOURCE_URL = collection.config['ripple_settings']['TERRAIN_SOURCE_URL']
        self.RAS_VERSION = collection.config['ripple_settings']['RAS_VERSION']
        self.DEPTH_INCREMENT = collection.config['ripple_settings']['DEPTH_INCREMENT']
        self.source_models_dir = collection.source_models_dir
        self.payloads = {
            "extract_submodel": {
                "source_model_directory": "{source_model_directory}\\{model_id}",
                "submodel_directory": "{submodels_directory}\\{nwm_reach_id}",
                "nwm_id": "{nwm_reach_id}",
            },
            "create_ras_terrain": {
                "submodel_directory": "{submodels_directory}\\{nwm_reach_id}",
                "terrain_source_url": self.TERRAIN_SOURCE_URL,
                # "resolution": self.RESOLUTION,
                # "resolution_units": self.RESOLUTION_UNITS,
            },
            "create_model_run_normal_depth": {
                "submodel_directory": "{submodels_directory}\\{nwm_reach_id}",
                "plan_suffix": "ind",
                "num_of_discharges_for_initial_normal_depth_runs": 10,
                "ras_version": self.RAS_VERSION,
            },
            "run_incremental_normal_depth": {
                "submodel_directory": "{submodels_directory}\\{nwm_reach_id}",
                "plan_suffix": "nd",
                "depth_increment": self.DEPTH_INCREMENT,
                "ras_version": self.RAS_VERSION,
            },
            "create_fim_lib": {
                "submodel_directory": "{submodels_directory}\\{nwm_reach_id}",
                "plans": ["nd", "kwse"],
                "resolution": self.RESOLUTION,
                "resolution_units": self.RESOLUTION_UNITS,
                "library_directory": "{library_directory}",
                "cleanup": True,
            },
            
        }
        self.RIPPLE1D_API_URL = collection.config['urls']['RIPPLE1D_API_URL']
        self.API_LAUNCH_JOBS_RETRY_WAIT = collection.config['polling']['API_LAUNCH_JOBS_RETRY_WAIT']
        self.RIPPLE1D_THREAD_COUNT = collection.config['polling']['RIPPLE1D_THREAD_COUNT']
        self.extract_submodel_job_statuses = {}
        self.create_ras_terrain_job_statuses = {}
        self.create_model_run_normal_depth_job_statuses = {}
        self.run_incremental_normal_depth_job_statuses = {}
        self.create_fim_lib_job_statuses = {}
        self.extract_submodel_job_timeout = 5
        self.create_ras_terrain_job_timeout = 3
        self.create_model_run_normal_depth_job_timeout = 10
        self.run_incremental_normal_depth_job_timeout = 15
        self.create_fim_lib_job_timeout = 20

    
    def format_payload(self, template: dict, nwm_reach_id: int, model_id : str = "") -> dict:
        """
        Formats a payload based on a given template and parameters.
        """
        payload = {}
        for key, value in template.items():
            if isinstance(value, str):
                payload[key] = value.format(
                    nwm_reach_id = nwm_reach_id,
                    model_id = model_id,
                    source_model_directory = self.source_models_dir,
                    submodels_directory = self.submodels_dir,
                    library_directory = self.library_dir,
                )
            else:
                payload[key] = value
        return payload

    def execute_request(self, nwm_reach_id: int, process_name: str, model_id: str = "") -> Tuple[int, str, str]:
        """
        Executes an API request for a given process and returns the job ID and status.
        Retries upto 5 times
        """

        for i in range(5):
            url = f"{self.RIPPLE1D_API_URL}/processes/{process_name}/execution"
            payload = json.dumps(self.format_payload(self.payloads[process_name], nwm_reach_id, model_id))
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


    # def create_ras_terrain_batch_processor(self, collection: CollectionData):
    #     formatted_payload = self.format_payload(collection)
    #     self.execute_request()

    # def create_model_run_normal_depth_batch_processor(self, collection: CollectionData):
    #     formatted_payload = self.format_payload(collection)
    #     self.execute_request()

    # def run_incremental_normal_depth_batch_processor(self, collection: CollectionData):
    #     formatted_payload = self.format_payload(collection)
    #     self.execute_request()

    # def run_kwse_batch_processor(self, collection: CollectionData):
    #     formatted_payload = self.format_payload(collection)
    #     self.execute_request()

class ExecuteReachStepBatchProcessor(ReachProcessor):
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

    def execute_extract_submodel_process(self):

        for reach_id, model_id in self.reach_data:
            reach_job_id_status = self.execute_request(reach_id, "extract_submodel", model_id)
            self.reach_job_id_statuses.append(reach_job_id_status)

        self.accepted = [job for job in self.reach_job_id_statuses if job[2] == "accepted"]
        self.not_accepted = [job for job in self.reach_job_id_statuses if job[2] == "not_accepted"]

        self._update_db(self.accepted, "extract_submodel", "accepted")
        logging.info("Jobs submission complete. Waiting for jobs to finish...")

        self._wait_for_jobs(self.extract_submodel_job_timeout)

        self._update_db(self.succeeded, "extract_submodel", "successful")
        self._update_db(self.failed, "extract_submodel", "failed")
        self._update_db(self.unknown, "extract_submodel", "unknown")

        logging.info(
            f"Successful: {len(self.succeeded)}\n"
            f"Failed: {len(self.failed)}\n"
            f"Not Accepted: {len(self.not_accepted)}\n"
            f"Unknown status: {len(self.unknown)}\n"
        )

        return self.succeeded, self.failed, self.not_accepted, self.unknown

    def execute_process(self, process_name: str, timeout: int):

        for reach_id in self.succesful_and_unknown_reaches:
            reach_job_id_status = self.execute_request(reach_id, process_name)
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

        return self.succeeded, self.failed, self.not_accepted, self.unknown

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


# @dataclass
# class ReachData:
#     reach_id : int
#     updated_to_id : int
#     model_id: str