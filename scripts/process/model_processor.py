import json
import logging
import requests
from typing import Type, Tuple, List
from dataclasses import dataclass
from time import sleep

from ..setup.database import Database
from ..setup.collection_data import CollectionData
from .batch_processor import BatchProcessor
from .job_client import JobClient


class ModelProcessor(BatchProcessor):

    def __init__(self, collection: Type[CollectionData]):
        self.stop_on_error = collection.config["execution"]["stop_on_error"]
        self.payloads = collection.config["payload_templates"]
        self.source_models_dir = collection.source_models_dir
        self.RIPPLE1D_API_URL = collection.RIPPLE1D_API_URL
        self.API_LAUNCH_JOBS_RETRY_WAIT = collection.config["polling"]["API_LAUNCH_JOBS_RETRY_WAIT"]
        self.model_ids = collection.get_models()
        self.conflate_model_job_statuses = {}

    def format_payload(self, template: dict, model_id: str) -> dict:
        """
        Formats a payload based on a given template and parameters.
        """
        payload = {}
        for key, value in template.items():
            if isinstance(value, str):
                payload[key] = value.format(
                    model_id=model_id,
                    source_model_directory=self.source_models_dir,
                )
            else:
                payload[key] = value
        return payload

    def execute_request(self, model_id: str, process_name: str) -> Tuple[int, str, str]:
        """
        Executes an API request for a given process and returns the job ID and status.
        Retries upto 5 times
        """

        for i in range(5):
            url = f"{self.RIPPLE1D_API_URL}/processes/{process_name}/execution"
            payload = json.dumps(self.format_payload(self.payloads[process_name], model_id))
            headers = {"Content-Type": "application/json"}

            response = requests.post(url, headers=headers, data=payload)
            if response.status_code == 201:
                job_id = response.json().get("jobID")
                return model_id, job_id, "accepted"
            elif response.status_code == 500:
                logging.info(f"Retrying. {model_id}")
            else:
                break
            sleep(i * self.API_LAUNCH_JOBS_RETRY_WAIT)
            logging.info(f"Failed to accept {model_id}, code: {response.status_code}, response: {response.text}")
            return model_id, "", "not_accepted"

    def _update_reach_ids(self, reach_data: List[Tuple[int, int, str]]) -> None:
        self.reach_data = reach_data


class ConflateModelBatchProcessor(ModelProcessor):
    """
    Inherits from the ModelProcessor class. This subclass sends API requests and
    manages the conflate_model step.
    Args:
        Collection object
        JobClient object
        Database object
    Processing:
        1. Request job for each id through API
        2. Wait for jobs to finish
        3. Update models table with final job status
        4. Assign succeeded, failed, not_accepted, unknown status jobs
    Returns:
        None
    """

    def __init__(self, collection: Type[CollectionData], model_ids: List):
        super().__init__(collection)
        self.model_ids = model_ids
        self.timeout_minutes = 10
        self.model_job_id_statuses = []

    def conflate_model_batch_process(self, job_client: Type[JobClient], database: Type[Database]):
        for model_id in self.model_ids:
            single_model_job_id_status = self.execute_request(model_id, "conflate_model")

            self.model_job_id_statuses.append(single_model_job_id_status)

        self.accepted = [job for job in self.model_job_id_statuses if job[2] == "accepted"]
        self.not_accepted = [job for job in self.model_job_id_statuses if job[2] == "not_accepted"]

        self._update_db(database, "accepted")
        logging.info("Jobs submission complete. Waiting for jobs to finish...")

        self._wait_for_jobs(job_client)

        self._update_db(database, "succeeded")
        self._update_db(database, "failed")

        logging.info(f"Successful: {len(self.succeeded)}")
        logging.info(f"Failed: {len(self.failed)}")
        logging.info(f"Not Accepted: {len(self.not_accepted)}")
        logging.info(f"Status Unknown: {len(self.unknown)}")

    def _update_db(self, database: Type[Database], status: str):

        if status == "accepted":
            database.update_models_table(self.accepted, "conflate_model", "accepted")
        elif status == "succeeded":
            database.update_models_table(self.succeeded, "conflate_model", "successful")
        elif status == "failed":
            database.update_models_table(self.failed, "conflate_model", "failed")
        elif status == "unknown":
            database.update_processing_table(self.unknown, "conflate_model", "unknown")

    def _wait_for_jobs(self, job_client):
        self.succeeded, self.failed, self.unknown = job_client.wait_for_jobs(
            self.accepted, timeout_minutes=self.timeout_minutes
        )
        self.conflate_model_job_statuses["succeeded"] = self.succeeded
        self.conflate_model_job_statuses["failed"] = self.failed
        self.conflate_model_job_statuses["unknown"] = self.unknown
