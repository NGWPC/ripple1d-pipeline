import logging
from abc import abstractmethod
from typing import Tuple

from ..setup.collection_data import CollectionData
from ..setup.database import Database
from .job_client import JobClient


class BaseStepProcessor:
    """Base class for all processing steps"""

    def __init__(self, collection: CollectionData):
        self.collection = collection
        self.job_statuses = {"accepted": [], "succeeded": [], "failed": [], "not_accepted": [], "unknown": []}

    def execute_step(self, job_client: JobClient, database: Database, timeout: int):
        """Template method defining the processing workflow"""
        self._execute_requests()
        self._update_database(database, "accepted")
        self._update_database(database, "not_accepted")
        logging.info("Jobs submitted, waiting for jobs to finish")
        self._wait_for_jobs(job_client, timeout)
        self._log_results()

    @abstractmethod
    def _execute_requests(self):
        """Execute API requests for all items"""
        pass

    @abstractmethod
    def _update_database(self, database: Database, status: str):
        """Update database with current status"""
        pass

    def _wait_for_jobs(self, job_client: JobClient, timeout: int):
        """Common job waiting implementation"""
        self.job_statuses["succeeded"], self.job_statuses["failed"], self.job_statuses["unknown"] = (
            job_client.wait_for_jobs(self.job_statuses["accepted"], timeout)
        )

    def _categorize_result(self, result: Tuple) -> None:
        """Categorizes a job result into appropriate status list"""
        _, _, status = result  # Unpack (item_id, job_id, status)

        if status == "accepted":
            self.job_statuses["accepted"].append(result)
        elif status == "not_accepted":
            self.job_statuses["not_accepted"].append(result)
        else:
            # For succeeded/failed/unknown from job_client
            self.job_statuses[status].append(result)

    def _log_results(self):
        """Common logging implementation"""
        logging.info(f"Successful: {len(self.job_statuses['succeeded'])}")
        logging.info(f"Failed: {len(self.job_statuses['failed'])}")
        logging.info(f"Not Accepted: {len(self.job_statuses['not_accepted'])}")
        logging.info(f"Unknown: {len(self.job_statuses['unknown'])}")
