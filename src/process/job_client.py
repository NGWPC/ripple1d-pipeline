import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Type

import pandas as pd
import requests

from ..setup.collection_data import CollectionData
from ..setup.database import Database


@dataclass
class JobRecord:
    entity: Any
    id: str
    status: str


class JobClient:
    """
    Main class to communicate with Ripple1d API.
    """

    def __init__(self, collection: Type[CollectionData]):
        self.stac_collection_id = collection.stac_collection_id
        self.DEFAULT_POLL_WAIT = collection.config["polling"]["DEFAULT_POLL_WAIT"]
        self.RIPPLE1D_API_URL = collection.RIPPLE1D_API_URL

    @staticmethod
    def datetime_to_epoch_utc(datetime_str):

        # Parse the string into a naive datetime object
        dt_naive = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
        # Make the datetime object timezone-aware (UTC)
        dt_utc = dt_naive.replace(tzinfo=timezone.utc)
        # Convert to epoch time
        epoch_time = int(dt_utc.timestamp())
        return epoch_time

    def get_job_update_time(self, job_id: str) -> str:
        """
        Get updated time of a job as string
        """
        url = f"{self.RIPPLE1D_API_URL}/jobs/{job_id}"

        response = requests.get(url)
        response.raise_for_status()
        job_update_time = response.json().get("updated")
        return job_update_time

    def get_job_status(self, job_id: str) -> str:
        """
        Get status of a job from API
        """
        url = f"{self.RIPPLE1D_API_URL}/jobs/{job_id}"

        response = requests.get(url)
        response.raise_for_status()
        job_status = response.json().get("status")
        return job_status

    def check_job_successful(self, job_id: str, timeout_minutes: int = 90):
        """
        Wait for a job to finish and return true or false based on success or failure
        timeout_minutes count start from the job last updated status
        """

        while True:
            status = self.get_job_status(job_id)
            if status == "successful":
                return True
            elif status == "failed":
                logging.error(f"{self.RIPPLE1D_API_URL}/jobs/{job_id}?tb=true job failed")
                return False
            elif status == "running":
                elapsed_time = time.time() - self.datetime_to_epoch_utc(self.get_job_update_time(job_id))
                if elapsed_time / 60 > timeout_minutes:
                    logging.warning(f"{self.RIPPLE1D_API_URL}/jobs/{job_id} client timeout")
                    return False
            time.sleep(self.DEFAULT_POLL_WAIT)

    def wait_for_jobs(self, job_records: List[JobRecord], timeout_minutes=90) -> Tuple[List[JobRecord]]:
        """
        Waits for jobs to finish and returns lists of successful, failed, and unknown status jobsjobs.
        """
        succeeded = []
        failed = []
        unknown = []

        for job_record in job_records:
            while True:
                status = self.get_job_status(job_record.id)
                if status == "successful":
                    job_record.status = "successful"
                    succeeded.append(job_record)
                    break
                elif status == "failed":
                    job_record.status = "failed"
                    failed.append(job_record)
                    logging.error(f"{self.RIPPLE1D_API_URL}/jobs/{job_record.id}?tb=true job failed")
                    break
                elif status == "running":
                    updated_time = self.get_job_update_time(job_record.id)
                    elapsed_time = time.time() - self.datetime_to_epoch_utc(updated_time)
                    if elapsed_time / 60 > timeout_minutes:
                        logging.info(f"{self.RIPPLE1D_API_URL}/jobs/{job_record.id} client timeout")
                        job_record.status = "unknown"
                        unknown.append(job_record)
                        break
                time.sleep(self.DEFAULT_POLL_WAIT)

        return succeeded, failed, unknown

    def get_failed_job_err_and_tb(self, job_id) -> Tuple[str, str]:
        headers = {"Content-Type": "application/json"}

        url = f"{self.RIPPLE1D_API_URL}/jobs/{job_id}?tb=true"

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            response_data = response.json()
            if response_data and response_data["result"]:
                err = response_data["result"].get("err", "No error message")
                tb = response_data["result"].get("tb", "No traceback")
            else:
                err = "No error message"
                tb = "No traceback"
            return (err, tb)
        else:
            return (f"Failed to get job status. Status code: {response.status_code}", "")

    def get_job_payload(self, job_id) -> Dict:
        headers = {"Content-Type": "application/json"}

        url = f"{self.RIPPLE1D_API_URL}/jobs/{job_id}/metadata"

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            response_data = response.json()
            if response_data and response_data[job_id]:
                payload = response_data[job_id].get("func_kwargs", {})
                return payload
            return {}
        else:
            return {"error": f"Failed to get job metadata. Status code: {response.status_code}"}

    def get_failed_jobs_df(self, failed_ids: List[Tuple[int, str, str]]) -> pd.DataFrame:
        """
        Sends a GET request to the API for each failed reach's job and returns a formatted table
        with reach_id, error message (err), and traceback (tb), payload.

        Args:
            api_url: The base URL of the API (e.g., 'http://localhost:5000').
            failed_reaches: List of tuples (reach_id, job_id, status) for failed reaches.

        Returns:
            A pandas DataFrame containing the reach_id, error (err), and traceback (tb).
        """
        results = []

        for id, job_id, _ in failed_ids:
            err, tb = self.get_failed_job_err_and_tb(job_id)
            payload = self.get_job_payload(job_id)
            results.append((id, err, tb, payload))

        # Convert results to a pandas DataFrame for formatted output
        df = pd.DataFrame(results, columns=["id", "err", "tb", "payload"])
        return df

    def poll_and_update_job_status(
        self,
        database: Type[Database],
        process_name: str,
        process_table: str = "processing",
    ):
        """
        Polls the API for the current status of each job for the given process and
            updates the status in the processing table.

        Args:
            collection (CollectionData object): Contains parameters from config file.
            database (Database object): Contains methods to intereact with SQLite library database.
            process_name: The name of the process (e.g., "create_fim_lib").
            process_table: Name of database table to update
        """
        # Step 1: Get all job IDs for the process
        job_ids = database.get_all_job_ids_for_process(process_name, process_table)

        # Step 2: Poll the API and update the database
        headers = {"Content-Type": "application/json"}

        for entity, job_id in job_ids:
            if job_id:  # Ensure job_id exists
                url = f"{self.RIPPLE1D_API_URL}/jobs/{job_id}"
                try:
                    response = requests.get(url, headers=headers)
                    if response.status_code == 200:
                        response_data = response.json()
                        job_status = response_data.get("status", "unknown")

                        # Step 3: Update the processing table with the new status
                        database.update_table_with_job_status(process_table, process_name, job_status, entity)
                    else:
                        logging.info(
                            f"Failed to poll job {job_id} for reach {entity}. Status code: {response.status_code}"
                        )
                except requests.RequestException as e:
                    logging.info(f"Error polling job {job_id} for reach {entity}: {e}")

    def dismiss_jobs(self, job_records: List[JobRecord]) -> None:
        """Silently dismiss multiple jobs with error logging"""
        for job in job_records:
            if not job.id:
                continue

            try:
                response = requests.delete(f"{self.RIPPLE1D_API_URL}/jobs/{job.id}")
                if response.status_code == 200:
                    logging.info(f"Dismissed job {job.id}")
                else:
                    logging.error(
                        f"Failed to dismiss {job.id} - Status {response.status_code}"
                    )
            except requests.RequestException as e:
                logging.error(f"Error dismissing {job.id}: {str(e)}")
                continue