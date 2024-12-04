import requests
import logging
import time
from datetime import datetime, timezone
import pandas as pd

from typing import Tuple, Type, List
from ..setup.collection_data import CollectionData
from ..setup.database import Database

class JobClient:
    """
    Main class to communicate with Ripple1d API.
    """
    def __init__(self, collection: Type[CollectionData]):
        self.stac_collection_id = collection.stac_collection_id
        self.DEFAULT_POLL_WAIT = collection.config['polling']['DEFAULT_POLL_WAIT']
        self.RIPPLE1D_API_URL = collection.config['urls']['RIPPLE1D_API_URL']

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

    def check_job_successful(self, job_id: str, timeout_minutes : int = 90):
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

    def wait_for_jobs(self, 
                      reach_job_ids: List[Tuple[int, str]], 
                      timeout_minutes=90) -> Tuple[List[Tuple[int, str]], List[Tuple[int, str]]]:
        """
        Waits for jobs to finish and returns lists of successful, failed, and unknown status jobsjobs.
        """
        succeeded = []
        failed = []
        unknown = []

        i = 0
        for i in range(len(reach_job_ids)):
            while True:
                status = self.get_job_status(reach_job_ids[i][1])
                if status == "successful":
                    succeeded.append((reach_job_ids[i][0], reach_job_ids[i][1], "successful"))
                    break
                elif status == "failed":
                    failed.append((reach_job_ids[i][0], reach_job_ids[i][1], "failed"))
                    logging.error(f"{self.RIPPLE1D_API_URL}/jobs/{reach_job_ids[i][1]}?tb=true job failed")
                    break
                elif status == "running":
                    updated_time = self.get_job_update_time(reach_job_ids[i][1])
                    elapsed_time = time.time() - self.datetime_to_epoch_utc(updated_time)
                    if elapsed_time / 60 > timeout_minutes:
                        logging.info(f"{self.RIPPLE1D_API_URL}/jobs/{reach_job_ids[i][1]}", "client timeout")
                        unknown.append((reach_job_ids[i][0], reach_job_ids[i][1], "unknown"))
                        break
                time.sleep(self.DEFAULT_POLL_WAIT)

        return succeeded, failed, unknown


    def get_failed_jobs_df(self, failed_ids: List[Tuple[int, str, str]]) -> pd.DataFrame:
        """
        Sends a GET request to the API for each failed reach's job and returns a formatted table
        with reach_id, error message (err), and traceback (tb).

        Args:
            api_url: The base URL of the API (e.g., 'http://localhost:5000').
            failed_reaches: List of tuples (reach_id, job_id, status) for failed reaches.

        Returns:
            A pandas DataFrame containing the reach_id, error (err), and traceback (tb).
        """
        headers = {"Content-Type": "application/json"}
        results = []

        for id, job_id, _ in failed_ids:
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
                results.append((id, err, tb))
            else:
                results.append((id, f"Failed to get job status. Status code: {response.status_code}", ""))

        # Convert results to a pandas DataFrame for formatted output
        df = pd.DataFrame(results, columns=["id", "err", "tb"])
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

