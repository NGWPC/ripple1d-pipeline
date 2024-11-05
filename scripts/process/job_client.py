import requests
import time


from typing import Tuple, Type, List
from ..setup.collection_data import CollectionData

class JobClient:
    """
    Main class to communicate with Ripple1d API.
    """
    def __init__(self, collection: Type[CollectionData]):
        self.stac_collection_id = collection.stac_collection_id
        self.DEFAULT_POLL_WAIT = collection.config['polling']['DEFAULT_POLL_WAIT']
        self.RIPPLE1D_API_URL = collection.config['urls']['RIPPLE1D_API_URL']

    def datetime_to_epoch_utc(datetime_str):
        from datetime import datetime, timezone

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
                print(f"{self.RIPPLE1D_API_URL}/jobs/{job_id}?tb=true", "job failed")
                return False
            elif status == "running":
                elapsed_time = time.time() - self.datetime_to_epoch_utc(self.get_job_update_time(job_id))
                if elapsed_time / 60 > timeout_minutes:
                    print(f"{self.RIPPLE1D_API_URL}/jobs/{job_id}", "client timeout")
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
                    print(f"{self.RIPPLE1D_API_URL}/jobs/{reach_job_ids[i][1]}?tb=true", "job failed")
                    break
                elif status == "running":
                    elapsed_time = time.time() - self.datetime_to_epoch_utc(self.get_job_update_time(reach_job_ids[i][1]))
                    if elapsed_time / 60 > timeout_minutes:
                        print(f"{self.RIPPLE1D_API_URL}/jobs/{reach_job_ids[i][1]}", "client timeout")
                        unknown.append((reach_job_ids[i][0], reach_job_ids[i][1], "unknown"))
                        break
                time.sleep(self.DEFAULT_POLL_WAIT)

        return succeeded, failed, unknown
