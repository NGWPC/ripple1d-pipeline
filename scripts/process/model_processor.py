import json
import requests
from typing import Type, Tuple
from time import sleep

from ..setup.database import Database
from ..setup.collection_data import CollectionData
from .batch_processor import BatchProcessor
from .job_client import JobClient


class ModelProcessor(BatchProcessor):

    def __init__(self, collection: Type[CollectionData]):
        self.stop_on_error = False
        self.SOURCE_NETWORK = collection.config['ripple_settings']['SOURCE_NETWORK']
        self.SOURCE_NETWORK_VERSION = collection.config['ripple_settings']['SOURCE_NETWORK_VERSION']
        self.SOURCE_NETWORK_TYPE = collection.config['ripple_settings']['SOURCE_NETWORK_TYPE']
        self.source_models_dir = collection.source_models_dir
        self.payloads = {
            "conflate_model": {
                "source_model_directory": "{source_model_directory}\\{model_id}",
                "source_network": {"file_name": self.SOURCE_NETWORK, "version": self.SOURCE_NETWORK_VERSION, "type": self.SOURCE_NETWORK_TYPE},
            },
            "extract_submodel": {
                "source_model_directory": "{source_model_directory}\\{model_id}",
                "submodel_directory": "{submodels_directory}\\{nwm_reach_id}",
                "nwm_id": "{nwm_reach_id}",}
        }
        self.RIPPLE1D_API_URL = collection.config['urls']['RIPPLE1D_API_URL']
        self.API_LAUNCH_JOBS_RETRY_WAIT = collection.config['polling']['API_LAUNCH_JOBS_RETRY_WAIT']
        self.model_ids = collection.get_models()
        self.conflate_model_job_statuses = {}
        self.extract_model_job_statuses = {}

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

    def execute_single_model(self, model_id: str, process_name: str)  -> Tuple[int, str, str]:
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
                print(f"Retrying. {model_id}")
            else:
                break
            sleep(i * self.API_LAUNCH_JOBS_RETRY_WAIT)
            print(f"Failed to accept {model_id}, code: {response.status_code}, response: {response.text}")
            return model_id, "", "not_accepted"

    #TODO Make this a class, which contains four attributes:
    #    succeded_models, failed_models, not_accepted_models, unknown_status_models
    # This is the refactor of the execute_model_step function
    def conflate_model_batch_processor(self):
        for model in self.model_ids:
            self.execute_single_model(model, "conflate_model")
    

    def load_conflation_model_batch_processor(self, collection: CollectionData):
        formatted_payload = self.format_payload(collection)
        self.execute_request()

    def update_network_model_batch_processor(self, collection: CollectionData):
        formatted_payload = self.format_payload(collection)
        self.execute_request()

    def extract_submodel_batch_processor(self, collection: CollectionData):
        formatted_payload = self.format_payload(collection)
        self.execute_request()

class ConflateModelBatchProcessor(ModelProcessor):
    """
    Inherits from the ModelProcessor class. This subclass sends API requests and 
    manages the conflate_model step. 
    Args:
        ModelProcessor object
        JobClient object
        Database object
    Processing:
        1. Request job for each id through API
        2. Wait for jobs to finish
        3. Update models table with final job status
        4. Return succeeded, failed, not_accepted, unknown status jobs
    Returns:
        None
    """
    def __init__(self, modelprocessor : Type[ModelProcessor], job_client : Type[JobClient], database : Type[Database]):
        super().__init__()
        self.modelprocessor = modelprocessor
        self.job_client = job_client
        self.database = database
        self.model_job_id_statuses = []
        self.accepted = None
        self.succeded = None
        self.failed = None
        self.not_accepted = None
        self.unknown = None


    def conflate_model_batch_process(self):
        for model_id in range(len(self.modelprocessor.model_ids)):
            single_model_job_id_status = self.execute_single_model(model_id, "conflate_model")
            self.model_job_id_statuses.append(single_model_job_id_status)
            self.accepted = [job for job in self.model_job_id_statuses if job[2] == "accepted"]
            self.not_accepted = [job for job in self.model_job_id_statuses if job[2] == "not_accepted"]
            
    
    def wait_for_jobs(self):
        self.job_client.wait_for_jobs(self.accepted)
    

## NEED TO IMPLEMENT THE REST OF THIS FUNCTIONALITY
#     Database.update_models_table(accepted, process_name, "accepted", db_path)
#     print("Jobs submission complete. Waiting for jobs to finish...")

#     succeeded, failed, unknown = JobClient.wait_for_jobs(accepted, timeout_minutes=timeout_minutes)
#     Database.update_models_table(succeeded, process_name, "successful", db_path)
#     Database.update_models_table(failed, process_name, "failed", db_path)

#     print(f"Successful: {len(succeeded)}")
#     print(f"Failed: {len(failed)}")
#     print(f"Not Accepted: {len(not_accepted)}")
#     print(f"Status Unknown: {len(unknown)}")

#     return succeeded, failed, not_accepted, unknown


## Example Workflow:
collection = CollectionData("example_collection")
modelprocessor = ModelProcessor(collection)
jobclient = JobClient(collection)
database = Database(collection)

conflatemodelbatch = ConflateModelBatchProcessor(modelprocessor, jobclient, database)
conflatemodelbatch.conflate_model_batch_process()
conflatemodelbatch.wait_for_jobs()