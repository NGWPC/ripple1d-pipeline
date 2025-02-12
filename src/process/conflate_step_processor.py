from time import sleep
from typing import Dict, List, Tuple

import requests

from ..setup.collection_data import CollectionData
from .base_model_step_processor import BaseModelStepProcessor


class ConflateModelStepProcessor(BaseModelStepProcessor):
    """Handles model conflation specific logic"""

    def __init__(self, collection: CollectionData, model_ids: List[str]):
        super().__init__(collection, model_ids)
        self.process_name = "conflate_model"

    def _execute_requests(self):
        """Model-specific request execution"""
        template = self.collection.config["payload_templates"][self.process_name]

        for model_id in self.model_ids:
            result = self._execute_single_request(model_id, template)
            self._categorize_result(result)

    def _execute_single_request(self, model_id: str, template: Dict) -> Tuple:
        """Single request implementation with retries"""
        url = f"{self.collection.RIPPLE1D_API_URL}/processes/{self.process_name}/execution"
        payload = self._format_model_payload(template, model_id)

        for attempt in range(5):
            response = requests.post(url, json=payload)
            if response.status_code == 201:
                return (model_id, response.json()["jobID"], "accepted")
            sleep(attempt * self.collection.config["polling"]["API_LAUNCH_JOBS_RETRY_WAIT"])

        return (model_id, "", "not_accepted")
