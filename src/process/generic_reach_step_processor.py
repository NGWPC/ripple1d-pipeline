from time import sleep
from typing import Dict, List, Tuple

import requests

from ..setup.collection_data import CollectionData
from .base_reach_step_processor import BaseReachStepProcessor
from .reach import Reach


class GenericReachStepProcessor(BaseReachStepProcessor):
    """Handles generic reach processing steps"""

    def __init__(self, collection: CollectionData, reaches: List[Reach], process_name: str):
        super().__init__(collection, reaches)
        self.process_name = process_name

    def _execute_requests(self):
        """Generic reach request execution"""
        template = self.collection.config["payload_templates"][self.process_name]

        for reach in self.reaches:
            result = self._execute_single_request(reach, template)
            self._categorize_result(result)

    def _execute_single_request(self, reach: Reach, template: Dict) -> Tuple:
        """Single request implementation"""
        url = f"{self.collection.RIPPLE1D_API_URL}/processes/{self.process_name}/execution"
        payload = self._format_reach_payload(template, reach.id, reach.model_id)

        for attempt in range(5):
            response = requests.post(url, json=payload)
            if response.status_code == 201:
                return (reach, response.json()["jobID"], "accepted")
            sleep(attempt * self.collection.config["polling"]["API_LAUNCH_JOBS_RETRY_WAIT"])

        return (reach, "", "not_accepted")
