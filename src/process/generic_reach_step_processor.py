from time import sleep
from typing import Dict, List, Tuple

import requests

from ..setup.collection_data import CollectionData
from .base_reach_step_processor import BaseReachStepProcessor


class GenericReachStepProcessor(BaseReachStepProcessor):
    """Handles generic reach processing steps"""

    def __init__(self, collection: CollectionData, reach_data: List[Tuple], process_name: str):
        super().__init__(collection, reach_data)
        self.process_name = process_name

    def _execute_requests(self):
        """Generic reach request execution"""
        template = self.collection.config["payload_templates"][self.process_name]

        for reach_item in self.reach_data:
            result = self._execute_single_request(reach_item, template)
            self._categorize_result(result)

    def _execute_single_request(self, reach_item: Tuple, template: Dict) -> Tuple:
        """Single request implementation"""
        reach_id, model_id = reach_item
        url = f"{self.collection.RIPPLE1D_API_URL}/processes/{self.process_name}/execution"
        payload = self._format_reach_payload(template, reach_id, model_id)

        for attempt in range(5):
            response = requests.post(url, json=payload)
            if response.status_code == 201:
                return (reach_id, response.json()["jobID"], "accepted")
            sleep(attempt * self.collection.config["polling"]["API_LAUNCH_JOBS_RETRY_WAIT"])

        return (reach_id, "", "not_accepted")
