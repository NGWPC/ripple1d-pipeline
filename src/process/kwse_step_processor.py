from time import sleep
from typing import Dict, List, Tuple

import requests

from ..setup.collection_data import CollectionData
from .base_reach_step_processor import BaseReachStepProcessor
from .ikwse_step import get_min_max_elevation


class KWSEStepProcessor(BaseReachStepProcessor):
    """Handles KWSE-specific reach processing"""

    def __init__(self, collection: CollectionData, reach_data: List[Tuple]):
        super().__init__(collection, reach_data)
        self.process_name = "run_known_wse"

    def _execute_requests(self):
        """KWSE-specific request execution with elevation data"""
        template = self.collection.config["payload_templates"][self.process_name]

        for reach_id, downstream_id in self.reach_data:
            result = self._execute_single_request(reach_id, downstream_id, template)
            self._categorize_result(result)

    def _execute_single_request(self, reach_id: int, downstream_id: int, template: Dict) -> Tuple:
        """KWSE-specific request implementation with elevation data"""
        submodels_dir = self.collection.submodels_dir
        min_elev, max_elev = get_min_max_elevation(downstream_id, submodels_dir)

        if not min_elev or not max_elev:
            return (reach_id, "", "not_accepted")

        payload = self._format_reach_payload(template, reach_id)
        payload.update({"min_elevation": min_elev, "max_elevation": max_elev})

        for attempt in range(5):
            response = requests.post(
                f"{self.collection.RIPPLE1D_API_URL}/processes/{self.process_name}/execution", json=payload
            )
            if response.status_code == 201:
                return (reach_id, response.json()["jobID"], "accepted")
            sleep(attempt * self.collection.config["polling"]["API_LAUNCH_JOBS_RETRY_WAIT"])

        return (reach_id, "", "not_accepted")
