import concurrent.futures
import json
import random
import sqlite3
from time import sleep
from typing import Tuple, Type

import requests

# from ..config import (
#     API_LAUNCH_JOBS_RETRY_WAIT,
#     PAYLOAD_TEMPLATES,
#     RIPPLE1D_API_URL,
#     RIPPLE1D_THREAD_COUNT,
# )
from ..setup.collection_data import CollectionData
from ..setup.database import Database

from .batch_processor import BatchProcessor
from .job_client import JobClient

class ReachProcessor(BatchProcessor):
    def format_payload(self, collection: Type[CollectionData]):
        self.payload = {}
        return self.payload

    def execute_request(self):
        # Implement request execution logic here
        print(f"Executing request with payload: {self.payload}")

    def run_kwse_batch_processor(self, collection: CollectionData):
        formatted_payload = self.format_payload(collection)
        self.execute_request()

    def create_ras_terrain_batch_processor(self, collection: CollectionData):
        formatted_payload = self.format_payload(collection)
        self.execute_request()

    def create_model_run_normal_depth_batch_processor(self, collection: CollectionData):
        formatted_payload = self.format_payload(collection)
        self.execute_request()

    def run_incremental_normal_depth_batch_processor(self, collection: CollectionData):
        formatted_payload = self.format_payload(collection)
        self.execute_request()