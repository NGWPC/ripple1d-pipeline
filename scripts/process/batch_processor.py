from typing import Type
import logging
from ..setup.collection_data import CollectionData

class BatchProcessor:
    def __init__(self):
        self.payload = None
        self.stop_on_error = False

    def format_payload(self, collection: Type[CollectionData]):
        self.payload = {}
        return self.payload

    def execute_request(self):
        # Implement request execution logic here
        logging.info(f"Executing request with payload: {self.payload}")

