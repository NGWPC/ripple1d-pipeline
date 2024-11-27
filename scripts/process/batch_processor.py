from typing import Type
import logging
from ..setup.collection_data import CollectionData
from abc import ABC, abstractmethod


#TODO evaluate if this ABC is really necessary...
class BatchProcessor(ABC):
    def __init__(self):
        self.payload = None
        self.stop_on_error = False

    @abstractmethod
    def format_payload(cls):
        pass

    @abstractmethod
    def execute_request(self):
        pass

