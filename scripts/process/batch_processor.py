from abc import ABC, abstractmethod

# TODO - is this ABC is really necessary?
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
