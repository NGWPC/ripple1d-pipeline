from dataclasses import dataclass


@dataclass
class Reach:
    id: int
    to_id: int
    model_id: str
