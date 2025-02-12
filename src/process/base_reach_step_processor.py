from typing import Dict, List, Optional, Tuple

from ..setup.collection_data import CollectionData
from ..setup.database import Database
from .base_step_processor import BaseStepProcessor


class BaseReachStepProcessor(BaseStepProcessor):
    """Base class for reach-level processing steps"""

    def __init__(self, collection: CollectionData, reach_data: List[Tuple]):
        super().__init__(collection)
        self.reach_data = reach_data
        self.db_table = "processing"

    def _format_reach_payload(self, template: Dict, reach_id: int, model_id: Optional[str] = None) -> Dict:
        """Common reach payload formatting"""
        replacements = {
            "nwm_reach_id": reach_id,
            "model_id": model_id or "",
            "submodels_directory": self.collection.submodels_dir,
            "library_directory": self.collection.library_dir,
            "source_model_directory": self.collection.source_models_dir,
        }
        return {
            key: value.format(**replacements) if isinstance(value, str) else value for key, value in template.items()
        }

    def _update_database(self, database: Database, status: str):
        """Common reach database update"""
        database.update_processing_table(self.job_statuses[status], self.process_name, status)
