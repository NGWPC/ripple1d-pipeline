import json
import logging
import os
from typing import Dict, List, Type

from ..setup.database import Database


def load_json(file_path: str) -> Dict:
    """
    Loads JSON data from a given file path.
    """
    with open(file_path, "r") as file:
        data = json.load(file)
    return data


def load_conflation(model_ids: List[str], database: Type[Database]) -> None:
    """
    Loads conflation data into the processing table from the specified model keys and source models directory.
    """
    source_models_directory = database.source_models_dir
    models_data = {}

    for model_id in model_ids:
        file_path = f"{source_models_directory}\\{model_id}\\{model_id}.conflation.json"
        if os.path.exists(file_path):
            json_data = load_json(f"{source_models_directory}\\{model_id}\\{model_id}.conflation.json")
            models_data[model_id] = json_data
        else:
            logging.info("Does not exist", file_path)

    # order matters because we want to overwrite model with least coverages when conflict
    sorted_models_data = sorted(models_data.items(), key=lambda item: len(item[1]["reaches"]))

    for model_id, json_data in sorted_models_data:
        database.update_model_id_and_eclipsed(json_data, model_id)

    logging.info(f"Conflation loaded to {database.db_path} from .conflation.json files")
