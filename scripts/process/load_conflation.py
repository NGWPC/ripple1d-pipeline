import json
import os
import sqlite3
from typing import Dict, List

from ..config import DB_CONN_TIMEOUT


def load_json(file_path: str) -> Dict:
    """
    Loads JSON data from a given file path.
    """
    with open(file_path, "r") as file:
        data = json.load(file)
    return data


def update_model_id_and_eclipsed(db_path: str, data: Dict, model_id: str) -> None:
    """
    Updates the model_id and eclipsed status in the processing table
    based on upstream and downstream reach conflation data.
    """
    conn = sqlite3.connect(db_path, timeout=DB_CONN_TIMEOUT)
    try:
        cursor = conn.cursor()

        for key, value in data["reaches"].items():
            eclipsed = value["eclipsed"] == True
            cursor.execute(
                """
                UPDATE processing
                SET model_id = ?, eclipsed = ?
                WHERE reach_id = ?;
                """,
                (model_id, eclipsed, key),
            )

        conn.commit()
    finally:
        conn.close()


def load_conflation(model_ids: List[str], source_models_directory: str, db_path: str) -> None:
    """
    Loads conflation data into the processing table from the specified model keys and source models directory.
    """
    models_data = {}

    for model_id in model_ids:
        file_path = f"{source_models_directory}\\{model_id}\\{model_id}.conflation.json"
        if os.path.exists(file_path):
            json_data = load_json(f"{source_models_directory}\\{model_id}\\{model_id}.conflation.json")
            models_data[model_id] = json_data
        else:
            print("Does not exist", file_path)

    # order matters because we want to overwrite model with least coverages when conflict
    sorted_models_data = sorted(models_data.items(), key=lambda item: len(item[1]["reaches"]))

    for model_id, json_data in sorted_models_data:
        update_model_id_and_eclipsed(db_path, json_data, model_id)

    print(f"Conflation loaded to {db_path} from .conflation.json files")


if __name__ == "__main__":
    db_path = "data/library.sqlite"
    model_ids = ["Baxter"]
    source_models_directory = "data/source_models"
    load_conflation(model_ids, source_models_directory, db_path)
