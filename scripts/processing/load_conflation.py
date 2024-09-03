import json
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


def update_model_key_and_eclipsed(db_path: str, data: Dict, model_key: str) -> None:
    """
    Updates the model_key and eclipsed status in the processing table
    based on upstream and downstream reach data.
    """
    conn = sqlite3.connect(db_path, timeout=DB_CONN_TIMEOUT)
    try:
        cursor = conn.cursor()

        for key, value in data.items():
            us_xs_river = value["us_xs"].get("river", None)
            us_xs_reach = value["us_xs"].get("reach", None)
            us_xs_id = value["us_xs"].get("xs_id", None)
            ds_xs_river = value.get("ds_xs", {}).get("river", None)
            ds_xs_reach = value.get("ds_xs", {}).get("reach", None)
            ds_xs_id = value.get("ds_xs", {}).get("xs_id", None)

            # Check if upstream and downstream cross-sections match
            eclipsed = any(
                [
                    (us_xs_id, us_xs_reach, us_xs_river) == (str(ds_xs_id), ds_xs_reach, ds_xs_river),
                    us_xs_id == "-9999",
                    us_xs_id == -9999.0,
                ]
            )

            cursor.execute(
                """
                UPDATE processing
                SET model_key = ?, eclipsed = ?
                WHERE reach_id = ?;
                """,
                (model_key, eclipsed, key),
            )

        conn.commit()
    finally:
        conn.close()


def load_conflation(model_keys: List[str], source_models_directory: str, db_path: str) -> None:
    """
    Loads conflation data into the processing table from the specified model keys and source models directory.
    """
    for model_key in model_keys:
        json_data = load_json(f"{source_models_directory}\\{model_key}\\{model_key}.conflation.json")
        update_model_key_and_eclipsed(db_path, json_data, model_key)

    print(f"Conflation loaded to {db_path} from .conflation.json files")


if __name__ == "__main__":
    db_path = "data/library.sqlite"
    model_keys = ["Baxter"]
    source_models_directory = "data/source_models"
    load_conflation(model_keys, source_models_directory, db_path)
