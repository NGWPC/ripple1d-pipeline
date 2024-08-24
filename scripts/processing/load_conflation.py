"""
Expects network table with id and to_id coloumns already populated for each reach
"""

import json
import sqlite3


def load_json(file_path):
    """"""
    with open(file_path, "r") as file:
        data = json.load(file)
    return data


def update_model_key_and_eclipsed(db_path, data, model_key):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for key, value in data.items():
        us_xs_river = value["us_xs"].get("river", None)
        us_xs_reach = value["us_xs"].get("reach", None)
        us_xs_id = value["us_xs"].get("xs_id", None)
        ds_xs_river = value.get("ds_xs", {}).get("river", None)
        ds_xs_reach = value.get("ds_xs", {}).get("reach", None)
        ds_xs_id = value.get("ds_xs", {}).get("xs_id", None)

        if (us_xs_id, us_xs_reach, us_xs_river) == (str(ds_xs_id), ds_xs_reach, ds_xs_river):
            cursor.execute(
                """
                UPDATE network
                SET
                    model_key = ?,
                    eclipsed = True
                WHERE reach_id = ?;
            """,
                (model_key, key),
            )
        else:
            cursor.execute(
                """
                UPDATE network
                SET
                    model_key = ?,
                    eclipsed = False
                WHERE reach_id = ?;
            """,
                (model_key, key),
            )

    conn.commit()
    conn.close()


def load_conflation(model_keys, source_models_directory, db_path):
    for model_key in model_keys:
        json_data = load_json(f"{source_models_directory}\\{model_key}\\{model_key}.conflation.json")
        update_model_key_and_eclipsed(db_path, json_data, model_key)

    print(f"Conflation loaded to {db_path} from .conflation.json files")


if __name__ == "__main__":

    db_path = "data/library.sqlite"  # Update this as necessary
    model_keys = ["Baxter"]  # Update this as necessary
    source_models_directory = "data/source_models"  # Update this as necessary
    load_conflation(db_path, model_keys, source_models_directory)
