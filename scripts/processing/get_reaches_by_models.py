import sqlite3
from typing import List, Tuple


def get_reaches_by_models(db_path: str, model_keys: List[str]) -> List[Tuple[int, int, str]]:
    """
    Retrieves reach IDs, updated_to_ids, and model keys from the network table
    where the model keys match and the reach is not eclipsed.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = f"""
        SELECT reach_id, updated_to_id, model_key FROM network
        WHERE eclipsed IS FALSE AND model_key IN ({','.join(['?']*len(model_keys))})
    """
    cursor.execute(query, model_keys)
    data = cursor.fetchall()
    conn.close()
    print(len(data), "reaches returned")
    return data


if __name__ == "__main__":
    db_path = "data/library.sqlite"
    model_keys = ["Baxter"]
    get_reaches_by_models(db_path, model_keys)
