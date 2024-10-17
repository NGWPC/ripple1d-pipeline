import sqlite3
from typing import List, Tuple

from ..config import DB_CONN_TIMEOUT


def get_reaches_by_models(db_path: str, model_ids: List[str]) -> List[Tuple[int, int, str]]:
    """
    Retrieves reach IDs, updated_to_ids, and model keys from the network and processing tables
    where the model keys match and the reach is not eclipsed.
    """
    conn = sqlite3.connect(db_path, timeout=DB_CONN_TIMEOUT)
    try:
        cursor = conn.cursor()

        query = f"""
            SELECT n.reach_id, n.updated_to_id, p.model_id
            FROM network n
            JOIN processing p ON n.reach_id = p.reach_id
            WHERE p.eclipsed IS FALSE AND p.model_id IN ({','.join(['?'] * len(model_ids))})
        """
        cursor.execute(query, model_ids)
        data = cursor.fetchall()
    finally:
        conn.close()
    print(len(data), "reaches returned")
    return data


if __name__ == "__main__":
    db_path = "data/library.sqlite"
    model_ids = ["Baxter"]
    get_reaches_by_models(db_path, model_ids)
