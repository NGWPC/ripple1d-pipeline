import sqlite3


def get_reaches_by_models(db_path, model_keys):
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
    db_path = "data/library.sqlite"  # Update this as necessary
    model_keys = ["Baxter"]  # Update this as necessary
    get_reaches_by_models(db_path, model_keys)
