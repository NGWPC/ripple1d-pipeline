import os
import sqlite3


def process_reach_db(submodel: str, reach_db_path: str, library_conn: sqlite3.Connection) -> None:
    """
    Processes a reach database and inserts rating curves into the central library database.
    """
    reach_conn = sqlite3.connect(reach_db_path)

    reach_cursor = reach_conn.cursor()
    reach_cursor.execute(
        "SELECT reach_id, us_flow, us_depth, us_wse, ds_depth, ds_wse, boundary_condition FROM rating_curves"
    )
    rows = reach_cursor.fetchall()

    cursor = library_conn.cursor()
    cursor.executemany(
        """
        INSERT OR IGNORE INTO rating_curves (
            reach_id, us_flow, us_depth, us_wse, ds_depth, ds_wse, boundary_condition
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    library_conn.commit()
    reach_conn.close()


def load_all_rating_curves(library_dir: str, db_path: str) -> None:
    """
    Loads all rating curves from submodel databases into the central library database.
    """
    conn = sqlite3.connect(db_path)

    for submodel in os.listdir(library_dir):
        sub_db_path = os.path.join(library_dir, submodel, f"{submodel}.db")
        if os.path.exists(sub_db_path):
            process_reach_db(submodel, sub_db_path, conn)
            os.remove(sub_db_path)

    print("All rating curves loaded into central database")
    conn.close()


if __name__ == "__main__":
    db_path = r"D:\Users\abdul.siddiqui\workbench\projects\test_production\library.sqlite"
    library_dir = r"D:\Users\abdul.siddiqui\workbench\projects\test_production\library"
    load_all_rating_curves(library_dir, db_path)
