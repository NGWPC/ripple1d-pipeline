import logging
import os
import sqlite3

from ..config import DB_CONN_TIMEOUT


# to do: remove submodel
def process_reach_db(submodel: str, reach_db_path: str, library_conn: sqlite3.Connection) -> None:
    """
    Inserts rating curves from reach_db_path into the central library database.
    """
    reach_conn = sqlite3.connect(reach_db_path)
    try:
        reach_cursor = reach_conn.cursor()
        reach_cursor.execute(
            """
            SELECT reach_id, us_flow, us_depth, us_wse, ds_depth, ds_wse, boundary_condition
            FROM rating_curves
            WHERE plan_suffix IN ('nd', 'kwse') AND map_exist IS TRUE
            """
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

        reach_cursor.execute(
            """
            SELECT reach_id, us_flow, us_depth, us_wse, ds_depth, ds_wse, boundary_condition
            FROM rating_curves
            WHERE plan_suffix IN ('nd', 'kwse') AND map_exist IS FALSE
            """
        )
        rows = reach_cursor.fetchall()

        cursor.executemany(
            """
            INSERT OR IGNORE INTO rating_curves_no_map (
                reach_id, us_flow, us_depth, us_wse, ds_depth, ds_wse, boundary_condition
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        library_conn.commit()
    finally:
        reach_conn.close()


def load_rating_curve(db_path, reach_id, sub_db_path, timeout=DB_CONN_TIMEOUT):
    """Inserts rating curves from sub_db_path into the central library database if sub_db_path exists."""
    conn = sqlite3.connect(db_path, timeout=timeout)
    try:
        if os.path.exists(sub_db_path):
            process_reach_db(reach_id, sub_db_path, conn)
            os.remove(sub_db_path)
    finally:
        conn.close()


def load_all_rating_curves(submodels_dir: str, db_path: str) -> None:
    """
    Loads all rating curves from submodel databases into the central library database.
    """
    conn = sqlite3.connect(db_path, timeout=DB_CONN_TIMEOUT)
    try:

        for submodel in os.listdir(submodels_dir):
            sub_db_path = os.path.join(submodels_dir, submodel, f"{submodel}.db")
            if os.path.exists(sub_db_path):
                process_reach_db(submodel, sub_db_path, conn)
                os.remove(sub_db_path)

        logging.info("All rating curves loaded into central database")
    finally:
        conn.close()


if __name__ == "__main__":
    db_path = r"D:\Users\abdul.siddiqui\workbench\projects\test_production\library.sqlite"
    library_dir = r"D:\Users\abdul.siddiqui\workbench\projects\test_production\library"
    load_all_rating_curves(library_dir, db_path)
