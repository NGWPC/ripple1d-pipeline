import logging
import os
import sqlite3
from typing import Type

from ..setup.database import Database

# TODO Move functions to ../setup/database.py, delete load_rating_curve?


def process_reach_db(reach_db_path: str, library_conn: sqlite3.Connection) -> None:
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


def load_rating_curve(db_path, reach_id, sub_db_path, timeout):
    """
    Inserts rating curves from sub_db_path into the central library database if sub_db_path exists.
    """
    conn = sqlite3.connect(db_path, timeout=timeout)
    try:
        if os.path.exists(sub_db_path):
            process_reach_db(sub_db_path, conn)
            try:
                os.remove(sub_db_path)
            except Exception as e:
                logging.error(f"Could not remove {sub_db_path} Error: {e}")
    finally:
        conn.close()


def load_all_rating_curves(database: Type[Database]) -> None:
    """
    Loads all rating curves from submodel databases into the central library database.
    """
    db_path = database.db_path
    db_timeout = database.timeout
    submodels_dir = database.submodels_dir

    conn = sqlite3.connect(db_path, timeout=db_timeout)
    try:
        for submodel in os.listdir(submodels_dir):
            sub_db_path = os.path.join(submodels_dir, submodel, f"{submodel}.db")
            if os.path.exists(sub_db_path):
                process_reach_db(sub_db_path, conn)
                try:
                    os.remove(sub_db_path)
                except Exception as e:
                    logging.error(f"Could not remove {sub_db_path} Error: {e}")

        logging.info("All rating curves loaded into central database")
    finally:
        conn.close()
