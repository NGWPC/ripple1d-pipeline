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
            SELECT reach_id, us_flow, us_depth, us_wse, ds_depth, ds_wse, boundary_condition, xs_overtopped
            FROM rating_curves
            WHERE plan_suffix IN ('nd', 'kwse') AND map_exist IS TRUE
            """
        )
        reach_db_rcs = reach_cursor.fetchall()

        if not reach_db_rcs:
            return

        cursor = library_conn.cursor()
        cursor.executemany(
            """
            INSERT OR IGNORE INTO rating_curves (
                reach_id, us_flow, us_depth, us_wse, ds_depth, ds_wse, boundary_condition
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [rc[:7] for rc in reach_db_rcs],
        )

        placeholders = ", ".join(["(?, ?, ?, ?)"] * len(reach_db_rcs))

        # Get mapping
        cursor.execute(
            f"""
            SELECT reach_id, us_flow, ds_wse, boundary_condition, id
            FROM rating_curves
            WHERE (reach_id, us_flow, ds_wse, boundary_condition) IN (
                VALUES {placeholders}
            )
        """,
            [param for rc in reach_db_rcs for param in (rc[0], rc[1], rc[5], rc[6])],
        )

        rc_id_map = {(row[0], row[1], row[2], row[3]): row[4] for row in cursor.fetchall()}

        # Prepare metrics data using mapped IDs
        metrics_data = [(rc_id_map[(rc[0], rc[1], rc[5], rc[6])], rc[7]) for rc in reach_db_rcs if rc[7] is not None]

        # Bulk insert metrics
        cursor.executemany(
            """
            INSERT OR REPLACE INTO rating_curves_metrics
            (rc_id, xs_overtopped) VALUES (?, ?)
        """,
            metrics_data,
        )

        reach_cursor.execute(
            """
            SELECT reach_id, us_flow, us_depth, us_wse, ds_depth, ds_wse, boundary_condition, xs_overtopped
            FROM rating_curves
            WHERE plan_suffix IN ('nd', 'kwse') AND map_exist IS FALSE
            """
        )
        rows = reach_cursor.fetchall()

        cursor.executemany(
            """
            INSERT OR IGNORE INTO rating_curves_no_map (
                reach_id, us_flow, us_depth, us_wse, ds_depth, ds_wse, boundary_condition, xs_overtopped
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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
