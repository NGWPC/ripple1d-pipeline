import sqlite3

from ..config import DB_CONN_TIMEOUT


def init_db(db_path):
    connection = sqlite3.connect(db_path, timeout=DB_CONN_TIMEOUT)
    try:
        cursor = connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL;")

        # # Create models table to store model-level information
        # cursor.execute(
        #     """
        #     CREATE TABLE IF NOT EXISTS models (
        #         model_id INTEGER PRIMARY KEY AUTOINCREMENT,
        #         model_key TEXT UNIQUE NOT NULL
        #     );
        # """
        # )

        # Create network table to store network relationships
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS network (
                reach_id INTEGER PRIMARY KEY,
                nwm_to_id INTEGER,
                updated_to_id INTEGER
            );
        """
        )

        # Indexes will speed up where downstream = queries
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS network_nwm_to_id_idx ON network (nwm_to_id);
        """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS network_updated_to_id_idx ON network (updated_to_id);
        """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS rating_curves (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                reach_id INTEGER,
                us_flow INTEGER,
                us_depth REAL,
                us_wse REAL,
                ds_depth REAL,
                ds_wse REAL,
                boundary_condition TEXT CHECK(boundary_condition IN ('nd','kwse')) NOT NULL,
                UNIQUE(reach_id, us_flow, ds_wse, boundary_condition)
            );
        """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS rating_curves_reach_id ON rating_curves (reach_id);
        """
        )

        # Create processing table to store reach-specific processing information
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS processing (
                reach_id INTEGER PRIMARY KEY,
                -- model_id INTEGER,
                model_key TEXT,
                eclipsed BOOL CHECK(eclipsed IN (0, 1)),
                extract_submodel_job_id TEXT,
                extract_submodel_status TEXT,
                create_ras_terrain_job_id TEXT,
                create_ras_terrain_status TEXT,
                create_model_run_normal_depth_job_id TEXT,
                create_model_run_normal_depth_status TEXT,
                run_incremental_normal_depth_job_id TEXT,
                run_incremental_normal_depth_status TEXT,
                run_known_wse_job_id TEXT,
                run_known_wse_status TEXT,
                create_fim_lib_job_id TEXT,
                create_fim_lib_status TEXT
                -- FOREIGN KEY (model_id) REFERENCES models (model_id)
            );
        """
        )

        # # Create metrics table to store reach-specific metrics
        # cursor.execute(
        #     """
        #     CREATE TABLE IF NOT EXISTS metrics (
        #         reach_id INTEGER PRIMARY KEY,
        #         conflation_metric REAL,
        #         xs_floods_metric REAL,
        #         FOREIGN KEY (reach_id) REFERENCES processing (reach_id)
        #     );
        # """
        # )

        connection.commit()
    finally:
        connection.close()
    print(f"Database initialized successfully at {db_path}")
