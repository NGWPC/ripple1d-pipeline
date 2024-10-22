import geopandas as gpd
import requests
import sqlite3
import time

from typing import Type, Dict, List, Tuple
from .collection_data import CollectionData

class Database:
    """
    Main database class to hold all Database methods (SQL Queries).
    """
    def __init__(self, collection: Type[CollectionData]):
        self.filepath = collection.db_path
        self.stac_collection_id = collection.stac_collection_id
        self.source_models_dir = collection.source_models_dir
        self.DEFAULT_POLL_WAIT = collection.config['polling']['DEFAULT_POLL_WAIT']
        self.DB_CONN_TIMEOUT = collection.config['database']['DB_CONN_TIMEOUT']
        # self.RIPPLE1D_API_URL = collection.config['urls']['RIPPLE1D_API_URL']

    @staticmethod
    def init_db(collection : Type[CollectionData]) -> None:
        """
        Initialize database and create tables
        """
        DB_CONN_TIMEOUT = collection.config['database']['DB_CONN_TIMEOUT']
        db_path = collection.db_path

        connection = sqlite3.connect(db_path, timeout=DB_CONN_TIMEOUT)
        try:
            cursor = connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL;")

            # Create models table to store model-level information
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS models (
                    collection_id TEXT,
                    model_id TEXT,
                    model_name TEXT,
                    conflate_model_job_id TEXT,
                    conflate_model_status TEXT,
                    PRIMARY KEY (collection_id, model_id)
                );
            """
            )

            # Create network table to store network relationships
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS network (
                    reach_id INTEGER PRIMARY KEY,
                    nwm_to_id INTEGER,
                    updated_to_id INTEGER,
                    FOREIGN KEY (reach_id) REFERENCES reaches (reach_id)
                );
                """
            )

            cursor.execute(
                """
                INSERT INTO network (reach_id, nwm_to_id)
                SELECT reach_id, nwm_to_id FROM reaches;
                """
            )

            # Indexes will speed up 'where downstream = ?' queries
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
                    FOREIGN KEY (reach_id) REFERENCES reaches (reach_id),
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
                    collection_id TEXT,
                    model_id TEXT,
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
                    create_fim_lib_status TEXT,
                    FOREIGN KEY (collection_id, model_id) REFERENCES models (collection_id, model_id),
                    FOREIGN KEY (reach_id) REFERENCES reaches (reach_id)
                );
            """
            )

            cursor.execute(
                """
                INSERT INTO processing (reach_id)
                SELECT reach_id FROM network;
                """
            )

            # # Create metrics table to store reach-specific metrics
            # cursor.execute(
            #     """
            #     CREATE TABLE IF NOT EXISTS metrics (
            #         reach_id INTEGER PRIMARY KEY,
            #         xs_centerline_offset_mean INTEGER,
            #         xs_thalweg_offset_mean INTEGER,
            #         xs_centerline_offset_max INTEGER,
            #         xs_thalweg_offset_max INTEGER,
            #         coverage_start REAL,
            #         coverage_end REAL,
            #         network_to_ras_ratio REAL,
            #         FOREIGN KEY (reach_id) REFERENCES reaches (reach_id)
            #     );
            # """
            # )

            connection.commit()
            print(f"Database initialized successfully at {db_path}")
        except Exception as e:
            print(e)
            connection.rollback()
        finally:
            connection.close()

    @staticmethod
    def insert_models(models_data: Dict, collection: Type[CollectionData]) -> None:
        """ 
        """
        collection_id = collection.stac_collection_id
        db_path = collection.db_path
        DB_CONN_TIMEOUT = collection.config['database']['DB_CONN_TIMEOUT']

        rows = [(collection_id, id, model_data["model_name"]) for id, model_data in models_data.items()]
        conn = sqlite3.connect(db_path, timeout=DB_CONN_TIMEOUT)
        try:
            cursor = conn.cursor()
            cursor.executemany(
                """
                INSERT OR IGNORE INTO models (collection_id, model_id, model_name)
                VALUES (?, ?, ?)
                """,
                rows,
            )
            conn.commit()
            print(f"Models record inserted at {db_path}")
        finally:
            conn.close()
    