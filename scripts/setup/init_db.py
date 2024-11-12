import logging
import sqlite3
from typing import Dict

import geopandas as gpd

from ..config import (
    DB_CONN_TIMEOUT,
    DS_DEPTH_INCREMENT,
    RIPPLE1D_VERSION,
    US_DEPTH_INCREMENT,
)


def filter_nwm_reaches(nwm_flowlines_path: str, river_gpkg_path: str, output_gpkg_path: str) -> None:
    """
    Filters NWM flowlines that intersect with the convex hull of the River table from the GPKG file
    and saves the result to a new GPKG file.

    Args:
        nwm_flowlines_path (str): Path to the NWM flowlines parquet file.
        river_gpkg_path (str): Path to the GPKG file containing the River table.
        output_gpkg_path (str): Path to save the filtered NWM flowlines in a GPKG file.
    """
    # Load NWM Flowlines from Parquet file
    nwm_flowlines_gdf = gpd.read_parquet(nwm_flowlines_path, columns=["id", "to_id", "geom"])

    # Load the River table from the GPKG file
    river_gdf = gpd.read_file(river_gpkg_path, layer="River")

    # Ensure both GeoDataFrames have the same CRS (coordinate reference system)
    if nwm_flowlines_gdf.crs != river_gdf.crs:
        nwm_flowlines_gdf = nwm_flowlines_gdf.to_crs(river_gdf.crs)

    # Calculate the convex hull of the entire river geometry
    river_convex_hull = river_gdf.unary_union.convex_hull

    # Filter NWM flowlines by intersecting with the convex hull of the river
    filtered_nwm_gdf = nwm_flowlines_gdf[nwm_flowlines_gdf.intersects(river_convex_hull)]

    # Rename columns
    filtered_nwm_gdf = filtered_nwm_gdf.rename(columns={"id": "reach_id", "to_id": "nwm_to_id"})

    # Save the filtered NWM flowlines to a new GeoPackage (GPKG) file
    filtered_nwm_gdf.to_file(output_gpkg_path, layer="reaches", driver="GPKG")

    logging.info(f"Subset NWM flowlines written to reaches table {output_gpkg_path}")


def init_db(db_path):
    """Initialize database and created tables"""
    connection = sqlite3.connect(db_path, timeout=DB_CONN_TIMEOUT)
    try:
        cursor = connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL;")

        # Create models table to store model-level information
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                ripple1d_version TEXT,
                us_depth_increment REAL,
                ds_depth_increment REAL
            );
        """
        )

        cursor.execute(
            f"""
            INSERT INTO metadata
            VALUES (?, ?, ?);
            """,
            (RIPPLE1D_VERSION, US_DEPTH_INCREMENT, DS_DEPTH_INCREMENT),
        )

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
            CREATE TABLE IF NOT EXISTS rating_curves_no_map (
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
                run_iknown_wse_job_id TEXT,
                run_iknown_wse_status TEXT,
                create_irating_curves_db_job_id TEXT,
                create_irating_curves_db_status TEXT,
                run_known_wse_job_id TEXT,
                run_known_wse_status TEXT,
                create_rating_curves_db_job_id TEXT,
                create_rating_curves_db_status TEXT,
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
        logging.info(f"Database initialized successfully at {db_path}")
    except Exception as e:
        logging.info(e)
        connection.rollback()
    finally:
        connection.close()


def insert_models(models_data: Dict, collection_id, db_path: str) -> None:
    """ """
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
        logging.info(f"Models record inserted at {db_path}")
    finally:
        conn.close()
