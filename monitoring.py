import logging
import socket
import sqlite3
from datetime import datetime

dateime_obj = datetime.now()
timestamp = dateime_obj.strftime("%m-%d-%Y_%H_%M")


class MonitoringDatabase:
    """
    Monitoring database class to monitor collection processing at scale.
    """

    def __init__(self, hostname: str):
        self.ripple1d_version = "0.10.1"
        self.db_path = "Z:\\shared\\monitoring.sqlite"
        self.ip_address = socket.gethostbyname(hostname)
        self.timeout = 60

    def create_tables(self) -> None:
        """
        Initialize database and create tables
        """

        current_time = datetime.now()

        connection = sqlite3.connect(self.db_path, timeout=self.timeout)

        try:
            cursor = connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL;")

            # Create metadata table to store metadata
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS metadata (
                    ripple1d_version TEXT,
                    start_time TIMESTAMP
                );
            """
            )

            cursor.execute(
                f"""
                INSERT INTO metadata
                VALUES (?, ?);
                """,
                (self.ripple1d_version, current_time),
            )

            # Create monitoring table to aggregate collection and machine level statuses
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS instances (
                    ip_address TEXT,
                    status_update_time TIMESTAMP,
                    current_collection_id TEXT,
                    last_collection_status TEXT,
                    total_collections_processed INTEGER,
                    total_successful_collections INTEGER,
                    total_collections_submitted INTEGER,
                    PRIMARY KEY (ip_address)
                );
            """
            )
            # Create error table to store error messages
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS collections (
                    ip_address TEXT,
                    collection_id TEXT,
                    collection_start_time TIMESTAMP,
                    collection_finish_time TIMESTAMP,
                    collection_status TEXT,
                    error_message TEXT,
                    PRIMARY KEY (ip_address, collection_id),
                    FOREIGN KEY (ip_address) REFERENCES instances(ip_address)
                );
                """
            )

            connection.commit()
            logging.info(f"Database initialized successfully at {self.db_path}")
        except Exception as e:
            logging.info(e)
            connection.rollback()
        finally:
            connection.close()

    def update_instances_table(
        self,
        status_update_time,
        current_collection_id,
        last_collection_status,
        total_collections_processed,
        total_successful_collections,
        total_collections_submitted,
    ) -> None:
        """
        Enter record to instances table in monitoring database.
        """
        ip_address = self.ip_address

        conn = sqlite3.connect(self.db_path, timeout=self.timeout)

        try:
            cursor = conn.cursor()
            cursor.execute("PRAGMA journal_mode=WAL;")
            cursor.execute(
                """
                INSERT OR REPLACE INTO instances (
                    ip_address, 
                    status_update_time, 
                    current_collection_id,  
                    last_collection_status,  
                    total_collections_processed, 
                    total_successful_collections, 
                    total_collections_submitted
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ip_address,
                    status_update_time,
                    current_collection_id,
                    last_collection_status,
                    total_collections_processed,
                    total_successful_collections,
                    total_collections_submitted,
                ),
            )
            conn.commit()
            logging.info(f"Instances Table record inserted in {self.db_path}")

        finally:
            conn.close()

    def update_collections_table(
        self,
        collection_id,
        collection_start_time,
        collection_finish_time,
        collection_status,
        error_message,
    ) -> None:
        """
        Enter record to collections table in monitoring database.
        """
        ip_address = self.ip_address

        conn = sqlite3.connect(self.db_path, timeout=self.timeout)

        try:
            cursor = conn.cursor()
            cursor.execute("PRAGMA journal_mode=WAL;")
            cursor.execute(
                """
                INSERT OR REPLACE INTO collections (
                    ip_address, 
                    collection_id,
                    collection_start_time,
                    collection_finish_time,
                    collection_status,
                    error_message
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    ip_address,
                    collection_id,
                    collection_start_time,
                    collection_finish_time,
                    collection_status,
                    error_message,
                ),
            )
            conn.commit()
            logging.info(f"Errors Table record inserted in {self.db_path}")
        finally:
            conn.close()
