import sqlite3

import duckdb


def read_parquet_with_duckdb(nwm_flowlines_path):
    con = duckdb.connect()
    query = f"SELECT id AS reach_id, to_id AS nwm_to_id FROM read_parquet('{nwm_flowlines_path}')"
    table = con.execute(query).fetchall()
    con.close()
    return table


def insert_table_to_sqlite(table, sqlite_db):
    conn = sqlite3.connect(sqlite_db)
    cursor = conn.cursor()

    # Prepare rows for insertion
    cursor.executemany(
        """
        INSERT INTO network (reach_id, nwm_to_id)
        VALUES (?, ?)
    """,
        table,
    )

    # Insert reaches into the processing table
    cursor.executemany(
        """
        INSERT INTO processing (reach_id)
        VALUES (?)
    """,
        [(row[0],) for row in table],
    )

    conn.commit()
    conn.close()


def populate_db(nwm_flowlines_path, sqlite_db):
    table = read_parquet_with_duckdb(nwm_flowlines_path)
    insert_table_to_sqlite(table, sqlite_db)
    print("Reaches information is populated.")


if __name__ == "__main__":
    nwm_flowlines_path = "data/nwm_flowlines.parquet"  # Update this as necessary
    sqlite_db = "data/library.sqlite"  # Update this as necessary
    populate_db(nwm_flowlines_path, sqlite_db)
