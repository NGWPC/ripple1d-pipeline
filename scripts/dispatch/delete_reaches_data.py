import os
import shutil
import sqlite3
from typing import List


def delete_reaches_data(
    reach_ids: List[int],
    library_dir: str,
    submodels_dir: str,
    db_location: str,
    delete_submodels: bool = False,
    delete_library: bool = False,
    delete_db_records: bool = False,
    reset_network_records: bool = False,
    reset_processing_records: bool = False,
) -> None:
    """
    Deletes or resets reach data from database, submodels, and library directories based on provided options.
    """
    # Connect to the database
    conn = sqlite3.connect(db_location)
    try:
        cursor = conn.cursor()

        # Delete records from the rating_curves table if enabled
        if delete_db_records:
            placeholders = ", ".join("?" for _ in reach_ids)
            cursor.execute(f"DELETE FROM rating_curves WHERE reach_id IN ({placeholders});", reach_ids)
            conn.commit()

        # Reset network records if enabled
        if reset_network_records:
            placeholders = ", ".join("?" for _ in reach_ids)
            cursor.execute(
                f"""
                UPDATE network
                SET updated_to_id = NULL,
                    eclipsed = NULL,
                    model_id = NULL
                WHERE reach_id IN ({placeholders});
                """,
                reach_ids,
            )
            conn.commit()

        # Reset processing records if enabled
        if reset_processing_records:
            placeholders = ", ".join("?" for _ in reach_ids)
            cursor.execute(
                f"""
                UPDATE processing
                SET extract_submodel_job_id = NULL,
                    extract_submodel_status = NULL,
                    create_ras_terrain_job_id = NULL,
                    create_ras_terrain_status = NULL,
                    create_model_run_normal_depth_job_id = NULL,
                    create_model_run_normal_depth_status = NULL,
                    run_incremental_normal_depth_job_id = NULL,
                    run_incremental_normal_depth_status = NULL,
                    run_known_wse_job_id = NULL,
                    run_known_wse_status = NULL,
                    create_fim_lib_job_id = NULL,
                    create_fim_lib_status = NULL
                WHERE reach_id IN ({placeholders});
                """,
                reach_ids,
            )
            conn.commit()
    finally:
        conn.close()

    # Delete submodel directories if enabled
    if delete_submodels:
        for reach_id in reach_ids:
            folder_path = os.path.join(submodels_dir, str(reach_id))
            if os.path.exists(folder_path):
                shutil.rmtree(folder_path)

    # Delete library directories if enabled
    if delete_library:
        for reach_id in reach_ids:
            folder_path = os.path.join(library_dir, str(reach_id))
            if os.path.exists(folder_path):
                shutil.rmtree(folder_path)


if __name__ == "__main__":
    reach_ids = []
    library_dir = r"D:\Users\abdul.siddiqui\workbench\projects\production\library"
    submodels_dir = r"D:\Users\abdul.siddiqui\workbench\projects\production\submodels"
    library_db_path = r"D:\Users\abdul.siddiqui\workbench\projects\production\library.sqlite"
    delete_submodels = False
    delete_library = False
    delete_rc_records = False
    reset_network_records = False
    reset_processing_records = False

    delete_reaches_data(
        reach_ids,
        library_dir,
        submodels_dir,
        library_db_path,
        delete_submodels,
        delete_library,
        delete_rc_records,
        reset_network_records,
        reset_processing_records,
    )
