import sqlite3
from typing import List, Tuple


def get_valid_reaches(db_path) -> List[Tuple[int, int]]:
    """Get reaches that are not eclipsed."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT reach_id, nwm_to_id FROM network
        WHERE eclipsed IS FALSE
    """
    )
    result = cursor.fetchall()
    conn.close()
    return result


def get_eclipsed_reaches(db_path) -> List[Tuple[int, int]]:
    """Get reaches that are eclipsed."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT reach_id, nwm_to_id FROM network
        WHERE eclipsed IS TRUE
    """
    )
    result = cursor.fetchall()
    conn.close()
    return result


def update_to_id_batch(updates: List[Tuple[int, int]], db_path) -> None:
    """Batch update the updated_to_id for multiple reaches."""

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.executemany(
        """
        UPDATE network
        SET updated_to_id = ?
        WHERE reach_id = ?
    """,
        updates,
    )
    conn.commit()
    conn.close()
    return


def update_network(db_path):
    """Build the modified network by updating updated_to_id."""
    valid_reaches = get_valid_reaches(db_path)
    eclipsed_reaches = get_eclipsed_reaches(db_path)

    valid_reaches_dict = {reach_id: nwm_to_id for reach_id, nwm_to_id in valid_reaches}
    eclipsed_reaches_dict = {reach_id: nwm_to_id for reach_id, nwm_to_id in eclipsed_reaches}

    updates = []
    for reach_id, nwm_to_id in valid_reaches:
        current_reach_id = nwm_to_id
        while current_reach_id:
            if current_reach_id in valid_reaches_dict:
                # Found a valid reach, prepare the update for updated_to_id and break the loop
                updates.append((current_reach_id, reach_id))
                break
            elif current_reach_id in eclipsed_reaches_dict:
                # Current reach is an eclipsed reach, continue to follow the nwm_to_id
                current_reach_id = eclipsed_reaches_dict[current_reach_id]
            else:
                # Reach is not in valid_reaches_dict or eclipsed_reaches_dict, break the loop
                break

    if updates:
        # Execute batch updates
        update_to_id_batch(updates, db_path)
        print(f"Updated {len(updates)} reaches successfully.")
    else:
        print("No updates to process.")


if __name__ == "__main__":
    # Paths
    db_path = r"D:\Users\abdul.siddiqui\workbench\projects\production\library.sqlite"
    update_network(db_path)
