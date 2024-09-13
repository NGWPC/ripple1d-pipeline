import os
import subprocess
from typing import List

from ..config import (
    FLOW_FILES_DIR,
    FLOWS2FIM_BIN_PATH,
    GDAL_BINS_PATH,
    GDAL_SCRIPTS_PATH,
)


def setup_gdal_environment():
    """
    Add GDAL binaries to the system PATH
    """

    if GDAL_BINS_PATH:
        # Add GDAL path to the system PATH
        os.environ["PATH"] = GDAL_BINS_PATH + os.pathsep + os.environ["PATH"]

    if GDAL_SCRIPTS_PATH:
        os.environ["PATH"] = GDAL_SCRIPTS_PATH + os.pathsep + os.environ["PATH"]


def run_flows2fim(
    output_dir: str,
    output_subfolder: str,
    library_path: str,
    library_db_path: str,
    start_reaches: List,
    flow_files_dir: str = FLOW_FILES_DIR,
    fim_format: str = "tif",
) -> None:
    """
    Processes flow files by generating control CSVs and FIM outputs.

    Args:
        output_dir (str): Directory where the output subfolder is located.
        output_subfolder (str): Subfolder where outputs will be placed.
        library_path (str): Path to the library directory.
        library_db_path (str): Path to the SQLite library database.
        flow_files_dir (str): Directory containing flow files (CSV format).
        fim_format (str): Output format for FIM files ('tif' or 'vrt').
    """
    setup_gdal_environment()

    output_subfolder_path = os.path.join(output_dir, output_subfolder)
    if not os.path.exists(output_subfolder_path):
        os.makedirs(output_subfolder_path)

    for flow_file in os.listdir(flow_files_dir):
        if flow_file.endswith(".csv"):
            flow_file_path = os.path.join(flow_files_dir, flow_file)
            basename = os.path.splitext(flow_file)[0]
            control_csv = os.path.join(output_subfolder_path, f"{basename.replace('flows', 'controls')}.csv")
            fim_output = os.path.join(output_subfolder_path, f"{basename.replace('flows', 'fim')}.{fim_format}")

            # Generate control CSV
            cmd_controls = [
                FLOWS2FIM_BIN_PATH,
                "controls",
                "-db",
                library_db_path,
                "-f",
                flow_file_path,
                "-o",
                control_csv,
                "-sids",
                ",".join([str(reach) for reach in start_reaches]),
            ]
            subprocess.run(cmd_controls, shell=True, check=True)

            # Generate FIM output
            cmd_fim = [
                FLOWS2FIM_BIN_PATH,
                "fim",
                "-lib",
                library_path,
                "-c",
                control_csv,
                "-o",
                fim_output,
                "-fmt",
                fim_format,
            ]
            subprocess.run(cmd_fim, shell=True, check=True)

            print(basename, "have been processed.")


if __name__ == "__main__":
    output_dir = r"Z:\collections\ebfe-12030106_EastForkTrinity"
    output_subfolder = "qc"

    library_path = r"Z:\collections\ebfe-12030106_EastForkTrinity\library"
    library_db_path = r"Z:\collections\ebfe-12030106_EastForkTrinity\ripple.db"

    # run_flows2fim(...)
