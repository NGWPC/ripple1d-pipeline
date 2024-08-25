import os
import subprocess


def run_flows2fim(
    output_dir: str,
    output_subfolder: str,
    library_path: str,
    library_db_path: str,
    flow_files_dir: str,
    start_csv: str,
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
        start_csv (str): Path to the start reaches CSV file.
        fim_format (str): Output format for FIM files ('tif' or 'vrt').
    """
    # Ensure the output subfolder exists
    output_subfolder_path = os.path.join(output_dir, output_subfolder)
    if not os.path.exists(output_subfolder_path):
        os.mkdir(output_subfolder_path)

    # Process each flow file in the flow_files_dir
    for flow_file in os.listdir(flow_files_dir):
        if flow_file.endswith(".csv"):
            flow_file_path = os.path.join(flow_files_dir, flow_file)
            basename = os.path.splitext(flow_file)[0]
            control_csv = os.path.join(output_subfolder_path, f"{basename.replace('flows', 'controls')}.csv")
            fim_output = os.path.join(output_subfolder_path, f"{basename.replace('flows', 'fim')}.{fim_format}")

            # Generate control CSV
            cmd_controls = [
                "flows2fim.exe",
                "controls",
                "-db",
                library_db_path,
                "-f",
                flow_file_path,
                "-c",
                control_csv,
                "-scsv",
                start_csv,
            ]
            subprocess.run(cmd_controls, shell=True, check=True)

            # Generate FIM output
            cmd_fim = [
                "flows2fim.exe",
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

    print("All flow files have been processed.")


if __name__ == "__main__":
    # Paths specific to the current run
    output_dir = r"D:\Users\abdul.siddiqui\workbench\projects\production\scenarios"
    output_subfolder = "wfsj_12040101"

    library_path = r"D:\Users\abdul.siddiqui\workbench\projects\production\library"
    library_db_path = r"D:/Users/abdul.siddiqui/workbench/projects/production/library.sqlite"
    flow_files_dir = r"D:\Users\abdul.siddiqui\workbench\projects\production\scenarios\flow_files"
    start_csv = r"D:\Users\abdul.siddiqui\workbench\projects\wfsj_huc8\startReaches.csv"

    # Execute the process for flow files
    run_flows2fim(
        output_dir,
        output_subfolder,
        library_path,
        library_db_path,
        flow_files_dir,
        start_csv,
    )
