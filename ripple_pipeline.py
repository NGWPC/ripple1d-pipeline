import argparse
import logging
import os
import sys
from datetime import datetime as dt

# Import necessary modules
from scripts import *
from scripts.debug import *
from scripts.processing import *
from scripts.process import *
from scripts.setup import *

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")


def setup(collection):
    """Setup the resources."""
    logging.info(f"Setting up the resources for collection: {collection}")
    stac_collection_id = collection
    root_dir = os.path.join(COLLECTIONS_ROOT_DIR, stac_collection_id)
    db_path = os.path.join(root_dir, "ripple.gpkg")
    merged_gpkg_path = os.path.join(root_dir, "source_models", "all_rivers.gpkg")

    logging.info(f"Creating folder structure in {root_dir}")
    source_models_dir, submodels_dir, library_dir = create_folders(root_dir)

    logging.info("Downloading models from STAC catalog")
    models_data = get_models_from_stac(STAC_URL, stac_collection_id)
    download_models_data(models_data, source_models_dir)
    model_ids = list(models_data.keys())
    combine_river_tables(source_models_dir, models_data, merged_gpkg_path)

    logging.info("Filtering NWM reaches")
    filter_nwm_reaches(NWM_FLOWLINES_PATH, merged_gpkg_path, db_path)
    logging.info("Initializing database")
    init_db(db_path)
    logging.info("Inserting models into database")
    insert_models(models_data, stac_collection_id, db_path)


def process(collection):
    """Process the data."""
    logging.info("Starting processing >>>>>>>>")
    stac_collection_id = collection
    stop_on_error = False
    root_dir = os.path.join(COLLECTIONS_ROOT_DIR, stac_collection_id)
    db_path = os.path.join(root_dir, "ripple.gpkg")
    source_models_dir = os.path.join(root_dir, "source_models")
    submodels_dir = os.path.join(root_dir, "submodels")
    library_dir = os.path.join(root_dir, "library")
    extent_library_dir = os.path.join(root_dir, "library_extent")
    f2f_start_file = os.path.join(root_dir, "start_reaches.csv")

    logging.info("Getting models from STAC")
    models_data = get_models_from_stac(STAC_URL, stac_collection_id)
    model_ids = list(models_data.keys())

    logging.info("Starting Conflate Model Step >>>>>>")
    succeded_models, failed_models, not_accepted_models, unknown_status_models = execute_model_step(
        model_ids, "conflate_model", db_path, source_models_dir, timeout_minutes=10
    )
    logging.info("<<<<<<Finished Conflate Model Step")

    logging.info("Starting Load Conflation Step >>>>>>")
    load_conflation(
        [model_id_job_id_status[0] for model_id_job_id_status in succeded_models + unknown_status_models],
        source_models_dir,
        db_path,
    )
    logging.info("Finished Load Conflation Step")

    logging.info("Starting Update Network Step >>>>>>")
    update_network(db_path)
    logging.info("<<<<<< Finished Update Network Step")

    logging.info("Starting Get Reaches by Models Step >>>>>>")
    reach_data = get_reaches_by_models(db_path, model_ids)
    logging.info("<<<<<< Finished Get Reaches by Models Step")

    logging.info("Starting Extract Submodel Step >>>>>>")
    succeded_reaches, failed_reaches, not_accepted_reaches, unknown_status_reaches = execute_step(
        [(data[0], data[2]) for data in reach_data],
        "extract_submodel",
        db_path,
        source_models_dir,
        submodels_dir,
        timeout_minutes=5,
    )
    logging.info("<<<<<< Finihsed Extract Submodel Step")

    logging.info("Starting Create Ras Terrain Step >>>>>>")
    succeded_reaches, failed_reaches, not_accepted_reaches, unknown_status_reaches = execute_step(
        [(reach[0], "") for reach in succeded_reaches + unknown_status_reaches],
        "create_ras_terrain",
        db_path,
        source_models_dir,
        submodels_dir,
        timeout_minutes=3,
    )
    logging.info("<<<<<< Finished Create Ras Terrain Step")

    logging.info("Starting Create Model Run Normal Depth Step  >>>>>>>>")
    succeded_reaches, failed_reaches, not_accepted_reaches, unknown_status_reaches = execute_step(
        [(reach[0], "") for reach in succeded_reaches + unknown_status_reaches],
        "create_model_run_normal_depth",
        db_path,
        source_models_dir,
        submodels_dir,
        timeout_minutes=10,
    )
    logging.info("<<<<<< Finished Create Model Run Normal Depth Step")

    logging.info("<<<<< Started Run Incremental Normal Depth Step")
    succeded_reaches, failed_reaches, not_accepted_reaches, unknown_status_reaches = execute_step(
        [(reach[0], "") for reach in succeded_reaches + unknown_status_reaches],
        "run_incremental_normal_depth",
        db_path,
        source_models_dir,
        submodels_dir,
        timeout_minutes=25,
    )
    logging.info("<<<<< Finished Run Incremental Normal Depth Step")

    logging.info("Starting Initial run_known_wse and Initial create_rating_curves_db Steps>>>>>>")
    outlet_reaches = [data[0] for data in reach_data if data[1] is None]
    execute_ikwse_for_network([(reach, None) for reach in outlet_reaches], submodels_dir, db_path, False, 20)
    logging.info("<<<<< Completed Initial run_known_wse and Initial create_rating_curves_db steps")

    logging.info("Starting Final execute_kwse_Step >>>>>>")
    kwse_reach_data = [
        (data[0], data[1])
        for data in reach_data
        if data[1] is not None and data[0] in [reach[0] for reach in succeded_reaches + unknown_status_reaches]
    ]
    succeded_reaches_kwse, failed_reaches_kwse, not_accepted_reaches_kwse, unknown_status_reaches_kwse = (
        execute_kwse_step(kwse_reach_data, db_path, submodels_dir, 180)
    )
    logging.info("<<<<< Finished Final execute_kwse_step")

    logging.info("Starting Final create_rating_curves_db Step >>>>>>")
    succeded_reaches_rcdb, failed_reaches_rcdb, not_accepted_reaches_rcdb, unknown_status_reaches_rcdb = execute_step(
        [(reach[0], "") for reach in succeded_reaches + unknown_status_reaches],
        "create_rating_curves_db",
        db_path,
        source_models_dir,
        submodels_dir,
        timeout_minutes=15,
    )
    logging.info("<<<<< Finished Final create_rating_curves_db Step")

    logging.info("Starting Merge Rating Curves Step >>>>>>")
    load_all_rating_curves(submodels_dir, db_path)
    logging.info("<<<<< Finished Merge Rating Curves Step")

    logging.info("Starting create_fim_lib Step >>>>>>")
    succeded_reaches_fim_lib, failed_reaches_fim_lib, not_accepted_reaches_fim_lib, unknown_status_reaches_fim_lib = (
        execute_step(
            [(reach[0], "") for reach in succeded_reaches + unknown_status_reaches],
            "create_fim_lib",
            db_path,
            source_models_dir,
            submodels_dir,
            library_dir,
            timeout_minutes=30,
        )
    )
    logging.info("<<<<< Finished create_fim_lib Step")

    try:
        logging.info("Starting create extent library Step >>>>>>")
        create_extent_lib(library_dir, extent_library_dir, submodels_dir)
        logging.info("<<<<< Finished create extent library Step")
    except:
        logging.error("Error - create extent library step failed")

    try:
        logging.info("Starting create f2f start file Step >>>>>>")
        outlet_reaches = [data[0] for data in reach_data if data[1] is None]
        create_f2f_start_file(outlet_reaches, f2f_start_file)
        logging.info("<<<<< Finished create f2f start file Step")
    except:
        logging.error("Error - create f2f start file step failed")


def run_qc(collection):
    """Perform quality control."""
    logging.info("Starting QC")
    stac_collection_id = collection
    root_dir = os.path.join(COLLECTIONS_ROOT_DIR, stac_collection_id)
    db_path = os.path.join(root_dir, "ripple.gpkg")
    library_dir = os.path.join(root_dir, "library")
    error_report_path = os.path.join(root_dir, "error_report.xlsx")
    f2f_start_file = os.path.join(root_dir, "start_reaches.csv")

    logging.info("Creating Excel Error Report >>>>>>>>")
    dfs = []
    for process_name in ["conflate_model"]:
        poll_and_update_job_status(db_path, process_name, "models")  # this captures final status of unknown status jobs
        _, failed_reaches, _ = get_reach_status_by_process(db_path, process_name, "models")
        df = get_failed_jobs_df(failed_reaches)
        dfs.append(df)
        write_failed_jobs_df_to_excel(df, process_name, error_report_path)

    dfs = []
    for process_name in [
        "extract_submodel",
        "create_ras_terrain",
        "create_model_run_normal_depth",
        "run_incremental_normal_depth",
        "run_iknown_wse",
        "create_irating_curves_db",
        "run_known_wse",
        "create_rating_curves_db",
        "create_fim_lib",
    ]:
        poll_and_update_job_status(db_path, process_name)  # this captures final status of unknown status jobs
        _, failed_reaches, _ = get_reach_status_by_process(db_path, process_name)
        df = get_failed_jobs_df(failed_reaches)
        dfs.append(df)
        write_failed_jobs_df_to_excel(df, process_name, error_report_path)

    logging.info("<<<<< Finished creating Excel error report")

    logging.info("Running copy_qc_map step >>>>>")
    copy_qc_map(root_dir)
    logging.info("<<<<< Finished copy_qc_map step")

    logging.info("Starting run_flows2fim step >>>>>>")
    run_flows2fim(root_dir, "qc", library_dir, db_path, start_file=f2f_start_file)
    logging.info("<<<<< Finished run_flows2fim step")


def run_pipeline(collection: str):
    """
    Automate the execution of all steps in setup.ipynb, process.ipynb, and qc.ipynb.
    """

    setup(collection)
    process(
        collection,
    )
    try:
        run_qc(collection)
    except:
        logging.error("Error - qc workflow failed")


if __name__ == "__main__":
    """
    Sample Usage:
        python ripple_pipeline.py -c ble_12100302_Medina
    """

    parser = argparse.ArgumentParser(description="Run ripple pipeline steps on one collection")

    parser.add_argument(
        "-c",
        "--collection",
        help=f"A valid collection of HEC-RAS models. The collection will initially be pulled "
        "locally from the provided STAC URL (in config.py). "
        "https://radiantearth.github.io/stac-browser/#/external/stac2.dewberryanalytics.com/?.language=en ",
        required=True,
    )
    args = vars(parser.parse_args())

    run_pipeline(**args)
