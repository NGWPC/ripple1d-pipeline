#!/usr/bin/env python3

import os
import argparse
import logging
import datetime as dt

from scripts import *
from scripts.setup import *
from scripts.processing import *
from scripts.debug import *


def setup_logger(output_dir: str, process_name: str) -> None:
    # Set logging to file and stderr
    curr_date = dt.datetime.now().strftime("%m_%d_%Y")

    log_file_name = f"ripple_pipeline_{curr_date}.log"

    log_file_path = os.path.join(output_dir, log_file_name)

    file_handler = logging.FileHandler(log_file_path)
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s - %(levelname)s - %(module)s - %(message)s",
            "%Y-%m-%d %H:%M:%S",
        )
    )
    # * Set Log file Logging Level *
    file_handler.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    # * Set Console Logging Level *
    console_handler.setLevel(logging.INFO)

    logger = logging.getLogger()
    logger.addHandler(file_handler)
    logger.setLevel(logging.INFO)

    # Print start time
    dt_string = dt.datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    logging.info("========================================================")
    logging.info(f"\n Starting: ' {process_name} ' process from ripple_pipeline.py")
    logging.info(f"\n \t Started: {dt_string} \n")


def setup(collection):
    # PARAMETERS
    stac_collection_id = collection
    root_dir = os.path.join(COLLECTIONS_ROOT_DIR, stac_collection_id)
    db_path = os.path.join(root_dir, "ripple.gpkg")
    merged_gpkg_path = os.path.join(root_dir, "source_models", "all_rivers.gpkg")

    # SETUP LOGGER
    start_time = dt.datetime.now()
    setup_logger(root_dir, "setup")

    # CREATE FOLDER STRUCTURE
    logging.info(f"Creating folders")
    source_models_dir, submodels_dir, library_dir = create_folders(root_dir)

    # DOWNLOAD MODELS
    logging.info(f"Getting Models from STAC Catalog")
    models_data = get_models_from_stac(
        STAC_URL, stac_collection_id
    )  # or get based on some geographic unit
    logging.info(f"Downloading Models Data")
    download_models_data(models_data, source_models_dir)
    model_ids = list(models_data.keys())
    logging.info(f"Combining river tables")
    combine_river_tables(source_models_dir, models_data, merged_gpkg_path)

    # CREATE DATABASE
    logging.info(f"Filter NWM Reaches")
    filter_nwm_reaches(NWM_FLOWLINES_PATH, merged_gpkg_path, db_path)
    logging.info(f"Initializing Database")
    init_db(db_path)
    logging.info(f"Inserting Models")
    insert_models(models_data, stac_collection_id, db_path)

    end_time = dt.datetime.now()
    time_duration = end_time - start_time
    logging.info("========================================================")
    logging.info(
        f"\t Finished: ' setup ' routine from ripple_pipeline.py \n"
        f"\t \t TOTAL RUN TIME: {str(time_duration).split('.')[0]}"
    )


def process(collection, poll_and_update=False, kwse=True):
    # PARAMETERS
    stac_collection_id = collection
    stop_on_error = False
    root_dir = os.path.join(COLLECTIONS_ROOT_DIR, stac_collection_id)
    db_path = os.path.join(root_dir, "ripple.gpkg")
    source_models_dir = os.path.join(root_dir, "source_models")
    submodels_dir = os.path.join(root_dir, "submodels")
    library_dir = os.path.join(root_dir, "library")
    extent_library_dir = os.path.join(root_dir, "library_extent")
    f2f_start_file = os.path.join(root_dir, "start_reaches.csv")

    # SETUP LOGGER
    start_time = dt.datetime.now()
    setup_logger(root_dir, "process")

    # GET WORKING MODELS
    logging.info(f"Get Models From STAC..")
    models_data = get_models_from_stac(STAC_URL, stac_collection_id)
    model_ids = list(models_data.keys())

    # RUN CONFLATE_MODEL STEP
    logging.info(f"Starting Conflate Model Step...")
    succeded_models, failed_models, not_accepted_models, unknown_status_models = (
        execute_model_step(
            model_ids, "conflate_model", db_path, source_models_dir, timeout_minutes=10
        )
    )
    if (
        stop_on_error
        and (len(failed_models) + len(not_accepted_models) + len(unknown_status_models))
        > 0
    ):
        logging.exception(
            f"One or more models failed. Stopping Execution. Please address and then run below cells."
        )
        raise Exception(
            "One or more model failed. Stopping Execution. Please address and then run below cells."
        )

    if poll_and_update:
        logging.info(f"Starting Poll and Update Job Status...")
        poll_and_update_job_status(db_path, "conflate_model", "models")
        succeded_models, failed_models, not_accepted_models = (
            get_reach_status_by_process(db_path, "conflate_model", "models")
        )
    logging.info(f"Finished Conflate Model Step.")

    # LOAD CONFLATION INFORMATION TO DATABASE
    logging.info(f"Starting Load Conflation Step...")
    load_conflation(
        [
            model_id_job_id_status[0]
            for model_id_job_id_status in succeded_models + unknown_status_models
        ],
        source_models_dir,
        db_path,
    )
    logging.info(f"Finished Load Conflation Step...")

    # UPDATE NETWORK_TO_ID TABLE IN DATABASE
    logging.info(f"Starting Update Network Step...")
    update_network(db_path)
    logging.info(f"Finished Update Network Step...")

    # GET WORKING REACHES
    logging.info(f"Starting Get Reaches by Models Step...")
    reach_data = get_reaches_by_models(db_path, model_ids)
    logging.info(f"Finished Get Reaches by Models Step...")

    # RUN EXTRACT_SUBMODEL STEP
    logging.info(f"Starting Extract Submodel Step...")
    succeded_reaches, failed_reaches, not_accepted_reaches, unknown_status_reaches = (
        execute_step(
            [(data[0], data[2]) for data in reach_data],
            "extract_submodel",
            db_path,
            source_models_dir,
            submodels_dir,
            timeout_minutes=5,
        )
    )
    if (
        stop_on_error
        and (
            len(failed_reaches)
            + len(not_accepted_reaches)
            + len(unknown_status_reaches)
        )
        > 0
    ):
        logging.exception(
            f"One or more models failed. Stopping Execution. Please address and  then run below cells."
        )
        raise Exception(
            "One or more reach failed. Stopping Execution. Please address and then run below cells."
        )

    if poll_and_update:
        logging.info(f"Starting Poll and Update Job Status...")
        poll_and_update_job_status(db_path, "extract_submodel")
        succeded_models, failed_models, not_accepted_models = (
            get_reach_status_by_process(db_path, "extract_submodel")
        )
    logging.info(f"Finihsed Extract Submodel Step...")

    # RUN CREATE_RAS_TERRAIN STEP
    logging.info(f"Starting Create Ras Terrain Step...")
    succeded_reaches, failed_reaches, not_accepted_reaches, unknown_status_reaches = (
        execute_step(
            [(reach[0], "") for reach in succeded_reaches + unknown_status_reaches],
            "create_ras_terrain",
            db_path,
            source_models_dir,
            submodels_dir,
            timeout_minutes=3,
        )
    )
    if stop_on_error and (len(failed_reaches) + len(not_accepted_reaches)) > 0:
        logging.exception(
            "One or more models failed. Stopping Execution. Please address and  then run below cells."
        )
        raise Exception(
            "One or more reach failed. Stopping Execution. Please address and then run below cells."
        )
    logging.info(f"Finished Create Ras Terrain Step...")

    # RUN CREATE_MODEL_RUN_NORMAL_DEPTH STEP
    logging.info(f"Starting Create Model Run Normal Depth Step...")
    succeded_reaches, failed_reaches, not_accepted_reaches, unknown_status_reaches = (
        execute_step(
            [(reach[0], "") for reach in succeded_reaches + unknown_status_reaches],
            "create_model_run_normal_depth",
            db_path,
            source_models_dir,
            submodels_dir,
            timeout_minutes=10,
        )
    )
    if (
        stop_on_error
        and (
            len(failed_reaches)
            + len(not_accepted_reaches)
            + len(unknown_status_reaches)
        )
        > 0
    ):
        logging.exception(
            "One or more models failed. Stopping Execution. Please address and  then run below cells."
        )
        raise Exception(
            "One or more reach failed. Stopping Execution. Please address and then run below cells."
        )
    logging.info(f"Finished Create Model Run Normal Depth Step...")

    # RUN RUN_INCREMENTAL_NORMAL_DEPTH STEP
    logging.info(f"Started Run Incremental Normal Depth Step...")
    succeded_reaches, failed_reaches, not_accepted_reaches, unknown_status_reaches = (
        execute_step(
            [(reach[0], "") for reach in succeded_reaches + unknown_status_reaches],
            "run_incremental_normal_depth",
            db_path,
            source_models_dir,
            submodels_dir,
            timeout_minutes=25,
        )
    )
    if (
        stop_on_error
        and (
            len(failed_reaches)
            + len(not_accepted_reaches)
            + len(unknown_status_reaches)
        )
        > 0
    ):
        logging.exception(
            "One or more models failed. Stopping Execution. Please address and  then run below cells."
        )
        raise Exception(
            "One or more reach failed. Stopping Execution. Please address and then run below cells."
        )
    logging.info(f"Finished Run Incremental Normal Depth Step...")

    # KWSE STEPS
    if kwse:
        # Initial run_known_wse and Initial create_rating_curves_db
        logging.info(
            f"Starting Initial run_known_wse and Initial create_rating_curves_db Steps (execute_ikwse_for_network)..."
        )
        outlet_reaches = [data[0] for data in reach_data if data[1] is None]
        execute_ikwse_for_network(
            [(reach, None) for reach in outlet_reaches],
            submodels_dir,
            db_path,
            False,
            20,
        )
        logging.info(
            f"Completed Initial run_known_wse and Initial create_rating_curves_db steps (execute_ikwse_for_network)..."
        )

        # Final run_known_wse step
        logging.info(f"Starting Final execute_kwse_step...")
        kwse_reach_data = [
            (data[0], data[1])
            for data in reach_data
            if data[1] is not None
            and data[0]
            in [reach[0] for reach in succeded_reaches + unknown_status_reaches]
        ]

        (
            succeded_reaches_kwse,
            failed_reaches_kwse,
            not_accepted_reaches_kwse,
            unknown_status_reaches_kwse,
        ) = execute_kwse_step(kwse_reach_data, db_path, submodels_dir, 180)
        if (
            stop_on_error
            and (
                len(failed_reaches_kwse)
                + len(not_accepted_reaches_kwse)
                + len(unknown_status_reaches_kwse)
            )
            > 0
        ):
            logging.exception(
                "One or more models failed. Stopping Execution. Please address and  then run below cells."
            )
            raise Exception(
                "One or more reach failed. Stopping Execution. Please address and then run below cells."
            )
        logging.info(f"Finished Final execute_kwse_step...")
    else:
        # Optional if KWSE is not performed
        logging.info(f"Starting create_fim_lib Step...")
        (
            succeded_reaches,
            failed_reaches,
            not_accepted_reaches,
            unknown_status_reaches,
        ) = execute_step(
            [(reach[0], "") for reach in succeded_reaches + unknown_status_reaches],
            "create_fim_lib",
            db_path,
            source_models_dir,
            submodels_dir,
            library_dir,
            timeout_minutes=20,
        )
        if (
            stop_on_error
            and (
                len(failed_reaches)
                + len(not_accepted_reaches)
                + len(unknown_status_reaches)
            )
            > 0
        ):
            logging.exception(
                "One or more models failed. Stopping Execution. Please address and  then run below cells."
            )
            raise Exception(
                "One or more reach failed. Stopping Execution. Please address and then run below cells."
            )
        outlet_reaches = [data[0] for data in reach_data if data[1] is None]
        logging.info(f"Finished create_fim_lib Step...")

    # FINAL CREATE RATING CURVES DB STEP
    logging.info(f"Starting Final create_rating_curves_db Step...")
    (
        succeded_reaches_rcdb,
        failed_reaches_rcdb,
        not_accepted_reaches_rcdb,
        unknown_status_reaches_rcdb,
    ) = execute_step(
        [(reach[0], "") for reach in succeded_reaches + unknown_status_reaches],
        "create_rating_curves_db",
        db_path,
        source_models_dir,
        submodels_dir,
        timeout_minutes=15,
    )
    if (
        stop_on_error
        and (
            len(failed_reaches_rcdb)
            + len(not_accepted_reaches_rcdb)
            + len(unknown_status_reaches_rcdb)
        )
        > 0
    ):
        logging.exception(
            "One or more models failed. Stopping Execution. Please address and  then run below cells."
        )
        raise Exception(
            "One or more reach failed. Stopping Execution. Please address and then run below cells."
        )
    logging.info(f"Finished Final create_rating_curves_db Step...")

    # MERGE RATING CURVES
    logging.info(f"Starting Merge Rating Curves Step...")
    load_all_rating_curves(submodels_dir, db_path)
    logging.info(f"Finished Merge Rating Curves Step...")

    # CREATE FIM LIBRARY STEP
    logging.info(f"Starting create_fim_lib Step")
    (
        succeded_reaches_fim_lib,
        failed_reaches_fim_lib,
        not_accepted_reaches_fim_lib,
        unknown_status_reaches_fim_lib,
    ) = execute_step(
        [(reach[0], "") for reach in succeded_reaches + unknown_status_reaches],
        "create_fim_lib",
        db_path,
        source_models_dir,
        submodels_dir,
        library_dir,
        timeout_minutes=15,
    )
    if (
        stop_on_error
        and (
            len(failed_reaches_fim_lib)
            + len(not_accepted_reaches_fim_lib)
            + len(unknown_status_reaches_fim_lib)
        )
        > 0
    ):
        logging.exception(
            "One or more models failed. Stopping Execution. Please address and  then run below cells."
        )
        raise Exception(
            "One or more reach failed. Stopping Execution. Please address and then run below cells."
        )
    logging.info(f"Finished create_fim_lib Step")

    # CREATE EXTENT LIBRARY
    logging.info(f"Starting create extent library Step")
    create_extent_lib(library_dir, extent_library_dir, submodels_dir)
    logging.info(f"Finished create extent library Step")

    # CREATE FLOWS2FIM START REACHES FILE
    logging.info(f"Starting create f2f start file Step")
    outlet_reaches = [data[0] for data in reach_data if data[1] is None]
    create_f2f_start_file(outlet_reaches, f2f_start_file)
    logging.info(f"Finished create f2f start file Step")

    end_time = dt.datetime.now()
    time_duration = end_time - start_time
    logging.info("========================================================")
    logging.info(
        f"\t Finished: ' process ' routine from ripple_pipeline.py \n"
        f"\t \t TOTAL RUN TIME: {str(time_duration).split('.')[0]}"
    )


def run_qc(collection, poll_and_update=False):
    # PARAMETERS
    stac_collection_id = collection
    root_dir = os.path.join(COLLECTIONS_ROOT_DIR, stac_collection_id)
    db_path = os.path.join(root_dir, "ripple.gpkg")
    library_dir = os.path.join(root_dir, "library")
    error_report_path = os.path.join(root_dir, "error_report.xlsx")
    f2f_start_file = os.path.join(root_dir, "start_reaches.csv")

    # SETUP LOGGER
    start_time = dt.datetime.now()
    setup_logger(root_dir, "qc")

    # CREATE EXCEL ERROR REPORT
    dfs = []
    for process_name in ["conflate_model"]:

        if poll_and_update:
            poll_and_update_job_status(db_path, process_name, "models")

        _, failed_reaches, _ = get_reach_status_by_process(
            db_path, process_name, "models"
        )
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
        if poll_and_update:
            poll_and_update_job_status(db_path, process_name)
        _, failed_reaches, _ = get_reach_status_by_process(db_path, process_name)
        df = get_failed_jobs_df(failed_reaches)
        dfs.append(df)
        write_failed_jobs_df_to_excel(df, process_name, error_report_path)

    # CREATE QC MAP
    copy_qc_map(root_dir)

    # CREATE COMPOSITE RASTERS
    run_flows2fim(root_dir, "qc", library_dir, db_path, start_file=f2f_start_file)

    end_time = dt.datetime.now()
    time_duration = end_time - start_time
    logging.info("========================================================")
    logging.info(
        f"\t Finished: ' qc ' routine from ripple_pipeline.py \n"
        f"\t \t TOTAL RUN TIME: {str(time_duration).split('.')[0]}"
    )


def run_pipeline(
    collection: str, poll_and_update: bool = False, kwse: bool = True, qc: bool = True
):
    """
    Automate the execution of all steps in setup.ipynb, process.ipynb, and qc.ipynb.
    """

    setup(collection)

    process(collection, poll_and_update, kwse)

    if qc:
        run_qc(collection, poll_and_update)


if __name__ == "__main__":
    """
    Sample Usage:
        ripple_pipeline.py -c ble_12100302_Medina -p -nokwse -skipqc
    """

    parser = argparse.ArgumentParser(
        description="Run ripple pipeline steps on one collection"
    )

    parser.add_argument(
        "-c",
        "--collection",
        help=f"A valid collection of HEC-RAS models. The collection will initially be pulled "
        "locally from the provided STAC URL (in config.py). "
        "https://radiantearth.github.io/stac-browser/#/external/stac2.dewberryanalytics.com/?.language=en ",
        required=True,
    )
    parser.add_argument(
        "-p",
        "--poll_and_update",
        help=f"OPTIONAL: provide the -p flag to Utilize the poll_and_update_job_status and get_reach_status_by_process functions to update the database. ",
        required=False,
        action="store_true",
    )
    parser.add_argument(
        "-nokwse",
        "--kwse",
        help=f"OPTIONAL: provide the -nokwse argument to skip the KWSE step, and use create_fim_lib API to Ripple1D instead. ",
        required=False,
        action="store_false",
    )
    parser.add_argument(
        "-skipqc",
        "--qc",
        help=f"OPTIONAL: provide the -skipqc flag to skip the automated quality control steps. ",
        required=False,
        action="store_false",
    )
    args = vars(parser.parse_args())

    run_pipeline(**args)
