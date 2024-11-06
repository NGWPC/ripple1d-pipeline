#!/usr/bin/env python3

import os
import argparse
import logging
from datetime import datetime

from scripts import *
from scripts.setup import *
from scripts.processing import *
from scripts.debug import *


def setup(collection):
    # PARAMETERS
    stac_collection_id = collection
    root_dir = os.path.join(COLLECTIONS_ROOT_DIR, stac_collection_id)
    db_path = os.path.join(root_dir, "ripple.gpkg")
    merged_gpkg_path = os.path.join(root_dir, "source_models", "all_rivers.gpkg")
    source_models_dir = os.path.join(root_dir, "source_models")

    # CREATE FOLDER STRUCTURE
    source_models_dir, submodels_dir, library_dir = create_folders(root_dir)

    # DOWNLOAD MODELS
    models_data = get_models_from_stac(
        STAC_URL, stac_collection_id
    )  # or get based on some geographic unit
    download_models_data(models_data, source_models_dir)
    model_ids = list(models_data.keys())
    combine_river_tables(source_models_dir, models_data, merged_gpkg_path)

    # CREATE DATABASE
    filter_nwm_reaches(NWM_FLOWLINES_PATH, merged_gpkg_path, db_path)
    init_db(db_path)
    insert_models(models_data, stac_collection_id, db_path)


def process(collection, poll_and_update=False, kwse=True):
    # PARAMETERS
    stac_collection_id = collection
    root_dir = os.path.join(COLLECTIONS_ROOT_DIR, stac_collection_id)
    db_path = os.path.join(root_dir, "ripple.gpkg")
    stop_on_error = False
    source_models_dir = os.path.join(root_dir, "source_models")
    submodels_dir = os.path.join(root_dir, "submodels")
    library_dir = os.path.join(root_dir, "library")
    extent_library_dir = os.path.join(root_dir, "library_extent")
    f2f_start_file = os.path.join(root_dir, "start_reaches.csv")

    # GET WORKING MODELS
    models_data = get_models_from_stac(STAC_URL, stac_collection_id)
    model_ids = list(models_data.keys())

    # RUN CONFLATE_MODEL
    succeded_models, failed_models, not_accepted_models, unknown_status_models = (
        execute_model_step(model_ids, "conflate_model", db_path, source_models_dir, 10)
    )
    if (
        stop_on_error
        and (len(failed_models) + len(not_accepted_models) + len(unknown_status_models))
        > 0
    ):
        raise Exception(
            "One or more model failed. Stopping Execution. Please address and then run below cells."
        )

    if poll_and_update:
        poll_and_update_job_status(db_path, "conflate_model", "models")
        succeded_models, failed_models, not_accepted_models = (
            get_reach_status_by_process(db_path, "conflate_model", "models")
        )

    # LOAD CONFLATION INFORMATION TO DATABASE
    load_conflation(
        [
            model_id_job_id_status[0]
            for model_id_job_id_status in succeded_models + unknown_status_models
        ],
        source_models_dir,
        db_path,
    )

    # UPDATE NETWORK_TO_ID TABLE IN DATABASE
    update_network(db_path)

    # GET WORKING REACHES
    reach_data = get_reaches_by_models(db_path, model_ids)

    # RUN EXTRACT_SUBMODEL
    succeded_reaches, failed_reaches, not_accepted_reaches, unknown_status_reaches = (
        execute_step(
            [(data[0], data[2]) for data in reach_data],
            "extract_submodel",
            db_path,
            source_models_dir,
            submodels_dir,
            5,
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
        raise Exception(
            "One or more reach failed. Stopping Execution. Please address and then run below cells."
        )

    if poll_and_update:
        poll_and_update_job_status(db_path, "extract_submodel")
        succeded_models, failed_models, not_accepted_models = (
            get_reach_status_by_process(db_path, "extract_submodel")
        )

    # RUN CREATE_RAS_TERRAIN
    succeded_reaches, failed_reaches, not_accepted_reaches, unknown_status_reaches = (
        execute_step(
            [(reach[0], "") for reach in succeded_reaches + unknown_status_reaches],
            "create_ras_terrain",
            db_path,
            source_models_dir,
            submodels_dir,
            3,
        )
    )
    if stop_on_error and (len(failed_reaches) + len(not_accepted_reaches)) > 0:
        raise Exception(
            "One or more reach failed. Stopping Execution. Please address and then run below cells."
        )

    # RUN CREATE_MODEL_RUN_NORMAL_DEPTH
    succeded_reaches, failed_reaches, not_accepted_reaches, unknown_status_reaches = (
        execute_step(
            [(reach[0], "") for reach in succeded_reaches + unknown_status_reaches],
            "create_model_run_normal_depth",
            db_path,
            source_models_dir,
            submodels_dir,
            10,
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
        raise Exception(
            "One or more reach failed. Stopping Execution. Please address and then run below cells."
        )

    # RUN RUN_INCREMENTAL_NORMAL_DEPTH
    succeded_reaches, failed_reaches, not_accepted_reaches, unknown_status_reaches = (
        execute_step(
            [(reach[0], "") for reach in succeded_reaches + unknown_status_reaches],
            "run_incremental_normal_depth",
            db_path,
            source_models_dir,
            submodels_dir,
            15,
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
        raise Exception(
            "One or more reach failed. Stopping Execution. Please address and then run below cells."
        )

    # RUN RUN_KNOWN_WSE FOR NETWORK
    if kwse:
        outlet_reaches = [data[0] for data in reach_data if data[1] is None]
        execute_kwse_for_network(
            [(reach, None) for reach in outlet_reaches],
            submodels_dir,
            db_path,
            True,
            library_dir,
            False,
        )
    else:
        # Optional if KWSE is not performed
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
            raise Exception(
                "One or more reach failed. Stopping Execution. Please address and then run below cells."
            )
        outlet_reaches = [data[0] for data in reach_data if data[1] is None]

    # CREATE EXTENT LIBRARY
    create_extent_lib(library_dir, extent_library_dir, submodels_dir)

    # MERGE RATING CURVES
    load_all_rating_curves(library_dir, db_path)

    # CREATE FLOWS2FIM START REACHES FILE
    create_f2f_start_file(outlet_reaches, f2f_start_file)


def run_qc(
    collection,
    poll_and_update=False,
):
    # PARAMETERS
    stac_collection_id = collection
    root_dir = os.path.join(COLLECTIONS_ROOT_DIR, stac_collection_id)
    db_path = os.path.join(root_dir, "ripple.gpkg")
    library_dir = os.path.join(root_dir, "library")
    error_report_path = os.path.join(root_dir, "error_report.xlsx")
    f2f_start_file = os.path.join(root_dir, "start_reaches.csv")

    # CREATE EXCEL ERROR REPORTS
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
        "run_known_wse",
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

    # CREATE COMPOSITION RASTERS
    run_flows2fim(root_dir, "qc", library_dir, db_path, start_file=f2f_start_file)


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
