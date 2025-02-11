#!/usr/bin/env python3

import argparse
import logging

# Import necessary modules
from src.process import *
from src.qc import *
from src.setup import *

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def setup(collection_name):
    """Setup the resources."""
    logging.info(f"Setting up the resources for collection: {collection_name}")

    # Instantiate CollectionData
    collection = CollectionData(collection_name)
    logging.info(f"Creating folder structure in {collection.root_dir}")
    collection.create_folders()

    # Instantiate STACImporter
    # models = STACImporter(collection)
    # logging.info("Downloading models from STAC catalog")
    # models.get_models_from_stac()
    # models.download_models_data()

    combine_river_tables({"Baxter": {"model_name": "Baxter"}}, collection)

    logging.info("Filtering NWM reaches")
    filter_nwm_reaches(collection)

    logging.info("Initializing database")
    Database.init_db(collection)

    logging.info("Inserting models into database")
    Database.insert_models({"Baxter": {"model_name": "Baxter"}}, collection)


def process(collection_name):
    """Process the data."""
    logging.info("Starting processing >>>>>>>>")
    # Instantiate CollectionData, Database, JobClient objects
    collection = CollectionData(collection_name)
    database = Database(collection)
    jobclient = JobClient(collection)

    model_ids = collection.get_models()
    logging.info(f"{len(model_ids)} unique models identified")

    # TODO - Create a @dataclass for model_job_status & reach_job_status
    logging.info("Starting Conflate Model Step >>>>>>")
    conflate_step_processor = ConflateModelStepProcessor(collection, model_ids)
    conflate_step_processor.execute_step(jobclient, database, timeout=5)
    logging.info("<<<<<<Finished Conflate Model Step")

    logging.info("Starting Load Conflation Step >>>>>>")
    succeeded_and_unknown_status_models = [
        job_status[0]
        for job_status in conflate_step_processor.job_statuses["succeeded"]
        + conflate_step_processor.job_statuses["unknown"]
    ]
    load_conflation(succeeded_and_unknown_status_models, database)
    logging.info("Finished Load Conflation Step")

    logging.info("Starting Update Network Step >>>>>>")
    update_network(database)
    logging.info("<<<<<< Finished Update Network Step")

    logging.info("Starting Get Reaches by Models Step >>>>>>")
    reaches = database.get_reaches_by_models(model_ids)
    reach_data = [(data[0], data[2]) for data in reaches]
    logging.info("<<<<<< Finished Get Reaches by Models Step")

    # Reach Steps

    logging.info("Starting Extract Submodel Step >>>>>>")
    submodel_step_processor = GenericReachStepProcessor(collection, reach_data, "extract_submodel")
    submodel_step_processor.execute_step(jobclient, database, timeout=5)
    logging.info("<<<<<< Finihsed Extract Submodel Step")

    logging.info("Starting Create Ras Terrain Step >>>>>>")
    valid_reaches = [
        (job_status[0], "")
        for job_status in submodel_step_processor.job_statuses["succeeded"]
        + submodel_step_processor.job_statuses["unknown"]
    ]
    terrain_step_processor = GenericReachStepProcessor(collection, valid_reaches, "create_ras_terrain")
    terrain_step_processor.execute_step(jobclient, database, timeout=3)
    logging.info("<<<<<< Finished Create Ras Terrain Step")

    logging.info("Starting Create Model Run Normal Depth Step  >>>>>>>>")
    valid_reaches = [
        (job_status[0], "")
        for job_status in terrain_step_processor.job_statuses["succeeded"]
        + terrain_step_processor.job_statuses["unknown"]
    ]
    create_model_step_processor = GenericReachStepProcessor(collection, valid_reaches, "create_model_run_normal_depth")
    create_model_step_processor.execute_step(jobclient, database, timeout=10)
    logging.info("<<<<<< Finished Create Model Run Normal Depth Step")

    logging.info("<<<<< Started Run Incremental Normal Depth Step")
    valid_reaches = [
        (job_status[0], "")
        for job_status in create_model_step_processor.job_statuses["succeeded"]
        + create_model_step_processor.job_statuses["unknown"]
    ]
    nd_step_processor = GenericReachStepProcessor(collection, reach_data, "run_incremental_normal_depth")
    nd_step_processor.execute_step(jobclient, database, timeout=25)
    logging.info("<<<<< Finished Run Incremental Normal Depth Step")

    logging.info("Starting Initial run_known_wse and Initial create_rating_curves_db Steps>>>>>>")
    outlet_reaches = [data[0] for data in reaches if data[1] is None]
    execute_ikwse_for_network(
        [(reach, None) for reach in outlet_reaches],
        collection,
        database,
        jobclient,
        timeout=20,
    )
    logging.info("<<<<< Completed Initial run_known_wse and Initial create_rating_curves_db steps")

    logging.info("Starting Final execute_kwse_step >>>>>>")
    valid_reaches = [
        (data[0], data[1])
        for data in reaches
        if data[1] is not None
        and data[0]
        in [
            job_status[0]
            for job_status in nd_step_processor.job_statuses["succeeded"] + nd_step_processor.job_statuses["unknown"]
            # to do: also check if ind successful for downstream reach, and that ikwse was successful for this reach
        ]
    ]

    kwse_step_processor = KWSEStepProcessor(collection, valid_reaches)
    kwse_step_processor.execute_step(jobclient, database, timeout=180)
    logging.info("<<<<< Finished Final execute_kwse_step")

    logging.info("Starting Final create_rating_curves_db Step >>>>>>")
    valid_reaches = [
        job_status[0]
        for job_status in kwse_step_processor.job_statuses["succeeded"] + kwse_step_processor.job_statuses["unknown"]
    ]
    rc_step_processor = GenericReachStepProcessor(collection, reach_data, "create_rating_curves_db")
    rc_step_processor.execute_step(jobclient, database, timeout=15)
    logging.info("<<<<< Finished Final create_rating_curves_db Step")

    logging.info("Starting Merge Rating Curves Step >>>>>>")
    load_all_rating_curves(database)
    logging.info("<<<<< Finished Merge Rating Curves Step")

    logging.info("Starting create_fim_lib Step >>>>>>")
    valid_reaches = [
        job_status[0]
        for job_status in nd_step_processor.job_statuses["succeeded"] + nd_step_processor.job_statuses["unknown"]
    ]
    fimlib_step_processor = GenericReachStepProcessor(collection, reach_data, "create_fim_lib")
    fimlib_step_processor.execute_step(jobclient, database, timeout=150)
    logging.info("<<<<< Finished create_fim_lib Step")

    try:
        logging.info("Starting create extent library Step >>>>>>")
        create_extent_lib(collection)
        logging.info("<<<<< Finished create extent library Step")
    except:
        logging.error("Error - create extent library step failed")

    try:
        logging.info("Creating f2f start file >>>>>>")
        outlet_reaches = [data[0] for data in reaches if data[1] is None]
        create_f2f_start_file(outlet_reaches, collection.f2f_start_file)
        logging.info("<<<<< Created f2f start file")
    except:
        logging.error("Error - unable to create f2f start file")


def run_qc(collection_name):
    """Perform quality control."""
    logging.info("Starting QC")
    collection = CollectionData(collection_name)
    database = Database(collection)
    job_client = JobClient(collection)

    logging.info("Creating Excel Error Report >>>>>>>>")
    dfs = []
    for process_name in ["conflate_model"]:
        # Capture the final status of unknown status jobs
        job_client.poll_and_update_job_status(database, process_name, "models")
        _, _, failed_reaches = database.get_reach_status_by_process(process_name, "models")
        df = job_client.get_failed_jobs_df(failed_reaches)
        dfs.append(df)
        write_failed_jobs_df_to_excel(df, process_name, collection.error_report_path)

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
        # Capture the final status of unknown status jobs
        job_client.poll_and_update_job_status(database, process_name)
        _, _, failed_reaches = database.get_reach_status_by_process(process_name)
        df = job_client.get_failed_jobs_df(failed_reaches)
        dfs.append(df)
        write_failed_jobs_df_to_excel(df, process_name, collection.error_report_path)

    logging.info("<<<<< Finished creating Excel error report")

    logging.info("Running copy_qc_map step >>>>>")
    copy_qc_map(collection)
    logging.info("<<<<< Finished copy_qc_map step")

    logging.info("Starting run_flows2fim step >>>>>>")
    run_flows2fim(collection)
    logging.info("<<<<< Finished run_flows2fim step")


def run_pipeline(collection: str):
    """
    Automate the execution of all steps previously in setup.ipynb, process.ipynb, and qc.ipynb.
    """

    setup(collection)
    process(collection)

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
