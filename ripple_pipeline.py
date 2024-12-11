import argparse
import logging
import os
import sys
from datetime import datetime as dt

# Import necessary modules
from scripts import *
from scripts.setup import *
from scripts.process import *
from scripts.qc import *

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
    models = STACImporter(collection)
    logging.info("Downloading models from STAC catalog")
    models.get_models_from_stac()
    models.download_models_data()

    combine_river_tables(models.models_data, collection)

    logging.info("Filtering NWM reaches")
    filter_nwm_reaches(collection)

    logging.info("Initializing database")
    Database.init_db(collection)

    logging.info("Inserting models into database")
    Database.insert_models(models.models_data, collection)


def process(collection_name):
    """Process the data."""
    logging.info("Starting processing >>>>>>>>")
    # Instantiate CollectionData, Database, JobClient objects
    collection = CollectionData(collection_name)
    database = Database(collection)
    jobclient = JobClient(collection)

    logging.info("Getting models succesfully pulled from STAC")
    model_ids = collection.get_models()

    # TODO - Create a @dataclass for model_job_status & reach_job_status
    logging.info("Starting Conflate Model Step >>>>>>")
    conflate_model_batch = ConflateModelBatchProcessor(collection, model_ids) #, jobclient, database)
    conflate_model_batch.conflate_model_batch_process(jobclient, database)
    logging.info("<<<<<<Finished Conflate Model Step")

    logging.info("Starting Load Conflation Step >>>>>>")
    succeeded_and_unknown_status_models = [
        model_id[0]
        for model_id in conflate_model_batch.succeeded + conflate_model_batch.unknown
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

    # Instantiate reach_processor
    reach_step_processor = ReachStepProcessor(collection, reach_data)

    logging.info("Starting Extract Submodel Step >>>>>>")
    reach_step_processor.execute_extract_submodel_process(jobclient, database, timeout=5)
    logging.info("<<<<<< Finihsed Extract Submodel Step")

    logging.info("Starting Create Ras Terrain Step >>>>>>")
    reach_step_processor.execute_process(jobclient, database, "create_ras_terrain", timeout=3)
    logging.info("<<<<<< Finished Create Ras Terrain Step")

    logging.info("Starting Create Model Run Normal Depth Step  >>>>>>>>")
    reach_step_processor.execute_process(jobclient, database, "create_model_run_normal_depth", timeout=10)
    logging.info("<<<<<< Finished Create Model Run Normal Depth Step")

    logging.info("<<<<< Started Run Incremental Normal Depth Step")
    reach_step_processor.execute_process(jobclient, database, "run_incremental_normal_depth", timeout=25)
    logging.info("<<<<< Finished Run Incremental Normal Depth Step")

    logging.info("Starting Initial run_known_wse and Initial create_rating_curves_db Steps>>>>>>")
    outlet_reaches = [data[0] for data in reaches if data[1] is None]
    execute_ikwse_for_network(
        [(reach, None) for reach in outlet_reaches],
        collection,
        database,
        jobclient,
        False,
        timeout=20,
    )
    logging.info("<<<<< Completed Initial run_known_wse and Initial create_rating_curves_db steps")

    logging.info("Starting Final execute_kwse_step >>>>>>")
    kwse_reach_data = [
        (data[0], data[1])
        for data in reaches
        if data[1] is not None
        and data[0]
        in [reach[0] for reach in reach_step_processor.succesful_and_unknown_reaches]
    ]

    kwse_step_processor = KWSEStepProcessor(collection, kwse_reach_data)
    kwse_step_processor.execute_kwse_step(jobclient, database, "run_known_wse", timeout=180)
    logging.info("<<<<< Finished Final execute_kwse_step")

    logging.info("Starting Final create_rating_curves_db Step >>>>>>")
    reach_step_processor.execute_process(jobclient, database, "create_rating_curves_db", timeout=15)
    logging.info("<<<<< Finished Final create_rating_curves_db Step")

    logging.info("Starting Merge Rating Curves Step >>>>>>")
    load_all_rating_curves(database)
    logging.info("<<<<< Finished Merge Rating Curves Step")

    logging.info("Starting create_fim_lib Step >>>>>>")
    reach_step_processor.execute_process(jobclient, database, "create_fim_lib", timeout=30)
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
        # logging.info(f"Getting job status for {process_name} ")
        # Capture the final status of unknown status jobs
        job_client.poll_and_update_job_status(database, process_name, "models")
        # logging.info("Finished poll_and_update_job_status")
        _, _, failed_reaches = database.get_reach_status_by_process(process_name, "models")
        # logging.info("Finished database.get_reach_status_by_process")
        df = job_client.get_failed_jobs_df(failed_reaches)
        # logging.info("Finished get_failed_jobs_df")
        dfs.append(df)
        write_failed_jobs_df_to_excel(df, process_name, collection.error_report_path)
        # logging.info("Finished write_failed_jobs_df_to_excel")

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
        # logging.info(f"Getting job status for: {process_name}")
        # Capture the final status of unknown status jobs
        job_client.poll_and_update_job_status(database, process_name)
        # logging.info("Finished poll_and_update_job_status")
        _, _, failed_reaches = database.get_reach_status_by_process(process_name)
        # logging.info("Finished database.get_reach_status_by_process")
        df = job_client.get_failed_jobs_df(failed_reaches)
        # logging.info("Finished get_failed_jobs_df")
        dfs.append(df)
        write_failed_jobs_df_to_excel(df, process_name, collection.error_report_path)
        # logging.info("Finished write_failed_jobs_df_to_excel")

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
    args = vars(parser.parse_args())

    run_pipeline(**args)
