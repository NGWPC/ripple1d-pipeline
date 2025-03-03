#!/usr/bin/env python3
import sys
import argparse
import logging
import os
import pathlib
import subprocess
import yaml
from pathlib import Path
from datetime import datetime

from ripple_pipeline import *
from src.setup import *

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

def load_config(config_file):
    try:
        with open(str(Path.cwd() / "src" / config_file), 'r') as file:
            config = yaml.safe_load(file)
    except FileNotFoundError:
        raise ValueError(f"File '{config_file}' not found. Ensure config.yaml is in the src directory.")
    except yaml.YAMLError:
        raise ValueError("Invalid YAML configuration")
    
    return config

def s3_move(s3_upload_prefix: str, collection: str, col_root_dir: str, ripple1d_version:str = None, failed: bool = False):


    if failed:
        dateime_obj = datetime.now()
        timestamp = dateime_obj.strftime("%m-%d-%Y_%H_%M")
        s3_mv_command = [
            "aws",
            "s3",
            "mv",
            f"{col_root_dir}/{collection}",
            f"{s3_upload_prefix}/{collection}_{ripple1d_version}_{timestamp}",
            "--recursive",
        ]
    else:
        s3_mv_command = [
            "aws",
            "s3",
            "mv",
            f"{col_root_dir}/{collection}",
            f"{s3_upload_prefix}/{collection}",
            "--recursive",
        ]

    subprocess.Popen(
        s3_mv_command, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL
    )
    logging.info(f"Submitted S3 mv command on collection: {collection} ...")


def batch_pipeline(collection_list):
    """
    Iterate over each collection in a list of collections, and execute all Ripple1D setup, processing, and qc steps for each collection.

    Inputs:
        collection_list: A filepath to line separated list of collections.
            OR a string in quotes with space delimeted collections.
    """

    config = load_config("config.yaml")

    COLLECTIONS_ROOT_DIR = config['paths']['COLLECTIONS_ROOT_DIR']
    S3_UPLOAD_PREFIX = config['paths']['S3_UPLOAD_PREFIX']
    S3_UPLOAD_FAILED_PREFIX = config['paths']['S3_UPLOAD_FAILED_PREFIX']
    RIPPLE1D_VERSION = config['RIPPLE1D_VERSION']

    collections = read_input(collection_list)

    for collection in collections:
        logging.info(f"Starting processing for collection: {collection} ...")
        # Construct the command to execute ripple_pipeline.py
        cmd = [
            "python",
            "ripple_pipeline.py",
            "--collection",
            collection,
        ]

        # Set up log files
        log_dir = os.path.join(COLLECTIONS_ROOT_DIR, collection)
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, f"{collection}.log")

        with open(log_file, "a") as f:
            f.write("************************************************************************")
            f.write(f"\n--- Starting processing for collection: {collection} ---\n")
            f.flush()

            try:
                
                process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                for line in process.stdout:
                    print(line.decode().strip())
                    f.write(line.decode())
                for line in process.stderr:
                    print(line.decode().strip())
                    f.write(line.decode())
                process.wait()


                if process.returncode != 0:
                    raise subprocess.CalledProcessError(process.returncode, cmd)

                logging.info(f"Collection {collection} processed successfully.")

                s3_move(S3_UPLOAD_PREFIX, collection, COLLECTIONS_ROOT_DIR)

            except subprocess.CalledProcessError as e:

                s3_move(S3_UPLOAD_FAILED_PREFIX, collection, COLLECTIONS_ROOT_DIR, RIPPLE1D_VERSION, True)

                logging.error(f"Error processing collection {collection}: {e} ")
                logging.info(f"See {log_file} for more details.")

            except Exception as e:

                s3_move(S3_UPLOAD_FAILED_PREFIX, collection, COLLECTIONS_ROOT_DIR, RIPPLE1D_VERSION, True)

                logging.error(f"Unexpected error occurred: {e}")
                logging.error(f"Executing run_pipeline on collection: {collection}")
                logging.info(f"See {log_file} for more details.")


def read_input(collection_list):
    collections = []
    if os.path.isfile(collection_list):
        source_file_extension = pathlib.Path(collection_list).suffix
        acceptable_file_formats = [".lst", ".txt", ".csv"]
        if source_file_extension.lower() not in acceptable_file_formats:
            raise Exception("Incoming file must be in .lst, .txt, or .csv format if submitting a file name and path.")

        with open(collection_list, "r") as collections_file:
            file_lines = collections_file.readlines()
            collections = [strip_newline(fl) for fl in file_lines]

    elif isinstance(collection_list, str):
        collection_list = collection_list.split()
        for collection in collection_list:
            collections.append(collection)

    else:
        raise Exception(
            "collection_list not a valid filepath, or a space seperated list of collections passed within quotes"
        )

    return collections


def strip_newline(collection):
    # Strips single or double quotes
    collection = collection.strip().replace('"', "")
    collection = collection.replace("'", "")
    return collection


if __name__ == "__main__":
    """
    Sample Usage:
        python batch_ripple_pipeline.py -l "collection1 collection2 collection3"
        python batch_ripple_pipeline.py -l ~/collections.lst
    """

    parser = argparse.ArgumentParser(description="Run ripple pipeline on each collection in the collection list")

    parser.add_argument(
        "-l",
        "--collection_list",
        help=f"A filepath (.txt or .lst) containaing a new line separated list of valid collections or a space separated string of collections",
        required=True,
    )

    args = vars(parser.parse_args())

    batch_pipeline(**args)
