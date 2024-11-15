#!/usr/bin/env python3

import argparse
import os
import pathlib
import subprocess
import time
from datetime import datetime

from ripple_pipeline import *
from scripts.config import COLLECTIONS_ROOT_DIR, S3_UPLOAD_PREFIX
from scripts.setup import *


def batch_pipeline(collection_list):
    """
    Iterate over each collection in a list of collections, and execute all Ripple1D setup, processing, and qc steps for each collection.

    Inputs:
        collection_list: A filepath to line separated list of collections.
            OR a string in quotes with space delimeted collections.
    """

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
                # Using shell=True to call this subprocess within the venv context
                # stdout is only being flushed at the end, not sure why
                process = subprocess.run(cmd, shell=True, stdout=f, stderr=f)

                if process.returncode != 0:
                    raise subprocess.CalledProcessError(process.returncode, cmd)

                logging.info(f"Collection {collection} processed successfully.")

                s3_mv_command = [
                    "aws",
                    "s3",
                    "mv",
                    f"{COLLECTIONS_ROOT_DIR}/{collection}",
                    f"{S3_UPLOAD_PREFIX}/{collection}",
                    "--recursive",
                ]

                subprocess.Popen(s3_mv_command, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
                logging.info(f"Submitted S3 mv command on collection: {collection} ...")

            except subprocess.CalledProcessError as e:
                logging.error(f"Error processing collection {collection}: {e} ")
                logging.info(f"See {log_file} for more details.")
            except Exception as e:
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
