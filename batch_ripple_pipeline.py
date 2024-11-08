#!/usr/bin/env python3

import argparse
import os
import pathlib
import subprocess
import time
from datetime import datetime
from ripple_pipeline import *
from scripts.setup import *


def batch_pipeline(
    collection_list, poll_and_update: bool = False, kwse: bool = True, qc: bool = True
):
    """
    Iterate over each collection in a list of collections, and execute all Ripple1D setup, processing, and qc steps for each collection.

    Inputs:
        collection_list: A filepath to line seperated list of collections.
            OR a string in quotes with space delimeted collections.
        poll_and_update: Poll ripple1D API and update database after conflate model and extract submodel steps
        kwse: Run kwse or not.
        qc: Run qc steps or not.
    """

    collections = read_input(collection_list)

    for collection in collections:
        # Construct the command to execute ripple_pipeline.py
        cmd = [
            "python",
            "ripple_pipeline.py",
            "--collection", collection,
            "--poll_and_update" if poll_and_update == True else "",
            "--kwse" if kwse == False  else "",
            "--qc" if qc == False else "",
        ]

        # Set up log files
        log_dir = os.path.join(COLLECTIONS_ROOT_DIR, collection)
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, f"{collection}_log")

        # Execute the command and redirect output
        try:
            result = subprocess.run(
                cmd,
                stdout=open(log_file + ".out", "w"),
                stderr=open(log_file + ".err", "w")
            )

            if result.returncode != 0:
                raise subprocess.CalledProcessError(result.returncode, cmd)

            print(f"Collection {collection} processed successfully.")
        
        except subprocess.CalledProcessError as e:
            print(f"Error processing collection {collection}: {e}")
        except Exception as e:
            print(f"Unexpected error occurred: {e}")
            print(f"Executing run_pipeline on collection:  {collection}")

        time.sleep(5)


def read_input(collection_list):
    collections = []
    if os.path.isfile(collection_list):
        source_file_extension = pathlib.Path(collection_list).suffix
        acceptable_file_formats = [".lst", ".txt", ".csv"]
        if source_file_extension.lower() not in acceptable_file_formats:
            raise Exception(
                "Incoming file must be in .lst, .txt, or .csv format if submitting a file name and path."
            )

        with open(collection_list, "r") as collections_file:
            file_lines = collections_file.readlines()
            f_list = [strip_newline(fl) for fl in file_lines]
            collections.append(f_list)

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
        batch_ripple_pipeline.py -l "collection1 collection2 collection3"
        batch_ripple_pipeline.py -l ~/collections.lst -p -nokwse -skipqc
    """

    parser = argparse.ArgumentParser(
        description="Run ripple pipeline on each collection in the collection list"
    )

    parser.add_argument(
        "-l",
        "--collection_list",
        help=f"A filepath (.txt or .lst) containaing a list of valid collections of HEC-RAS models given in a. ",
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

    batch_pipeline(**args)
