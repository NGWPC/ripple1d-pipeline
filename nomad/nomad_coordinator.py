#!/usr/bin/env python3
# import nomad
import json
import requests
import argparse
import os
import time
from typing import Dict, Any, Set, List
from urllib.parse import urlparse
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

from rich.console import Console
from rich.table import Table
from rich.panel import Panel

import subprocess


class NomadCoordinator:
    def __init__(self):
        self.load_dotenv("./.env")
        self.headers = {
            "Content-Type": "application/json",
            "X-Nomad-Token": self.NOMAD_TOKEN,
        }
        self.console = Console()
        self.error_console = Console(stderr=True, style="bold red")

    def load_dotenv(self, dotenv_file):
        # Construct path to .env in grandparent directory
        grandparent_dir = Path(__file__).resolve().parent.parent
        dotenv_path = grandparent_dir / ".env"
        if os.path.isfile(dotenv_path):
            try:
                load_dotenv(dotenv_path, override=True)
                self.NOMAD_TOKEN = os.getenv("NOMAD_TOKEN")
                self.nomad_addr = os.getenv("NOMAD_ADDR")
                self.AWS_PROFILE = os.getenv("AWS_PROFILE")
                self.aws_access_key_id = os.getenv("aws_access_key_id")
                self.aws_secret_access_key = os.getenv("aws_secret_access_key")
                self.aws_region = os.getenv("aws_region")                
                #Remove after new AMI
                self.gitlab_un =  os.getenv("GITLAB_USERNAME")
                self.gitlab_pat =  os.getenv("GITLAB_PAT")
                
            except:
                raise ValueError("Invalid .env configuration")
        else:
            raise ValueError("Invalid .env configuration")

    def register_job(self, job_file):
        """Register a job to Nomad. The nomad CLI is used
        since .hcl filed are "easily" ported to valid JSON for an API request"""
        # https://discuss.hashicorp.com/t/how-nomad-uses-curl-api-to-create-job-through-hcl-file/43988
        # TODO
        # Potentially parse nomad.hcl file into json and read for payload to use HTTP request?
        #     jq -Rsc '{ JobHCL: ., Canonicalize: true }' job_file > payload.json
        try:
            # Build CLI command
            register_job_cmd = ["nomad", "job", "run", job_file]
            # Execute register_job_cmd with no output redirection
            register = subprocess.run(register_job_cmd, check=True)

        except subprocess.CalledProcessError as e:
            self.error_console.print(f"Error Registering job: {e} ")
            exit(1)
        except Exception as e:
            self.error_console.print(f"Unexpected error occurred: {e}")
            self.error_console.print(f" Error registering job: {register}")
            exit(1)

        # If job submission successful, exit script
        self.console.print("Submitted job for registration")
        exit(0)

    def dispatch_job(self, job_name, job_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Submit a parameterized job to Nomad."""
        url = f"{self.nomad_addr}/v1/job/{job_name}/dispatch"
        dispatch_payload = {"Meta": job_metadata}
        self.console.print(
            f"Submitting job with metadata:\n{json.dumps(job_metadata, indent=2)}"
        )
        response = requests.post(url, json=dispatch_payload, headers=self.headers)
        response.raise_for_status()
        return response.json()

    #FIXME This function, and -m flag to this script are not working
    def monitor_job(self, job_prefix: str, max_retries: int = 5, retry_delay: int = 5):
        """Monitor job events using Nomad's event stream with automatic reconnection.

        Args:
            job_prefix: The job prefix to filter events for
            max_retries: Maximum number of reconnection attempts (-1 for infinite)
            retry_delay: Seconds to wait between reconnection attempts
        """
        uri = f"{self.nomad_addr}/v1/event/stream"

        # Add topic filters for job-related events
        # FIXME 'params' below does not work!!
        params = {
            "topic": [
                f"job:{job_prefix}*",  # Job events for our job
                "Evaluation",  # Evaluation events
                "Allocation",  # Allocation events
                "Deployment",  # Deployment events
            ]
        }

        retry_count = 0
        last_index = 0  # Track the last event index we've seen

        while True:
            try:
                self.console.print(
                    Panel(f"[yellow]Connecting to Nomad event stream at {uri}")
                )

                # Add index parameter to resume from last seen event
                if last_index > 0:
                    params["index"] = last_index

                with requests.get(
                    uri, params=params, stream=True, timeout=90
                ) as response:
                    response.raise_for_status()
                    self.console.print("[green]Connected to event stream")
                    retry_count = 0  # Reset retry count on successful connection

                    # Process the ndjson stream line by line
                    for line in response.iter_lines():
                        if line:
                            try:
                                message = json.loads(line)
                                # The Index could be at the message level or within Events
                                if "Index" in message:
                                    last_index = message["Index"]

                                # Handle both single events and event arrays
                                events = message.get("Events", [message])
                                for event in events:
                                    if "Index" in event:
                                        last_index = max(last_index, event["Index"])
                                    self._pretty_print_event(event)
                            except json.JSONDecodeError as e:
                                self.console.print(f"[red]Error decoding event: {e}")
                                continue

            except (
                requests.exceptions.RequestException,
                requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
            ) as e:
                self.console.print(f"[red]Error in event stream: {e}")

                if max_retries != -1 and retry_count >= max_retries:
                    self.console.print("[bold red]Max retry attempts reached. Exiting.")
                    break

                retry_count += 1
                self.console.print(
                    f"[yellow]Retrying connection in {retry_delay} seconds... (Attempt {retry_count})"
                )
                time.sleep(retry_delay)
                continue

            except KeyboardInterrupt:
                self.console.print(
                    "[yellow]Received interrupt signal. Shutting down gracefully..."
                )
                break

    def _pretty_print_event(self, event: Dict):
        """Pretty print job-related events."""
        # Create a rich table for the event
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Field", style="cyan")
        table.add_column("Value", style="yellow")

        # Add basic event info
        table.add_row(
            "Time",
            datetime.fromtimestamp(event.get("Index", 0) / 1000).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        )
        table.add_row("Topic", event.get("Topic", "N/A"))
        table.add_row("Type", event.get("Type", "N/A"))

        # Add event-specific fields based on topic
        payload = event.get("Payload", {})

        if event.get("Topic") == "Job":
            job = payload.get("Job", {})
            table.add_row("Job ID", job.get("ID", "N/A"))
            table.add_row("Status", job.get("Status", "N/A"))

        elif event.get("Topic") == "Evaluation":
            eval_data = payload.get("Evaluation", {})
            table.add_row("Eval ID", eval_data.get("ID", "N/A"))
            table.add_row("Status", eval_data.get("Status", "N/A"))
            table.add_row("Type", eval_data.get("Type", "N/A"))

        elif event.get("Topic") == "Allocation":
            alloc = payload.get("Allocation", {})
            table.add_row("Alloc ID", alloc.get("ID", "N/A"))
            table.add_row("Node ID", alloc.get("NodeID", "N/A"))
            table.add_row("Desired Status", alloc.get("DesiredStatus", "N/A"))
            table.add_row("Client Status", alloc.get("ClientStatus", "N/A"))

            # Add task states if available
            task_states = alloc.get("TaskStates", {})
            for task_name, task_state in task_states.items():
                events = task_state.get("Events", [])
                recent_events = events[-3:] if events else []  # Show last 3 events
                events_str = "\n".join(
                    f"  - {e.get('DisplayMessage', 'N/A')}" for e in recent_events
                )
                table.add_row(
                    f"Task: {task_name}",
                    f"State: {task_state.get('State', 'N/A')}\nRecent Events:\n{events_str}",
                )

        self.console.print(
            Panel(table, title=f"[bold blue]Event {event.get('Index', 'N/A')}")
        )

    def read_input(self, collection_list: str) -> List:
        """
        Takes absolute or relative path to a .csv or .lst file, reads each line, and returns a List of collections
        """
        parent_dir = Path(__file__).parent
        relative_collection_list = parent_dir / collection_list

        collections = []
        if os.path.isfile(relative_collection_list):
            source_file_extension = Path(relative_collection_list).suffix
            acceptable_file_formats = [".lst", ".txt", ".csv"]
            if source_file_extension.lower() not in acceptable_file_formats:
                raise Exception(
                    "Incoming file must be in .lst, .txt, or .csv format if submitting a file name and path."
                )

            with open(relative_collection_list, "r") as collections_file:
                file_lines = collections_file.readlines()
                collections = [self.strip_newline(fl) for fl in file_lines]

        elif isinstance(relative_collection_list, str):
            collection_list = collection_list.split()
            for collection in collection_list:
                collections.append(collection)

        else:
            raise Exception(
                "collection_list not a valid filepath, or a space seperated list of collections passed within quotes"
            )

        return collections

    def strip_newline(self, collection):
        # Strips single or double quotes
        collection = collection.strip().replace('"', "")
        collection = collection.replace("'", "")
        collection = collection.replace(",", "")
        return collection


def main(
    collection_list: str,
    job_name: str,
    job_file: str,
    monitor,
    register_job,
):

    coordinator = NomadCoordinator()

    if register_job:
        coordinator.register_job(job_file)

    collections = coordinator.read_input(collection_list)

    print(f"Collections to submit as parameterized jobs : {collections}")

    # Submit parameterized jobs for collection
    for collection in collections:

        metadata = {
            # "job_id": collection
            "collection": collection,
            "gitlab_pat": coordinator.gitlab_pat,
            "gitlab_un" : coordinator.gitlab_un,
            "AWS_PROFILE" : coordinator.AWS_PROFILE,
            "aws_access_key_id" : coordinator.aws_access_key_id,
            "aws_secret_access_key" : coordinator.aws_secret_access_key,
            "aws_region" : coordinator.aws_region            
        }

        try:
            result = coordinator.dispatch_job(job_name, metadata)
            coordinator.console.print(
                f"[green]Submitted job for collection {collection}: {result}"
                # f"[green]Submitted job for collection {collection}: {result['EvalID']}"
            )
        except requests.exceptions.RequestException as e:
            coordinator.console.print(
                f"[red]Failed to submit job for collection {collection}: {e}"
            )

    # If monitoring is enabled, start the event stream with job prefix
    if monitor:
        coordinator.console.print("\n[yellow]Starting event stream monitor...")
        coordinator.monitor_job(job_name)


if __name__ == "__main__":
    """
    Test Example:
        python3 nomad_coordinator.py -r --job_file "batch-test-windows.nomad.hcl"

        python3 nomad_coordinator.py -l "test.csv" -n "batch-test-windows" -m


    Ripple Example:
        python3 nomad_coordinator.py -r -j "ripple_batch_pipeline.nomad.hcl"

        python3 nomad_coordinator.py -l "collections.csv" -n "ripple_batch_pipeline" -m

    """

    parser = argparse.ArgumentParser(
        description="Coordinate Nomad ripple pipeline processing jobs"
    )
    parser.add_argument(
        "-l",
        "--collection_list",
        # required=True,
        help="A filepath (.txt or .lst) containaing a new line separated list of valid collections or a space separated string of collections",
    )
    parser.add_argument(
        "-n",
        "--job_name",
        default="test",
        help="Nomad Job name to register job and/or submit parameterized jobs",
    )
    parser.add_argument(
        "-j",
        "--job_file",
        default="test.nomad",
        help="Nomad Job filename to register (must be in current directory (/nomad))",
    )
    parser.add_argument(
        "-m",
        "--monitor",
        action="store_true",
        help="Monitor job events after submission",
    )
    parser.add_argument(
        "-r",
        "--register_job",
        action="store_true",
        help="Register job file in the current directory with the NOMAD scheduler. ",
    )

    args = vars(parser.parse_args())

    main(**args)
