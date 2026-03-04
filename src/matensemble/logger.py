"""
logger.py
---------

Logging + status utilities for MatEnsemble.

This module creates a per-run workflow directory::

   <JOBID>_matensemble_workflow/
     status.log
     logs/
       <timestamp>_matensemble_workflow.log
     out/
       <task-id>/
         stdout
         stderr

The status file is overwritten on each update so users can monitor progress::

   watch -n 1 cat <JOBDIR>/status.log

The verbose log file contains timestamped messages and is always written to disk.
"""

from __future__ import annotations

import logging
import sys
import os
import json

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class WorkflowPaths:
    """
    Dataclass to hold the locations for the status file, log file, and output
    directory.
    """

    base_dir: Path
    status_file: Path
    logs_dir: Path
    out_dir: Path
    verbose_log_file: Path


def job_id() -> str:
    """
    Helper function to get the job_id to set the name of the
    <SLURM_JOB_ID>_matensemble_workflow directory

    Return
    ------
    str
        The SLURM_JOB_ID environment variable or the process ID
    """

    return os.environ.get("SLURM_JOB_ID") or datetime.now().strftime(
        "%_Y-%m-%d_%H-%M-%S"
    )


def timestamp_for_filename() -> str:
    """
    Helper function to get the date and time

    Return
    ------
    str
        The date and time in the format YYYY-MM-DD_HH-mm-SS
    """

    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


class StatusWriter:
    """
    Class to handle updating the status file

    Attributes
    ----------
    path : Path
        the path to the status file
    nnodes : int
        The number of nodes that flux is managing (total_allocation - 1 for flux borker)
    cores_per_node : int
        The number of CPU cores that are available on each node
    gpus_per_node : int
        The number of GPUs that are available on each node

    """

    def __init__(
        self, path: Path, allocation_information: tuple[int, int, int]
    ) -> None:
        self.path = path
        self.nnodes = allocation_information[0]
        self.cores_per_node = allocation_information[1]
        self.gpus_per_node = allocation_information[2]

    def update(
        self,
        pending: int,
        running: int,
        completed: int,
        failed: int,
        free_cores: int,
        free_gpus: int,
    ) -> None:
        data = {
            "nodes": self.nnodes,
            "coresPerNode": self.cores_per_node,
            "gpusPerNode": self.gpus_per_node,
            "pending": pending,
            "running": running,
            "completed": completed,
            "failed": failed,
            "freeCores": free_cores,
            "freeGpus": free_gpus,
        }
        self.path.write_text(json.dumps(data))


def create_workflow_paths(base_dir: str | Path | None = None) -> WorkflowPaths:
    """
    Helper function to create the output directory

    Returns
    -------
    WorkflowPaths
        An object that encapsulates all of the paths for the output
        files/directoriesof the matensemble workflow

    Notes
    -----
    Structure of the output directory::

        <SLURM_JOB_ID>_matensemble_workflow/
            |- status.log
            |- logs/
                |- <timestamp>_matensemble_workflow.log
            |- out/
                |- <output_of_workflow>

    """

    base_dir = Path(base_dir) if base_dir is not None else Path.cwd()

    workflow_dir = base_dir / f"matensemble_workflow{job_id()}"
    logs_dir = workflow_dir / "logs"
    out_dir = workflow_dir / "out"
    status_file = workflow_dir / "status.json"

    workflow_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    verbose_log_file = (
        workflow_dir / f"{timestamp_for_filename()}_matensemble_workflow.log"
    )

    return WorkflowPaths(
        base_dir=workflow_dir,
        status_file=status_file,
        logs_dir=logs_dir,
        out_dir=out_dir,
        verbose_log_file=verbose_log_file,
    )


def setup_workflow_logging(
    allocation_information: tuple[int, int, int],
    logger_name: str = "matensemble",
    base_dir: str | Path | None = None,
    console: bool | None = None,
) -> tuple[logging.Logger, StatusWriter, WorkflowPaths]:
    """
    Creates:
      - workflow directory tree
      - status writer (status.log)
      - verbose python logger (timestamped file, optional console)

    Parameters
    ----------
    logger_name: str
        The name of the logger
    base_dir: str | Path
        Where the matensemble_workflow directory will be setup
    console: bool | None
        Whether or not we are in an interactive environment

    Return
    ------
    tuple
        a three element tuple with the logger, StatusWriter and WorkflowPaths


    """
    paths = create_workflow_paths(base_dir)
    status = StatusWriter(paths.status_file, allocation_information)

    logger = logging.getLogger(logger_name)
    logger.setLevel(
        logging.DEBUG
    )  # file gets everything; handler levels control output
    logger.propagate = False

    # Prevent duplicate handlers if setup is called twice
    if logger.handlers:
        logger.handlers.clear()

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(paths.verbose_log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    if console is None:
        console = sys.stderr.isatty()

    if console:
        console_handler = logging.StreamHandler(stream=sys.stderr)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(fmt)
        logger.addHandler(console_handler)
    else:
        pass

    # print hint for user to watch the file
    hint = (
        f"Status file: {paths.status_file}\n"
        f"Watch it with: watch -n 1 cat {paths.status_file}\n"
        f"Verbose log: {paths.verbose_log_file}\n"
        f"Task outputs: {paths.out_dir}\n"
    )
    print(hint, file=sys.stderr)

    logger.info("Workflow initialized at %s", paths.base_dir)
    return logger, status, paths
