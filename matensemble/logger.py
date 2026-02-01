"""
I want the logger to create a directory that all logs will go into when the user
runs the program. When the logger is setup it will create a directory named
<SLURM_JOB_ID>_matensemble_workflow/ and inside of this directory there will
be the status file and directories for output, logs and errors.

There should be two files that are created when running the program. A status
file that shows the resource count and the number of jobs that the user will be
able to 'watch' during execution to see the status of their program. The status
should be located at <SLURM_JOB_ID>_matensemble_workflow/status.log.
```python
import os
job_id = os.environ.get('SLURM_JOB_ID')
```

There should be a second more verbose log file that get written to
<SLURM_JOB_ID>_matensemble_workflow/ that is named in the format
YYYY-MM-DD:HH:MM:SS_matensemble_workflow.log. This file will have everything that
the status file has but be more versbose and have timestamps. If the user
runs the same workflow multiple times this will be

The last thing is the out/ directory that will hold all of the tasks and will
have all of their output including stdout and stderr.

Here is the format that I want the status file to hold to:
```
JOBS:      Pending     Running     Completed   Failed
           {num     }  {num     }  {num     }  {num     }

RESOURCES: Free Cores -Free GPUs
           {num     } {num     }
```
This is just a suggestion if there is a more user friendly way I am open to it.

For the verbose log files they will look like this.
"""

from __future__ import annotations

import logging
import sys
import os

from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from pathlib import Path


@dataclass(frozen=True)
class WorkflowPaths:
    base_dir: Path
    status_file: Path
    logs_dir: Path
    out_dir: Path
    verbose_log_file: Path


def _job_id() -> str:
    return os.environ.get("SLURM_JOB_ID") or f"local-{os.getpid()}"


def _timestamp_for_filename() -> str:
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


def _atomic_write_text(path: Path, text: str) -> None:
    """
    Atomic-ish replace so `watch cat status.log` never sees a half-written file.
    """
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


class StatusWriter:
    """
    Maintains the human-friendly status file (overwritten each update).
    """

    def __init__(self, path: Path):
        self.path = path

    @staticmethod
    def render(
        pending: int,
        running: int,
        completed: int,
        failed: int,
        free_cores: int,
        free_gpus: int,
        include_updated_line: bool = True,
    ) -> str:
        updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # fixed-width columns to keep it stable in watch/tmux
        lines = []
        if include_updated_line:
            lines.append(f"UPDATED:   {updated}")
            lines.append("")

        lines.append("JOBS:        Pending     Running   Completed     Failed")
        lines.append(
            f"            {pending:>8}   {running:>8}   {completed:>8}   {failed:>8}"
        )
        lines.append("")
        lines.append("RESOURCES:  Free Cores   Free GPUs")
        lines.append(f"            {free_cores:>8}   {free_gpus:>8}")
        lines.append("")  # trailing newline-friendly

        return "\n".join(lines)

    def update(
        self,
        pending: int,
        running: int,
        completed: int,
        failed: int,
        free_cores: int,
        free_gpus: int,
    ) -> None:
        text = self.render(pending, running, completed, failed, free_cores, free_gpus)
        _atomic_write_text(self.path, text)


def create_workflow_paths(base_dir: Optional[str | Path] = None) -> WorkflowPaths:
    base_dir = Path(base_dir) if base_dir is not None else Path.cwd()

    workflow_dir = base_dir / f"{_job_id()}_matensemble_workflow"
    logs_dir = workflow_dir / "logs"
    out_dir = workflow_dir / "out"
    status_file = workflow_dir / "status.log"

    workflow_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    verbose_log_file = (
        workflow_dir / f"{_timestamp_for_filename()}_matensemble_workflow.log"
    )

    return WorkflowPaths(
        base_dir=workflow_dir,
        status_file=status_file,
        logs_dir=logs_dir,
        out_dir=out_dir,
        verbose_log_file=verbose_log_file,
    )


def setup_workflow_logging(
    logger_name: str = "matensemble",
    base_dir: Optional[str | Path] = None,
    console: Optional[bool] = None,
) -> tuple[logging.Logger, StatusWriter, WorkflowPaths]:
    """
    Creates:
      - workflow directory tree
      - status writer (status.log)
      - verbose python logger (timestamped file, optional console)
    """
    paths = create_workflow_paths(base_dir)
    status = StatusWriter(paths.status_file)

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
