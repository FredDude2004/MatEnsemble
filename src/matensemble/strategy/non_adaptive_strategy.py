"""
non_adaptive_strategy.py
------------------------

The non-adaptive strategy is a FutureProcessingStrategy that simply processes futures
"""

import concurrent.futures
import traceback

from matensemble.strategy.process_futures_strategy_base import FutureProcessingStrategy
from datetime import datetime
from pathlib import Path


class NonAdaptiveStrategy(FutureProcessingStrategy):
    """
    Implements the FutureProcessingStrategy interface. Non adaptive, does not
    try and submit a new task after each new one is completed
    """

    def __init__(self, manager) -> None:
        """
        Parameters
        ----------
        manager: SuperFluxManager
            manages resources and calls this method based on its strategy

        Return
        ------
        None
        """

        self.manager = manager

    def append_text(self, path: Path, text: str) -> None:
        """
        Used for writing error messages to stderr on a specifig task

        Parameters
        ----------
        path: Path
            The path to the file to be written to
        text: str
            The text to write to the file

        Return
        ------
        None
        """

        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(text)

    def process_futures(self, buffer_time) -> None:
        """
        Process the FluxExecutorFuture objects and update the status lists in
        the manager and adaptively submit the

        Parameters
        ----------
        buffer_time: int | float
            The amount of time in seconds to wait for the future objects to
            complete
        """

        completed, self.manager.futures = concurrent.futures.wait(
            self.manager.futures, timeout=buffer_time
        )
        for fut in completed:
            self.manager.running_tasks.remove(fut.task)

            task = getattr(fut, "task", getattr(fut, "task_", "<unknown>"))
            workdir = Path(
                getattr(fut, "workdir", self.manager.paths.out_dir / str(task))
            )
            stdout = workdir / "stdout"
            stderr = workdir / "stderr"

            try:
                return_code = fut.result()  # raises if the task submission/run failed
            except Exception as e:
                tb = traceback.format_exc()
                stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                self.append_text(
                    stderr,
                    (
                        f"\n\n===== MATENSEMBLE WRAPPER ERROR ({stamp}) =====\n"
                        f"task={task}\n"
                        f"workdir={workdir}\n"
                        f"exception={repr(e)}\n"
                        f"{tb}\n"
                    ),
                )

                self.manager.failed_tasks.append((task, fut.job_spec))
                self.manager.logger.exception(
                    "TASK FAILED: task=%s | workdir=%s | stdout=%s | stderr=%s",
                    task,
                    workdir,
                    stdout,
                    stderr,
                )
                continue

            if return_code != 0:
                self.append_text(
                    stderr,
                    f"\n\n===== MATENSEMBLE: NONZERO EXIT =====\n"
                    f"task={task} rc={return_code}\n"
                    f"See workflow log for details: {self.manager.paths.verbose_log_file}\n",
                )
                self.manager.failed_tasks.append((task, fut.job_spec))
                self.manager.logger.error(
                    "TASK NONZERO EXIT: task=%s rc=%s | workdir=%s | stdout=%s | stderr=%s",
                    task,
                    return_code,
                    workdir,
                    stdout,
                    stderr,
                )
                continue

            self.manager.completed_tasks.append(task)

            if len(self.manager.completed_tasks) % self.manager.write_restart_freq == 0:
                self.manager.create_restart_file()
