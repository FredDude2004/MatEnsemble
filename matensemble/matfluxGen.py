import concurrent.futures
import flux.job
import os.path
import logging
import pickle
import flux
import copy
import time
import sys
import os

from matensemble.logger import setup_logger, format_status, finalize_progress
from matensemble.fluxlet import Fluxlet


__author__ = "Soumendu Bagchi"
__package__ = "matensemble"


class SuperFluxManager:
    def __init__(
        self,
        gen_task_list,
        gen_task_cmd,
        ml_task_cmd,
        ml_task_freq=100,
        write_restart_freq=100,
        tasks_per_job=None,
        cores_per_task=1,
        gpus_per_task=0,
        cores_per_ml_task=1,
        restart_filename=None,
    ):
        self._running_tasks = []
        self._completed_tasks = []
        self._pending_tasks = copy.copy(gen_task_list)
        self._failed_tasks = []
        self.flux_handle = flux.Flux()

        self.futures = set()
        self.tasks_per_job = copy.copy(list(tasks_per_job))
        self.cores_per_task = cores_per_task
        self.gpus_per_task = gpus_per_task
        self.cores_per_ml_task = cores_per_ml_task

        self.gen_task_cmd = gen_task_cmd
        self.ml_task_cmd = ml_task_cmd
        self.ml_task_freq = ml_task_freq
        self.write_restart_freq = write_restart_freq

        setup_logger("matensemble")
        self.logger = logging.getLogger("matensemble")
        self.load_restart(restart_filename)

        self.logger.warning(
            "stdout_isatty=%s stderr_isatty=%s",
            sys.stdout.isatty(),
            sys.stderr.isatty(),
        )

    @property
    def running_tasks(self):
        return self._running_tasks

    @property
    def completed_tasks(self):
        return self._completed_tasks

    @property
    def pending_tasks(self):
        return self._pending_tasks

    @property
    def failed_tasks(self):
        return self._failed_tasks

    @running_tasks.setter
    def running_tasks(self, value):
        if value < 0:
            pass
        self._running_tasks = value

    @completed_tasks.setter
    def completed_tasks(self, value):
        if value < 0:
            pass
        self._completed_tasks = value

    @pending_tasks.setter
    def pending_tasks(self, value):
        if value < 0:
            pass
        self._pending_tasks = value

    @failed_tasks.setter
    def failed_tasks(self, value):
        if value < 0:
            pass
        self._failed_tasks = value

    def check_resources(self):
        self.status = flux.resource.status.ResourceStatusRPC(self.flux_handle).get()
        self.resource_list = flux.resource.list.resource_list(self.flux_handle).get()
        self.resource = flux.resource.list.resource_list(self.flux_handle).get()
        self.free_gpus = self.resource.free.ngpus
        self.free_cores = self.resource.free.ncores
        self.free_excess_cores = self.free_cores - self.free_gpus

    def update_resources(self, curr_num_task):
        self.free_excess_cores -= self.cores_per_task * curr_num_task
        self.free_cores -= self.cores_per_task * curr_num_task

        if self.gpus_per_task is not None:
            self.free_gpus -= self.gpus_per_task * curr_num_task

    def load_restart(self, filename):
        if (filename is not None) and os.path.isfile(filename):
            try:
                self.completed_tasks, self.pending_tasks = pickle.load(
                    open(filename, "rb")
                )
                # 2311
                self.logger.info(
                    "================= WORKFLOW RESTARTING =================="
                )
                self.logger.progress(
                    format_status(
                        completed=len(self.completed_tasks),
                        running=len(self.running_tasks),
                        pending=len(self.pending_tasks),
                        failed=len(self.failed_tasks),
                        free_cores=getattr(self, "free_cores", None),
                        free_gpus=getattr(self, "free_gpus", None),
                    )
                )
            except Exception as e:
                self.logger.warning("%s", e)

    def progress_monitor(self, completed_tasks, running_tasks, pending_tasks):
        width = 10
        self.logger.info(f"=== COMPLETED JOBS === RUNNING JOBS === PENDING JOBS ===")
        self.logger.info(
            f"===     {str(completed_tasks).rjust(width, ' ')}  |    {str(running_tasks).rjust(width, ' ')}  |    {str(pending_tasks).rjust(width, ' ')} ==="
        )

    def process_futures(self, buffer_time):
        done, self.futures = concurrent.futures.wait(self.futures, timeout=buffer_time)
        for fut in done:
            self._completed_tasks.append(fut.task_)

            if len(self.completed_tasks) % self.ml_task_freq == 0:
                pickle.dump(
                    (self.completed_tasks, self.pending_tasks),
                    open(f"restart_{len(self.completed_tasks)}.dat", "wb"),
                )
            try:
                if fut.result() != 0:
                    self.logger.info(
                        f"Task {fut.task_} exited with ERROR CODE {fut.result()}"
                    )
                self._running_tasks.remove(fut.task_)
            except Exception as e:
                self.logger.info("%s", e)

            self.logger.progress(
                format_status(
                    completed=len(self.completed_tasks),
                    running=len(self.running_tasks),
                    pending=len(self.pending_tasks),
                    failed=len(self.failed_tasks),
                    free_cores=getattr(self, "free_cores", None),
                    free_gpus=getattr(self, "free_gpus", None),
                )
            )

    def poolexecutor(
        self, task_arg_list, buffer_time=0.5, task_dir_list=None, adaptive=True
    ):
        """High-throughput executor implementation"""

        # trigger_ml = False
        gen_task_arg_list = copy.copy(task_arg_list)
        gen_task_dir_list = copy.copy(task_dir_list)

        self.flux_handle.rpc("resource.drain", {"targets": "0"}).get()

        with flux.job.FluxExecutor() as executor:
            ### Queue Manager ###

            while True:
                # ===== STOPPING CRITERIA =====
                if len(self.pending_tasks) == 0 and len(self.running_tasks) == 0:
                    # 9
                    finalize_progress(logging.getLogger("matensemble"))
                    self.logger.info(
                        "============= EXITING WORKFLOW ENVIRONMENT ============="
                    )
                    break
                # =============================

                # self.logger.info("=============INITIALIZING WORKFLOW ENVIRONMENT====================")

                if len(self.pending_tasks) > 0:
                    self.check_resources()

                    self.flux_handle.rpc(
                        "resource.drain", {"targets": "0"}
                    ).get()  # preparing node stoping sending new jobs
                    if self.gpus_per_task > 0:
                        self.check_resources()
                        self.logger.progress(
                            format_status(
                                completed=len(self.completed_tasks),
                                running=len(self.running_tasks),
                                pending=len(self.pending_tasks),
                                failed=len(self.failed_tasks),
                                free_cores=getattr(self, "free_cores", None),
                                free_gpus=getattr(self, "free_gpus", None),
                            )
                        )

                        #   FOR FRONTIER FREE CORES ARE EQUIV. TO FREE "EXCESS" CORES! SO CHANGING THE LOGIC BELOW ACCORDINGLY . . .
                        while (
                            self.free_cores
                            >= self.tasks_per_job[0] * self.cores_per_task
                            and self.free_gpus
                            >= self.tasks_per_job[0] * self.gpus_per_task
                            and len(self.pending_tasks) > 0
                        ):
                            cur_task = self.pending_tasks[0]
                            cur_task_args = gen_task_arg_list[0]

                            _ = self._pending_tasks.pop(0)
                            _ = gen_task_arg_list.pop(0)

                            if gen_task_dir_list is not None:
                                cur_task_dir = gen_task_dir_list[0]
                                _ = gen_task_dir_list.pop(0)
                            else:
                                cur_task_dir = None

                            flxt = Fluxlet(
                                self.flux_handle,
                                self.tasks_per_job[0],
                                self.cores_per_task,
                                self.gpus_per_task,
                            )
                            flxt.job_submit(
                                executor,
                                self.gen_task_cmd,
                                cur_task,
                                cur_task_args,
                                cur_task_dir,
                            )

                            self.futures.add(flxt.future)
                            self._running_tasks.append(cur_task)
                            # self.update_resources(int(self.tasks_per_job[0]))

                            self.check_resources()
                            self.logger.progress(
                                format_status(
                                    completed=len(self.completed_tasks),
                                    running=len(self.running_tasks),
                                    pending=len(self.pending_tasks),
                                    failed=len(self.failed_tasks),
                                    free_cores=getattr(self, "free_cores", None),
                                    free_gpus=getattr(self, "free_gpus", None),
                                )
                            )

                            _ = self.tasks_per_job.pop(0)
                            if len(self.tasks_per_job) == 0:
                                self.tasks_per_job = [0]
                            time.sleep(buffer_time)

                    else:
                        self.logger.progress(
                            format_status(
                                completed=len(self.completed_tasks),
                                running=len(self.running_tasks),
                                pending=len(self.pending_tasks),
                                failed=len(self.failed_tasks),
                                free_cores=getattr(self, "free_cores", None),
                                free_gpus=getattr(self, "free_gpus", None),
                            )
                        )
                        while (
                            self.free_cores
                            >= self.tasks_per_job[0] * self.cores_per_task
                            and len(self.pending_tasks) > 0
                        ):
                            self.check_resources()
                            cur_task = self.pending_tasks[0]

                            cur_task_args = gen_task_arg_list[0]

                            _ = self._pending_tasks.pop(0)
                            _ = gen_task_arg_list.pop(0)

                            if gen_task_dir_list != None:
                                cur_task_dir = gen_task_dir_list[0]
                                _ = gen_task_dir_list.pop(0)
                            else:
                                cur_task_dir = None

                            flxt = Fluxlet(
                                self.flux_handle,
                                self.tasks_per_job[0],
                                self.cores_per_task,
                                self.gpus_per_task,
                            )
                            flxt.job_submit(
                                executor,
                                self.gen_task_cmd,
                                cur_task,
                                cur_task_args,
                                cur_task_dir,
                            )

                            self.futures.add(flxt.future)
                            self._running_tasks.append(cur_task)
                            # self.update_resources(int(self.tasks_per_job[0]))
                            self.check_resources()
                            self.logger.progress(
                                format_status(
                                    completed=len(self.completed_tasks),
                                    running=len(self.running_tasks),
                                    pending=len(self.pending_tasks),
                                    failed=len(self.failed_tasks),
                                    free_cores=getattr(self, "free_cores", None),
                                    free_gpus=getattr(self, "free_gpus", None),
                                )
                            )
                            _ = self.tasks_per_job.pop(0)
                            if len(self.tasks_per_job) == 0:
                                self.tasks_per_job = [0]
                            time.sleep(buffer_time)

                        #   TO BE IMPLEMENTED FOR THE ACTIVE LEARNING LOOP
                        #
                        # if trigger_ml and self.free_excess_cores >=  self.cores_per_ml_task:

                        #     """
                        #     implement ML Workflow HERE--also DATA EXTRACTION WORKLFLOW GOES HERE
                        #     """

                        #     trigger_ml=False

                # time.sleep(120)
                # """

                # self.process_futures(buffer_time)

                done, self.futures = concurrent.futures.wait(
                    self.futures, timeout=buffer_time
                )  # return_when=concurrent.futures.FIRST_COMPLETED) #timeout=buffer_time)
                for fut in done:
                    self._completed_tasks.append(fut.task_)

                    ## Adaptive have to be implemented for heterogeneous task requirements

                    if len(self.completed_tasks) % self.write_restart_freq == 0:
                        # self.trigger_ml=True
                        # print("Triggering ML workflow: ",len(self.completed_tasks))
                        self.task_log = {
                            "Completed tasks": self.completed_tasks,
                            "Running tasks": self.running_tasks,
                            "Pending tasks": self.pending_tasks,
                            "Failed tasks": self.failed_tasks,
                        }
                        pickle.dump(
                            self.task_log,
                            open(f"restart_{len(self.completed_tasks)}.dat", "wb"),
                        )
                    try:
                        print(f"Task {fut.task_} result: {fut.result(timeout=1200)}")
                        if fut.result(timeout=1200) != 0:
                            self.logger.info(
                                f"Task {fut.task_} exited with ERROR CODE {fut.result()}"
                            )
                            self._failed_tasks.append(fut.task_)
                        self._running_tasks.remove(fut.task_)
                    except Exception as e:
                        print(
                            f"Task {fut.task_}: could not process future.results() and exited with exception {e}"
                        )
                        self._running_tasks.remove(fut.task_)

                    width = 10
                    self.logger.info(
                        f"=== COMPLETED JOBS === RUNNING JOBS === PENDING JOBS ==="
                    )
                    self.logger.info(
                        f"===     {str(len(self.completed_tasks)).rjust(width, '_')}  |    {str(len(self.running_tasks)).rjust(width, '_')}  |    {str(len(self.pending_tasks)).rjust(width, '_')} ==="
                    )
