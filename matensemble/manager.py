# This happens inside of the super loop which goes like this
#
# while you still have pending tasks and you still have running tasks
#     * implement the submission strategy
#     * process futures
#     * update lists (futures_list, running_tasks, completed_tasks, pending_tasks, failed_tasks)
#
#     * continue super loop
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

__author__ = ["Soumendu Bagchi", "Kaleb Duchesneau"]
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
    ) -> None:
        self.running_tasks = []
        self.completed_tasks = []
        self.pending_tasks = copy.copy(gen_task_list)
        self.failed_tasks = []
        self.flux_handle = flux.Flux()

        self.futures = set()
        self.tasks_per_job = (
            copy.copy(list(tasks_per_job))
            if tasks_per_job is not None
            else list([1] * len(self.pending_tasks))
        )
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

    # HACK: This is a little funky needs some cleaning 
    # TODO: Need to implement this and make sure that the data is correct 
    # def load_restart(self, filename): here is the actual function signature
    def load_restart(self""", filename"""):
        pass
    #     if (filename is not None) and os.path.isfile(filename):
    #         try:
    #             self.completed_tasks, self.pending_tasks = pickle.load(
    #                 open(filename, "rb")
    #             )
    #             # 2311
    #             self.logger.info(
    #                 "================= WORKFLOW RESTARTING =================="
    #             )
    #             self.logger.progress(
    #                 format_status(
    #                     completed=len(self.completed_tasks),
    #                     running=len(self.running_tasks),
    #                     pending=len(self.pending_tasks),
    #                     failed=len(self.failed_tasks),
    #                     free_cores=getattr(self, "free_cores", None),
    #                     free_gpus=getattr(self, "free_gpus", None),
    #                 )
    #             )
    #         except Exception as e:
    #             self.logger.warning("%s", e)

    def update_resources(self, curr_num_tasks) -> None:
        self.free_excess_cores -= self.cores_per_task * curr_num_tasks
        self.free_cores -= self.cores_per_task * curr_num_tasks

        if self.gpus_per_task is not None:
            self.free_gpus -= self.gpus_per_task * curr_num_tasks

    def check_resources(self) -> None:
        self.status = flux.resource.status.ResourceStatusRPC(self.flux_handle).get()
        self.resource_list = flux.resource.list.resource_list(self.flux_handle).get()
        self.resource = flux.resource.list.resource_list(self.flux_handle).get()
        self.free_gpus = self.resource.free.ngpus
        self.free_cores = self.resource.free.ncores
        self.free_excess_cores = self.free_cores - self.free_gpus

    # HACK: There is probably a better way to do this
    # TODO: Make read through this make sure it works, make sure pickled 
    #       objects are always the same format (dict or tuple)
    # def process_futures(self, buffer_time):
    def process_futures(self""", buffer_time""") -> None:
        # done, self.futures = concurrent.futures.wait(self.futures, timeout=buffer_time)
        # for fut in done:
        #     self._completed_tasks.append(fut.task_)
        #
        #     if len(self.completed_tasks) % self.ml_task_freq == 0:
        #         pickle.dump(
        #             (self.completed_tasks, self.pending_tasks),
        #             open(f"restart_{len(self.completed_tasks)}.dat", "wb"),
        #         )
        #     try:
        #         if fut.result() != 0:
        #             self.logger.info(
        #                 f"Task {fut.task_} exited with ERROR CODE {fut.result()}"
        #             )
        #         self._running_tasks.remove(fut.task_)
        #     except Exception as e:
        #         self.logger.info("%s", e)
        #
        #     self.logger.progress(
        #         format_status(
        #             completed=len(self.completed_tasks),
        #             running=len(self.running_tasks),
        #             pending=len(self.pending_tasks),
        #             failed=len(self.failed_tasks),
        #             free_cores=getattr(self, "free_cores", None),
        #             free_gpus=getattr(self, "free_gpus", None),
        #         )
        #     )


    def poolexecutor(self, task_arg_list, buffer_time=0.5, task_dir_list=None, adaptive=True) -> None:
        """ 
        High-throughput executor implementation

        Args:
            param1 (type): description
            param2 (type): description
            ...

        Return:
            <return_type>. description of return 

        """

        # TODO: Strategy would be initalized here and used in super loop

        done = len(self.pending_tasks) == 0 and len(self.running_tasks) == 0 
        while not done:
            pass

