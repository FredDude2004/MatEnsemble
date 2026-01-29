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
from matensemble.strategy.not_adaptive_strategy import NonAdaptiveStrategy
from matensemble.strategy.cpu_affine_strategy import CPUAffineStrategy
from matensemble.strategy.gpu_affine_strategy import GPUAffineStrategy
from matensemble.strategy.adaptive_strategy import AdaptiveStrategy
from matensemble.strategy.dynopro_strategy import DynoproStrategy
from matensemble.fluxlet import Fluxlet
from collections import deque

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
        nnodes=None,
        gpus_per_node=None,
        restart_filename=None,
    ) -> None:
        self.pending_tasks = deque(copy.copy(gen_task_list))
        self.running_tasks = deque()
        self.completed_tasks = []
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
        self.nnodes = nnodes
        self.gpus_per_node = gpus_per_node

        self.gen_task_cmd = gen_task_cmd
        self.ml_task_cmd = ml_task_cmd
        self.ml_task_freq = ml_task_freq
        self.write_restart_freq = write_restart_freq

        setup_logger("matensemble")
        self.logger = logging.getLogger("matensemble")
        self.load_restart(restart_filename)

    # HACK: make sure this is consistent with what create_restart_file() produces
    # TODO: Need to implement this and make sure that the data is correct
    # def load_restart(self, filename): here is the actual function signature
    def load_restart(self, filename):
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

    # HACK: Make sure this is consistent with what a load_restart() expects
    # TODO: Implement this,
    def create_restart_file(self):
        pass
        # self.task_log = {
        #     "Completed tasks": self.completed_tasks,
        #     "Running tasks": self.running_tasks,
        #     "Pending tasks": self.pending_tasks,
        #     "Failed tasks": self.failed_tasks,
        # }
        # pickle.dump(
        #     self.task_log,
        #     open(f"restart_{len(self.completed_tasks)}.dat", "wb"),
        # )

    def check_resources(self) -> None:
        self.status = flux.resource.status.ResourceStatusRPC(self.flux_handle).get()
        self.resource_list = flux.resource.list.resource_list(self.flux_handle).get()
        self.resource = flux.resource.list.resource_list(self.flux_handle).get()
        self.free_gpus = self.resource.free.ngpus
        self.free_cores = self.resource.free.ncores
        self.free_excess_cores = self.free_cores - self.free_gpus

    # HACK: move method to logger somehow or just call it here
    # TODO: Implement this,
    def log_progress(self) -> None:
        pass

    def poolexecutor(
        self,
        task_arg_list,
        buffer_time=0.5,
        task_dir_list=None,
        adaptive=True,
        dynopro=False,
    ) -> None:
        """
        High-throughput executor implementation

        Args:
            param1 (type): description
            param2 (type): description
            ...

        Return:
            <return_type>. description of return

        """

        # use double ended queue and  popleft for O(1) time complexity
        gen_task_arg_list = deque(copy.copy(task_arg_list))
        gen_task_dir_list = deque(copy.copy(task_dir_list)) if task_dir_list else None

        # Initialize submission strategy based on params at run-time
        if dynopro:
            submission_strategy = DynoproStrategy(self)
        elif self.gpus_per_task > 0:
            submission_strategy = GPUAffineStrategy(self)
        else:
            submission_strategy = CPUAffineStrategy(self)

        if adaptive:
            future_processing_strategy = AdaptiveStrategy(
                self, gen_task_arg_list, gen_task_dir_list
            )
        else:
            future_processing_strategy = NonAdaptiveStrategy(self)

        done = len(self.pending_tasks) == 0 and len(self.running_tasks) == 0
        while not done:
            self.check_resources()
            self.log_progress()

            submission_strategy.submit_until_ooresources(
                gen_task_arg_list, gen_task_dir_list, buffer_time
            )
            future_processing_strategy.process_futures(buffer_time)

            self.check_resources()
            self.log_progress()

            if len(self.completed_tasks) % self.write_restart_freq == 0:
                # TODO: implement create_restart_file() method
                self.create_restart_file()

            done = len(self.pending_tasks) == 0 and len(self.running_tasks) == 0
