import numpy as np
import flux.job
import os.path
import logging
import numbers
import pickle
import time
import flux
import copy
import os

from matensemble.strategy.not_adaptive_strategy import NonAdaptiveStrategy
from matensemble.strategy.cpu_affine_strategy import CPUAffineStrategy
from matensemble.strategy.gpu_affine_strategy import GPUAffineStrategy
from matensemble.strategy.adaptive_strategy import AdaptiveStrategy
from matensemble.strategy.dynopro_strategy import DynoproStrategy
from collections import deque

__author__ = ["Soumendu Bagchi", "Kaleb Duchesneau"]
__package__ = "matensemble"


class SuperFluxManager:
    def __init__(
        self,
        gen_task_list,
        gen_task_cmd,
        write_restart_freq=100,
        tasks_per_job=None,
        cores_per_task=1,
        gpus_per_task=0,
        nnodes=None,
        gpus_per_node=None,
        restart_filename=None,
    ) -> None:
        self.pending_tasks = deque(copy.copy(gen_task_list))
        self.running_tasks = set()
        self.completed_tasks = []
        self.failed_tasks = []

        self.flux_handle = flux.Flux()

        self.futures = set()

        if tasks_per_job is None:
            self.tasks_per_job = deque([1] * len(self.pending_tasks))
        # Use numbers.Real to catch everything castable to int (int, float, Decimal, etc.)
        elif isinstance(tasks_per_job, numbers.Real):
            self.tasks_per_job = deque([int(tasks_per_job)] * len(self.pending_tasks))
        elif isinstance(tasks_per_job, (list, np.ndarray)):
            self.tasks_per_job = deque(copy.copy(tasks_per_job))
        else:
            raise TypeError("tasks_per_job must be a real number or a list of numbers")

        self.cores_per_task = cores_per_task
        self.gpus_per_task = gpus_per_task
        self.nnodes = nnodes
        self.gpus_per_node = gpus_per_node

        self.gen_task_cmd = gen_task_cmd
        self.write_restart_freq = write_restart_freq

        # TODO: Make the logger actually work the way you want it to
        # self.logger = logging.getLogger("matensemble")
        # self.load_restart(restart_filename)

        self.setup_logger()

    # HACK: make sure this is consistent with what create_restart_file() produces
    # TODO: This probably doesn't work, it you will lose any jobs that were
    #       running and you don't resent the task arg and dir lists. Talk with
    #       Dr. Bagchi about this
    def load_restart(self, filename):
        if (filename is not None) and os.path.isfile(filename):
            try:
                task_log = pickle.load(open(filename, "rb"))
                self.completed_tasks = task_log["Completed tasks"]
                self.running_tasks = task_log["Running tasks"]
                self.pending_tasks = task_log["Pending tasks"]
                self.failed_tasks = task_log["Failed tasks"]
                # self.logger.info(
                #     "================= WORKFLOW RESTARTING =================="
                # )
            except Exception as e:
                print("%s", e)
                # self.logger.warning("%s", e)

    def create_restart_file(self) -> None:
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

    def check_resources(self) -> None:
        self.status = flux.resource.status.ResourceStatusRPC(self.flux_handle).get()
        self.resource_list = flux.resource.list.resource_list(self.flux_handle).get()
        self.resource = flux.resource.list.resource_list(self.flux_handle).get()
        self.free_gpus = self.resource.free.ngpus
        self.free_cores = self.resource.free.ncores
        self.free_excess_cores = self.free_cores - self.free_gpus

    def setup_logger(self):
        self.logger = logging.getLogger(__name__)

        stdoutHandler = logging.StreamHandler(stream=sys.stdout)
        date_time_hash = str(datetime.now())
        fileHandler = logging.FileHandler(f"logs-{date_time_hash}.txt")

        Fmt = logging.Formatter(
            "%(name)s %(asctime)s %(levelname)s %(filename)s %(lineno)s %(process)d %(message)s",
            defaults={"levelname": "severity", "asctime": "timestamp"},
            datefmt="%Y-%m-%dT%H:%M:%SZ",
        )

        stdoutHandler.setFormatter(Fmt)
        fileHandler.setFormatter(Fmt)

        self.logger.addHandler(stdoutHandler)
        self.logger.addHandler(fileHandler)

        self.logger.setLevel(logging.INFO)

    # HACK: move method to logger somehow or just call it here
    # TODO: Implement this,
    def log_progress(self) -> None:
        num_pending_tasks = len(self.pending_tasks)
        num_running_tasks = len(self.running_tasks)
        num_completed_tasks = len(self.completed_tasks)
        num_failed_tasks = len(self.failed_tasks)
        # TODO: Make this use a logger instead of print statements
        print(
            f"TASKS     === Pending tasks: {num_pending_tasks} | Running tasks: {num_running_tasks} | Completed tasks: {num_completed_tasks} | Failed tasks: {num_failed_tasks}"
        )
        print(
            f"RESOURCES === Free Cores: {self.free_cores} | Free GPUs: {self.free_gpus}\n"
        )

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
            task_arg_list (List): List of tasks to be schedules and completed
            buffer_time (num): The amount of time that will be used as the timeout= option for Future objects
            task_dir_list (List): Where completed tasks output files will be placed
            adaptive (bool): Whether or not tasks are scheduled adaptively
            dynopro (bool): Whether or not the dynopro module will be used for task submission

        Return:
            None

        """

        # use double ended-queue and popleft for O(1) time complexity off front of lists
        gen_task_arg_list = deque(copy.copy(task_arg_list))
        gen_task_dir_list = deque(copy.copy(task_dir_list)) if task_dir_list else None

        # prepare resources

        # initialize submission strategy based on params at run-time

        if dynopro:
            submission_strategy = DynoproStrategy(self)
        elif self.gpus_per_task > 0:
            submission_strategy = GPUAffineStrategy(self)
        else:
            submission_strategy = CPUAffineStrategy(self)

        # initialize future processing strategy at run-time
        if adaptive:
            future_processing_strategy = AdaptiveStrategy(
                self, gen_task_arg_list, gen_task_dir_list
            )
        else:
            future_processing_strategy = NonAdaptiveStrategy(self)

        self.flux_handle.rpc("resource.drain", {"targets": "0"}).get()
        with flux.job.FluxExecutor() as executor:
            # set executor in manager so that strategies can use it
            self.executor = executor

            """
            Super loop: while you have jobs to run and/or running jobs
                - submit jobs until you are out of resources 
                - process running jobs 
                - update resources
                - create restart file if needed
                - continue...

            """
            print("=== ENTERING WORKFLOW ENVIRONMENT ===")
            start = time.perf_counter()
            done = len(self.pending_tasks) == 0 and len(self.running_tasks) == 0
            while not done:
                self.check_resources()
                self.log_progress()

                submission_strategy.submit_until_ooresources(
                    gen_task_arg_list, gen_task_dir_list, buffer_time
                )
                future_processing_strategy.process_futures(buffer_time)

                done = len(self.pending_tasks) == 0 and len(self.running_tasks) == 0

            # TODO: Log that you are finished here
            end = time.perf_counter()
            print("=== EXITING WORKFLOW ENVIRONMENT ===")
            print(f"Workflow took {(start - end):.4f} seconds to run.")
