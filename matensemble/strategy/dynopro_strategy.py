import time
import flux

from matensemble.strategy.submission_strategy_base import TaskSubmissionStrategy
from matensemble.fluxlet import Fluxlet


class DynoproStrategy(TaskSubmissionStrategy):
    """
    Implements the TaskSubmissionStrategy interface, a strategy class
    allowing the manager (SuperFluxManager) to implement a different strategy
    based on the parameters given to it at run time
    """

    # TODO: potentially add back type annotation for manager
    def __init__(self, manager) -> None:
        self.manager = manager

    def submit_until_ooresources(
        self, task_arg_list, task_dir_list, buffer_time
    ) -> None:
        """Submit pending tasks until you are out of resources

        Args:
            manager (SuperFluxManager): manages resources and calls this method
                                        based on its strategy
        Returns:
            None.

        """

        while (
            self.manager.tasks_per_job
            and self.manager.free_cores
            >= self.manager.tasks_per_job[0] * self.manager.cores_per_task
            and len(self.manager.pending_tasks) > 0
        ):
            self.manager.check_resources()
            self.manager.log_progress()

            cur_task = self.manager.pending_tasks.popleft()
            cur_task_args = task_arg_list.popleft()

            if task_dir_list is not None:
                cur_task_dir = task_dir_list.popleft()
            else:
                cur_task_dir = None

            self.manager.futures.add(
                self.submit(
                    cur_task, self.manager.tasks_per_job[0], cur_task_args, cur_task_dir
                )
            )
            self.manager.running_tasks.add(cur_task)

            self.manager.check_resources()
            self.manager.log_progress()
            self.manager.tasks_per_job.popleft()
            time.sleep(buffer_time)

    def submit(
        self, task, tasks_per_job, task_args, task_dir
    ) -> flux.job.executor.FluxExecutorFuture:
        if self.manager.nnodes is None or self.manager.gpus_per_node is None:
            raise ValueError(
                "ERROR: For dynopro provisioning, nnodes and gpus_per_node can not be None"
            )
        fluxlet = Fluxlet(
            self.manager.flux_handle,
            tasks_per_job,
            self.manager.cores_per_task,
            self.manager.gpus_per_task,
        )
        fluxlet.hetero_job_submit(
            self.manager.executor,
            nnodes=self.manager.nnodes,
            gpus_per_node=self.manager.gpus_per_node,
            command=self.manager.gen_task_cmd,
            task=task,
            task_args=task_args,
            task_directory=task_dir,
        )

        return fluxlet.future
