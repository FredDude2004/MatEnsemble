import time
import flux

from matensemble.strategy.submission_strategy_base import TaskSubmissionStrategy
from matensemble.fluxlet import Fluxlet


class CPUAffineStrategy(TaskSubmissionStrategy):
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
            self.manager.free_cores
            >= self.manager.tasks_per_job * self.manager.cores_per_task
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

            self.manager.futures.add(self.submit(cur_task, cur_task_args, cur_task_dir))
            self.manager.running_tasks.append(cur_task)

            self.manager.check_resources()
            self.manager.log_progress()
            time.sleep(buffer_time)

    def submit(self, task, task_args, task_dir) -> flux.job.executor.FluxExecutorFuture:
        fluxlet = Fluxlet(
            self.manager.flux_handle,
            self.manager.tasks_per_job,
            self.manager.cores_per_task,
            self.manager.gpus_per_task,
        )
        fluxlet.job_submit(
            flux.job.FluxExecutor(),
            self.manager.gen_task_cmd,
            task,
            task_args,
            task_dir,
        )

        return fluxlet.future
