import concurrent.futures
import flux
import time

from matensemble.strategy.process_futures_strategy_base import FutureProcessingStrategy
from matensemble.fluxlet import Fluxlet


class AdaptiveStrategy(FutureProcessingStrategy):
    # TODO: potentially add back the type annotation here need protocol
    def __init__(self, manager, task_arg_list=None, task_dir_list=None) -> None:
        self.manager = manager
        self.task_arg_list = task_arg_list
        self.task_dir_list = task_dir_list

    def submit(
        self, task, tasks_per_job, task_args, task_dir
    ) -> flux.job.executor.FluxExecutorFuture:
        fluxlet = Fluxlet(
            self.manager.flux_handle,
            tasks_per_job,
            self.manager.cores_per_task,
            self.manager.gpus_per_task,
        )
        fluxlet.job_submit(
            self.manager.executor,
            self.manager.gen_task_cmd,
            task,
            task_args,
            task_dir,
        )

        return fluxlet.future

    def adaptive_submit(self, buffer_time) -> None:
        if (
            self.manager.tasks_per_job
            and self.task_arg_list is not None
            and self.manager.free_cores
            >= self.manager.tasks_per_job[0] * self.manager.cores_per_task
            and len(self.manager.pending_tasks)
        ):
            self.manager.check_resources()
            self.manager.log_progress()

            cur_task = self.manager.pending_tasks.popleft()
            cur_task_args = self.task_arg_list.popleft()

            if self.task_dir_list is not None:
                cur_task_dir = self.task_dir_list.popleft()
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

    def process_futures(self, buffer_time) -> None:
        completed, self.manager.futures = concurrent.futures.wait(
            self.manager.futures, timeout=buffer_time
        )
        for fut in completed:
            self.manager.running_tasks.remove(fut.task)
            try:
                exc = fut.exception()
                if exc is not None:
                    self.manager.failed_tasks.append((fut.task, fut.job_spec))
                    # TODO:
                    # self.manager.logger.debug(
                    #     f"Task {fut.task} failed with exception: {exc}"
                    # )
                    continue

                res = fut.result()
                if res != 0:
                    self.manager.failed_tasks.append((fut.task, fut.job_spec))
                    # TODO:
                    # self.manager.logger.debug(
                    #     f"Task {fut.task} exited with ERROR CODE {res}"
                    # )
                    continue

                self.manager.completed_tasks.append(fut.task)

            except concurrent.futures.CancelledError as e:
                print(f"Task was cancelled before it was completed: INFO {e}")

            self.adaptive_submit(buffer_time)

            if len(self.manager.completed_tasks) % self.write_restart_freq == 0:
                self.manager.create_restart_file()
