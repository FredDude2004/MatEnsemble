import concurrent.futures
import time

from matensemble.strategy.process_futures_strategy_base import FutureProcessingStrategy


class AdaptiveStrategy(FutureProcessingStrategy):
    # TODO: potentially add back the type annotation here need protocol
    def __init__(self, manager, task_arg_list=None, task_dir_list=None) -> None:
        self.manager = manager
        self.task_arg_list = task_arg_list
        self.task_dir_list = task_dir_list

    def process_futures(self, buffer_time) -> None:
        completed, self.manager.futures = concurrent.futures.wait(
            self.manager.futures, timeout=buffer_time
        )
        for fut in completed:
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
                self.manager.running_tasks.remove(fut.task)

            except concurrent.futures.CancelledError as e:
                print(f"Task {fut.task} was cancelled before it was completed")

            if (
                self.task_arg_list is not None
                and self.manager.free_cores
                >= self.manager.tasks_per_job * self.manager.cores_per_task
                and len(self.manager.pending_tasks)
            ):
                self.manager.check_resources()
                self.manager.log_progress()

                cur_task = self.manager.pending_tasks.popleft()
                cur_task_args = self.task_arg_list.popleft()

                if self.task_dir_list is not None:
                    cur_task_dir = task_dir_list.popleft()
                else:
                    cur_task_dir = None

                self.manager.futures.add(
                    self.submit(cur_task, cur_task_args, cur_task_dir)
                )
                self.manager.running_tasks.append(cur_task)

                self.manager.check_resources()
                self.manager.log_progress()
                time.sleep(buffer_time)
