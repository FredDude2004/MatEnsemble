import concurrent.futures

from matensemble.strategy.process_futures_strategy_base import FutureProcessingStrategy


class NonAdaptiveStrategy(FutureProcessingStrategy):
    # TODO: potentially add back type annotation for manager
    def __init__(self, manager) -> None:
        self.manager = manager

    def process_futures(self, buffer_time) -> None:
        completed, self.manager.futures = concurrent.futures.wait(
            self.manager.futures, timeout=buffer_time
        )
        for fut in completed:
            try:
                exc = fut.exception(timeout=buffer_time)
                if exc is not None:
                    self.manager.failed_tasks.append((fut.task, fut.job_spec))
                    # TODO:
                    # self.manager.logger.debug(
                    #     f"Task {fut.task} failed with exception: {exc}"
                    # )
                    continue

                res = fut.result(timeout=buffer_time)
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
                print(f"Task {fut.task} was cancelled before it was completed\n {e}")
