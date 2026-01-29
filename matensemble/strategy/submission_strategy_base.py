import flux

from abc import ABC, abstractmethod


class TaskSubmissionStrategy(ABC):
    @abstractmethod
    def submit_until_ooresources(
        self, task_arg_list, task_dir_list, buffer_time
    ) -> None:
        pass

    @abstractmethod
    def submit(self, task, task_args, task_dir) -> flux.job.executor.FluxExecutorFuture:
        pass
