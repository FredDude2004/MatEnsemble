from abc import ABC, abstractmethod


class TaskSubmissionStrategy(ABC):
    @abstractmethod
    def poolexecutor(
        self, task_arg_list, buffer_time=0.5, task_dir_list=None, adaptive: bool = True
    ) -> None:
        pass
