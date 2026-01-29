import flux

from abc import ABC, abstractmethod


class FutureProcessingStrategy(ABC):
    @abstractmethod
    def process_futures(self, buffer_time) -> None:
        pass
