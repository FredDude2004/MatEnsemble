# TODO: this may be a good idea but get it working first
# matensemble/protocols.py
from __future__ import annotations

from typing import Protocol, Deque, Set, Callable, Any, Optional


class SuperFluxManagerLike(Protocol):
    free_cores: int
    free_gpus: int
    tasks_per_job: int
    cores_per_task: int
    gpus_per_task: int

    pending_tasks: Deque[Any]
    running_tasks: Deque[Any]
    completed_tasks: list
    failed_tasks: list
    futures: Set[Any]  # could tighten to Future type later

    flux_handle: Any
    gen_task_cmd: Any

    # methods used by strategies
    def check_resources(self) -> None: ...
    def log_progress(self) -> None: ...
