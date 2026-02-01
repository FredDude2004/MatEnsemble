import numpy as np
import flux.job
import shlex
import os

from pathlib import Path


def _normalize_task_args(task_args):
    if isinstance(task_args, list):
        return [str(arg) for arg in task_args]
    if task_args is None:
        return []
    if isinstance(task_args, (str, int, float, np.int64, np.float64, dict)):
        return [str(task_args)]
    raise TypeError(
        f"ERROR: Task argument can not be {type(task_args)}. "
        "Currently supports `list`, `str`, `int`, `float`, `np.int64`, `np.int64`, and `dict` types"
    )


def _resolve_workdir(
    task,
    task_directory=None,
    base_out_dir=None,
    launch_dir=None,
) -> Path:
    """
    Decide where the task should run and where stdout/stderr land.
    No cwd changes; just returns an absolute Path that exists.
    """
    launch_dir = Path(launch_dir or os.getcwd())

    if task_directory is not None:
        p = Path(task_directory)
        if not p.is_absolute():
            root = Path(base_out_dir) if base_out_dir is not None else launch_dir
            p = root / p
    else:
        root = Path(base_out_dir) if base_out_dir is not None else launch_dir
        p = root / str(task)

    p.mkdir(parents=True, exist_ok=True)
    return p.resolve()


class Fluxlet:
    def __init__(self, handle, tasks_per_job, cores_per_task, gpus_per_task):
        self.flux_handle = handle
        self.future = []
        self.tasks_per_job = tasks_per_job
        self.cores_per_task = cores_per_task
        self.gpus_per_task = gpus_per_task

    def job_submit(
        self,
        executor,
        command,
        task,
        task_args,
        task_directory=None,
        base_out_dir=None,  # NEW (optional): preferred integration with your workflow out/ dir
        set_gpu_affinity=False,
        set_cpu_affinity=True,
        set_mpi=None,
        env=None,  # NEW (optional): allow caller to control environment
    ):
        workdir = _resolve_workdir(
            task=task,
            task_directory=task_directory,
            base_out_dir=base_out_dir,
            launch_dir=os.getcwd(),
        )

        # safer than command.split(" ") because it respects quoting
        cmd_list = shlex.split(command)
        cmd_list.extend(_normalize_task_args(task_args))

        jobspec = flux.job.JobspecV1.from_command(
            cmd_list,
            num_tasks=int(self.tasks_per_job),
            cores_per_task=self.cores_per_task,
            gpus_per_task=self.gpus_per_task,
        )

        jobspec.cwd = str(workdir)

        if set_mpi is not None:
            jobspec.setattr_shell_option("mpi", "pmi2")
        if set_cpu_affinity:
            jobspec.setattr_shell_option("cpu-affinity", "per-task")
        if set_gpu_affinity and self.gpus_per_task > 0:
            jobspec.setattr_shell_option("gpu-affinity", "per-task")

        job_env = dict(os.environ) if env is None else dict(env)
        jobspec.environment = job_env

        jobspec.stdout = str(workdir / "stdout")
        jobspec.stderr = str(workdir / "stderr")

        self.resources = getattr(jobspec, "resources", None)
        future = executor.submit(jobspec)

        # compatibility with existing code
        future.task_ = task
        future.task = task
        future.job_spec = jobspec
        future.workdir = str(workdir)  # NEW: convenient for debugging/reporting

        self.future = future
        return future

    def hetero_job_submit(
        self,
        executor,
        nnodes,
        gpus_per_node,
        command,
        task,
        task_args,
        task_directory=None,
        base_out_dir=None,  # NEW
        set_gpu_affinity=False,
        set_cpu_affinity=True,
        set_mpi=None,
        env=None,  # NEW
    ):
        workdir = _resolve_workdir(
            task=task,
            task_directory=task_directory,
            base_out_dir=base_out_dir,
            launch_dir=os.getcwd(),
        )

        cmd_list = shlex.split(command)
        cmd_list.extend(_normalize_task_args(task_args))

        jobspec = flux.job.JobspecV1.per_resource(
            cmd_list,
            ncores=int(self.tasks_per_job),
            nnodes=nnodes,
            gpus_per_node=gpus_per_node,
            per_resource_type="core",
            per_resource_count=1,
        )

        jobspec.cwd = str(workdir)

        if set_mpi is not None:
            jobspec.setattr_shell_option("mpi", "pmi2")
        if set_cpu_affinity:
            jobspec.setattr_shell_option("cpu-affinity", "per-task")
        if set_gpu_affinity and self.gpus_per_task > 0:
            jobspec.setattr_shell_option("gpu-affinity", "per-task")

        job_env = dict(os.environ) if env is None else dict(env)
        # avoid global os.environ mutation (current code does this)
        job_env["SLURM_GPUS_PER_NODE"] = str(gpus_per_node)
        jobspec.environment = job_env

        jobspec.stdout = str(workdir / "stdout")
        jobspec.stderr = str(workdir / "stderr")

        self.resources = getattr(jobspec, "resources", None)
        future = executor.submit(jobspec)

        future.task_ = task
        future.task = task
        future.job_spec = jobspec
        future.workdir = str(workdir)

        self.future = future
        return future
