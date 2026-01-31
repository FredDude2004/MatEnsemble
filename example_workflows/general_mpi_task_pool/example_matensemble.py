from matensemble.manager import SuperFluxManager
import numpy as np
import os

__author__ = "Soumendu Bagchi"

# -----------------------
# Consistent, deterministic test
# -----------------------

N_task = 10

# Task IDs in order: 1, 2, 3, ... N_task
task_list = list(range(1, N_task + 1))

# Command / executable path
task_command = os.path.abspath("mpi_helloworld.py")

# Constant tasks_per_job for every task (match original behavior)
# (Use 56 if you want the same as your earlier benchmark)
TASKS_PER_JOB = 50
tasks_per_job = TASKS_PER_JOB * np.ones(N_task, dtype=int)

master = SuperFluxManager(
    task_list,
    task_command,
    write_restart_freq=5,
    tasks_per_job=tasks_per_job,
    cores_per_task=1,
    gpus_per_task=0,
)

# Deterministic args, aligned with task order: 1..N_task
task_arg_list = list(range(1, N_task + 1))

# Run
master.poolexecutor(task_arg_list=task_arg_list, buffer_time=1, task_dir_list=None)
