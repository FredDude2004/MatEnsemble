import logging
import time
from matensemble.logger import setup_logger, format_status, finalize_progress

setup_logger("matensemble", interactive=True)  # force interactive for test
log = logging.getLogger("matensemble.test")

completed = running = pending = failed = 0
pending = 50
free_cores = 1024
free_gpus = 8

try:
    for i in range(1, 101):
        # fake state changes
        if pending > 0:
            pending -= 1
            running += 1
        if i % 3 == 0 and running > 0:
            running -= 1
            completed += 1
        if i % 17 == 0:
            failed += 1
            log.warning("Simulated warning at step %d", i)

        log.progress(
            format_status(completed, running, pending, failed, free_cores, free_gpus)
        )
        time.sleep(0.05)

    log.info("Done.")
finally:
    finalize_progress(logging.getLogger("matensemble"))
