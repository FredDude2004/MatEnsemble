from __future__ import annotations

import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, TextIO

# Custom level for "in-place" status updates
PROGRESS = 25
logging.addLevelName(PROGRESS, "PROGRESS")


def is_interactive(stream: TextIO = sys.stdout) -> bool:
    """True if we're attached to a TTY (interactive shell)."""
    return bool(getattr(stream, "isatty", lambda: False)())


def _logger_progress(self: logging.Logger, msg: str, *args, **kwargs) -> None:
    """logger.progress(...) -> emits an in-place status update (interactive only)."""
    if self.isEnabledFor(PROGRESS):
        extra = kwargs.setdefault("extra", {})
        extra["inplace"] = True
        self._log(PROGRESS, msg, args, **kwargs)


# Monkey-patch convenience method once
if not hasattr(logging.Logger, "progress"):
    logging.Logger.progress = _logger_progress


class ExcludeProgressFilter(logging.Filter):
    """Prevent PROGRESS spam from going into the logfile."""

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno != PROGRESS


class InPlaceStatusHandler(logging.Handler):
    """
    Writes a single updating status line to a stream using carriage return.
    Only meaningful on a TTY.
    """

    def __init__(self, stream: TextIO = sys.stdout) -> None:
        super().__init__(level=PROGRESS)
        self.stream = stream
        self._last_len = 0

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            pad = max(self._last_len - len(msg), 0)
            self.stream.write("\r" + msg + (" " * pad))
            self.stream.flush()
            self._last_len = len(msg)
        except Exception:
            self.handleError(record)

    def clear(self) -> None:
        if self._last_len:
            self.stream.write("\r" + (" " * self._last_len) + "\r")
            self.stream.flush()
            self._last_len = 0

    def newline(self) -> None:
        self.stream.write("\n")
        self.stream.flush()
        self._last_len = 0


class StatusAwareStreamHandler(logging.StreamHandler):
    """
    Before printing a normal line (warning/error), clear the status line so output
    doesn't get visually mangled in interactive mode.
    """

    def __init__(
        self, status_handler: InPlaceStatusHandler, stream: Optional[TextIO] = None
    ) -> None:
        super().__init__(stream=stream)
        self._status_handler = status_handler

    def emit(self, record: logging.LogRecord) -> None:
        self._status_handler.clear()
        super().emit(record)


_CONFIGURED = False


def setup_logger(
    package_name: str = "matensemble",
    log_dir: str = "logs",
    interactive: Optional[bool] = None,
) -> logging.Logger:
    """
    Configure package-wide logging once.

    - Always writes DEBUG+ to a timestamped logfile.
    - If interactive (TTY): shows an in-place PROGRESS status line and prints WARNING+ to stderr.
    - If not interactive: prints WARNING+ to stderr
    """
    global _CONFIGURED

    logger = logging.getLogger(package_name)
    logger.setLevel(logging.DEBUG)

    if _CONFIGURED or getattr(logger, "_matensemble_configured", False):
        return logger

    logger.propagate = False
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    time_stamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    logfile = Path(log_dir) / f"{package_name}_{time_stamp}.log"

    # File formatter (UTC timestamps)
    file_fmt = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(filename)s:%(lineno)d | %(process)d | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    file_fmt.converter = time.gmtime  # type: ignore[attr-defined]

    fh = logging.FileHandler(logfile, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(file_fmt)
    fh.addFilter(ExcludeProgressFilter())
    logger.addHandler(fh)

    if interactive is None:
        interactive = is_interactive(sys.stdout) and is_interactive(sys.stderr)

    if interactive:
        # In-place status line (stdout)
        status_handler = InPlaceStatusHandler(stream=sys.stdout)
        status_handler.setLevel(PROGRESS)
        status_handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(status_handler)

        # Only warnings/errors to terminal (stderr)
        wh = StatusAwareStreamHandler(status_handler, stream=sys.stderr)
        wh.setLevel(logging.WARNING)
        wh.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
        logger.addHandler(wh)
    else:
        # Non-interactive: keep terminal quiet, but show WARNING+ so jobs still surface errors.
        wh = logging.StreamHandler(stream=sys.stderr)
        wh.setLevel(logging.WARNING)
        wh.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
        logger.addHandler(wh)

    logger._matensemble_configured = True  # type: ignore[attr-defined]
    _CONFIGURED = True
    return logger


def format_status(
    completed: int,
    running: int,
    pending: int,
    failed: int,
    free_cores: int,
    free_gpus: int,
) -> str:
    parts = [
        f"Completed: {completed},",
        f"Running: {running},",
        f"Pending: {pending},",
        f"Failed: {failed},",
    ]
    if free_cores is not None or free_gpus is not None:
        parts.append("|")
    if free_cores is not None:
        parts.append(f"free_cores:{free_cores}")
    if free_gpus is not None:
        parts.append(f"free_gpus:{free_gpus}")
    return " ".join(parts)


def finalize_progress(logger: logging.Logger) -> None:
    """Call at program end so the shell prompt doesn't stay on the status line."""
    for h in logger.handlers:
        if isinstance(h, InPlaceStatusHandler):
            h.newline()
            break
