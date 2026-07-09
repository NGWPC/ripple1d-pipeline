import logging
import os

from dotenv import load_dotenv

_LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def configure_logging(level=None):
    """Configure root logging for the whole pipeline in one place.

    This is the single source of truth for logging setup. Call it once from an
    entry point (a script's `__main__` or the first cell of a notebook).

    Level resolution order:
        1. The explicit `level` argument, e.g. "DEBUG" or `logging.DEBUG`.
        2. The `RP_LOG_LEVEL` environment variable (may come from ``.env``).
        3. `INFO` (default).

    `force=True` is used so this always wins, even when a handler was already
    installed (e.g. by Jupyter/IPython or an earlier import).
    """
    if level is None:
        # Best-effort load of .env so RP_LOG_LEVEL works before CollectionData runs.
        load_dotenv(".env", override=False)
        level = os.getenv("RP_LOG_LEVEL", "INFO")

    if isinstance(level, str):
        level = getattr(logging, level.strip().upper(), logging.INFO)

    # Propagate to child processes (e.g. batch_ripple_pipeline -> ripple_pipeline subprocess).
    os.environ["RP_LOG_LEVEL"] = logging.getLevelName(level)

    logging.basicConfig(
        level=level,
        format=_LOG_FORMAT,
        datefmt=_DATE_FORMAT,
        force=True,
    )
