import logging
import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

_LOG_FORMAT = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Loggers owned by this project. Everything else is third-party and is held at the
# floor level unless promoted via `logging.FIRST_PARTY` in config.yaml.
PROJECT_LOGGERS = ("ripple1d_pipeline", "run_collection", "run_batch")


def _first_party_loggers(config_file="config.yaml"):
    """Read `logging.FIRST_PARTY` from config.yaml. A missing file or section is not fatal."""
    try:
        with open(Path.cwd() / "ripple1d_pipeline" / config_file) as file:
            config = yaml.safe_load(file) or {}
    except (FileNotFoundError, yaml.YAMLError):
        return ()
    return tuple((config.get("logging") or {}).get("FIRST_PARTY") or ())


def _resolve(level, env_var, default):
    """Resolve a level from an explicit arg, then an env var, then a default."""
    if level is None:
        level = os.getenv(env_var, default)
    if isinstance(level, str):
        level = getattr(logging, level.strip().upper(), getattr(logging, default))
    # Propagate to child processes (batch_ripple_pipeline -> ripple_pipeline subprocess).
    os.environ[env_var] = logging.getLevelName(level)
    return level


def configure_logging(level=None, third_party_level=None):
    """Configure logging for the whole pipeline in one place.

    This is the single source of truth for logging setup. Call it once from an
    entry point (a script's `__main__` or the first cell of a notebook).

    The root logger is pinned to `third_party_level`.
    Our own loggers are raised to `level` independently. To have a library follow
    `level` too, add its name to `logging.FIRST_PARTY` in config.yaml.

    Each level resolves from, in order: the explicit argument, the environment
    variable, then the default..

        level              RP_LOG_LEVEL              default INFO
        third_party_level  RP_THIRD_PARTY_LOG_LEVEL  default WARNING

    `force=True` is used so this always wins, even when a handler was already
    installed (e.g. by Jupyter/IPython or an earlier import).
    """
    # Best-effort load of .env so RP_* levels work before CollectionData runs.
    load_dotenv(".env", override=False)

    level = _resolve(level, "RP_LOG_LEVEL", "INFO")
    third_party_level = _resolve(third_party_level, "RP_THIRD_PARTY_LOG_LEVEL", "WARNING")

    # Root sits at the floor, so unowned libraries inherit it and stay quiet.
    logging.basicConfig(
        level=third_party_level,
        format=_LOG_FORMAT,
        datefmt=_DATE_FORMAT,
        force=True,
    )

    # Our code, plus anything promoted in config.yaml, is raised above the floor.
    # Their records still reach the handler installed on root.
    for name in PROJECT_LOGGERS + _first_party_loggers():
        logging.getLogger(name).setLevel(level)
