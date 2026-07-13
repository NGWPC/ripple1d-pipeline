"""Load pipeline configuration.

Two disjoint key-spaces, merged into one dict:
  - Behavior/tuning:      default_config.yaml, optionally overridden by repo-root config.yaml.
  - Machine/environment:  .env (RP_* vars), overlaid onto the config object.

.env and the YAML never define the same key, so there is no precedence between them.
(config.yaml overriding default_config.yaml is the one intentional override, within
the behavior key-space.) See design_guide.md.
"""

import logging
import os
from pathlib import Path

import yaml
from dotenv import find_dotenv, load_dotenv

logger = logging.getLogger(__name__)

_PKG = Path(__file__).resolve().parent
DEFAULTS_PATH = _PKG / "default_config.yaml"
# repo root = package parent; resolves via __file__, not cwd
DEFAULT_USER_CONFIG = _PKG.parent / "config.yaml"

# RP_* environment variable -> (nested config keys, cast). See design_guide.md.
_ENV_OVERLAY = {
    "RP_RIPPLE1D_VERSION": (("RIPPLE1D_VERSION",), str),
    "RP_COLLECTIONS_ROOT_DIR": (("paths", "COLLECTIONS_ROOT_DIR"), str),
    "RP_NWM_FLOWLINES_PATH": (("paths", "NWM_FLOWLINES_PATH"), str),
    "RP_MONITORING_DB_PATH": (("paths", "MONITORING_DB_PATH"), str),
    "RP_BRIDGE_TILE_INDEX_PATH": (("paths", "BRIDGE_TILE_INDEX_PATH"), str),
    "RP_TERRAIN_SOURCE_URL": (("paths", "TERRAIN_SOURCE_URL"), str),
    "RP_SOURCE_NETWORK": (("paths", "SOURCE_NETWORK"), str),
    "RP_S3_UPLOAD_PREFIX": (("paths", "S3_UPLOAD_PREFIX"), str),
    "RP_S3_UPLOAD_FAILED_PREFIX": (("paths", "S3_UPLOAD_FAILED_PREFIX"), str),
    "RP_FLOW_FILES_DIR": (("flows2fim", "FLOW_FILES_DIR"), str),
    "RP_FLOWS2FIM_BIN_PATH": (("flows2fim", "FLOWS2FIM_BIN_PATH"), str),
    "RP_GDAL_BINS_PATH": (("flows2fim", "GDAL_BINS_PATH"), str),
    "RP_GDAL_SCRIPTS_PATH": (("flows2fim", "GDAL_SCRIPTS_PATH"), str),
    "RP_QC_TEMPLATE_QGIS_FILE": (("qc", "QC_TEMPLATE_QGIS_FILE"), str),
    "RP_OPTIMUM_PARALLEL_PROCESS_COUNT": (("execution", "OPTIMUM_PARALLEL_PROCESS_COUNT"), int),
}


def load_env(override=False):
    """Load .env, located independent of cwd (search up from cwd, else repo root)."""
    load_dotenv(find_dotenv(usecwd=True) or str(_PKG.parent / ".env"), override=override)


def user_config_path() -> Path:
    return Path(os.getenv("RP_CONFIG_PATH", DEFAULT_USER_CONFIG))


def _deep_merge(base: dict, over: dict) -> dict:
    out = dict(base)
    for k, v in (over or {}).items():
        out[k] = _deep_merge(out[k], v) if isinstance(out.get(k), dict) and isinstance(v, dict) else v
    return out


def _set_nested(cfg: dict, keys: tuple, value) -> None:
    d = cfg
    for k in keys[:-1]:
        d = d.setdefault(k, {})
    d[keys[-1]] = value


def _overlay_env(cfg: dict) -> dict:
    for env_var, (keys, cast) in _ENV_OVERLAY.items():
        raw = os.getenv(env_var)
        if raw is None:
            continue
        try:
            value = cast(raw)
        except (TypeError, ValueError):
            raise ValueError(f"{env_var}={raw!r} is not a valid {cast.__name__}")
        _set_nested(cfg, keys, value)
    return cfg


def load_config() -> dict:
    """Return the merged config: defaults <- optional config.yaml <- environment."""
    load_env()
    cfg = yaml.safe_load(DEFAULTS_PATH.read_text()) or {}
    p = user_config_path()
    if p.exists():
        cfg = _deep_merge(cfg, yaml.safe_load(p.read_text()) or {})
    return _overlay_env(cfg)
