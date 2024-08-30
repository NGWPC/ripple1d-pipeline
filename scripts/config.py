# config.py

# ENV Variable # to do move to .env file
AWS_PROFILE = ""

# API Base URL
RIPPLE1D_API_URL = "http://localhost"

# STAC API settings
STAC_ENDPOINT = ""
STAC_COLLECTION = "ripple_test_data"

# Ripple settings
RAS_VERSION = "631"
DEPTH_INCREMENT = 1
RESOLUTION = 3.0
RESOLUTION_UNITS = "Meters"
TERRAIN_SOURCE_URL = ""

# Payload templates
PAYLOAD_TEMPLATES = {
    "extract_submodel": {
        "source_model_directory": "{source_model_directory}\\{model_key}",
        "submodel_directory": "{submodels_directory}\\{nwm_reach_id}",
        "nwm_id": "{nwm_reach_id}",
    },
    "create_ras_terrain": {
        "submodel_directory": "{submodels_directory}\\{nwm_reach_id}",
        "resolution": RESOLUTION,
        "resolution_units": RESOLUTION_UNITS,
        "terrain_source_url": TERRAIN_SOURCE_URL,
    },
    "create_model_run_normal_depth": {
        "submodel_directory": "{submodels_directory}\\{nwm_reach_id}",
        "plan_suffix": "ind",
        "num_of_discharges_for_initial_normal_depth_runs": 10,
        "ras_version": RAS_VERSION,
    },
    "run_incremental_normal_depth": {
        "submodel_directory": "{submodels_directory}\\{nwm_reach_id}",
        "plan_suffix": "nd",
        "depth_increment": DEPTH_INCREMENT,
        "ras_version": RAS_VERSION,
    },
}

# Poll wait time for job status checks
DEFAULT_POLL_WAIT = 3

API_LAUNCH_JOBS_WAIT_RANGE = [0, 120]
RIPPLE1D_THREAD_COUNT = 90

DB_CONN_TIMEOUT = 30
