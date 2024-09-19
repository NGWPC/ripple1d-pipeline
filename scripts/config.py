# config.py

# ENV Variable # to do move to .env file
AWS_PROFILE = ""

# API Base URL
RIPPLE1D_API_URL = "http://10.9.0.18:80"

# STAC API settings
STAC_ENDPOINT = "https://stac2.dewberryanalytics.com"
STAC_COLLECTION = "ripple_test_data"

# Ripple settings
RAS_VERSION = "631"
DEPTH_INCREMENT = 1
RESOLUTION = 3.0
RESOLUTION_UNITS = "Meters"
TERRAIN_SOURCE_URL = r"Z:\reference_data\seamless_3dep_dem_3m_5070.vrt"
SOURCE_NETWORK = r"Z:\reference_data\nwm_flowlines_with_bbox.parquet"
SOURCE_NETWORK_VERSION = "2.1"
SOURCE_NETWORK_TYPE = "nwm_hydrofabric"

# Payload templates
PAYLOAD_TEMPLATES = {
    "conflate_model": {
        "source_model_directory": "{source_model_directory}\\{model_id}",
        # "model_name": "{model_name}",
        "source_network": {"file_name": SOURCE_NETWORK, "version": SOURCE_NETWORK_VERSION, "type": SOURCE_NETWORK_TYPE},
    },
    "extract_submodel": {
        "source_model_directory": "{source_model_directory}\\{model_id}",
        "submodel_directory": "{submodels_directory}\\{nwm_reach_id}",
        "nwm_id": "{nwm_reach_id}",
    },
    "create_ras_terrain": {
        "submodel_directory": "{submodels_directory}\\{nwm_reach_id}",
        # "resolution": RESOLUTION,
        # "resolution_units": RESOLUTION_UNITS,
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

# Flows2FIM
FLOW_FILES_DIR = r"Z:\reference_data\flow_files"
FLOWS2FIM_BIN_PATH = r"C:\OSGeo4W\bin\flows2fim.exe"
GDAL_BINS_PATH = r"C:\OSGeo4W\bin"
GDAL_SCRIPTS_PATH = r"C:\OSGeo4W\apps\Python312\Scripts"

# QC
QC_TEMPLATE_QGIS_FILE = r"Z:\reference_data\qc_map.qgs"

# Poll wait time for job status checks
DEFAULT_POLL_WAIT = 3

API_LAUNCH_JOBS_RETRY_WAIT = 0.5
RIPPLE1D_THREAD_COUNT = 50

DB_CONN_TIMEOUT = 85
