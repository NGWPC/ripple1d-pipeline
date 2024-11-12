# config.py
import os

# ENV Variable # to do move to .env file
AWS_PROFILE = ""

# URLs
RIPPLE1D_API_URL = ""
STAC_URL = ""
RIPPLE1D_VERSION = "0.7.0"

# Specs
COLLECTIONS_ROOT_DIR = r"Z:\collections"
NWM_FLOWLINES_PATH = r"Z:\reference_data\nwm_flowlines.parquet"

OPTIMUM_PARALLEL_PROCESS_COUNT = 5
# Ripple settings
RAS_VERSION = "631"
US_DEPTH_INCREMENT = 0.5
DS_DEPTH_INCREMENT = 1
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
    "run_known_wse": {
        "submodel_directory": "{submodels_directory}\\{nwm_reach_id}",
        "plan_suffix": "kwse",
        "min_elevation": -9999,
        "max_elevation": -9999,
        "depth_increment": DS_DEPTH_INCREMENT,
        "ras_version": "631",
        "write_depth_grids": True,
    },
    "create_rating_curves_db": {
        "submodel_directory": "{submodels_directory}\\{nwm_reach_id}",
        "plans": ["kwse"],
    },
    "create_fim_lib": {
        "submodel_directory": "{submodels_directory}\\{nwm_reach_id}",
        "plans": ["nd", "kwse"],
        "resolution": RESOLUTION,
        "resolution_units": RESOLUTION_UNITS,
        "library_directory": "{library_directory}",
        "cleanup": True,
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
DEFAULT_POLL_WAIT = 5

API_LAUNCH_JOBS_RETRY_WAIT = 0.5
DB_CONN_TIMEOUT = 30
