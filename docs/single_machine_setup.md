# Ripple1D Pipeline: OWP Operational Guide

Step-by-step guide to run the Ripple1D Pipeline on a single Windows machine.
This guide covers local setup and processing.
S3 upload and multi-machine scaling are covered in optional sections at the end.

## Prerequisites

The following must be installed before starting:

- Windows Server with Desktop Experience (GUI, not headless)
- HEC-RAS v6.3.1, opened once in the GUI to accept the EULA
- Git
- AWS CLI (only needed for the optional S3 section)

## 1. Install pixi

Open a Command Prompt (`cmd`, not PowerShell) and run:

```cmd
powershell -ExecutionPolicy ByPass -c "irm -useb https://pixi.sh/install.ps1 | iex"
```

Close and reopen the terminal so `pixi` is on PATH.
Verify with:

```cmd
pixi --version
```

## 2. Clone the repository

```cmd
git clone --branch v0.11.0-rc.2 https://github.com/NGWPC/ripple1d-pipeline.git C:\ripple1d-pipeline
cd /d C:\ripple1d-pipeline
```

> **Note:** The pipeline and ripple1d versions must match. Use the same release tag for both.

## 3. Install the pipeline environment

```cmd
pixi install
```

This installs Python, GDAL, flows2fim, and all Python dependencies into a project-local environment.
No virtual environment activation is needed -- `pixi run <command>` handles it automatically.

Verify GDAL is working:

```cmd
pixi run gdalinfo --version
```

## 4. Install ripple1d

Ripple1d runs as a separate server and needs its own Python environment.

```cmd
mkdir C:\venvs
cd /d C:\venvs
python -m venv ripple1d
cd ripple1d
Scripts\activate.bat
pip install git+https://github.com/NGWPC/ripple1d.git@v0.11.0-rc.2
```

> **Note:** Replace `v0.11.0-rc.2` with the latest release tag from the [NGWPC/ripple1d releases](https://github.com/NGWPC/ripple1d/releases).

Verify the install:

```cmd
pip show ripple1d
```

## 5. Stage reference data

The pipeline expects reference data at fixed local paths.
Stage the following files on disk before running:

| Dataset | Expected path |
|---------|--------------|
| Seamless 3DEP DEM (3 m, EPSG:5070) VRT | `C:\reference_data\dem\seamless_3dep_dem_3m_5070.vrt` |
| NWM flowlines (parquet) | `C:\reference_data\nwm_flowlines.parquet` |
| NWM flowlines with bbox (parquet) | `C:\reference_data\nwm_flowlines_with_bbox.parquet` |
| NWM return-period flow files | `C:\reference_data\flow_files\` |
| QGIS QC template | `C:\reference_data\qc_map.qgs` |
| OWP bridge tile index (parquet or geopackage) | `C:\reference_data\bridge_index.parquet` |

These paths are configured in `.env` and can be changed to match your layout.

> **Note:** The bridge tile index can also be read from S3 using a `/vsis3/` path (e.g. `/vsis3/bucket/path/bridge_index.parquet`), but local storage is recommended.

## 6. Configure the environment file

```cmd
cd /d C:\ripple1d-pipeline
copy example.env .env
```

Edit `.env` and set the values for your machine.

**Variables to set:**

| Variable | What to set |
|----------|-------------|
| `RP_STAC_URL` | URL of the STAC API serving the model catalog |
| `RP_RIPPLE1D_VERSION` | `0.11.0-rc.2` |
| `RP_COLLECTIONS_ROOT_DIR` | Local directory for processing output (e.g. `C:\collections`) |
| `RP_NWM_FLOWLINES_PATH` | Path to `nwm_flowlines.parquet` (from step 5) |
| `RP_TERRAIN_SOURCE_URL` | Path to the DEM VRT (from step 5) |
| `RP_SOURCE_NETWORK` | Path to `nwm_flowlines_with_bbox.parquet` (from step 5) |
| `RP_FLOW_FILES_DIR` | Path to the flow files directory (from step 5) |
| `RP_BRIDGE_TILE_INDEX_PATH` | Path to `bridge_index.parquet` or `.gpkg` (from step 5) |
| `RP_QC_TEMPLATE_QGIS_FILE` | Path to the QGIS QC template (from step 5) |
| `RP_OPTIMUM_PARALLEL_PROCESS_COUNT` | `22` for c6i.8xlarge (rule of thumb: half the cores; keep <= ~31) |
| `RP_STAC_S3_KEY_PREFIX` | Bucket/prefix prepended to the STAC item's `s3_key` to form the download path. Set based on how your STAC catalog stores `s3_key` values (e.g. `fimc-data/`). Leave blank if `s3_key` already includes the bucket. **Commented out in `example.env` -- uncomment if needed** |

**Variables to leave as default:**

| Variable | Default | Notes |
|----------|---------|-------|
| `RP_RIPPLE1D_API_URL` | `http://127.0.0.1` | Loopback to the local ripple1d server |
| `RP_MONITORING_DB_PATH` | `C:\path\to\monitoring.sqlite` | Placeholder; only used by `run_batch.py` (set a real path before batch runs) |
| `RP_FLOWS2FIM_BIN_PATH` | (blank) | pixi puts flows2fim on PATH automatically |
| `RP_LOG_LEVEL` | `INFO` | Set to `DEBUG` for troubleshooting |
| `RP_THIRD_PARTY_LOG_LEVEL` | `WARNING` | |

**Variables to leave blank (EC2 with instance role):**

| Variable | Notes |
|----------|-------|
| `RP_STAC_AWS_ACCESS_KEY_ID` | boto3 falls through to the instance role |
| `RP_STAC_AWS_SECRET_ACCESS_KEY` | |
| `RP_STAC_AWS_REGION` | |
| `RP_S3_UPLOAD_PREFIX` | Not used by `run_collection.py`; see the optional S3 section |
| `RP_S3_UPLOAD_FAILED_PREFIX` | Not used by `run_collection.py`; see the optional S3 section |

## 7. Start the ripple1d server

Open a **separate** Command Prompt window:

```cmd
cd /d C:\venvs\ripple1d
Scripts\activate.bat
ripple1d start --thread_count 22
```

Two new terminal windows will appear (Huey consumer and Flask API).
Minimize them but do not close them.
The server is ready when both windows are running.

> **Note:** `--thread_count` (Huey worker threads) and `RP_OPTIMUM_PARALLEL_PROCESS_COUNT` (pipeline-side pools) are independent controls -- both should be tuned for your instance type.
> Keep `--thread_count` below ~58 (Windows handle limit; crashes with `ValueError: need at most 63 handles`).

## 8. Run a single collection

Back in the original Command Prompt, from the repo root:

```cmd
cd /d C:\ripple1d-pipeline
pixi run python entrypoints/run_collection.py -c <collection_name>
```

For a test run, use a small known-good collection (e.g. `mip_05120110`).

This will:
1. Query the STAC catalog for models in the collection
2. Download model GeoPackages
3. Process each NWM reach (conflation, terrain, normal depth, known WSE, FIM library)
4. Run bridge masking and extent library
5. Generate QC outputs (error reports, composite FIMs via flows2fim)

Processing time depends on collection size.
A typical collection can take several hours on a c6i.8xlarge.

## 9. Verify output

After a successful run, check the collection output directory (e.g. `C:\collections\<collection_name>\`):

| Output | What to check |
|--------|---------------|
| `source_models/` | Downloaded source model data |
| `submodels/` | Extracted HEC-RAS submodels per NWM reach |
| `library/` | FIM depth rasters per reach, per flow |
| `library_extent/` | FIM extent rasters |
| `qc/` | Composite FIM rasters, QC map |
| `failed_jobs_report.xlsx` | Per-step failures encountered during processing |
| `timedout_jobs_report.xlsx` | Per-step timed-out jobs |
| `ripple.gpkg` | Database with reach, model, and rating curve records |
| `start_reaches.csv` | Flows2FIM start file |

Some per-reach failures are expected and appear in `failed_jobs_report.xlsx`.
These are not deployment defects.

## 10. Cleanup and restart

Before re-running on the same machine:

1. Stop the ripple1d server (close the Huey and Flask terminal windows)
2. Delete leftover job files:
   - `C:\Users\<username>\jobs*`
   - `C:\Users\<username>\server-logs`
3. Clear the collections output directory if needed
4. Restart the ripple1d server (step 7)

---

## Optional: S3 upload with batch processing

`run_batch.py` processes multiple collections in sequence and uploads results to S3 after each one.

### Additional prerequisites

- AWS CLI installed and on PATH
- IAM role or credentials with read access to the model bucket and write access to both output prefixes
- `RP_S3_UPLOAD_PREFIX` and `RP_S3_UPLOAD_FAILED_PREFIX` set in `.env`
- `RP_MONITORING_DB_PATH` set to a real path in `.env` (e.g. `C:\monitoring.sqlite`)

> **Warning:** Do not leave `RP_S3_UPLOAD_PREFIX` and `RP_S3_UPLOAD_FAILED_PREFIX` blank when using `run_batch.py`.
> Empty prefixes cause a silent local move that loses data.

### Configure S3 prefixes

In `.env`, set both prefixes (exclude trailing slash):

```
RP_S3_UPLOAD_PREFIX=s3://your-bucket/ripple/fim_output
RP_S3_UPLOAD_FAILED_PREFIX=s3://your-bucket/ripple/failed_collections
```

### Run a batch

Create a collection list file (one collection per line, `.lst` or `.txt`):

```cmd
pixi run python entrypoints/run_batch.py -l "C:\collection_lists\collections.lst"
```

### Shutdown procedure

After batch processing completes, **do not terminate the instance immediately**.
`run_batch.py` fires S3 uploads asynchronously -- they may still be in progress.

1. Check your `RP_COLLECTIONS_ROOT_DIR` folder properties -- wait until the file count reaches 0
2. If files remain and the count is decreasing, S3 upload is still running; wait
3. If files remain and the count is not decreasing, investigate which collections are stuck
4. Only shut down once the file count is 0

### Monitoring

The batch script writes to a local SQLite database at `RP_MONITORING_DB_PATH`.
It tracks processing status in two tables:

- `instances` -- overall progress (collections submitted, processed, succeeded)
- `collections` -- per-collection status, start/end time, errors

## Optional: Scaling to multiple machines

This guide covers single-machine processing.
To process multiple collections in parallel across machines:

1. Complete all steps above on one machine and verify it works
2. Create an AMI from the working machine
3. Launch additional instances from that AMI
4. Distribute the collection list across instances (non-overlapping subsets)
5. Each instance runs its own `run_batch.py` with its own monitoring database
