# Design Guide

The purpose of this file is to capture the reasoning behind the
major design decisions so it doesn't have to be re-derived (or re-argued) later. This is not a Readme or setup guide.

---

## Project layout

The repo is mainly separated into

`tools` building blocks are condensed in package is a flat directory at the repo root, `ripple1d_pipeline/`, imported as `ripple1d_pipeline`.

1. `entrypoints` - scripts and notebooks that are the ways to run the pipeline. Notebooks are in a separate folder to separate .py files from .ipynb
2. `ripple1d_pipeline` - package containing building block for the pipeline. This is the package name, so it can't be `src`. Recommended `src/ripple1d_pipeline/`layout is not adapted because the benefit of src/pkg-name (not having pkg in sys.path so that imported pkg is always from the install not from the src code) does not out weight the simplicity here. This is not a distributed library, we always want to run against the source code anyways. This could be debated but we are deciding to go with a simpler approach.
3. `tools` - completely separate and independent tools. No code from here is being used in main pipeline.

- Putting notebooks and entrypoints scripts into a folder means `import ripple1d_pipeline` won't work unless we have an **install** i.e. the package is installed into the environment so `import ripple1d_pipeline` resolves from any working directory (notebooks included). We do this by editible install. This is _not_ publishing, it just registers the local package.

## Logging

- **Named loggers.** Every module uses `logging.getLogger(__name__)`, so a log line names its source (`ripple1d_pipeline.process.job_client`). Entry scripts run as `__main__`, so they name their logger explicitly (`run_collection`, `run_batch`) to match `PROJECT_LOGGERS`.
- **Third-party logging** is managed separately from this repos logging so that separate logging level can be configured.

## Configuration

- Config that varies between deploys are managed through the environment variable. This keeps per-machine values (`C:\Users\...`) out of source code entirely.
- We read config and env one time at load so as not to have scattered `os.getenv` calls.
- Both YAML files and `.env` are located relative to the package, not the current working directory, so loading works identically from a script, a notebook, or a tool in any directory.

## Dependencies & environment (pixi)

This repo adopt `pixi` because it manage the whole enviornment in cluding non python dependencies, such as the GDAL command-line tools — originally we had OSGeo4W shell separate setup but it was brittle. `uv` alone can't do this as there is no reliable GDAL CLI on PyPI for Windows.\
\
`pixi` also provide benefit of not having to activate environment separately which we were doing before with .venv setup.