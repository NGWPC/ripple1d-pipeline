# Ripple1D Pipeline: OWP Deployment Scope

> **Scoping document.** Defines what OWP needs to run the pipeline, and feeds two
> deliverables: `deployment/Deployment_Runbook.md` (how to stand it up) and
> `deployment/Verification_Guide.md` (how to prove it works). Open items and decisions are
> collected in §9 — none block handoff; **[STUB]** marks a value that must be filled before
> the runbook can be written.

## Purpose

Scopes what OWP needs — infrastructure, dependencies, reference data, credentials — to run
the pipeline in an OWP-owned AWS account, and how the two downstream documents divide it.

---

## 1. Overview

Ripple1D Pipeline converts published HEC-RAS models into FIM libraries and rating curves.
Unlike the containerized, Linux-based `auto-eval-coordinator` and `hec-ras-stac`
deployments, it is a **single-node Windows workload** — no cluster, no scheduler, no
container runtime. Each instance processes collections one at a time and writes outputs to
S3. The delivered scope is **single-instance** (§4).

The table below maps the system's components; each row points at the section that scopes its
requirements and migration.

| Component | What it is | Scoped in |
|-----------|-----------|-----------|
| Compute | Windows EC2, `c6i.8xlarge` (32 vCPU), 500 GB `gp2` root, RDP access | §3 |
| Image | Windows w/ Desktop Experience, built from an approved base | §3.1 |
| Orchestrator | This repo (`batch_ripple_pipeline.py` → `ripple_pipeline.py`) + `ripple1d==0.10.4` engine | §2 |
| Model source | `hec-ras-stac` STAC API (`RP_STAC_URL`) + model GeoPackages on S3 | §2, §5 |
| Reference data | DEM, NWM flowlines, flow files, bridge index, QC template | §5 |
| Monitoring | Local SQLite DB (`MONITORING_DB_PATH`); no shared storage | §4 |
| Credentials | `.env`, STAC/S3 access, output prefixes | §6 |
| Outputs | S3, one `aws s3 mv` per collection | §5, §6 |

Appendix B details how the engine runs on a single instance — the per-step job sequence and
concurrency tuning.

---

## 2. Dependencies

The pipeline source lives at `NGWPC/ripple1d-pipeline` (`origin`), with `upstream` already
pointing at `NOAA-OWP/ripple1d-pipeline` — the handoff target. Confirm the OWP fork is
current before delivery. Below is what it depends *on*.

| Dependency | Role | Coupling | Handoff status |
|------------|------|----------|----------------|
| [`NGWPC/hec-ras-stac`](https://github.com/NGWPC/hec-ras-stac) | **Input source.** Serves the model catalog queried via `RP_STAC_URL`; its assets are the GeoPackages the pipeline downloads | Schema contract (below) | Its own OWP deployment. **Must be deployed + catalog loaded before Phase 3.** `RP_STAC_URL` is one of its outputs, not a value to invent |
| [`Dewberry/ripple1d`](https://github.com/dewberry/ripple1d) | Compute engine; provides the API this repo calls | **Version-pinned** `0.10.4`; moves together with `RAS_VERSION` and the `config.yaml` step payloads | Public on PyPI + GitHub. No action |
| [`NGWPC/flows2fim`](https://github.com/NGWPC/flows2fim) | Composite FIM generation in QC | Binary **`v0.4.1`**, invoked via `subprocess` | Authoritative source; [`v0.4.1` release](https://github.com/NGWPC/flows2fim/releases/tag/v0.4.1) published. Stage `flows2fim.exe` into the image (§3.1) |
| HEC-RAS 6.3.1 | Hydraulic engine invoked by ripple1d | Hard pin (`RAS_VERSION: "631"`) | Public USACE download. GUI EULA step in §3.1 (row 2) |
| OSGeo4W / GDAL | Raster ops in bridge masking, extent library, flows2fim | Absolute paths pinned in `config.yaml` | Public installer. Paths must match the image layout |

**Upgrade risk:** a ripple1d minor bump can change API process names or payload fields, so
`RIPPLE1D_VERSION`, `RAS_VERSION`, and the `config.yaml` step payloads move together — use
repository tags for older versions.

The download URI is built as `s3://{RP_S3_KEY_PREFIX}{s3_key}` — the item's `href` is *not*
used. So the bucket is chosen by the `RP_S3_KEY_PREFIX` environment variable plus the
catalog's `s3_key`, not by a bucket baked into the catalog. OWP points this at its own bucket
after `hec-ras-stac` completes its asset migration; the instance role must be able to read
it.

---

## 3. Infrastructure & Image Scope

Proposed: **OWP builds their own image.** 
NGWPC's AMI may not port — it is understood that OWP standardizes on their own
approved Windows base. The machine is specified below **as requirements**, not as an artifact
to hand over.

**Open question — image starting point (§9 decision A).** Whether OWP reuses an existing
NGWPC AMI (`Ripple-NomadWorker-v9`, or a more recent build) or provisions fresh from their
approved base is **not yet settled**. Either way the machine must meet the §3.1 spec; the
requirements below hold regardless of which path is chosen. The team should resolve this
before the image work in Phase 1 begins.

### 3.1 Machine provisioning spec

Every requirement is load-bearing, and many fail deep into a run rather than at startup.

| # | Requirement | Why / constraint |
|---|-------------|------------------|
| 1 | Windows Server **with Desktop Experience** | ripple1d hard requirement — GUI, not Server Core |
| 2 | **HEC-RAS 6.3.1**, opened once in the GUI to accept the EULA | Pinned (`RAS_VERSION: "631"`). The EULA prompt is why the image cannot be headless |
| 3 | **Python ≥ 3.10** | ripple1d and this repo |
| 4 | **OSGeo4W/GDAL** at `C:\OSGeo4W\bin` and `C:\OSGeo4W\apps\Python312\Scripts` | Hard-coded in `config.yaml`. The **`Python312`** segment pins OSGeo4W's bundled Python — a different build changes this path |
| 5 | `C:\OSGeo4W\bin` writable by the running user | The operating guide widens this by hand per instance; bake it in |
| 6 | **`flows2fim.exe`** at `C:\OSGeo4W\bin\flows2fim.exe` | `FLOWS2FIM_BIN_PATH`. From [`flows2fim v0.4.1`](https://github.com/NGWPC/flows2fim/releases/download/v0.4.1/flows2fim-windows-amd64.zip) |
| 7 | Reference data staged to `C:\reference_data\` | §5. Large — the DEM VRT especially |
| 8 | AWS CLI | `batch_ripple_pipeline.py` shells out to `aws s3 mv` |

`config.yaml` encodes absolute Windows paths, so an image whose OSGeo4W layout differs needs
a `config.yaml` change, not just a rebuild — the image and `config.yaml` are coupled (§9,
operating caveats). **TC1 is this spec's acceptance test.**

### 3.2 Terraform

Terraform should own the durable, shared resources — not the instances, which are launched
and configured by hand (venvs, `.env`, `config.yaml`), have no ASG or bootstrap, and are
terminated manually (§7). A documented console/CLI launch is sufficient.

The resource list below is a **proposal pending team sign-off (§9 decision B)** — the team
must agree on exactly what Infra is asked to capture as IaC before the request is made. In
particular, whether the **`Z:` shared drive** (Windows FSx) is in scope is unresolved: this
single-instance deployment does not require it, but a decision is needed rather than an
omission by default.

| Resource | Notes |
|----------|-------|
| IAM instance role | S3 read (models, reference data), write (both output prefixes). Most consequential resource here — a missing write permission surfaces as silent data loss |
| Security group | RDP (3389) from the OWP workstation CIDR only. **No inbound app port** — the ripple1d API binds `127.0.0.1` |
| S3 buckets / prefixes | Output + failed-output prefixes, reference-data bucket |
| `Z:` shared drive (FSx) | **Undecided** — not required for single-instance (§4); include only if the team scopes it in |

**Out of scope:** no VPC/subnet creation (assumes an existing OWP VPC), no Route53, ALB,
ECS/EKS, RDS, or container registry. The pipeline has no network-listening service.
CloudWatch is not used; logs are per-instance local files.

---

## 4. Deployment Scale

**Scope: single-collection / single-instance processing.** The pipeline runs one collection
at a time on one instance with **no shared storage** — point `MONITORING_DB_PATH` at a local
path and it is fully self-contained. No FSx, no shared drive, no cross-instance coupling.

The `Z:` drive in the operating guide is not required here: the monitoring DB is local, and its
other `Z:` uses (source, `flows2fim.exe`, collection lists) are operator convenience covered
by `git clone`, the image, and S3/the repo.

---

## 5. Data & Migration

`s3://fimc-data` is **NGWPC-owned.** OWP instance roles will not have cross-account access,
so everything the pipeline reads from it must land in an OWP bucket first, and every path
referencing it must be rewritten. Mirrors the `fimc-data` migration in the `hec-ras-stac`
handoff.

### 5.1 Reference data

Every path is set in `config.yaml` and is **assumed present** (never fetched by the pipeline),
so a missing one fails deep into a run rather than at startup. The `config.yaml` values are
*destinations*: **all six datasets must be migrated to an OWP-owned location** — none can stay
on `fimc-data`. Two carry their `s3://fimc-data` source below; the other four are **[STUB]** —
NGWPC fills in the source path before handoff so OWP can sync them.

| Dataset | `config.yaml` key | Configured path | Upstream source | Migration |
|---------|-------------------|-----------------|-----------------|-----------|
| Seamless 3DEP DEM (3 m, EPSG:5070) VRT | `TERRAIN_SOURCE_URL` | `C:\reference_data\dem\seamless_3dep_dem_3m_5070.vrt` | **[STUB]** — large | migrate |
| NWM flowlines (parquet) | `NWM_FLOWLINES_PATH` | `C:\reference_data\nwm_flowlines.parquet` | **[STUB]** | migrate |
| NWM flowlines with bbox (parquet) | `SOURCE_NETWORK` | `C:\reference_data\nwm_flowlines_with_bbox.parquet` | **[STUB]** | migrate |
| NWM return-period flow files | `flows2fim.FLOW_FILES_DIR` | `C:\reference_data\flow_files` | **`s3://fimc-data/reference/nwm_return_period_flows`** | migrate |
| QGIS QC template | `qc.QC_TEMPLATE_QGIS_FILE` | `C:\reference_data\qc_map.qgs` | **[STUB]** — small | migrate |
| OWP bridge tile index | `BRIDGE_TILE_INDEX_PATH` | `/vsis3/fimc-data/...` — **not staged locally** | **`s3://fimc-data/reference/owp_bridge_data/bridge_index.parquet`** | migrate (rewrite path, §5.3) |


### 5.2 What this deployment does *not* migrate

**Model GeoPackages.** They live at `s3://fimc-data/hv-fim-dev-data/hec-ras/` —
`hec-ras-stac`'s runbook already copies them to OWP's `hv-fim-dev-data`. Because the pipeline
builds its download URI from the asset's `s3_key` plus `RP_S3_KEY_PREFIX`, pointing at the
OWP bucket is a config change, not a copy. This is a **sequencing dependency** —
`hec-ras-stac` must finish first, and the instance role must read the resulting bucket.
Verified by TC5 (the `s3://{RP_S3_KEY_PREFIX}{s3_key}` read).

**Output prefixes.** The operating guide's production run wrote to
`s3://fimc-data/ripple/fim_100_domain_0_10_4` and `.../failed_collections_0_10_4`. These are
**destinations, not sources** — OWP creates its own and sets both `config.yaml` keys. No data
moves: `fim100` is being regenerated, so the existing NGWPC output does not need copying.

**`tools/`.** `tools/rc_points/` defaults to `s3://fimc-data/ripple/fim_100_domain/collections`
and `tools/extent_library/` takes an S3 root as a parameter. Both are downstream analyses over
a prior NGWPC run, not pipeline inputs, and are not required to deploy or run.

### 5.3 Migration checklist

1. Create the OWP reference bucket; `aws s3 sync` all six §5.1 datasets from their source
   (temporary NGWPC read credentials required, as in the `hec-ras-stac` handoff). NGWPC fills
   in the four **[STUB]** source paths in §5.1 before handoff (§9, NGWPC to record).
2. Update `config.yaml: BRIDGE_TILE_INDEX_PATH` to the OWP `/vsis3/` path. It is read from S3
   at runtime and never staged to disk, so rewrite the path — do not copy the file.
3. Update the `aws s3 sync` source in the README / runbook flow-files step.
4. Create OWP output destination buckets; set `S3_UPLOAD_PREFIX` and `S3_UPLOAD_FAILED_PREFIX`.
5. Confirm `hec-ras-stac` has completed its own migration (§5.2).
6. Grant the instance role: read on the OWP reference bucket + model asset bucket, write on
   both output prefixes.

**Gate:** no `fimc-data` reference remains in `config.yaml`, `.env`, or the runbook, and
`aws sts get-caller-identity` + a read of each OWP bucket succeeds from an instance (TC5).

---

## 6. Credentials & Configuration

`.env` is copied from `example.env`:

| Variable | Purpose | Note |
|----------|---------|------|
| `RP_RIPPLE1D_API_URL` | ripple1d Flask API base URL | Default `http://127.0.0.1` — correct as-is; loopback only, no port needed |
| `RP_STAC_URL` | STAC API serving the model catalog | OWP's `hec-ras-stac` instance (§2), not `stac2.dewberryanalytics.com` |
| `RP_STAC_AWS_ACCESS_KEY_ID` / `_SECRET_ACCESS_KEY` / `_REGION` | S3 read for model `.gpkg` download | **Leave empty or omit on EC2** — see below |
| `RP_S3_KEY_PREFIX` | Optional bucket/prefix prepended to the STAC asset's `s3_key` | Commented out by default. Useful for testing against a different bucket |

**Credential model — use the instance role.** `STACImporter` builds a `boto3.Session` from
`RP_STAC_AWS_*`; when those are empty or omitted, boto3 falls through to the default
credential chain, so the EC2 **instance role** is used. `batch_ripple_pipeline.py` already
uses the ambient chain for `aws s3 mv`. On EC2, leave `RP_STAC_AWS_*` blank — `example.env`
documents this ("Provide empty string or omit for direct access") — and grant the instance
role read on the model bucket.

**Empty prefixes are a data-loss trap.** `S3_UPLOAD_PREFIX` / `S3_UPLOAD_FAILED_PREFIX`
default to `""`. With an empty prefix `batch_ripple_pipeline.py` issues
`aws s3 mv <src> /<collection>` — a **local move** — with `stderr` sent to `DEVNULL`. Setting
both is not optional, and failure is invisible.

---

## 7. Operational Procedure (Runbook Skeleton)

Derived from the latest Ripple1D Pipeline operating guide, restructured into phases with
gates. This is the **ordered procedure** the Runbook expands — all eight phases, build
through shutdown. Each phase's **Gate** cites a §8 test case; the gate is the interface to
the Verification Guide, which holds the pass criteria (§8 boundary note).

| Phase | Content | Gate |
|-------|---------|------|
| **0. Prerequisites** | OWP account, VPC/subnet, RDP path, **`fimc-data` → OWP S3 migration (§5.3)**, smoke-test collection chosen, **`hec-ras-stac` deployed + catalog loaded** | §5.3 gate passes; `RP_STAC_URL` reachable and returns the smoke-test collection |
| **1. Build image** | Provision OWP's approved Windows base to the §3.1 spec | TC1 passes on a launched instance |
| **2. Account scaffolding** | Terraform: IAM role, security group, S3 buckets (§3.2). Launch instance from the Phase 1 image | `terraform apply` clean; instance reachable via RDP |
| **3. Per-instance setup** | ripple1d venv; clone repo to `C:\ripple1d-pipeline`; pipeline venv; `pip install -r requirements.txt`; `.env`; `config.yaml` | TC2–TC5 pass |
| **4. Smoke test** | `ripple_pipeline.py -c <smoke-collection>` | TC6–TC8 pass |
| **5. Batch run** | `batch_ripple_pipeline.py -l <list>` over the collection list | TC9–TC10 pass |
| **6. Monitoring** | Inspect `instances` / `collections` tables | — |
| **7. Shutdown** | Verify `C:\collections` file count is 0, then terminate (see shutdown note below) | File count 0 |

Two non-obvious details that must survive into the runbook:

- **Restart hygiene.** Before re-running on a dirty instance: stop the ripple1d API, delete
  `C:\Users\<user>\jobs*` and `C:\Users\<user>\server-logs`, clear `C:\collections`.
- **Shutdown is gated on S3 drain, not on the pipeline exiting.** `batch_ripple_pipeline.py`
  fires `aws s3 mv` via `subprocess.Popen` **without waiting**, output discarded. Terminating
  when the pipeline exits silently loses outputs — the guide's "count files until 0" loop
  exists for this, and it is the most dangerous step to omit.

> **Windows shell:** run from **Command Prompt, not PowerShell** (per README) —
> `subprocess.run(..., shell=True)` with list arguments quotes differently between them.

---

## 8. Verification Strategy

**Document boundary.** The two deliverables interlock at the **gate**, not by splitting the
phases between them. The **Runbook** owns the ordered procedure for *every* phase 0–7 — the
commands to build, configure, smoke-test, batch, monitor, and shut down. The **Verification
Guide** owns the **pass criteria**: the TC1–TC10 matrix below, the known-acceptable failures,
and production sign-off. Each Runbook phase names a gate (§7); that gate cites a TC id; the
TC's pass criteria live here. Rule of thumb — *how to do it* is the Runbook, *how to know it
worked* is the Verification Guide.

**Smoke-test collection.** The reference deployments each pin a known-good test unit; this one
needs an equivalent. The operating guide uses `mip_03160109`; the `ripple_pipeline.py` docstring
uses `ble_12100302_Medina`. One small, fast, known-good collection must be pinned in both
deliverables (§9, NGWPC to record). Selection criteria: few models, no 2D elements, English units,
has steady flow files — exactly the `STACImporter.filter_model` exclusions.

| ID | Test | Pass criteria |
|----|------|---------------|
| TC1 | Image spec (§3.1) | HEC-RAS 6.3.1 opens; `gdalinfo --version`; `flows2fim` on PATH; Python ≥3.10 |
| TC2 | ripple1d API health | `ripple1d start` yields Huey + Flask windows; API responds |
| TC3 | Reference data | Local `C:\reference_data\` paths exist; DEM VRT opens; bridge index readable via `/vsis3` (S3, not local) |
| TC4 | STAC connectivity | `curl -s -o /dev/null -w "%{http_code}" {RP_STAC_URL}/collections/<smoke-collection>` returns 200 — the handed-off `hec-ras-stac` catalog is reachable and serves the smoke-test collection |
| TC5 | AWS access | `aws sts get-caller-identity`; read a model asset `s3://{RP_S3_KEY_PREFIX}{s3_key}` (any `s3_key` from the smoke-test collection's items) via `aws s3 ls`; read model bucket; write both output prefixes |
| TC6 | Single-collection run | `ripple_pipeline.py -c <smoke-collection>` completes; expected outputs present; **reach completion count matches the collection's total** (Appendix A) |
| TC7 | Error reporting | `failed_jobs_report.xlsx` / `timedout_jobs_report.xlsx` generated |
| TC8 | flows2fim QC | Composite FIM rasters + control CSVs in `qc/` |
| TC9 | Batch + monitoring | `batch_ripple_pipeline.py` records rows in `instances` and `collections` tables |
| TC10 | S3 upload | Collection appears under `S3_UPLOAD_PREFIX`; `C:\collections` drains to 0 files |

**Known-acceptable failures.** Not every failed job is a deployment defect; the Verification
Guide must say so or operators will chase noise.

- Models filtered at STAC import (2D, non-English units, no steady flow files) are **expected
  omissions**, logged by `STACImporter.filter_model`.
- Per-reach failures are expected at some rate and land in `failed_jobs_report.xlsx` rather
  than aborting the run (`execution.stop_on_error: False`).

---

## 9. Open Items & Decisions

Nothing here blocks handoff — the pipeline runs today on NGWPC infrastructure. These are
items to settle, grouped below by owner: a couple of **team decisions**, a few facts **NGWPC
records** in the deliverables before handoff, **tasks OWP executes** during standup, and
**operating caveats** the runbook and verification guide carry forward so a customer who did
not write the code handles them correctly.

### Team decisions (settle before the corresponding step)

| ID | Decision | Settle before | Where |
|----|----------|---------------|-------|
| A | Image starting point — reuse an NGWPC AMI (`Ripple-NomadWorker-v9` or newer) vs. provision fresh from OWP's approved base. The §3.1 spec holds either way | Phase 1 image work | §3 |
| B | Terraform / Infra request scope — agree exactly what Infra captures as IaC, including whether the `Z:` shared drive (FSx) is in scope (not required for single-instance). Decide rather than omit by default | The IaC request to Infra | §3.2 |

### NGWPC to record before handoff

| Item | Note | Where |
|------|------|-------|
| Reference-data source paths | DEM VRT, NWM flowlines, QGIS template — NGWPC records the source path for each in §5.1 so OWP can sync them | §5.1 **[STUB]** |
| Pin one smoke-test collection | `mip_03160109` (operating guide) vs `ble_12100302_Medina` (`ripple_pipeline.py` docstring) — choose one, set it in both deliverables | §8 |

### OWP executes during standup

| Item | Note | Where |
|------|------|-------|
| `fimc-data` → OWP S3 migration | Reference + model data copied to OWP buckets | §5.3 |
| `hec-ras-stac` deployed + catalog loaded | Sequencing dependency; `RP_S3_KEY_PREFIX` then points at the OWP asset bucket | §2, §5.2 |
| Build the Phase-1 image | Largest single work item, whichever starting point decision A picks | §3.1, TC1 |

### Operating caveats (carried by the deliverables)

| Item | Note | Where |
|------|------|-------|
| Image ↔ `config.yaml` path coupling | A differing OSGeo4W layout needs a `config.yaml` edit, not just a rebuild | §3.1 |
| **Warning:** set both S3 upload prefixes | Empty prefix → silent local move, `stderr` discarded | §6 |
| **Warning:** drain S3 before shutdown | Terminating before `C:\collections` hits 0 loses async uploads | §7 |

---

## 10. FIMC Deliverables

| Document | Contents | Source material |
|----------|----------|-----------------|
| `deployment/Deployment_Runbook.md` | The **ordered procedure**: engine internals + sizing (Appendices A–B), §3.1 image spec, §3.2 Terraform, §5 data & migration, §6 config, **§7 phases 0–7** with gates (build → shutdown), troubleshooting, rollback | Operating guide; README; Appendices A–B; [`NGWPC/hec-ras-stac`](https://github.com/NGWPC/hec-ras-stac) `deployment/Deployment_Runbook.md` for phase/gate structure |
| `deployment/Verification_Guide.md` | The **pass criteria**: §8 test matrix TC1–TC10 (the checks each §7 gate cites), known-acceptable failures, production sign-off | [`NGWPC/auto-eval-coordinator`](https://github.com/NGWPC/auto-eval-coordinator) `docs/Verification_Guide.md` for Part 1 / Part 2 split |

Both should carry a **DRAFT disclaimer** until executed end-to-end against a live deployment.

---

## Appendix A: Instance Sizing Benchmark

> Raw reference data, captured here for preservation. To be folded into the Deployment
> Runbook as sizing guidance. Establishes the `c6i.8xlarge` / `22` production default and
> maps the failure boundary.

**How to read this table.** Every row processes the **same single test collection**, which
contains **1135 NWM reaches**. The final column is the count of reaches that completed
successfully: `1135` means every reach finished, and any lower number means reaches
**failed**. Wall time and cost are therefore the time and dollar cost to process that one
collection — not a throughput rate.

The last column is a **correctness signal, not a productivity one.** A run reporting `846`
did not do less work per hour; it failed ~25% of its reaches. Cheap rows that drop reaches
are not bargains.

| Instance | vCPU | Threads | Wall time | $/hr | Cost | Reaches (of 1135) |
|----------|-----:|--------:|----------:|-----:|-----:|--------|
| c6i.8xlarge | 32 | **22** | 9:33:54 | 1.36 | **13.01** | **1135** *(production default)* |
| c6i.8xlarge | 32 | 26 | 8:13:01 | 1.36 | 11.18 | 1133 — 2 lost |
| c6i.8xlarge | 32 | 36 | 9:05:42 | 1.36 | 12.37 | **1135** |
| c6i.8xlarge | 32 | 32 | 8:33:14 | 1.36 | 11.63 | 1081 — 54 lost |
| c6i.8xlarge | 32 | 48 | 9:39:36 | 1.36 | 13.14 | 1132 — 3 lost |
| c6a.8xlarge | 32 | 32 | 11:00:35 | 1.22 | 13.43 | 1092 — 43 lost |
| c7a.8xlarge | 32 | 32 | 9:08:14 | 1.64 | 14.99 | 1121 — 14 lost |
| c7i.8xlarge | 32 | 32 | 8:37:24 | 1.42 | 12.25 | 1088 — 47 lost |
| c7i.8xlarge | 32 | 48 | 4:50:53 | 1.42 | 6.88 | 846 — **289 lost** *hyperthread test* |
| m7a.8xlarge | 32 | 32 | 8:58:46 | 1.85 | 16.61 | **1135** |
| m7i.8xlarge | 32 | 32 | 9:16:00 | 1.61 | 14.92 | **1135** |
| m7i.8xlarge | 32 | 22 | 8:44:28 | 1.61 | 14.07 | **1135** |
| m7i.8xlarge | 32 | 26 | 8:41:07 | 1.61 | 13.98 | **1135** |
| r7i.8xlarge | 32 | 22 | 8:30:19 | 2.11 | 17.95 | **1135** |
| c6i.12xlarge | 48 | 48 | 6:27:13 | 2.04 | 13.17 | 1077 — 58 lost |
| c6i.12xlarge | 48 | 40 | 8:33:07 | 2.04 | 17.45 | **1135** |
| m7i.12xlarge | 48 | 36 | 9:05:03 | 2.41 | 21.89 | **1135** |
| c6i.16xlarge | 64 | 58 | 6:30:35 | 2.72 | 17.71 | 1048 — 87 lost |
| c6i.16xlarge | 64 | **64** | — | 2.72 | — | **`ValueError: need at most 63 handles`** |
| c6i.24xlarge | 96 | 32 | 8:36:00 | 4.08 | 35.09 | **1135** |
| c6i.24xlarge | 96 | 44 | 10:23:33 | 4.08 | 42.40 | **1135** |
| c6i.24xlarge | 96 | 48 | 9:42:41 | 4.08 | 39.62 | **1135** |
| c6i.24xlarge | 96 | 58 | 10:35:52 | 4.08 | 43.24 | **1135** |
| c6i.24xlarge | 96 | **64** | — | 4.08 | — | **`ValueError: need at most 63 handles`** |

**1. Scaling up the instance buys nothing.** `c6i.24xlarge` (96 vCPU) processes the test
collection in 10:23:33 for **$42.40**; `c6i.8xlarge` (32 vCPU) does it in 9:33:54 for
**$13.01** — 3× the cost to run *slower*. The fastest 24xlarge row (8:36:00, $35.09) beats
the 8xlarge by under an hour at ~2.7× the price. **Scale horizontally, never vertically.**

**2. Thread count barely moves wall time — even in the case that should favor it.** Threads
only help steps where a collection has **more reaches than threads**. This test collection
has 1135 reaches against ≤64 threads, so it is the *most* thread-favorable workload
available, and scaling still flattens out. Real collections are frequently smaller than the
thread count, where extra threads do nothing at all. The workload is bounded by serial
per-reach HEC-RAS execution, not by available parallelism.

**3. Oversubscribing physical cores costs reaches, not just speed.** The c7i hyperthread
test (48 threads on 32 vCPU) posted the fastest wall time in the table (4:50:53) but
completed only **846 of 1135 reaches**. It "won" by failing 289 of them. Read every
sub-1135 row the same way.

**4. `22` is chosen for reach completeness, not minimum cost.** The 26-thread run is cheaper
($11.18 vs. $13.01) but loses 2 reaches; 32 threads loses 54. **The acceptance criterion is
1135/1135, and cost is the tiebreaker among rows that hit it.** Among full-completion rows,
`c6i.8xlarge` at 22 is the cheapest in the table.

**5. The two crash rows are a hard Windows limit**, not a tuning artifact — see the
concurrency constraints in [Appendix B](#appendix-b-engine-internals).

> **Caveat:** `valid_entities` counts timed-out reaches as valid alongside succeeded ones,
> so `1135` means "no reach was dropped," not strictly "1135 clean successes." Per-reach
> failures are captured in `failed_jobs_report.xlsx`.

---

## Appendix B: Engine Internals

Reference detail on how the pipeline runs on a single instance. Not required to scope the
deployment; useful for the Runbook and for anyone tuning a batch.

### Execution model

`ripple1d start --thread_count 22` launches a local Flask API and Huey task queue.
`ripple_pipeline.py` drives that API over HTTP as ~11 steps, each dispatching one job per
model or per NWM reach and polling to completion:

```
setup:   STAC query → download model .gpkg → filter NWM reaches → init ripple.gpkg
process: conflate_model → extract_submodel → create_ras_terrain
         → create_model_run_normal_depth → run_incremental_normal_depth
         → nd_create_rating_curves_db → run_iknown_wse → run_known_wse
         → kwse_create_rating_curves_db → create_fim_lib
         → bridge masking → extent library → flows2fim start file
qc:      failed/timed-out job reports → flows2fim composite FIMs → QGIS QC map
```

`batch_ripple_pipeline.py` wraps this: per collection it shells out to
`ripple_pipeline.py`, records status in the monitoring DB, then issues `aws s3 mv` to push
outputs to `S3_UPLOAD_PREFIX` (success) or `S3_UPLOAD_FAILED_PREFIX` (failure), freeing local
disk for the next collection.

### Concurrency

Two independent parameters, both `22` in production — **not the same knob**:

| Parameter | Set via | Controls |
|-----------|---------|----------|
| `--thread_count` | `ripple1d start --thread_count 22` | Huey worker threads running HEC-RAS jobs |
| `OPTIMUM_PARALLEL_PROCESS_COUNT` | `config.yaml` | Pipeline-side pools in the ikwse, extent-library, bridge-masking steps |

Both are empirically tuned for `c6i.8xlarge` (Appendix A). Two constraints follow:

- **Scale horizontally, never vertically.** Larger instances cost ~3× for no throughput gain.
- **Windows caps `multiprocessing` pool size.** Keep `OPTIMUM_PARALLEL_PROCESS_COUNT` ≤ ~31
  and `--thread_count` ≤ ~58 on any instance type. Exceeding it crashes bridge masking,
  which fails silently and still reports the collection successful — see the known-acceptable
  failures in §8.
