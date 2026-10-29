# Data Model Studio

A self-contained **Databricks App** for data modeling. One deploy gives you two things:

1. **Explore Unity Catalog** — pick one or many schemas and it reverse-engineers a live
   entity-relationship diagram (tables, columns, primary & foreign keys). Optionally attach
   Lakeflow (Spark Declarative) pipelines to see how the model is built. Edit the diagram on
   a drag-and-drop canvas, then have the agent generate the **DML** to realize your changes or
   an **updated pipeline**.
2. **Design a new model** — sketch entities and relationships by hand, or point the agent at a
   volume of raw files and let it propose a **medallion**, **star-schema**, or **Data Vault**
   model. Generate the Unity Catalog **DDL** and a **Spark Declarative Pipeline**.

The workspace has three resizable panels: **sources** (Unity Catalog schemas and pipelines in
Explore, the model's tables and *Build from raw files* in Design), the **canvas**, and
**Properties · Assistant · Code**. A sample model on the home page is a quick way to try it.
Light and dark themes; press `?` for keyboard shortcuts (`N` add table, `L` arrange, `0` fit,
`F` focus the canvas). Anything that would replace unsaved work asks first and offers an export.

The data-modeling agent is **Claude Opus 5, hosted on Databricks**
(`databricks-claude-opus-5`), with embedded data-modeling skills (see `backend/agent/skills/`).

---

## Architecture

```
React + Vite + React Flow (drag-drop ER canvas)      ← frontend/  (built to build/)
        │  JSON / SSE
FastAPI (backend/)
        ├─ Unity Catalog introspection & read_files()  → SQL warehouse (Statement Execution API)
        ├─ Pipelines: list / read source (notebook, file, glob libraries) / parse tables,
        │  lineage and join-inferred FKs, resolved onto the canvas     → backend/pipelines.py
        ├─ Deterministic codegen: DDL / DML migration / SDP           → backend/codegen.py
        ├─ Agent (ReAct loop on the SDK's OpenAI-compatible client, Opus 5) → backend/agent/
        └─ Self-provisioning bootstrap (schema / volume / job)        → backend/bootstrap.py
```

There is **no `databricks-connect`** dependency — all data access (metadata, constraints, and
file sampling via `read_files()`) runs on a SQL warehouse through the SDK. That keeps the app
process light and Apps-friendly.

---

## Prerequisites

- A Databricks workspace with:
  - the **`databricks-claude-opus-5`** foundation-model endpoint enabled,
  - a **SQL warehouse** (serverless recommended),
  - Unity Catalog.
- Databricks CLI ≥ 0.230 configured with a profile for the workspace
  (this repo's `databricks.yml` targets profile `fe-vm-houssem-demo`).
- Node ≥ 20.19 and Python ≥ 3.11 to build locally.

### Permissions the app's service principal needs

| To… | Grant |
|---|---|
| Query the agent | `CAN_QUERY` on `databricks-claude-opus-5` |
| Run introspection + `read_files()` | `CAN_USE` on the SQL warehouse |
| Browse & read metadata | `USE CATALOG` / `USE SCHEMA` / `SELECT` on the catalogs you explore |
| Self-provision | `CREATE SCHEMA` on `TARGET_CATALOG`, then `CREATE VOLUME` in that schema |
| Apply generated DDL | `CAN_MANAGE_RUN` on the apply job (granted by the bundle) |

The bundle's app resource declares the serving-endpoint / warehouse / job bindings, which grant
most of these automatically. The app **degrades gracefully** if a grant is missing — it reports
what failed in the **Workspace setup** dialog (the status pill in the top bar) rather than crashing.

---

## Build & deploy

> **Deploying to your own workspace (any cloud)?** Follow **[DEPLOY.md](DEPLOY.md)** — a
> complete, customer-ready guide (auth, model/warehouse selection, grants, first-run setup,
> troubleshooting). The quickstart below assumes this repo's preconfigured target.

### 1. Build the frontend

```bash
cd frontend
npm install
npm run build          # outputs ../build (served by the FastAPI backend)
```

> **npm registry note.** On a *complete* npm registry this installs cleanly. If you build behind
> the Databricks internal npm proxy (which currently mirrors only canary builds of the React-team
> shims `scheduler` and `use-sync-external-store`), keep the `overrides` block in
> `frontend/package.json`. On a public registry you can delete that block.

### 2. Deploy the bundle

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev        # creates the app, apply job, working schema + volume
databricks bundle run data_model_studio -t dev   # or open the app from the Apps UI
```

`bundle deploy` provisions everything declaratively (app, job, schema, volume). The app **also**
self-provisions on first run (idempotent) so it works even when re-run as its own service
principal — see the **Setup** pill in the top bar to view/trigger provisioning.

### Configuration (`app.yaml`)

| Env | Default | Purpose |
|---|---|---|
| `SERVING_ENDPOINT` | `databricks-claude-opus-5` | Agent model endpoint |
| `WAREHOUSE_ID` | *(auto)* | SQL warehouse; blank → auto-pick a running serverless one |
| `TARGET_CATALOG` | `houssem_demo_catalog` | Catalog the app provisions into |
| `TARGET_SCHEMA` | `data_model_studio` | Working schema it creates |
| `ARTIFACTS_VOLUME` | `artifacts` | Volume for generated artifacts + embedded skills |
| `EXEC_JOB_NAME` | `data_model_studio_apply` | Job the app triggers to apply DDL |
| `AUTO_BOOTSTRAP` | `true` | Provision automatically on startup |

---

## Local development

```bash
# backend (uses your CLI profile for workspace auth)
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
DATABRICKS_CONFIG_PROFILE=fe-vm-houssem-demo \
  WAREHOUSE_ID=<id> AUTO_BOOTSTRAP=false \
  .venv/bin/python -m uvicorn backend.main:app --reload --port 8000

# frontend (proxies /api → :8000)
cd frontend && npm run dev
```

Set `AUTO_BOOTSTRAP=false` locally so you don't create workspace resources while developing.

---

## How the agent generates code

- **Deterministic first.** `backend/codegen.py` produces full DDL (with idempotent PK/FK
  constraints), an additive migration diff (edits → `ALTER`/`ADD`), and an SDP `pipeline.py`
  scaffold. The **Code** tab uses these directly — instant and reliable.
- **Agentic for the hard parts.** The **Assistant** tab streams Opus 5 with tools to read live
  volumes, schemas, and pipelines, and to call the deterministic generators. It returns prose +
  code, and — when it changes the model — only the changed tables, which **Apply to canvas**
  merges into the diagram (nothing else on the canvas moves or disappears).

## Layout

```
app.yaml                 Databricks Apps runtime (command + env)
databricks.yml           Asset Bundle (variables + targets)
resources/               app / job / schema+volume resource definitions
requirements.txt         backend deps (no databricks-connect)
backend/                 FastAPI app, codegen, UC/pipeline/volume access, agent, bootstrap
  agent/skills/          embedded data-modeling skills (system, uc_modeling, sdp, patterns…)
  jobs/apply_notebook.py notebook the apply job runs
frontend/                React + Vite + React Flow source
build/                   compiled frontend (generated)
tests/                   pytest unit tests + functional_sweep.py (live API checks)
demo_data/tawal/         Tawal (KSA tower company) synthetic dataset + bronze/silver/gold pipelines
```

## Test data and checks

`demo_data/tawal/` builds a realistic test bed in `hec_demo_catalog`: raw CSV / JSON-lines
feeds in the `tawal_raw.landing` volume, a constraint-backed operational model
(`tawal_ref` + `tawal_ops`: composite, cross-schema and one-to-one FKs), and three Lakeflow pipelines
(`tawal-bronze-ingest` → `tawal-silver` → `tawal-gold`) in its own bundle.

```bash
python3 demo_data/tawal/generate_tawal.py          # writes demo_data/tawal/out/landing
.venv/bin/python -m pytest tests/ -q               # unit tests
.venv/bin/python tests/functional_sweep.py http://127.0.0.1:8000 [--apply] [--skip-agent]
.venv/bin/python tests/functional_sweep.py https://<app-url> --profile <profile>
```
