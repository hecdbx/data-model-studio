# Deploying Data Model Studio

A step-by-step guide to deploy **Data Model Studio** as a Databricks App in **your own
workspace** — on AWS, Azure, or GCP. No prior knowledge of the project is assumed.

`databricks bundle deploy` provisions four things in your workspace:

| Resource | What it is |
|---|---|
| **App** `data-model-studio` | The web app (React + FastAPI), runs as its own service principal |
| **Job** `<schema>_apply` | Applies generated DDL when you click *Apply* in the app |
| **Schema** `<catalog>.<target_schema>` | The app's working schema |
| **Volume** `…/<artifacts_volume>` | Stores generated artifacts + the agent's embedded skills |

Everything runs **inside your workspace**. Your data never leaves it: the app reads Unity
Catalog metadata and samples files through a SQL warehouse, and calls a Claude model hosted
on your Databricks account.

---

## 1. Prerequisites

**In your Databricks workspace**
- **Unity Catalog** enabled (this app models UC tables).
- **Databricks Apps** enabled for your workspace.
- A **SQL warehouse** (serverless recommended) that you can run queries on.
- A **Claude chat foundation-model endpoint**. Databricks hosts these as
  pay-per-token endpoints named `databricks-claude-*` (for example
  `databricks-claude-opus-5`, `databricks-claude-sonnet-4-5`). Availability varies
  by region and cloud — you'll pick one that exists in your workspace in Step 2.
- Permission to deploy an App and to `CREATE SCHEMA` in your target catalog (or ask a
  workspace admin to run the grants in Step 7).

**On your machine**
- **Databricks CLI ≥ 0.230** — `databricks -v`
  (install: <https://docs.databricks.com/dev-tools/cli/install.html>)
- **Node.js ≥ 20.19** and **npm** — `node -v`
- **Python ≥ 3.11** — `python3 --version`

---

## 2. Authenticate the CLI to your workspace

```bash
databricks auth login --host https://<your-workspace-host> --profile my-workspace
```

This opens a browser to sign in and saves a profile called `my-workspace`. Verify:

```bash
databricks current-user me -p my-workspace
```

---

## 3. Choose your model endpoint

List the Claude chat endpoints available in your workspace:

```bash
databricks serving-endpoints list -p my-workspace -o json \
  | python3 -c "import sys,json; [print(e['name']) for e in json.load(sys.stdin) if 'claude' in e['name'].lower()]"
```

Pick one (e.g. `databricks-claude-sonnet-4-5`). You'll pass it as the `serving_endpoint`
bundle variable. Any Claude **chat** endpoint works — the app auto-omits the `temperature`
parameter, so reasoning models like Opus 5 and GPT-5-class endpoints work too.

> No Claude endpoints listed? Enable Foundation Model APIs (pay-per-token) for your workspace,
> or create a custom serving endpoint, then use its name.

---

## 4. Choose your SQL warehouse

```bash
databricks warehouses list -p my-workspace -o json \
  | python3 -c "import sys,json; [print(w['id'], w['name']) for w in json.load(sys.stdin)]"
```

Copy the **id** of a warehouse you can use (serverless preferred). You'll pass it as the
`warehouse_id` bundle variable.

---

## 5. Point the bundle at your workspace

Open **`databricks.yml`** and edit the `targets` block so it references your workspace and
profile (replace the example host/profile in both `dev` and `prod`):

```yaml
targets:
  dev:
    default: true
    mode: development
    workspace:
      host: https://<your-workspace-host>
      profile: my-workspace
```

The other settings (catalog, schema, warehouse, endpoint) are **bundle variables** you'll
pass on the command line in Step 6 — no need to edit them here.

---

## 6. Build the frontend and deploy

### 6a. Build the React frontend

```bash
cd frontend
npm install
npm run build          # outputs ../build, which the backend serves
cd ..
```

> **⚠️ npm registry note.** `frontend/package.json` contains an `overrides` block that pins
> two React-team helper packages (`scheduler`, `use-sync-external-store`). That block exists
> **only** to satisfy an internal Databricks npm mirror. **If you install from the public npm
> registry (npmjs.org), delete the `overrides` block** — you'll get the correct stable versions
> automatically. Keep it only if your `npm config get registry` points at a private mirror that
> lacks those stable packages.

### 6b. Deploy the bundle

Pass your catalog, schema, warehouse, and endpoint as variables:

```bash
databricks bundle validate -t dev -p my-workspace \
  --var target_catalog=<your_catalog> \
  --var target_schema=data_model_studio \
  --var warehouse_id=<your_warehouse_id> \
  --var serving_endpoint=<your_claude_endpoint>

databricks bundle deploy -t dev -p my-workspace \
  --var target_catalog=<your_catalog> \
  --var target_schema=data_model_studio \
  --var warehouse_id=<your_warehouse_id> \
  --var serving_endpoint=<your_claude_endpoint>
```

`deploy` uploads the code and creates the app, job, schema, and volume. Now start the app:

```bash
databricks bundle run data_model_studio -t dev -p my-workspace
```

Get its URL and status any time with:

```bash
databricks apps get data-model-studio -p my-workspace -o json \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print('URL:', d.get('url')); print('State:', (d.get('app_status') or {}).get('state'))"
```

> **Tip — avoid retyping variables.** Create a `.databricks/bundle-vars.json` and pass
> `--var-file`, or set the four variables' defaults directly in `databricks.yml` once.

---

## 7. Grant the app's service principal access

Each Databricks App runs as its **own service principal (SP)**. The bundle already grants that
SP `CAN_QUERY` on the model endpoint, `CAN_USE` on the warehouse, and `CAN_MANAGE_RUN` on the
apply job. You must additionally grant it **Unity Catalog** access to the data it will model.

Find the SP's application id:

```bash
databricks apps get data-model-studio -p my-workspace -o json \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('service_principal_client_id'))"
```

Then run this in a Databricks SQL editor or notebook, replacing `<SP_APP_ID>` and your
catalog names. Grant `USE`/`SELECT` on every catalog you want to **explore**, and
`CREATE SCHEMA` on the catalog the app **provisions into**:

```sql
-- Let the app read metadata + sample data from catalogs you want to model:
GRANT USE CATALOG ON CATALOG <catalog_to_explore> TO `<SP_APP_ID>`;
GRANT USE SCHEMA  ON CATALOG <catalog_to_explore> TO `<SP_APP_ID>`;
GRANT SELECT      ON CATALOG <catalog_to_explore> TO `<SP_APP_ID>`;

-- Let the app sample raw files in volumes you seed a model from (Design mode):
GRANT READ VOLUME ON CATALOG <catalog_to_explore> TO `<SP_APP_ID>`;

-- Let the app self-provision its working schema + volume (first run):
GRANT USE CATALOG   ON CATALOG <your_catalog> TO `<SP_APP_ID>`;
GRANT CREATE SCHEMA ON CATALOG <your_catalog> TO `<SP_APP_ID>`;
```

**Lakeflow pipelines (optional).** The Pipelines API has no on-behalf-of-user scope, so the
pipeline picker lists only the pipelines the SP can view. Grant `CAN_VIEW` on each pipeline you
want to browse:

```bash
databricks permissions update pipelines <PIPELINE_ID> -p my-workspace --json \
  '{"access_control_list":[{"service_principal_name":"<SP_APP_ID>","permission_level":"CAN_VIEW"}]}'
```

The pipeline's source files are read with the signed-in user's token (the app declares the
`workspace.workspace` user scope), so people only see code they can already open. Pasting a
pipeline's source under **Paste pipeline source instead** (Explore → Lakeflow pipelines) never
needs a grant.

> Minimum footprint: if you don't want the app creating a schema, pre-create
> `<your_catalog>.data_model_studio` and a volume `artifacts` inside it, grant the SP
> `ALL PRIVILEGES` on just that schema, and skip `CREATE SCHEMA`.

---

## 8. First run — self-provisioning

Open the app URL. The top-right pill shows **Set up** (amber) until its schema/volume/job
exist. It provisions automatically on startup (`AUTO_BOOTSTRAP=true`); to (re-)run it
manually, click the pill → **Run / re-run setup**. The panel reports each step and degrades
gracefully if a grant is missing.

---

## 9. Verify it works

1. Open the app URL. You should see the **Data Model Studio** landing page.
2. Click **Explore Unity Catalog** → pick a catalog → select a schema → **Load diagram**.
   Tables render as an ER diagram with primary/foreign-key links.
3. Open the **Code** tab (right panel) — it generates Unity Catalog DDL for the model.
4. Health check: `curl https://<app-url>/api/health` returns `sdk_connected: true` and your
   `warehouse_id`.

---

## Configuration reference

Set via bundle `--var` at deploy time (they flow into the app's environment). To change one
after deploy, edit the value in `app.yaml` and redeploy, or set the variable and re-run
`bundle deploy`.

| Variable / env | Default | Purpose |
|---|---|---|
| `serving_endpoint` | `databricks-claude-opus-5` | Claude chat endpoint the agent uses |
| `warehouse_id` | *(required)* | SQL warehouse for `information_schema` + `read_files()` |
| `target_catalog` | `houssem_demo_catalog` → **set yours** | Catalog the app provisions into |
| `target_schema` | `data_model_studio` | Working schema it creates |
| `artifacts_volume` | `artifacts` | Volume for artifacts + embedded skills |
| `AUTO_BOOTSTRAP` (app.yaml) | `true` | Provision on startup vs. only when you click *Set up* |
| `AGENT_MAX_TOKENS` (app.yaml) | `32000` | Max agent output tokens |
| `AGENT_TEMPERATURE` (app.yaml) | *(unset)* | Only set for endpoints that accept `temperature` |

---

## Updating, redeploying, and teardown

```bash
# after editing code: rebuild frontend if you changed it, then:
npm --prefix frontend run build
databricks bundle deploy -t dev -p my-workspace  --var …   # same vars as first deploy
databricks bundle run    data_model_studio -t dev -p my-workspace   # ship a new app version

# tear everything down (app, job, schema, volume):
databricks bundle destroy -t dev -p my-workspace
```

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `bundle deploy` fails: *warehouse … not found* | `warehouse_id` is wrong or the deploying user can't see it. Re-check Step 4. |
| App page loads but **Explore** shows a permission error | The app SP lacks UC grants — run Step 7 for that catalog. |
| **Lakeflow pipelines** list is empty | The SP has no `CAN_VIEW` on any pipeline — see *Lakeflow pipelines* in Step 7, or paste the source. |
| Attached pipeline shows *couldn't read its source* | The signed-in user hasn't consented to the `workspace.workspace` scope yet — reload the app (or open it in a new session) to re-authorize. |
| **Generate with agent** (seed from a volume) fails on the volume | Grant the SP `READ VOLUME` (Step 7). |
| `RESOURCE_DOES_NOT_EXIST` / endpoint error from the agent | `serving_endpoint` isn't available in your region. Pick another Claude endpoint (Step 3). |
| Agent 400: *does not support the temperature parameter* | Ensure `AGENT_TEMPERATURE` is **unset** (it is by default). |
| *No SQL warehouse available* in Set up | Grant the SP `CAN_USE` on a warehouse, or set `warehouse_id`. |
| `npm install` fails on `scheduler`/`use-sync-external-store` | You're on a private mirror — keep the `overrides` block. On public npm, delete it. |
| App won't start | `databricks apps get data-model-studio -o json` for state; open the app's **Logs** tab in the UI. |
| Blank page, 404 on assets | You didn't run `npm run build` before deploying — `build/` must exist. |

---

## Security & data residency

- The app runs as a workspace **service principal** with only the grants you give it.
- All data access (metadata, file samples) runs on **your** SQL warehouse; file contents are
  sampled server-side via `read_files()` and never downloaded to the app host.
- The agent calls a **Databricks-hosted** Claude endpoint — prompts stay within your
  Databricks account's model serving.
- Generated artifacts are written to a **managed volume** in your catalog. Nothing is applied
  to your tables unless you click **Apply**, which runs the review-visible `<schema>_apply` job.
```
