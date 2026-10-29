"""FastAPI application for Data Model Studio.

Serves the compiled React app and the JSON/SSE API for both features:
- Feature 1: volume browsing + agent model generation + drag-drop designer codegen.
- Feature 2: Unity Catalog / Lakeflow reverse-engineering + editing + codegen.
"""

from __future__ import annotations

import io
import os
import threading
import uuid

from fastapi import Body, FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from . import bootstrap
from .codegen import generate_ddl, generate_migration, generate_sdp
from .config import get_settings
from .databricks_clients import SqlError, get_user_client, get_workspace_client, resolve_warehouse_id
from .model_schema import coerce_model
from . import pipelines as pl
from . import uc_explore as uc
from . import volumes as vol

settings = get_settings()
app = FastAPI(title="Data Model Studio", version="1.0.0")

# In-memory async job store for long-running agent generation.
_jobs: dict[str, dict] = {}


# ═══════════════════════════════════════════════════════════════════════
# Startup — kick off self-provisioning in the background (non-blocking).
# ═══════════════════════════════════════════════════════════════════════
@app.on_event("startup")
def _startup() -> None:
    threading.Thread(target=bootstrap.auto_bootstrap_if_needed, daemon=True).start()


def _err(e: Exception, code: int = 500):
    return JSONResponse({"error": str(e)}, status_code=code)


# ═══════════════════════════════════════════════════════════════════════
# Health & bootstrap
# ═══════════════════════════════════════════════════════════════════════
@app.get("/api/health")
def health():
    out = {"status": "healthy", "service": "data-model-studio",
           "serving_endpoint": settings.serving_endpoint,
           "sdk_connected": False, "warehouse_id": None}
    try:
        get_workspace_client().current_user.me()
        out["sdk_connected"] = True
    except Exception as e:  # noqa: BLE001
        out["sdk_error"] = str(e)
    try:
        out["warehouse_id"] = resolve_warehouse_id()
    except Exception as e:  # noqa: BLE001
        out["warehouse_error"] = str(e)
    return out


@app.get("/api/bootstrap/status")
def bootstrap_status():
    try:
        return bootstrap.status()
    except Exception as e:  # noqa: BLE001
        return _err(e)


@app.post("/api/bootstrap/provision")
def bootstrap_provision():
    try:
        return bootstrap.provision()
    except Exception as e:  # noqa: BLE001
        return _err(e)


# ═══════════════════════════════════════════════════════════════════════
# Unity Catalog browsing + reverse-engineering (Feature 2)
# ═══════════════════════════════════════════════════════════════════════
@app.get("/api/catalogs")
def catalogs():
    try:
        return {"catalogs": uc.list_catalogs()}
    except Exception as e:  # noqa: BLE001
        return _err(e)


@app.get("/api/catalogs/{catalog}/schemas")
def schemas(catalog: str):
    try:
        return {"schemas": uc.list_schemas(catalog)}
    except Exception as e:  # noqa: BLE001
        return _err(e)


class FromSchemasReq(BaseModel):
    catalog: str
    schemas: list[str]


@app.post("/api/model/from-schemas")
def model_from_schemas(req: FromSchemasReq):
    try:
        model = uc.build_model_from_schemas(req.catalog, req.schemas)
        return {"model": model.model_dump(by_alias=True)}
    except (ValueError, SqlError) as e:
        return _err(e, 400)
    except Exception as e:  # noqa: BLE001
        return _err(e)


# ═══════════════════════════════════════════════════════════════════════
# Lakeflow pipelines (Feature 2, optional)
# ═══════════════════════════════════════════════════════════════════════
def _user_token(request: Request) -> str | None:
    """The logged-in user's token, forwarded by Databricks Apps. Enables reading
    pipeline notebooks the app service principal can't see."""
    return request.headers.get("x-forwarded-access-token")


@app.get("/api/pipelines")
def pipelines_list():
    try:
        return {"pipelines": pl.list_pipelines()}
    except Exception as e:  # noqa: BLE001
        return _err(e)


@app.get("/api/pipelines/{pipeline_id}")
def pipeline_detail(pipeline_id: str, request: Request):
    try:
        export_client = get_user_client(_user_token(request))
        return pl.build_pipeline_overlay(pipeline_id, export_client=export_client)
    except Exception as e:  # noqa: BLE001
        return _err(e)


class AttachPipelineReq(BaseModel):
    entities: list[dict] = []


@app.post("/api/pipelines/{pipeline_id}/attach")
def pipeline_attach(pipeline_id: str, req: AttachPipelineReq, request: Request):
    """Overlay for one pipeline + its inferred FKs resolved onto the canvas
    entities (`relationships`, entity-id based, columns validated)."""
    try:
        export_client = get_user_client(_user_token(request))
        ov = pl.build_pipeline_overlay(pipeline_id, export_client=export_client)
        ov["relationships"] = pl.resolve_relationships(ov, req.entities)
        return ov
    except Exception as e:  # noqa: BLE001
        return _err(e)


class ParsePipelineReq(BaseModel):
    source: str
    name: str | None = None
    entities: list[dict] = []


@app.post("/api/pipelines/parse")
def pipeline_parse(req: ParsePipelineReq):
    """Parse pasted pipeline source — no workspace permissions needed. Returns an
    overlay with produced tables, lineage, and inferred FK relationships
    (resolved onto `entities` when given)."""
    try:
        if not (req.source or "").strip():
            return _err(ValueError("Paste some pipeline source first."), 400)
        ov = pl.overlay_from_pasted_source(req.source, req.name or "")
        ov["relationships"] = pl.resolve_relationships(ov, req.entities)
        return ov
    except Exception as e:  # noqa: BLE001
        return _err(e)


# ═══════════════════════════════════════════════════════════════════════
# Volumes + file sampling (Feature 1)
# ═══════════════════════════════════════════════════════════════════════
@app.get("/api/catalogs/{catalog}/schemas/{schema}/volumes")
def volumes_list(catalog: str, schema: str):
    try:
        return {"volumes": vol.list_volumes(catalog, schema)}
    except Exception as e:  # noqa: BLE001
        return _err(e)


@app.get("/api/volume-files")
def volume_files(path: str = Query(...)):
    try:
        return {"files": vol.list_volume_files(path)}
    except ValueError as e:
        return _err(e, 400)
    except Exception as e:  # noqa: BLE001
        return _err(e)


@app.get("/api/file-sample")
def file_sample(path: str = Query(...), max_rows: int = Query(30)):
    try:
        return vol.read_file_sample(path, max_rows=max_rows)
    except ValueError as e:
        return _err(e, 400)
    except Exception as e:  # noqa: BLE001
        return _err(e)


@app.get("/api/volume-snapshot")
def volume_snapshot(catalog: str = Query(...), schema: str = Query(...), volume: str = Query(...)):
    try:
        return vol.get_volume_snapshot(catalog, schema, volume)
    except Exception as e:  # noqa: BLE001
        return _err(e)


# ═══════════════════════════════════════════════════════════════════════
# Deterministic code generation (both features)
# ═══════════════════════════════════════════════════════════════════════
class CodegenReq(BaseModel):
    model: dict
    catalog: str | None = None
    schema_name: str | None = None
    source_volume: str | None = None


def _target(model, catalog, schema):
    cat = catalog or (model.source.catalog if model.source else None) or settings.target_catalog
    sch = schema or (model.source.schemas[0] if (model.source and model.source.schemas)
                     else settings.target_schema)
    return cat, sch


@app.post("/api/codegen/ddl")
def codegen_ddl(req: CodegenReq):
    try:
        model = coerce_model(req.model)
        cat, sch = _target(model, req.catalog, req.schema_name)
        return {"language": "sql", "code": generate_ddl(model, cat, sch)}
    except Exception as e:  # noqa: BLE001
        return _err(e)


def _sdp_source_volume(model, explicit: str | None) -> str | None:
    """Raw files for the pipeline's source tables: the request's volume, else the
    volume the model was seeded from."""
    if explicit:
        return explicit
    src = model.source
    if src and src.kind == "volume" and src.catalog and src.volume:
        # `schema` is an extra field — attribute access would hit BaseModel.schema()
        sch = (src.model_extra or {}).get("schema") or (src.schemas[0] if src.schemas else None)
        if sch:
            return f"/Volumes/{src.catalog}/{sch}/{src.volume}"
    return None


@app.post("/api/codegen/sdp")
def codegen_sdp(req: CodegenReq):
    try:
        model = coerce_model(req.model)
        cat, sch = _target(model, req.catalog, req.schema_name)
        return {"language": "python",
                "code": generate_sdp(model, cat, sch, _sdp_source_volume(model, req.source_volume))}
    except Exception as e:  # noqa: BLE001
        return _err(e)


class MigrationReq(BaseModel):
    base: dict
    edited: dict
    catalog: str | None = None
    schema_name: str | None = None


@app.post("/api/codegen/migration")
def codegen_migration(req: MigrationReq):
    try:
        base = coerce_model(req.base)
        edited = coerce_model(req.edited)
        cat, sch = _target(edited, req.catalog, req.schema_name)
        return {"language": "sql", "code": generate_migration(base, edited, cat, sch)}
    except Exception as e:  # noqa: BLE001
        return _err(e)


# ═══════════════════════════════════════════════════════════════════════
# Agent — generate model from a volume (async job)
# ═══════════════════════════════════════════════════════════════════════
class GenFromVolumeReq(BaseModel):
    model_config = {"protected_namespaces": ()}
    catalog: str
    schema_name: str
    volume: str
    model_type: str = "medallion"


@app.post("/api/agent/generate-from-volume")
def agent_generate_from_volume(req: GenFromVolumeReq):
    if req.model_type not in ("medallion", "star_schema", "data_vault"):
        return _err(ValueError("model_type must be medallion, star_schema, or data_vault"), 400)
    job_id = str(uuid.uuid4())
    _jobs[job_id] = {"status": "running"}

    def _run():
        try:
            from .agent.agent import generate_model_from_volume
            res = generate_model_from_volume(req.catalog, req.schema_name, req.volume, req.model_type)
            if "error" in res:
                _jobs[job_id] = {"status": "error", "error": res["error"], "raw": res.get("raw")}
            else:
                _jobs[job_id] = {"status": "done", "model": res["model"]}
        except Exception as e:  # noqa: BLE001
            _jobs[job_id] = {"status": "error", "error": str(e)}

    threading.Thread(target=_run, daemon=True).start()
    return {"job_id": job_id}


@app.get("/api/agent/job/{job_id}")
def agent_job(job_id: str):
    job = _jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


class InferKeysReq(BaseModel):
    model: dict


@app.post("/api/agent/infer-keys")
def agent_infer_keys(req: InferKeysReq):
    """Kick off an agent run that infers primary + foreign keys for the whole
    model. Poll the result via /api/agent/job/{job_id}."""
    job_id = str(uuid.uuid4())
    _jobs[job_id] = {"status": "running"}

    def _run():
        try:
            from .agent.agent import infer_keys
            res = infer_keys(req.model)
            if "error" in res:
                _jobs[job_id] = {"status": "error", "error": res["error"], "raw": res.get("raw")}
            else:
                _jobs[job_id] = {"status": "done", "model": res["model"]}
        except Exception as e:  # noqa: BLE001
            _jobs[job_id] = {"status": "error", "error": str(e)}

    threading.Thread(target=_run, daemon=True).start()
    return {"job_id": job_id}


# ═══════════════════════════════════════════════════════════════════════
# Agent — streaming chat (both features)
# ═══════════════════════════════════════════════════════════════════════
class ChatReq(BaseModel):
    history: list[dict]
    model: dict | None = None
    pipeline_source: str | None = None


@app.post("/api/agent/chat")
def agent_chat(req: ChatReq):
    from .agent.agent import stream_chat
    gen = stream_chat(req.history, req.model, req.pipeline_source)
    return StreamingResponse(gen, media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ═══════════════════════════════════════════════════════════════════════
# Apply — write an artifact to the volume and run the apply Job
# ═══════════════════════════════════════════════════════════════════════
class ApplyReq(BaseModel):
    name: str
    content: str
    mode: str = "sql"


@app.post("/api/artifacts/save")
def artifacts_save(req: ApplyReq):
    try:
        path = f"{settings.artifacts_volume_path}/{os.path.basename(req.name)}"
        get_workspace_client().files.upload(path, io.BytesIO(req.content.encode("utf-8")), overwrite=True)
        return {"path": path}
    except Exception as e:  # noqa: BLE001
        return _err(e)


@app.post("/api/apply")
def apply(req: ApplyReq):
    try:
        w = get_workspace_client()
        path = f"{settings.artifacts_volume_path}/{os.path.basename(req.name)}"
        w.files.upload(path, io.BytesIO(req.content.encode("utf-8")), overwrite=True)
        job_id = bootstrap._find_job(settings.exec_job_name)
        if not job_id:
            return _err(ValueError(
                f"Apply job '{settings.exec_job_name}' not found. Deploy the bundle "
                f"(databricks bundle deploy) to create it, then retry."), 400)
        run = w.jobs.run_now(job_id=job_id,
                             job_parameters={"artifact_path": path, "mode": req.mode})
        return {"run_id": run.run_id, "job_id": job_id, "artifact_path": path}
    except Exception as e:  # noqa: BLE001
        return _err(e)


@app.get("/api/apply/run/{run_id}")
def apply_run(run_id: int):
    try:
        run = get_workspace_client().jobs.get_run(run_id)
        life = run.state.life_cycle_state.value if (run.state and run.state.life_cycle_state) else None
        result = run.state.result_state.value if (run.state and run.state.result_state) else None
        return {"run_id": run_id, "life_cycle_state": life, "result_state": result,
                "run_page_url": run.run_page_url}
    except Exception as e:  # noqa: BLE001
        return _err(e)


# ═══════════════════════════════════════════════════════════════════════
# Static frontend (mounted last so /api wins)
# ═══════════════════════════════════════════════════════════════════════
_BUILD = settings.build_dir
_ASSETS = os.path.join(_BUILD, "assets")
if os.path.isdir(_ASSETS):
    app.mount("/assets", StaticFiles(directory=_ASSETS), name="assets")


@app.get("/")
def _index():
    idx = os.path.join(_BUILD, "index.html")
    if os.path.isfile(idx):
        return FileResponse(idx)
    return JSONResponse({"message": "Frontend not built. Run `npm --prefix frontend run build`."},
                        status_code=200)


@app.get("/{full_path:path}")
def _spa(full_path: str):
    if full_path.startswith("api/"):
        raise HTTPException(status_code=404, detail="Not found")
    candidate = os.path.join(_BUILD, full_path)
    if os.path.isfile(candidate):
        return FileResponse(candidate)
    idx = os.path.join(_BUILD, "index.html")
    if os.path.isfile(idx):
        return FileResponse(idx)
    raise HTTPException(status_code=404, detail="Not found")
