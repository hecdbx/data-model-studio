"""Self-provisioning — the "self-contained" behaviour.

On first run (or when the user clicks *Set up* in the UI) the app ensures its own
Unity Catalog assets exist:
- the working schema,
- a managed volume for artifacts,
- the embedded agent skills, materialized into the volume as reference assets,
- the apply Job (normally created by the bundle; discovered here).

Every step is idempotent and permission-graceful: a failure is reported in the
returned log, never raised, so the app still runs with reduced capability.
"""

from __future__ import annotations

import glob
import os
import threading

from .config import get_settings
from .databricks_clients import get_workspace_client, quote_ident, resolve_warehouse_id, run_sql

_state_lock = threading.Lock()
_provisioned_once = False
_job_id_cache: str | None = None

_SKILLS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "agent", "skills")


def _schema_exists(cat: str, sch: str) -> bool:
    try:
        get_workspace_client().schemas.get(f"{cat}.{sch}")
        return True
    except Exception:  # noqa: BLE001
        return False


def _volume_exists(cat: str, sch: str, vol: str) -> bool:
    try:
        get_workspace_client().volumes.read(f"{cat}.{sch}.{vol}")
        return True
    except Exception:  # noqa: BLE001
        return False


def _find_job(name: str):
    """Resolve the apply job's id. Handles bundle *development* mode, which
    prefixes job names like ``[dev <user>] <name>`` so an exact-name lookup
    misses. Result is cached once found."""
    global _job_id_cache
    if _job_id_cache:
        return _job_id_cache
    # Explicit override wins (e.g. EXEC_JOB_ID injected via the app resource).
    from .config import get_settings
    override = getattr(get_settings(), "exec_job_id", "")
    if override:
        _job_id_cache = override
        return _job_id_cache
    w = get_workspace_client()
    # Fast path: exact name (production mode).
    try:
        for j in w.jobs.list(name=name):
            _job_id_cache = j.job_id
            return _job_id_cache
    except Exception:  # noqa: BLE001
        pass
    # Fallback: dev/staging prefix the name — match by suffix.
    try:
        for j in w.jobs.list():
            nm = (j.settings.name if j.settings else None) or ""
            if nm == name or nm.endswith(f"] {name}") or nm.endswith(f" {name}"):
                _job_id_cache = j.job_id
                return _job_id_cache
    except Exception:  # noqa: BLE001
        pass
    return None


def status() -> dict:
    s = get_settings()
    cat, sch, vol = s.target_catalog, s.target_schema, s.artifacts_volume
    schema_ok = _schema_exists(cat, sch)
    volume_ok = schema_ok and _volume_exists(cat, sch, vol)
    job_id = _find_job(s.exec_job_name)
    warehouse = None
    warehouse_err = None
    try:
        warehouse = resolve_warehouse_id()
    except Exception as e:  # noqa: BLE001
        warehouse_err = str(e)
    return {
        "catalog": cat, "schema": sch, "volume": vol,
        "artifacts_path": s.artifacts_volume_path,
        "serving_endpoint": s.serving_endpoint,
        "schema_exists": schema_ok,
        "volume_exists": volume_ok,
        "job_name": s.exec_job_name,
        "job_id": job_id,
        "job_exists": job_id is not None,
        "warehouse_id": warehouse,
        "warehouse_error": warehouse_err,
        "initialized": bool(schema_ok and volume_ok),
        # the app's service principal (Databricks Apps inject its client id);
        # shown in the UI when it needs a grant, e.g. CAN_VIEW on pipelines
        "app_principal": os.environ.get("DATABRICKS_CLIENT_ID") or None,
    }


def _seed_skills(cat: str, sch: str, vol: str, log: list[dict]) -> None:
    w = get_workspace_client()
    base = f"/Volumes/{cat}/{sch}/{vol}/agent_skills"
    count = 0
    for path in sorted(glob.glob(os.path.join(_SKILLS_DIR, "*.md"))):
        name = os.path.basename(path)
        try:
            with open(path, "rb") as f:
                w.files.upload(f"{base}/{name}", f, overwrite=True)
            count += 1
        except Exception as e:  # noqa: BLE001
            log.append({"step": f"seed_skill:{name}", "ok": False, "detail": str(e)})
    log.append({"step": "seed_skills", "ok": count > 0, "detail": f"{count} skill file(s) → {base}"})


def provision() -> dict:
    """Create schema + volume, seed skills, locate the job. Returns {log, status}."""
    global _provisioned_once
    s = get_settings()
    cat, sch, vol = s.target_catalog, s.target_schema, s.artifacts_volume
    log: list[dict] = []

    # 1) schema
    try:
        run_sql(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(cat)}.{quote_ident(sch)} "
                f"COMMENT 'Data Model Studio working schema'")
        log.append({"step": "schema", "ok": True, "detail": f"{cat}.{sch}"})
    except Exception as e:  # noqa: BLE001
        log.append({"step": "schema", "ok": False, "detail": str(e)})

    # 2) volume
    try:
        run_sql(f"CREATE VOLUME IF NOT EXISTS {quote_ident(cat)}.{quote_ident(sch)}.{quote_ident(vol)} "
                f"COMMENT 'Generated artifacts + embedded agent skills'")
        log.append({"step": "volume", "ok": True, "detail": f"{cat}.{sch}.{vol}"})
    except Exception as e:  # noqa: BLE001
        log.append({"step": "volume", "ok": False, "detail": str(e)})

    # 3) embedded skills → volume
    if _volume_exists(cat, sch, vol):
        _seed_skills(cat, sch, vol, log)

    # 4) apply job (created by the bundle; just locate it)
    job_id = _find_job(s.exec_job_name)
    log.append({"step": "apply_job", "ok": job_id is not None,
                "detail": (f"job_id={job_id}" if job_id
                           else f"'{s.exec_job_name}' not found — deploy the bundle to create it")})

    with _state_lock:
        _provisioned_once = True
    return {"log": log, "status": status()}


def auto_bootstrap_if_needed() -> None:
    """Called on startup. Runs provisioning once if AUTO_BOOTSTRAP is on and the
    assets aren't already present. Never raises."""
    global _provisioned_once
    s = get_settings()
    if not s.auto_bootstrap or _provisioned_once:
        return
    try:
        st = status()
        if not st.get("initialized"):
            provision()
        with _state_lock:
            _provisioned_once = True
    except Exception:  # noqa: BLE001
        pass
