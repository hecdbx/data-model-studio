"""Databricks client layer.

Everything the app needs from Databricks goes through here:
- a shared ``WorkspaceClient`` (SDK) for catalog/pipeline/job/workspace calls,
- ``run_sql`` for Unity Catalog ``information_schema`` queries and ``read_files()``
  file sampling, executed on a SQL warehouse via the Statement Execution API.

No ``databricks-connect`` / Spark session is used — the app is a thin client.
"""

from __future__ import annotations

import os
import re
import threading
import time
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    Disposition,
    ExecuteStatementRequestOnWaitTimeout,
    Format,
    StatementParameterListItem,
    StatementState,
)

from .config import get_settings

_client: WorkspaceClient | None = None
_client_lock = threading.Lock()
_warehouse_id: str | None = None
_warehouse_lock = threading.Lock()

# Unity Catalog identifiers: letters, digits, underscore. We validate before
# interpolating into fully-qualified names (parameters can't bind identifiers).
_IDENT_RE = re.compile(r"^[A-Za-z0-9_]+$")


class SqlError(RuntimeError):
    """Raised when a SQL statement fails on the warehouse."""


def get_workspace_client() -> WorkspaceClient:
    global _client
    if _client is not None:
        return _client
    with _client_lock:
        if _client is None:
            _client = WorkspaceClient()
    return _client


def get_user_client(user_token: str | None) -> WorkspaceClient:
    """Build a WorkspaceClient that acts **on behalf of the logged-in user**,
    using the token Databricks Apps forwards in ``X-Forwarded-Access-Token``.

    Used for reads that the app service principal can't perform but the user can
    (e.g. exporting pipeline notebooks that live in the user's workspace folder).
    Falls back to the app SP client when no token is present or construction fails.
    The token is only as capable as the app's declared ``user_api_scopes``.
    """
    if not user_token:
        return get_workspace_client()
    host = (os.environ.get("DATABRICKS_HOST")
            or os.environ.get("DATABRICKS_HOSTNAME") or "").strip()
    try:
        if host:
            return WorkspaceClient(host=host, token=user_token, auth_type="pat")
        return WorkspaceClient(token=user_token, auth_type="pat")
    except Exception:  # noqa: BLE001
        return get_workspace_client()


def valid_ident(name: str) -> bool:
    return bool(name) and bool(_IDENT_RE.match(name))


def quote_ident(name: str) -> str:
    """Validate and backtick-quote a Unity Catalog identifier."""
    if not valid_ident(name):
        raise ValueError(f"Invalid Unity Catalog identifier: {name!r}")
    return f"`{name}`"


def resolve_warehouse_id() -> str:
    """Resolve the SQL warehouse id: explicit env first, else auto-pick a
    running (preferably serverless) warehouse the app SP can use."""
    global _warehouse_id
    if _warehouse_id:
        return _warehouse_id
    with _warehouse_lock:
        if _warehouse_id:
            return _warehouse_id
        settings = get_settings()
        if settings.warehouse_id_env:
            _warehouse_id = settings.warehouse_id_env
            return _warehouse_id

        w = get_workspace_client()
        warehouses = list(w.warehouses.list())
        if not warehouses:
            raise SqlError(
                "No SQL warehouse available. Set WAREHOUSE_ID or grant the app "
                "service principal CAN_USE on a SQL warehouse."
            )

        def score(wh) -> tuple:
            running = 1 if (wh.state and wh.state.value == "RUNNING") else 0
            serverless = 1 if getattr(wh, "enable_serverless_compute", False) else 0
            return (running, serverless)

        best = max(warehouses, key=score)
        _warehouse_id = best.id
        return _warehouse_id


def run_sql(
    statement: str,
    *,
    catalog: str | None = None,
    schema: str | None = None,
    parameters: dict[str, Any] | None = None,
    row_limit: int | None = None,
    warehouse_id: str | None = None,
    timeout_s: int = 120,
) -> dict:
    """Execute a SQL statement and return normalized results.

    Returns ``{"columns": [...], "types": [...], "rows": [[...], ...]}``.
    Values come back as strings (JSON_ARRAY disposition); callers coerce.
    """
    w = get_workspace_client()
    wid = warehouse_id or resolve_warehouse_id()

    params = None
    if parameters:
        params = [
            StatementParameterListItem(name=k, value=(None if v is None else str(v)))
            for k, v in parameters.items()
        ]

    resp = w.statement_execution.execute_statement(
        warehouse_id=wid,
        statement=statement,
        catalog=catalog,
        schema=schema,
        parameters=params,
        row_limit=row_limit,
        wait_timeout="30s",
        format=Format.JSON_ARRAY,
        disposition=Disposition.INLINE,
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
    )

    deadline = time.time() + timeout_s
    while resp.status and resp.status.state in (
        StatementState.PENDING,
        StatementState.RUNNING,
    ):
        if time.time() > deadline:
            try:
                w.statement_execution.cancel_execution(resp.statement_id)
            except Exception:  # noqa: BLE001
                pass
            raise SqlError(f"SQL statement timed out after {timeout_s}s")
        time.sleep(0.8)
        resp = w.statement_execution.get_statement(resp.statement_id)

    state = resp.status.state if resp.status else None
    if state != StatementState.SUCCEEDED:
        msg = "unknown error"
        if resp.status and resp.status.error:
            msg = resp.status.error.message or str(resp.status.error.error_code)
        raise SqlError(msg)

    cols: list[str] = []
    types: list[str] = []
    if resp.manifest and resp.manifest.schema and resp.manifest.schema.columns:
        for c in resp.manifest.schema.columns:
            cols.append(c.name)
            types.append(c.type_text or "")

    rows: list[list] = []
    if resp.result and resp.result.data_array:
        rows.extend(resp.result.data_array)

    # Fetch any additional inline chunks.
    manifest = resp.manifest
    if manifest and manifest.chunks and len(manifest.chunks) > 1:
        for chunk in manifest.chunks[1:]:
            part = w.statement_execution.get_statement_result_chunk_n(
                resp.statement_id, chunk.chunk_index
            )
            if part and part.data_array:
                rows.extend(part.data_array)

    return {"columns": cols, "types": types, "rows": rows}


def run_sql_dicts(statement: str, **kwargs) -> list[dict]:
    """Same as run_sql but returns a list of column-keyed dicts."""
    res = run_sql(statement, **kwargs)
    cols = res["columns"]
    return [dict(zip(cols, row)) for row in res["rows"]]
