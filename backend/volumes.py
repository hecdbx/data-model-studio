"""Feature 1 — browse Unity Catalog volumes and sample data files.

File reading goes through the SQL warehouse via ``read_files()`` (schema
inference happens server-side), so no databricks-connect / Spark client is
needed in the app process.
"""

from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor

from .databricks_clients import get_workspace_client, run_sql, valid_ident

# Volume paths: /Volumes/<cat>/<schema>/<volume>/<...>. Allow common filename
# characters but reject anything that could break out of the string literal.
_VOLUME_PATH_RE = re.compile(r"^/Volumes/[A-Za-z0-9_./=+\- ]+$")

_SUPPORTED = {"csv", "tsv", "json", "jsonl", "parquet", "orc", "avro", "txt"}


def _safe_path(path: str) -> str:
    if not path or not _VOLUME_PATH_RE.match(path) or "'" in path or ";" in path or ".." in path:
        raise ValueError(f"Unsafe or invalid volume path: {path!r}")
    return path


def list_volumes(catalog: str, schema: str) -> list[dict]:
    if not (valid_ident(catalog) and valid_ident(schema)):
        raise ValueError("Invalid catalog/schema.")
    w = get_workspace_client()
    out = []
    for v in w.volumes.list(catalog_name=catalog, schema_name=schema):
        out.append({
            "name": v.name,
            "full_name": v.full_name,
            "volume_type": v.volume_type.value if v.volume_type else "MANAGED",
            "comment": v.comment or "",
        })
    return out


def list_volume_files(path: str) -> list[dict]:
    path = _safe_path(path)
    w = get_workspace_client()
    files = []
    for f in w.files.list_directory_contents(directory_path=path):
        files.append({
            "name": f.name,
            "path": f.path,
            "is_directory": bool(f.is_directory),
            "file_size": getattr(f, "file_size", None),
        })
    files.sort(key=lambda x: (not x["is_directory"], x["name"].lower()))
    return files


def _ext(path: str) -> str:
    return path.rsplit(".", 1)[-1].lower() if "." in path.rsplit("/", 1)[-1] else ""


def _read_files_sql(path: str, ext: str, max_rows: int) -> str:
    p = _safe_path(path)
    n = max(1, min(int(max_rows), 200))
    if ext in ("csv", "tsv"):
        sep = "\\t" if ext == "tsv" else ","
        opts = f"format => 'csv', header => true, sep => '{sep}', inferSchema => true, multiLine => true"
    elif ext in ("json", "jsonl"):
        opts = f"format => 'json', multiLine => {'true' if ext == 'json' else 'false'}"
    elif ext == "parquet":
        opts = "format => 'parquet'"
    elif ext == "orc":
        opts = "format => 'orc'"
    elif ext == "avro":
        opts = "format => 'avro'"
    else:
        opts = "format => 'text'"
    return f"SELECT * FROM read_files('{p}', {opts}) LIMIT {n}"


def read_file_sample(path: str, max_rows: int = 30) -> dict:
    ext = _ext(path)
    res = run_sql(_read_files_sql(path, ext, max_rows))
    cols = [c for c in res["columns"] if not c.startswith("_rescued")]
    schema_info = {
        name: typ for name, typ in zip(res["columns"], res["types"])
        if not name.startswith("_rescued")
    }
    idx = [i for i, c in enumerate(res["columns"]) if not c.startswith("_rescued")]
    sample_rows = [{res["columns"][i]: row[i] for i in idx} for row in res["rows"]]
    return {
        "file_path": path,
        "file_type": ext,
        "columns": cols,
        "schema_info": schema_info,
        "sample_rows": sample_rows,
        "row_count_sample": len(sample_rows),
        "total_columns": len(cols),
    }


def get_volume_snapshot(catalog: str, schema: str, volume: str, max_datasets: int = 40,
                        max_rows: int = 8) -> dict:
    """Schema + sample rows for every dataset in a volume. A dataset is a data
    file at the top level, or a folder of same-shaped files (e.g. monthly
    parts), which is sampled once. Samples are read in parallel."""
    if not (valid_ident(catalog) and valid_ident(schema) and valid_ident(volume)):
        raise ValueError("Invalid catalog/schema/volume.")
    base = f"/Volumes/{catalog}/{schema}/{volume}"
    snapshot = {"volume": {"catalog": catalog, "schema": schema, "volume": volume, "path": base},
                "files": [], "data_sources": []}
    top = list_volume_files(base)
    snapshot["files"] = top

    def folder_files(f):
        try:
            return [sf for sf in list_volume_files(f["path"])
                    if not sf["is_directory"] and _ext(sf["name"]) in _SUPPORTED]
        except Exception:  # noqa: BLE001
            return []

    dirs = [f for f in top if f["is_directory"]]
    with ThreadPoolExecutor(max_workers=8) as ex:
        listed = dict(zip((d["path"] for d in dirs), ex.map(folder_files, dirs)))
    datasets: list[tuple[str, list[dict]]] = []
    for f in top:
        if f["is_directory"]:
            if listed.get(f["path"]):
                datasets.append((f["name"], listed[f["path"]]))
        elif _ext(f["name"]) in _SUPPORTED:
            datasets.append((f["name"], [f]))

    def sample(ds):
        name, files = ds
        fp = files[0]["path"]
        try:
            out = read_file_sample(fp, max_rows=max_rows)
        except Exception as e:  # noqa: BLE001
            out = {"file_path": fp, "error": str(e)}
        out["dataset"] = name
        out["files_in_dataset"] = len(files)
        return out

    with ThreadPoolExecutor(max_workers=6) as ex:
        snapshot["data_sources"] = list(ex.map(sample, datasets[:max_datasets]))
    snapshot["total_data_sources"] = len(datasets)
    if len(datasets) > max_datasets:
        snapshot["not_sampled"] = [n for n, _ in datasets[max_datasets:]]
    return snapshot
