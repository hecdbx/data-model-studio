"""Feature 2 (optional) — bring a Lakeflow / Spark Declarative Pipeline into the
diagram, either by browsing the Pipelines API or by pasting its source, then:
- parse the tables it builds + internal lineage ("what builds this table"), and
- **infer foreign-key relationships** from the joins in the pipeline code so the
  ER diagram can be enriched with relationships the Unity Catalog constraints may
  not declare.

Sources are analysed **per file** (a pipeline mixes .py and .sql files):
- Python is parsed with ``ast`` — decorated ``@dp``/``@dlt`` definitions,
  module constants such as ``SILVER = f"{CATALOG}.silver"`` (folded into the
  bodies), and metadata-driven definitions generated in a loop / factory
  (``for feed in FEEDS: make_bronze(feed)``). Join conditions inside each
  definition are then read with targeted regexes. Unparseable Python falls back
  to a regex-only scan.
- SQL is split into statements; each ``CREATE … <table> AS SELECT`` yields the
  table, its upstreams (lineage) and its joins.

Auth note: listing pipelines and reading a pipeline's spec use the app service
principal (``spec_client``). Exporting / listing the source files is best done
with the *user's* token (``export_client``, on-behalf-of-user) because pipeline
sources usually live in a user's workspace folder the app SP can't read.
"""

from __future__ import annotations

import ast
import base64
import fnmatch
import re
import uuid

from databricks.sdk.service.workspace import ExportFormat, ObjectType

from .databricks_clients import get_workspace_client
from .model_schema import infer_cardinality

# ── Shared patterns ──────────────────────────────────────────────────────────
_STR_PREFIX = r"[rRbBuUfF]{0,2}"
# spark.read.table("t") | spark.readStream.table("t") | spark.table("t") | dp.read("t") | dlt.read_stream("t")
_READ_FN = (r"(?:spark\s*\.\s*read(?:Stream)?\s*\.\s*table|spark\s*\.\s*table"
            r"|(?:dp|dlt|sdp)\s*\.\s*read(?:_stream|Stream)?)")
_READ_CALL = _READ_FN + r"\s*\(\s*(?:name\s*=\s*)?" + _STR_PREFIX + r"""["']([^"']+)["']"""
_REF_READ = re.compile(_READ_CALL)
_INLINE_READ = re.compile(r"\s*" + _READ_CALL)
# variable = spark.read.table("t")…   (optionally wrapped in a parenthesis)
_ASSIGN_READ = re.compile(r"(\w+)\s*=\s*\(?\s*" + _READ_CALL)
# variable = spark.read.table("t").alias("a")  → DataFrame alias "a" for t
_DF_ALIAS = re.compile(r"(\w+)\s*=\s*\(?\s*" + _READ_CALL + r"""[^\n]*?\.\s*alias\(\s*["'](\w+)["']\s*\)""")
_REF_LIVE = re.compile(r"\bLIVE\.`?([A-Za-z0-9_]+)`?", re.IGNORECASE)
# embedded spark.sql("""...""") / spark.sql(f"...")
_SPARK_SQL = re.compile(
    r"""spark\.sql\s*\(\s*""" + _STR_PREFIX + r"""(\"\"\"|'''|\"|')(.*?)\1\s*[,)]""", re.DOTALL)

# Legacy regex scan (used only when a Python file does not parse).
_PY_TABLE = re.compile(r"@(?:dp|dlt)\.(?:table|view|materialized_view|temporary_view)\s*\(([^)]*)\)", re.DOTALL)
_PY_TABLE_NAME = re.compile(r"""name\s*=\s*["']([^"']+)["']""")
_PY_TABLE_POS = re.compile(r"""@(?:dp|dlt)\.(?:table|view|materialized_view|temporary_view)\s*\(\s*["']([^"']+)["']""")
_PY_STREAMING = re.compile(
    r"""(?:dp|dlt)\.create_streaming_table\s*\(\s*(?:name\s*=\s*)?["']([^"']+)["']""")
_DEF_SPLIT = re.compile(r"^(?=@|def\s)", re.MULTILINE)

# ── Join-inference patterns ──────────────────────────────────────────────────
# .select(F.col("orig").alias("new"))  — a joined-in column renamed before the condition
_ALIAS = re.compile(
    r"""(?:F\.)?col\(\s*["']([A-Za-z0-9_]+)["']\s*\)\s*\.\s*alias\(\s*["']([A-Za-z0-9_]+)["']\s*\)""")
# a single join-condition operand: F.col("x") | col("x") | v["x"] | v.x
_OPERAND = (
    r"""(?:F\.col\(\s*["'][^"']+["']\s*\)"""
    r"""|col\(\s*["'][^"']+["']\s*\)"""
    r"""|\w+\s*\[\s*["'][^"']+["']\s*\]"""
    r"""|\w+\.\w+)"""
)
_PRED = re.compile("(" + _OPERAND + r")\s*==\s*(" + _OPERAND + ")")
_OP_FCOL = re.compile(r"""^\s*(?:F\.)?col\(\s*["']([^"']+)["']\s*\)\s*$""")
_OP_BRACKET = re.compile(r"""^\s*(\w+)\s*\[\s*["']([^"']+)["']\s*\]\s*$""")
_OP_ATTR = re.compile(r"""^\s*(\w+)\.(\w+)\s*$""")
_QUOTED = re.compile(r"""["']([A-Za-z0-9_]+)["']""")

# ── SQL patterns ─────────────────────────────────────────────────────────────
_SQL_NAME = r"((?:`?[A-Za-z0-9_]+`?\s*\.\s*)*`?[A-Za-z0-9_]+`?)"
_SQL_CREATE = re.compile(
    r"\bCREATE\s+(?:OR\s+(?:REFRESH|REPLACE)\s+)?(?:(?:PRIVATE|TEMPORARY|TEMP)\s+)?(?:STREAMING\s+)?"
    r"(?:LIVE\s+)?(?:TABLE|MATERIALIZED\s+VIEW|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:LIVE\.)?" + _SQL_NAME,
    re.IGNORECASE,
)
# flows writing into an existing target: INSERT INTO t / AUTO CDC INTO t / APPLY CHANGES INTO t
_SQL_INTO = re.compile(
    r"\b(?:INSERT\s+INTO|AUTO\s+CDC\s+INTO|APPLY\s+CHANGES\s+INTO)\s+(?:TABLE\s+)?(?:LIVE\.)?" + _SQL_NAME,
    re.IGNORECASE,
)
_SQL_STREAM_FN = re.compile(r"\bSTREAM\s*\(\s*([^()]+?)\s*\)", re.IGNORECASE)
_SQL_STREAM_KW = re.compile(r"\bSTREAM\s+(?=[`A-Za-z_{])", re.IGNORECASE)
# FROM/JOIN <table> [AS] <alias>; never a table-valued function call (read_files(…))
_SQL_FROMJOIN = re.compile(
    r"\b(?:FROM|JOIN)\s+" + _SQL_NAME + r"(?![\w`.])(?!\s*\()(?:\s+(?:AS\s+)?([A-Za-z_]\w*))?",
    re.IGNORECASE,
)
_SQL_KEYWORDS = {
    "on", "where", "and", "or", "join", "inner", "left", "right", "full", "outer", "cross",
    "using", "group", "order", "having", "select", "from", "as", "when", "then", "lateral",
    "semi", "anti", "natural", "distinct", "limit", "union", "except", "intersect", "minus",
    "qualify", "window", "pivot", "unpivot", "tablesample", "version", "timestamp", "with",
    "values", "stream", "live", "keys", "sequence", "by", "into", "set", "is", "not", "null",
}
# CTE names introduced by WITH <name> AS ( … ), <name2> AS ( … ) — NOT real tables.
_CTE_NAME = re.compile(r"(?:\bWITH|,)\s+([A-Za-z_]\w*)\s+AS\s*\(", re.IGNORECASE)
# f-string interpolated catalog/schema prefixes left after folding: {S}.tbl, {CATALOG}.{SCHEMA}.tbl
_FSTRING_PREFIX = re.compile(r"\{[^}]*\}\s*\.\s*")
# SQL equi-join predicate: alias.col = alias.col
_SQL_EQ = re.compile(r"""\b([A-Za-z_]\w*)\.([A-Za-z_]\w*)\s*=\s*([A-Za-z_]\w*)\.([A-Za-z_]\w*)\b""")
_SQL_USING = re.compile(
    r"\bJOIN\s+" + _SQL_NAME + r"(?:\s+(?:AS\s+)?[A-Za-z_]\w*)?\s+USING\s*\(([^)]+)\)", re.IGNORECASE)
# where a JOIN clause's condition ends
_SQL_CLAUSE_END = re.compile(
    r"\b(?:JOIN|WHERE|GROUP\s+BY|ORDER\s+BY|HAVING|WINDOW|QUALIFY|LIMIT|UNION|EXCEPT|INTERSECT)\b",
    re.IGNORECASE)

_FK_SUFFIXES = ("_id", "_sk", "_key", "_fk", "_no", "_code", "_num", "_uuid", "_guid")
_DATASET_DECORATORS = {"table", "materialized_view", "view", "temporary_view", "append_flow"}
_PIPELINE_MODULES = {"dp", "dlt", "sdp", "pipelines"}


# ── Auth-aware SDK helpers ───────────────────────────────────────────────────
def list_pipelines(spec_client=None) -> list[dict]:
    w = spec_client or get_workspace_client()
    out = []
    for p in w.pipelines.list_pipelines():
        out.append({
            "pipeline_id": p.pipeline_id,
            "name": p.name or "",
            "state": p.state.value if p.state else "",
        })
    return sorted(out, key=lambda x: x["name"].lower())


def _spec_of(pipeline_id: str, spec_client=None):
    w = spec_client or get_workspace_client()
    resp = w.pipelines.get(pipeline_id)
    return resp.spec, resp.name


def _clients(export_client=None):
    """The user's client first, then (lazily) the app service principal's."""
    if export_client is not None:
        yield export_client
    yield get_workspace_client()


def _export_ws_file(path: str, export_client=None) -> str:
    for w in _clients(export_client):
        for fmt in (ExportFormat.SOURCE, ExportFormat.AUTO):
            try:
                resp = w.workspace.export(path, format=fmt)
                if resp and resp.content:
                    return base64.b64decode(resp.content).decode("utf-8", errors="replace")
            except Exception:  # noqa: BLE001
                continue
    return ""


_SOURCE_EXTS = (".py", ".sql")


def _glob_files(pattern: str, export_client=None) -> list[str]:
    """Expand a pipeline ``glob`` library (e.g. ``<root>/transformations/**``)
    into the workspace notebooks / .py / .sql files it includes."""
    pattern = (pattern or "").rstrip()
    if not pattern:
        return []
    wild = re.search(r"[*?\[]", pattern)
    base = pattern[:wild.start()] if wild else pattern
    base = base.rsplit("/", 1)[0] if wild else base.rstrip("/")
    if not base:
        return []
    for w in _clients(export_client):
        try:
            objs = list(w.workspace.list(base, recursive=True))
        except Exception:  # noqa: BLE001
            continue
        def norm(p):
            return p[len("/Workspace"):] if p.startswith("/Workspace/") else p

        pat = norm(pattern)
        out = []
        for o in objs:
            p = o.path or ""
            if o.object_type not in (ObjectType.FILE, ObjectType.NOTEBOOK):
                continue
            if o.object_type == ObjectType.FILE and not p.lower().endswith(_SOURCE_EXTS):
                continue
            if wild and not fnmatch.fnmatch(norm(p), pat):
                continue
            out.append(p)
        return sorted(out)
    return []


# ── Naming helpers ───────────────────────────────────────────────────────────
def _last_segment(name: str) -> str:
    return (name or "").split(".")[-1].strip("`").strip()


def _qualifier(name: str) -> str | None:
    """Schema part of a qualified name: cat.sch.tbl → sch, sch.tbl → sch."""
    parts = [p.strip("`").strip() for p in (name or "").split(".") if p.strip()]
    return parts[-2] if len(parts) >= 2 and "{" not in parts[-2] else None


def _singular(name: str) -> str:
    n = name.lower()
    if n.endswith("ies") and len(n) > 4:
        return n[:-3] + "y"
    if n.endswith("ses") and len(n) > 4:
        return n[:-2]
    if n.endswith("s") and not n.endswith("ss") and len(n) > 3:
        return n[:-1]
    return n


_LAYER_PREFIXES = ("dim_", "fact_", "bridge_", "hub_", "lnk_", "link_", "sat_",
                   "bronze_", "silver_", "gold_", "stg_", "staging_", "raw_")
_LAYER_SUFFIXES = ("_bronze", "_silver", "_gold", "_dim", "_fact", "_raw")


def _strip_layer(table: str) -> str:
    """dim_account -> account, silver_contacts -> contacts, sites_silver -> sites."""
    t = _last_segment(table).lower()
    for pre in _LAYER_PREFIXES:
        if t.startswith(pre) and len(t) > len(pre):
            t = t[len(pre):]
            break
    for suf in _LAYER_SUFFIXES:
        if t.endswith(suf) and len(t) > len(suf):
            t = t[:-len(suf)]
            break
    return t


_DESCRIPTIVE_SUFFIXES = ("_name", "_desc", "_description", "_label", "_text",
                         "_date", "_at", "_ts", "_time", "_flag", "_status", "_type")


def _fk_score(col: str, own_table: str, other_table: str) -> int:
    """How strongly `col` (on `own_table`) looks like a foreign key into `other_table`."""
    c = col.lower()
    other = _strip_layer(other_table)
    other_s = _singular(other)
    score = 0
    if c in (f"{other}_id", f"{other_s}_id", f"{other}_sk", f"{other_s}_sk",
             f"{other}_key", f"{other_s}_key", f"{other}_fk", f"{other_s}_fk"):
        score += 6
    elif (c.startswith(other_s) or c.startswith(other)) and c.endswith(_FK_SUFFIXES):
        score += 4
    elif c.endswith(_FK_SUFFIXES) and c != "id":
        score += 2
    # Descriptive columns (…_name, …_date, …) are natural attributes, not FKs.
    if c.endswith(_DESCRIPTIVE_SUFFIXES):
        score -= 2
    # A bare "id" (or the table's own key) reads as a PK, not an FK.
    own = _strip_layer(own_table)
    own_s = _singular(own)
    if c in ("id", "sk", "pk", f"{own}_id", f"{own_s}_id", f"{own}_sk", f"{own_s}_sk",
             f"{own}_key", f"{own_s}_key"):
        score -= 3
    return score


def _order_pair(t1: str, c1: str, t2: str, c2: str):
    """Return (child_tbl, child_col, parent_tbl, parent_col): child holds the FK."""
    s1 = _fk_score(c1, t1, t2)  # c1 as FK from t1 -> t2
    s2 = _fk_score(c2, t2, t1)  # c2 as FK from t2 -> t1
    if s2 > s1:
        return t2, c2, t1, c1
    return t1, c1, t2, c2


def _stem_matches(stem: str, table: str) -> bool:
    base = _strip_layer(table)
    sing = _singular(base)
    stem = stem.lower()
    return stem in (base, sing) or base.endswith("_" + stem) or sing.endswith("_" + stem)


def _foreign_looking(col: str, table: str) -> bool:
    """True when `col` reads like a key of *another* entity (e.g. ``site_id`` on
    ``utility_bills``) — such a column can't be the referenced side of an FK."""
    c = col.lower()
    for suf in _FK_SUFFIXES:
        if c.endswith(suf) and len(c) > len(suf):
            return not _stem_matches(c[:-len(suf)], table)
    return False


def _key_like_columns(table: str, columns: list[str]) -> list[str]:
    """Columns that look like `table`'s own key, surrogate keys first."""
    out = []
    for pref in ("_key", "_sk", "_id", "_code", "_no"):
        for c in columns:
            cl = c.lower()
            if cl.endswith(pref) and _stem_matches(cl[:-len(pref)], table) and c not in out:
                out.append(c)
    for c in columns:
        if c.lower() in ("id", "key", "sk") and c not in out:
            out.append(c)
    return out


class _RelAgg:
    """Aggregate inferred column pairs per join instance. A relationship records
    the candidate child tables (the left side of a join can be several tables
    already joined together — the column decides which one holds the FK), the
    parent (joined-in) table, and the context table whose definition joined them."""

    def __init__(self):
        self._rels: dict[tuple, dict] = {}

    def add(self, child, child_col, parent, parent_col, context=None, join_id=None,
            source="pipeline-join"):
        cands = [child] if isinstance(child, str) else list(child or [])
        cands = [_last_segment(c) for c in cands if c]
        parent_n = _last_segment(parent)
        cands = [c for c in dict.fromkeys(cands) if c and c != parent_n]
        if not cands or not parent_n or not child_col or not parent_col:
            return
        key = (tuple(cands), parent_n, context, join_id)
        r = self._rels.setdefault(key, {"cands": cands, "parent": parent_n, "pairs": [],
                                        "context": context, "source": source})
        if (child_col, parent_col) not in r["pairs"]:
            r["pairs"].append((child_col, parent_col))

    def result(self) -> list[dict]:
        out, seen = [], set()
        for r in self._rels.values():
            fc = [p[0] for p in r["pairs"]]
            tc = [p[1] for p in r["pairs"]]
            sig = (tuple(r["cands"]), r["parent"], tuple(fc), tuple(tc), r["context"])
            if sig in seen:
                continue
            seen.add(sig)
            out.append({
                "from_table": r["cands"][0],
                "from_columns": fc,
                "to_table": r["parent"],
                "to_columns": tc,
                "type": "many-to-one",
                "source": r["source"],
                "context_table": r["context"],
                "from_candidates": r["cands"],
            })
        return out


# ── Python: definitions via AST ──────────────────────────────────────────────
def _eval_str(node, env: dict):
    """Best-effort constant folding of a string expression (None if dynamic)."""
    if node is None:
        return None
    if isinstance(node, ast.Constant) and isinstance(node.value, (str, int, float)):
        return str(node.value)
    if isinstance(node, ast.Name):
        v = env.get(node.id)
        return str(v) if isinstance(v, (str, int, float)) else None
    if isinstance(node, ast.JoinedStr):
        parts = []
        for v in node.values:
            if isinstance(v, ast.Constant):
                parts.append(str(v.value))
            elif isinstance(v, ast.FormattedValue):
                s = _eval_str(v.value, env)
                if s is None:
                    return None
                parts.append(s)
            else:
                return None
        return "".join(parts)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        a, b = _eval_str(node.left, env), _eval_str(node.right, env)
        return a + b if a is not None and b is not None else None
    # spark.conf.get("k", "default") / os.environ.get("K", "default") → the default
    if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
            and node.func.attr == "get" and len(node.args) >= 2):
        return _eval_str(node.args[1], env)
    return None


def _eval_value(node, env: dict):
    try:
        return ast.literal_eval(node)
    except Exception:  # noqa: BLE001
        pass
    if isinstance(node, ast.Name) and node.id in env:
        return env[node.id]
    return _eval_str(node, env)


def _module_constants(tree) -> dict:
    env: dict = {}
    for node in tree.body:
        targets, value = [], None
        if isinstance(node, ast.Assign):
            targets, value = node.targets, node.value
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            targets, value = [node.target], node.value
        if len(targets) != 1 or not isinstance(targets[0], ast.Name):
            continue
        v = _eval_value(value, env)
        if v is not None:
            env[targets[0].id] = v
    return env


def _pipeline_aliases(tree) -> set[str]:
    aliases = set(_PIPELINE_MODULES)
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module in ("pyspark", "pyspark.pipelines"):
            for a in node.names:
                if a.name in ("pipelines",):
                    aliases.add(a.asname or a.name)
        elif isinstance(node, ast.Import):
            for a in node.names:
                if a.name in ("dlt", "pyspark.pipelines"):
                    aliases.add(a.asname or a.name.split(".")[-1])
    return aliases


def _dataset_decorator(fn, aliases: set[str]):
    for d in fn.decorator_list:
        target = d.func if isinstance(d, ast.Call) else d
        if (isinstance(target, ast.Attribute) and target.attr in _DATASET_DECORATORS
                and isinstance(target.value, ast.Name) and target.value.id in aliases):
            return d
    return None


def _dataset_name(fn, deco, env: dict):
    """Table name a decorated definition produces; None when it is dynamic."""
    if isinstance(deco, ast.Call):
        attr = deco.func.attr if isinstance(deco.func, ast.Attribute) else ""
        key = "target" if attr == "append_flow" else "name"
        for kw in deco.keywords:
            if kw.arg == key:
                return _eval_str(kw.value, env)
        if deco.args:
            return _eval_str(deco.args[0], env)
        if attr == "append_flow":
            return None
    return fn.name


def _loop_bindings(node, env: dict) -> list[dict]:
    it = node.iter
    values = None
    if isinstance(it, ast.Call) and isinstance(it.func, ast.Attribute) and it.func.attr in ("items", "keys", "values"):
        base = _eval_value(it.func.value, env)
        if isinstance(base, dict):
            values = list(getattr(base, it.func.attr)())
    elif isinstance(it, ast.Call) and isinstance(it.func, ast.Name) and it.func.id == "enumerate" and it.args:
        base = _eval_value(it.args[0], env)
        if isinstance(base, (list, tuple, dict)):
            values = list(enumerate(base))
    else:
        base = _eval_value(it, env)
        if isinstance(base, dict):
            values = list(base.keys())
        elif isinstance(base, (list, tuple, set)):
            values = list(base)
    if values is None:
        return []

    def bind(target, value, out):
        if isinstance(target, ast.Name):
            out[target.id] = value
            return True
        if isinstance(target, (ast.Tuple, ast.List)) and isinstance(value, (tuple, list)) \
                and len(target.elts) == len(value):
            return all(bind(t, v, out) for t, v in zip(target.elts, value))
        return False

    out = []
    for v in values[:500]:
        b: dict = {}
        if bind(node.target, v, b):
            out.append(b)
    return out


def _bind_call(fn, call, env: dict) -> dict:
    params = [a.arg for a in fn.args.args]
    b = {}
    for p, a in zip(params, call.args):
        v = _eval_value(a, env)
        if v is not None:
            b[p] = v
    for kw in call.keywords:
        if kw.arg:
            v = _eval_value(kw.value, env)
            if v is not None:
                b[kw.arg] = v
    return b


def _fold_constants(text: str, env: dict) -> str:
    """Inline `{NAME}` f-string placeholders whose value is a known constant."""
    def rep(m):
        v = env.get(m.group(1))
        return str(v) if isinstance(v, (str, int, float)) else m.group(0)
    return re.sub(r"\{([A-Za-z_]\w*)\}", rep, text)


def _py_definitions(source: str):
    """[(table_name, body_text)] for every dataset definition, or None when the
    source is not valid Python."""
    try:
        tree = ast.parse(source)
    except (SyntaxError, ValueError):
        return None
    consts = _module_constants(tree)
    aliases = _pipeline_aliases(tree)
    funcs = {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef)}
    out: list[tuple[str, str]] = []

    def seg(node):
        return ast.get_source_segment(source, node) or ""

    def emit_fn(fn, env):
        deco = _dataset_decorator(fn, aliases)
        if deco is None:
            return False
        name = _dataset_name(fn, deco, env)
        if name and "{" not in name:
            out.append((_last_segment(name), _fold_constants(seg(fn), env)))
        return True

    def emit_factory(call, env):
        """A call to a module-level factory function that defines datasets inside."""
        if not (isinstance(call, ast.Call) and isinstance(call.func, ast.Name) and call.func.id in funcs):
            return
        fn = funcs[call.func.id]
        fenv = {**env, **_bind_call(fn, call, env)}
        for inner in ast.walk(fn):
            if isinstance(inner, ast.FunctionDef) and inner is not fn:
                emit_fn(inner, fenv)

    def call_of(stmt):
        if isinstance(stmt, ast.Expr):
            return stmt.value
        if isinstance(stmt, ast.Assign):
            return stmt.value
        return None

    def visit(stmts, env, depth=0):
        for node in stmts:
            if isinstance(node, ast.FunctionDef):
                emit_fn(node, env)
            elif isinstance(node, ast.For) and depth < 3:
                for b in _loop_bindings(node, env):
                    visit(node.body, {**env, **b}, depth + 1)
            elif isinstance(node, ast.If) and depth < 3:
                visit(node.body, env, depth + 1)
                visit(node.orelse, env, depth + 1)
            else:
                emit_factory(call_of(node), env)

    visit(tree.body, consts)
    return out


def _py_produced_without_body(source: str) -> list[str]:
    """Tables created without a decorated body (create_streaming_table, CDC flows)."""
    out = [_last_segment(m.group(1)) for m in _PY_STREAMING.finditer(source)]
    for m in re.finditer(r"""(?:create_auto_cdc_flow|create_auto_cdc_from_snapshot_flow|apply_changes)\s*\(([^)]*)\)""",
                         source, re.DOTALL):
        t = re.search(r"""target\s*=\s*["']([^"']+)["']""", m.group(1))
        if t:
            out.append(_last_segment(t.group(1)))
    return out


def _py_cdc_lineage(source: str) -> list[dict]:
    out = []
    for m in re.finditer(r"""(?:create_auto_cdc_flow|create_auto_cdc_from_snapshot_flow|apply_changes)\s*\(([^)]*)\)""",
                         source, re.DOTALL):
        t = re.search(r"""target\s*=\s*["']([^"']+)["']""", m.group(1))
        s = re.search(r"""source\s*=\s*["']([^"']+)["']""", m.group(1))
        if t and s:
            out.append({"source": _last_segment(s.group(1)), "target": _last_segment(t.group(1))})
    return out


def _iter_blocks(source: str):
    """Regex fallback: split source into definition units, keeping @dp/@dlt
    decorators attached to their `def`."""
    blocks, pending = [], ""
    for b in _DEF_SPLIT.split(source):
        if b.lstrip().startswith("@") and "def " not in b:
            pending += b
        else:
            blocks.append(pending + b)
            pending = ""
    if pending:
        blocks.append(pending)
    return blocks


def _regex_definitions(source: str) -> list[tuple[str, str]]:
    out = []
    for block in _iter_blocks(source):
        m = _PY_TABLE.search(block)
        tbl = None
        if m:
            if re.search(r"\bname\s*=", m.group(1)):
                nm = _PY_TABLE_NAME.search(m.group(1))
                tbl = _last_segment(nm.group(1)) if nm else None  # dynamic name → skip
            else:
                mp = _PY_TABLE_POS.search(block)
                if mp:
                    tbl = _last_segment(mp.group(1))
                else:
                    fn = re.search(r"def\s+([A-Za-z0-9_]+)\s*\(", block)
                    tbl = fn.group(1) if fn else None
        elif re.search(r"@(?:dp|dlt)\.(?:table|view|materialized_view|temporary_view)\b", block):
            fn = re.search(r"def\s+([A-Za-z0-9_]+)\s*\(", block)
            tbl = fn.group(1) if fn else None
        if tbl and "{" not in tbl:
            out.append((tbl, block))
    return out


# ── Python: joins inside one definition body ─────────────────────────────────
def _matching_paren(text: str, open_idx: int) -> int:
    depth, q, j = 0, None, open_idx
    while j < len(text):
        ch = text[j]
        if q:
            if ch == "\\":
                j += 2
                continue
            if ch == q:
                q = None
        elif ch in "\"'":
            q = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return j
        j += 1
    return len(text)


def _split_top_level(s: str) -> list[str]:
    parts, buf, depth, q, i = [], [], 0, None, 0
    while i < len(s):
        ch = s[i]
        if q:
            buf.append(ch)
            if ch == "\\" and i + 1 < len(s):
                buf.append(s[i + 1])
                i += 2
                continue
            if ch == q:
                q = None
        elif ch in "\"'":
            q = ch
            buf.append(ch)
        elif ch in "([{":
            depth += 1
            buf.append(ch)
        elif ch in ")]}":
            depth -= 1
            buf.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        parts.append(tail)
    return parts


def _chain_root(text: str, dot: int):
    """Walk left from the '.' of ``.join(`` to the identifier that starts the
    method chain — ``sites.join(a).join(b)`` → ``sites``. Returns (name, pos)."""
    k = dot - 1
    while True:
        while k >= 0 and text[k].isspace():
            k -= 1
        if k < 0:
            return None, -1
        ch = text[k]
        if ch == ")":
            depth = 0
            while k >= 0:
                if text[k] == ")":
                    depth += 1
                elif text[k] == "(":
                    depth -= 1
                    if depth == 0:
                        break
                k -= 1
            k -= 1
            while k >= 0 and text[k].isspace():
                k -= 1
            if k < 0 or not (text[k].isalnum() or text[k] == "_"):
                return None, -1  # parenthesised expression, not a method call
            while k >= 0 and (text[k].isalnum() or text[k] == "_"):
                k -= 1
            kk = k
            while kk >= 0 and text[kk].isspace():
                kk -= 1
            if kk >= 0 and text[kk] == ".":
                k = kk - 1
                continue
            return None, -1  # a bare function call such as F.broadcast(x).join — unknown
        if ch.isalnum() or ch == "_":
            e = k
            while k >= 0 and (text[k].isalnum() or text[k] == "_"):
                k -= 1
            kk = k
            while kk >= 0 and text[kk].isspace():
                kk -= 1
            if kk >= 0 and text[kk] == ".":
                k = kk - 1
                continue  # attribute chain: spark.read.table(...) → keep walking to `spark`
            return text[k + 1:e + 1], k + 1
        return None, -1


def _literal_cols(expr: str):
    try:
        v = ast.literal_eval(expr.strip())
    except Exception:  # noqa: BLE001
        return None
    if isinstance(v, str):
        return [v]
    if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) for x in v):
        return list(v)
    return None


def _statement_end(text: str, i: int) -> int:
    """Index of the newline that ends the (possibly multi-line) statement at `i`."""
    depth, q = 0, None
    while i < len(text):
        ch = text[i]
        if q:
            if ch == "\\":
                i += 2
                continue
            if ch == q:
                q = None
        elif ch in "\"'":
            q = ch
        elif ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif ch == "\n" and depth <= 0:
            return i
        i += 1
    return len(text)


def _var_aliases(body: str) -> dict[str, dict[str, str]]:
    """Per DataFrame variable, the columns its assignment renamed:
    ``v = t.select(F.col("orig").alias("new"))`` → {"v": {"new": "orig"}}."""
    out: dict[str, dict[str, str]] = {}
    for m in re.finditer(r"^[ \t]*(\w+)\s*=(?!=)", body, re.MULTILINE):
        stmt = body[m.end():_statement_end(body, m.end())]
        al = {alias: orig for orig, alias in _ALIAS.findall(stmt)}
        if al:
            out.setdefault(m.group(1), {}).update(al)
    return out


def _infer_python_joins(body: str, agg: _RelAgg, context: str | None) -> None:
    if ".join(" not in body.replace(" ", ""):
        return
    var2tbl: dict[str, str] = {m.group(1): m.group(2) for m in _ASSIGN_READ.finditer(body)}
    dfalias: dict[str, str] = {m.group(3): m.group(2) for m in _DF_ALIAS.finditer(body)}
    var_alias = _var_aliases(body)
    var2set: dict[str, list[str]] = {}

    def tables_of(v):
        if v in var2set:
            return list(var2set[v])
        if v in var2tbl:
            return [var2tbl[v]]
        if v in dfalias:
            return [dfalias[v]]
        return []

    chains: dict[int, list[str]] = {}
    for m in re.finditer(r"\.\s*join\s*\(", body):
        root, root_pos = _chain_root(body, m.start())
        if root is None:
            continue
        open_idx = m.end() - 1
        args_txt = body[open_idx + 1:_matching_paren(body, open_idx)]
        if root in ("spark", "dp", "dlt", "sdp"):
            im = _INLINE_READ.match(body, root_pos)
            base = [im.group(1)] if im else []
        else:
            base = tables_of(root)
        left = list(chains.get(root_pos) or base)
        args = _split_top_level(args_txt)
        if not left or not args:
            continue
        rhs_expr = re.sub(r"^\s*(?:F\.)?broadcast\(\s*", "", args[0])
        im = _INLINE_READ.match(rhs_expr)
        rv = re.match(r"\s*(\w+)", rhs_expr)
        rhs_var = None if im else (rv.group(1) if rv else None)
        rhs_tables = [im.group(1)] if im else (tables_of(rhs_var) if rhs_var else [])
        if not rhs_tables:
            chains[root_pos] = left
            continue
        rhs_tbl = rhs_tables[0]
        alias_map = dict(var_alias.get(rhs_var, {})) if rhs_var else {}
        alias_map.update({alias: orig for orig, alias in _ALIAS.findall(args[0])})
        kw, pos = {}, []
        for a in args[1:]:
            km = re.match(r"^(\w+)\s*=(?!=)\s*(.*)$", a, re.DOTALL)
            if km:
                kw[km.group(1)] = km.group(2)
            else:
                pos.append(a)
        on = kw.get("on", pos[0] if pos else None)
        join_id = m.start()
        if on is not None:
            cols = _literal_cols(on)
            if cols is not None:
                for c in cols:
                    agg.add(left, c, rhs_tbl, alias_map.get(c, c), context, join_id)
            else:
                for lop, rop in _PRED.findall(on):
                    _emit_pred(lop, rop, left, rhs_var, rhs_tbl, alias_map, tables_of, dfalias,
                               agg, context, join_id)
        new_left = left + [t for t in rhs_tables if t not in left]
        chains[root_pos] = new_left
        am = re.search(r"(\w+)\s*=\s*\(?\s*$", body[max(0, root_pos - 160):root_pos])
        if am:
            var2set[am.group(1)] = new_left


def _emit_pred(lop, rop, left, rhs_var, rhs_tbl, alias_map, tables_of, dfalias, agg, context, join_id):
    def side(op):
        m = _OP_FCOL.match(op)
        if m:
            name = m.group(1)
            if "." in name:
                a, c = name.split(".", 1)
                if a in dfalias:
                    t = dfalias[a]
                    return ("rhs" if t == rhs_tbl else "left", [t], c)
                return (None, [], c)
            if name in alias_map:
                return ("rhs", [rhs_tbl], alias_map[name])
            return ("?", [], name)
        m = _OP_BRACKET.match(op) or _OP_ATTR.match(op)
        if m:
            v, c = m.group(1), m.group(2)
            if v == rhs_var:
                return ("rhs", [rhs_tbl], alias_map.get(c, c))
            ts = tables_of(v)
            if ts == [rhs_tbl]:
                return ("rhs", [rhs_tbl], alias_map.get(c, c))
            if ts:
                return ("left", ts, c)
        return (None, [], None)

    ls, rs = side(lop), side(rop)
    if ls[0] == "rhs" and rs[0] != "rhs":
        ls, rs = rs, ls
    if rs[0] != "rhs":
        if ls[0] == "left" and rs[0] == "?":
            rs = ("rhs", [rhs_tbl], alias_map.get(rs[2], rs[2]))
        elif rs[0] == "left" and ls[0] == "?":
            ls, rs = rs, ("rhs", [rhs_tbl], alias_map.get(ls[2], ls[2]))
        else:
            return
    if ls[0] not in ("left", "?") or not ls[2]:
        return
    cands = ls[1] if ls[0] == "left" else left
    agg.add(cands, ls[2], rhs_tbl, rs[2], context, join_id)


# ── SQL ──────────────────────────────────────────────────────────────────────
def _split_sql(text: str) -> list[str]:
    """Split a SQL script into statements on `;` (outside quotes / comments) and
    on notebook cell separators."""
    text = re.sub(r"(?m)^\s*--\s*COMMAND\s*-+\s*$", ";", text)
    out, buf, q, i, n = [], [], None, 0, len(text)
    while i < n:
        ch = text[i]
        if q:
            buf.append(ch)
            if ch == q:
                q = None
            i += 1
            continue
        if ch == "-" and text[i:i + 2] == "--":
            while i < n and text[i] != "\n":
                i += 1
            continue
        if ch == "/" and text[i:i + 2] == "/*":
            j = text.find("*/", i + 2)
            i = n if j < 0 else j + 2
            continue
        if ch in "'\"`":
            q = ch
        if ch == ";":
            s = "".join(buf).strip()
            if s:
                out.append(s)
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    s = "".join(buf).strip()
    if s:
        out.append(s)
    return out


def _sql_normalize(sql: str) -> str:
    sql = _FSTRING_PREFIX.sub("", sql)
    sql = _SQL_STREAM_FN.sub(r" \1 ", sql)
    sql = _SQL_STREAM_KW.sub("", sql)
    return sql


def _sql_real_tables(sql: str):
    """(normalized sql, alias->table map, ordered real tables, cte-name set)."""
    sql = _sql_normalize(sql)
    cte = {m.lower() for m in _CTE_NAME.findall(sql)}
    alias2tbl: dict[str, str] = {}
    order: list[str] = []
    for m in _SQL_FROMJOIN.finditer(sql):
        full = m.group(1)
        tbl = _last_segment(full)
        if not tbl or tbl.lower() in _SQL_KEYWORDS or tbl.lower() in cte:
            continue
        alias = (m.group(2) or "").strip()
        if alias and alias.lower() in _SQL_KEYWORDS:
            alias = ""
        alias2tbl[tbl.lower()] = full
        if alias:
            alias2tbl[alias.lower()] = full
        if full not in order:
            order.append(full)
    return sql, alias2tbl, order, cte


def _sql_table_refs(sql: str) -> list[str]:
    """Real tables a SQL statement reads from (for lineage)."""
    _, _, order, _ = _sql_real_tables(sql)
    return [_last_segment(t) for t in order]


def _infer_from_sql(sql: str, agg: _RelAgg, context: str | None = None) -> None:
    sql, alias2tbl, order, cte = _sql_real_tables(sql)
    order_index = {_last_segment(t): i for i, t in enumerate(order)}

    def emit(t1, c1, t2, c2, join_id):
        # Prefer the name heuristic when it's decisive; otherwise fall back to
        # appearance order — the later (joined-in) table is the parent/dimension.
        n1, n2 = _last_segment(t1), _last_segment(t2)
        s1, s2 = _fk_score(c1, n1, n2), _fk_score(c2, n2, n1)
        if abs(s1 - s2) >= 3:
            ct, cc, pt, pc = _order_pair(t1, c1, t2, c2)
        elif order_index.get(n1, 0) <= order_index.get(n2, 0):
            ct, cc, pt, pc = t1, c1, t2, c2
        else:
            ct, cc, pt, pc = t2, c2, t1, c1
        agg.add(ct, cc, pt, pc, context, join_id, source="pipeline-sql")

    # Each JOIN … ON <cond> clause (and the WHERE clause, for implicit joins) is
    # one join instance, so two joins to the same table never merge.
    starts = [m.start() for m in re.finditer(r"\bJOIN\b|\bWHERE\b", sql, re.IGNORECASE)]
    for idx, st in enumerate(starts):
        nxt = _SQL_CLAUSE_END.search(sql, st + 4)
        segment = sql[st:nxt.start() if nxt else len(sql)]
        for a1, c1, a2, c2 in _SQL_EQ.findall(segment):
            t1, t2 = alias2tbl.get(a1.lower()), alias2tbl.get(a2.lower())
            if t1 and t2 and _last_segment(t1) != _last_segment(t2):
                emit(t1, c1, t2, c2, f"sql{st}")
    # JOIN b USING (col, ...) — child is the driving (first real) table.
    for m in _SQL_USING.finditer(sql):
        parent = m.group(1)
        pn = _last_segment(parent)
        if not pn or pn.lower() in cte or pn.lower() in _SQL_KEYWORDS:
            continue
        cols = _QUOTED.findall(m.group(2)) or [c.strip().strip("`") for c in m.group(2).split(",") if c.strip()]
        child = next((t for t in order if _last_segment(t) != pn), None)
        if child:
            for c in cols:
                emit(child, c, parent, c, f"using{m.start()}")


def _sql_definitions(source: str) -> list[tuple[str, str, bool]]:
    """[(table, statement, is_flow)] for every CREATE … / INSERT INTO / AUTO CDC statement."""
    out = []
    for stmt in _split_sql(source):
        m = _SQL_CREATE.search(stmt)
        if m and m.start() < 200:
            # a CREATE FLOW … INSERT INTO target writes into `target`
            into = _SQL_INTO.search(stmt, m.end())
            if into and re.search(r"\bCREATE\s+FLOW\b", stmt, re.IGNORECASE):
                out.append((_last_segment(into.group(1)), stmt, True))
            else:
                out.append((_last_segment(m.group(1)), stmt, False))
            continue
        into = _SQL_INTO.search(stmt)
        if into and into.start() < 200:
            out.append((_last_segment(into.group(1)), stmt, True))
    return out


# ── Per-file analysis ────────────────────────────────────────────────────────
def _detect_language(code: str, path: str | None) -> str:
    p = (path or "").lower()
    if p.endswith(".sql"):
        return "sql"
    if p.endswith(".py"):
        return "python"
    head = code.lstrip()[:200].lower()
    if head.startswith("-- databricks notebook source"):
        return "sql"
    if head.startswith("# databricks notebook source"):
        return "python"
    try:
        ast.parse(code)
        if re.search(r"^\s*(?:import|from|def|@)\b", code, re.MULTILINE):
            return "python"
    except (SyntaxError, ValueError):
        pass
    if _SQL_CREATE.search(code) or re.search(r"\bSELECT\b", code, re.IGNORECASE):
        return "sql"
    return "python"


class _Analysis:
    def __init__(self):
        self.produced: list[str] = []
        self.lineage: list[dict] = []
        self.primary: dict[str, str] = {}
        self.schemas: dict[str, str] = {}  # table → schema qualifier seen in references
        self.agg = _RelAgg()

    def add_table(self, name: str):
        if name and name not in self.produced:
            self.produced.append(name)

    def add_refs(self, target: str, refs: list[str]):
        for full in refs:
            r = _last_segment(full)
            q = _qualifier(full)
            if q:
                self.schemas.setdefault(r, q)
            if not r or r == target:
                continue
            self.primary.setdefault(target, r)
            edge = {"source": r, "target": target}
            if edge not in self.lineage:
                self.lineage.append(edge)


def _python_refs(body: str) -> list[str]:
    hits = [(m.start(), m.group(1)) for m in _REF_READ.finditer(body)]
    hits += [(m.start(), m.group(1)) for m in _REF_LIVE.finditer(body)]
    for m in _SPARK_SQL.finditer(body):
        for t in _sql_real_tables(m.group(2))[2]:
            hits.append((m.start(), t))
    return [t for _, t in sorted(hits, key=lambda x: x[0])]


def _analyze_python(code: str, an: _Analysis) -> None:
    defs = _py_definitions(code)
    if defs is None:
        defs = _regex_definitions(code)
    for name in _py_produced_without_body(code):
        an.add_table(name)
    for e in _py_cdc_lineage(code):
        an.add_refs(e["target"], [e["source"]])
    for name, body in defs:
        an.add_table(name)
        an.add_refs(name, _python_refs(body))
        _infer_python_joins(body, an.agg, name)
        for m in _SPARK_SQL.finditer(body):
            _infer_from_sql(m.group(2), an.agg, name)


def _analyze_sql(code: str, an: _Analysis) -> None:
    for name, stmt, _is_flow in _sql_definitions(code):
        an.add_table(name)
        body = stmt
        cm = _SQL_CREATE.search(stmt)
        if cm:
            body = stmt[cm.end():]  # don't count the created name as a reference
        if _is_flow:
            im = _SQL_INTO.search(body)
            if im:
                body = body[im.end():]
        an.add_refs(name, [t for t in _sql_real_tables(body)[2]])
        _infer_from_sql(body, an.agg, name)


def _analyze_file(code: str, path: str | None, an: _Analysis) -> None:
    if not code or not code.strip():
        return
    if _detect_language(code, path) == "sql":
        _analyze_sql(code, an)
    else:
        _analyze_python(code, an)


def _analyze_files(files: list[tuple[str | None, str]]) -> _Analysis:
    an = _Analysis()
    for path, code in files:
        _analyze_file(code, path, an)
    return an


# ── Public parsing API ───────────────────────────────────────────────────────
def infer_relationships_from_source(source: str) -> list[dict]:
    """Best-effort foreign-key inference from the joins in a pipeline's source.
    Returns table-name-keyed relationship dicts; ``resolve_relationships`` maps
    them onto the entities actually on the canvas."""
    if not source:
        return []
    return _analyze_files([(None, source)]).agg.result()


def parse_pipeline_code(source: str) -> tuple[list[str], list[dict]]:
    """Return (produced_tables, lineage_edges[{source,target}]) from pipeline source."""
    an = _analyze_files([(None, source or "")])
    return an.produced, an.lineage


def _assemble_overlay(pipeline_id: str, name: str, catalog, target,
                      files: list[tuple[str | None, str]], paths: list[str]) -> dict:
    an = _analyze_files(files)
    schemas = dict(an.schemas)
    if target:
        for t in an.produced:
            schemas[t] = target
    full_source = "\n\n".join((f"# ── {p} ──\n{c}" if p else c) for p, c in files if c)
    return {
        "pipeline_id": pipeline_id,
        "name": name or "",
        "catalog": catalog,
        "target": target,
        "tables": an.produced,
        "source_paths": paths,
        "source_path": paths[0] if paths else None,
        "source_code": full_source,
        "lineage": an.lineage,
        "primary_sources": an.primary,
        "table_schemas": schemas,
        "inferred_relationships": an.agg.result(),
    }


def build_pipeline_overlay(pipeline_id: str, spec_client=None, export_client=None) -> dict:
    """Full detail for one pipeline: spec (via the app SP) + its source files (via
    the user's token when available) + produced tables + parsed lineage +
    inferred FK relationships. Used to enrich the model in Feature 2."""
    spec, name = _spec_of(pipeline_id, spec_client)
    paths: list[str] = []
    for lib in (spec.libraries or []):
        if lib.notebook and lib.notebook.path:
            paths.append(lib.notebook.path)
        elif lib.file and lib.file.path:
            paths.append(lib.file.path)
        elif getattr(lib, "glob", None) and lib.glob.include:
            paths.extend(_glob_files(lib.glob.include, export_client))
    paths = list(dict.fromkeys(paths))
    files = []
    for path in paths:
        code = _export_ws_file(path, export_client)
        if code:
            files.append((path, code))
    target = getattr(spec, "schema", None) or spec.target
    return _assemble_overlay(pipeline_id, name, spec.catalog, target, files, paths)


def overlay_from_pasted_source(source: str, name: str = "") -> dict:
    """Build an overlay directly from pasted pipeline source — no workspace
    permissions required. This is the reliable path for FK inference."""
    pid = f"pasted:{uuid.uuid4().hex[:8]}"
    return _assemble_overlay(pid, name or "Pasted pipeline", None, None, [(None, source or "")], [])


# ── Resolve inferred relationships onto the canvas entities ──────────────────
def resolve_relationships(overlay: dict, entities: list[dict]) -> list[dict]:
    """Map the overlay's table-name-keyed join relationships onto the entities
    on the canvas, validating columns. Three levels, most specific first:

    1. **join** — the joined tables themselves (both pipeline inputs, or both
       produced by the pipeline): ``silver_leases.site_id → silver_sites``.
    2. **lineage** — the FK carried along lineage onto produced tables: when the
       pipeline builds ``silver_sites`` from ``bronze_sites`` and joins
       ``bronze_cities`` (whose successor is ``silver_cities``), then
       ``silver_sites.city_id → silver_cities``.
    3. **carried key** — a produced table that joined a produced dimension and
       keeps its key: ``fact_tenancy.site_key → dim_site``.

    One relationship per (child, parent) pair; columns must exist on both
    entities and the referenced columns must plausibly be the parent's key.
    """
    by_name: dict[str, list[dict]] = {}
    for e in entities or []:
        by_name.setdefault((e.get("name") or "").lower(), []).append(e)
    schemas = {k.lower(): v for k, v in (overlay.get("table_schemas") or {}).items()}

    def ent(table):
        cands = by_name.get((table or "").lower()) or []
        if len(cands) > 1:
            hint = schemas.get((table or "").lower())
            for e in cands:
                if hint and (e.get("schema") or e.get("schema_name")) == hint:
                    return e
        return cands[0] if cands else None

    def colmap(e):
        return {(c.get("name") or "").lower(): c.get("name") for c in (e.get("columns") or [])}

    def pk_of(e):
        pk = e.get("primary_key") or [c.get("name") for c in (e.get("columns") or []) if c.get("is_primary_key")]
        return [p.lower() for p in pk if p]

    def plausible(parent, tcols):
        pk = pk_of(parent)
        if pk:
            return {c.lower() for c in tcols} == set(pk)
        flags = [_foreign_looking(c, parent.get("name") or "") for c in tcols]
        return not all(flags)

    produced = {t.lower() for t in (overlay.get("tables") or [])}
    primary = {k.lower(): v.lower() for k, v in (overlay.get("primary_sources") or {}).items()}
    succ: dict[str, list[str]] = {}
    for tgt, src in primary.items():
        succ.setdefault(src, []).append(tgt)

    out: list[dict] = []
    by_pair: dict[tuple, list[dict]] = {}

    def try_add(child, fcols, parent, tcols, via, ctx=None):
        if not child or not parent or child.get("id") == parent.get("id"):
            return False
        cm, pm = colmap(child), colmap(parent)
        if not fcols or len(fcols) != len(tcols):
            return False
        if not all(c.lower() in cm for c in fcols) or not all(c.lower() in pm for c in tcols):
            return False
        if not plausible(parent, tcols):
            return False
        pair = (child.get("id"), parent.get("id"))
        fc = [cm[c.lower()] for c in fcols]
        existing = by_pair.get(pair, [])
        if any(e["from_columns"] == fc for e in existing):
            return True
        # One link per table pair — except role-playing joins (the same definition
        # joining one parent twice on different columns, e.g. a_site_id / z_site_id).
        if existing and not (via == "join" and all(e["via"] == "join" and e["_ctx"] == ctx for e in existing)):
            return True
        rel = {
            "from_entity": child.get("id"), "from_columns": fc,
            "to_entity": parent.get("id"), "to_columns": [pm[c.lower()] for c in tcols],
            "type": infer_cardinality(pk_of(child), fc), "description": f"Inferred from pipeline ({via})",
            "inferred": "pipeline", "via": via, "_ctx": ctx,
        }
        by_pair.setdefault(pair, []).append(rel)
        out.append(rel)
        return True

    def linked(a, b):
        return bool(a and b and by_pair.get((a.get("id"), b.get("id"))))

    rels = overlay.get("inferred_relationships") or []
    for r in rels:
        fcols, tcols = r.get("from_columns") or [], r.get("to_columns") or []
        to_t = (r.get("to_table") or "").lower()
        cands = [c.lower() for c in (r.get("from_candidates") or [r.get("from_table")]) if c]
        ctx = (r.get("context_table") or "").lower() or None
        to_produced = to_t in produced

        # 1) the joined tables themselves (never mix pipeline inputs with outputs)
        for c in cands:
            if (c in produced) != to_produced:
                continue
            if try_add(ent(c), fcols, ent(to_t), tcols, "join", ctx):
                break

        # 2) carried along lineage onto produced tables
        parents2 = list(succ.get(to_t, [])) + ([to_t] if to_produced else [])
        children2 = ([ctx] if ctx else []) + [s for c in cands for s in succ.get(c, [])]
        for c2 in dict.fromkeys(children2):
            for p2 in parents2:
                try_add(ent(c2), fcols, ent(p2), tcols, "lineage", ctx)

        # 3) a produced table that keeps the produced parent's key
        if ctx:
            ce = ent(ctx)
            for p2 in parents2:
                pe = ent(p2)
                if not ce or not pe or linked(ce, pe):
                    continue
                keys = pk_of(pe) or [k.lower() for k in _key_like_columns(pe.get("name") or "",
                                                                          list(colmap(pe).values()))]
                cm = colmap(ce)
                if pk_of(pe):
                    if all(k in cm for k in keys):
                        try_add(ce, keys, pe, keys, "carried-key", ctx)
                else:
                    for k in keys:
                        if k in cm and try_add(ce, [k], pe, [k], "carried-key", ctx):
                            break
    for rel in out:
        rel.pop("_ctx", None)
    return out
