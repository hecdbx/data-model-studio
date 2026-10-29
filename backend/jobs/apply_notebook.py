# Databricks notebook source
# MAGIC %md
# MAGIC # Data Model Studio — Apply artifact
# MAGIC
# MAGIC Executes a generated artifact produced by the app. Triggered by the app
# MAGIC (or run manually) with two parameters:
# MAGIC - `artifact_path`: a `/Volumes/.../*.sql` file written by the app
# MAGIC - `mode`: `sql` (execute DDL/DML statement by statement)
# MAGIC
# MAGIC `spark` and `dbutils` are pre-initialized by Databricks — do not create a
# MAGIC SparkSession.

# COMMAND ----------

dbutils.widgets.text("artifact_path", "")
dbutils.widgets.text("mode", "sql")

artifact_path = dbutils.widgets.get("artifact_path").strip()
mode = (dbutils.widgets.get("mode") or "sql").strip().lower()

if not artifact_path:
    raise ValueError("artifact_path is required (a /Volumes/... path to a .sql artifact).")

print(f"Applying artifact: {artifact_path}  (mode={mode})")

# COMMAND ----------

# Read the artifact from the volume. Volume files are readable as local files
# on the driver via their /Volumes path.
with open(artifact_path, "r") as f:
    body = f.read()

print(f"Artifact length: {len(body)} chars")

# COMMAND ----------


def split_sql_statements(text: str):
    """Split a SQL script into statements on semicolons, ignoring semicolons
    inside single/double quotes, backtick identifiers, and -- / /* */ comments."""
    statements, buf = [], []
    i, n = 0, len(text)
    quote = None  # ' " or `
    while i < n:
        ch = text[i]
        nxt = text[i + 1] if i + 1 < n else ""
        if quote:
            buf.append(ch)
            if ch == quote:
                quote = None
            i += 1
            continue
        if ch == "-" and nxt == "-":  # line comment
            while i < n and text[i] != "\n":
                i += 1
            continue
        if ch == "/" and nxt == "*":  # block comment
            i += 2
            while i < n - 1 and not (text[i] == "*" and text[i + 1] == "/"):
                i += 1
            i += 2
            continue
        if ch in ("'", '"', "`"):
            quote = ch
            buf.append(ch)
            i += 1
            continue
        if ch == ";":
            stmt = "".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


if mode != "sql":
    raise ValueError(f"Unsupported mode '{mode}'. This job applies SQL artifacts; "
                     f"create Spark Declarative Pipelines from generated pipeline.py separately.")

statements = split_sql_statements(body)
print(f"Executing {len(statements)} statement(s)...")

# COMMAND ----------

results = []
for idx, stmt in enumerate(statements, start=1):
    preview = " ".join(stmt.split())[:90]
    try:
        spark.sql(stmt)
        results.append((idx, "OK", preview))
        print(f"[{idx}/{len(statements)}] OK   {preview}")
    except Exception as e:  # noqa: BLE001 — surface every failure, keep going
        results.append((idx, "FAIL", f"{preview}  ->  {e}"))
        print(f"[{idx}/{len(statements)}] FAIL {preview}\n       {e}")

# COMMAND ----------

ok = sum(1 for _, s, _ in results if s == "OK")
fail = sum(1 for _, s, _ in results if s == "FAIL")
print(f"\nDone. {ok} succeeded, {fail} failed, {len(statements)} total.")
if fail:
    raise RuntimeError(f"{fail} statement(s) failed — see the log above.")
