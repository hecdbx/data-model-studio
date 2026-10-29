"""Run a .sql file statement-by-statement on a SQL warehouse (fails fast).

Usage: python run_sql.py <file.sql> [--profile P] [--warehouse W]
"""
import argparse
import re
import sys
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


def split_statements(text: str):
    # strip line comments, then split on ; outside quotes
    out, buf, q = [], [], None
    i = 0
    while i < len(text):
        ch = text[i]
        if q:
            buf.append(ch)
            if ch == q:
                q = None
        elif ch == "-" and text[i:i + 2] == "--":
            while i < len(text) and text[i] != "\n":
                i += 1
            continue
        elif ch in "'\"`":
            q = ch
            buf.append(ch)
        elif ch == ";":
            s = "".join(buf).strip()
            if s:
                out.append(s)
            buf = []
        else:
            buf.append(ch)
        i += 1
    s = "".join(buf).strip()
    if s:
        out.append(s)
    return out


ap = argparse.ArgumentParser()
ap.add_argument("file")
ap.add_argument("--profile", default="fevm-hec-demo")
ap.add_argument("--warehouse", default="3cdad602e9d7f4c7")
a = ap.parse_args()

w = WorkspaceClient(profile=a.profile)
stmts = split_statements(open(a.file, encoding="utf-8").read())
for n, s in enumerate(stmts, 1):
    head = " ".join(s.split())[:100]
    t0 = time.time()
    r = w.statement_execution.execute_statement(warehouse_id=a.warehouse, statement=s, wait_timeout="50s")
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(1.5)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state != StatementState.SUCCEEDED:
        print(f"[{n}/{len(stmts)}] FAIL {head}\n   {r.status.error.message if r.status.error else r.status.state}")
        sys.exit(1)
    rows = (r.result.data_array or []) if r.result else []
    extra = f" -> {rows[:3]}" if rows and s.lstrip().upper().startswith("SELECT") else ""
    print(f"[{n}/{len(stmts)}] OK {time.time() - t0:5.1f}s {head}{extra}")
