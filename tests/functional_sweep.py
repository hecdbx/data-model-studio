"""Live functional sweep of every Data Model Studio API feature against the Tawal
synthetic dataset (see demo_data/tawal). Not collected by pytest.

Usage:
  python tests/functional_sweep.py http://127.0.0.1:8000
  python tests/functional_sweep.py https://<app-url> --profile fevm-hec-demo   # bearer token via CLI
  add --skip-agent to skip the (slow) LLM checks, --apply to also run the apply job.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

CAT = "hec_demo_catalog"
results: list[tuple[str, bool, str]] = []


def check(name, ok, detail=""):
    results.append((name, bool(ok), detail))
    print(f"{'PASS' if ok else 'FAIL'}  {name}  {detail}", flush=True)
    return ok


class Api:
    def __init__(self, base, token=None):
        self.base, self.token = base.rstrip("/"), token

    def _req(self, method, path, body=None, timeout=300):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        if self.token:
            req.add_header("Authorization", f"Bearer {self.token}")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                raw = r.read().decode()
                return r.status, (json.loads(raw) if raw and raw.lstrip()[:1] in "{[" else raw)
        except urllib.error.HTTPError as e:
            raw = e.read().decode()
            try:
                return e.code, json.loads(raw)
            except json.JSONDecodeError:
                return e.code, raw

    def get(self, path, **kw):
        return self._req("GET", path, **kw)

    def post(self, path, body, **kw):
        return self._req("POST", path, body, **kw)

    def sse(self, path, body, timeout=300):
        data = json.dumps(body).encode()
        req = urllib.request.Request(self.base + path, data=data, method="POST")
        req.add_header("Content-Type", "application/json")
        if self.token:
            req.add_header("Authorization", f"Bearer {self.token}")
        events = []
        with urllib.request.urlopen(req, timeout=timeout) as r:
            buf = ""
            for line in r:
                buf += line.decode()
                while "\n\n" in buf:
                    part, buf = buf.split("\n\n", 1)
                    ev, dat = "message", ""
                    for ln in part.split("\n"):
                        if ln.startswith("event:"):
                            ev = ln[6:].strip()
                        elif ln.startswith("data:"):
                            dat += ln[5:].strip()
                    if dat:
                        events.append((ev, json.loads(dat)))
        return events

    def poll_job(self, job_id, timeout=420):
        t0 = time.time()
        while time.time() - t0 < timeout:
            _, j = self.get(f"/api/agent/job/{job_id}")
            if isinstance(j, dict) and j.get("status") in ("done", "error"):
                return j, time.time() - t0
            time.sleep(3)
        return {"status": "timeout"}, time.time() - t0


def slim(entities):
    return [{"id": e["id"], "name": e["name"], "schema": e.get("schema"), "primary_key": e.get("primary_key") or [],
             "columns": [{"name": c["name"], "is_primary_key": c.get("is_primary_key", False)} for c in e["columns"]]}
            for e in entities]


def rel_set(model):
    return {(r["from_entity"], tuple(r["from_columns"]), r["to_entity"], tuple(r["to_columns"]))
            for r in model["relationships"]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("base")
    ap.add_argument("--profile")
    ap.add_argument("--skip-agent", action="store_true")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    token = None
    if a.profile:
        token = json.loads(subprocess.check_output(["databricks", "auth", "token", "-p", a.profile]))["access_token"]
    api = Api(a.base, token)

    # ── health / setup ───────────────────────────────────────────────────
    s, h = api.get("/api/health")
    check("health", s == 200 and h.get("status") == "healthy" and h.get("sdk_connected"), str(h)[:160])
    s, st = api.get("/api/bootstrap/status")
    check("bootstrap status", s == 200 and st.get("initialized") and st.get("job_exists"),
          f"schema={st.get('schema_exists')} volume={st.get('volume_exists')} job={st.get('job_id')}")

    # ── UC browse ────────────────────────────────────────────────────────
    s, c = api.get("/api/catalogs")
    check("catalogs list", s == 200 and any(x["name"] == CAT for x in c.get("catalogs", [])),
          f"{len(c.get('catalogs', []))} catalogs")
    s, sc = api.get(f"/api/catalogs/{CAT}/schemas")
    names = {x["name"] for x in sc.get("schemas", [])}
    want = {"tawal_raw", "tawal_ref", "tawal_ops", "tawal_bronze", "tawal_silver", "tawal_gold"}
    check("schemas list has tawal_*", want <= names, f"{len(names)} schemas")
    s, bad = api.get("/api/catalogs/not-a-valid%20name/schemas")
    check("invalid catalog rejected", s >= 400 and "error" in (bad if isinstance(bad, dict) else {}), str(bad)[:100])

    # ── ER from UC constraints ───────────────────────────────────────────
    t0 = time.time()
    s, r = api.post("/api/model/from-schemas", {"catalog": CAT, "schemas": ["tawal_ops"]})
    ops = r.get("model", {})
    check("from-schemas tawal_ops", s == 200 and len(ops.get("entities", [])) == 14,
          f"{len(ops.get('entities', []))} tables, {len(ops.get('relationships', []))} rels, {time.time()-t0:.1f}s")
    check("ops-only rels exclude unloaded ref parents", len(ops.get("relationships", [])) == 20,
          f"{len(ops.get('relationships', []))} (expect 20)")
    s, r = api.post("/api/model/from-schemas", {"catalog": CAT, "schemas": ["tawal_ref", "tawal_ops"]})
    core = r.get("model", {})
    rs = rel_set(core)
    check("from-schemas ref+ops", len(core.get("entities", [])) == 19 and len(core.get("relationships", [])) == 31,
          f"{len(core.get('entities', []))} tables, {len(core.get('relationships', []))} rels (expect 19 / 31)")
    check("composite FK leases→tower_slots",
          ("tawal_ops.leases", ("tower_id", "slot_no"), "tawal_ops.tower_slots", ("tower_id", "slot_no")) in rs)
    check("two FKs fiber_links→sites (a/z)",
          ("tawal_ops.fiber_links", ("a_site_id",), "tawal_ops.sites", ("site_id",)) in rs
          and ("tawal_ops.fiber_links", ("z_site_id",), "tawal_ops.sites", ("site_id",)) in rs)
    check("cross-schema FK sites→tawal_ref.cities",
          ("tawal_ops.sites", ("city_id",), "tawal_ref.cities", ("city_id",)) in rs)
    kinds = [r.get("type") for r in core.get("relationships", [])]
    one = [(r["from_entity"], r["to_entity"]) for r in core.get("relationships", []) if r.get("type") == "one-to-one"]
    check("cardinality: 1:1 extension vs N:1 FKs",
          one == [("tawal_ops.site_power_profiles", "tawal_ops.sites")] and kinds.count("many-to-one") == 30,
          f"one-to-one={one}, many-to-one={kinds.count('many-to-one')}")
    ts = next((e for e in core.get("entities", []) if e["id"] == "tawal_ops.tower_slots"), {})
    check("composite PK tower_slots", ts.get("primary_key") == ["tower_id", "slot_no"], str(ts.get("primary_key")))
    reg = next((e for e in core.get("entities", []) if e["id"] == "tawal_ref.regions"), {})
    check("comments + arabic-safe metadata", bool(reg.get("description")), reg.get("description", "")[:60])
    s, r = api.post("/api/model/from-schemas", {"catalog": CAT, "schemas": ["tawal_bronze", "tawal_silver", "tawal_gold"]})
    med = r.get("model", {})
    med_names = {e["name"] for e in med.get("entities", [])}
    check("medallion schemas load (internal tables hidden)",
          len(med_names) == 51 and not any(n.startswith(("__", "event_log_")) for n in med_names),
          f"{len(med_names)} tables (expect 51)")
    s, r = api.post("/api/model/from-schemas", {"catalog": CAT, "schemas": ["bad name;"]})
    check("from-schemas rejects invalid schema", s == 400, str(r)[:100])

    # ── pipelines ────────────────────────────────────────────────────────
    s, pls = api.get("/api/pipelines")
    plist = pls.get("pipelines", []) if isinstance(pls, dict) else []
    by = {p["name"]: p["pipeline_id"] for p in plist}
    tawal = {k: v for k, v in by.items() if k.startswith("tawal-")}
    check("pipelines list", s == 200, f"{len(plist)} visible, tawal: {sorted(tawal)}")
    ents = slim(med.get("entities", []))
    expect = {"tawal-bronze-ingest": (18, 0), "tawal-silver": (18, 11), "tawal-gold": (15, 17)}
    for name, (n_tables, n_rels) in expect.items():
        pid = tawal.get(name)
        if not pid:
            print(f"SKIP  attach {name}  not visible to the app identity (grant it CAN_VIEW on the pipeline)", flush=True)
            continue
        t0 = time.time()
        s, ov = api.post(f"/api/pipelines/{pid}/attach", {"entities": ents})
        ok = s == 200 and len(ov.get("tables", [])) == n_tables and len(ov.get("relationships", [])) >= n_rels
        check(f"attach {name}", ok,
              f"{len(ov.get('source_paths', []))} files, {len(ov.get('tables', []))} tables, "
              f"{len(ov.get('lineage', []))} lineage, {len(ov.get('relationships', []))} FKs, "
              f"target={ov.get('target')}, {time.time()-t0:.1f}s")
    gold_src = "\n".join(open(f"demo_data/tawal/pipelines/gold/transformations/{f}").read()
                         for f in ("dimensions.py", "facts.py"))
    s, ov = api.post("/api/pipelines/parse", {"source": gold_src, "name": "gold (pasted)", "entities": ents})
    check("parse pasted gold source", s == 200 and len(ov.get("relationships", [])) >= 15,
          f"{len(ov.get('tables', []))} tables, {len(ov.get('relationships', []))} FKs")
    s, ov = api.post("/api/pipelines/parse", {"source": "  "})
    check("parse rejects empty source", s == 400)

    # ── volumes ──────────────────────────────────────────────────────────
    s, v = api.get(f"/api/catalogs/{CAT}/schemas/tawal_raw/volumes")
    check("volumes list", s == 200 and any(x["name"] == "landing" for x in v.get("volumes", [])), str(v)[:120])
    base = f"/Volumes/{CAT}/tawal_raw/landing"
    s, f = api.get("/api/volume-files?path=" + urllib.parse.quote(base))
    check("volume files", s == 200 and len(f.get("files", [])) == 18, f"{len(f.get('files', []))} entries")
    s, smp = api.get("/api/file-sample?path=" + urllib.parse.quote(f"{base}/sites/sites.csv") + "&max_rows=5")
    check("csv sample", s == 200 and smp.get("row_count_sample") == 5 and "site_id" in smp.get("columns", []),
          f"cols={smp.get('total_columns')} types={list(smp.get('schema_info', {}).items())[:3]}")
    s, smp = api.get("/api/file-sample?path=" + urllib.parse.quote(f"{base}/alarms/alarms.json") + "&max_rows=5")
    check("json-lines sample", s == 200 and smp.get("row_count_sample") == 5 and "alarm_id" in smp.get("columns", []),
          f"rows={smp.get('row_count_sample')} cols={smp.get('columns')}")
    s, smp = api.get("/api/file-sample?path=" + urllib.parse.quote(f"{base}/regions/regions.csv") + "&max_rows=13")
    arabic = any("الرياض" == (row.get("region_name_ar") or "") for row in smp.get("sample_rows", []))
    check("arabic text round-trips", s == 200 and arabic)
    s, smp = api.get("/api/file-sample?path=" + urllib.parse.quote("/Volumes/x/y/z/../../etc/passwd"))
    check("unsafe path rejected", s == 400)
    t0 = time.time()
    s, snap = api.get(f"/api/volume-snapshot?catalog={CAT}&schema=tawal_raw&volume=landing", timeout=600)
    srcs = snap.get("data_sources", []) if isinstance(snap, dict) else []
    errs = [d for d in srcs if d.get("error")]
    check("volume snapshot covers every feed", s == 200 and len({d['file_path'].split('/')[-2] for d in srcs}) == 18 and not errs,
          f"{len(srcs)} sampled, {len(errs)} errors, {time.time()-t0:.1f}s")

    # ── codegen ──────────────────────────────────────────────────────────
    s, d = api.post("/api/codegen/ddl", {"model": core})
    code = d.get("code", "")
    check("ddl codegen", s == 200 and code.count("CREATE TABLE IF NOT EXISTS") == 19
          and "FOREIGN KEY (`tower_id`, `slot_no`)" in code and code.count("FOREIGN KEY") == 31,
          f"{len(code)} chars, {code.count('FOREIGN KEY')} FKs")
    s, d = api.post("/api/codegen/sdp", {"model": med})
    sdp = d.get("code", "")
    try:
        compile(sdp, "pipeline.py", "exec")
        sdp_ok = True
    except SyntaxError as e:
        sdp_ok = str(e)
    n_defs = sdp.count("@dp.table(") + sdp.count("@dp.materialized_view(")
    check("sdp codegen compiles", s == 200 and sdp_ok is True and n_defs == 51 and "dp.read(" not in sdp,
          f"compiles={sdp_ok} definitions={n_defs}")
    edited = json.loads(json.dumps(core))
    sites = next(e for e in edited["entities"] if e["name"] == "sites")
    sites["columns"].append({"name": "fiber_ready", "type": "BOOLEAN", "nullable": True, "description": "FTTS available"})
    edited["entities"].append({"id": "tawal_ops.site_permits", "name": "site_permits", "schema": "tawal_ops",
                               "columns": [{"name": "permit_id", "type": "STRING", "is_primary_key": True, "nullable": False},
                                           {"name": "site_id", "type": "STRING"}], "primary_key": ["permit_id"]})
    edited["relationships"].append({"id": "r_new", "from_entity": "tawal_ops.site_permits", "from_columns": ["site_id"],
                                    "to_entity": "tawal_ops.sites", "to_columns": ["site_id"], "type": "many-to-one"})
    s, d = api.post("/api/codegen/migration", {"base": core, "edited": edited})
    mig = d.get("code", "")
    check("migration codegen (additive)", s == 200 and "ADD COLUMN `fiber_ready` BOOLEAN" in mig
          and "`site_permits`" in mig and "fk_site_permits_site_id" in mig and mig.count("FOREIGN KEY") == 1,
          f"{mig.count('ALTER TABLE')} ALTERs")

    # ── artifacts + apply ────────────────────────────────────────────────
    s, d = api.post("/api/artifacts/save", {"name": "tawal_sweep_check.sql", "content": "-- sweep\nSELECT 1;", "mode": "sql"})
    check("artifact save", s == 200 and d.get("path", "").endswith("/tawal_sweep_check.sql"), d.get("path", ""))
    if a.apply:
        ddl = ("CREATE TABLE IF NOT EXISTS `hec_demo_catalog`.`data_model_studio`.`sweep_probe` (id BIGINT NOT NULL, note STRING);\n"
               "INSERT INTO `hec_demo_catalog`.`data_model_studio`.`sweep_probe` VALUES (1, 'it''s; fine');\n"
               "DROP TABLE IF EXISTS `hec_demo_catalog`.`data_model_studio`.`sweep_probe`;")
        s, d = api.post("/api/apply", {"name": "sweep_probe.sql", "content": ddl, "mode": "sql"})
        ok = s == 200 and d.get("run_id")
        state = {}
        if ok:
            t0 = time.time()
            while time.time() - t0 < 600:
                _, state = api.get(f"/api/apply/run/{d['run_id']}")
                if state.get("result_state"):
                    break
                time.sleep(10)
        check("apply job runs DDL", ok and state.get("result_state") == "SUCCESS",
              f"run={d.get('run_id')} state={state.get('life_cycle_state')}/{state.get('result_state')}")

    if a.skip_agent:
        return

    # ── agent (LLM) — run concurrently ───────────────────────────────────
    gold = {**med, "entities": [e for e in med["entities"] if e.get("schema") == "tawal_gold"], "relationships": []}

    def infer_keys():
        s, j = api.post("/api/agent/infer-keys", {"model": gold})
        job, dt = api.poll_job(j.get("job_id"))
        m = job.get("model") or {}
        pks = sum(1 for e in m.get("entities", []) if e.get("primary_key"))
        return ("agent infer-keys (gold)", job.get("status") == "done" and len(m.get("entities", [])) == 15
                and pks >= 12 and len(m.get("relationships", [])) >= 12,
                f"{job.get('status')} {dt:.0f}s pks={pks} rels={len(m.get('relationships', []))} {job.get('error', '')[:120]}")

    def from_volume():
        s, j = api.post("/api/agent/generate-from-volume",
                        {"catalog": CAT, "schema_name": "tawal_raw", "volume": "landing", "model_type": "star_schema"})
        job, dt = api.poll_job(j.get("job_id"), timeout=600)
        m = job.get("model") or {}
        return ("agent generate-from-volume (star)", job.get("status") == "done" and len(m.get("entities", [])) >= 8,
                f"{job.get('status')} {dt:.0f}s entities={len(m.get('entities', []))} rels={len(m.get('relationships', []))} "
                f"{job.get('error', '')[:160]}")

    def chat_tool():
        t0 = time.time()
        ev = api.sse("/api/agent/chat", {"history": [{"role": "user", "content":
                     "Use inspect_uc_schema on hec_demo_catalog.tawal_ref and tell me how many tables it has. One sentence."}],
                     "model": None})
        kinds = [k for k, _ in ev]
        final = next((d for k, d in ev if k == "final"), {})
        return ("agent chat + tool call", "tool_call" in kinds and "final" in kinds and "5" in (final.get("text") or ""),
                f"{time.time()-t0:.0f}s events={sorted(set(kinds))} text={(final.get('text') or '')[:100]!r}")

    def chat_edit():
        t0 = time.time()
        small = {**core, "entities": [e for e in core["entities"] if e["name"] in ("sites", "towers")],
                 "relationships": [r for r in core["relationships"] if r["from_entity"] == "tawal_ops.towers"
                                   and r["to_entity"] == "tawal_ops.sites"]}
        ev = api.sse("/api/agent/chat", {"history": [{"role": "user", "content":
                     "Add a column wind_load_rating STRING to towers. Return the updated model."}], "model": small})
        final = next((d for k, d in ev if k == "final"), {})
        m = final.get("model") or {}
        tw = next((e for e in m.get("entities", []) if e.get("name") == "towers"), {})
        ok = any(c.get("name") == "wind_load_rating" for c in tw.get("columns", []))
        return ("agent chat edit → model JSON", ok, f"{time.time()-t0:.0f}s model={'yes' if m else 'no'}")

    with cf.ThreadPoolExecutor(4) as ex:
        for name, ok, detail in ex.map(lambda f: f(), [infer_keys, from_volume, chat_tool, chat_edit]):
            check(name, ok, detail)


if __name__ == "__main__":
    try:
        main()
    finally:
        failed = [r for r in results if not r[1]]
        print(f"\n{len(results) - len(failed)}/{len(results)} passed")
        sys.exit(1 if failed else 0)
