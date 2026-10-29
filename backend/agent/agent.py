"""The data-modeling agent: a self-contained ReAct loop over the Databricks
OpenAI-compatible chat client (Claude Opus 5) with the embedded skills + tools.

We deliberately avoid langchain / databricks-langchain: in the Databricks Apps
runtime those pull a `databricks-ai-bridge`/`mcp` transitive that fails to import.
The OpenAI client comes from `databricks-sdk` core (`serving_endpoints
.get_open_ai_client()`), which authenticates as the app's service principal.

Two entry points:
- ``generate_model_from_volume`` — one-shot model design from a UC volume (Feature 1).
- ``stream_chat`` — streaming conversational assistant for editing models and
  generating DDL/DML/SDP (both features).
"""

from __future__ import annotations

import json
import re
from typing import Iterator, Optional

import json as _json

from ..config import get_settings
from ..model_schema import infer_cardinality
from .prompts import build_generate_user_prompt, build_infer_keys_prompt, build_system_prompt
from .tools import build_tools

_MAX_STEPS = 12


def _client():
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient().serving_endpoints.get_open_ai_client()


def _as_text(content) -> str:
    """Coerce a chat message/delta `content` to text. Reasoning models (Opus 5)
    may return a list of content blocks instead of a plain string."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        out = []
        for b in content:
            if isinstance(b, dict):
                t = b.get("text")
                if isinstance(t, str):
                    out.append(t)
            elif isinstance(b, str):
                out.append(b)
        return "".join(out)
    return str(content)


_MODEL_KEYS = ("entities", "model_type", "removed_entities", "removed_relationships")


def extract_model_from_text(text: str) -> Optional[dict]:
    """Pull the last valid model (or model patch) JSON object out of an agent message."""
    if not text:
        return None
    candidates: list[str] = []
    for pat in (r"```json\s*(.*?)```", r"```\s*(.*?)```"):
        candidates += re.findall(pat, text, re.DOTALL)
    candidates.append(text)
    for cand in reversed(candidates):
        cand = cand.strip()
        try:
            obj = json.loads(cand)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict) and any(k in obj for k in _MODEL_KEYS):
            return obj
    depth, start, found = 0, -1, []
    for i, ch in enumerate(text):
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start >= 0:
                found.append(text[start:i + 1])
    for cand in sorted(found, key=len, reverse=True):
        try:
            obj = json.loads(cand)
            if isinstance(obj, dict) and any(k in obj for k in _MODEL_KEYS if k != "model_type"):
                return obj
        except json.JSONDecodeError:
            continue
    return None


def _oai_messages(history: list[dict], system: str, context: Optional[str]) -> list[dict]:
    msgs = [{"role": "system", "content": system}]
    if context:
        msgs.append({"role": "system", "content": context})
    for m in history:
        role = m.get("role", "user")
        role = role if role in ("user", "assistant", "system") else "user"
        msgs.append({"role": role, "content": m.get("content", "")})
    return msgs


def _agent_events(messages: list[dict], specs, funcs, stream: bool) -> Iterator[tuple]:
    """Drive the ReAct loop, yielding ('token', str) / ('tool_call', name) /
    ('tool_result', name) / ('final', full_text)."""
    client = _client()
    s = get_settings()
    ep = s.serving_endpoint
    full: list[str] = []
    finish = None

    for _ in range(_MAX_STEPS):
        content = ""
        tcs: list[dict] = []

        create_kwargs = {"model": ep, "messages": messages, "max_tokens": s.agent_max_tokens}
        if specs:  # omit `tools` entirely when empty — the endpoint rejects tools=null
            create_kwargs["tools"] = specs

        if stream:
            resp = client.chat.completions.create(**create_kwargs, stream=True)
            parts, acc = [], {}
            for ch in resp:
                if not ch.choices:
                    continue
                finish = getattr(ch.choices[0], "finish_reason", None) or finish
                d = ch.choices[0].delta
                piece = _as_text(getattr(d, "content", None))
                if piece:
                    parts.append(piece)
                    yield ("token", piece)
                for tcd in (getattr(d, "tool_calls", None) or []):
                    slot = acc.setdefault(tcd.index, {"id": "", "name": "", "args": ""})
                    if tcd.id:
                        slot["id"] = tcd.id
                    fn = getattr(tcd, "function", None)
                    if fn and fn.name:
                        slot["name"] = fn.name
                    if fn and fn.arguments:
                        slot["args"] += fn.arguments
            content = "".join(parts)
            tcs = [acc[i] for i in sorted(acc)]
        else:
            resp = client.chat.completions.create(**create_kwargs)
            finish = getattr(resp.choices[0], "finish_reason", None)
            m = resp.choices[0].message
            content = _as_text(m.content)
            tcs = [{"id": t.id, "name": t.function.name, "args": t.function.arguments}
                   for t in (m.tool_calls or [])]

        full.append(content)
        asst: dict = {"role": "assistant", "content": content or None}
        if tcs:
            asst["tool_calls"] = [
                {"id": t["id"] or f"call_{i}", "type": "function",
                 "function": {"name": t["name"], "arguments": t["args"] or "{}"}}
                for i, t in enumerate(tcs)
            ]
        messages.append(asst)

        if not tcs:
            break

        for i, t in enumerate(tcs):
            name = t["name"]
            cid = t["id"] or f"call_{i}"
            yield ("tool_call", name)
            try:
                args = json.loads(t["args"] or "{}")
            except json.JSONDecodeError:
                args = {}
            fn = funcs.get(name)
            try:
                out = fn(args) if fn else f"ERROR: unknown tool {name}"
            except Exception as e:  # noqa: BLE001
                out = f"ERROR calling {name}: {e}"
            yield ("tool_result", name)
            messages.append({"role": "tool", "tool_call_id": cid, "content": str(out)})

    yield ("finish_reason", finish)
    yield ("final", "".join(full))


# ─── Feature 1: model from volume (non-streaming) ────────────────────────────
_COL_SPEC = re.compile(r"^\s*`?([A-Za-z_]\w*)`?\s+(.+?)\s*$")


def _name_list(v) -> list[str]:
    if isinstance(v, str):
        return [x.strip() for x in v.split(",") if x.strip()]
    return [str(x) for x in (v or []) if isinstance(x, (str, int))]


def expand_compact_model(obj: dict) -> dict:
    """Expand the compact generation format — ``"name TYPE"`` column strings,
    ``pk`` lists, ``[child, [cols], parent, [cols]]`` relationship tuples — into
    the canvas schema. Canvas-schema entities / relationships pass through."""
    entities, by_ref = [], {}
    for e in obj.get("entities") or []:
        if not isinstance(e, dict) or not e.get("name"):
            continue
        schema = e.get("schema") or e.get("schema_name") or None
        pk = _name_list(e.get("pk") or e.get("primary_key"))
        cols = []
        for c in e.get("columns") or []:
            if isinstance(c, dict):
                if c.get("name"):
                    cols.append(dict(c))
                continue
            if isinstance(c, (list, tuple)) and c:
                name, typ = str(c[0]), (str(c[1]) if len(c) > 1 else "STRING")
            elif isinstance(c, str):
                m = _COL_SPEC.match(c)
                name, typ = (m.group(1), m.group(2)) if m else (c.strip(), "STRING")
            else:
                continue
            cols.append({"name": name, "type": typ.strip().upper(), "nullable": True,
                         "is_primary_key": False, "is_foreign_key": False, "description": ""})
        if not pk:
            pk = [c["name"] for c in cols if c.get("is_primary_key")]
        for c in cols:
            if c["name"] in pk:
                c["is_primary_key"] = True
                c["nullable"] = False
        ent = {k: v for k, v in e.items() if k not in ("pk", "columns", "schema_name", "source", "format")}
        # where a source table's raw data lives in the volume (used by the SDP generator)
        if isinstance(e.get("source"), str) and e["source"].strip():
            ent["source_path"] = e["source"].strip().strip("/")
        if isinstance(e.get("format"), str) and e["format"].strip():
            ent["source_format"] = e["format"].strip().lower()
        ent.update({"id": e.get("id") or (f"{schema}.{e['name']}" if schema else e["name"]),
                    "name": e["name"], "schema": schema, "layer": e.get("layer") or "table",
                    "description": e.get("description") or "", "columns": cols, "primary_key": pk})
        entities.append(ent)
        by_ref.setdefault(ent["name"], ent)
        by_ref[ent["id"]] = ent

    rels = []
    for i, r in enumerate(obj.get("relationships") or []):
        if isinstance(r, dict):
            child, fc, parent, pc = r.get("from_entity"), r.get("from_columns"), r.get("to_entity"), r.get("to_columns")
        elif isinstance(r, (list, tuple)) and len(r) >= 4:
            child, fc, parent, pc = r[:4]
        else:
            continue
        ce, pe = by_ref.get(child), by_ref.get(parent)
        fc, pc = _name_list(fc), _name_list(pc)
        if not ce or not pe or ce is pe or not fc:
            continue
        rels.append({"id": (r.get("id") if isinstance(r, dict) else None) or f"rel_{i}",
                     "from_entity": ce["id"], "from_columns": fc, "to_entity": pe["id"], "to_columns": pc,
                     "type": (r.get("type") if isinstance(r, dict) else None) or infer_cardinality(ce.get("primary_key"), fc),
                     "description": (r.get("description") if isinstance(r, dict) else None) or ""})
        for j, col_name in enumerate(fc):
            col = next((c for c in ce["columns"] if c.get("name") == col_name), None)
            if col is not None:
                col["is_foreign_key"] = True
                col["references"] = {"entity": pe["id"], "column": pc[j] if j < len(pc) else (pc[0] if pc else "")}
    out = {k: v for k, v in obj.items() if k not in ("entities", "relationships")}
    out.update({"entities": entities, "relationships": rels})
    return out


def generate_model_from_volume(catalog: str, schema: str, volume: str,
                               model_type: str = "medallion") -> dict:
    try:
        specs, funcs = build_tools(model_type)
        sys = build_system_prompt(model_type=model_type)
        user = build_generate_user_prompt(catalog, schema, volume, model_type)
        messages = _oai_messages([{"role": "user", "content": user}], sys, None)
        raw, finish = "", None
        for kind, payload in _agent_events(messages, specs, funcs, stream=False):
            if kind == "final":
                raw = payload
            elif kind == "finish_reason":
                finish = payload
        model = extract_model_from_text(raw)
        if not model or not model.get("entities"):
            if finish == "length":
                return {"error": "The model was too large to finish in one response (output token limit). "
                                 "Try the star schema pattern, or a volume with fewer files.", "raw": raw[-2000:]}
            return {"error": "The agent did not return a parseable model.", "raw": raw[:2000]}
        model = expand_compact_model(model)
        model.setdefault("source", {})
        model["source"].update({"kind": "volume", "catalog": catalog, "schema": schema, "volume": volume})
        return {"model": model}
    except Exception as e:  # noqa: BLE001
        return {"error": f"Agent error: {e}"}


# ─── Both features: infer primary/foreign keys for the whole model ───────────
def _slim_model_for_keys(model: dict) -> dict:
    """Send the agent only what it needs to reason about keys — entity ids/names
    and column names/types. Dropping positions/descriptions/tags keeps the prompt
    small so even large models fit well within limits."""
    ents = []
    for e in (model.get("entities") or []):
        ents.append({
            "id": e.get("id"),
            "name": e.get("name"),
            "schema": e.get("schema") or e.get("schema_name"),
            "layer": e.get("layer"),
            "columns": [{"name": c.get("name"), "type": c.get("type")}
                        for c in (e.get("columns") or [])],
        })
    return {"entities": ents}


def _extract_json_object(text: str) -> Optional[dict]:
    """Pull the last valid JSON object out of an agent message (any shape)."""
    if not text:
        return None
    cands: list[str] = []
    for pat in (r"```json\s*(.*?)```", r"```\s*(.*?)```"):
        cands += re.findall(pat, text, re.DOTALL)
    cands.append(text)
    for c in reversed(cands):
        try:
            obj = json.loads(c.strip())
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            continue
    depth, start, found = 0, -1, []
    for i, ch in enumerate(text):
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start >= 0:
                found.append(text[start:i + 1])
    for c in sorted(found, key=len, reverse=True):
        try:
            obj = json.loads(c)
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            continue
    return None


def _apply_key_diff(original: dict, diff: dict) -> dict:
    """Apply a compact {primary_keys, foreign_keys} diff onto the real model,
    never dropping an entity, column, position, or id."""
    import copy
    import uuid as _uuid

    out = copy.deepcopy(original)
    entities = out.get("entities") or []
    valid_ids = {e.get("id") for e in entities}
    name_to_id = {e.get("name"): e.get("id") for e in entities}
    ent_by_id = {e.get("id"): e for e in entities}
    ent_by_name: dict = {}
    for e in entities:
        ent_by_name.setdefault(e.get("name"), e)

    def _norm(ref):
        return ref if ref in valid_ids else name_to_id.get(ref, ref)

    def _find(ref):
        return ent_by_id.get(ref) or ent_by_id.get(_norm(ref)) or ent_by_name.get(ref)

    # primary keys
    for ref, cols in (diff.get("primary_keys") or {}).items():
        e = _find(ref)
        if not e or not isinstance(cols, list):
            continue
        names = {c.get("name") for c in (e.get("columns") or [])}
        pk = [c for c in cols if c in names]
        colset = set(pk)
        for col in (e.get("columns") or []):
            if col.get("name") in colset:
                col["is_primary_key"] = True
                col["nullable"] = False
        if pk:
            e["primary_key"] = pk

    # foreign keys → relationships + child column flags
    if not isinstance(out.get("relationships"), list):
        out["relationships"] = []

    def _relkey(r):
        return (_norm(r.get("from_entity")), tuple(r.get("from_columns") or []),
                _norm(r.get("to_entity")))

    seen = {_relkey(r) for r in out["relationships"]}
    for fk in (diff.get("foreign_keys") or []):
        fe, te = _norm(fk.get("from_entity")), _norm(fk.get("to_entity"))
        if fe not in valid_ids or te not in valid_ids or fe == te:
            continue
        fcols = fk.get("from_columns") or []
        tcols = fk.get("to_columns") or []
        child = ent_by_id.get(fe)
        child_pk = (child or {}).get("primary_key") or [
            c.get("name") for c in ((child or {}).get("columns") or []) if c.get("is_primary_key")]
        rel = {"id": f"rel_{_uuid.uuid4().hex[:8]}", "from_entity": fe,
               "from_columns": fcols, "to_entity": te, "to_columns": tcols,
               "type": fk.get("type") or infer_cardinality(child_pk, fcols), "description": "inferred"}
        k = _relkey(rel)
        if k in seen:
            continue
        seen.add(k)
        out["relationships"].append(rel)
        if child:
            for i, fc in enumerate(fcols):
                col = next((c for c in (child.get("columns") or []) if c.get("name") == fc), None)
                if col:
                    col["is_foreign_key"] = True
                    col["references"] = {"entity": te,
                                         "column": (tcols[i] if i < len(tcols) else (tcols[0] if tcols else ""))}
    return out


def infer_keys(model: dict) -> dict:
    """Infer primary + foreign keys for the model on the canvas. Uses a single,
    tool-free reasoning call that returns a COMPACT diff (not the whole model),
    then applies it — fast and immune to output-token/prompt-size limits.
    Returns {'model': ...} or {'error': ...}."""
    try:
        sys = build_system_prompt()
        user = build_infer_keys_prompt(_json.dumps(_slim_model_for_keys(model), default=str))
        messages = _oai_messages([{"role": "user", "content": user}], sys, None)
        raw = ""
        for kind, payload in _agent_events(messages, [], {}, stream=False):  # no tools → one call
            if kind == "final":
                raw = payload
        diff = _extract_json_object(raw)
        if not diff or not (diff.get("primary_keys") or diff.get("foreign_keys")):
            return {"error": "The agent did not return usable keys.", "raw": raw[:2000]}
        return {"model": _apply_key_diff(model, diff)}
    except Exception as e:  # noqa: BLE001
        return {"error": f"Agent error: {e}"}


# ─── Both features: streaming chat ───────────────────────────────────────────
def _sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


_MODEL_BUDGET = 60000  # chars of canvas JSON sent as context


def _compact_model(m: dict, budget: int = _MODEL_BUDGET) -> str:
    """The canvas model as compact JSON that fits the budget — never truncated
    mid-object (a cut-off model makes the agent drop tables it can't see).
    Detail degrades step by step: descriptions, then column types, then columns."""
    ents = m.get("entities") or []
    rels = [{"id": r.get("id"), "from": r.get("from_entity"), "from_columns": r.get("from_columns") or [],
             "to": r.get("to_entity"), "to_columns": r.get("to_columns") or []}
            for r in (m.get("relationships") or [])]

    def ent(e, level):
        out = {"id": e.get("id"), "name": e.get("name"), "schema": e.get("schema") or e.get("schema_name"),
               "layer": e.get("layer")}
        if level == 0 and e.get("description"):
            out["description"] = str(e["description"])[:120]
        if level < 3:
            cols = []
            for c in e.get("columns") or []:
                if level == 2:
                    cols.append(c.get("name"))
                    continue
                cc = {"name": c.get("name"), "type": c.get("type")}
                if c.get("is_primary_key"):
                    cc["pk"] = True
                if c.get("is_foreign_key"):
                    cc["fk"] = True
                if c.get("nullable") is False:
                    cc["not_null"] = True
                if level == 0 and c.get("description"):
                    cc["description"] = str(c["description"])[:80]
                cols.append(cc)
            out["columns"] = cols
        return out

    s = ""
    for level in (0, 1, 2, 3):
        payload = {"name": m.get("name"), "model_type": m.get("model_type"),
                   "entities": [ent(e, level) for e in ents],
                   "relationships": rels if level < 3 else []}
        s = json.dumps(payload, default=str, ensure_ascii=False)
        if len(s) <= budget:
            return s
    return json.dumps({"entities": [e.get("id") for e in ents]}, ensure_ascii=False)[:budget]


_PATCH_RULES = (
    "## Applying changes to the canvas\n"
    "When your answer changes the model, end with ONE ```json block holding a **patch** the canvas "
    "merges into the current model:\n"
    "```json\n"
    '{"entities": [<complete entity object, canvas schema, for every table you ADD or CHANGE — '
    "with ALL of its columns, not only the new ones>],\n"
    ' "relationships": [<relationships you add or change>],\n'
    ' "removed_entities": ["<entity id>"], "removed_relationships": ["<relationship id>"]}\n'
    "```\n"
    "Leave out everything you did not change — the canvas keeps it. Reuse the entity and relationship "
    "ids shown above; list deletions explicitly in removed_entities / removed_relationships.")


def _context_message(model_json: Optional[dict], pipeline_source: Optional[str]) -> Optional[str]:
    chunks = []
    if model_json and (model_json.get("entities") or []):
        n_e = len(model_json.get("entities") or [])
        n_r = len(model_json.get("relationships") or [])
        chunks.append(f"The model currently on the canvas — {n_e} tables, {n_r} relationships "
                      "(compact JSON; column flags: pk = primary key, fk = foreign key, "
                      "not_null = NOT NULL):\n```json\n" + _compact_model(model_json) + "\n```")
    if pipeline_source:
        chunks.append("The attached Lakeflow pipeline source:\n```python\n" + pipeline_source[:16000] + "\n```")
    if not chunks:
        return None
    return "## Current context\n" + "\n\n".join(chunks) + "\n\n" + _PATCH_RULES


def stream_chat(history: list[dict], model_json: Optional[dict] = None,
                pipeline_source: Optional[str] = None) -> Iterator[str]:
    """Yield Server-Sent-Events strings: token, tool_call, tool_result, final, error."""
    try:
        specs, funcs = build_tools(None)
        messages = _oai_messages(history, build_system_prompt(include_all_patterns=True),
                                 _context_message(model_json, pipeline_source))
    except Exception as e:  # noqa: BLE001
        yield _sse("error", {"message": f"Could not initialize the agent: {e}"})
        return

    full = ""
    try:
        for kind, payload in _agent_events(messages, specs, funcs, stream=True):
            if kind == "token":
                yield _sse("token", {"text": payload})
            elif kind == "tool_call":
                yield _sse("tool_call", {"name": payload})
            elif kind == "tool_result":
                yield _sse("tool_result", {"name": payload})
            elif kind == "finish_reason" and payload == "length":
                yield _sse("token", {"text": "\n\n_(The reply hit the output limit and was cut off — ask me to continue.)_"})
            elif kind == "final":
                full = payload
    except Exception as e:  # noqa: BLE001
        yield _sse("error", {"message": str(e)})
        return

    yield _sse("final", {"text": full, "model": extract_model_from_text(full)})
