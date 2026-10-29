"""Unit tests for agent-side pure helpers (no network)."""
from backend.agent.agent import (
    _slim_model_for_keys,
    _extract_json_object,
    _apply_key_diff,
    extract_model_from_text,
)


BASE = {
    "name": "shop",
    "entities": [
        {"id": "e1", "name": "customers", "schema": "s", "layer": "table",
         "position": {"x": 10, "y": 20}, "description": "x" * 200,
         "columns": [{"name": "id", "type": "BIGINT", "description": "d"},
                     {"name": "email", "type": "STRING"}]},
        {"id": "e2", "name": "orders",
         "columns": [{"name": "order_id", "type": "BIGINT"},
                     {"name": "customer_id", "type": "BIGINT"}]},
    ],
    "relationships": [],
}


def test_slim_drops_noise():
    slim = _slim_model_for_keys(BASE)
    txt = str(slim)
    assert "position" not in txt and "description" not in txt
    assert slim["entities"][0]["columns"][0]["name"] == "id"


def test_extract_json_object_from_fenced():
    text = 'Here:\n```json\n{"primary_keys": {"e1": ["id"]}, "foreign_keys": []}\n```\ndone'
    obj = _extract_json_object(text)
    assert obj["primary_keys"]["e1"] == ["id"]


def test_extract_json_object_bare():
    obj = _extract_json_object('noise {"foreign_keys": [{"from_entity": "e2"}]} tail')
    assert obj["foreign_keys"][0]["from_entity"] == "e2"


def test_extract_json_object_none():
    assert _extract_json_object("no json here") is None


def test_apply_key_diff_sets_pk_fk_and_preserves():
    diff = {
        "primary_keys": {"e1": ["id"], "e2": ["order_id"]},
        "foreign_keys": [
            {"from_entity": "e2", "from_columns": ["customer_id"],
             "to_entity": "e1", "to_columns": ["id"]},
        ],
    }
    out = _apply_key_diff(BASE, diff)
    cust, orders = out["entities"][0], out["entities"][1]
    # positions/columns preserved
    assert cust["position"] == {"x": 10, "y": 20}
    assert [c["name"] for c in cust["columns"]] == ["id", "email"]
    # keys applied
    assert cust["primary_key"] == ["id"]
    fk = [c for c in orders["columns"] if c.get("is_foreign_key")]
    assert fk and fk[0]["references"] == {"entity": "e1", "column": "id"}
    # relationship added with an id
    assert len(out["relationships"]) == 1
    r = out["relationships"][0]
    assert r["from_entity"] == "e2" and r["to_entity"] == "e1" and r.get("id")


def test_apply_key_diff_normalizes_name_refs_to_ids():
    # agent used names instead of ids for the FK endpoints
    diff = {"primary_keys": {}, "foreign_keys": [
        {"from_entity": "orders", "from_columns": ["customer_id"],
         "to_entity": "customers", "to_columns": ["id"]}]}
    out = _apply_key_diff(BASE, diff)
    r = out["relationships"][0]
    assert r["from_entity"] == "e2" and r["to_entity"] == "e1"


def test_apply_key_diff_skips_unknown_entities():
    diff = {"foreign_keys": [{"from_entity": "ghost", "from_columns": ["x"],
                              "to_entity": "e1", "to_columns": ["id"]}]}
    out = _apply_key_diff(BASE, diff)
    assert out["relationships"] == []


def test_extract_model_from_text():
    text = '```json\n{"entities": [{"id": "a", "name": "a", "columns": []}]}\n```'
    m = extract_model_from_text(text)
    assert m and m["entities"][0]["id"] == "a"
