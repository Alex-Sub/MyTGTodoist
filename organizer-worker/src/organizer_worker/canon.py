from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def _repo_root() -> Path:
    # organizer-worker/src/organizer_worker/canon.py -> repo root
    return Path(__file__).resolve().parents[3]


def _canon_path() -> Path:
    return _repo_root() / "canon" / "intents_v2.yml"


def _load_canon() -> dict[str, Any]:
    path = _canon_path()
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        return {}
    return data


_CANON: dict[str, Any] = _load_canon()


def get_canon() -> dict[str, Any]:
    return _CANON


def _path_to_key(path: str) -> str:
    out = path.strip()
    if out.startswith("entities."):
        out = out[len("entities.") :]
    return out


def _has_value(entities: dict[str, Any], path: str) -> bool:
    key = _path_to_key(path)
    if "|" in key:
        return any(_has_value(entities, part.strip()) for part in key.split("|"))

    value = entities.get(key)
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, dict, tuple, set)):
        return len(value) > 0
    return True


def get_intent_spec(intent: str) -> dict[str, Any] | None:
    intents = get_canon().get("intents", {})
    if not isinstance(intents, dict):
        return None
    spec = intents.get(intent)
    return spec if isinstance(spec, dict) else None


def _validate_group(entities: dict[str, Any], keys: list[str]) -> bool:
    return any(_has_value(entities, k) for k in keys)


def validate_required(intent: str, entities: dict[str, Any]) -> list[str]:
    spec = get_intent_spec(intent) or {}
    missing: list[str] = []

    required = spec.get("required", [])
    if isinstance(required, list):
        for key in required:
            if isinstance(key, str) and not _has_value(entities, key):
                missing.append(key)

    for field_name in ("required_any", "required_any_2", "required_one_of_fields"):
        keys = spec.get(field_name, [])
        if isinstance(keys, list) and keys:
            str_keys = [k for k in keys if isinstance(k, str)]
            if str_keys and not _validate_group(entities, str_keys):
                missing.append("|".join(str_keys))

    return missing


def build_one_question(intent: str, entities: dict[str, Any]) -> str | None:
    spec = get_intent_spec(intent) or {}

    rules = spec.get("question_priority", [])
    if isinstance(rules, list):
        for rule in rules:
            if not isinstance(rule, dict):
                continue
            missing_expr = rule.get("if_missing")
            ask = rule.get("ask")
            if not isinstance(missing_expr, str) or not isinstance(ask, str):
                continue
            if not _has_value(entities, missing_expr):
                return ask

    q = spec.get("question_on_missing")
    if isinstance(q, str) and q.strip():
        return q.strip()
    return None


def get_disambiguation_top_k() -> int:
    common = get_canon().get("common", {})
    if not isinstance(common, dict):
        return 5
    dis = common.get("disambiguation", {})
    if not isinstance(dis, dict):
        return 5
    try:
        return int(dis.get("top_k", 5))
    except Exception:
        return 5


def get_disambiguation_default_question() -> str:
    common = get_canon().get("common", {})
    if not isinstance(common, dict):
        return "Уточните, что именно вы имеете в виду?"
    dis = common.get("disambiguation", {})
    if not isinstance(dis, dict):
        return "Уточните, что именно вы имеете в виду?"
    q = dis.get("default_question")
    return q if isinstance(q, str) and q.strip() else "Уточните, что именно вы имеете в виду?"

