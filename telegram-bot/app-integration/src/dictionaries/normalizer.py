from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple


def _norm_space(s: str) -> str:
    return " ".join((s or "").strip().split())


def _lower(s: str) -> str:
    return (s or "").lower()


@dataclass(frozen=True)
class Dictionaries:
    global_dict: Dict[str, Any]
    app_dict: Dict[str, Any]

    def merged(self) -> Dict[str, Any]:
        # APP overrides GLOBAL on collisions.
        g = self.global_dict if isinstance(self.global_dict, dict) else {}
        a = self.app_dict if isinstance(self.app_dict, dict) else {}
        out: Dict[str, Any] = dict(g)
        for k, v in a.items():
            out[k] = v
        return out


def _as_map(d: Dict[str, Any], key: str) -> Dict[str, Any]:
    v = d.get(key)
    return v if isinstance(v, dict) else {}


def normalize_text(text: str, merged_dict: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """
    Deterministic normalization:
    1) replacements (token-ish)
    2) phrase synonyms (longest-first)
    """
    t0 = _norm_space(text)
    rep = _as_map(merged_dict, "replacements")
    syn = _as_map(merged_dict, "synonyms")

    # 1) replacements: simple substring replace on lowercased text, applied in key length order.
    t = t0
    dbg: Dict[str, Any] = {"replacements": [], "synonyms": []}
    for k in sorted([str(x) for x in rep.keys()], key=lambda s: len(s), reverse=True):
        v = str(rep.get(k) or "")
        if not k or not v:
            continue
        if _lower(k) in _lower(t):
            t = _lower(t).replace(_lower(k), v)
            dbg["replacements"].append({"from": k, "to": v})

    t = _norm_space(t)

    # 2) synonyms: canonical phrase -> list of variants.
    # Apply longest variant first to keep deterministic behavior.
    variants: List[Tuple[str, str]] = []
    for canon, vs in syn.items():
        canon_s = _norm_space(str(canon))
        if not canon_s:
            continue
        if isinstance(vs, list):
            for v in vs:
                vv = _norm_space(str(v))
                if vv:
                    variants.append((vv, canon_s))

    variants.sort(key=lambda p: len(p[0]), reverse=True)
    low = _lower(t)
    for frm, to in variants:
        f = _lower(frm)
        if f and f in low:
            low = low.replace(f, to)
            dbg["synonyms"].append({"from": frm, "to": to})
    out = _norm_space(low)
    return out, dbg
