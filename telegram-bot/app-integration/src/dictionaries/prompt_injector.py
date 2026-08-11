from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


def _norm(s: str) -> str:
    return " ".join((s or "").strip().split())


def select_relevant_dict_lines(merged_dict: Dict[str, Any], text: str, *, max_lines: int = 20) -> List[str]:
    """
    Deterministic, short "dictionary block" selection based on keyword match.
    """
    t = _norm(text).lower()
    lines: List[str] = []

    rep = merged_dict.get("replacements") if isinstance(merged_dict.get("replacements"), dict) else {}
    terms = merged_dict.get("terms") if isinstance(merged_dict.get("terms"), dict) else {}
    hints = merged_dict.get("intent_hints") if isinstance(merged_dict.get("intent_hints"), dict) else {}

    scored: List[Tuple[int, str]] = []

    for k, v in sorted(rep.items(), key=lambda kv: str(kv[0])):
        kk = _norm(str(k)).lower()
        if kk and kk in t:
            scored.append((100, f"replace: {k} -> {v}"))

    for k, v in sorted(terms.items(), key=lambda kv: str(kv[0])):
        kk = _norm(str(k)).lower()
        if kk and kk in t:
            scored.append((90, f"term: {k} = {v}"))

    for intent, spec in sorted(hints.items(), key=lambda kv: str(kv[0])):
        if not isinstance(spec, dict):
            continue
        kws = spec.get("keywords") if isinstance(spec.get("keywords"), list) else []
        hit = 0
        for kw in kws:
            k = _norm(str(kw)).lower()
            if k and k in t:
                hit += 1
        if hit:
            scored.append((80 + hit, f"intent_hint: {intent} keywords={kws}"))

    scored.sort(key=lambda p: (p[0], p[1]), reverse=True)
    for _, line in scored[: max(1, int(max_lines))]:
        lines.append(line)
    return lines


def build_dictionary_block(
    lines: Optional[List[str]] = None,
    *,
    text: str = "",
    merged_dict: Optional[Dict[str, Any]] = None,
    max_lines: int = 20,
) -> str:
    """
    Backward compatible:
    - build_dictionary_block(lines=[...])
    - build_dictionary_block(text="...", merged_dict={...}, max_lines=20)

    `max_lines` limits the total non-empty lines in the returned block, including the header.
    """
    max_total = max(0, int(max_lines))
    if max_total <= 1:
        return ""

    if lines is None and merged_dict is not None:
        # Reserve 1 line for the header.
        max_items = max_total - 1
        lines = select_relevant_dict_lines(merged_dict, text, max_lines=max_items)

    lines = lines or []
    if not lines:
        return ""

    # Keep short and deterministic.
    return "DICTIONARY:\n" + "\n".join(f"- {l}" for l in lines[: max_total - 1])
