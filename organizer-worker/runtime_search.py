import logging
import re
from datetime import datetime, timedelta
from typing import Any, Callable


def _runtime_candidate_signature(candidate: dict[str, Any]) -> str:
    title = str(candidate.get("title") or "").strip().lower()
    start_at = str(candidate.get("start_at") or "").strip()
    duration = str(candidate.get("duration_minutes") or "").strip()
    return f"{title}|{start_at}|{duration}"


def _runtime_meeting_source_for_user(
    user_id: str,
    *,
    trace_list_rows_for_user: Callable[..., list[tuple[str, dict[str, Any]]]],
    conflict_snapshot_from_payload: Callable[[str, dict[str, Any]], dict[str, Any]],
    calendar_get_event: Callable[[str], dict[str, Any]],
    sync_conflict_create_or_reopen: Callable[..., dict[str, Any]],
    parse_iso_datetime_to_local: Callable[[Any], datetime | None],
    default_duration_min: int,
    timezone_name: str,
) -> dict[str, Any]:
    uid = str(user_id or "").strip()
    if not uid:
        return {"ok": False, "error": "user_id_required"}
    for trace_id, payload in trace_list_rows_for_user(uid, limit=200):
        snapshot = conflict_snapshot_from_payload(trace_id, payload)
        event_id = str(snapshot.get("calendar_event_id") or "").strip()
        if not event_id:
            continue
        event = calendar_get_event(event_id)
        if not bool(event.get("ok")):
            if int(event.get("http_status") or 0) == 404:
                conflict = sync_conflict_create_or_reopen(
                    user_id=uid,
                    source="runtime_trace_dedup",
                    entity_ref=trace_id,
                    calendar_event_id=event_id,
                    payload=snapshot,
                )
                return {"ok": False, "reason": "sync_conflict", "conflict": conflict}
            return {"ok": False, "error": "calendar_get_failed", "calendar_event_id": event_id}
        start_dt = parse_iso_datetime_to_local(event.get("event_start"))
        end_dt = parse_iso_datetime_to_local(event.get("event_end"))
        if start_dt is None:
            return {"ok": False, "error": "invalid_event_start", "calendar_event_id": event_id}
        if end_dt is None or end_dt <= start_dt:
            end_dt = start_dt + timedelta(minutes=default_duration_min)
        duration = int(max(1, int((end_dt - start_dt).total_seconds() // 60)))
        return {
            "ok": True,
            "calendar_event_id": event_id,
            "start_at": start_dt.isoformat(),
            "start_at_date": start_dt.date().isoformat(),
            "start_at_time": start_dt.strftime("%H:%M"),
            "duration_minutes": duration,
            "timezone": str(event.get("time_zone") or timezone_name),
            "user_id": uid,
        }
    return {"ok": False, "error": "not_found"}


def _runtime_meeting_search_for_user(
    user_id: str,
    target_hint: str = "",
    meeting_kind: str = "",
    *,
    normalize_meeting_kind: Callable[[Any], str],
    extract_meeting_kind_from_text: Callable[[Any], str],
    trace_list_rows_for_user: Callable[..., list[tuple[str, dict[str, Any]]]],
    conflict_snapshot_from_payload: Callable[[str, dict[str, Any]], dict[str, Any]],
    calendar_get_event: Callable[[str], dict[str, Any]],
    sync_conflict_create_or_reopen: Callable[..., dict[str, Any]],
    parse_iso_datetime_to_local: Callable[[Any], datetime | None],
    runtime_candidate_signature: Callable[[dict[str, Any]], str],
    default_duration_min: int,
    meeting_kind_default: str,
) -> dict[str, Any]:
    uid = str(user_id or "").strip()
    hint = str(target_hint or "").strip().lower()
    requested_kind = normalize_meeting_kind(meeting_kind) or extract_meeting_kind_from_text(hint)
    shortlist_limit = 3
    if not uid:
        return {"ok": False, "reason": "user_id_required", "candidates": []}

    def _normalize_query(raw: str) -> tuple[str, list[str], list[str], list[str], str]:
        q = str(raw or "").strip().lower().replace("ё", "е")
        q = re.sub(r"[^\w\s:.-]", " ", q)
        q = re.sub(r"\s+", " ", q).strip()
        times: list[str] = []
        for m in re.finditer(r"\b([01]?\d|2[0-3])(?::(\d{2}))?\b", q):
            hh = int(m.group(1))
            mm = int(m.group(2) or "0")
            if 0 <= hh <= 23 and 0 <= mm <= 59:
                times.append(f"{hh:02d}:{mm:02d}")
        tokens: list[str] = []
        for tok in q.split():
            if tok in {
                "в", "на", "с", "по", "час", "часа", "часов",
                "встреча", "встречу", "созвон", "собрание", "мероприятие",
                "измени", "перенеси", "сдвинь", "обнови", "поменяй", "нужно", "надо",
            }:
                continue
            if re.fullmatch(r"\d{1,2}(:\d{2})?", tok):
                continue
            if len(tok) < 2:
                continue
            tokens.append(tok)
        date_hints: list[str] = []
        for m in re.finditer(r"\b(\d{4})-(\d{2})-(\d{2})\b", q):
            date_hints.append(f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}")
        for m in re.finditer(r"\b(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?\b", q):
            day = int(m.group(1))
            month = int(m.group(2))
            year_raw = m.group(3)
            if not (1 <= day <= 31 and 1 <= month <= 12):
                continue
            year = int(year_raw) + 2000 if year_raw and int(year_raw) < 100 else int(year_raw or datetime.now().year)
            date_hints.append(f"{year:04d}-{month:02d}-{day:02d}")
        today = datetime.now().date()
        if "сегодня" in q:
            date_hints.append(today.isoformat())
        if "завтра" in q:
            date_hints.append((today + timedelta(days=1)).isoformat())
        if "послезавтра" in q:
            date_hints.append((today + timedelta(days=2)).isoformat())
        participant_hint = ""
        m_participant = re.search(r"\bс\s+([a-zа-я0-9._-]{2,})\b", q)
        if m_participant:
            participant_hint = str(m_participant.group(1) or "").strip()
        return q, times, tokens, date_hints, participant_hint

    normalized_query, query_times, query_tokens, query_dates, participant_hint = _normalize_query(hint)
    logging.info("meeting_search_query", extra={"user_id": uid, "raw_query": hint, "normalized_query": normalized_query, "query_times": query_times, "query_tokens": query_tokens, "query_dates": query_dates, "participant_hint": participant_hint, "meeting_kind": requested_kind})

    candidates: list[dict[str, Any]] = []
    matching_conflicts: list[dict[str, Any]] = []
    has_any_trace = False
    for trace_id, payload in trace_list_rows_for_user(uid, limit=200):
        snapshot = conflict_snapshot_from_payload(trace_id, payload)
        event_id = str(snapshot.get("calendar_event_id") or "").strip()
        if not event_id:
            continue
        has_any_trace = True
        event = calendar_get_event(event_id)
        if not bool(event.get("ok")):
            if int(event.get("http_status") or 0) == 404:
                conflict = sync_conflict_create_or_reopen(user_id=uid, source="runtime_trace_dedup", entity_ref=trace_id, calendar_event_id=event_id, payload=snapshot)
                snapshot_blob = " ".join([str(conflict.get("title") or ""), str(conflict.get("start_at_date") or ""), str(conflict.get("start_at_time") or ""), str(conflict.get("comment_text") or "")]).lower()
                target_tokens = [tok for tok in query_tokens if tok]
                matches_conflict = (not hint or (participant_hint and participant_hint in snapshot_blob) or any(tok in snapshot_blob for tok in target_tokens) or any(dt in snapshot_blob for dt in query_dates) or any(tm in snapshot_blob for tm in query_times))
                if matches_conflict:
                    matching_conflicts.append(conflict)
            continue
        start_dt = parse_iso_datetime_to_local(event.get("event_start"))
        end_dt = parse_iso_datetime_to_local(event.get("event_end"))
        if start_dt is None:
            continue
        if end_dt is None or end_dt <= start_dt:
            end_dt = start_dt + timedelta(minutes=default_duration_min)
        duration = int(max(1, int((end_dt - start_dt).total_seconds() // 60)))
        title = str(event.get("title") or "Встреча").strip() or "Встреча"
        description = str(event.get("description") or "").strip()
        event_kind = extract_meeting_kind_from_text(title) or meeting_kind_default
        candidates.append({"calendar_event_id": event_id, "title": title, "description": description, "meeting_kind": event_kind, "start_at": start_dt.isoformat(), "start_at_date": start_dt.date().isoformat(), "start_at_time": start_dt.strftime("%H:%M"), "duration_minutes": duration})

    unique_candidates: list[dict[str, Any]] = []
    unique_signatures: set[str] = set()
    collapsed_duplicates = 0
    for cand in candidates:
        signature = runtime_candidate_signature(cand)
        if signature in unique_signatures:
            collapsed_duplicates += 1
            continue
        unique_signatures.add(signature)
        unique_candidates.append(cand)
    candidates = unique_candidates
    if requested_kind:
        kind_candidates = [cand for cand in candidates if normalize_meeting_kind(cand.get("meeting_kind")) == requested_kind]
        if kind_candidates:
            candidates = kind_candidates
    if hint:
        ranked: list[tuple[tuple[int, int, int, int, float], dict[str, Any]]] = []
        match_reasons: list[str] = []
        has_specific_hint = bool(participant_hint or query_tokens or query_times or query_dates)
        now_ts = datetime.now().timestamp()
        for cand in candidates:
            title = str(cand.get("title") or "").lower()
            description = str(cand.get("description") or "").lower()
            cand_time = str(cand.get("start_at_time") or "")
            cand_date = str(cand.get("start_at_date") or "")
            kind_match = 1 if (requested_kind and normalize_meeting_kind(cand.get("meeting_kind")) == requested_kind) else 0
            exact_participant_or_title = 3 if ((participant_hint and participant_hint in title) or (normalized_query and normalized_query in title)) else 2 if ((participant_hint and participant_hint in description) or (normalized_query and normalized_query in description)) else 0
            time_match = 1 if (query_times and cand_time in query_times) else 0
            date_match = 1 if (query_dates and cand_date in query_dates) else 0
            temporal_match = 2 if (time_match and date_match) else (1 if (time_match or date_match) else 0)
            token_overlap = sum(1 for tok in query_tokens if tok in title or tok in description) if query_tokens else 0
            matched = bool(exact_participant_or_title or token_overlap or temporal_match) if has_specific_hint else bool(kind_match or exact_participant_or_title or temporal_match or token_overlap)
            if not matched:
                continue
            start_dt = parse_iso_datetime_to_local(cand.get("start_at"))
            recency = -abs((start_dt.timestamp() if start_dt else now_ts) - now_ts)
            rank_key = (exact_participant_or_title, kind_match, temporal_match, token_overlap, recency)
            ranked.append((rank_key, cand))
            reason_parts: list[str] = []
            if exact_participant_or_title:
                reason_parts.append("participant_or_title")
            if kind_match:
                reason_parts.append("meeting_kind")
            if temporal_match:
                reason_parts.append("temporal")
            if token_overlap:
                reason_parts.append("token_overlap")
            match_reasons.append(",".join(reason_parts) if reason_parts else "matched")
        ranked.sort(key=lambda item: item[0], reverse=True)
        candidates = [dict(item[1]) for item in ranked]
        logging.info("meeting_search_match_reason", extra={"user_id": uid, "query": normalized_query, "reasons": match_reasons[:10]})
    logging.info("meeting_search_candidate_count", extra={"user_id": uid, "count": len(candidates), "collapsed_duplicates": collapsed_duplicates})
    logging.info("meeting_search_candidate_titles", extra={"user_id": uid, "titles": [str(c.get('title') or '') for c in candidates[:10]]})
    if not candidates:
        if matching_conflicts:
            return {"ok": False, "reason": "sync_conflict", "conflict": matching_conflicts[0], "candidates": []}
        return {"ok": False, "reason": "not_found", "candidates": []}
    if len(candidates) > 1:
        return {"ok": False, "reason": "multiple", "candidates": candidates[:shortlist_limit]}
    candidate = dict(candidates[0])
    candidate.pop("description", None)
    return {"ok": True, "candidate": candidate, "candidates": [candidate]}


def _runtime_task_search_for_user(user_id: str, target_hint: str = "", *, get_conn: Callable[[], Any], as_dict: Callable[[Any], dict[str, Any]]) -> dict[str, Any]:
    uid = str(user_id or "").strip()
    hint = str(target_hint or "").strip()
    if not uid:
        return {"ok": False, "reason": "user_id_required", "candidates": []}
    if not hint:
        return {"ok": False, "reason": "target_hint_required", "candidates": []}
    hint_norm = hint.lower()
    task_id_exact: int | None = None
    if re.fullmatch(r"\d+", hint_norm):
        try:
            task_id_exact = int(hint_norm)
        except Exception:
            task_id_exact = None
    with get_conn() as conn:
        has_comment = False
        try:
            cols = conn.execute("PRAGMA table_info(tasks)").fetchall()
            has_comment = any(str(as_dict(r).get("name") or "") == "comment" for r in cols)
        except Exception:
            has_comment = False
        query = """SELECT id, title, status, state, planned_at, comment FROM tasks WHERE CAST(id AS TEXT) = :hint OR LOWER(title) LIKE :like OR LOWER(COALESCE(comment, '')) LIKE :like ORDER BY CASE WHEN CAST(id AS TEXT) = :hint THEN 0 ELSE 1 END, id DESC LIMIT 10""" if has_comment else """SELECT id, title, status, state, planned_at, '' AS comment FROM tasks WHERE CAST(id AS TEXT) = :hint OR LOWER(title) LIKE :like ORDER BY CASE WHEN CAST(id AS TEXT) = :hint THEN 0 ELSE 1 END, id DESC LIMIT 10"""
        rows = conn.execute(query, {"hint": hint_norm, "like": f"%{hint_norm}%"}).fetchall()
    candidates: list[dict[str, Any]] = []
    for row in rows:
        item = as_dict(row)
        candidates.append({"task_id": int(item.get("id")), "title": str(item.get("title") or "").strip(), "status": str(item.get("status") or item.get("state") or "").strip(), "planned_at": str(item.get("planned_at") or "").strip(), "comment": str(item.get("comment") or "").strip()})
    if task_id_exact is not None:
        candidates = [c for c in candidates if int(c.get("task_id") or 0) == task_id_exact] or candidates
    if not candidates:
        return {"ok": False, "reason": "not_found", "candidates": []}
    if len(candidates) > 1:
        return {"ok": False, "reason": "multiple", "candidates": candidates[:5]}
    return {"ok": True, "candidate": dict(candidates[0]), "candidates": [dict(candidates[0])]}


def _runtime_timeblock_search_for_user(user_id: str, target_hint: str = "", *, get_conn: Callable[[], Any], as_dict: Callable[[Any], dict[str, Any]]) -> dict[str, Any]:
    uid = str(user_id or "").strip()
    hint = str(target_hint or "").strip().lower().replace("ё", "е")
    if not uid:
        return {"ok": False, "reason": "user_id_required", "candidates": []}
    if not hint:
        return {"ok": False, "reason": "target_hint_required", "candidates": []}
    with get_conn() as conn:
        cols = conn.execute("PRAGMA table_info(time_blocks)").fetchall()
        has_comment = any(str(as_dict(r).get("name") or "") == "comment" for r in cols)
        select_comment = "tb.comment" if has_comment else "'' AS comment"
        rows = conn.execute(
            f"""
            SELECT tb.id, tb.task_id, tb.start_at, tb.end_at, {select_comment},
                   t.title AS task_title
            FROM time_blocks tb
            LEFT JOIN tasks t ON t.id = tb.task_id
            WHERE (tb.user_id = :uid OR :uid = '')
            ORDER BY tb.start_at DESC, tb.id DESC
            LIMIT 50
            """,
            {"uid": uid},
        ).fetchall()
    candidates: list[dict[str, Any]] = []
    exact_id = int(hint) if re.fullmatch(r"\d+", hint) else None
    time_tokens = {f"{int(m.group(1)):02d}:{int(m.group(2) or '0'):02d}" for m in re.finditer(r"\b([01]?\d|2[0-3])(?::(\d{2}))?\b", hint)}
    date_tokens = set(re.findall(r"\d{4}-\d{2}-\d{2}", hint))
    text_tokens = [tok for tok in re.split(r"\s+", re.sub(r"[^\w\s:.-]", " ", hint)) if tok and len(tok) >= 2]
    for row in rows:
        item = as_dict(row)
        block_id = int(item.get("id") or 0)
        start_at = str(item.get("start_at") or "").strip()
        end_at = str(item.get("end_at") or "").strip()
        start_date = start_at[:10] if len(start_at) >= 10 else ""
        start_time = start_at[11:16] if len(start_at) >= 16 else ""
        duration_minutes = 0
        try:
            start_dt = datetime.fromisoformat(start_at.replace("Z", "+00:00"))
            end_dt = datetime.fromisoformat(end_at.replace("Z", "+00:00"))
            duration_minutes = int(max(1, int((end_dt - start_dt).total_seconds() // 60)))
        except Exception:
            duration_minutes = 0
        title = str(item.get("task_title") or "Блок времени").strip() or "Блок времени"
        comment = str(item.get("comment") or "").strip()
        haystack = " ".join([str(block_id), title.lower(), comment.lower(), start_date, start_time]).lower()
        matched = False
        if exact_id is not None and block_id == exact_id:
            matched = True
        elif any(token in haystack for token in date_tokens | time_tokens):
            matched = True
        elif text_tokens and all(tok in haystack for tok in text_tokens if tok not in {"измени", "перенеси", "исправь", "обнови", "блок", "времени", "таймблок"}):
            matched = True
        if matched:
            candidates.append(
                {
                    "time_block_id": block_id,
                    "task_id": int(item.get("task_id") or 0) if item.get("task_id") is not None else None,
                    "title": title,
                    "comment": comment,
                    "start_at": start_at,
                    "start_at_date": start_date,
                    "start_at_time": start_time,
                    "duration_minutes": duration_minutes,
                }
            )
    if exact_id is not None:
        candidates = [c for c in candidates if int(c.get("time_block_id") or 0) == exact_id] or candidates
    if not candidates:
        return {"ok": False, "reason": "not_found", "candidates": []}
    if len(candidates) > 1:
        return {"ok": False, "reason": "multiple", "candidates": candidates[:3]}
    return {"ok": True, "candidate": dict(candidates[0]), "candidates": [dict(candidates[0])]}
