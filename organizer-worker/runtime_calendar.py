import importlib
import logging
import os
import socket
from datetime import datetime, timedelta
from typing import Any


def _get_calendar_service(*, deps: dict[str, Any]):
    deps["set_calendar_not_configured_reason"](None)
    if not deps["google_service_account_file"]:
        logging.warning("calendar_not_configured: missing_file")
        deps["set_calendar_not_configured_reason"]("missing_file")
        return None
    if os.path.isdir(deps["google_service_account_file"]):
        logging.warning("calendar_not_configured: file_is_directory")
        deps["set_calendar_not_configured_reason"]("file_is_directory")
        return None
    if not os.path.exists(deps["google_service_account_file"]):
        logging.warning("calendar_not_configured: missing_file")
        deps["set_calendar_not_configured_reason"]("missing_file")
        return None
    if not deps["google_calendar_id"]:
        logging.warning("calendar_not_configured: missing_calendar_id")
        deps["set_calendar_not_configured_reason"]("missing_calendar_id")
        return None
    try:
        creds_mod = importlib.import_module("google.oauth2.service_account")
        discovery_mod = importlib.import_module("googleapiclient.discovery")
        Credentials = getattr(creds_mod, "Credentials", None)
        build = getattr(discovery_mod, "build", None)
        if Credentials is None or build is None:
            logging.error(
                "calendar_config_error: google api deps missing or broken; "
                "install google api deps (google-auth, google-auth-oauthlib, "
                "google-api-python-client, googleapis-common-protos, httplib2)"
            )
            deps["set_calendar_not_configured_reason"]("config_error_missing_google_deps")
            return None
    except Exception as exc:
        logging.error(
            "calendar_config_error: google api deps import failed err=%s; "
            "install google api deps (google-auth, google-auth-oauthlib, "
            "google-api-python-client, googleapis-common-protos, httplib2)",
            str(exc)[:200],
        )
        deps["set_calendar_not_configured_reason"]("config_error_missing_google_deps")
        return None

    creds = Credentials.from_service_account_file(
        deps["google_service_account_file"],
        scopes=["https://www.googleapis.com/auth/calendar"],
    )
    service = build("calendar", "v3", credentials=creds, cache_discovery=False)
    if deps["calendar_debug"]:
        try:
            data = service.calendarList().list().execute()
            for cal in (data.get("items") or []):
                logging.info(
                    "calendar_list id=%s summary=%s",
                    cal.get("id"),
                    cal.get("summary"),
                )
        except Exception as exc:
            logging.warning("calendar_list failed err=%s", str(exc)[:200])
    return service


def _calendar_normalize_local_dt(value: datetime, *, deps: dict[str, Any]) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError("calendar datetime is required")
    local_tz = deps["local_tz"]()
    if value.tzinfo is None:
        dt_local = value.replace(tzinfo=local_tz)
    else:
        dt_local = value.astimezone(local_tz)
    return dt_local.replace(microsecond=0)


def _calendar_datetime_payload(start: datetime, end: datetime, *, deps: dict[str, Any]) -> tuple[dict[str, str], dict[str, str]]:
    start_local = _calendar_normalize_local_dt(start, deps=deps)
    end_local = _calendar_normalize_local_dt(end, deps=deps)
    return (
        {"dateTime": start_local.isoformat(), "timeZone": deps["timezone_name"]},
        {"dateTime": end_local.isoformat(), "timeZone": deps["timezone_name"]},
    )


def _create_event(
    item_id: int | str,
    title: str,
    start: datetime,
    end: datetime,
    description: str | None = None,
    existing_event_id: str | None = None,
    flow_id: str | None = None,
    *,
    deps: dict[str, Any],
) -> str | None:
    service = deps["get_calendar_service"]()
    if service is None:
        logging.warning("calendar service not configured; skipping")
        return None
    if existing_event_id and existing_event_id not in {"PENDING", "FAILED"}:
        patch_res = deps["patch_event"](str(existing_event_id), start, end)
        if patch_res == "ok":
            logging.info(
                "calendar_idempotent item_id=%s iCalUID=%s action=update google_event_id=%s",
                item_id,
                deps["build_item_ical_uid"](item_id),
                existing_event_id,
            )
            return str(existing_event_id)
        if patch_res in {"error", "no_service"}:
            return None

    start_payload, end_payload = _calendar_datetime_payload(start, end, deps=deps)
    event = {
        "summary": title,
        "start": start_payload,
        "end": end_payload,
    }
    logging.info(
        "calendar_payload_start=%s calendar_payload_end=%s timezone=%s",
        start_payload.get("dateTime"),
        end_payload.get("dateTime"),
        deps["timezone_name"],
    )
    if flow_id:
        logging.info(
            "calendar_adapter_prepare flow_id=%s item_id=%s start=%s end=%s timezone=%s payload=%s",
            flow_id,
            item_id,
            start.isoformat(),
            end.isoformat(),
            deps["timezone_name"],
            event,
        )
    if description:
        event["description"] = description
    ical_uid = deps["build_item_ical_uid"](item_id)
    event_id, action = deps["create_or_reuse_event"](
        service,
        calendar_id=deps["google_calendar_id"],
        item_id=item_id,
        event=event,
        flow_id=flow_id,
    )
    if event_id:
        logging.info(
            "calendar_idempotent item_id=%s iCalUID=%s action=%s google_event_id=%s",
            item_id,
            ical_uid,
            action,
            event_id,
        )
    return event_id


def _patch_event(event_id: str, start: datetime, end: datetime, *, deps: dict[str, Any]) -> str:
    service = deps["get_calendar_service"]()
    if service is None:
        return "no_service"
    start_payload, end_payload = _calendar_datetime_payload(start, end, deps=deps)
    body = {
        "start": start_payload,
        "end": end_payload,
    }
    try:
        service.events().patch(calendarId=deps["google_calendar_id"], eventId=event_id, body=body).execute()
        return "ok"
    except Exception as exc:
        try:
            err_mod = importlib.import_module("googleapiclient.errors")
            HttpError = getattr(err_mod, "HttpError", None)
        except Exception:
            HttpError = None
        if HttpError is not None and isinstance(exc, HttpError):
            status = getattr(getattr(exc, "resp", None), "status", None)
            if status == 404:
                return "not_found"
            return "error"
        return "error"


def _patch_event_description(event_id: str, description: str, *, deps: dict[str, Any]) -> str:
    service = deps["get_calendar_service"]()
    if service is None:
        return "no_service"
    body = {"description": str(description or "")}
    try:
        service.events().patch(calendarId=deps["google_calendar_id"], eventId=event_id, body=body).execute()
        return "ok"
    except Exception as exc:
        try:
            err_mod = importlib.import_module("googleapiclient.errors")
            HttpError = getattr(err_mod, "HttpError", None)
        except Exception:
            HttpError = None
        if HttpError is not None and isinstance(exc, HttpError):
            status = getattr(getattr(exc, "resp", None), "status", None)
            if status == 404:
                return "not_found"
            return "error"
        return "error"


def _calendar_http_status(exc: Exception) -> int | None:
    resp = getattr(exc, "resp", None)
    status = getattr(resp, "status", None) if resp is not None else None
    try:
        return int(status) if status is not None else None
    except Exception:
        return None


def _calendar_result(
    ok: bool,
    event_id: str | None,
    http_status: int | None,
    err: str | None,
    etag: str | None,
) -> dict:
    return {
        "ok": bool(ok),
        "event_id": event_id,
        "http_status": http_status,
        "err": err,
        "etag": etag,
    }


def _calendar_create_event(
    item_id: int | str,
    title: str,
    start: datetime,
    end: datetime,
    description: str | None = None,
    *,
    deps: dict[str, Any],
) -> dict:
    try:
        event_id = _create_event(item_id, title, start, end, description=description, deps=deps)
    except Exception as exc:
        status = _calendar_http_status(exc)
        return _calendar_result(False, None, status, f"exception:{type(exc).__name__}", None)
    if not event_id:
        return _calendar_result(False, None, None, "no_event_id", None)
    return _calendar_result(True, str(event_id), None, None, None)


def _calendar_patch_event(event_id: str, start: datetime, end: datetime, *, deps: dict[str, Any]) -> dict:
    try:
        patch_fn = deps.get("patch_event")
        if callable(patch_fn):
            res = patch_fn(event_id, start, end)
        else:
            res = _patch_event(event_id, start, end, deps=deps)
    except Exception as exc:
        status = _calendar_http_status(exc)
        return _calendar_result(False, None, status, f"exception:{type(exc).__name__}", None)
    if res == "ok":
        return _calendar_result(True, event_id, None, None, None)
    if res == "not_found":
        return _calendar_result(False, None, 404, "not_found", None)
    if res == "no_service":
        return _calendar_result(False, None, None, "no_service", None)
    return _calendar_result(False, None, None, "error", None)


def _calendar_patch_event_description(event_id: str, description: str, *, deps: dict[str, Any]) -> dict:
    try:
        patch_description_fn = deps.get("patch_event_description")
        if callable(patch_description_fn):
            res = patch_description_fn(event_id, description)
        else:
            res = _patch_event_description(event_id, description, deps=deps)
    except Exception as exc:
        status = _calendar_http_status(exc)
        return _calendar_result(False, None, status, f"exception:{type(exc).__name__}", None)
    if res == "ok":
        return _calendar_result(True, event_id, None, None, None)
    if res == "not_found":
        return _calendar_result(False, None, 404, "not_found", None)
    if res == "no_service":
        return _calendar_result(False, None, None, "no_service", None)
    return _calendar_result(False, None, None, "error", None)


def _calendar_delete_event(event_id: str, *, deps: dict[str, Any]) -> dict:
    service = deps["get_calendar_service"]()
    if service is None:
        return _calendar_result(False, None, None, "no_service", None)
    try:
        service.events().delete(calendarId=deps["google_calendar_id"], eventId=event_id).execute()
        return _calendar_result(True, None, 204, None, None)
    except Exception as exc:
        status = _calendar_http_status(exc)
        if status == 404:
            return _calendar_result(True, None, 404, "not_found", None)
        return _calendar_result(False, None, status, f"exception:{type(exc).__name__}", None)


def _calendar_get_event(event_id: str, *, deps: dict[str, Any]) -> dict:
    service = deps["get_calendar_service"]()
    if service is None:
        return _calendar_result(False, None, None, "no_service", None)
    try:
        event = service.events().get(calendarId=deps["google_calendar_id"], eventId=event_id).execute()
        start = (event or {}).get("start") or {}
        end = (event or {}).get("end") or {}
        start_dt = start.get("dateTime") or start.get("date")
        end_dt = end.get("dateTime") or end.get("date")
        timezone_name = start.get("timeZone") or end.get("timeZone") or deps["timezone_name"]
        summary = str((event or {}).get("summary") or "").strip()
        description = str((event or {}).get("description") or "").strip()
        return {
            "ok": True,
            "event_id": event_id,
            "http_status": None,
            "err": None,
            "event_start": start_dt,
            "event_end": end_dt,
            "time_zone": timezone_name,
            "title": summary,
            "description": description,
        }
    except Exception as exc:
        status = _calendar_http_status(exc)
        if status == 404:
            return {
                "ok": False,
                "event_id": None,
                "http_status": 404,
                "err": "not_found",
                "event_start": None,
                "event_end": None,
                "time_zone": None,
            }
        return {
            "ok": False,
            "event_id": None,
            "http_status": status,
            "err": f"exception:{type(exc).__name__}",
            "event_start": None,
            "event_end": None,
            "time_zone": None,
        }


def _runtime_temporal_window_local(
    entities: dict[str, Any],
    *,
    fallback_minutes: int,
    deps: dict[str, Any],
) -> tuple[datetime | None, datetime | None, int]:
    duration = deps["runtime_duration_minutes"](entities) or int(max(1, fallback_minutes))
    local_tz = deps["local_tz"]()
    start_raw = str(entities.get("start_at") or "").strip()
    if start_raw:
        start_dt = deps["parse_iso_datetime_to_local"](start_raw)
        if start_dt is not None:
            has_explicit_time = bool(deps["re"].search(r"[T\s]\d{1,2}:\d{2}", start_raw))
            has_split_time = bool(str(entities.get("start_at_time") or entities.get("time") or "").strip())
            if not has_explicit_time and has_split_time:
                start_dt = None
            else:
                if start_dt.tzinfo is None:
                    start_dt = start_dt.replace(tzinfo=local_tz)
                else:
                    start_dt = start_dt.astimezone(local_tz)
                end_dt = start_dt + timedelta(minutes=duration)
                return start_dt, end_dt, duration

    date_raw = str(entities.get("start_at_date") or entities.get("date") or "").strip()
    time_raw = str(entities.get("start_at_time") or entities.get("time") or "").strip()
    if not (date_raw and time_raw):
        return None, None, duration
    date_norm = date_raw.replace("/", "-")
    if deps["re"].fullmatch(r"\d{1,2}\.\d{1,2}\.\d{4}", date_raw):
        day, month, year = [int(x) for x in date_raw.split(".")]
        date_norm = f"{year:04d}-{month:02d}-{day:02d}"
    mt = deps["re"].fullmatch(r"(\d{1,2})(?::(\d{2}))?", time_raw)
    if not mt:
        return None, None, duration
    hour = int(mt.group(1))
    minute = int(mt.group(2) or "0")
    if hour > 23 or minute > 59:
        return None, None, duration
    try:
        parsed_date = datetime.fromisoformat(date_norm)
    except Exception:
        return None, None, duration
    start_dt = datetime(parsed_date.year, parsed_date.month, parsed_date.day, hour, minute, tzinfo=local_tz)
    end_dt = start_dt + timedelta(minutes=duration)
    return start_dt, end_dt, duration


def _runtime_commit_temporal_calendar(
    *,
    flow_id: str,
    intent: str,
    entities: dict[str, Any],
    execution_result: dict[str, Any],
    source_timezone: str,
    deps: dict[str, Any],
) -> dict[str, Any]:
    out = dict(execution_result if isinstance(execution_result, dict) else {})
    payload = entities if isinstance(entities, dict) else {}
    user_id = str(payload.get("user_id") or "").strip()
    start_local, end_local, duration = _runtime_temporal_window_local(
        payload,
        fallback_minutes=deps["default_duration_min"],
        deps=deps,
    )
    if start_local is None or end_local is None:
        logging.warning(
            "runtime_temporal_calendar_prepare_failed flow_id=%s reason=missing_or_invalid_datetime intent=%s entities=%s",
            flow_id,
            intent,
            entities,
        )
        return out

    meeting_kind = deps["meeting_kind_from_entities"](intent, payload)
    if meeting_kind:
        entities["meeting_kind"] = meeting_kind
    title = deps["runtime_temporal_title"](intent, payload)
    description_raw = str(
        payload.get("comment_text")
        or payload.get("description")
        or payload.get("comment")
        or payload.get("notes")
        or ""
    ).strip()
    description = description_raw or None
    item_key: int | str = str(out.get("time_block_id") or "").strip() or f"flow:{flow_id}"
    payload_preview = {
        "summary": title,
        "start": {"dateTime": start_local.isoformat(), "timeZone": deps["timezone_name"]},
        "end": {"dateTime": end_local.isoformat(), "timeZone": deps["timezone_name"]},
    }
    payload_start, payload_end = _calendar_datetime_payload(start_local, end_local, deps=deps)
    logging.info(
        "meeting_create_commit_start flow_id=%s meeting_kind=%s merged_start_at=%s merged_end_at=%s payload.start=%s payload.end=%s",
        flow_id,
        meeting_kind or deps["meeting_kind_default"],
        start_local.isoformat(),
        end_local.isoformat(),
        payload_start,
        payload_end,
    )
    logging.info(
        "runtime_temporal_calendar_prepare flow_id=%s normalized_local_date=%s normalized_local_time=%s duration_minutes=%s timezone=%s requested_timezone=%s final_start=%s final_end=%s payload=%s",
        flow_id,
        start_local.date().isoformat(),
        start_local.strftime("%H:%M"),
        duration,
        deps["timezone_name"],
        source_timezone,
        start_local.isoformat(),
        end_local.isoformat(),
        payload_preview,
    )
    try:
        event_id = deps["create_event"](
            item_key,
            title,
            start_local,
            end_local,
            description=description,
            flow_id=flow_id,
        )
    except Exception as exc:
        logging.exception("runtime_temporal_calendar_commit_failed flow_id=%s err=%s", flow_id, str(exc)[:300])
        debug = out.get("debug")
        debug = dict(debug) if isinstance(debug, dict) else {}
        debug["calendar_commit"] = "error"
        debug["calendar_error"] = str(exc)[:300]
        debug["user_id"] = user_id
        out["debug"] = debug
        return out

    if event_id:
        out["calendar_event_id"] = str(event_id)
        out["user_message"] = deps["meeting_success_text"](meeting_kind, "create")
        debug = out.get("debug")
        debug = dict(debug) if isinstance(debug, dict) else {}
        debug["calendar_commit"] = "ok"
        debug["flow_id"] = flow_id
        debug["user_id"] = user_id
        out["debug"] = debug
        logging.info(
            "runtime_temporal_calendar_commit_success flow_id=%s event_id=%s created_start=%s created_end=%s timezone=%s",
            flow_id,
            str(event_id),
            start_local.isoformat(),
            end_local.isoformat(),
            deps["timezone_name"],
        )
    else:
        debug = out.get("debug")
        debug = dict(debug) if isinstance(debug, dict) else {}
        debug["calendar_commit"] = "no_event_id"
        debug["flow_id"] = flow_id
        debug["user_id"] = user_id
        out["debug"] = debug
        out["ok"] = False
        out["outcome"] = "error"
        out["error"] = str(out.get("error") or "calendar_event_id_missing")
        out["user_message"] = "Не получилось создать встречу в календаре. Попробуйте еще раз."
        logging.warning("runtime_temporal_calendar_commit_no_event_id flow_id=%s", flow_id)
    return out


def _runtime_commit_meeting_update_calendar(
    *,
    flow_id: str,
    entities: dict[str, Any],
    execution_result: dict[str, Any],
    deps: dict[str, Any],
) -> dict[str, Any]:
    out = dict(execution_result if isinstance(execution_result, dict) else {})
    payload = entities if isinstance(entities, dict) else {}
    user_id = str(payload.get("user_id") or "").strip()
    event_id = str(payload.get("calendar_event_id") or "").strip()
    sync_conflict_id = str(payload.get("sync_conflict_id") or "").strip()

    if not event_id:
        logging.warning(
            "runtime_meeting_update_commit_skipped trace_id=%s flow_id=%s reason=missing_calendar_event_id",
            flow_id,
            flow_id,
        )
        return {
            "ok": False,
            "user_message": "Не нашел встречу для переноса. Уточните, какую встречу изменить.",
            "clarifying_question": "Какую встречу перенести?",
            "debug": {"reason": "missing_calendar_event_id", "requires_target_confirmation": True},
        }

    current = deps["calendar_get_event"](event_id)
    if not bool(current.get("ok")):
        if int(current.get("http_status") or 0) == 404 and sync_conflict_id:
            logging.warning(
                "runtime_meeting_update_calendar_missing trace_id=%s flow_id=%s event_id=%s sync_conflict_id=%s",
                flow_id,
                flow_id,
                event_id,
                sync_conflict_id,
            )
            conflict = deps["runtime_sync_conflict_get"](sync_conflict_id)
            if isinstance(conflict, dict):
                restore_result = deps["runtime_trace_conflict_restore_from_snapshot"](
                    conflict,
                    override={
                        "title": str(payload.get("title") or conflict.get("title") or "Встреча").strip() or "Встреча",
                        "start_at": str(payload.get("start_at") or "").strip(),
                        "start_at_date": str(payload.get("start_at_date") or payload.get("date") or "").strip(),
                        "start_at_time": str(payload.get("start_at_time") or payload.get("time") or "").strip(),
                        "duration_minutes": deps["runtime_duration_minutes"](payload),
                        "comment_text": str(
                            payload.get("comment_text")
                            or payload.get("description")
                            or payload.get("comment")
                            or payload.get("notes")
                            or ""
                        ).strip(),
                        "intent": "meeting.update",
                        "meeting_kind": str(payload.get("meeting_kind") or conflict.get("meeting_kind") or "").strip(),
                        "user_id": user_id or str(conflict.get("user_id") or ""),
                    },
                )
                if bool(restore_result.get("ok")):
                    logging.info(
                        "runtime_meeting_update_conflict_restore_result trace_id=%s flow_id=%s sync_conflict_id=%s result=ok restored_event_id=%s",
                        flow_id,
                        flow_id,
                        sync_conflict_id,
                        str(restore_result.get("calendar_event_id") or ""),
                    )
                    out["calendar_event_id"] = str(restore_result.get("calendar_event_id") or "")
                    out["ok"] = True
                    out["user_message"] = "Встреча обновлена и восстановлена в календаре."
                    debug = out.get("debug")
                    debug = debug if isinstance(debug, dict) else {}
                    debug["calendar_commit"] = "ok"
                    debug["calendar_operation"] = "restore_from_conflict"
                    debug["flow_id"] = flow_id
                    debug["user_id"] = user_id
                    debug["sync_conflict_id"] = sync_conflict_id
                    out["debug"] = debug
                    return out
        return {
            "ok": False,
            "user_message": "Не получилось получить текущую встречу в календаре.",
            "debug": {"reason": "calendar_get_failed", "calendar_event_id": event_id},
        }
    current_start = deps["parse_iso_datetime_to_local"](current.get("event_start"))
    if current_start is None:
        return {
            "ok": False,
            "user_message": "Не получилось определить текущее время встречи.",
            "debug": {"reason": "invalid_existing_start", "calendar_event_id": event_id},
        }
    current_end = deps["parse_iso_datetime_to_local"](current.get("event_end"))
    current_duration = (
        int(max(1, int((current_end - current_start).total_seconds() // 60)))
        if current_end is not None
        else deps["default_duration_min"]
    )

    start_at_raw = str(payload.get("start_at") or "").strip()
    start_at_dt = deps["parse_iso_datetime_to_local"](start_at_raw) if start_at_raw else None
    start_at_has_time = bool(deps["re"].search(r"[T\s]\d{1,2}:\d{2}", start_at_raw))

    date_token = deps["parse_date_token_ymd"](payload.get("start_at_date") or payload.get("date"))
    time_token = deps["parse_time_token_hhmm"](payload.get("start_at_time") or payload.get("time"))

    if date_token is None and start_at_dt is not None:
        date_token = start_at_dt.date()
    if time_token is None and start_at_dt is not None and start_at_has_time:
        time_token = (start_at_dt.hour, start_at_dt.minute)

    next_date = date_token or current_start.date()
    hour = time_token[0] if time_token else current_start.hour
    minute = time_token[1] if time_token else current_start.minute
    next_start = datetime(next_date.year, next_date.month, next_date.day, hour, minute, tzinfo=deps["local_tz"]())

    next_duration = deps["runtime_duration_minutes"](payload) or current_duration
    next_end = next_start + timedelta(minutes=next_duration)
    comment_text = str(
        payload.get("comment_text")
        or payload.get("description")
        or payload.get("comment")
        or payload.get("notes")
        or ""
    ).strip()
    patch_start_payload, patch_end_payload = _calendar_datetime_payload(next_start, next_end, deps=deps)
    logging.info(
        "meeting_update_patch_start trace_id=%s flow_id=%s event_id=%s source_start=%s source_end=%s merged_start_at=%s merged_end_at=%s payload.start=%s payload.end=%s timezone=%s duration=%s",
        flow_id,
        flow_id,
        event_id,
        current_start.isoformat(),
        current_end.isoformat() if current_end is not None else "",
        next_start.isoformat(),
        next_end.isoformat(),
        patch_start_payload,
        patch_end_payload,
        str(current.get("time_zone") or deps["timezone_name"]),
        next_duration,
    )
    patch_res = deps["calendar_patch_event"](event_id, next_start, next_end)
    if bool(patch_res.get("ok")):
        if comment_text:
            patch_description_res = deps["calendar_patch_event_description"](event_id, comment_text)
            if not bool(patch_description_res.get("ok")):
                debug = out.get("debug")
                debug = debug if isinstance(debug, dict) else {}
                debug["calendar_commit"] = "error"
                debug["calendar_operation"] = "patch_description"
                debug["flow_id"] = flow_id
                debug["user_id"] = user_id
                debug["calendar_error"] = str(patch_description_res.get("err") or "patch_description_failed")
                out["debug"] = debug
                out["ok"] = False
                out["user_message"] = "Не получилось обновить комментарий встречи в календаре."
                logging.warning(
                    "meeting_update_patch_description_result trace_id=%s flow_id=%s event_id=%s result=error err=%s",
                    flow_id,
                    flow_id,
                    event_id,
                    str(patch_description_res.get("err") or "")[:120],
                )
                return out
        meeting_kind = deps["meeting_kind_from_entities"]("meeting.update", payload)
        debug = out.get("debug")
        debug = debug if isinstance(debug, dict) else {}
        debug["calendar_commit"] = "ok"
        debug["calendar_operation"] = "patch_with_description" if comment_text else "patch"
        debug["flow_id"] = flow_id
        debug["user_id"] = user_id
        out["debug"] = debug
        out["calendar_event_id"] = event_id
        out["user_message"] = deps["meeting_success_text"](meeting_kind, "reschedule")
        out["ok"] = True
        logging.info(
            "meeting_update_patch_result trace_id=%s flow_id=%s event_id=%s result=ok",
            flow_id,
            flow_id,
            event_id,
        )
        return out

    debug = out.get("debug")
    debug = debug if isinstance(debug, dict) else {}
    debug["calendar_commit"] = "error"
    debug["calendar_operation"] = "patch"
    debug["flow_id"] = flow_id
    debug["user_id"] = user_id
    debug["calendar_error"] = str(patch_res.get("err") or "patch_failed")
    out["debug"] = debug
    out["ok"] = False
    out["user_message"] = "Не получилось перенести встречу в календаре."
    logging.warning(
        "meeting_update_patch_result trace_id=%s flow_id=%s event_id=%s result=error err=%s",
        flow_id,
        flow_id,
        event_id,
        str(patch_res.get("err") or "")[:120],
    )
    return out


def _runtime_commit_meeting_comment_update_calendar(
    *,
    flow_id: str,
    entities: dict[str, Any],
    execution_result: dict[str, Any],
    deps: dict[str, Any],
) -> dict[str, Any]:
    out = dict(execution_result if isinstance(execution_result, dict) else {})
    payload = entities if isinstance(entities, dict) else {}
    user_id = str(payload.get("user_id") or "").strip()
    event_id = str(payload.get("calendar_event_id") or "").strip()
    comment_text = str(
        payload.get("comment_text")
        or payload.get("comment")
        or payload.get("description")
        or payload.get("notes")
        or ""
    ).strip()

    if not event_id:
        return {
            "ok": False,
            "user_message": "Не нашел встречу для изменения комментария. Уточните, какую встречу изменить.",
            "clarifying_question": "Какую встречу изменить?",
            "debug": {"reason": "missing_calendar_event_id", "requires_target_confirmation": True},
        }
    if not comment_text:
        return {
            "ok": False,
            "user_message": "Не вижу новый комментарий. Напишите текст комментария.",
            "clarifying_question": "Какой комментарий добавить?",
            "debug": {"reason": "missing_comment_text"},
        }

    logging.info(
        "meeting_comment_update_patch_start flow_id=%s event_id=%s comment_text=%s",
        flow_id,
        event_id,
        comment_text[:300],
    )
    patch_res = deps["calendar_patch_event_description"](event_id, comment_text)
    if bool(patch_res.get("ok")):
        debug = out.get("debug")
        debug = debug if isinstance(debug, dict) else {}
        debug["calendar_commit"] = "ok"
        debug["calendar_operation"] = "patch_description"
        debug["flow_id"] = flow_id
        debug["user_id"] = user_id
        out["debug"] = debug
        out["calendar_event_id"] = event_id
        out["user_message"] = "Комментарий встречи обновлён."
        out["ok"] = True
        logging.info("meeting_comment_update_patch_result flow_id=%s event_id=%s result=ok", flow_id, event_id)
        return out

    debug = out.get("debug")
    debug = debug if isinstance(debug, dict) else {}
    debug["calendar_commit"] = "error"
    debug["calendar_operation"] = "patch_description"
    debug["flow_id"] = flow_id
    debug["user_id"] = user_id
    debug["calendar_error"] = str(patch_res.get("err") or "patch_failed")
    out["debug"] = debug
    out["ok"] = False
    out["user_message"] = "Не получилось обновить комментарий встречи в календаре."
    logging.warning(
        "meeting_comment_update_patch_result flow_id=%s event_id=%s result=error err=%s",
        flow_id,
        event_id,
        str(patch_res.get("err") or "")[:120],
    )
    return out
