from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any


_RU_MONTHS = {
    "январ": 1,
    "феврал": 2,
    "март": 3,
    "апрел": 4,
    "ма": 5,
    "июн": 6,
    "июл": 7,
    "август": 8,
    "сентябр": 9,
    "октябр": 10,
    "ноябр": 11,
    "декабр": 12,
}

_RU_WEEKDAYS = {
    "понедельник": 0,
    "понедельника": 0,
    "вторник": 1,
    "вторника": 1,
    "среда": 2,
    "среду": 2,
    "среды": 2,
    "четверг": 3,
    "четверга": 3,
    "пятница": 4,
    "пятницу": 4,
    "пятницы": 4,
    "суббота": 5,
    "субботу": 5,
    "субботы": 5,
    "воскресенье": 6,
    "воскресенья": 6,
}

_TEXT_DATE_PATTERNS = (
    r"\b(?:на\s+)?(сегодня|завтра|послезавтра)\b",
    r"\b(?:на\s+|в\s+)?(?:следующий\s+)?(понедельник(?:а)?|вторник(?:а)?|сред(?:а|у|ы)|четверг(?:а)?|пятниц(?:а|у|ы)|суббот(?:а|у|ы)|воскресень(?:е|я))\b",
    r"\b\d{4}-\d{2}-\d{2}\b",
    r"\b\d{1,2}[./-]\d{1,2}(?:[./-]\d{4})?\b",
    r"\b\d{1,2}\s+[а-яё]+(?:\s+\d{4})?\b",
)

_CANONICAL_DATE_FIELDS = ("due_date", "planned_at", "date", "start_date", "target_date", "start_at_date")
_DATEISH_FIELDS = ("when",)
_DATETIME_CAPABLE_FIELDS = {"planned_at"}


@dataclass(frozen=True, slots=True)
class DateResolution:
    ok: bool
    raw: str
    normalized_text: str
    date: str | None
    datetime: str | None
    granularity: str
    confidence: str
    needs_clarification: bool
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "raw": self.raw,
            "normalized_text": self.normalized_text,
            "date": self.date,
            "datetime": self.datetime,
            "granularity": self.granularity,
            "confidence": self.confidence,
            "needs_clarification": self.needs_clarification,
            "reason": self.reason,
        }


def _coerce_now(now: datetime | date | None) -> datetime:
    if isinstance(now, datetime):
        return now
    if isinstance(now, date):
        return datetime.combine(now, datetime.min.time())
    return datetime.now()


def _normalize_text(raw: Any) -> str:
    text = str(raw or "").strip().lower().replace("ё", "е")
    text = re.sub(r"[,\u00a0]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _safe_date(year: int, month: int, day: int) -> date | None:
    try:
        return date(year, month, day)
    except ValueError:
        return None


def _ok(raw: str, resolved_date: date, *, confidence: str = "high") -> DateResolution:
    iso = resolved_date.isoformat()
    return DateResolution(
        ok=True,
        raw=raw,
        normalized_text=iso,
        date=iso,
        datetime=None,
        granularity="day",
        confidence=confidence,
        needs_clarification=False,
        reason=None,
    )


def _ok_datetime(raw: str, resolved_dt: datetime, *, confidence: str = "high") -> DateResolution:
    iso_dt = resolved_dt.isoformat()
    return DateResolution(
        ok=True,
        raw=raw,
        normalized_text=iso_dt,
        date=resolved_dt.date().isoformat(),
        datetime=iso_dt,
        granularity="datetime",
        confidence=confidence,
        needs_clarification=False,
        reason=None,
    )


def _clarify(raw: str, reason: str, *, confidence: str = "low") -> DateResolution:
    return DateResolution(
        ok=False,
        raw=raw,
        normalized_text="",
        date=None,
        datetime=None,
        granularity="unknown",
        confidence=confidence,
        needs_clarification=True,
        reason=reason,
    )


def resolve_date_phrase(
    raw: str,
    now: datetime,
    locale: str = "ru",
    prefer_future: bool = True,
) -> DateResolution:
    _ = locale
    base_dt = _coerce_now(now)
    base = base_dt.date()
    text = _normalize_text(raw)
    if not text:
        return _clarify("", "empty_date_phrase")

    if text in {"на выходных", "выходные", "на выходные", "на уикенде"}:
        return _clarify(str(raw or "").strip(), "weekend_policy_required")
    if text in {"на неделе", "на этой неделе", "в течение недели"}:
        return _clarify(str(raw or "").strip(), "week_policy_required")

    if text.startswith("на "):
        text = text[3:].strip()
    if text.startswith("в "):
        text = text[2:].strip()

    if text in {"сегодня", "today"}:
        return _ok(str(raw or "").strip(), base)
    if text in {"завтра", "tomorrow"}:
        return _ok(str(raw or "").strip(), base + timedelta(days=1))
    if text == "послезавтра":
        return _ok(str(raw or "").strip(), base + timedelta(days=2))

    weekday_text = text
    next_after_nearest = False
    if weekday_text.startswith("следующий "):
        next_after_nearest = True
        weekday_text = weekday_text[len("следующий ") :].strip()

    weekday = _RU_WEEKDAYS.get(weekday_text)
    if weekday is not None:
        delta = (weekday - base.weekday()) % 7
        if prefer_future and delta == 0:
            delta = 7
        resolved = base + timedelta(days=delta)
        if next_after_nearest:
            resolved += timedelta(days=7)
        return _ok(str(raw or "").strip(), resolved)

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        parsed = _safe_date(*[int(x) for x in text.split("-")])
        if parsed is None:
            return _clarify(str(raw or "").strip(), "invalid_iso_date")
        return _ok(str(raw or "").strip(), parsed)

    iso_candidate = text
    if iso_candidate.endswith("z"):
        iso_candidate = iso_candidate[:-1] + "+00:00"
    try:
        parsed_dt = datetime.fromisoformat(iso_candidate)
    except Exception:
        parsed_dt = None
    if parsed_dt is not None:
        return _ok_datetime(str(raw or "").strip(), parsed_dt)

    match = re.fullmatch(r"(\d{1,2})[./-](\d{1,2})(?:[./-](\d{4}))?", text)
    if match:
        day = int(match.group(1))
        month = int(match.group(2))
        year = int(match.group(3)) if match.group(3) else base.year
        parsed = _safe_date(year, month, day)
        if parsed is None:
            return _clarify(str(raw or "").strip(), "invalid_numeric_date")
        return _ok(str(raw or "").strip(), parsed)

    match = re.fullmatch(r"(\d{1,2})\s+([а-яё]+)(?:\s+(\d{4}))?", text)
    if match:
        day = int(match.group(1))
        month_token = str(match.group(2) or "").strip().rstrip(".")
        month = None
        for stem, month_idx in _RU_MONTHS.items():
            if month_token.startswith(stem):
                month = month_idx
                break
        if month is None:
            return _clarify(str(raw or "").strip(), "unknown_month_name")
        year = int(match.group(3)) if match.group(3) else base.year
        parsed = _safe_date(year, month, day)
        if parsed is None:
            return _clarify(str(raw or "").strip(), "invalid_month_name_date")
        return _ok(str(raw or "").strip(), parsed)

    if re.fullmatch(r"\d{1,2}", text):
        return _clarify(str(raw or "").strip(), "ambiguous_day_without_month", confidence="medium")

    return _clarify(str(raw or "").strip(), "unresolved_date_phrase")


def extract_date_resolution(
    raw_text: Any,
    now: datetime,
    *,
    locale: str = "ru",
    prefer_future: bool = True,
) -> DateResolution:
    text = _normalize_text(raw_text)
    if not text:
        return _clarify("", "empty_date_phrase")
    for pattern in _TEXT_DATE_PATTERNS:
        match = re.search(pattern, text)
        if not match:
            continue
        candidate = match.group(0)
        resolution = resolve_date_phrase(candidate, now=now, locale=locale, prefer_future=prefer_future)
        if resolution.ok or resolution.needs_clarification:
            return resolution
    return _clarify(str(raw_text or "").strip(), "unresolved_date_phrase")


def normalize_temporal_fields(payload: Any, now: datetime) -> Any:
    if not isinstance(payload, dict):
        return payload
    out = dict(payload)
    entities = out.get("entities")
    entities_out = dict(entities) if isinstance(entities, dict) else None
    resolutions: dict[str, dict[str, Any]] = {}

    def _record(field: str, raw_value: str, resolution: DateResolution) -> None:
        resolutions[field] = resolution.to_dict()
        if resolution.ok and (resolution.date or resolution.datetime):
            resolved_value = (
                resolution.datetime
                if field in _DATETIME_CAPABLE_FIELDS and resolution.datetime
                else resolution.date
            )
            out[field] = resolved_value
            if entities_out is not None:
                entities_out[field] = resolved_value
        else:
            out.pop(field, None)
            if entities_out is not None:
                entities_out.pop(field, None)
        raw_key = f"__raw_{field}"
        if raw_value:
            out.setdefault(raw_key, raw_value)
            if entities_out is not None:
                entities_out.setdefault(raw_key, raw_value)

    for field in _CANONICAL_DATE_FIELDS + _DATEISH_FIELDS:
        raw_value = str(out.get(field) or (entities_out.get(field) if entities_out is not None else "") or "").strip()
        if not raw_value:
            continue
        resolution = resolve_date_phrase(raw_value, now=now, prefer_future=True)
        if not resolution.ok and field in _DATEISH_FIELDS:
            continue
        _record(field, raw_value, resolution)

    if not any(out.get(field) for field in ("planned_at", "due_date")):
        text_source = str(out.get("text") or (entities_out.get("text") if entities_out is not None else "") or "").strip()
        if text_source:
            resolution = extract_date_resolution(text_source, now=now, prefer_future=True)
            if resolution.ok and resolution.date:
                out["planned_at"] = resolution.date
                out["due_date"] = resolution.date
                resolutions.setdefault("planned_at", resolution.to_dict())
                resolutions.setdefault("due_date", resolution.to_dict())
                if entities_out is not None:
                    entities_out["planned_at"] = resolution.date
                    entities_out["due_date"] = resolution.date

    due_date = str(out.get("due_date") or (entities_out.get("due_date") if entities_out is not None else "") or "").strip()
    planned_at = str(out.get("planned_at") or (entities_out.get("planned_at") if entities_out is not None else "") or "").strip()
    if due_date and not planned_at:
        out["planned_at"] = due_date
        if entities_out is not None:
            entities_out["planned_at"] = due_date
    if planned_at and not due_date:
        out["due_date"] = planned_at
        if entities_out is not None:
            entities_out["due_date"] = planned_at

    if entities_out is not None:
        out["entities"] = entities_out
    if resolutions:
        out["__date_resolution"] = resolutions
    return out
