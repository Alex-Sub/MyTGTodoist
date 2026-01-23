from __future__ import annotations

import re


_AFTER_TOMORROW_RE = re.compile(r"\bпосле\s+завтра\b", flags=re.IGNORECASE)
_TIME_HOURS_MIN_RE = re.compile(
    r"\bв\s+(\d{1,2})\s*час(?:а|ов)?\s+(\d{1,2})\s*мин(?:ут|)\b",
    flags=re.IGNORECASE,
)
_TIME_HOURS_RE = re.compile(
    r"\bв\s+(\d{1,2})\s*час(?:а|ов)?\b",
    flags=re.IGNORECASE,
)
_TIME_COLON_RE = re.compile(r"\bв\s+(\d{1,2})\s*[:.]\s*(\d{2})\b", flags=re.IGNORECASE)
_TIME_SPACE_RE = re.compile(r"\bв\s+(\d{1,2})\s+(\d{2})\b", flags=re.IGNORECASE)
_TIME_BARE_SEP_RE = re.compile(r"\b([01]?\d|2[0-3])\s*([.\-])\s*([0-5]\d)\b")
_TIME_BARE_SPACE_RE = re.compile(r"\b([01]?\d|2[0-3])\s+([0-5]\d)\b")
_TIME_CONTEXT_RE = re.compile(
    r"\b(встреча|митинг|созвон|созвониться|совещание|перенеси|сдвинь|создай|запланируй)\b",
    flags=re.IGNORECASE,
)


def _format_time(hour: str, minute: str) -> str | None:
    try:
        h = int(hour)
        m = int(minute)
    except ValueError:
        return None
    if h < 0 or h > 23 or m < 0 or m > 59:
        return None
    return f"{h}:{m:02d}"


def normalize_asr_text(text: str) -> str:
    """
    Делает канонизацию для NLU:
    - 'в 16 часов' -> '16:00'
    - 'в 16 час' -> '16:00'
    - 'в 16 часов 10 минут' -> '16:10'
    - '18.30'/'18 30'/'18-30' -> '18:30'
    - 'после завтра' -> 'послезавтра'
    - лишние пробелы/переносы
    """
    if not text:
        return ""

    normalized = _AFTER_TOMORROW_RE.sub("послезавтра", text)

    def _replace_hours_min(match: re.Match[str]) -> str:
        formatted = _format_time(match.group(1), match.group(2))
        if not formatted:
            return match.group(0)
        return formatted

    normalized = _TIME_HOURS_MIN_RE.sub(_replace_hours_min, normalized)

    def _replace_hours(match: re.Match[str]) -> str:
        formatted = _format_time(match.group(1), "00")
        if not formatted:
            return match.group(0)
        return formatted

    normalized = _TIME_HOURS_RE.sub(_replace_hours, normalized)

    def _replace_colon(match: re.Match[str]) -> str:
        formatted = _format_time(match.group(1), match.group(2))
        if not formatted:
            return match.group(0)
        return formatted

    normalized = _TIME_COLON_RE.sub(_replace_colon, normalized)
    normalized = _TIME_SPACE_RE.sub(_replace_colon, normalized)

    def _replace_bare_sep(match: re.Match[str]) -> str:
        hour = match.group(1)
        sep = match.group(2)
        minute = match.group(3)
        formatted = _format_time(hour, minute)
        if not formatted:
            return match.group(0)
        if sep == ".":
            try:
                minute_value = int(minute)
            except ValueError:
                minute_value = 99
            if minute_value <= 12 and not _TIME_CONTEXT_RE.search(normalized):
                return match.group(0)
        return formatted

    def _replace_bare_space(match: re.Match[str]) -> str:
        formatted = _format_time(match.group(1), match.group(2))
        if not formatted:
            return match.group(0)
        return formatted

    normalized = _TIME_BARE_SEP_RE.sub(_replace_bare_sep, normalized)
    normalized = _TIME_BARE_SPACE_RE.sub(_replace_bare_space, normalized)

    normalized = " ".join(normalized.split())
    return normalized
