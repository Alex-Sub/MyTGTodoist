from pathlib import Path

import pytest

from organizer_worker.startup_preflight import ensure_canon_mounted


def test_ensure_canon_mounted_ok(tmp_path: Path) -> None:
    canon_file = tmp_path / "intents_v2.yml"
    canon_file.write_text("intents: {}\n", encoding="utf-8")
    resolved = ensure_canon_mounted(canon_file)
    assert resolved == canon_file


def test_ensure_canon_mounted_missing() -> None:
    with pytest.raises(RuntimeError, match="canon not mounted: add ./canon:/canon:ro to compose"):
        ensure_canon_mounted("/path/does/not/exist/intents_v2.yml")
