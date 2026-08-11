from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_legacy_src_api_app_removed() -> None:
    assert not (ROOT / "src" / "api" / "app.py").exists()


def test_legacy_src_api_package_marker_removed() -> None:
    assert not (ROOT / "src" / "api" / "__init__.py").exists()


def test_legacy_src_telegram_webhook_removed() -> None:
    assert not (ROOT / "src" / "telegram" / "webhook.py").exists()
