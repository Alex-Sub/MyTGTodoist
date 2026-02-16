import importlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
API_PATH = ROOT / "organizer-api"
sys.path.append(str(API_PATH))


def test_health_ok_payload() -> None:
    app_mod = importlib.import_module("app")
    app_mod = importlib.reload(app_mod)
    assert app_mod.health() == {"ok": True}
