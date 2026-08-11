from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_docker_build_contract_script_passes_for_current_repo() -> None:
    script_path = ROOT / "scripts" / "check_docker_build_contract.py"
    result = subprocess.run(
        [sys.executable, str(script_path), "--services", "telegram-bot", "organizer-worker", "organizer-api", "google-sync"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr or result.stdout
    assert "DOCKER BUILD CONTRACT CHECK OK" in result.stdout
