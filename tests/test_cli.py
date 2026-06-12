"""Smoke tests for the CLI entry point.

The unit tests never import main.py, so a syntax error or broken
subcommand wiring there would slip through them. Running --help in a
subprocess catches both. The replay test runs the real pipeline over
the bundled recording of live feed messages.
"""

import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
MAIN = str(ROOT / "main.py")

SUBCOMMANDS = ["record", "monitor", "replay", "bars", "alerts"]


def run_cli(*args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, MAIN, *args],
        capture_output=True,
        text=True,
        timeout=120,
        cwd=ROOT,
        env=env,
    )


class TestCliWiring:
    def test_top_level_help(self):
        result = run_cli("--help")
        assert result.returncode == 0
        for command in SUBCOMMANDS:
            assert command in result.stdout

    @pytest.mark.parametrize("command", SUBCOMMANDS)
    def test_subcommand_help(self, command):
        result = run_cli(command, "--help")
        assert result.returncode == 0, result.stderr


class TestReplayEndToEnd:
    def test_bundled_recording_replays(self, tmp_path):
        import os

        env = dict(os.environ, DATABASE_URL=f"sqlite:///{tmp_path}/test-stream.db")
        result = run_cli("replay", "data/sample-stream.jsonl", env=env)
        assert result.returncode == 0, result.stderr
        assert "Replayed" in result.stdout
        # the same database again: idempotent, zero new bars
        again = run_cli("replay", "data/sample-stream.jsonl", env=env)
        assert "(0 new)" in again.stdout
