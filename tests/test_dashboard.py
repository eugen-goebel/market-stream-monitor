"""Smoke test for the Streamlit dashboard.

The auto-refresh loop sleeps and reruns forever, which would hang
AppTest, so the dashboard skips it when STREAM_MONITOR_NO_REFRESH is
set. This test sets that guard and points the app at a temporary
SQLite database seeded by replaying the bundled recording, then drives
the script through streamlit.testing.v1.AppTest.

db.database binds its engine and SessionLocal to DATABASE_URL at import
time, and conftest already imports that module, so setting the env var
later would not move the connection. The fixture instead seeds the temp
database in a subprocess and rebinds db.database.engine and SessionLocal
to it. AppTest runs app.py in this process, and app.py reads init_db and
SessionLocal off db.database at run time, so the rebind takes effect and
no stray default database file is created.
"""

import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import db.database as database

ROOT = Path(__file__).resolve().parent.parent
APP = str(ROOT / "app.py")
MAIN = str(ROOT / "main.py")


@pytest.fixture()
def seeded_app(tmp_path: Path) -> Iterator[str]:
    db_path = tmp_path / "dashboard-test.db"
    database_url = f"sqlite:///{db_path}"

    # Seed the temp database by replaying the bundled recording in a
    # subprocess, which writes the bars through the real pipeline.
    env = dict(os.environ, DATABASE_URL=database_url)
    result = subprocess.run(
        [sys.executable, MAIN, "replay", "data/sample-stream.jsonl"],
        capture_output=True,
        text=True,
        timeout=120,
        cwd=ROOT,
        env=env,
    )
    assert result.returncode == 0, result.stderr

    # Rebind the live engine and SessionLocal at the temp database so the
    # in-process app reads the seeded bars and init_db touches only this
    # file, and disable the refresh loop so AppTest does not hang on the
    # sleep and rerun.
    original_engine = database.engine
    original_session_local = database.SessionLocal
    test_engine = create_engine(database_url, connect_args={"check_same_thread": False})
    database.engine = test_engine
    database.SessionLocal = sessionmaker(bind=test_engine)
    os.environ["STREAM_MONITOR_NO_REFRESH"] = "1"
    try:
        yield APP
    finally:
        database.engine = original_engine
        database.SessionLocal = original_session_local
        test_engine.dispose()
        os.environ.pop("STREAM_MONITOR_NO_REFRESH", None)


def test_dashboard_runs_and_lists_products(seeded_app: str) -> None:
    from streamlit.testing.v1 import AppTest

    app = AppTest.from_file(seeded_app)
    app.run(timeout=30)

    assert not app.exception
    # With data present a product selectbox is rendered and populated.
    assert len(app.selectbox) >= 1
    selectbox = app.selectbox[0]
    assert selectbox.options
    assert selectbox.value in selectbox.options
