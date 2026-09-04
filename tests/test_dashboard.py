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


@pytest.fixture()
def empty_app(tmp_path: Path) -> Iterator[str]:
    """Point the dashboard at an empty database, the way a first visitor finds it.

    The seeded_app fixture replays the recording before starting the app, so
    it could never catch what someone sees on a fresh clone.
    """
    db_path = tmp_path / "empty.db"
    database_url = f"sqlite:///{db_path}"

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


def test_first_visit_loads_the_sample_instead_of_a_dead_end(empty_app: str) -> None:
    """A fresh database must still render a chart, not a CLI instruction.

    Regression: the dashboard opened on nothing but "run this command, then
    come back", which is a dead end for anyone who just wants to look at it.
    The recording ships in the repo, so there is no reason to ask.
    """
    from streamlit.testing.v1 import AppTest

    import app as app_module

    # st.cache_resource persists across tests in one process, which would
    # let a previous run satisfy this one without seeding.
    app_module._seed_sample_data.clear()

    app = AppTest.from_file(empty_app)
    app.run(timeout=60)

    assert not app.exception
    assert len(app.selectbox) >= 1, "no product selector rendered on a fresh database"
    assert app.selectbox[0].options, "product selector is empty on a fresh database"

    # The metric strip only renders once bars exist.
    labels = {m.label for m in app.metric}
    assert "Latest close" in labels

    # And the visitor is told the data is the bundled sample, not live.
    captions = " ".join(c.value for c in app.caption)
    assert "sample recording" in captions
