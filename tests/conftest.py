"""Shared fixtures.

By default tests run on an in-memory SQLite database. When
TEST_DATABASE_URL is set (the CI PostgreSQL job does this) the same
suite runs against that server instead, with a clean schema per test.
"""

import os
from collections.abc import Iterator
from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from db import models  # noqa: F401  (registers the tables on Base)
from db.database import Base
from streaming.messages import Trade

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL", "")


@pytest.fixture()
def db() -> Iterator[Session]:
    if TEST_DATABASE_URL:
        engine = create_engine(TEST_DATABASE_URL)
    else:
        engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    TestSession = sessionmaker(bind=engine)
    session = TestSession()
    yield session
    session.close()
    engine.dispose()


def make_trade(
    minute: int = 0,
    second: int = 0,
    price: float = 100.0,
    size: float = 1.0,
    product: str = "BTC-USD",
    side: str = "buy",
) -> Trade:
    return Trade(
        ts=datetime(2026, 6, 12, 12, minute, second, tzinfo=UTC),
        product=product,
        price=price,
        size=size,
        side=side,
    )
