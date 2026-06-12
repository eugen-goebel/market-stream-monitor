"""Database setup with SQLAlchemy.

The connection string comes from the DATABASE_URL environment variable.
It defaults to a local SQLite file for development, docker compose and
production point it at PostgreSQL.
"""

import os
from collections.abc import Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///stream.db")


class Base(DeclarativeBase):
    pass


def _connect_args(url: str) -> dict[str, bool]:
    if url.startswith("sqlite"):
        return {"check_same_thread": False}
    return {}


engine = create_engine(DATABASE_URL, connect_args=_connect_args(DATABASE_URL))
SessionLocal = sessionmaker(bind=engine)


def init_db() -> None:
    """Create all tables if they do not exist."""
    from db import models  # noqa: F401

    Base.metadata.create_all(bind=engine)


def get_db() -> Iterator[Session]:
    """Yield a database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
