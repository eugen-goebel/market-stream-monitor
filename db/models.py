"""ORM models for stored minute bars and alerts."""

from datetime import datetime

from sqlalchemy import DateTime, Float, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from db.database import Base


class StoredBar(Base):
    __tablename__ = "minute_bars"
    __table_args__ = (UniqueConstraint("product", "minute", name="uq_bar_per_minute"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    product: Mapped[str] = mapped_column(String(32), index=True)
    minute: Mapped[datetime] = mapped_column(DateTime, index=True)
    open: Mapped[float] = mapped_column(Float)
    high: Mapped[float] = mapped_column(Float)
    low: Mapped[float] = mapped_column(Float)
    close: Mapped[float] = mapped_column(Float)
    volume: Mapped[float] = mapped_column(Float)
    vwap: Mapped[float] = mapped_column(Float)
    trade_count: Mapped[int] = mapped_column(Integer)


class StoredAlert(Base):
    __tablename__ = "alerts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    product: Mapped[str] = mapped_column(String(32), index=True)
    ts: Mapped[datetime] = mapped_column(DateTime, index=True)
    rule: Mapped[str] = mapped_column(String(32))
    message: Mapped[str] = mapped_column(String(256))
    value: Mapped[float] = mapped_column(Float)
