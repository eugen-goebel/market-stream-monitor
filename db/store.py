"""Persist completed bars and alerts.

Bar inserts are idempotent on (product, minute), so replaying the same
recording twice never duplicates rows. The check works the same on
SQLite and PostgreSQL. Stored timestamps are naive UTC.
"""

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from db.models import StoredAlert, StoredBar
from processing.alerts import AlertEvent
from processing.bars import MinuteBar


def _naive_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts
    return ts.astimezone(UTC).replace(tzinfo=None)


def store_minute_bars(db: Session, bars: list[MinuteBar]) -> int:
    """Insert completed bars, return how many were new."""
    inserted = 0
    for bar in bars:
        minute = _naive_utc(bar.minute)
        exists = db.scalar(
            select(StoredBar.id).where(StoredBar.product == bar.product, StoredBar.minute == minute)
        )
        if exists is not None:
            continue
        db.add(
            StoredBar(
                product=bar.product,
                minute=minute,
                open=bar.open,
                high=bar.high,
                low=bar.low,
                close=bar.close,
                volume=bar.volume,
                vwap=bar.vwap,
                trade_count=bar.trade_count,
            )
        )
        inserted += 1
    db.commit()
    return inserted


def store_alerts(db: Session, alerts: list[AlertEvent]) -> int:
    for alert in alerts:
        db.add(
            StoredAlert(
                product=alert.product,
                ts=_naive_utc(alert.ts),
                rule=alert.rule,
                message=alert.message,
                value=alert.value,
            )
        )
    db.commit()
    return len(alerts)
