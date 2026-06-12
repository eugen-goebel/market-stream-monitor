"""Parsing for the public Coinbase Exchange websocket feed.

The matches channel delivers one JSON message per executed trade, for
example:

    {"type": "match", "trade_id": 1234, "product_id": "BTC-USD",
     "price": "63717.17", "size": "0.003", "side": "buy",
     "time": "2026-06-12T18:36:42.155489Z", ...}

Prices and sizes arrive as strings and are converted to floats here.
Messages of any other type (subscriptions, heartbeats, errors) parse
to None and are ignored by the pipeline.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

TRADE_TYPES = {"match", "last_match"}


@dataclass(frozen=True)
class Trade:
    ts: datetime
    product: str
    price: float
    size: float
    side: str


def parse_coinbase_message(message: dict[str, Any]) -> Trade | None:
    """Turn one feed message into a Trade, or None for non-trade types."""
    if message.get("type") not in TRADE_TYPES:
        return None
    try:
        return Trade(
            ts=datetime.fromisoformat(message["time"]),
            product=str(message["product_id"]),
            price=float(message["price"]),
            size=float(message["size"]),
            side=str(message["side"]),
        )
    except (KeyError, ValueError) as exc:
        raise ValueError(f"Malformed trade message: {message}") from exc
