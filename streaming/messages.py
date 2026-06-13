"""Parsing for the public exchange websocket feeds.

Coinbase and Binance ship a trade per message but in different shapes,
so each provider has its own parser. Both return the same frozen Trade
dataclass, which lets one pipeline aggregate either feed.

The Coinbase matches channel delivers one JSON message per executed
trade, for example:

    {"type": "match", "trade_id": 1234, "product_id": "BTC-USD",
     "price": "63717.17", "size": "0.003", "side": "buy",
     "time": "2026-06-12T18:36:42.155489Z", ...}

Prices and sizes arrive as strings and are converted to floats here.
Messages of any other type (subscriptions, heartbeats, errors) parse
to None and are ignored by the pipeline.
"""

from dataclasses import dataclass
from datetime import UTC, datetime
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


def parse_binance_message(message: dict[str, Any]) -> Trade | None:
    """Turn one Binance trade message into a Trade, or None otherwise.

    The combined stream wraps each payload as {"stream": ...,
    "data": {...}} while the single stream sends the bare trade dict,
    so the data envelope is unwrapped when present. Price and quantity
    arrive as strings, the trade time T is epoch milliseconds, and m
    flags the buyer as the maker, so the taker sold and the side is
    "sell" when m is true and "buy" otherwise.
    """
    trade = message.get("data", message)
    if trade.get("e") != "trade":
        return None
    try:
        return Trade(
            ts=datetime.fromtimestamp(trade["T"] / 1000, tz=UTC),
            product=str(trade["s"]),
            price=float(trade["p"]),
            size=float(trade["q"]),
            side="sell" if trade["m"] else "buy",
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"Malformed trade message: {message}") from exc
