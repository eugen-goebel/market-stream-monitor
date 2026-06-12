"""Tumbling one-minute bars from a trade stream.

The aggregator keeps one open bar per product. When a trade arrives
whose timestamp falls into a later minute, the open bar is emitted as
completed and a new one starts. Trades inside one minute fold into
open, high, low, close, summed volume, the volume weighted average
price and the trade count.
"""

from collections.abc import Iterable
from datetime import datetime

from pydantic import BaseModel

from streaming.messages import Trade


class MinuteBar(BaseModel):
    product: str
    minute: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float
    trade_count: int


class _OpenBar:
    def __init__(self, trade: Trade, minute: datetime) -> None:
        self.minute = minute
        self.open = trade.price
        self.high = trade.price
        self.low = trade.price
        self.close = trade.price
        self.volume = trade.size
        self.notional = trade.price * trade.size
        self.trade_count = 1

    def add(self, trade: Trade) -> None:
        self.high = max(self.high, trade.price)
        self.low = min(self.low, trade.price)
        self.close = trade.price
        self.volume += trade.size
        self.notional += trade.price * trade.size
        self.trade_count += 1

    def to_bar(self, product: str) -> MinuteBar:
        vwap = self.notional / self.volume if self.volume > 0 else self.close
        return MinuteBar(
            product=product,
            minute=self.minute,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=round(self.volume, 8),
            vwap=round(vwap, 8),
            trade_count=self.trade_count,
        )


def _floor_to_minute(ts: datetime) -> datetime:
    return ts.replace(second=0, microsecond=0)


class BarAggregator:
    """Folds a trade stream into completed one-minute bars per product."""

    def __init__(self) -> None:
        self._open: dict[str, _OpenBar] = {}

    def add(self, trade: Trade) -> list[MinuteBar]:
        """Feed one trade, return any bars this trade completed."""
        minute = _floor_to_minute(trade.ts)
        current = self._open.get(trade.product)
        if current is None:
            self._open[trade.product] = _OpenBar(trade, minute)
            return []
        if minute > current.minute:
            completed = current.to_bar(trade.product)
            self._open[trade.product] = _OpenBar(trade, minute)
            return [completed]
        current.add(trade)
        return []

    def flush(self) -> list[MinuteBar]:
        """Emit all open bars, used when a stream ends."""
        bars = [open_bar.to_bar(product) for product, open_bar in self._open.items()]
        self._open.clear()
        return sorted(bars, key=lambda b: (b.product, b.minute))


def aggregate_minute_bars(trades: Iterable[Trade]) -> list[MinuteBar]:
    """Batch helper: fold an entire trade sequence into minute bars."""
    aggregator = BarAggregator()
    bars: list[MinuteBar] = []
    for trade in trades:
        bars.extend(aggregator.add(trade))
    bars.extend(aggregator.flush())
    return bars
