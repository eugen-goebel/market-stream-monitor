"""Anomaly alerts on the completed-bar stream.

Four rules, all evaluated when a bar completes:

- volume spike: the bar's volume exceeds a multiple of the average
  volume of the trailing bars for the same product. Needs a minimum
  history before it can fire, so the first minutes stay quiet.
- price jump: the bar moved more than a threshold percentage between
  its open and its close.
- vwap deviation: the bar's close sits more than a threshold percentage
  away from the volume weighted rolling VWAP of the trailing bars, a
  dislocation signal. Needs a minimum history before it can fire.
- trade rate burst: the bar's trade count exceeds a multiple of the
  average trade count of the trailing bars, a surge in activity and the
  trade count analogue of the volume spike. Needs a minimum history.
"""

from collections import deque
from datetime import datetime

from pydantic import BaseModel

from processing.bars import MinuteBar

VOLUME_FACTOR = 3.0
PRICE_MOVE_PCT = 0.5
VWAP_DEVIATION_PCT = 1.0
TRADE_RATE_FACTOR = 3.0
HISTORY_SIZE = 20
MIN_HISTORY = 5


class AlertEvent(BaseModel):
    product: str
    ts: datetime
    rule: str
    message: str
    value: float


class AlertEngine:
    def __init__(
        self,
        volume_factor: float = VOLUME_FACTOR,
        price_move_pct: float = PRICE_MOVE_PCT,
        vwap_deviation_pct: float = VWAP_DEVIATION_PCT,
        trade_rate_factor: float = TRADE_RATE_FACTOR,
        history_size: int = HISTORY_SIZE,
        min_history: int = MIN_HISTORY,
    ) -> None:
        self.volume_factor = volume_factor
        self.price_move_pct = price_move_pct
        self.vwap_deviation_pct = vwap_deviation_pct
        self.trade_rate_factor = trade_rate_factor
        self.min_history = min_history
        self._volumes: dict[str, deque[float]] = {}
        self._vwaps: dict[str, deque[tuple[float, float]]] = {}
        self._trade_counts: dict[str, deque[int]] = {}
        self._history_size = history_size

    def on_bar(self, bar: MinuteBar) -> list[AlertEvent]:
        """Evaluate one completed bar, return the alerts it triggered."""
        alerts: list[AlertEvent] = []

        history = self._volumes.setdefault(bar.product, deque(maxlen=self._history_size))
        if len(history) >= self.min_history:
            average = sum(history) / len(history)
            if average > 0 and bar.volume > self.volume_factor * average:
                ratio = bar.volume / average
                alerts.append(
                    AlertEvent(
                        product=bar.product,
                        ts=bar.minute,
                        rule="volume_spike",
                        message=(
                            f"{bar.product} volume {bar.volume:.4f} is {ratio:.1f}x "
                            f"the trailing average {average:.4f}"
                        ),
                        value=round(ratio, 4),
                    )
                )
        history.append(bar.volume)

        move_pct = (bar.close / bar.open - 1.0) * 100 if bar.open > 0 else 0.0
        if abs(move_pct) >= self.price_move_pct:
            alerts.append(
                AlertEvent(
                    product=bar.product,
                    ts=bar.minute,
                    rule="price_jump",
                    message=(
                        f"{bar.product} moved {move_pct:+.2f}% within one minute "
                        f"({bar.open} to {bar.close})"
                    ),
                    value=round(move_pct, 4),
                )
            )

        vwaps = self._vwaps.setdefault(bar.product, deque(maxlen=self._history_size))
        if len(vwaps) >= self.min_history:
            total_volume = sum(volume for _, volume in vwaps)
            if total_volume > 0:
                rolling_vwap = sum(vwap * volume for vwap, volume in vwaps) / total_volume
                if rolling_vwap > 0:
                    deviation_pct = (bar.close / rolling_vwap - 1.0) * 100
                    if abs(deviation_pct) >= self.vwap_deviation_pct:
                        alerts.append(
                            AlertEvent(
                                product=bar.product,
                                ts=bar.minute,
                                rule="vwap_deviation",
                                message=(
                                    f"{bar.product} close {bar.close} is {deviation_pct:+.2f}% "
                                    f"from the rolling VWAP {rolling_vwap:.4f}"
                                ),
                                value=round(deviation_pct, 4),
                            )
                        )
        vwaps.append((bar.vwap, bar.volume))

        trade_counts = self._trade_counts.setdefault(bar.product, deque(maxlen=self._history_size))
        if len(trade_counts) >= self.min_history:
            average = sum(trade_counts) / len(trade_counts)
            if average > 0 and bar.trade_count > self.trade_rate_factor * average:
                ratio = bar.trade_count / average
                alerts.append(
                    AlertEvent(
                        product=bar.product,
                        ts=bar.minute,
                        rule="trade_rate_burst",
                        message=(
                            f"{bar.product} trade count {bar.trade_count} is {ratio:.1f}x "
                            f"the trailing average {average:.2f}"
                        ),
                        value=round(ratio, 4),
                    )
                )
        trade_counts.append(bar.trade_count)

        return alerts
