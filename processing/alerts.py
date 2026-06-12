"""Anomaly alerts on the completed-bar stream.

Two rules, both evaluated when a bar completes:

- volume spike: the bar's volume exceeds a multiple of the average
  volume of the trailing bars for the same product. Needs a minimum
  history before it can fire, so the first minutes stay quiet.
- price jump: the bar moved more than a threshold percentage between
  its open and its close.
"""

from collections import deque
from datetime import datetime

from pydantic import BaseModel

from processing.bars import MinuteBar

VOLUME_FACTOR = 3.0
PRICE_MOVE_PCT = 0.5
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
        history_size: int = HISTORY_SIZE,
        min_history: int = MIN_HISTORY,
    ) -> None:
        self.volume_factor = volume_factor
        self.price_move_pct = price_move_pct
        self.min_history = min_history
        self._volumes: dict[str, deque[float]] = {}
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
        return alerts
