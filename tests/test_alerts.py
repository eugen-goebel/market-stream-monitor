"""Alert rules checked against hand-computed thresholds."""

import pytest

from processing.alerts import AlertEngine
from processing.bars import MinuteBar
from tests.conftest import make_trade


def bar(volume: float = 1.0, open_: float = 100.0, close: float = 100.0) -> MinuteBar:
    return MinuteBar(
        product="BTC-USD",
        minute=make_trade().ts,
        open=open_,
        high=max(open_, close),
        low=min(open_, close),
        close=close,
        volume=volume,
        vwap=(open_ + close) / 2,
        trade_count=10,
    )


class TestVolumeSpike:
    def test_spike_above_three_times_average_fires(self):
        engine = AlertEngine()
        # five quiet bars build the history, average volume 1.0
        for _ in range(5):
            assert engine.on_bar(bar(volume=1.0)) == []
        # 3.1 > 3.0 * 1.0
        alerts = engine.on_bar(bar(volume=3.1))
        assert len(alerts) == 1
        assert alerts[0].rule == "volume_spike"
        assert alerts[0].value == pytest.approx(3.1)

    def test_below_threshold_is_quiet(self):
        engine = AlertEngine()
        for _ in range(5):
            engine.on_bar(bar(volume=1.0))
        assert engine.on_bar(bar(volume=2.9)) == []

    def test_no_alert_before_minimum_history(self):
        engine = AlertEngine()
        for _ in range(4):
            engine.on_bar(bar(volume=1.0))
        # only 4 bars of history, the rule must stay quiet
        assert engine.on_bar(bar(volume=100.0)) == []

    def test_histories_are_per_product(self):
        engine = AlertEngine()
        for _ in range(5):
            engine.on_bar(bar(volume=1.0))
        other = bar(volume=100.0).model_copy(update={"product": "ETH-USD"})
        assert engine.on_bar(other) == []


class TestPriceJump:
    def test_move_above_threshold_fires(self):
        engine = AlertEngine()
        # 100 to 100.6 is +0.6%, above the 0.5% threshold. The exact
        # threshold itself is not asserted, float division puts
        # 100.5/100 - 1 a hair below 0.005.
        alerts = engine.on_bar(bar(open_=100.0, close=100.6))
        assert len(alerts) == 1
        assert alerts[0].rule == "price_jump"
        assert alerts[0].value == pytest.approx(0.6)

    def test_negative_move_fires(self):
        engine = AlertEngine()
        alerts = engine.on_bar(bar(open_=100.0, close=99.4))
        assert alerts[0].value == pytest.approx(-0.6)

    def test_small_move_is_quiet(self):
        engine = AlertEngine()
        assert engine.on_bar(bar(open_=100.0, close=100.4)) == []
