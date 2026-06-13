"""Alert rules checked against hand-computed thresholds."""

import pytest

from processing.alerts import AlertEngine
from processing.bars import MinuteBar
from tests.conftest import make_trade


def bar(
    volume: float = 1.0,
    open_: float = 100.0,
    close: float = 100.0,
    vwap: float | None = None,
    trade_count: int = 10,
) -> MinuteBar:
    return MinuteBar(
        product="BTC-USD",
        minute=make_trade().ts,
        open=open_,
        high=max(open_, close),
        low=min(open_, close),
        close=close,
        volume=volume,
        vwap=(open_ + close) / 2 if vwap is None else vwap,
        trade_count=trade_count,
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


class TestVwapDeviation:
    def test_deviation_above_threshold_fires(self):
        engine = AlertEngine()
        # five prior bars all vwap=100, volume=10, so the rolling VWAP is
        # (100*10 * 5) / (10 * 5) = 100.0 exactly. The current close is set
        # equal to open so price_jump stays quiet, volume matches the history
        # so volume_spike stays quiet, and trade_count holds at the default 10.
        for _ in range(5):
            assert engine.on_bar(bar(volume=10.0, vwap=100.0)) == []
        # close 101.5 against rolling VWAP 100.0 is (101.5/100 - 1)*100 = +1.5%,
        # above the 1.0 default.
        alerts = engine.on_bar(bar(volume=10.0, open_=101.5, close=101.5, vwap=101.5))
        assert len(alerts) == 1
        assert alerts[0].rule == "vwap_deviation"
        assert alerts[0].value == pytest.approx(1.5)

    def test_volume_weighting_matters(self):
        # Two prior bars only, so build an engine with min_history=2.
        # rolling VWAP = (100*10 + 102*30) / (10 + 30) = 4060 / 40 = 101.5,
        # the heavier 102 bar pulls the average up past the simple mean of 101.
        engine = AlertEngine(min_history=2)
        engine.on_bar(bar(volume=10.0, vwap=100.0))
        engine.on_bar(bar(volume=30.0, vwap=102.0))
        # close 101.5 equals the rolling VWAP, deviation is 0% and stays quiet.
        assert engine.on_bar(bar(volume=10.0, open_=101.5, close=101.5, vwap=101.5)) == []

        engine = AlertEngine(min_history=2)
        engine.on_bar(bar(volume=10.0, vwap=100.0))
        engine.on_bar(bar(volume=30.0, vwap=102.0))
        # close 103 against rolling VWAP 101.5 is (103/101.5 - 1)*100 = +1.4778%,
        # above the 1.0 default.
        alerts = engine.on_bar(bar(volume=10.0, open_=103.0, close=103.0, vwap=103.0))
        assert len(alerts) == 1
        assert alerts[0].rule == "vwap_deviation"
        assert alerts[0].value == pytest.approx(1.4778, abs=1e-4)

    def test_close_within_threshold_is_quiet(self):
        engine = AlertEngine()
        for _ in range(5):
            engine.on_bar(bar(volume=10.0, vwap=100.0))
        # close 100.5 against rolling VWAP 100.0 is +0.5%, below the 1.0 default.
        assert engine.on_bar(bar(volume=10.0, open_=100.5, close=100.5, vwap=100.5)) == []

    def test_no_alert_before_minimum_history(self):
        engine = AlertEngine()
        for _ in range(4):
            engine.on_bar(bar(volume=10.0, vwap=100.0))
        # only 4 prior bars, the rule must stay quiet even on a large dislocation
        assert engine.on_bar(bar(volume=10.0, open_=120.0, close=120.0, vwap=120.0)) == []

    def test_histories_are_per_product(self):
        engine = AlertEngine()
        for _ in range(5):
            engine.on_bar(bar(volume=10.0, vwap=100.0))
        other = bar(volume=10.0, open_=120.0, close=120.0, vwap=120.0).model_copy(
            update={"product": "ETH-USD"}
        )
        assert engine.on_bar(other) == []


class TestTradeRateBurst:
    def test_burst_above_three_times_average_fires(self):
        engine = AlertEngine()
        # five prior bars with trade_count=10 build an average of 10.0. Close
        # equals open so price_jump stays quiet, vwap matches close so
        # vwap_deviation stays quiet, volume holds steady at the default 1.0.
        for _ in range(5):
            assert engine.on_bar(bar(trade_count=10)) == []
        # 31 / 10.0 = 3.1, above the 3.0 default.
        alerts = engine.on_bar(bar(trade_count=31))
        assert len(alerts) == 1
        assert alerts[0].rule == "trade_rate_burst"
        assert alerts[0].value == pytest.approx(3.1)

    def test_below_threshold_is_quiet(self):
        engine = AlertEngine()
        for _ in range(5):
            engine.on_bar(bar(trade_count=10))
        # 29 / 10.0 = 2.9, below the 3.0 default.
        assert engine.on_bar(bar(trade_count=29)) == []

    def test_no_alert_before_minimum_history(self):
        engine = AlertEngine()
        for _ in range(4):
            engine.on_bar(bar(trade_count=10))
        # only 4 prior bars, the rule must stay quiet
        assert engine.on_bar(bar(trade_count=100)) == []
