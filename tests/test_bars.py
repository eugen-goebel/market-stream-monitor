"""Bar aggregation checked against hand-computed values."""

import pytest

from processing.bars import BarAggregator, aggregate_minute_bars
from tests.conftest import make_trade


class TestBarAggregator:
    def test_hand_computed_single_minute(self):
        # three trades in one minute: 100 x 1.0, 102 x 2.0, 101 x 1.0
        # open 100, high 102, low 100, close 101, volume 4
        # vwap = (100*1 + 102*2 + 101*1) / 4 = 405 / 4 = 101.25
        trades = [
            make_trade(second=5, price=100.0, size=1.0),
            make_trade(second=20, price=102.0, size=2.0),
            make_trade(second=40, price=101.0, size=1.0),
        ]
        bars = aggregate_minute_bars(trades)
        assert len(bars) == 1
        bar = bars[0]
        assert bar.open == 100.0
        assert bar.high == 102.0
        assert bar.low == 100.0
        assert bar.close == 101.0
        assert bar.volume == pytest.approx(4.0)
        assert bar.vwap == pytest.approx(101.25)
        assert bar.trade_count == 3

    def test_minute_boundary_emits_completed_bar(self):
        aggregator = BarAggregator()
        assert aggregator.add(make_trade(minute=0, second=59, price=100.0)) == []
        completed = aggregator.add(make_trade(minute=1, second=0, price=200.0))
        assert len(completed) == 1
        assert completed[0].close == 100.0
        assert completed[0].minute.minute == 0

    def test_products_are_independent(self):
        aggregator = BarAggregator()
        aggregator.add(make_trade(product="BTC-USD", price=100.0))
        aggregator.add(make_trade(product="ETH-USD", price=10.0))
        bars = aggregator.flush()
        assert {b.product for b in bars} == {"BTC-USD", "ETH-USD"}
        assert all(b.trade_count == 1 for b in bars)

    def test_flush_clears_state(self):
        aggregator = BarAggregator()
        aggregator.add(make_trade())
        assert len(aggregator.flush()) == 1
        assert aggregator.flush() == []

    def test_minute_is_floored(self):
        bars = aggregate_minute_bars([make_trade(minute=7, second=33)])
        assert bars[0].minute.minute == 7
        assert bars[0].minute.second == 0

    def test_batch_spanning_minutes(self):
        trades = [
            make_trade(minute=0, second=10, price=100.0, size=1.0),
            make_trade(minute=1, second=10, price=110.0, size=1.0),
            make_trade(minute=2, second=10, price=120.0, size=1.0),
        ]
        bars = aggregate_minute_bars(trades)
        assert [b.close for b in bars] == [100.0, 110.0, 120.0]
