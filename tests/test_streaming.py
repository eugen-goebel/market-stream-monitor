"""Message parsing, replay, storage, and the websocket client.

The client test runs a local plaintext websocket server that speaks
the Coinbase feed protocol, so the reconnect and subscribe logic is
exercised without the network.
"""

import asyncio
import json
from datetime import UTC, datetime

import pytest
import websockets

from db.store import store_alerts, store_minute_bars
from processing.alerts import AlertEvent
from processing.bars import aggregate_minute_bars
from streaming.client import stream_messages, subscribe_message
from streaming.messages import parse_binance_message, parse_coinbase_message
from streaming.providers import BINANCE, COINBASE, PROVIDERS, stream_provider_messages
from streaming.replay import iter_trades
from tests.conftest import make_trade

MATCH_MESSAGE = {
    "type": "match",
    "trade_id": 1,
    "product_id": "BTC-USD",
    "price": "63717.17",
    "size": "0.003",
    "side": "buy",
    "time": "2026-06-12T18:36:42.155489+00:00",
}

BINANCE_TRADE = {
    "e": "trade",
    "E": 1234567890123,
    "s": "BTCUSDT",
    "t": 12345,
    "p": "63774.19000000",
    "q": "0.00010000",
    "T": 1234567890120,
    "m": True,
}


class TestParseMessage:
    def test_match_message(self):
        trade = parse_coinbase_message(MATCH_MESSAGE)
        assert trade is not None
        assert trade.product == "BTC-USD"
        assert trade.price == pytest.approx(63717.17)
        assert trade.size == pytest.approx(0.003)
        assert trade.side == "buy"
        assert trade.ts.year == 2026

    def test_zulu_timestamps_parse(self):
        message = dict(MATCH_MESSAGE, time="2026-06-12T18:36:42.155489Z")
        trade = parse_coinbase_message(message)
        assert trade is not None and trade.ts.tzinfo is not None

    def test_non_trade_types_are_none(self):
        assert parse_coinbase_message({"type": "subscriptions"}) is None
        assert parse_coinbase_message({"type": "heartbeat"}) is None

    def test_malformed_trade_raises(self):
        with pytest.raises(ValueError, match="Malformed"):
            parse_coinbase_message({"type": "match", "price": "oops"})


class TestParseBinanceMessage:
    def test_bare_trade_maker_true_is_a_sell(self):
        trade = parse_binance_message(BINANCE_TRADE)
        assert trade is not None
        assert trade.product == "BTCUSDT"
        assert trade.price == pytest.approx(63774.19)
        assert trade.size == pytest.approx(0.0001)
        assert trade.side == "sell"
        assert trade.ts == datetime(2009, 2, 13, 23, 31, 30, 120000, tzinfo=UTC)

    def test_maker_false_is_a_buy(self):
        trade = parse_binance_message(dict(BINANCE_TRADE, m=False))
        assert trade is not None and trade.side == "buy"

    def test_combined_stream_wrapper_unwraps(self):
        wrapped = {"stream": "btcusdt@trade", "data": BINANCE_TRADE}
        trade = parse_binance_message(wrapped)
        assert trade is not None
        assert trade.product == "BTCUSDT"
        assert trade.price == pytest.approx(63774.19)

    def test_non_trade_is_none(self):
        assert parse_binance_message({"result": None, "id": 1}) is None
        assert parse_binance_message({"e": "kline", "s": "BTCUSDT"}) is None

    def test_malformed_trade_raises(self):
        with pytest.raises(ValueError, match="Malformed"):
            parse_binance_message({"e": "trade", "p": "oops"})


class TestProviders:
    def test_registry_has_both_providers(self):
        assert set(PROVIDERS) == {"coinbase", "binance"}
        assert PROVIDERS["coinbase"] is COINBASE
        assert PROVIDERS["binance"] is BINANCE

    def test_binance_url_is_a_combined_stream(self):
        url = BINANCE.build_url(["BTCUSDT", "ETHUSDT"])
        assert url.startswith("wss://stream.binance.com:9443/stream?streams=")
        assert "btcusdt@trade" in url
        assert "ethusdt@trade" in url

    def test_subscribe_presence_differs(self):
        assert COINBASE.subscribe is not None
        assert BINANCE.subscribe is None


class TestReplay:
    def test_jsonl_roundtrip(self, tmp_path):
        path = tmp_path / "recording.jsonl"
        lines = [
            json.dumps({"type": "subscriptions"}),
            json.dumps(MATCH_MESSAGE),
            "",
            json.dumps(dict(MATCH_MESSAGE, price="63800.0", trade_id=2)),
        ]
        path.write_text("\n".join(lines))
        trades = list(iter_trades(path))
        assert len(trades) == 2
        assert trades[1].price == pytest.approx(63800.0)


class TestStore:
    def test_bar_insert_is_idempotent(self, db):
        bars = aggregate_minute_bars([make_trade(), make_trade(minute=1)])
        assert store_minute_bars(db, bars) == 2
        assert store_minute_bars(db, bars) == 0

    def test_alerts_are_appended(self, db):
        alert = AlertEvent(
            product="BTC-USD",
            ts=make_trade().ts,
            rule="price_jump",
            message="test",
            value=1.0,
        )
        assert store_alerts(db, [alert]) == 1


class TestClient:
    async def test_subscribes_and_streams_from_local_server(self):
        received_subscriptions = []

        async def fake_feed(ws):
            received_subscriptions.append(json.loads(await ws.recv()))
            for i in range(3):
                await ws.send(json.dumps(dict(MATCH_MESSAGE, trade_id=i)))
            await asyncio.sleep(5)

        async with websockets.serve(fake_feed, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            url = f"ws://127.0.0.1:{port}"
            messages = []
            async for message in stream_messages(["BTC-USD"], 2.0, url=url, use_tls=False):
                messages.append(message)
                if len(messages) == 3:
                    break

        assert received_subscriptions[0]["type"] == "subscribe"
        assert received_subscriptions[0]["product_ids"] == ["BTC-USD"]
        assert [m["trade_id"] for m in messages] == [0, 1, 2]

    async def test_duration_limit_ends_the_stream(self):
        async def silent_feed(ws):
            await ws.recv()
            await asyncio.sleep(10)

        async with websockets.serve(silent_feed, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            url = f"ws://127.0.0.1:{port}"
            messages = [m async for m in stream_messages(["BTC-USD"], 1.0, url=url, use_tls=False)]
        assert messages == []

    def test_subscribe_message_shape(self):
        payload = json.loads(subscribe_message(["BTC-USD", "ETH-USD"]))
        assert payload["channels"] == ["matches"]
        assert payload["product_ids"] == ["BTC-USD", "ETH-USD"]

    async def test_binance_provider_streams_from_local_server(self):
        sent_anything = []

        async def fake_binance(ws):
            # Binance encodes the subscription in the URL, so nothing is
            # sent on connect. Record whether the client sends regardless.
            async def watch_for_sends():
                try:
                    sent_anything.append(await ws.recv())
                except Exception:
                    pass

            asyncio.ensure_future(watch_for_sends())
            for i in range(3):
                wrapped = {"stream": "btcusdt@trade", "data": dict(BINANCE_TRADE, t=i)}
                await ws.send(json.dumps(wrapped))
            await asyncio.sleep(5)

        async with websockets.serve(fake_binance, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            url = f"ws://127.0.0.1:{port}"
            messages = []
            async for message in stream_provider_messages(
                BINANCE, ["BTCUSDT"], 2.0, url=url, use_tls=False
            ):
                trade = BINANCE.parse(message)
                assert trade is not None
                messages.append(trade)
                if len(messages) == 3:
                    break

        assert sent_anything == []
        assert all(t.product == "BTCUSDT" and t.side == "sell" for t in messages)
        assert messages[0].price == pytest.approx(63774.19)
