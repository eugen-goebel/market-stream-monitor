# Market Stream Monitor

![Tests](https://github.com/eugen-goebel/market-stream-monitor/actions/workflows/tests.yml/badge.svg)
![Python](https://img.shields.io/badge/Python-3.12+-blue)
![License](https://img.shields.io/badge/License-MIT-green)

Real-time market monitor that consumes live trades from the public Coinbase Exchange websocket feed, folds them into one-minute bars on the fly, and raises alerts on volume spikes and sudden price moves.

No API key is required. The async client reconnects with exponential backoff when the connection drops, every completed bar is stored idempotently, and a recorded session can be replayed offline through the exact same pipeline, which keeps tests and demos deterministic.

## Quick Start

```bash
git clone https://github.com/eugen-goebel/market-stream-monitor.git
cd market-stream-monitor

# Install dependencies (https://docs.astral.sh/uv/)
uv sync

# Watch the live stream for two minutes, store bars and alerts
uv run main.py monitor BTC-USD ETH-USD --duration 120

# Watch Binance instead, products use the exchange symbol format
uv run main.py monitor BTCUSDT ETHUSDT --provider binance

# Offline demo: replay the bundled recording of real feed messages
uv run main.py replay data/sample-stream.jsonl
```

A second provider, Binance, is supported through `--provider binance` on the `record` and `monitor` commands. A small provider abstraction holds the per-exchange details, the websocket URL, the optional subscribe payload and the message parser, so both feeds run through one ingestion, aggregation and alert pipeline.

```
BTC-USD 2026-06-12 18:40  O 63851  H 63852  L 63843.5  C 63850  vol 0.2784  vwap 63846.44  trades 57
ETH-USD 2026-06-12 18:40  O 1671.8  H 1671.87  L 1671.41  C 1671.87  vol 22.2077  vwap 1671.56  trades 71
...
Replayed 4 bars (4 new) with 0 alerts
```

Record your own session with `uv run main.py record BTC-USD --duration 60 --output data/my-session.jsonl` and query stored data with `uv run main.py bars BTC-USD` and `uv run main.py alerts`.

## Dashboard

```bash
uv run streamlit run app.py
```

The dashboard reads the same database the CLI writes to, so it works as a live monitoring console while a `monitor` process runs in another terminal. It shows a candlestick chart of the latest minute bars with the VWAP overlaid, a per-bar volume chart, metric cards for the selected product and a feed of the most recent alerts. A sidebar toggle drives the auto refresh: when it is on the page sleeps for the chosen interval (five seconds by default) and reruns, so new bars appear without a manual reload.

## How it works

| Stage | What happens |
|-------|--------------|
| Ingest | An async websocket client subscribes to the matches channel and yields one JSON message per executed trade, reconnecting with exponential backoff |
| Aggregate | A tumbling-window aggregator folds trades into one-minute bars per product: open, high, low, close, volume, VWAP and trade count |
| Alert | Completed bars run through two rules: volume above 3x the trailing 20-bar average, and a price move of more than 0.5% within one minute |
| Store | Bars persist idempotently on (product, minute), alerts append, SQLite by default and PostgreSQL via DATABASE_URL |

The aggregation and alert math is tested against hand-computed values, and the websocket client is tested against a local fake feed server, so the full pipeline runs in CI without touching the network.

## Architecture

```
market-stream-monitor/
├── streaming/     # Websocket client, message parsing, JSONL replay
├── processing/    # Minute bar aggregation and alert rules
├── db/            # SQLAlchemy models and idempotent storage
├── data/          # A bundled recording of real feed messages
├── tests/         # 40 tests, run on SQLite and PostgreSQL in CI
├── app.py         # Streamlit dashboard: candlesticks, VWAP, alerts
└── main.py        # CLI: record, monitor, replay, bars, alerts
```

The database connection is configured through `DATABASE_URL`. It defaults to a local SQLite file, PostgreSQL works without code changes:

```bash
export DATABASE_URL=postgresql+psycopg2://user:pass@localhost:5432/stream
```

## Testing

```bash
uv run pytest -v
```

CI runs Ruff, mypy, the test suite with a coverage floor on Python 3.12 and 3.13, the same suite against a real PostgreSQL service container, and CodeQL scanning.

## License

MIT
