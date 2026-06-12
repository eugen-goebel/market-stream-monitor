"""Command line entry point for the market stream monitor.

Examples:
    uv run main.py record BTC-USD --duration 30 --output data/sample-stream.jsonl
    uv run main.py monitor BTC-USD ETH-USD --duration 120
    uv run main.py replay data/sample-stream.jsonl
    uv run main.py bars BTC-USD --limit 10
    uv run main.py alerts --limit 10
"""

import argparse
import asyncio
import json
import sys
from typing import TYPE_CHECKING

from db.database import SessionLocal, init_db

if TYPE_CHECKING:
    from db.models import StoredBar
    from processing.bars import MinuteBar


def _print_bar(bar: "MinuteBar | StoredBar") -> None:
    print(
        f"{bar.product} {bar.minute:%Y-%m-%d %H:%M}  "
        f"O {bar.open:<10g} H {bar.high:<10g} L {bar.low:<10g} C {bar.close:<10g} "
        f"vol {bar.volume:.4f}  vwap {bar.vwap:.2f}  trades {bar.trade_count}"
    )


def cmd_record(products: list[str], duration: float, output: str) -> int:
    from streaming.client import stream_messages

    async def run() -> int:
        count = 0
        with open(output, "w", encoding="utf-8") as handle:
            async for message in stream_messages(products, duration):
                handle.write(json.dumps(message) + "\n")
                count += 1
        return count

    count = asyncio.run(run())
    print(f"Recorded {count} messages to {output}")
    return 0


def cmd_monitor(products: list[str], duration: float) -> int:
    from processing.alerts import AlertEngine
    from processing.bars import BarAggregator
    from streaming.client import stream_messages
    from streaming.messages import parse_coinbase_message

    init_db()
    db = SessionLocal()
    aggregator = BarAggregator()
    engine = AlertEngine()

    async def run() -> tuple[int, int]:
        from db.store import store_alerts, store_minute_bars

        bar_count = 0
        alert_count = 0
        async for message in stream_messages(products, duration):
            trade = parse_coinbase_message(message)
            if trade is None:
                continue
            for bar in aggregator.add(trade):
                _print_bar(bar)
                store_minute_bars(db, [bar])
                bar_count += 1
                alerts = engine.on_bar(bar)
                for alert in alerts:
                    print(f"  ALERT {alert.rule}: {alert.message}")
                store_alerts(db, alerts)
                alert_count += len(alerts)
        return bar_count, alert_count

    try:
        bar_count, alert_count = asyncio.run(run())
    finally:
        db.close()
    print(f"Stored {bar_count} completed bars and {alert_count} alerts")
    return 0


def cmd_replay(path: str) -> int:
    from db.store import store_alerts, store_minute_bars
    from processing.alerts import AlertEngine
    from processing.bars import aggregate_minute_bars
    from streaming.replay import iter_trades

    init_db()
    db = SessionLocal()
    engine = AlertEngine()
    try:
        bars = aggregate_minute_bars(iter_trades(path))
        alerts = []
        for bar in bars:
            _print_bar(bar)
            alerts.extend(engine.on_bar(bar))
        inserted = store_minute_bars(db, bars)
        for alert in alerts:
            print(f"  ALERT {alert.rule}: {alert.message}")
        store_alerts(db, alerts)
    finally:
        db.close()
    print(f"Replayed {len(bars)} bars ({inserted} new) with {len(alerts)} alerts")
    return 0


def cmd_bars(product: str, limit: int) -> int:
    from sqlalchemy import select

    from db.models import StoredBar

    init_db()
    db = SessionLocal()
    try:
        rows = db.scalars(
            select(StoredBar)
            .where(StoredBar.product == product)
            .order_by(StoredBar.minute.desc())
            .limit(limit)
        ).all()
    finally:
        db.close()
    if not rows:
        print(f"No bars stored for {product}")
        return 1
    for row in reversed(rows):
        _print_bar(row)
    return 0


def cmd_alerts(limit: int) -> int:
    from sqlalchemy import select

    from db.models import StoredAlert

    init_db()
    db = SessionLocal()
    try:
        rows = db.scalars(select(StoredAlert).order_by(StoredAlert.ts.desc()).limit(limit)).all()
    finally:
        db.close()
    if not rows:
        print("No alerts stored")
        return 0
    for row in reversed(rows):
        print(f"{row.ts:%Y-%m-%d %H:%M} {row.rule:<14} {row.message}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Market stream monitor")
    sub = parser.add_subparsers(dest="command", required=True)

    p_record = sub.add_parser("record", help="Record raw feed messages to a JSONL file")
    p_record.add_argument("products", nargs="+", help="Products, e.g. BTC-USD ETH-USD")
    p_record.add_argument("--duration", type=float, default=30.0)
    p_record.add_argument("--output", default="data/recording.jsonl")

    p_monitor = sub.add_parser("monitor", help="Watch the live stream, store bars and alerts")
    p_monitor.add_argument("products", nargs="+", help="Products, e.g. BTC-USD ETH-USD")
    p_monitor.add_argument("--duration", type=float, default=120.0)

    p_replay = sub.add_parser("replay", help="Run the pipeline over a recorded JSONL file")
    p_replay.add_argument("path")

    p_bars = sub.add_parser("bars", help="Show stored minute bars for a product")
    p_bars.add_argument("product")
    p_bars.add_argument("--limit", type=int, default=10)

    p_alerts = sub.add_parser("alerts", help="Show stored alerts")
    p_alerts.add_argument("--limit", type=int, default=10)

    args = parser.parse_args()
    if args.command == "record":
        return cmd_record(args.products, args.duration, args.output)
    if args.command == "monitor":
        return cmd_monitor(args.products, args.duration)
    if args.command == "replay":
        return cmd_replay(args.path)
    if args.command == "bars":
        return cmd_bars(args.product, args.limit)
    return cmd_alerts(args.limit)


if __name__ == "__main__":
    sys.exit(main())
