"""Replay recorded feed messages from a JSONL file.

A recording made with the record CLI command holds one raw feed
message per line. Replaying it runs the exact same pipeline as the
live stream, which keeps demos and tests deterministic and offline.
"""

import json
from collections.abc import Iterator
from pathlib import Path

from streaming.messages import Trade, parse_coinbase_message


def iter_trades(path: str | Path) -> Iterator[Trade]:
    """Yield the trades from a JSONL recording, skipping non-trade lines."""
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            trade = parse_coinbase_message(json.loads(line))
            if trade is not None:
                yield trade
