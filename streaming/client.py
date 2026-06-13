"""Async websocket client for public exchange feeds.

No API key is required. The client yields raw JSON messages,
reconnects with exponential backoff when the connection drops, and
stops cleanly after an optional duration. Providers that subscribe
in-band, like Coinbase with its matches channel, pass a subscribe
payload that is sent right after connecting. Providers that encode
the subscription in the URL, like Binance, pass subscribe=None so
nothing is sent. TLS verification uses the certifi bundle so it works
on systems whose Python lacks a wired-up trust store.
"""

import asyncio
import json
import ssl
import time
from collections.abc import AsyncIterator
from typing import Any

import certifi
import websockets

FEED_URL = "wss://ws-feed.exchange.coinbase.com"
SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())
MAX_BACKOFF_SECONDS = 30.0

# Sentinel for "send no subscribe payload" so None can keep meaning
# "send the default Coinbase subscription for these products".
NO_SUBSCRIBE = ""


def subscribe_message(products: list[str]) -> str:
    return json.dumps({"type": "subscribe", "product_ids": products, "channels": ["matches"]})


async def stream_messages(
    products: list[str],
    duration: float | None = None,
    url: str = FEED_URL,
    use_tls: bool = True,
    subscribe: str | None = None,
) -> AsyncIterator[dict[str, Any]]:
    """Yield raw feed messages, reconnecting on connection loss.

    With a duration in seconds the stream ends on its own, otherwise it
    runs until cancelled. By default the Coinbase matches subscription
    for the given products is sent after connecting. Passing an explicit
    subscribe payload overrides it, and subscribe=NO_SUBSCRIBE sends
    nothing, for providers that encode the subscription in the URL. The
    url and use_tls knobs exist so tests can point the client at a local
    plaintext server.
    """
    payload = subscribe_message(products) if subscribe is None else subscribe
    deadline = time.monotonic() + duration if duration is not None else None
    backoff = 1.0

    while deadline is None or time.monotonic() < deadline:
        try:
            ssl_context = SSL_CONTEXT if use_tls else None
            async with websockets.connect(url, open_timeout=10, ssl=ssl_context) as ws:
                if payload:
                    await ws.send(payload)
                backoff = 1.0
                while deadline is None or time.monotonic() < deadline:
                    remaining = None if deadline is None else max(deadline - time.monotonic(), 0.1)
                    try:
                        async with asyncio.timeout(remaining):
                            raw = await ws.recv()
                    except TimeoutError:
                        return
                    message: dict[str, Any] = json.loads(raw)
                    yield message
        except asyncio.CancelledError:
            raise
        except Exception:
            if deadline is not None and time.monotonic() >= deadline:
                return
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
