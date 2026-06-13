"""Provider registry that drives one pipeline across exchange feeds.

Each Provider bundles the few things that differ between exchanges: a
display name, how to build the websocket URL from a list of products,
an optional subscribe payload, and how to parse a raw message into a
Trade. Coinbase subscribes in-band on the matches channel, so it has a
subscribe function. Binance encodes the subscription in the URL path,
so its subscribe is None and nothing is sent after connecting. The
client and the CLI read a Provider instead of branching on the
exchange, which keeps a single ingestion pipeline for both feeds.
"""

from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Any

from streaming.client import FEED_URL, NO_SUBSCRIBE, stream_messages, subscribe_message
from streaming.messages import Trade, parse_binance_message, parse_coinbase_message

BINANCE_STREAM_URL = "wss://stream.binance.com:9443/stream"


@dataclass(frozen=True)
class Provider:
    name: str
    build_url: Callable[[list[str]], str]
    subscribe: Callable[[list[str]], str] | None
    parse: Callable[[dict], Trade | None]


def _binance_url(products: list[str]) -> str:
    """Build the combined-stream URL with one trade stream per product."""
    streams = "/".join(f"{product.lower()}@trade" for product in products)
    return f"{BINANCE_STREAM_URL}?streams={streams}"


COINBASE = Provider(
    name="Coinbase",
    build_url=lambda products: FEED_URL,
    subscribe=subscribe_message,
    parse=parse_coinbase_message,
)

BINANCE = Provider(
    name="Binance",
    build_url=_binance_url,
    subscribe=None,
    parse=parse_binance_message,
)

PROVIDERS: dict[str, Provider] = {"coinbase": COINBASE, "binance": BINANCE}


def stream_provider_messages(
    provider: Provider,
    products: list[str],
    duration: float | None = None,
    url: str | None = None,
    use_tls: bool = True,
) -> AsyncIterator[dict[str, Any]]:
    """Stream raw messages for a provider, reusing the shared client.

    The provider builds the URL from the products unless an explicit
    url override is passed, which tests use to point at a local server.
    A provider without a subscribe function sends nothing on connect.
    """
    feed_url = provider.build_url(products) if url is None else url
    subscribe = provider.subscribe(products) if provider.subscribe is not None else NO_SUBSCRIBE
    return stream_messages(products, duration, url=feed_url, use_tls=use_tls, subscribe=subscribe)
