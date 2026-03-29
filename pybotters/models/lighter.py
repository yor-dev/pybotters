from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ..store import DataStore, DataStoreCollection

if TYPE_CHECKING:
    from ..typedefs import Item
    from ..ws import ClientWebSocketResponse

logger = logging.getLogger(__name__)


class LighterDataStore(DataStoreCollection):
    """Lighter の DataStoreCollection クラス

    https://apidocs.lighter.xyz/docs/websocket-reference
    """

    def _init(self) -> None:
        self._create("ticker", datastore_class=Ticker)
        self._create("market_stats", datastore_class=MarketStats)
        self._create("order_book", datastore_class=OrderBook)
        self._create("trade", datastore_class=Trade)

    def _onmessage(self, msg: Item, ws: ClientWebSocketResponse | None = None) -> None:
        type_: str = msg.get("type", "")

        if type_.startswith("error"):
            logger.warning(msg)
            return

        if type_ == "update/ticker":
            self["ticker"]._onmessage(msg)
        elif type_ == "update/market_stats":
            self["market_stats"]._onmessage(msg)
        elif type_ == "update/order_book":
            self["order_book"]._onmessage(msg)
        elif type_ == "update/trade":
            self["trade"]._onmessage(msg)

    @property
    def ticker(self) -> Ticker:
        """ticker channel (BBO).

        https://apidocs.lighter.xyz/docs/websocket-reference#ticker
        """
        return self._get("ticker", Ticker)

    @property
    def market_stats(self) -> MarketStats:
        """market_stats channel.

        https://apidocs.lighter.xyz/docs/websocket-reference#market-stats
        """
        return self._get("market_stats", MarketStats)

    @property
    def order_book(self) -> OrderBook:
        """order_book channel.

        https://apidocs.lighter.xyz/docs/websocket-reference#order-book
        """
        return self._get("order_book", OrderBook)

    @property
    def trade(self) -> Trade:
        """trade channel.

        https://apidocs.lighter.xyz/docs/websocket-reference#trade
        """
        return self._get("trade", Trade)


class Ticker(DataStore):
    """Best Bid/Offer per symbol."""

    _KEYS = ["s"]

    def _onmessage(self, msg: Item) -> None:
        self._update([msg["ticker"]])


class MarketStats(DataStore):
    """Market statistics including funding rate, mark price, OI."""

    _KEYS = ["market_id"]

    def _onmessage(self, msg: Item) -> None:
        data = msg["market_stats"]
        # market_stats/all: {"0": {...}, "1": {...}, ...}
        # market_stats/{id}: {"symbol": "ETH", "market_id": 0, ...}
        if "market_id" in data:
            self._update([data])
        else:
            self._update(list(data.values()))


class OrderBook(DataStore):
    """Order book with diff updates.

    初回購読時にスナップショットが送信され、以後は差分更新が送られる。
    スナップショットと差分は同じ type ("update/order_book") だが、
    初回は begin_nonce が含まれる。
    """

    _KEYS = ["market_id", "side", "price"]

    def _onmessage(self, msg: Item) -> None:
        channel: str = msg["channel"]
        market_id = int(channel.split(":")[1])
        book = msg["order_book"]

        # begin_nonce の存在でスナップショットを判定
        is_snapshot = "begin_nonce" in book

        if is_snapshot:
            self._delete(self.find({"market_id": market_id}))

        operation: dict[str, list[Item]] = {"delete": [], "upsert": []}

        for side_key, side_name in (("asks", "ask"), ("bids", "bid")):
            for entry in book.get(side_key, []):
                item = {
                    "market_id": market_id,
                    "side": side_name,
                    "price": entry["price"],
                    "size": entry["size"],
                }
                if not is_snapshot and entry["size"] == "0":
                    operation["delete"].append(item)
                else:
                    operation["upsert"].append(item)

        self._delete(operation["delete"])
        if is_snapshot:
            self._insert(operation["upsert"])
        else:
            self._update(operation["upsert"])

    def sorted(
        self, query: Item | None = None, limit: int | None = None
    ) -> dict[str, list[Item]]:
        return self._sorted(
            item_key="side",
            item_asc_key="ask",
            item_desc_key="bid",
            sort_key="price",
            query=query,
            limit=limit,
        )


class Trade(DataStore):
    """Recent trades."""

    _MAXLEN = 99999

    def _onmessage(self, msg: Item) -> None:
        self._insert(msg["trades"])
