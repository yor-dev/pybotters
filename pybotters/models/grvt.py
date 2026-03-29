from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ..store import DataStore, DataStoreCollection

if TYPE_CHECKING:
    from ..typedefs import Item
    from ..ws import ClientWebSocketResponse

logger = logging.getLogger(__name__)


class GRVTDataStore(DataStoreCollection):
    """GRVT の DataStoreCollection クラス

    https://api-docs.grvt.io/market_data_streams/
    """

    def _init(self) -> None:
        self._create("ticker", datastore_class=Ticker)
        self._create("book", datastore_class=Book)
        self._create("trade", datastore_class=Trade)

    def _onmessage(self, msg: Item, ws: ClientWebSocketResponse | None = None) -> None:
        stream: str = msg.get("stream", "")

        if not stream:
            # 購読応答等はスキップ
            return

        feed = msg.get("feed")
        if feed is None:
            return

        if stream.startswith("v1.ticker"):
            self["ticker"]._onmessage(msg)
        elif stream.startswith("v1.book"):
            self["book"]._onmessage(msg)
        elif stream == "v1.trade":
            self["trade"]._onmessage(msg)

    @property
    def ticker(self) -> Ticker:
        """ticker stream.

        https://api-docs.grvt.io/market_data_streams/
        """
        return self._get("ticker", Ticker)

    @property
    def book(self) -> Book:
        """book stream.

        https://api-docs.grvt.io/market_data_streams/
        """
        return self._get("book", Book)

    @property
    def trade(self) -> Trade:
        """trade stream.

        https://api-docs.grvt.io/market_data_streams/
        """
        return self._get("trade", Trade)


class Ticker(DataStore):
    """Ticker including mark_price, index_price, funding_rate, OI."""

    _KEYS = ["instrument"]

    def _onmessage(self, msg: Item) -> None:
        feed = msg["feed"]
        if isinstance(feed, list):
            self._update(feed)
        else:
            self._update([feed])


class Book(DataStore):
    """Order book with snapshot and delta updates."""

    _KEYS = ["instrument", "side", "price"]

    def _onmessage(self, msg: Item) -> None:
        feed = msg["feed"]
        # sequence_number "0" = snapshot
        is_snapshot = str(msg.get("sequence_number", "")) == "0"

        if isinstance(feed, list):
            entries = feed
        else:
            entries = [feed]

        for entry in entries:
            instrument = entry.get("instrument", "")

            if is_snapshot:
                self._delete(self.find({"instrument": instrument}))

            operation: dict[str, list[Item]] = {"delete": [], "upsert": []}

            for side_key, side_name in (("asks", "ask"), ("bids", "bid")):
                for level in entry.get(side_key, []):
                    price = level["price"]
                    size = level["size"]
                    item = {
                        "instrument": instrument,
                        "side": side_name,
                        "price": price,
                        "size": size,
                    }
                    if not is_snapshot and size == "0":
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
        feed = msg["feed"]
        if isinstance(feed, list):
            self._insert(feed)
        else:
            self._insert([feed])
