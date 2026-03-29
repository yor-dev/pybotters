from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from ..store import DataStore, DataStoreCollection

if TYPE_CHECKING:
    from ..typedefs import Item
    from ..ws import ClientWebSocketResponse

logger = logging.getLogger(__name__)


class EdgeXDataStore(DataStoreCollection):
    """EdgeX の DataStoreCollection クラス

    https://edgex-1.gitbook.io/edgeX-documentation/api/websocket-api
    """

    def _init(self) -> None:
        self._create("ticker", datastore_class=Ticker)
        self._create("depth", datastore_class=Depth)
        self._create("trades", datastore_class=Trades)

    def _onmessage(self, msg: Item, ws: ClientWebSocketResponse | None = None) -> None:
        type_ = msg.get("type", "")

        # サーバーからのpingに対してpongを返す
        if type_ == "ping":
            if ws is not None:
                asyncio.ensure_future(
                    ws.send_json({"type": "pong", "time": msg.get("time", "")})
                )
            return

        if type_ != "quote-event":
            return

        channel: str = msg.get("channel", "")
        content: dict = msg.get("content", {})

        if channel.startswith("ticker"):
            self["ticker"]._onmessage(content)
        elif channel.startswith("depth"):
            self["depth"]._onmessage(content)
        elif channel.startswith("trades"):
            self["trades"]._onmessage(content)

    @property
    def ticker(self) -> Ticker:
        """ticker channel.

        https://edgex-1.gitbook.io/edgeX-documentation/api/websocket-api#ticker
        """
        return self._get("ticker", Ticker)

    @property
    def depth(self) -> Depth:
        """depth channel.

        https://edgex-1.gitbook.io/edgeX-documentation/api/websocket-api#depth
        """
        return self._get("depth", Depth)

    @property
    def trades(self) -> Trades:
        """trades channel.

        https://edgex-1.gitbook.io/edgeX-documentation/api/websocket-api#trades
        """
        return self._get("trades", Trades)


class Ticker(DataStore):
    """Ticker including lastPrice, fundingRate, indexPrice, OI."""

    _KEYS = ["contractId"]

    def _onmessage(self, content: Item) -> None:
        self._update(content.get("data", []))


class Depth(DataStore):
    """Order book with snapshot and diff updates."""

    _KEYS = ["contractId", "side", "price"]

    def _onmessage(self, content: Item) -> None:
        for entry in content.get("data", []):
            contract_id = entry["contractId"]
            is_snapshot = entry.get("depthType") == "SNAPSHOT"

            if is_snapshot:
                self._delete(self.find({"contractId": contract_id}))

            operation: dict[str, list[Item]] = {"delete": [], "upsert": []}

            for side_key, side_name in (("asks", "ask"), ("bids", "bid")):
                for level in entry.get(side_key, []):
                    price = level["price"]
                    size = level["size"]
                    item = {
                        "contractId": contract_id,
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


class Trades(DataStore):
    """Recent trades."""

    _MAXLEN = 99999

    def _onmessage(self, content: Item) -> None:
        self._insert(content.get("data", []))
