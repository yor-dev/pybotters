from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, NamedTuple

import pytest
import pytest_asyncio
from aiohttp import web
from aiohttp.test_utils import TestServer
from yarl import URL

import pybotters

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from typing import Any

    from pybotters.typedefs import Item


class ParamArg(NamedTuple):
    test_input: Any
    expected: Any


@dataclass
class StoreArg:
    name: str
    data: Any


def test_bitgetv2_positions() -> None:
    """Check the behavior of BitgetV2DataStore.positions."""
    store = pybotters.BitgetV2DataStore()
    ws: Any = object()

    # Initial messages
    message_list = [
        {
            "event": "subscribe",
            "arg": {
                "instType": "USDT-FUTURES",
                "channel": "positions",
                "instId": "default",
            },
        },
        {
            "action": "snapshot",
            "arg": {
                "instType": "USDT-FUTURES",
                "channel": "positions",
                "instId": "default",
            },
            "data": [],
            "ts": 1741850535818,
        },
    ]
    for message in message_list:
        store.onmessage(message, ws)

    assert store.positions.find() == []

    # Open the position
    message = {
        "action": "snapshot",
        "arg": {
            "instType": "USDT-FUTURES",
            "channel": "positions",
            "instId": "default",
        },
        "data": [
            {
                "posId": "1244249778959294501",
                "instId": "XRPUSDT",
                "marginCoin": "USDT",
                "marginSize": "0.67098",
                "marginMode": "crossed",
                "holdSide": "long",
                "posMode": "one_way_mode",
                "total": "3",
                "available": "3",
                "frozen": "0",
                "openPriceAvg": "2.2366",
                "leverage": 10,
                "achievedProfits": "0",
                "unrealizedPL": "0.0009",
                "unrealizedPLR": "0.001341321649",
                "liquidationPrice": "-5.882918946895",
                "keepMarginRate": "0.0040",
                "marginRate": "0.001265633757",
                "autoMargin": "off",
                "breakEvenPrice": "2.238882701621",
                "deductedFee": "0.002818116",
                "totalFee": "",
                "cTime": "1732378185023",
                "uTime": "1741850543856",
            }
        ],
        "ts": 1741850543864,
    }
    store.onmessage(message, ws)

    assert store.positions.find() == [
        {
            "instType": "USDT-FUTURES",
            "posId": "1244249778959294501",
            "instId": "XRPUSDT",
            "marginCoin": "USDT",
            "marginSize": "0.67098",
            "marginMode": "crossed",
            "holdSide": "long",
            "posMode": "one_way_mode",
            "total": "3",
            "available": "3",
            "frozen": "0",
            "openPriceAvg": "2.2366",
            "leverage": 10,
            "achievedProfits": "0",
            "unrealizedPL": "0.0009",
            "unrealizedPLR": "0.001341321649",
            "liquidationPrice": "-5.882918946895",
            "keepMarginRate": "0.0040",
            "marginRate": "0.001265633757",
            "autoMargin": "off",
            "breakEvenPrice": "2.238882701621",
            "deductedFee": "0.002818116",
            "totalFee": "",
            "cTime": "1732378185023",
            "uTime": "1741850543856",
        }
    ]

    # Update the position
    message_list = [
        {
            "action": "snapshot",
            "arg": {
                "instType": "USDT-FUTURES",
                "channel": "positions",
                "instId": "default",
            },
            "data": [
                {
                    "posId": "1244249778959294501",
                    "instId": "XRPUSDT",
                    "marginCoin": "USDT",
                    "marginSize": "0.67098",
                    "marginMode": "crossed",
                    "holdSide": "long",
                    "posMode": "one_way_mode",
                    "total": "3",
                    "available": "3",
                    "frozen": "0",
                    "openPriceAvg": "2.2366",
                    "leverage": 10,
                    "achievedProfits": "0",
                    "unrealizedPL": "0.0018",
                    "unrealizedPLR": "0.002682643298",
                    "liquidationPrice": "-5.872646147358",
                    "keepMarginRate": "0.0040",
                    "marginRate": "0.002529090521",
                    "autoMargin": "off",
                    "breakEvenPrice": "2.238882701621",
                    "deductedFee": "0.002818116",
                    "totalFee": "",
                    "cTime": "1732378185023",
                    "uTime": "1741850552269",
                }
            ],
            "ts": 1741850552278,
        },
        {
            "action": "snapshot",
            "arg": {
                "instType": "USDT-FUTURES",
                "channel": "positions",
                "instId": "default",
            },
            "data": [
                {
                    "posId": "1244249778959294501",
                    "instId": "XRPUSDT",
                    "marginCoin": "USDT",
                    "marginSize": "1.34205",
                    "marginMode": "crossed",
                    "holdSide": "long",
                    "posMode": "one_way_mode",
                    "total": "6",
                    "available": "6",
                    "frozen": "0",
                    "openPriceAvg": "2.23675",
                    "leverage": 10,
                    "achievedProfits": "0",
                    "unrealizedPL": "0.0027",
                    "unrealizedPLR": "0.002011847547",
                    "liquidationPrice": "-1.817393474448",
                    "keepMarginRate": "0.0040",
                    "marginRate": "0.002531712712",
                    "autoMargin": "off",
                    "breakEvenPrice": "2.239032854713",
                    "deductedFee": "0.00563661",
                    "totalFee": "",
                    "cTime": "1732378185023",
                    "uTime": "1741850552397",
                }
            ],
            "ts": 1741850552406,
        },
    ]
    for message in message_list:
        store.onmessage(message, ws)

    assert store.positions.find() == [
        {
            "instType": "USDT-FUTURES",
            "posId": "1244249778959294501",
            "instId": "XRPUSDT",
            "marginCoin": "USDT",
            "marginSize": "1.34205",
            "marginMode": "crossed",
            "holdSide": "long",
            "posMode": "one_way_mode",
            "total": "6",
            "available": "6",
            "frozen": "0",
            "openPriceAvg": "2.23675",
            "leverage": 10,
            "achievedProfits": "0",
            "unrealizedPL": "0.0027",
            "unrealizedPLR": "0.002011847547",
            "liquidationPrice": "-1.817393474448",
            "keepMarginRate": "0.0040",
            "marginRate": "0.002531712712",
            "autoMargin": "off",
            "breakEvenPrice": "2.239032854713",
            "deductedFee": "0.00563661",
            "totalFee": "",
            "cTime": "1732378185023",
            "uTime": "1741850552397",
        }
    ]

    # Close the position
    message_list = [
        {
            "action": "snapshot",
            "arg": {
                "instType": "USDT-FUTURES",
                "channel": "positions",
                "instId": "default",
            },
            "data": [
                {
                    "posId": "1244249778959294501",
                    "instId": "XRPUSDT",
                    "marginCoin": "USDT",
                    "marginSize": "1.34205",
                    "marginMode": "crossed",
                    "holdSide": "long",
                    "posMode": "one_way_mode",
                    "total": "6",
                    "available": "6",
                    "frozen": "0",
                    "openPriceAvg": "2.23675",
                    "leverage": 10,
                    "achievedProfits": "0",
                    "unrealizedPL": "0.0159",
                    "unrealizedPLR": "0.011847546664",
                    "liquidationPrice": "-1.817383354448",
                    "keepMarginRate": "0.0040",
                    "marginRate": "0.002532831507",
                    "autoMargin": "off",
                    "breakEvenPrice": "2.239032854713",
                    "deductedFee": "0.00563661",
                    "totalFee": "",
                    "cTime": "1732378185023",
                    "uTime": "1741850552397",
                }
            ],
            "ts": 1741850567305,
        },
        {
            "action": "snapshot",
            "arg": {
                "instType": "USDT-FUTURES",
                "channel": "positions",
                "instId": "default",
            },
            "data": [],
            "ts": 1741850567434,
        },
    ]
    for message in message_list:
        store.onmessage(message, ws)

    assert store.positions.find() == []


@pytest.mark.parametrize(
    "test_input,expected",
    [
        pytest.param(
            # test_input
            (
                # msg
                [
                    {
                        "method": "asset_update",
                        "params": [
                            {
                                "asset": "xrp",
                                "amount_precision": 6,
                                "free_amount": "0.000000",
                                "locked_amount": "0.000000",
                                "onhand_amount": "0.000000",
                                "withdrawing_amount": "0.000000",
                            },
                            {
                                "asset": "jpy",
                                "amount_precision": 4,
                                "free_amount": "1000000.0000",
                                "locked_amount": "0.0000",
                                "onhand_amount": "1000000.0100",
                                "withdrawing_amount": "0.0000",
                            },
                        ],
                    },
                    {
                        "method": "asset_update",
                        "params": [
                            {
                                "asset": "jpy",
                                "amount_precision": 4,
                                "free_amount": "1000000.0000",
                                "locked_amount": "0.0000",
                                "onhand_amount": "1000000.0000",
                                "withdrawing_amount": "0.0000",
                            },
                            {
                                "asset": "xrp",
                                "amount_precision": 6,
                                "free_amount": "0.000100",
                                "locked_amount": "0.000000",
                                "onhand_amount": "0.000100",
                                "withdrawing_amount": "0.000000",
                            },
                        ],
                    },
                ],
                # name
                "asset",
                # query
                {"asset": "xrp"},
            ),
            # expected
            {
                "asset": "xrp",
                "amount_precision": 6,
                "free_amount": "0.000100",
                "locked_amount": "0.000000",
                "onhand_amount": "0.000100",
                "withdrawing_amount": "0.000000",
            },
            id="asset",
        ),
        pytest.param(
            # test_input
            (
                # msg
                [
                    {
                        "method": "spot_trade",
                        "params": [
                            {
                                "amount": "0.0001",
                                "executed_at": 1742545716306,
                                "fee_amount_base": "0.000000",
                                "fee_amount_quote": "0.0000",
                                "fee_occurred_amount_quote": "0.0000",
                                "maker_taker": "taker",
                                "order_id": 44280655864,
                                "pair": "xrp_jpy",
                                "price": "359.003",
                                "position_side": None,
                                "side": "buy",
                                "trade_id": 1396696263,
                                "type": "market",
                                "profit_loss": None,
                                "interest": None,
                            }
                        ],
                    },
                    {
                        "method": "spot_trade",
                        "params": [
                            {
                                "amount": "0.0001",
                                "executed_at": 1742545746457,
                                "fee_amount_base": "0.000000",
                                "fee_amount_quote": "0.0000",
                                "fee_occurred_amount_quote": "0.0000",
                                "maker_taker": "taker",
                                "order_id": 44280665790,
                                "pair": "xrp_jpy",
                                "price": "359.003",
                                "position_side": None,
                                "side": "sell",
                                "trade_id": 1396696377,
                                "type": "market",
                                "profit_loss": None,
                                "interest": None,
                            }
                        ],
                    },
                ],
                # name
                "spot_trade",
                # query
                {"trade_id": 1396696377},
            ),
            # expected
            {
                "amount": "0.0001",
                "executed_at": 1742545746457,
                "fee_amount_base": "0.000000",
                "fee_amount_quote": "0.0000",
                "fee_occurred_amount_quote": "0.0000",
                "maker_taker": "taker",
                "order_id": 44280665790,
                "pair": "xrp_jpy",
                "price": "359.003",
                "position_side": None,
                "side": "sell",
                "trade_id": 1396696377,
                "type": "market",
                "profit_loss": None,
                "interest": None,
            },
            id="spot_trade",
        ),
        pytest.param(
            # test_input
            (
                # msg
                [
                    {
                        "method": "dealer_order_new",
                        "params": [
                            {
                                "order_id": "44280314922",
                                "asset": "xrp",
                                "side": "sell",
                                "price": "352.372",
                                "amount": "0.000100",
                                "ordered_at": 1742544784403,
                            }
                        ],
                    }
                ],
                # name
                "dealer_order",
                # query
                {"order_id": "44280314922"},
            ),
            # expected
            {
                "order_id": "44280314922",
                "asset": "xrp",
                "side": "sell",
                "price": "352.372",
                "amount": "0.000100",
                "ordered_at": 1742544784403,
            },
            id="dealer_order",
        ),
        pytest.param(
            # test_input
            (
                # msg
                [
                    {
                        "method": "margin_position_update",
                        "params": [
                            {
                                "pair": "xrp_jpy",
                                "position_side": "short",
                                "open": "0.0001",
                                "locked": "0.0000",
                                "product": "0.0360",
                                "average_price": "360.007",
                                "unrealized_fee": "0.0000",
                                "unrealized_interest": "0.0000",
                            }
                        ],
                    },
                    {
                        "method": "margin_position_update",
                        "params": [
                            {
                                "pair": "xrp_jpy",
                                "position_side": "short",
                                "open": "0.0000",
                                "locked": "0.0000",
                                "product": "0.0000",
                                "average_price": "0",
                                "unrealized_fee": "0.0000",
                                "unrealized_interest": "0.0000",
                            }
                        ],
                    },
                ],
                # name
                "margin_position",
                # query
                {"pair": "xrp_jpy", "position_side": "short"},
            ),
            # expected
            {
                "pair": "xrp_jpy",
                "position_side": "short",
                "open": "0.0000",
                "locked": "0.0000",
                "product": "0.0000",
                "average_price": "0",
                "unrealized_fee": "0.0000",
                "unrealized_interest": "0.0000",
            },
            id="margin_position",
        ),
    ],
)
def test_bitbank_private_basic_stores(
    test_input: tuple[list[Any], str, Item], expected: Item
) -> None:
    """Tests for bitbankPrivateDataStore other than spot_order."""
    messages, name, query = test_input

    store = pybotters.bitbankPrivateDataStore()
    for msg in messages:
        store.onmessage(msg)

    s = store[name]
    assert s is not None
    assert s.get(query) == expected


def test_bitbank_private_order() -> None:
    """Tests for bitbankPrivateDataStore spot_order only."""
    store = pybotters.bitbankPrivateDataStore()

    msg: Any
    # Create an order
    msg = {
        "method": "spot_order_new",
        "params": [
            {
                "average_price": "0.000",
                "executed_amount": "0.0000",
                "order_id": 44280998810,
                "ordered_at": 1742546617276,
                "pair": "xrp_jpy",
                "price": "359.412",
                "remaining_amount": "0.0002",
                "side": "buy",
                "start_amount": "0.0002",
                "status": "UNFILLED",
                "type": "limit",
                "expire_at": 1758098617276,
                "post_only": True,
                "user_cancelable": True,
            }
        ],
    }
    store.onmessage(msg)

    assert store.spot_order.find() == [
        {
            "average_price": "0.000",
            "executed_amount": "0.0000",
            "order_id": 44280998810,
            "ordered_at": 1742546617276,
            "pair": "xrp_jpy",
            "price": "359.412",
            "remaining_amount": "0.0002",
            "side": "buy",
            "start_amount": "0.0002",
            "status": "UNFILLED",
            "type": "limit",
            "expire_at": 1758098617276,
            "post_only": True,
            "user_cancelable": True,
        }
    ]

    # Create anoter order
    msg = {
        "method": "spot_order_new",
        "params": [
            {
                "average_price": "0.000",
                "executed_amount": "0.0000",
                "order_id": 44280888880,
                "ordered_at": 1742546316395,
                "pair": "xrp_jpy",
                "price": "354.426",
                "remaining_amount": "0.0001",
                "side": "buy",
                "start_amount": "0.0001",
                "status": "UNFILLED",
                "type": "limit",
                "expire_at": 1758098316395,
                "post_only": True,
                "user_cancelable": True,
            }
        ],
    }
    store.onmessage(msg)

    assert store.spot_order.get({"order_id": 44280888880}) == {
        "average_price": "0.000",
        "executed_amount": "0.0000",
        "order_id": 44280888880,
        "ordered_at": 1742546316395,
        "pair": "xrp_jpy",
        "price": "354.426",
        "remaining_amount": "0.0001",
        "side": "buy",
        "start_amount": "0.0001",
        "status": "UNFILLED",
        "type": "limit",
        "expire_at": 1758098316395,
        "post_only": True,
        "user_cancelable": True,
    }

    # Canceled
    msg = {
        "method": "spot_order",
        "params": [
            {
                "average_price": "0.000",
                "canceled_at": 1742546533125,
                "executed_amount": "0.0000",
                "expire_at": 1758098316395,
                "order_id": 44280888880,
                "ordered_at": 1742546316395,
                "pair": "xrp_jpy",
                "price": "354.426",
                "remaining_amount": "0.0001",
                "side": "buy",
                "start_amount": "0.0001",
                "status": "CANCELED_UNFILLED",
                "type": "limit",
                "post_only": True,
                "user_cancelable": True,
            }
        ],
    }
    store.onmessage(msg)

    assert store.spot_order.find() == [
        {
            "average_price": "0.000",
            "executed_amount": "0.0000",
            "order_id": 44280998810,
            "ordered_at": 1742546617276,
            "pair": "xrp_jpy",
            "price": "359.412",
            "remaining_amount": "0.0002",
            "side": "buy",
            "start_amount": "0.0002",
            "status": "UNFILLED",
            "type": "limit",
            "expire_at": 1758098617276,
            "post_only": True,
            "user_cancelable": True,
        }
    ]

    # Create anoter order (for spot_order_invalidation)
    msg = {
        "method": "spot_order_new",
        "params": [
            {
                "average_price": "0.000",
                "executed_amount": "0.0000",
                "order_id": 44280888880,
                "ordered_at": 1742546316395,
                "pair": "xrp_jpy",
                "price": "354.426",
                "remaining_amount": "0.0001",
                "side": "buy",
                "start_amount": "0.0001",
                "status": "UNFILLED",
                "type": "limit",
                "expire_at": 1758098316395,
                "post_only": True,
                "user_cancelable": True,
            }
        ],
    }
    store.onmessage(msg)

    assert store.spot_order.get({"order_id": 44280888880}) == {
        "average_price": "0.000",
        "executed_amount": "0.0000",
        "order_id": 44280888880,
        "ordered_at": 1742546316395,
        "pair": "xrp_jpy",
        "price": "354.426",
        "remaining_amount": "0.0001",
        "side": "buy",
        "start_amount": "0.0001",
        "status": "UNFILLED",
        "type": "limit",
        "expire_at": 1758098316395,
        "post_only": True,
        "user_cancelable": True,
    }

    # spot_order_invalidation
    msg = {"method": "spot_order_invalidation", "params": {"order_id": [44280888880]}}
    store.onmessage(msg)

    assert store.spot_order.find() == [
        {
            "average_price": "0.000",
            "executed_amount": "0.0000",
            "order_id": 44280998810,
            "ordered_at": 1742546617276,
            "pair": "xrp_jpy",
            "price": "359.412",
            "remaining_amount": "0.0002",
            "side": "buy",
            "start_amount": "0.0002",
            "status": "UNFILLED",
            "type": "limit",
            "expire_at": 1758098617276,
            "post_only": True,
            "user_cancelable": True,
        }
    ]

    # Partially filled
    msg = {
        "method": "spot_order",
        "params": [
            {
                "average_price": "359.412",
                "executed_amount": "0.0001",
                "executed_at": 1742546671700,
                "expire_at": 1758098617276,
                "order_id": 44280998810,
                "ordered_at": 1742546617276,
                "pair": "xrp_jpy",
                "price": "359.412",
                "remaining_amount": "0.0001",
                "side": "buy",
                "start_amount": "0.0002",
                "status": "PARTIALLY_FILLED",
                "type": "limit",
                "post_only": True,
                "user_cancelable": True,
            }
        ],
    }
    store.onmessage(msg)

    assert store.spot_order.find() == [
        {
            "average_price": "359.412",
            "executed_amount": "0.0001",
            "executed_at": 1742546671700,
            "expire_at": 1758098617276,
            "order_id": 44280998810,
            "ordered_at": 1742546617276,
            "pair": "xrp_jpy",
            "price": "359.412",
            "remaining_amount": "0.0001",
            "side": "buy",
            "start_amount": "0.0002",
            "status": "PARTIALLY_FILLED",
            "type": "limit",
            "post_only": True,
            "user_cancelable": True,
        }
    ]

    # Fully filled
    msg = {
        "method": "spot_order",
        "params": [
            {
                "average_price": "359.412",
                "executed_amount": "0.0002",
                "executed_at": 1742546671700,
                "expire_at": 1758098617276,
                "order_id": 44280998810,
                "ordered_at": 1742546617276,
                "pair": "xrp_jpy",
                "price": "359.412",
                "remaining_amount": "0.0000",
                "side": "buy",
                "start_amount": "0.0002",
                "status": "FULLY_FILLED",
                "type": "limit",
                "post_only": True,
                "user_cancelable": True,
            }
        ],
    }
    store.onmessage(msg)

    assert store.spot_order.find() == []


@pytest_asyncio.fixture
async def server_bitbank_private_initialize() -> AsyncGenerator[str]:
    routes = web.RouteTableDef()

    @routes.get("/v1/user/assets")
    async def assets(request: web.Request) -> web.Response:
        return web.json_response(
            {
                "success": 1,
                "data": {
                    "assets": [
                        {
                            "asset": "string",
                            "free_amount": "string",
                            "amount_precision": 0,
                            "onhand_amount": "string",
                            "locked_amount": "string",
                            "withdrawing_amount": "string",
                            "withdrawal_fee": {"min": "string", "max": "string"},
                            "stop_deposit": False,
                            "stop_withdrawal": False,
                            "network_list": [
                                {
                                    "asset": "string",
                                    "network": "string",
                                    "stop_deposit": False,
                                    "stop_withdrawal": False,
                                    "withdrawal_fee": "string",
                                }
                            ],
                            "collateral_ratio": "string",
                        },
                        {
                            "asset": "jpy",
                            "free_amount": "string",
                            "amount_precision": 0,
                            "onhand_amount": "string",
                            "locked_amount": "string",
                            "withdrawing_amount": "string",
                            "withdrawal_fee": {
                                "under": "string",
                                "over": "string",
                                "threshold": "string",
                            },
                            "stop_deposit": False,
                            "stop_withdrawal": False,
                            "collateral_ratio": "string",
                        },
                    ]
                },
            }
        )

    @routes.get("/v1/user/spot/active_orders")
    async def spot_orders(request: web.Request) -> web.Response:
        return web.json_response(
            {
                "success": 1,
                "data": {
                    "orders": [
                        {
                            "order_id": 0,
                            "pair": "string",
                            "side": "string",
                            "position_side": "string",
                            "type": "string",
                            "start_amount": "string",
                            "remaining_amount": "string",
                            "executed_amount": "string",
                            "price": "string",
                            "post_only": False,
                            "user_cancelable": True,
                            "average_price": "string",
                            "ordered_at": 0,
                            "expire_at": 0,
                            "triggered_at": 0,
                            "trigger_price": "string",
                            "status": "string",
                        }
                    ]
                },
            }
        )

    @routes.get("/v1/user/margin/positions")
    async def positions(request: web.Request) -> web.Response:
        return web.json_response(
            {
                "success": 1,
                "data": {
                    "notice": {
                        "what": "string",
                        "occurred_at": 0,
                        "amount": "0",
                        "due_date_at": 0,
                    },
                    "payables": {"amount": "0"},
                    "positions": [
                        {
                            "pair": "string",
                            "position_side": "string",
                            "open_amount": "0",
                            "product": "0",
                            "average_price": "0",
                            "unrealized_fee_amount": "0",
                            "unrealized_interest_amount": "0",
                        }
                    ],
                    "losscut_threshold": {"individual": "0", "company": "0"},
                },
            }
        )

    app = web.Application()
    app.add_routes(routes)

    async with TestServer(app) as server:
        yield str(server.make_url(URL()))


@dataclass
class InputBitbankPrivateInitialize:
    method: str
    url: str
    store_name: str


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "test_input,expected",
    [
        pytest.param(
            InputBitbankPrivateInitialize("GET", "/v1/user/assets", "asset"),
            [
                {
                    "asset": "string",
                    "free_amount": "string",
                    "amount_precision": 0,
                    "onhand_amount": "string",
                    "locked_amount": "string",
                    "withdrawing_amount": "string",
                    "withdrawal_fee": {"min": "string", "max": "string"},
                    "stop_deposit": False,
                    "stop_withdrawal": False,
                    "network_list": [
                        {
                            "asset": "string",
                            "network": "string",
                            "stop_deposit": False,
                            "stop_withdrawal": False,
                            "withdrawal_fee": "string",
                        }
                    ],
                    "collateral_ratio": "string",
                },
                {
                    "asset": "jpy",
                    "free_amount": "string",
                    "amount_precision": 0,
                    "onhand_amount": "string",
                    "locked_amount": "string",
                    "withdrawing_amount": "string",
                    "withdrawal_fee": {
                        "under": "string",
                        "over": "string",
                        "threshold": "string",
                    },
                    "stop_deposit": False,
                    "stop_withdrawal": False,
                    "collateral_ratio": "string",
                },
            ],
            id="asset",
        ),
        pytest.param(
            InputBitbankPrivateInitialize(
                "GET", "/v1/user/spot/active_orders", "spot_order"
            ),
            [
                {
                    "order_id": 0,
                    "pair": "string",
                    "side": "string",
                    "position_side": "string",
                    "type": "string",
                    "start_amount": "string",
                    "remaining_amount": "string",
                    "executed_amount": "string",
                    "price": "string",
                    "post_only": False,
                    "user_cancelable": True,
                    "average_price": "string",
                    "ordered_at": 0,
                    "expire_at": 0,
                    "triggered_at": 0,
                    "trigger_price": "string",
                    "status": "string",
                }
            ],
            id="spot_order",
        ),
        pytest.param(
            InputBitbankPrivateInitialize(
                "GET", "/v1/user/margin/positions", "margin_position"
            ),
            [
                {
                    "pair": "string",
                    "position_side": "string",
                    "open_amount": "0",
                    "product": "0",
                    "average_price": "0",
                    "unrealized_fee_amount": "0",
                    "unrealized_interest_amount": "0",
                }
            ],
            id="margin_position",
        ),
    ],
)
async def test_bitbank_private_initialize(
    server_bitbank_private_initialize: str,
    test_input: InputBitbankPrivateInitialize,
    expected: list[Item],
) -> None:
    store = pybotters.bitbankPrivateDataStore()

    async with pybotters.Client(base_url=server_bitbank_private_initialize) as client:
        await store.initialize(client.request(test_input.method, test_input.url))

    s = store[test_input.store_name]
    assert s is not None
    assert s.find() == expected


@pytest.mark.parametrize(
    "test_input,expected",
    [
        pytest.param(
            *ParamArg(
                test_input=StoreArg(
                    name="allMids",
                    data={
                        "channel": "allMids",
                        "data": {
                            "mids": {
                                "APE": "4.33245",
                                "ARB": "1.21695",
                            }
                        },
                    },
                ),
                expected=[
                    {"coin": "APE", "px": "4.33245"},
                    {"coin": "ARB", "px": "1.21695"},
                ],
            ),
            id="allMids",
        ),
        pytest.param(
            *ParamArg(
                test_input=StoreArg(
                    name="notification",
                    data={
                        "channel": "notification",
                        "data": {
                            "notification": "<notification>",
                        },
                    },
                ),
                expected=[
                    {"notification": "<notification>"},
                ],
            ),
            id="notification",
        ),
        pytest.param(
            *ParamArg(
                test_input=StoreArg(
                    name="webData2",
                    data={
                        "channel": "webData2",
                        "data": {
                            "clearinghouseState": {
                                "marginSummary": {
                                    "accountValue": "29.78001",
                                    "totalNtlPos": "0.0",
                                    "totalRawUsd": "29.78001",
                                    "totalMarginUsed": "0.0",
                                },
                                "crossMarginSummary": {
                                    "accountValue": "29.78001",
                                    "totalNtlPos": "0.0",
                                    "totalRawUsd": "29.78001",
                                    "totalMarginUsed": "0.0",
                                },
                                "crossMaintenanceMarginUsed": "0.0",
                                "withdrawable": "29.78001",
                                "assetPositions": [],
                                "time": 1733968369395,
                            },
                            "user": "0x0000000000000000000000000000000000000001",
                        },
                    },
                ),
                expected=[
                    {
                        "clearinghouseState": {
                            "marginSummary": {
                                "accountValue": "29.78001",
                                "totalNtlPos": "0.0",
                                "totalRawUsd": "29.78001",
                                "totalMarginUsed": "0.0",
                            },
                            "crossMarginSummary": {
                                "accountValue": "29.78001",
                                "totalNtlPos": "0.0",
                                "totalRawUsd": "29.78001",
                                "totalMarginUsed": "0.0",
                            },
                            "crossMaintenanceMarginUsed": "0.0",
                            "withdrawable": "29.78001",
                            "assetPositions": [],
                            "time": 1733968369395,
                        },
                        "user": "0x0000000000000000000000000000000000000001",
                    },
                ],
            ),
            id="webData2",
        ),
        pytest.param(
            *ParamArg(
                test_input=StoreArg(
                    name="candle",
                    data={
                        "channel": "candle",
                        "data": {
                            "T": 1681924499999,
                            "c": "29258.0",
                            "h": "29309.0",
                            "i": "15m",
                            "l": "29250.0",
                            "n": 189,
                            "o": "29295.0",
                            "s": "BTC",
                            "t": 1681923600000,
                            "v": "0.98639",
                        },
                    },
                ),
                expected=[
                    {
                        "T": 1681924499999,
                        "c": "29258.0",
                        "h": "29309.0",
                        "i": "15m",
                        "l": "29250.0",
                        "n": 189,
                        "o": "29295.0",
                        "s": "BTC",
                        "t": 1681923600000,
                        "v": "0.98639",
                    }
                ],
            ),
            id="candle",
        ),
        pytest.param(
            *ParamArg(
                test_input=StoreArg(
                    name="l2Book",
                    data={
                        "channel": "l2Book",
                        "data": {
                            "coin": "TEST",
                            "time": 1681222254710,
                            "levels": [
                                [
                                    {"px": "19900", "sz": "1", "n": 1},
                                    {"px": "19800", "sz": "2", "n": 2},
                                    {"px": "19700", "sz": "3", "n": 3},
                                ],
                                [
                                    {"px": "20100", "sz": "1", "n": 1},
                                    {"px": "20200", "sz": "2", "n": 2},
                                    {"px": "20300", "sz": "3", "n": 3},
                                ],
                            ],
                        },
                    },
                ),
                expected=[
                    {"coin": "TEST", "side": "B", "px": "19900", "sz": "1", "n": 1},
                    {"coin": "TEST", "side": "B", "px": "19800", "sz": "2", "n": 2},
                    {"coin": "TEST", "side": "B", "px": "19700", "sz": "3", "n": 3},
                    {"coin": "TEST", "side": "A", "px": "20100", "sz": "1", "n": 1},
                    {"coin": "TEST", "side": "A", "px": "20200", "sz": "2", "n": 2},
                    {"coin": "TEST", "side": "A", "px": "20300", "sz": "3", "n": 3},
                ],
            ),
            id="l2Book",
        ),
        pytest.param(
            *ParamArg(
                test_input=StoreArg(
                    name="trades",
                    data={
                        "channel": "trades",
                        "data": [
                            {
                                "coin": "AVAX",
                                "side": "B",
                                "px": "18.435",
                                "sz": "93.53",
                                "hash": "0xa166e3fa63c25663024b03f2e0da011a00307e4017465df020210d3d432e7cb8",
                                "time": 1681222254710,
                                "tid": 118906512037719,
                                "users": [
                                    "0x72d73fea74d7ff40c3e5a70e17f5b1aaf47dfc26",
                                    "0x4b9d7caad51e284a45112395da621b94ec82b03f",
                                ],
                            },
                            {
                                "coin": "@107",
                                "side": "A",
                                "px": "18.620413815",
                                "sz": "43.84",
                                "time": 1735969713869,
                                "hash": "0x2222138cc516e3fe746c0411dd733f02e60086f43205af2ae37c93f6a792430b",
                                "tid": 907359904431134,
                                "users": [
                                    "0x0000000000000000000000000000000000000001",
                                    "0xffffffffffffffffffffffffffffffffffffffff",
                                ],
                            },
                        ],
                    },
                ),
                expected=[
                    {
                        "coin": "AVAX",
                        "side": "B",
                        "px": "18.435",
                        "sz": "93.53",
                        "hash": "0xa166e3fa63c25663024b03f2e0da011a00307e4017465df020210d3d432e7cb8",
                        "time": 1681222254710,
                        "tid": 118906512037719,
                        "users": [
                            "0x72d73fea74d7ff40c3e5a70e17f5b1aaf47dfc26",
                            "0x4b9d7caad51e284a45112395da621b94ec82b03f",
                        ],
                    },
                    {
                        "coin": "@107",
                        "side": "A",
                        "px": "18.620413815",
                        "sz": "43.84",
                        "time": 1735969713869,
                        "hash": "0x2222138cc516e3fe746c0411dd733f02e60086f43205af2ae37c93f6a792430b",
                        "tid": 907359904431134,
                        "users": [
                            "0x0000000000000000000000000000000000000001",
                            "0xffffffffffffffffffffffffffffffffffffffff",
                        ],
                    },
                ],
            ),
            id="trades",
        ),
        pytest.param(
            *ParamArg(
                test_input=StoreArg(
                    name="orderUpdates",
                    data={
                        "channel": "orderUpdates",
                        "data": [
                            {
                                "order": {
                                    "coin": "BTC",
                                    "limitPx": "29792.0",
                                    "oid": 91490942,
                                    "side": "A",
                                    "sz": "0.0",
                                    "timestamp": 1681247412573,
                                },
                                "status": "open",
                                "statusTimestamp": 1750141385054,
                            }
                        ],
                    },
                ),
                expected=[
                    {
                        "coin": "BTC",
                        "limitPx": "29792.0",
                        "oid": 91490942,
                        "side": "A",
                        "sz": "0.0",
                        "timestamp": 1681247412573,
                        "status": "open",
                        "statusTimestamp": 1750141385054,
                    }
                ],
            ),
            id="orderUpdates",
        ),
        pytest.param(
            *ParamArg(
                test_input=StoreArg(
                    name="userEvents",
                    data={
                        "channel": "user",
                        "data": {
                            "fills": [
                                {
                                    "coin": "AVAX",
                                    "px": "18.435",
                                    "sz": "93.53",
                                    "side": "B",
                                    "time": 1681222254710,
                                    "startPosition": "26.86",
                                    "dir": "Open Long",
                                    "closedPnl": "0.0",
                                    "hash": "0xa166e3fa63c25663024b03f2e0da011a00307e4017465df020210d3d432e7cb8",
                                    "oid": 90542681,
                                    "crossed": False,
                                    "fee": "0.01",
                                    "tid": 118906512037719,
                                    "liquidation": {
                                        "liquidatedUser": "0x0000000000000000000000000000000000000000",
                                        "markPx": "18.435",
                                        "method": "<method>",
                                    },
                                    "feeToken": "USDC",
                                    "builderFee": "0.01",
                                },
                            ],
                            "funding": {
                                "time": 1681222254710,
                                "coin": "ETH",
                                "usdc": "-3.625312",
                                "szi": "49.1477",
                                "fundingRate": "0.0000417",
                            },
                            "liquidation": {
                                "lid": 0,
                                "liquidator": "0x0000000000000000000000000000000000000000",
                                "liquidated_user": "0x0000000000000000000000000000000000000000",
                                "liquidated_ntl_pos": "0.0",
                                "liquidated_account_value": "0.0",
                            },
                            "nonUserCancel": [
                                {
                                    "coin": "AVAX",
                                    "oid": 90542681,
                                }
                            ],
                        },
                    },
                ),
                expected=[
                    {
                        "type": "fills",
                        "coin": "AVAX",
                        "px": "18.435",
                        "sz": "93.53",
                        "side": "B",
                        "time": 1681222254710,
                        "startPosition": "26.86",
                        "dir": "Open Long",
                        "closedPnl": "0.0",
                        "hash": "0xa166e3fa63c25663024b03f2e0da011a00307e4017465df020210d3d432e7cb8",
                        "oid": 90542681,
                        "crossed": False,
                        "fee": "0.01",
                        "tid": 118906512037719,
                        "liquidation": {
                            "liquidatedUser": "0x0000000000000000000000000000000000000000",
                            "markPx": "18.435",
                            "method": "<method>",
                        },
                        "feeToken": "USDC",
                        "builderFee": "0.01",
                    },
                    {
                        "type": "funding",
                        "time": 1681222254710,
                        "coin": "ETH",
                        "usdc": "-3.625312",
                        "szi": "49.1477",
                        "fundingRate": "0.0000417",
                    },
                    {
                        "type": "liquidation",
                        "lid": 0,
                        "liquidator": "0x0000000000000000000000000000000000000000",
                        "liquidated_user": "0x0000000000000000000000000000000000000000",
                        "liquidated_ntl_pos": "0.0",
                        "liquidated_account_value": "0.0",
                    },
                    {
                        "type": "nonUserCancel",
                        "coin": "AVAX",
                        "oid": 90542681,
                    },
                ],
            ),
            id="userEvents",
        ),
        pytest.param(
            *ParamArg(
                test_input=StoreArg(
                    name="userFundings",
                    data={
                        "channel": "userFundings",
                        "data": {
                            "isSnapshot": True,
                            "user": "0x0000000000000000000000000000000000000001",
                            "fundings": [
                                {
                                    "time": 1681222254710,
                                    "coin": "ETH",
                                    "usdc": "-3.625312",
                                    "szi": "49.1477",
                                    "fundingRate": "0.0000417",
                                },
                            ],
                        },
                    },
                ),
                expected=[
                    {
                        "time": 1681222254710,
                        "coin": "ETH",
                        "usdc": "-3.625312",
                        "szi": "49.1477",
                        "fundingRate": "0.0000417",
                    },
                ],
            ),
            id="userFundings",
        ),
        pytest.param(
            *ParamArg(
                test_input=StoreArg(
                    name="userNonFundingLedgerUpdates",
                    data={
                        "channel": "userNonFundingLedgerUpdates",
                        "data": {
                            "isSnapshot": True,
                            "user": "0x0000000000000000000000000000000000000001",
                            "nonFundingLedgerUpdates": [
                                {
                                    "time": 1681222254710,
                                    "hash": "0xa166e3fa63c25663024b03f2e0da011a00307e4017465df020210d3d432e7cb8",
                                    "delta": {"type": "<type>", "usdc": "0.0"},
                                }
                            ],
                        },
                    },
                ),
                expected=[
                    {
                        "time": 1681222254710,
                        "hash": "0xa166e3fa63c25663024b03f2e0da011a00307e4017465df020210d3d432e7cb8",
                        "delta": {"type": "<type>", "usdc": "0.0"},
                    }
                ],
            ),
            id="userNonFundingLedgerUpdates",
        ),
        pytest.param(
            *ParamArg(
                test_input=StoreArg(
                    name="activeAssetCtx",
                    data={
                        "channel": "activeAssetCtx",
                        "data": {
                            "coin": "BTC",
                            "ctx": {
                                "dayNtlVlm": "1169046.29406",
                                "prevDayPx": "15.322",
                                "markPx": "14.3161",
                                "midPx": "14.314",
                                "funding": "0.0000125",
                                "openInterest": "688.11",
                                "oraclePx": "14.32",
                            },
                        },
                    },
                ),
                expected=[
                    {
                        "coin": "BTC",
                        "dayNtlVlm": "1169046.29406",
                        "prevDayPx": "15.322",
                        "markPx": "14.3161",
                        "midPx": "14.314",
                        "funding": "0.0000125",
                        "openInterest": "688.11",
                        "oraclePx": "14.32",
                    }
                ],
            ),
            id="activeAssetCtx",
        ),
        pytest.param(
            *ParamArg(
                test_input=StoreArg(
                    name="activeAssetData",
                    data={
                        "channel": "activeAssetData",
                        "data": {
                            "user": "0x0000000000000000000000000000000000000001",
                            "coin": "ETH",
                            "leverage": {
                                "type": "cross",
                                "value": 20,
                            },
                            "maxTradeSzs": ["0.0", "0.0"],
                            "availableToTrade": ["0.0", "0.0"],
                        },
                    },
                ),
                expected=[
                    {
                        "user": "0x0000000000000000000000000000000000000001",
                        "coin": "ETH",
                        "leverage": {
                            "type": "cross",
                            "value": 20,
                        },
                        "maxTradeSzs": ["0.0", "0.0"],
                        "availableToTrade": ["0.0", "0.0"],
                    }
                ],
            ),
            id="activeAssetData",
        ),
        pytest.param(
            *ParamArg(
                test_input=StoreArg(
                    name="userTwapSliceFills",
                    data={
                        "channel": "userTwapSliceFills",
                        "data": {
                            "isSnapshot": True,
                            "user": "0x0000000000000000000000000000000000000001",
                            "twapSliceFills": [
                                {
                                    "coin": "AVAX",
                                    "px": "18.435",
                                    "sz": "93.53",
                                    "side": "B",
                                    "time": 1681222254710,
                                    "startPosition": "26.86",
                                    "dir": "Open Long",
                                    "closedPnl": "0.0",
                                    "hash": "0xa166e3fa63c25663024b03f2e0da011a00307e4017465df020210d3d432e7cb8",
                                    "oid": 90542681,
                                    "crossed": False,
                                    "fee": "0.01",
                                    "tid": 118906512037719,
                                    "liquidation": {
                                        "liquidatedUser": "0x0000000000000000000000000000000000000000",
                                        "markPx": "18.435",
                                        "method": "<method>",
                                    },
                                    "feeToken": "USDC",
                                    "builderFee": "0.01",
                                    "twapId": 3156,
                                },
                            ],
                        },
                    },
                ),
                expected=[
                    {
                        "coin": "AVAX",
                        "px": "18.435",
                        "sz": "93.53",
                        "side": "B",
                        "time": 1681222254710,
                        "startPosition": "26.86",
                        "dir": "Open Long",
                        "closedPnl": "0.0",
                        "hash": "0xa166e3fa63c25663024b03f2e0da011a00307e4017465df020210d3d432e7cb8",
                        "oid": 90542681,
                        "crossed": False,
                        "fee": "0.01",
                        "tid": 118906512037719,
                        "liquidation": {
                            "liquidatedUser": "0x0000000000000000000000000000000000000000",
                            "markPx": "18.435",
                            "method": "<method>",
                        },
                        "feeToken": "USDC",
                        "builderFee": "0.01",
                        "twapId": 3156,
                    },
                ],
            ),
            id="userTwapSliceFills",
        ),
        pytest.param(
            *ParamArg(
                test_input=StoreArg(
                    name="userTwapHistory",
                    data={
                        "channel": "userTwapHistory",
                        "data": {
                            "isSnapshot": True,
                            "user": "0x0000000000000000000000000000000000000001",
                            "history": [
                                {
                                    "state": {},
                                    "status": {
                                        "status": "<status>",
                                        "description": "<description>",
                                    },
                                    "time": 1681222254710,
                                },
                            ],
                        },
                    },
                ),
                expected=[
                    {
                        "state": {},
                        "status": {
                            "status": "<status>",
                            "description": "<description>",
                        },
                        "time": 1681222254710,
                    },
                ],
            ),
            id="userTwapHistory",
        ),
        pytest.param(
            *ParamArg(
                test_input=StoreArg(
                    name="bbo",
                    data={
                        "channel": "bbo",
                        "data": {
                            "coin": "TEST",
                            "time": 1708622398623,
                            "bbo": [
                                {"px": "19900", "sz": "1", "n": 1},
                                {"px": "20100", "sz": "1", "n": 1},
                            ],
                        },
                    },
                ),
                expected=[
                    {
                        "coin": "TEST",
                        "time": 1708622398623,
                        "bbo": [
                            {"px": "19900", "sz": "1", "n": 1},
                            {"px": "20100", "sz": "1", "n": 1},
                        ],
                    },
                ],
            ),
            id="bbo",
        ),
    ],
)
def test_hyperliquid(test_input: StoreArg, expected: object) -> None:
    store = pybotters.HyperliquidDataStore()
    store.onmessage(test_input.data)

    s = store[test_input.name]
    assert s is not None
    assert s.find() == expected


def test_hyperliquid_active_orders() -> None:
    store = pybotters.HyperliquidDataStore()

    # Receive the open order event
    store.onmessage(
        {
            "channel": "orderUpdates",
            "data": [
                {
                    "order": {
                        "coin": "BTC",
                        "limitPx": "29792.0",
                        "oid": 91490942,
                        "side": "A",
                        "sz": "0.0",
                        "timestamp": 1681247412573,
                    },
                    "status": "open",
                    "statusTimestamp": 1750141385054,
                }
            ],
        }
    )

    assert len(store.order_updates) == 1
    assert store.order_updates.get({"coin": "BTC", "oid": 91490942}) == {
        "coin": "BTC",
        "limitPx": "29792.0",
        "oid": 91490942,
        "side": "A",
        "sz": "0.0",
        "timestamp": 1681247412573,
        "status": "open",
        "statusTimestamp": 1750141385054,
    }

    # Receive the filled order event
    store.onmessage(
        {
            "channel": "orderUpdates",
            "data": [
                {
                    "order": {
                        "coin": "BTC",
                        "limitPx": "29792.0",
                        "oid": 91490942,
                        "side": "A",
                        "sz": "0.0",
                        "timestamp": 1681247412573,
                    },
                    "status": "filled",  # FILLED ORDER
                    "statusTimestamp": 1750141385054,
                }
            ],
        }
    )

    assert len(store.order_updates) == 0
    assert store.order_updates.get({"coin": "BTC", "oid": 91490942}) is None


@pytest.mark.parametrize(
    "test_input,expected",
    [
        pytest.param(
            *ParamArg(
                test_input=StoreArg(
                    name="execution-events",
                    data={
                        "channel": "execution-events",
                        "id": 583710515,
                        "order_id": 8047158658,
                        "event_time": "2025-08-24T04:52:25.000Z",
                        "funds": {"btc": "0.005", "jpy": "-84628.36"},
                        "pair": "btc_jpy",
                        "rate": "16925672.0",
                        "fee_currency": None,
                        "fee": "0.0",
                        "liquidity": "T",
                        "side": "buy",
                    },
                ),
                expected=[
                    {
                        "channel": "execution-events",
                        "id": 583710515,
                        "order_id": 8047158658,
                        "event_time": "2025-08-24T04:52:25.000Z",
                        "funds": {"btc": "0.005", "jpy": "-84628.36"},
                        "pair": "btc_jpy",
                        "rate": "16925672.0",
                        "fee_currency": None,
                        "fee": "0.0",
                        "liquidity": "T",
                        "side": "buy",
                    }
                ],
            ),
            id="execution-events",
        ),
    ],
)
def test_coincheck_private(test_input: StoreArg, expected: object) -> None:
    store = pybotters.CoincheckPrivateDataStore()

    store.onmessage(test_input.data)

    s = store[test_input.name]
    assert s is not None
    assert s.find() == expected


@pytest_asyncio.fixture
async def server_coincheck_private() -> AsyncGenerator[str]:
    routes = web.RouteTableDef()

    @routes.get("/api/exchange/orders/opens")
    async def assets(request: web.Request) -> web.Response:
        return web.json_response(
            {
                "success": True,
                "orders": [
                    {
                        "id": 8169566829,
                        "order_type": "buy",
                        "rate": "16391712.0",
                        "pair": "btc_jpy",
                        "pending_amount": "0.001",
                        "pending_market_buy_amount": None,
                        "stop_loss_rate": None,
                        "created_at": "2025-09-28T06:26:42.000Z",
                    }
                ],
            }
        )

    app = web.Application()
    app.add_routes(routes)

    async with TestServer(app) as server:
        yield str(server.make_url(URL()))


@pytest.mark.asyncio
async def test_coincheck_private_order_initialize(
    server_coincheck_private: str,
) -> None:
    store = pybotters.CoincheckPrivateDataStore()

    async with pybotters.Client(base_url=server_coincheck_private) as client:
        await store.initialize(client.request("GET", "/api/exchange/orders/opens"))

    assert store.order.find() == [
        {
            "id": 8169566829,
            "order_type": "buy",
            "rate": "16391712.0",
            "pair": "btc_jpy",
            "pending_amount": "0.001",
            "pending_market_buy_amount": None,
            "stop_loss_rate": None,
            "created_at": "2025-09-28T06:26:42.000Z",
        }
    ]


def test_coincheck_private_order() -> None:
    store = pybotters.CoincheckPrivateDataStore()

    # Step 1: Place a new order
    store.order.feed_response(
        {
            "success": True,
            "id": 8173686428,
            "amount": "0.0064",
            "rate": "16998705.0",
            "order_type": "buy",
            "pair": "btc_jpy",
            "created_at": "2025-09-29T14:50:21.000Z",
            "market_buy_amount": None,
            "time_in_force": "good_til_cancelled",
            "stop_loss_rate": None,
        }
    )

    assert store.order.find() == [
        {
            "success": True,
            "id": 8173686428,
            "amount": "0.0064",
            "rate": "16998705.0",
            "order_type": "buy",
            "pair": "btc_jpy",
            "created_at": "2025-09-29T14:50:21.000Z",
            "market_buy_amount": None,
            "time_in_force": "good_til_cancelled",
            "stop_loss_rate": None,
            "pending_amount": "0.0064",
            "pending_market_buy_amount": None,
        }
    ]

    # Step 2: Receive a partial fill event
    store.onmessage(
        {
            "channel": "order-events",
            "id": 8173686428,
            "pair": "btc_jpy",
            "order_event": "PARTIALLY_FILL",
            "order_type": "buy",
            "rate": "16998705.0",
            "stop_loss_rate": None,
            "maker_fee_rate": "0.0",
            "taker_fee_rate": "0.0",
            "amount": "0.0064",
            "market_buy_amount": None,
            "latest_executed_amount": "0.001",
            "latest_executed_market_buy_amount": None,
            "expired_type": None,
            "prevented_match_id": None,
            "expired_amount": None,
            "expired_market_buy_amount": None,
            "time_in_force": "good_til_cancelled",
            "event_time": "2025-09-29T14:50:24.000Z",
        }
    )

    assert store.order.find() == [
        {
            "success": True,
            "channel": "order-events",
            "id": 8173686428,
            "pair": "btc_jpy",
            "order_event": "PARTIALLY_FILL",
            "order_type": "buy",
            "rate": "16998705.0",
            "stop_loss_rate": None,
            "maker_fee_rate": "0.0",
            "taker_fee_rate": "0.0",
            "amount": "0.0064",
            "market_buy_amount": None,
            "latest_executed_amount": "0.001",
            "latest_executed_market_buy_amount": None,
            "expired_type": None,
            "prevented_match_id": None,
            "expired_amount": None,
            "expired_market_buy_amount": None,
            "time_in_force": "good_til_cancelled",
            "created_at": "2025-09-29T14:50:21.000Z",
            "event_time": "2025-09-29T14:50:24.000Z",
            "pending_amount": "0.0054",
            "pending_market_buy_amount": None,
        }
    ]

    # Step 3: Receive a fill event
    store.onmessage(
        {
            "channel": "order-events",
            "id": 8173686428,
            "pair": "btc_jpy",
            "order_event": "FILL",
            "order_type": "buy",
            "rate": "16998705.0",
            "stop_loss_rate": None,
            "maker_fee_rate": "0.0",
            "taker_fee_rate": "0.0",
            "amount": "0.0064",
            "market_buy_amount": None,
            "latest_executed_amount": "0.0054",
            "latest_executed_market_buy_amount": None,
            "expired_type": None,
            "prevented_match_id": None,
            "expired_amount": None,
            "expired_market_buy_amount": None,
            "time_in_force": "good_til_cancelled",
            "event_time": "2025-09-29T14:50:27.000Z",
        }
    )

    assert store.order.find() == []


def test_coincheck_private_order_market() -> None:
    store = pybotters.CoincheckPrivateDataStore()

    # Step 1: Place a new order
    store.order.feed_response(
        {
            "success": True,
            "id": 8173686428,
            "amount": None,
            "rate": None,
            "order_type": "market_buy",
            "pair": "btc_jpy",
            "created_at": "2025-09-29T14:50:21.000Z",
            "market_buy_amount": "108792.0",
            "time_in_force": "good_til_cancelled",
            "stop_loss_rate": None,
        }
    )

    assert store.order.find() == [
        {
            "success": True,
            "id": 8173686428,
            "amount": None,
            "rate": None,
            "order_type": "market_buy",
            "pair": "btc_jpy",
            "created_at": "2025-09-29T14:50:21.000Z",
            "market_buy_amount": "108792.0",
            "time_in_force": "good_til_cancelled",
            "stop_loss_rate": None,
            "pending_amount": None,
            "pending_market_buy_amount": "108792.0",
        }
    ]

    # Step 2: Receive a partial fill event
    store.onmessage(
        {
            "channel": "order-events",
            "id": 8173686428,
            "pair": "btc_jpy",
            "order_event": "PARTIALLY_FILL",
            "order_type": "market_buy",
            "rate": None,
            "stop_loss_rate": None,
            "maker_fee_rate": "0.0",
            "taker_fee_rate": "0.0",
            "amount": None,
            "market_buy_amount": "108792.0",
            "latest_executed_amount": None,
            "latest_executed_market_buy_amount": "16999.0",
            "expired_type": None,
            "prevented_match_id": None,
            "expired_amount": None,
            "expired_market_buy_amount": None,
            "time_in_force": "good_til_cancelled",
            "event_time": "2025-09-29T14:50:24.000Z",
        }
    )

    assert store.order.find() == [
        {
            "success": True,
            "channel": "order-events",
            "id": 8173686428,
            "pair": "btc_jpy",
            "order_event": "PARTIALLY_FILL",
            "order_type": "market_buy",
            "rate": None,
            "stop_loss_rate": None,
            "maker_fee_rate": "0.0",
            "taker_fee_rate": "0.0",
            "amount": None,
            "market_buy_amount": "108792.0",
            "latest_executed_amount": None,
            "latest_executed_market_buy_amount": "16999.0",
            "expired_type": None,
            "prevented_match_id": None,
            "expired_amount": None,
            "expired_market_buy_amount": None,
            "time_in_force": "good_til_cancelled",
            "created_at": "2025-09-29T14:50:21.000Z",
            "event_time": "2025-09-29T14:50:24.000Z",
            "pending_amount": None,
            "pending_market_buy_amount": "91793.0",
        }
    ]

    # Step 3: Receive a fill event
    store.onmessage(
        {
            "channel": "order-events",
            "id": 8173686428,
            "pair": "btc_jpy",
            "order_event": "FILL",
            "order_type": "market_buy",
            "rate": None,
            "stop_loss_rate": None,
            "maker_fee_rate": "0.0",
            "taker_fee_rate": "0.0",
            "amount": None,
            "market_buy_amount": None,
            "latest_executed_amount": None,
            "latest_executed_market_buy_amount": "91793",
            "expired_type": None,
            "prevented_match_id": None,
            "expired_amount": None,
            "expired_market_buy_amount": None,
            "time_in_force": "good_til_cancelled",
            "event_time": "2025-09-29T14:50:27.000Z",
        }
    )

    assert store.order.find() == []


def test_lighter_ticker() -> None:
    """Check the behavior of LighterDataStore.ticker."""
    store = pybotters.LighterDataStore()
    ws: Any = object()

    store.onmessage(
        {
            "channel": "ticker:0",
            "nonce": 6442420597,
            "ticker": {
                "s": "ETH",
                "a": {"price": "2150.10", "size": "4.6512"},
                "b": {"price": "2149.99", "size": "17.4551"},
            },
            "timestamp": 1773158679717,
            "type": "update/ticker",
        },
        ws,
    )

    assert store.ticker.find() == [
        {
            "s": "ETH",
            "a": {"price": "2150.10", "size": "4.6512"},
            "b": {"price": "2149.99", "size": "17.4551"},
        }
    ]

    # Update with new price
    store.onmessage(
        {
            "channel": "ticker:0",
            "nonce": 6442420598,
            "ticker": {
                "s": "ETH",
                "a": {"price": "2151.00", "size": "3.0000"},
                "b": {"price": "2150.50", "size": "10.0000"},
            },
            "timestamp": 1773158680000,
            "type": "update/ticker",
        },
        ws,
    )

    assert store.ticker.find() == [
        {
            "s": "ETH",
            "a": {"price": "2151.00", "size": "3.0000"},
            "b": {"price": "2150.50", "size": "10.0000"},
        }
    ]


def test_lighter_market_stats() -> None:
    """Check the behavior of LighterDataStore.market_stats."""
    store = pybotters.LighterDataStore()
    ws: Any = object()

    store.onmessage(
        {
            "channel": "market_stats:0",
            "market_stats": {
                "symbol": "ETH",
                "market_id": 0,
                "index_price": "2965.30",
                "mark_price": "2963.63",
                "open_interest": "185926683.471886",
                "current_funding_rate": "-0.0005",
                "funding_rate": "0.0011",
                "funding_timestamp": 1769187600001,
                "daily_base_token_volume": 296009.9355,
                "daily_quote_token_volume": 870882976.341333,
                "daily_price_low": 2888.37,
                "daily_price_high": 2984,
                "daily_price_change": 0.830824189844348,
            },
            "timestamp": 1773158679717,
            "type": "update/market_stats",
        },
        ws,
    )

    result = store.market_stats.find()
    assert len(result) == 1
    assert result[0]["market_id"] == 0
    assert result[0]["current_funding_rate"] == "-0.0005"
    assert result[0]["funding_rate"] == "0.0011"
    assert result[0]["mark_price"] == "2963.63"

    # Update the same market
    store.onmessage(
        {
            "channel": "market_stats:0",
            "market_stats": {
                "symbol": "ETH",
                "market_id": 0,
                "index_price": "2970.00",
                "mark_price": "2968.00",
                "open_interest": "186000000.000000",
                "current_funding_rate": "0.0001",
                "funding_rate": "0.0011",
                "funding_timestamp": 1769187600001,
                "daily_base_token_volume": 300000.0,
                "daily_quote_token_volume": 880000000.0,
                "daily_price_low": 2888.37,
                "daily_price_high": 2990,
                "daily_price_change": 1.0,
            },
            "timestamp": 1773158680000,
            "type": "update/market_stats",
        },
        ws,
    )

    result = store.market_stats.find()
    assert len(result) == 1
    assert result[0]["mark_price"] == "2968.00"
    assert result[0]["current_funding_rate"] == "0.0001"


def test_lighter_market_stats_all() -> None:
    """Check the behavior of LighterDataStore.market_stats with market_stats/all."""
    store = pybotters.LighterDataStore()
    ws: Any = object()

    store.onmessage(
        {
            "channel": "market_stats:all",
            "market_stats": {
                "0": {
                    "symbol": "ETH",
                    "market_id": 0,
                    "mark_price": "1995.83",
                    "current_funding_rate": "-0.0008",
                },
                "1": {
                    "symbol": "BTC",
                    "market_id": 1,
                    "mark_price": "66421.2",
                    "current_funding_rate": "-0.0002",
                },
            },
            "timestamp": 1774814400000,
            "type": "update/market_stats",
        },
        ws,
    )

    result = store.market_stats.find()
    assert len(result) == 2

    eth = store.market_stats.get({"market_id": 0})
    assert eth is not None
    assert eth["symbol"] == "ETH"
    assert eth["mark_price"] == "1995.83"

    btc = store.market_stats.get({"market_id": 1})
    assert btc is not None
    assert btc["symbol"] == "BTC"
    assert btc["mark_price"] == "66421.2"


def test_lighter_order_book() -> None:
    """Check the behavior of LighterDataStore.order_book."""
    store = pybotters.LighterDataStore()
    ws: Any = object()

    # Snapshot (initial)
    store.onmessage(
        {
            "channel": "order_book:0",
            "offset": 41692864,
            "order_book": {
                "code": 0,
                "asks": [
                    {"price": "3327.46", "size": "29.0915"},
                    {"price": "3328.00", "size": "10.0000"},
                ],
                "bids": [
                    {"price": "3326.00", "size": "15.5000"},
                    {"price": "3325.50", "size": "20.0000"},
                ],
                "offset": 41692864,
                "nonce": 4037957053,
                "begin_nonce": 4037957034,
            },
            "timestamp": 1766434222583,
            "type": "update/order_book",
        },
        ws,
    )

    assert len(store.order_book.find({"side": "ask"})) == 2
    assert len(store.order_book.find({"side": "bid"})) == 2

    # Diff update: modify an ask, remove a bid
    store.onmessage(
        {
            "channel": "order_book:0",
            "offset": 41692865,
            "order_book": {
                "code": 0,
                "asks": [{"price": "3327.46", "size": "50.0000"}],
                "bids": [{"price": "3325.50", "size": "0"}],
                "offset": 41692865,
                "nonce": 4037957054,
            },
            "timestamp": 1766434222600,
            "type": "update/order_book",
        },
        ws,
    )

    asks = store.order_book.find({"side": "ask"})
    bids = store.order_book.find({"side": "bid"})
    assert len(asks) == 2
    assert len(bids) == 1

    updated_ask = store.order_book.get(
        {"market_id": 0, "side": "ask", "price": "3327.46"}
    )
    assert updated_ask is not None
    assert updated_ask["size"] == "50.0000"

    removed_bid = store.order_book.get(
        {"market_id": 0, "side": "bid", "price": "3325.50"}
    )
    assert removed_bid is None

    # Remove with "0.0000" (real Lighter format)
    store.onmessage(
        {
            "channel": "order_book:0",
            "offset": 41692866,
            "order_book": {
                "code": 0,
                "asks": [],
                "bids": [{"price": "3326.00", "size": "0.0000"}],
                "offset": 41692866,
                "nonce": 4037957055,
            },
            "timestamp": 1766434222700,
            "type": "update/order_book",
        },
        ws,
    )

    assert store.order_book.get(
        {"market_id": 0, "side": "bid", "price": "3326.00"}
    ) is None


def test_lighter_order_book_sorted() -> None:
    """Check the behavior of LighterDataStore.order_book.sorted()."""
    store = pybotters.LighterDataStore()
    ws: Any = object()

    store.onmessage(
        {
            "channel": "order_book:0",
            "offset": 100,
            "order_book": {
                "code": 0,
                "asks": [
                    {"price": "3330.00", "size": "5.0"},
                    {"price": "3328.00", "size": "10.0"},
                ],
                "bids": [
                    {"price": "3325.00", "size": "8.0"},
                    {"price": "3327.00", "size": "12.0"},
                ],
                "offset": 100,
                "nonce": 1000,
                "begin_nonce": 999,
            },
            "timestamp": 1766434222583,
            "type": "update/order_book",
        },
        ws,
    )

    result = store.order_book.sorted()
    # asks: ascending
    assert result["ask"][0]["price"] == "3328.00"
    assert result["ask"][1]["price"] == "3330.00"
    # bids: descending
    assert result["bid"][0]["price"] == "3327.00"
    assert result["bid"][1]["price"] == "3325.00"


def test_lighter_trade() -> None:
    """Check the behavior of LighterDataStore.trade."""
    store = pybotters.LighterDataStore()
    ws: Any = object()

    store.onmessage(
        {
            "channel": "trade:0",
            "liquidation_trades": [],
            "nonce": 8630448841,
            "trades": [
                {
                    "trade_id": 16164557907,
                    "type": "trade",
                    "market_id": 0,
                    "size": "0.1336",
                    "price": "2181.83",
                    "is_maker_ask": False,
                    "timestamp": 1773854156654,
                },
                {
                    "trade_id": 16164557908,
                    "type": "trade",
                    "market_id": 0,
                    "size": "0.5000",
                    "price": "2182.00",
                    "is_maker_ask": True,
                    "timestamp": 1773854156700,
                },
            ],
            "type": "update/trade",
        },
        ws,
    )

    assert len(store.trade.find()) == 2

    # Additional trades are appended
    store.onmessage(
        {
            "channel": "trade:0",
            "liquidation_trades": [],
            "nonce": 8630448842,
            "trades": [
                {
                    "trade_id": 16164557909,
                    "type": "trade",
                    "market_id": 0,
                    "size": "1.0000",
                    "price": "2183.00",
                    "is_maker_ask": False,
                    "timestamp": 1773854157000,
                },
            ],
            "type": "update/trade",
        },
        ws,
    )

    assert len(store.trade.find()) == 3


def test_lighter_error_message() -> None:
    """Check that error messages are logged, not processed as data."""
    store = pybotters.LighterDataStore()
    ws: Any = object()

    store.onmessage(
        {
            "type": "error",
            "message": "invalid channel",
        },
        ws,
    )

    assert store.ticker.find() == []
    assert store.market_stats.find() == []
    assert store.order_book.find() == []
    assert store.trade.find() == []


def test_lighter_subscribed_snapshot() -> None:
    """Check that subscribed/ type messages are processed as initial snapshots."""
    store = pybotters.LighterDataStore()
    ws: Any = object()

    # subscribed/ticker
    store.onmessage(
        {
            "channel": "ticker:0",
            "nonce": 100,
            "ticker": {
                "s": "ETH",
                "a": {"price": "2000.00", "size": "1.0"},
                "b": {"price": "1999.00", "size": "2.0"},
            },
            "timestamp": 1774000000000,
            "type": "subscribed/ticker",
        },
        ws,
    )
    assert len(store.ticker.find()) == 1
    assert store.ticker.find()[0]["s"] == "ETH"

    # subscribed/market_stats
    store.onmessage(
        {
            "channel": "market_stats:0",
            "market_stats": {
                "symbol": "ETH",
                "market_id": 0,
                "mark_price": "2000.00",
                "current_funding_rate": "0.0001",
            },
            "timestamp": 1774000000000,
            "type": "subscribed/market_stats",
        },
        ws,
    )
    assert len(store.market_stats.find()) == 1

    # subscribed/order_book
    store.onmessage(
        {
            "channel": "order_book:0",
            "order_book": {
                "code": 0,
                "asks": [{"price": "2001.00", "size": "1.0"}],
                "bids": [{"price": "1999.00", "size": "2.0"}],
                "nonce": 100,
                "begin_nonce": 99,
            },
            "timestamp": 1774000000000,
            "type": "subscribed/order_book",
        },
        ws,
    )
    assert len(store.order_book.find()) == 2

    # subscribed/trade
    store.onmessage(
        {
            "channel": "trade:0",
            "trades": [
                {"trade_id": 1, "price": "2000.00", "size": "0.1", "market_id": 0},
            ],
            "type": "subscribed/trade",
        },
        ws,
    )
    assert len(store.trade.find()) == 1


def test_edgex_ticker() -> None:
    """Check the behavior of EdgeXDataStore.ticker."""
    store = pybotters.EdgeXDataStore()
    ws: Any = object()

    store.onmessage(
        {
            "type": "quote-event",
            "channel": "ticker.10000001",
            "content": {
                "dataType": "Snapshot",
                "channel": "ticker.10000001",
                "data": [
                    {
                        "contractId": "10000001",
                        "contractName": "BTCUSD",
                        "lastPrice": "66393.5",
                        "indexPrice": "66411.046559188",
                        "oraclePrice": "66421.375000",
                        "markPrice": "66421.375000",
                        "openInterest": "8235.101",
                        "fundingRate": "-0.00005096",
                        "fundingTime": "1774800000000",
                        "nextFundingTime": "1774814400000",
                        "priceChange": "-411.5",
                        "priceChangePercent": "-0.006159",
                        "high": "67100.9",
                        "low": "66129.3",
                        "open": "66805.0",
                        "close": "66393.5",
                        "trades": "60982",
                        "size": "7318.603",
                        "value": "487392951.3009",
                    }
                ],
            },
        },
        ws,
    )

    result = store.ticker.find()
    assert len(result) == 1
    assert result[0]["contractId"] == "10000001"
    assert result[0]["lastPrice"] == "66393.5"
    assert result[0]["fundingRate"] == "-0.00005096"
    assert result[0]["indexPrice"] == "66411.046559188"

    # Update (changed)
    store.onmessage(
        {
            "type": "quote-event",
            "channel": "ticker.10000001",
            "content": {
                "dataType": "changed",
                "data": [
                    {
                        "contractId": "10000001",
                        "contractName": "BTCUSD",
                        "lastPrice": "66500.0",
                        "indexPrice": "66510.0",
                        "oraclePrice": "66520.0",
                        "markPrice": "66520.0",
                        "openInterest": "8240.000",
                        "fundingRate": "-0.00004000",
                        "fundingTime": "1774800000000",
                        "nextFundingTime": "1774814400000",
                        "priceChange": "-305.0",
                        "priceChangePercent": "-0.004",
                        "high": "67100.9",
                        "low": "66129.3",
                        "open": "66805.0",
                        "close": "66500.0",
                        "trades": "61000",
                        "size": "7320.000",
                        "value": "487500000.0",
                    }
                ],
            },
        },
        ws,
    )

    result = store.ticker.find()
    assert len(result) == 1
    assert result[0]["lastPrice"] == "66500.0"


def test_edgex_depth() -> None:
    """Check the behavior of EdgeXDataStore.depth."""
    store = pybotters.EdgeXDataStore()
    ws: Any = object()

    # Snapshot
    store.onmessage(
        {
            "type": "quote-event",
            "channel": "depth.10000001.15",
            "content": {
                "dataType": "Snapshot",
                "data": [
                    {
                        "contractId": "10000001",
                        "depthType": "SNAPSHOT",
                        "startVersion": "100",
                        "endVersion": "101",
                        "asks": [
                            {"price": "66393.6", "size": "0.128"},
                            {"price": "66394.6", "size": "3.199"},
                        ],
                        "bids": [
                            {"price": "66392.1", "size": "0.067"},
                            {"price": "66391.1", "size": "3.877"},
                        ],
                    }
                ],
            },
        },
        ws,
    )

    assert len(store.depth.find({"side": "ask"})) == 2
    assert len(store.depth.find({"side": "bid"})) == 2

    # Diff update: modify an ask, remove a bid
    store.onmessage(
        {
            "type": "quote-event",
            "channel": "depth.10000001.15",
            "content": {
                "dataType": "changed",
                "data": [
                    {
                        "contractId": "10000001",
                        "depthType": "CHANGED",
                        "startVersion": "101",
                        "endVersion": "102",
                        "asks": [{"price": "66393.6", "size": "0.042"}],
                        "bids": [{"price": "66391.1", "size": "0"}],
                    }
                ],
            },
        },
        ws,
    )

    asks = store.depth.find({"side": "ask"})
    bids = store.depth.find({"side": "bid"})
    assert len(asks) == 2
    assert len(bids) == 1

    updated_ask = store.depth.get(
        {"contractId": "10000001", "side": "ask", "price": "66393.6"}
    )
    assert updated_ask is not None
    assert updated_ask["size"] == "0.042"

    removed_bid = store.depth.get(
        {"contractId": "10000001", "side": "bid", "price": "66391.1"}
    )
    assert removed_bid is None


def test_edgex_depth_sorted() -> None:
    """Check the behavior of EdgeXDataStore.depth.sorted()."""
    store = pybotters.EdgeXDataStore()
    ws: Any = object()

    store.onmessage(
        {
            "type": "quote-event",
            "channel": "depth.10000001.15",
            "content": {
                "dataType": "Snapshot",
                "data": [
                    {
                        "contractId": "10000001",
                        "depthType": "SNAPSHOT",
                        "asks": [
                            {"price": "66400.0", "size": "1.0"},
                            {"price": "66395.0", "size": "2.0"},
                        ],
                        "bids": [
                            {"price": "66390.0", "size": "3.0"},
                            {"price": "66393.0", "size": "4.0"},
                        ],
                    }
                ],
            },
        },
        ws,
    )

    result = store.depth.sorted()
    assert result["ask"][0]["price"] == "66395.0"
    assert result["ask"][1]["price"] == "66400.0"
    assert result["bid"][0]["price"] == "66393.0"
    assert result["bid"][1]["price"] == "66390.0"


def test_edgex_trades() -> None:
    """Check the behavior of EdgeXDataStore.trades."""
    store = pybotters.EdgeXDataStore()
    ws: Any = object()

    store.onmessage(
        {
            "type": "quote-event",
            "channel": "trades.10000001",
            "content": {
                "dataType": "changed",
                "data": [
                    {
                        "ticketId": "aaa-bbb-001",
                        "time": "1774812452452",
                        "price": "66393.5",
                        "size": "0.006",
                        "contractId": "10000001",
                        "isBuyerMaker": False,
                    },
                    {
                        "ticketId": "aaa-bbb-002",
                        "time": "1774812452500",
                        "price": "66394.0",
                        "size": "0.010",
                        "contractId": "10000001",
                        "isBuyerMaker": True,
                    },
                ],
            },
        },
        ws,
    )

    assert len(store.trades.find()) == 2

    # Additional trades
    store.onmessage(
        {
            "type": "quote-event",
            "channel": "trades.10000001",
            "content": {
                "dataType": "changed",
                "data": [
                    {
                        "ticketId": "aaa-bbb-003",
                        "time": "1774812453000",
                        "price": "66400.0",
                        "size": "0.100",
                        "contractId": "10000001",
                        "isBuyerMaker": False,
                    },
                ],
            },
        },
        ws,
    )

    assert len(store.trades.find()) == 3


@pytest.mark.asyncio
async def test_edgex_ping_pong() -> None:
    """Check that EdgeXDataStore responds to server ping with pong."""
    from unittest.mock import AsyncMock

    store = pybotters.EdgeXDataStore()
    ws = AsyncMock()

    store.onmessage(
        {"type": "ping", "time": "1774812460002"},
        ws,
    )

    # Give the ensure_future task a chance to run
    await asyncio.sleep(0)

    ws.send_json.assert_called_once_with(
        {"type": "pong", "time": "1774812460002"}
    )

    # Data stores should be unaffected
    assert store.ticker.find() == []
    assert store.depth.find() == []
    assert store.trades.find() == []


def test_grvt_ticker() -> None:
    """Check the behavior of GRVTDataStore.ticker."""
    store = pybotters.GRVTDataStore()
    ws: Any = object()

    store.onmessage(
        {
            "stream": "v1.ticker.s",
            "selector": "BTC_USDT_Perp@500",
            "sequence_number": "0",
            "feed": {
                "event_time": "1774813674500000000",
                "instrument": "BTC_USDT_Perp",
                "mark_price": "66372.18",
                "index_price": "66408.40",
                "last_price": "66369.4",
                "last_size": "2.6",
                "mid_price": "66369.35",
                "best_bid_price": "66369.3",
                "best_bid_size": "9.626",
                "best_ask_price": "66369.4",
                "best_ask_size": "1.185",
                "funding_rate_8h_curr": "0.005",
                "funding_rate_8h_avg": "0.005",
                "open_interest": "2824.875",
                "long_short_ratio": "1.648",
            },
        },
        ws,
    )

    result = store.ticker.find()
    assert len(result) == 1
    assert result[0]["instrument"] == "BTC_USDT_Perp"
    assert result[0]["last_price"] == "66369.4"
    assert result[0]["funding_rate_8h_curr"] == "0.005"
    assert result[0]["mark_price"] == "66372.18"

    # Update
    store.onmessage(
        {
            "stream": "v1.ticker.s",
            "selector": "BTC_USDT_Perp@500",
            "sequence_number": "382135",
            "feed": {
                "event_time": "1774813675000000000",
                "instrument": "BTC_USDT_Perp",
                "mark_price": "66400.00",
                "index_price": "66410.00",
                "last_price": "66398.0",
                "last_size": "1.0",
                "mid_price": "66397.50",
                "best_bid_price": "66397.0",
                "best_bid_size": "5.0",
                "best_ask_price": "66398.0",
                "best_ask_size": "3.0",
                "funding_rate_8h_curr": "0.004",
                "funding_rate_8h_avg": "0.004",
                "open_interest": "2830.000",
                "long_short_ratio": "1.65",
            },
        },
        ws,
    )

    result = store.ticker.find()
    assert len(result) == 1
    assert result[0]["last_price"] == "66398.0"


def test_grvt_book() -> None:
    """Check the behavior of GRVTDataStore.book."""
    store = pybotters.GRVTDataStore()
    ws: Any = object()

    # Snapshot (sequence_number "0")
    store.onmessage(
        {
            "stream": "v1.book.d",
            "selector": "BTC_USDT_Perp@500",
            "sequence_number": "0",
            "feed": {
                "event_time": "1774813674650000000",
                "instrument": "BTC_USDT_Perp",
                "asks": [
                    {"price": "66369.4", "size": "1.185", "num_orders": 5},
                    {"price": "66370.0", "size": "0.032", "num_orders": 2},
                ],
                "bids": [
                    {"price": "66369.3", "size": "9.626", "num_orders": 10},
                    {"price": "66369.2", "size": "0.075", "num_orders": 1},
                ],
            },
        },
        ws,
    )

    assert len(store.book.find({"side": "ask"})) == 2
    assert len(store.book.find({"side": "bid"})) == 2

    # Delta update: modify an ask, remove a bid
    store.onmessage(
        {
            "stream": "v1.book.d",
            "selector": "BTC_USDT_Perp@500",
            "sequence_number": "560991",
            "feed": {
                "event_time": "1774813675000000000",
                "instrument": "BTC_USDT_Perp",
                "asks": [
                    {"price": "66369.4", "size": "2.000", "num_orders": 8},
                ],
                "bids": [
                    {"price": "66369.2", "size": "0", "num_orders": 0},
                ],
            },
        },
        ws,
    )

    asks = store.book.find({"side": "ask"})
    bids = store.book.find({"side": "bid"})
    assert len(asks) == 2
    assert len(bids) == 1

    updated = store.book.get(
        {"instrument": "BTC_USDT_Perp", "side": "ask", "price": "66369.4"}
    )
    assert updated is not None
    assert updated["size"] == "2.000"

    removed = store.book.get(
        {"instrument": "BTC_USDT_Perp", "side": "bid", "price": "66369.2"}
    )
    assert removed is None


def test_grvt_book_sorted() -> None:
    """Check the behavior of GRVTDataStore.book.sorted()."""
    store = pybotters.GRVTDataStore()
    ws: Any = object()

    store.onmessage(
        {
            "stream": "v1.book.d",
            "selector": "BTC_USDT_Perp@500",
            "sequence_number": "0",
            "feed": {
                "event_time": "1774813674650000000",
                "instrument": "BTC_USDT_Perp",
                "asks": [
                    {"price": "66400.0", "size": "1.0", "num_orders": 1},
                    {"price": "66395.0", "size": "2.0", "num_orders": 1},
                ],
                "bids": [
                    {"price": "66390.0", "size": "3.0", "num_orders": 1},
                    {"price": "66393.0", "size": "4.0", "num_orders": 1},
                ],
            },
        },
        ws,
    )

    result = store.book.sorted()
    assert result["ask"][0]["price"] == "66395.0"
    assert result["ask"][1]["price"] == "66400.0"
    assert result["bid"][0]["price"] == "66393.0"
    assert result["bid"][1]["price"] == "66390.0"


def test_grvt_trade() -> None:
    """Check the behavior of GRVTDataStore.trade."""
    store = pybotters.GRVTDataStore()
    ws: Any = object()

    store.onmessage(
        {
            "stream": "v1.trade",
            "selector": "BTC_USDT_Perp@500",
            "sequence_number": "0",
            "feed": {
                "event_time": "1774809573002272849",
                "instrument": "BTC_USDT_Perp",
                "is_taker_buyer": True,
                "size": "0.01",
                "price": "66460.0",
                "mark_price": "66442.97",
                "trade_id": "109626184-2",
                "venue": "ORDERBOOK",
            },
        },
        ws,
    )

    assert len(store.trade.find()) == 1

    store.onmessage(
        {
            "stream": "v1.trade",
            "selector": "BTC_USDT_Perp@500",
            "sequence_number": "70460",
            "feed": {
                "event_time": "1774809574000000000",
                "instrument": "BTC_USDT_Perp",
                "is_taker_buyer": False,
                "size": "0.05",
                "price": "66455.0",
                "mark_price": "66443.00",
                "trade_id": "109626185-1",
                "venue": "ORDERBOOK",
            },
        },
        ws,
    )

    assert len(store.trade.find()) == 2


def test_grvt_subscribe_response_ignored() -> None:
    """Check that subscribe responses are ignored."""
    store = pybotters.GRVTDataStore()
    ws: Any = object()

    store.onmessage(
        {
            "request_id": 1,
            "stream": "v1.ticker.s",
            "subs": ["BTC_USDT_Perp@500"],
            "unsubs": [],
            "num_snapshots": [1],
            "first_sequence_number": ["382134"],
        },
        ws,
    )

    assert store.ticker.find() == []
    assert store.book.find() == []
    assert store.trade.find() == []
