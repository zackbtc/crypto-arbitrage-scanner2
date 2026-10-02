import asyncio
import json
import time
import websockets


# =========================
# CONFIG
# =========================

SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "DOGEUSDT",
]

TRADE_SIZE_USD = 500.0

# Temporary assumptions for research
BINANCE_FEE = 0.001
OKX_FEE = 0.001
BYBIT_FEE = 0.001

NET_THRESHOLD = 0.0005
STALE_SECONDS = 2.0

REPORT_INTERVAL = 30


# =========================
# ORDER BOOK STORAGE
# =========================

books = {
    "binance": {},
    "okx": {},
    "bybit": {},
}


last_update = {
    "binance": {},
    "okx": {},
    "bybit": {},
}


# =========================
# STATISTICS
# =========================

stats = {
    "binance_okx": 0,
    "okx_binance": 0,
    "binance_bybit": 0,
    "bybit_binance": 0,
    "okx_bybit": 0,
    "bybit_okx": 0,
}


best_gross = {
    "value": -999,
    "route": None,
    "symbol": None,
}

best_net = {
    "value": -999,
    "route": None,
    "symbol": None,
    "pnl": None,
}


# =========================
# VWAP
# =========================

def calculate_vwap(levels, usd_size):

    remaining_usd = usd_size
    total_base = 0.0
    total_usd = 0.0

    for level in levels:

        if len(level) < 2:
            continue

        price = float(level[0])
        quantity = float(level[1])

        if price <= 0 or quantity <= 0:
            continue

        level_usd = price * quantity

        take_usd = min(
            remaining_usd,
            level_usd
        )

        if take_usd <= 0:
            continue

        base_amount = take_usd / price

        total_base += base_amount
        total_usd += take_usd

        remaining_usd -= take_usd

        if remaining_usd <= 0:
            break

    if total_base <= 0:
        return None

    if remaining_usd > 0:
        return None

    return total_usd / total_base


# =========================
# NET EDGE
# =========================

def calculate_net_edge(
    buy_price,
    sell_price,
    buy_fee,
    sell_fee
):

    gross = (sell_price - buy_price) / buy_price

    net = gross - buy_fee - sell_fee

    return gross, net


# =========================
# BINANCE
# =========================

async def binance_stream():

    streams = "/".join(
        symbol.lower() + "@depth20@100ms"
        for symbol in SYMBOLS
    )

    url = (
        "wss://stream.binance.com:9443/stream?streams="
        + streams
    )

    while True:

        try:

            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20
            ) as ws:

                async for message in ws:

                    data = json.loads(message)

                    stream_name = data.get("stream")

                    payload = data.get("data", {})

                    if not stream_name:
                        continue

                    symbol = stream_name.split("@")[0].upper()

                    bids = payload.get("bids", [])
                    asks = payload.get("asks", [])

                    if not bids or not asks:
                        continue

                    books["binance"][symbol] = {
                        "bids": bids,
                        "asks": asks,
                    }

                    last_update["binance"][symbol] = time.time()

        except Exception as e:

            print(
                "Binance error:",
                e
            )

            await asyncio.sleep(3)


# =========================
# OKX
# =========================

async def okx_stream():

    url = "wss://ws.okx.com/ws/v5/public"

    while True:

        try:

            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20
            ) as ws:

                args = []

                for symbol in SYMBOLS:

                    base = symbol.replace(
                        "USDT",
                        "-USDT"
                    )

                    args.append({
                        "channel": "books5",
                        "instId": base
                    })

                await ws.send(
                    json.dumps({
                        "op": "subscribe",
                        "args": args
                    })
                )

                async for message in ws:

                    data = json.loads(message)

                    if data.get("event") == "subscribe":
                        continue

                    arg = data.get("arg", {})

                    inst_id = arg.get("instId")

                    if not inst_id:
                        continue

                    symbol = inst_id.replace(
                        "-",
                        ""
                    )

                    payload = data.get("data")

                    if not payload:
                        continue

                    book = payload[0]

                    bids = book.get("bids", [])
                    asks = book.get("asks", [])

                    if not bids or not asks:
                        continue

                    books["okx"][symbol] = {
                        "bids": bids,
                        "asks": asks,
                    }

                    last_update["okx"][symbol] = time.time()

        except Exception as e:

            print(
                "OKX error:",
                e
            )

            await asyncio.sleep(3)


# =========================
# BYBIT
# =========================

async def bybit_stream():

    url = "wss://stream.bybit.com/v5/public/spot"

    while True:

        try:

            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20
            ) as ws:

                args = [
                    "orderbook.50." + symbol
                    for symbol in SYMBOLS
                ]

                await ws.send(
                    json.dumps({
                        "op": "subscribe",
                        "args": args
                    })
                )

                async for message in ws:

                    data = json.loads(message)

                    topic = data.get("topic", "")

                    if not topic.startswith(
                        "orderbook."
                    ):
                        continue

                    symbol = topic.split(".")[-1]

                    payload = data.get("data", {})

                    bids = payload.get("b", [])
                    asks = payload.get("a", [])

                    if not bids or not asks:
                        continue

                    books["bybit"][symbol] = {
                        "bids": bids,
                        "asks": asks,
                    }

                    last_update["bybit"][symbol] = time.time()

        except Exception as e:

            print(
                "Bybit error:",
                e
            )

            await asyncio.sleep(3)


# =========================
# FRESHNESS
# =========================

def is_fresh(exchange, symbol):

    timestamp = last_update[
        exchange
    ].get(symbol)

    if timestamp is None:
        return False

    return (
        time.time() - timestamp
        <= STALE_SECONDS
    )


# =========================
# MARKET CHECK
# =========================

def check_route(
    buy_exchange,
    sell_exchange,
    symbol,
    buy_fee,
    sell_fee,
    route_name
):

    if not is_fresh(
        buy_exchange,
        symbol
    ):
        return

    if not is_fresh(
        sell_exchange,
        symbol
    ):
        return

    buy_book = books[
        buy_exchange
    ].get(symbol)

    sell_book = books[
        sell_exchange
    ].get(symbol)

    if not buy_book or not sell_book:
        return

    buy_price = calculate_vwap(
        buy_book["asks"],
        TRADE_SIZE_USD
    )

    sell_price = calculate_vwap(
        sell_book["bids"],
        TRADE_SIZE_USD
    )

    if buy_price is None or sell_price is None:
        return

    gross, net = calculate_net_edge(
        buy_price,
        sell_price,
        buy_fee,
        sell_fee
    )

    # Track best gross
    if gross > best_gross["value"]:

        best_gross["value"] = gross
        best_gross["route"] = route_name
        best_gross["symbol"] = symbol

    # Track positive net observations
    if net > 0:

        stats[route_name] += 1

    # Track best net
    if net > best_net["value"]:

        pnl = TRADE_SIZE_USD * net

        best_net["value"] = net
        best_net["route"] = route_name
        best_net["symbol"] = symbol
        best_net["pnl"] = pnl


# =========================
# RUN ALL CHECKS
# =========================

def run_checks():

    for symbol in SYMBOLS:

        check_route(
            "binance",
            "okx",
            symbol,
            BINANCE_FEE,
            OKX_FEE,
            "binance_okx"
        )

        check_route(
            "okx",
            "binance",
            symbol,
            OKX_FEE,
            BINANCE_FEE,
            "okx_binance"
        )

        check_route(
            "binance",
            "bybit",
            symbol,
            BINANCE_FEE,
            BYBIT_FEE,
            "binance_bybit"
        )

        check_route(
            "bybit",
            "binance",
            symbol,
            BYBIT_FEE,
            BINANCE_FEE,
            "bybit_binance"
        )

        check_route(
            "okx",
            "bybit",
            symbol,
            OKX_FEE,
            BYBIT_FEE,
            "okx_bybit"
        )

        check_route(
            "bybit",
            "okx",
            symbol,
            BYBIT_FEE,
            OKX_FEE,
            "bybit_okx"
        )


# =========================
# STATUS
# =========================

def print_status():

    print()
    print("=" * 60)
    print("V4 MARKET INTELLIGENCE")
    print("=" * 60)

    print()
    print("Data status:")

    for exchange in [
        "binance",
        "okx",
        "bybit"
    ]:

        ready = sum(
            1
            for symbol in SYMBOLS
            if is_fresh(
                exchange,
                symbol
            )
        )

        print(
            f"{exchange.upper():8} "
            f"{ready}/{len(SYMBOLS)} symbols"
        )

    print()
    print("Opportunity counts:")

    for route, count in stats.items():

        print(
            f"{route:18} {count}"
        )

    print()
    print(
        "Best gross seen:"
    )

    if best_gross["route"]:

        print(
            f"{best_gross['route']} "
            f"{best_gross['symbol']} "
            f"{best_gross['value'] * 100:.4f}%"
        )

    else:

        print("None yet")

    print()
    print(
        "Best net seen:"
    )

    if best_net["route"]:

        print(
            f"{best_net['route']} "
            f"{best_net['symbol']} "
            f"{best_net['value'] * 100:.4f}%"
        )

        print(
            f"Estimated P&L on ${TRADE_SIZE_USD:.0f}: "
            f"${best_net['pnl']:.4f}"
        )

    else:

        print("None yet")

    print()
    print(
        "Threshold:",
        f"{NET_THRESHOLD * 100:.3f}%"
    )

    print(
        "Trade size:",
        f"${TRADE_SIZE_USD:.0f}"
    )

    print("=" * 60)


# =========================
# REPORT LOOP
# =========================

async def reporter():

    while True:

        await asyncio.sleep(
            REPORT_INTERVAL
        )

        run_checks()

        print_status()


# =========================
# MAIN
# =========================

async def main():

    await asyncio.gather(
        binance_stream(),
        okx_stream(),
        bybit_stream(),
        reporter()
    )


if __name__ == "__main__":

    asyncio.run(main())