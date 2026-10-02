import asyncio
import json
import time
import websockets


# ==========================================
# SETTINGS
# ==========================================

SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "DOGEUSDT",
]

TRADE_SIZE_USD = 500

# Temporary assumptions.
# We will replace these with your actual fee tiers later.
BINANCE_FEE = 0.001
OKX_FEE = 0.001

# Minimum NET edge required.
MIN_NET_EDGE = 0.0005


# ==========================================
# WEBSOCKET URLS
# ==========================================

BINANCE_URL = "wss://stream.binance.com:9443/stream"
OKX_URL = "wss://ws.okx.com/ws/v5/public"


# ==========================================
# LOCAL MARKET DATA
# ==========================================

books = {
    "binance": {},
    "okx": {}
}


# ==========================================
# VWAP
# ==========================================

def calculate_vwap(levels, usd_size):

    remaining_usd = usd_size
    total_base = 0.0
    total_usd = 0.0

    for price, quantity in levels:

        price = float(price)
        quantity = float(quantity)

        level_usd = price * quantity

        take_usd = min(remaining_usd, level_usd)

        if take_usd <= 0:
            continue

        base_amount = take_usd / price

        total_base += base_amount
        total_usd += take_usd

        remaining_usd -= take_usd

        if remaining_usd <= 0:
            break

    if total_base == 0:
        return None

    if remaining_usd > 0:
        return None

    return total_usd / total_base


# ==========================================
# BINANCE
# ==========================================

async def binance():

    streams = [
        f"{symbol.lower()}@depth20@100ms"
        for symbol in SYMBOLS
    ]

    params = {
        "method": "SUBSCRIBE",
        "params": streams,
        "id": 1
    }

    while True:

        try:

            async with websockets.connect(
                BINANCE_URL,
                ping_interval=20,
                ping_timeout=20
            ) as ws:

                await ws.send(json.dumps(params))

                async for message in ws:

                    data = json.loads(message)

                    if "data" not in data:
                        continue

                    payload = data["data"]

                    symbol = payload.get("s")

                    if symbol not in SYMBOLS:
                        continue

                    bids = payload.get("bids", [])
                    asks = payload.get("asks", [])

                    if not bids or not asks:
                        continue

                    books["binance"][symbol] = {
                        "bids": bids,
                        "asks": asks,
                        "timestamp": time.time()
                    }

        except Exception as e:

            print("Binance connection error:", e)

            await asyncio.sleep(3)


# ==========================================
# OKX
# ==========================================

async def okx():

    while True:

        try:

            async with websockets.connect(
                OKX_URL,
                ping_interval=20,
                ping_timeout=20
            ) as ws:

                args = [
                    {
                        "channel": "books5",
                        "instId": symbol.replace(
                            "USDT",
                            "-USDT"
                        )
                    }
                    for symbol in SYMBOLS
                ]

                subscribe = {
                    "op": "subscribe",
                    "args": args
                }

                await ws.send(
                    json.dumps(subscribe)
                )

                async for message in ws:

                    if message == "pong":
                        continue

                    data = json.loads(message)

                    if "data" not in data:
                        continue

                    if not data["data"]:
                        continue

                    book = data["data"][0]

                    bids = book.get("bids", [])
                    asks = book.get("asks", [])

                    if not bids or not asks:
                        continue

                    inst_id = data["arg"]["instId"]

                    symbol = inst_id.replace(
                        "-",
                        ""
                    )

                    books["okx"][symbol] = {
                        "bids": bids,
                        "asks": asks,
                        "timestamp": time.time()
                    }

        except Exception as e:

            print("OKX connection error:", e)

            await asyncio.sleep(3)


# ==========================================
# ARBITRAGE ENGINE
# ==========================================

def check_arbitrage():

    for symbol in SYMBOLS:

        binance_book = books["binance"].get(symbol)
        okx_book = books["okx"].get(symbol)

        if not binance_book or not okx_book:
            continue

        # Ignore stale data
        now = time.time()

        if now - binance_book["timestamp"] > 2:
            continue

        if now - okx_book["timestamp"] > 2:
            continue


        # ======================================
        # Binance -> OKX
        # ======================================

        buy_vwap = calculate_vwap(
            binance_book["asks"],
            TRADE_SIZE_USD
        )

        sell_vwap = calculate_vwap(
            okx_book["bids"],
            TRADE_SIZE_USD
        )

        if buy_vwap and sell_vwap:

            gross_edge = (
                sell_vwap - buy_vwap
            ) / buy_vwap

            fees = (
                BINANCE_FEE +
                OKX_FEE
            )

            net_edge = gross_edge - fees

            expected_profit = (
                TRADE_SIZE_USD *
                net_edge
            )

            if net_edge >= MIN_NET_EDGE:

                print(
                    f"\n🚨 NET ARBITRAGE\n"
                    f"{symbol}\n"
                    f"\n"
                    f"BUY Binance VWAP: "
                    f"{buy_vwap:.6f}\n"
                    f"SELL OKX VWAP:   "
                    f"{sell_vwap:.6f}\n"
                    f"\n"
                    f"Gross: "
                    f"{gross_edge * 100:.3f}%\n"
                    f"Fees: "
                    f"{fees * 100:.3f}%\n"
                    f"NET: "
                    f"{net_edge * 100:.3f}%\n"
                    f"\n"
                    f"Size: ${TRADE_SIZE_USD}\n"
                    f"Estimated P&L: "
                    f"${expected_profit:.2f}\n"
                )


        # ======================================
        # OKX -> Binance
        # ======================================

        buy_vwap = calculate_vwap(
            okx_book["asks"],
            TRADE_SIZE_USD
        )

        sell_vwap = calculate_vwap(
            binance_book["bids"],
            TRADE_SIZE_USD
        )

        if buy_vwap and sell_vwap:

            gross_edge = (
                sell_vwap - buy_vwap
            ) / buy_vwap

            fees = (
                OKX_FEE +
                BINANCE_FEE
            )

            net_edge = gross_edge - fees

            expected_profit = (
                TRADE_SIZE_USD *
                net_edge
            )

            if net_edge >= MIN_NET_EDGE:

                print(
                    f"\n🚨 NET ARBITRAGE\n"
                    f"{symbol}\n"
                    f"\n"
                    f"BUY OKX VWAP:     "
                    f"{buy_vwap:.6f}\n"
                    f"SELL Binance VWAP: "
                    f"{sell_vwap:.6f}\n"
                    f"\n"
                    f"Gross: "
                    f"{gross_edge * 100:.3f}%\n"
                    f"Fees: "
                    f"{fees * 100:.3f}%\n"
                    f"NET: "
                    f"{net_edge * 100:.3f}%\n"
                    f"\n"
                    f"Size: ${TRADE_SIZE_USD}\n"
                    f"Estimated P&L: "
                    f"${expected_profit:.2f}\n"
                )


# ==========================================
# MONITOR
# ==========================================

async def monitor():

    while True:

        check_arbitrage()

        await asyncio.sleep(0.25)


# ==========================================
# MAIN
# ==========================================

async def main():

    await asyncio.gather(
        binance(),
        okx(),
        monitor()
    )


if __name__ == "__main__":

    asyncio.run(main())