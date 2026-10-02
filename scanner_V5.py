import asyncio
import json
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


# =========================
# DATA STORAGE
# =========================

data = {
    symbol: {
        "binance": None,
        "bybit": None,
        "okx": None,
    }
    for symbol in SYMBOLS
}


# =========================
# BINANCE
# =========================

async def binance_stream():

    streams = "/".join(
        symbol.lower() + "@bookTicker"
        for symbol in SYMBOLS
    )

    url = (
        "wss://fstream.binance.com/stream?streams="
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

                    msg = json.loads(message)

                    payload = msg.get("data", {})

                    symbol = payload.get("s")

                    if symbol not in SYMBOLS:
                        continue

                    bid = float(payload.get("b", 0))
                    ask = float(payload.get("a", 0))

                    if bid <= 0 or ask <= 0:
                        continue

                    data[symbol]["binance"] = {
                        "bid": bid,
                        "ask": ask,
                    }

        except Exception as e:

            print("Binance error:", e)

            await asyncio.sleep(3)


# =========================
# BYBIT
# =========================

async def bybit_stream():

    url = "wss://stream.bybit.com/v5/public/linear"

    while True:

        try:

            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20
            ) as ws:

                args = [
                    "tickers." + symbol
                    for symbol in SYMBOLS
                ]

                await ws.send(
                    json.dumps({
                        "op": "subscribe",
                        "args": args
                    })
                )

                async for message in ws:

                    msg = json.loads(message)

                    topic = msg.get("topic", "")

                    if not topic.startswith("tickers."):
                        continue

                    symbol = topic.split(".")[-1]

                    if symbol not in SYMBOLS:
                        continue

                    payload = msg.get("data", {})

                    bid = float(
                        payload.get(
                            "bid1Price",
                            0
                        )
                    )

                    ask = float(
                        payload.get(
                            "ask1Price",
                            0
                        )
                    )

                    if bid <= 0 or ask <= 0:
                        continue

                    data[symbol]["bybit"] = {
                        "bid": bid,
                        "ask": ask,
                    }

        except Exception as e:

            print("Bybit error:", e)

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

                    inst_id = (
                        symbol.replace(
                            "USDT",
                            "-USDT-SWAP"
                        )
                    )

                    args.append({
                        "channel": "tickers",
                        "instId": inst_id
                    })

                await ws.send(
                    json.dumps({
                        "op": "subscribe",
                        "args": args
                    })
                )

                async for message in ws:

                    msg = json.loads(message)

                    if msg.get("event") == "subscribe":
                        continue

                    arg = msg.get("arg", {})

                    inst_id = arg.get("instId")

                    if not inst_id:
                        continue

                    symbol = (
                        inst_id
                        .replace(
                            "-USDT-SWAP",
                            "USDT"
                        )
                    )

                    if symbol not in SYMBOLS:
                        continue

                    payload = msg.get("data", [])

                    if not payload:
                        continue

                    ticker = payload[0]

                    bid = float(
                        ticker.get(
                            "bidPx",
                            0
                        )
                    )

                    ask = float(
                        ticker.get(
                            "askPx",
                            0
                        )
                    )

                    if bid <= 0 or ask <= 0:
                        continue

                    data[symbol]["okx"] = {
                        "bid": bid,
                        "ask": ask,
                    }

        except Exception as e:

            print("OKX error:", e)

            await asyncio.sleep(3)


# =========================
# REPORT
# =========================

async def reporter():

    while True:

        await asyncio.sleep(10)

        print()
        print("=" * 60)
        print("V5 FUNDING & BASIS — MARKET DATA")
        print("=" * 60)

        for symbol in SYMBOLS:

            print()
            print(symbol)

            for exchange in [
                "binance",
                "bybit",
                "okx"
            ]:

                value = data[symbol][exchange]

                if value:

                    print(
                        f"{exchange.upper():8} "
                        f"Bid: {value['bid']} "
                        f"Ask: {value['ask']}"
                    )

                else:

                    print(
                        f"{exchange.upper():8} "
                        f"NO DATA"
                    )

        print("=" * 60)


# =========================
# MAIN
# =========================

async def main():

    await asyncio.gather(
        binance_stream(),
        bybit_stream(),
        okx_stream(),
        reporter()
    )


if __name__ == "__main__":

    asyncio.run(main())