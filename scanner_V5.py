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
        symbol.lower() + "@markPrice@1s"
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

                    mark_price = float(
                        payload.get("p", 0)
                    )

                    index_price = float(
                        payload.get("i", 0)
                    )

                    funding_rate = float(
                        payload.get("r", 0)
                    )

                    next_funding = int(
                        payload.get("T", 0)
                    )

                    if mark_price <= 0:
                        continue

                    data[symbol]["binance"] = {
                        "mark": mark_price,
                        "index": index_price,
                        "funding": funding_rate,
                        "next_funding": next_funding,
                        "interval_hours": 8.0,
                        "timestamp": time.time(),
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

                    if not topic.startswith(
                        "tickers."
                    ):
                        continue

                    symbol = topic.split(".")[-1]

                    if symbol not in SYMBOLS:
                        continue

                    payload = msg.get(
                        "data",
                        {}
                    )

                    mark_price = float(
                        payload.get(
                            "markPrice",
                            0
                        )
                    )

                    index_price = float(
                        payload.get(
                            "indexPrice",
                            0
                        )
                    )

                    funding_rate = float(
                        payload.get(
                            "fundingRate",
                            0
                        )
                    )

                    next_funding = int(
                        payload.get(
                            "nextFundingTime",
                            0
                        )
                    )

                    interval_hours = float(
                        payload.get(
                            "fundingIntervalHour",
                            8
                        )
                        or 8
                    )

                    if mark_price <= 0:
                        continue

                    data[symbol]["bybit"] = {
                        "mark": mark_price,
                        "index": index_price,
                        "funding": funding_rate,
                        "next_funding": next_funding,
                        "interval_hours": interval_hours,
                        "timestamp": time.time(),
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
                        "channel": "funding-rate",
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

                    arg = msg.get(
                        "arg",
                        {}
                    )

                    if arg.get("channel") != "funding-rate":
                        continue

                    inst_id = arg.get("instId")

                    if not inst_id:
                        continue

                    symbol = (
                        inst_id.replace(
                            "-USDT-SWAP",
                            "USDT"
                        )
                    )

                    if symbol not in SYMBOLS:
                        continue

                    payload = msg.get(
                        "data",
                        []
                    )

                    if not payload:
                        continue

                    funding_data = payload[0]

                    funding_rate = float(
                        funding_data.get(
                            "fundingRate",
                            0
                        )
                    )

                    next_funding = int(
                        funding_data.get(
                            "nextFundingTime",
                            0
                        )
                    )

                    # OKX funding intervals can vary.
                    # Use 8h as the initial research assumption.
                    interval_hours = 8.0

                    data[symbol]["okx"] = {
                        "mark": None,
                        "index": None,
                        "funding": funding_rate,
                        "next_funding": next_funding,
                        "interval_hours": interval_hours,
                        "timestamp": time.time(),
                    }

        except Exception as e:

            print("OKX error:", e)

            await asyncio.sleep(3)


# =========================
# FRESHNESS
# =========================

def fresh(exchange_data):

    if not exchange_data:
        return False

    return (
        time.time()
        - exchange_data["timestamp"]
        < 120
    )


# =========================
# FUNDING CALCULATIONS
# =========================

def daily_funding_rate(
    funding_rate,
    interval_hours
):

    if interval_hours <= 0:
        return 0

    periods_per_day = (
        24 / interval_hours
    )

    return (
        funding_rate
        * periods_per_day
    )


def annualized_funding_rate(
    funding_rate,
    interval_hours
):

    return (
        daily_funding_rate(
            funding_rate,
            interval_hours
        )
        * 365
    )


def format_percent(value):

    return f"{value * 100:.5f}%"


# =========================
# REPORT
# =========================

async def reporter():

    while True:

        await asyncio.sleep(10)

        print()
        print("=" * 70)
        print("V5.1 FUNDING INTELLIGENCE")
        print("=" * 70)

        for symbol in SYMBOLS:

            print()
            print(symbol)

            available = []

            for exchange in [
                "binance",
                "bybit",
                "okx"
            ]:

                value = data[symbol][exchange]

                if not fresh(value):
                    print(
                        f"{exchange.upper():8} NO DATA"
                    )
                    continue

                available.append(
                    (
                        exchange,
                        value
                    )
                )

                funding = value["funding"]

                daily = daily_funding_rate(
                    funding,
                    value["interval_hours"]
                )

                annual = annualized_funding_rate(
                    funding,
                    value["interval_hours"]
                )

                print(
                    f"{exchange.upper():8} "
                    f"Funding: "
                    f"{format_percent(funding):>10} "
                    f"Daily: "
                    f"{format_percent(daily):>10} "
                    f"APR*: "
                    f"{format_percent(annual):>10}"
                )

            # =====================
            # BEST FUNDING SPREAD
            # =====================

            if len(available) >= 2:

                best_pair = None

                best_spread = -999

                for exchange_a, data_a in available:

                    for exchange_b, data_b in available:

                        if exchange_a == exchange_b:
                            continue

                        # Normalize each funding rate to daily
                        rate_a = daily_funding_rate(
                            data_a["funding"],
                            data_a["interval_hours"]
                        )

                        rate_b = daily_funding_rate(
                            data_b["funding"],
                            data_b["interval_hours"]
                        )

                        spread = rate_a - rate_b

                        if spread > best_spread:

                            best_spread = spread

                            best_pair = (
                                exchange_a,
                                exchange_b,
                                rate_a,
                                rate_b
                            )

                if best_pair:

                    short_exchange = (
                        best_pair[0]
                    )

                    long_exchange = (
                        best_pair[1]
                    )

                    short_rate = (
                        best_pair[2]
                    )

                    long_rate = (
                        best_pair[3]
                    )

                    print()

                    print(
                        "BEST FUNDING SPREAD:"
                    )

                    print(
                        f"SHORT {short_exchange.upper()} "
                        f"| LONG {long_exchange.upper()}"
                    )

                    print(
                        f"Daily funding advantage: "
                        f"{format_percent(best_spread)}"
                    )

                    print(
                        f"Approx annualized: "
                        f"{format_percent(best_spread * 365)}"
                    )

        print()
        print(
            "* APR is a simple annualization of the "
            "current funding rate. It is NOT a guaranteed return."
        )

        print("=" * 70)


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