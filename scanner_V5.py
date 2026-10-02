import asyncio
import json
import time
import websockets

# ============================================================
# V5.1 FUNDING INTELLIGENCE
# Binance + Bybit + OKX
# PAPER / PUBLIC DATA ONLY
# ============================================================

SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "DOGEUSDT",
]

OKX_SYMBOLS = {
    "BTCUSDT": "BTC-USDT-SWAP",
    "ETHUSDT": "ETH-USDT-SWAP",
    "SOLUSDT": "SOL-USDT-SWAP",
    "XRPUSDT": "XRP-USDT-SWAP",
    "DOGEUSDT": "DOGE-USDT-SWAP",
}

# ------------------------------------------------------------
# Shared market data
# ------------------------------------------------------------

funding = {
    symbol: {
        "BINANCE": None,
        "BYBIT": None,
        "OKX": None,
    }
    for symbol in SYMBOLS
}

intervals = {
    symbol: {
        "BINANCE": None,
        "BYBIT": None,
        "OKX": None,
    }
    for symbol in SYMBOLS
}

next_funding = {
    symbol: {
        "BINANCE": None,
        "BYBIT": None,
        "OKX": None,
    }
    for symbol in SYMBOLS
}

last_update = {
    symbol: {
        "BINANCE": 0,
        "BYBIT": 0,
        "OKX": 0,
    }
    for symbol in SYMBOLS
}


# ============================================================
# Helpers
# ============================================================

def safe_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except (ValueError, TypeError):
        return None


def funding_percent(rate):
    """
    Convert decimal funding rate to percentage.

    Example:
    0.0001 -> 0.01000%
    """
    if rate is None:
        return None

    return rate * 100.0


def daily_rate(rate, interval_hours):
    """
    Simple daily annualization.

    rate = funding rate per settlement
    interval_hours = settlement interval

    Example:
    0.0001 every 8h
    => 0.0003 per day
    """
    if rate is None or interval_hours is None:
        return None

    if interval_hours <= 0:
        return None

    return rate * (24.0 / interval_hours)


def annualized_rate(rate, interval_hours):
    daily = daily_rate(rate, interval_hours)

    if daily is None:
        return None

    return daily * 365.0


def fmt_percent(value):
    if value is None:
        return "NO DATA"

    return f"{value * 100:.5f}%"


def fmt_apr(value):
    if value is None:
        return "NO DATA"

    return f"{value * 100:.5f}%"


# ============================================================
# BINANCE
# ============================================================

async def binance_worker():
    """
    Binance USD-M Futures mark price stream.

    The markPrice stream contains:
    p = mark price
    i = index price
    r = funding rate
    T = next funding time
    """

    streams = "/".join(
        f"{symbol.lower()}@markPrice@1s"
        for symbol in SYMBOLS
    )

    url = (
        "wss://fstream.binance.com/stream?streams="
        + streams
    )

    while True:

        try:
            print("BINANCE connecting...")

            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
            ) as ws:

                print("BINANCE connected.")

                async for message in ws:

                    try:
                        payload = json.loads(message)

                        # Combined Binance stream:
                        # {
                        #   "stream": "...",
                        #   "data": {...}
                        # }

                        data = payload.get("data")

                        if not isinstance(data, dict):
                            continue

                        symbol = data.get("s")

                        if not symbol:
                            stream_name = payload.get("stream")

                            if stream_name:
                                symbol = (
                                    stream_name
                                    .split("@")[0]
                                    .upper()
                                )

                        if symbol not in SYMBOLS:
                            continue

                        rate = safe_float(data.get("r"))

                        if rate is None:
                            continue

                        funding[symbol]["BINANCE"] = rate

                        # Binance markPrice stream does not reliably
                        # expose instrument-specific interval.
                        # We use 8h only as an initial research value.
                        intervals[symbol]["BINANCE"] = 8.0

                        next_time = data.get("T")

                        if next_time is not None:
                            next_funding[symbol]["BINANCE"] = (
                                int(next_time)
                            )

                        last_update[symbol]["BINANCE"] = time.time()

                    except Exception:
                        continue

        except Exception as e:
            print(
                f"BINANCE connection error: {type(e).__name__}"
            )

        print("BINANCE reconnecting in 5 seconds...")
        await asyncio.sleep(5)


# ============================================================
# BYBIT
# ============================================================

async def bybit_worker():
    """
    Bybit V5 linear perpetual ticker.

    Topic:
    tickers.BTCUSDT

    Important fields:
    fundingRate
    fundingIntervalHour
    nextFundingTime
    markPrice
    indexPrice
    """

    url = "wss://stream.bybit.com/v5/public/linear"

    while True:

        try:
            print("BYBIT connecting...")

            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
            ) as ws:

                print("BYBIT connected.")

                subscribe_message = {
                    "op": "subscribe",
                    "args": [
                        f"tickers.{symbol}"
                        for symbol in SYMBOLS
                    ],
                }

                await ws.send(
                    json.dumps(subscribe_message)
                )

                async for message in ws:

                    try:
                        payload = json.loads(message)

                        topic = payload.get("topic", "")

                        if not topic.startswith("tickers."):
                            continue

                        symbol = topic.replace(
                            "tickers.",
                            ""
                        )

                        if symbol not in SYMBOLS:
                            continue

                        data = payload.get("data")

                        if isinstance(data, list):
                            if not data:
                                continue
                            data = data[0]

                        if not isinstance(data, dict):
                            continue

                        rate = safe_float(
                            data.get("fundingRate")
                        )

                        interval = safe_float(
                            data.get("fundingIntervalHour")
                        )

                        next_time = data.get(
                            "nextFundingTime"
                        )

                        # We specifically require a real
                        # fundingRate field.
                        if rate is None:
                            continue

                        funding[symbol]["BYBIT"] = rate

                        if interval is not None and interval > 0:
                            intervals[symbol]["BYBIT"] = interval
                        else:
                            intervals[symbol]["BYBIT"] = 8.0

                        if next_time is not None:
                            try:
                                next_funding[symbol]["BYBIT"] = int(
                                    next_time
                                )
                            except Exception:
                                pass

                        last_update[symbol]["BYBIT"] = time.time()

                    except Exception:
                        continue

        except Exception as e:
            print(
                f"BYBIT connection error: {type(e).__name__}"
            )

        print("BYBIT reconnecting in 5 seconds...")
        await asyncio.sleep(5)


# ============================================================
# OKX
# ============================================================

async def okx_worker():
    """
    OKX public funding-rate channel.
    """

    url = "wss://ws.okx.com/ws/v5/public"

    while True:

        try:
            print("OKX connecting...")

            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
            ) as ws:

                print("OKX connected.")

                args = []

                for symbol in SYMBOLS:
                    args.append({
                        "channel": "funding-rate",
                        "instId": OKX_SYMBOLS[symbol],
                    })

                subscribe_message = {
                    "op": "subscribe",
                    "args": args,
                }

                await ws.send(
                    json.dumps(subscribe_message)
                )

                async for message in ws:

                    try:
                        payload = json.loads(message)

                        if payload.get("event") in (
                            "subscribe",
                            "error",
                        ):
                            continue

                        arg = payload.get("arg", {})

                        inst_id = arg.get("instId")

                        if not inst_id:
                            continue

                        symbol = None

                        for local_symbol, okx_symbol in OKX_SYMBOLS.items():
                            if okx_symbol == inst_id:
                                symbol = local_symbol
                                break

                        if symbol is None:
                            continue

                        data_list = payload.get("data")

                        if not isinstance(data_list, list):
                            continue

                        if not data_list:
                            continue

                        data = data_list[0]

                        rate = safe_float(
                            data.get("fundingRate")
                        )

                        if rate is None:
                            continue

                        funding[symbol]["OKX"] = rate

                        # Initial research assumption.
                        # We will later verify OKX intervals
                        # instrument by instrument.
                        intervals[symbol]["OKX"] = 8.0

                        next_time = data.get(
                            "nextFundingTime"
                        )

                        if next_time is not None:
                            try:
                                next_funding[symbol]["OKX"] = int(
                                    next_time
                                )
                            except Exception:
                                pass

                        last_update[symbol]["OKX"] = time.time()

                    except Exception:
                        continue

        except Exception as e:
            print(
                f"OKX connection error: {type(e).__name__}"
            )

        print("OKX reconnecting in 5 seconds...")
        await asyncio.sleep(5)


# ============================================================
# REPORT
# ============================================================

def exchange_status(symbol, exchange):
    updated = last_update[symbol][exchange]

    if updated == 0:
        return False

    # Data older than 30 seconds is considered stale.
    return (time.time() - updated) <= 30


def calculate_best_spread(symbol):
    valid = []

    for exchange in ["BINANCE", "BYBIT", "OKX"]:

        rate = funding[symbol][exchange]
        interval = intervals[symbol][exchange]

        if rate is None:
            continue

        if interval is None:
            continue

        if not exchange_status(symbol, exchange):
            continue

        valid.append(
            (exchange, rate, interval)
        )

    if len(valid) < 2:
        return None

    best = None

    for short_exchange, short_rate, short_interval in valid:

        for long_exchange, long_rate, long_interval in valid:

            if short_exchange == long_exchange:
                continue

            # We want to receive more funding from the
            # short side than we pay on the long side.
            #
            # Simple daily comparison.
            short_daily = daily_rate(
                short_rate,
                short_interval
            )

            long_daily = daily_rate(
                long_rate,
                long_interval
            )

            if short_daily is None or long_daily is None:
                continue

            spread = short_daily - long_daily

            if spread <= 0:
                continue

            candidate = {
                "short_exchange": short_exchange,
                "long_exchange": long_exchange,
                "spread": spread,
                "annualized": spread * 365.0,
            }

            if best is None:
                best = candidate
            elif spread > best["spread"]:
                best = candidate

    return best


def print_report():

    print()
    print("=" * 70)
    print("V5.1 FUNDING INTELLIGENCE")
    print("=" * 70)

    for symbol in SYMBOLS:

        print(symbol)

        for exchange in ["BINANCE", "BYBIT", "OKX"]:

            rate = funding[symbol][exchange]
            interval = intervals[symbol][exchange]

            if (
                rate is None
                or interval is None
                or not exchange_status(symbol, exchange)
            ):
                print(
                    f"{exchange:<8} NO DATA"
                )
                continue

            daily = daily_rate(
                rate,
                interval
            )

            annual = annualized_rate(
                rate,
                interval
            )

            print(
                f"{exchange:<8}"
                f"Funding: {funding_percent(rate):>10.5f}% "
                f"Daily: {funding_percent(daily):>10.5f}% "
                f"APR*: {fmt_apr(annual):>10}"
            )

        best = calculate_best_spread(symbol)

        if best is None:

            print(
                "BEST FUNDING SPREAD: "
                "NOT ENOUGH VALID DATA"
            )

        else:

            print("BEST FUNDING SPREAD:")

            print(
                f"SHORT {best['short_exchange']} | "
                f"LONG {best['long_exchange']}"
            )

            print(
                "Daily funding advantage: "
                f"{funding_percent(best['spread']):.5f}%"
            )

            print(
                "Approx annualized: "
                f"{best['annualized'] * 100:.5f}%"
            )

        print()

    print("-" * 70)
    print(
        "* APR = simple annualization of the current "
        "funding rate."
    )
    print(
        "* It is NOT a guaranteed return."
    )
    print(
        "* No trades are executed."
    )
    print("=" * 70)


# ============================================================
# REPORT LOOP
# ============================================================

async def report_loop():

    while True:

        await asyncio.sleep(15)

        print_report()


# ============================================================
# MAIN
# ============================================================

async def main():

    print("=" * 70)
    print("V5.1 FUNDING INTELLIGENCE STARTING")
    print("PUBLIC DATA / PAPER ONLY")
    print("=" * 70)

    await asyncio.gather(
        binance_worker(),
        bybit_worker(),
        okx_worker(),
        report_loop(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped.")