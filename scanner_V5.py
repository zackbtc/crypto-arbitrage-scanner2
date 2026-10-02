import asyncio
import json
import time
import urllib.request
import websockets
import aiohttp

# ============================================================
# V5.1.1 FUNDING INTELLIGENCE
# DATA RELIABILITY VERSION
# Binance + Bybit + OKX
# PUBLIC DATA / PAPER ONLY
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

EXCHANGES = [
    "BINANCE",
    "BYBIT",
    "OKX",
]

# Data older than this is ignored.
STALE_SECONDS = 30

# How often the report is printed.
REPORT_INTERVAL = 15

# ------------------------------------------------------------
# Shared state
# ------------------------------------------------------------

funding = {
    symbol: {
        exchange: None
        for exchange in EXCHANGES
    }
    for symbol in SYMBOLS
}

intervals = {
    symbol: {
        exchange: None
        for exchange in EXCHANGES
    }
    for symbol in SYMBOLS
}

next_funding = {
    symbol: {
        exchange: None
        for exchange in EXCHANGES
    }
    for symbol in SYMBOLS
}

last_update = {
    symbol: {
        exchange: 0.0
        for exchange in EXCHANGES
    }
    for symbol in SYMBOLS
}

connection_status = {
    "BINANCE": False,
    "BYBIT": False,
    "OKX": False,
}


# ============================================================
# HELPERS
# ============================================================

def safe_float(value):
    try:
        if value is None:
            return None

        return float(value)

    except (ValueError, TypeError):
        return None


def funding_percent(rate):
    if rate is None:
        return None

    return rate * 100.0


def daily_rate(rate, interval_hours):
    if rate is None:
        return None

    if interval_hours is None:
        return None

    if interval_hours <= 0:
        return None

    return rate * (24.0 / interval_hours)


def annualized_rate(rate, interval_hours):
    daily = daily_rate(
        rate,
        interval_hours
    )

    if daily is None:
        return None

    return daily * 365.0


def is_fresh(symbol, exchange):
    timestamp = last_update[symbol][exchange]

    if timestamp <= 0:
        return False

    age = time.time() - timestamp

    return age <= STALE_SECONDS


def format_funding(rate):
    if rate is None:
        return "NO DATA"

    return f"{rate * 100:10.5f}%"


def format_apr(rate):
    if rate is None:
        return "NO DATA"

    return f"{rate * 100:10.5f}%"


def format_age(symbol, exchange):
    timestamp = last_update[symbol][exchange]

    if timestamp <= 0:
        return "never"

    age = max(
        0,
        time.time() - timestamp
    )

    return f"{age:.1f}s"


# ============================================================
# BINANCE
# ============================================================

async def binance_worker():

    url = (
        "https://fapi.binance.com"
        "/fapi/v1/premiumIndex"
    )

    while True:

        try:

            request = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0"
                },
            )

            with urllib.request.urlopen(
                request,
                timeout=10,
            ) as response:

                raw = response.read().decode(
                    "utf-8"
                )

                data = json.loads(raw)

            if not isinstance(data, list):
                raise ValueError(
                    "Unexpected Binance response"
                )

            received = 0

            for item in data:

                if not isinstance(item, dict):
                    continue

                symbol = item.get(
                    "symbol"
                )

                if symbol not in SYMBOLS:
                    continue

                rate = safe_float(
                    item.get(
                        "lastFundingRate"
                    )
                )

                if rate is None:
                    continue

                funding[symbol]["BINANCE"] = rate

                # Temporary research assumption.
                intervals[symbol]["BINANCE"] = 8.0

                next_time = item.get(
                    "nextFundingTime"
                )

                if next_time is not None:

                    try:

                        next_funding[symbol]["BINANCE"] = int(
                            next_time
                        )

                    except (ValueError, TypeError):
                        pass

                last_update[symbol]["BINANCE"] = time.time()

                received += 1

            connection_status["BINANCE"] = (
                received > 0
            )

            print(
                f"BINANCE funding updated: "
                f"{received}/{len(SYMBOLS)}"
            )

        except Exception as e:

            connection_status["BINANCE"] = False

            print(
                "BINANCE error:",
                type(e).__name__,
                str(e)
            )

        await asyncio.sleep(10)


# ============================================================
# BYBIT
# ============================================================

async def bybit_worker():
    url = "https://api.bybit.com/v5/market/tickers"

    while True:
        try:
            print("BYBIT worker running")

            async with aiohttp.ClientSession() as session:

                for symbol in SYMBOLS:

                    params = {
                        "category": "linear",
                        "symbol": symbol
                    }

                    async with session.get(
                        url,
                        params=params,
                        timeout=10
                    ) as response:

                        data = await response.json()

                        if symbol == "BTCUSDT":
                            print("BYBIT RESPONSE:", data)

                    if data.get("retCode") != 0:
                        print(
                            "BYBIT API ERROR:",
                            data.get("retCode"),
                            data.get("retMsg")
                        )
                        continue

                    result = data.get("result", {})
                    items = result.get("list", [])

                    if not items:
                        print("BYBIT NO ITEM:", symbol)
                        continue

                    item = items[0]

                    funding_rate = item.get("fundingRate")
                    next_funding_time = item.get("nextFundingTime")
                    interval_hours = item.get("fundingIntervalHour")

                    if funding_rate is None:
                        print("BYBIT NO FUNDING:", symbol)
                        continue

                    funding[symbol]["BYBIT"] = float(funding_rate)

                    if next_funding_time:
                        next_funding[symbol]["BYBIT"] = (
                            int(next_funding_time) / 1000
                        )

                    if interval_hours:
                        intervals[symbol]["BYBIT"] = float(
                            interval_hours
                        )
                    else:
                        intervals[symbol]["BYBIT"] = 8.0

                    last_update[symbol]["BYBIT"] = time.time()

            await asyncio.sleep(10)

        except Exception as e:
            connection_status["BYBIT"] = (
                f"ERROR: {str(e)[:80]}"
            )
            print("BYBIT ERROR:", e)

            await asyncio.sleep(5)


# ============================================================
# OKX
# ============================================================

async def okx_worker():

    url = (
        "wss://ws.okx.com"
        "/ws/v5/public"
    )

    while True:

        try:

            print("OKX connecting...")

            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
            ) as ws:

                connection_status["OKX"] = True

                print("OKX connected.")

                args = []

                for symbol in SYMBOLS:

                    args.append(
                        {
                            "channel": "funding-rate",
                            "instId": OKX_SYMBOLS[symbol],
                        }
                    )

                subscribe = {
                    "op": "subscribe",
                    "args": args,
                }

                await ws.send(
                    json.dumps(subscribe)
                )

                async for message in ws:

                    try:

                        payload = json.loads(
                            message
                        )

                        if payload.get(
                            "event"
                        ) in (
                            "subscribe",
                            "error",
                        ):
                            continue

                        arg = payload.get(
                            "arg",
                            {}
                        )

                        inst_id = arg.get(
                            "instId"
                        )

                        if not inst_id:
                            continue

                        symbol = None

                        for (
                            local_symbol,
                            okx_symbol,
                        ) in OKX_SYMBOLS.items():

                            if (
                                okx_symbol
                                == inst_id
                            ):

                                symbol = local_symbol
                                break

                        if symbol is None:
                            continue

                        data_list = payload.get(
                            "data"
                        )

                        if not isinstance(
                            data_list,
                            list
                        ):
                            continue

                        if not data_list:
                            continue

                        data = data_list[0]

                        if not isinstance(
                            data,
                            dict
                        ):
                            continue

                        rate = safe_float(
                            data.get(
                                "fundingRate"
                            )
                        )

                        if rate is None:
                            continue

                        funding[symbol]["OKX"] = rate

                        # Temporary research assumption.
                        intervals[symbol]["OKX"] = 8.0

                        next_time = data.get(
                            "nextFundingTime"
                        )

                        if next_time is not None:

                            try:

                                next_funding[symbol]["OKX"] = int(
                                    next_time
                                )

                            except (
                                ValueError,
                                TypeError
                            ):
                                pass

                        last_update[symbol]["OKX"] = time.time()

                    except Exception:
                        continue

        except Exception as e:

            connection_status["OKX"] = False

            print(
                "OKX error:",
                type(e).__name__,
                str(e)
            )

        print(
            "OKX reconnecting in 5 seconds..."
        )

        await asyncio.sleep(5)


# ============================================================
# DATA STATUS
# ============================================================

def print_data_status():

    print(
        "DATA STATUS"
    )

    for exchange in EXCHANGES:

        connected = connection_status[
            exchange
        ]

        fresh_count = sum(
            1
            for symbol in SYMBOLS
            if is_fresh(
                symbol,
                exchange
            )
        )

        mark = "OK" if connected else "OFF"

        print(
            f"{exchange:<8} "
            f"{mark:<3} "
            f"{fresh_count}/{len(SYMBOLS)} fresh"
        )


# ============================================================
# BEST SPREAD
# ============================================================

def calculate_best_spread(symbol):

    valid = []

    for exchange in EXCHANGES:

        if not is_fresh(
            symbol,
            exchange
        ):
            continue

        rate = funding[symbol][exchange]

        interval = intervals[
            symbol
        ][exchange]

        if rate is None:
            continue

        if interval is None:
            continue

        valid.append(
            (
                exchange,
                rate,
                interval,
            )
        )

    if len(valid) < 2:
        return None

    best = None

    for (
        exchange_a,
        rate_a,
        interval_a,
    ) in valid:

        for (
            exchange_b,
            rate_b,
            interval_b,
        ) in valid:

            if exchange_a == exchange_b:
                continue

            daily_a = daily_rate(
                rate_a,
                interval_a
            )

            daily_b = daily_rate(
                rate_b,
                interval_b
            )

            if (
                daily_a is None
                or daily_b is None
            ):
                continue

            spread = daily_a - daily_b

            if spread <= 0:
                continue

            candidate = {
                "high_exchange": exchange_a,
                "low_exchange": exchange_b,
                "spread": spread,
                "annualized": spread * 365.0,
            }

            if (
                best is None
                or spread > best["spread"]
            ):

                best = candidate

    return best


# ============================================================
# REPORT
# ============================================================

async def report_loop():

    while True:

        await asyncio.sleep(
            REPORT_INTERVAL
        )

        # Build the entire report first.
        # This prevents multiple workers from
        # interleaving the report output.

        lines = []

        lines.append("")
        lines.append(
            "=" * 70
        )
        lines.append(
            "V5.1.1 FUNDING INTELLIGENCE"
        )
        lines.append(
            "=" * 70
        )

        # Data status

        lines.append(
            "DATA STATUS"
        )

        for exchange in EXCHANGES:

            connected = connection_status[
                exchange
            ]

            fresh_count = sum(
                1
                for symbol in SYMBOLS
                if is_fresh(
                    symbol,
                    exchange
                )
            )

            status = "OK" if connected else "OFF"

            lines.append(
                f"{exchange:<8} "
                f"{status:<3} "
                f"{fresh_count}/{len(SYMBOLS)} fresh"
            )

        lines.append(
            "-" * 70
        )

        # Symbol reports

        for symbol in SYMBOLS:

            lines.append(
                symbol
            )

            for exchange in EXCHANGES:

                rate = funding[
                    symbol
                ][exchange]

                interval = intervals[
                    symbol
                ][exchange]

                fresh = is_fresh(
                    symbol,
                    exchange
                )

                if (
                    not fresh
                    or rate is None
                    or interval is None
                ):

                    age = format_age(
                        symbol,
                        exchange
                    )

                    lines.append(
                        f"{exchange:<8} "
                        f"NO DATA "
                        f"(age {age})"
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

                lines.append(
                    f"{exchange:<8} "
                    f"Funding: "
                    f"{format_funding(rate)} "
                    f"Daily: "
                    f"{format_funding(daily)} "
                    f"APR*: "
                    f"{format_apr(annual)} "
                    f"Age: "
                    f"{format_age(symbol, exchange)}"
                )

            best = calculate_best_spread(
                symbol
            )

            if best is None:

                lines.append(
                    "BEST FUNDING SPREAD: "
                    "NOT ENOUGH FRESH DATA"
                )

            else:

                lines.append(
                    "BEST FUNDING SPREAD:"
                )

                lines.append(
                    f"HIGH FUNDING: "
                    f"{best['high_exchange']}"
                )

                lines.append(
                    f"LOW FUNDING : "
                    f"{best['low_exchange']}"
                )

                lines.append(
                    "Daily funding difference: "
                    f"{best['spread'] * 100:.5f}%"
                )

                lines.append(
                    "Approx annualized: "
                    f"{best['annualized'] * 100:.5f}%"
                )

            lines.append("")

        lines.append(
            "-" * 70
        )

        lines.append(
            "* APR = simple annualization "
            "of the current funding rate."
        )

        lines.append(
            "* It is NOT a guaranteed return."
        )

        lines.append(
            "* No trades are executed."
        )

        lines.append(
            "=" * 70
        )

        print(
            "\n".join(lines)
        )


# ============================================================
# MAIN
# ============================================================

async def main():

    print(
        "=" * 70
    )

    print(
        "V5.1.1 FUNDING INTELLIGENCE STARTING"
    )

    print(
        "PUBLIC DATA / PAPER ONLY"
    )

    print(
        "=" * 70
    )

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

        print(
            "Stopped."
        )