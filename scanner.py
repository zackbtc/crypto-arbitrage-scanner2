# ============================================================
# PERSONAL QUANT SYSTEM — V1
# DATA ENGINE
# ============================================================
#
# Purpose:
#   Collect public futures/perpetual market data from:
#       - Binance
#       - Bybit
#       - OKX
#
#   Symbols:
#       BTCUSDT
#       ETHUSDT
#       SOLUSDT
#       XRPUSDT
#       DOGEUSDT
#
#   V1 does NOT:
#       - place orders
#       - use API keys
#       - calculate trading signals
#       - recommend LONG/SHORT
#
# ============================================================

import asyncio
import aiohttp
import time
from datetime import datetime, timezone


# ============================================================
# CONFIGURATION
# ============================================================

SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "DOGEUSDT",
]

UPDATE_INTERVAL = 10
STALE_SECONDS = 30
REQUEST_TIMEOUT = 8


# ============================================================
# ENDPOINTS
# ============================================================

BINANCE_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"

BYBIT_URL = "https://api.bybit.com/v5/market/tickers"

OKX_URL = "https://www.okx.com/api/v5/market/ticker"


# ============================================================
# DATA STORAGE
# ============================================================

market_data = {
    "BINANCE": {},
    "BYBIT": {},
    "OKX": {},
}


# ============================================================
# HELPERS
# ============================================================

def now_ms():
    return int(time.time() * 1000)


def utc_time():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def is_fresh(timestamp_ms):
    if not timestamp_ms:
        return False

    age = (now_ms() - timestamp_ms) / 1000

    return age <= STALE_SECONDS


def safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# ============================================================
# BINANCE
# ============================================================

async def fetch_binance(session):

    try:

        async with session.get(
            BINANCE_URL,
            timeout=REQUEST_TIMEOUT
        ) as response:

            if response.status != 200:
                print(
                    f"[BINANCE] HTTP ERROR: {response.status}"
                )
                return

            data = await response.json()

            for item in data:

                symbol = item.get("symbol")

                if symbol not in SYMBOLS:
                    continue

                market_data["BINANCE"][symbol] = {
                    "timestamp": now_ms(),

                    "price": safe_float(
                        item.get("markPrice")
                    ),

                    "bid": None,
                    "ask": None,

                    "mark_price": safe_float(
                        item.get("markPrice")
                    ),

                    "index_price": safe_float(
                        item.get("indexPrice")
                    ),

                    "funding_rate": safe_float(
                        item.get("lastFundingRate")
                    ),

                    "next_funding_time": item.get(
                        "nextFundingTime"
                    ),

                    "volume": None,
                }

    except Exception as e:

        print(f"[BINANCE] ERROR: {e}")


# ============================================================
# BYBIT
# ============================================================

async def fetch_bybit(session):

    try:

        for symbol in SYMBOLS:

            params = {
                "category": "linear",
                "symbol": symbol,
            }

            async with session.get(
                BYBIT_URL,
                params=params,
                timeout=REQUEST_TIMEOUT
            ) as response:

                if response.status != 200:
                    print(
                        f"[BYBIT] HTTP ERROR {symbol}: "
                        f"{response.status}"
                    )
                    continue

                data = await response.json()

                result = data.get(
                    "result",
                    {}
                )

                items = result.get(
                    "list",
                    []
                )

                if not items:
                    continue

                item = items[0]

                market_data["BYBIT"][symbol] = {
                    "timestamp": now_ms(),

                    "price": safe_float(
                        item.get("lastPrice")
                    ),

                    "bid": safe_float(
                        item.get("bid1Price")
                    ),

                    "ask": safe_float(
                        item.get("ask1Price")
                    ),

                    "mark_price": safe_float(
                        item.get("markPrice")
                    ),

                    "index_price": safe_float(
                        item.get("indexPrice")
                    ),

                    "funding_rate": safe_float(
                        item.get("fundingRate")
                    ),

                    "next_funding_time": item.get(
                        "nextFundingTime"
                    ),

                    "funding_interval_hour": safe_float(
                        item.get("fundingIntervalHour")
                    ),

                    "volume": safe_float(
                        item.get("volume24h")
                    ),
                }

    except Exception as e:

        print(f"[BYBIT] ERROR: {e}")


# ============================================================
# OKX
# ============================================================

async def fetch_okx(session):

    try:

        for symbol in SYMBOLS:

            inst_id = symbol.replace(
                "USDT",
                "-USDT-SWAP"
            )

            params = {
                "instId": inst_id
            }

            async with session.get(
                OKX_URL,
                params=params,
                timeout=REQUEST_TIMEOUT
            ) as response:

                if response.status != 200:
                    print(
                        f"[OKX] HTTP ERROR {symbol}: "
                        f"{response.status}"
                    )
                    continue

                data = await response.json()

                items = data.get(
                    "data",
                    []
                )

                if not items:
                    continue

                item = items[0]

                market_data["OKX"][symbol] = {
                    "timestamp": now_ms(),

                    "price": safe_float(
                        item.get("last")
                    ),

                    "bid": safe_float(
                        item.get("bidPx")
                    ),

                    "ask": safe_float(
                        item.get("askPx")
                    ),

                    "mark_price": None,

                    "index_price": None,

                    "funding_rate": None,

                    "next_funding_time": None,

                    "volume": safe_float(
                        item.get("vol24h")
                    ),
                }

    except Exception as e:

        print(f"[OKX] ERROR: {e}")


# ============================================================
# STATUS
# ============================================================

def get_exchange_status(exchange):

    total = len(SYMBOLS)

    fresh = 0

    for symbol in SYMBOLS:

        data = market_data[exchange].get(symbol)

        if data and is_fresh(
            data.get("timestamp")
        ):
            fresh += 1

    if fresh == total:
        status = "OK"

    elif fresh > 0:
        status = "PARTIAL"

    else:
        status = "OFFLINE"

    return status, fresh, total


# ============================================================
# FORMAT HELPERS
# ============================================================

def format_percent(value):

    if value is None:
        return "N/A"

    return f"{value * 100:+.5f}%"


def format_price(value):

    if value is None:
        return "N/A"

    return f"${value:,.4f}"


# ============================================================
# REPORT
# ============================================================

def print_report():

    print("\n")
    print("=" * 72)
    print("             PERSONAL QUANT SYSTEM — V1")
    print("                    DATA ENGINE")
    print("=" * 72)

    print(f"UTC: {utc_time()}")
    print()

    print("CONNECTION STATUS")
    print("-" * 72)

    for exchange in [
        "BINANCE",
        "BYBIT",
        "OKX",
    ]:

        status, fresh, total = get_exchange_status(
            exchange
        )

        print(
            f"{exchange:<10} "
            f"{status:<8} "
            f"{fresh}/{total} fresh"
        )

    print()
    print("=" * 72)

    for symbol in SYMBOLS:

        print()
        print(f" {symbol}")
        print("-" * 72)

        for exchange in [
            "BINANCE",
            "BYBIT",
            "OKX",
        ]:

            data = market_data[
                exchange
            ].get(symbol)

            if not data:

                print(
                    f"{exchange:<10} NO DATA"
                )

                continue

            fresh = is_fresh(
                data.get("timestamp")
            )

            freshness = (
                "FRESH"
                if fresh
                else "STALE"
            )

            print(
                f"{exchange:<10} "
                f"{freshness:<6} "
                f"Price: "
                f"{format_price(data.get('price')):<18} "
                f"Funding: "
                f"{format_percent(data.get('funding_rate'))}"
            )

    print()
    print("=" * 72)
    print("V1 STATUS: DATA COLLECTION ONLY")
    print("NO TRADING / NO ORDERS / NO SIGNALS")
    print("=" * 72)


# ============================================================
# MAIN LOOP
# ============================================================

async def main():

    timeout = aiohttp.ClientTimeout(
        total=REQUEST_TIMEOUT
    )

    connector = aiohttp.TCPConnector(
        limit=20
    )

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector
    ) as session:

        while True:

            start = time.time()

            await asyncio.gather(
                fetch_binance(session),
                fetch_bybit(session),
                fetch_okx(session),
            )

            print_report()

            elapsed = time.time() - start

            sleep_time = max(
                0,
                UPDATE_INTERVAL - elapsed
            )

            await asyncio.sleep(
                sleep_time
            )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:

        asyncio.run(main())

    except KeyboardInterrupt:

        print("\nSystem stopped by user.")