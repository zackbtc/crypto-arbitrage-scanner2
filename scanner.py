# ============================================================
# PERSONAL QUANT SYSTEM — V1.1
# DATA ENGINE
# ============================================================
#
# V1.1 objectives:
#   - Binance Futures
#   - Bybit Linear Futures
#   - OKX Perpetuals
#   - 5 liquid symbols
#   - Clean synchronized reporting
#   - Freshness validation
#   - Public data only
#
# NO:
#   - API keys
#   - Real orders
#   - Trading signals
#   - Position execution
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
# API ENDPOINTS
# ============================================================

BINANCE_URL = (
    "https://fapi.binance.com/fapi/v1/premiumIndex"
)

BYBIT_URL = (
    "https://api.bybit.com/v5/market/tickers"
)

OKX_URL = (
    "https://www.okx.com/api/v5/market/ticker"
)


# ============================================================
# MARKET DATA STORAGE
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
    return datetime.now(
        timezone.utc
    ).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )


def safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def is_fresh(timestamp_ms):

    if not timestamp_ms:
        return False

    age = (
        now_ms() - timestamp_ms
    ) / 1000

    return age <= STALE_SECONDS


def format_price(value):

    if value is None:
        return "N/A"

    if value >= 1000:
        return f"${value:,.2f}"

    if value >= 1:
        return f"${value:,.4f}"

    return f"${value:.6f}"


def format_percent(value):

    if value is None:
        return "N/A"

    return f"{value * 100:+.5f}%"


# ============================================================
# BINANCE DATA
# ============================================================

async def fetch_binance(session):

    result = {}

    try:

        async with session.get(
            BINANCE_URL,
            timeout=REQUEST_TIMEOUT
        ) as response:

            if response.status != 200:
                return result

            data = await response.json()

            timestamp = now_ms()

            for item in data:

                symbol = item.get(
                    "symbol"
                )

                if symbol not in SYMBOLS:
                    continue

                result[symbol] = {

                    "timestamp": timestamp,

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

                    "next_funding_time":
                        item.get(
                            "nextFundingTime"
                        ),

                    "funding_interval_hour":
                        8.0,

                    "volume": None,
                }

    except Exception:
        pass

    return result


# ============================================================
# BYBIT DATA
# ============================================================

async def fetch_bybit_symbol(
    session,
    symbol
):

    try:

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
                return symbol, None

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
                return symbol, None

            item = items[0]

            record = {

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

                "next_funding_time":
                    item.get(
                        "nextFundingTime"
                    ),

                "funding_interval_hour":
                    safe_float(
                        item.get(
                            "fundingIntervalHour"
                        )
                    ),

                "volume": safe_float(
                    item.get("volume24h")
                ),
            }

            return symbol, record

    except Exception:

        return symbol, None


async def fetch_bybit(session):

    tasks = [

        fetch_bybit_symbol(
            session,
            symbol
        )

        for symbol in SYMBOLS
    ]

    results = await asyncio.gather(
        *tasks
    )

    result = {}

    for symbol, data in results:

        if data is not None:
            result[symbol] = data

    return result


# ============================================================
# OKX DATA
# ============================================================

async def fetch_okx_symbol(
    session,
    symbol
):

    try:

        inst_id = (
            symbol.replace(
                "USDT",
                "-USDT-SWAP"
            )
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
                return symbol, None

            data = await response.json()

            items = data.get(
                "data",
                []
            )

            if not items:
                return symbol, None

            item = items[0]

            record = {

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

                "funding_interval_hour": None,

                "volume": safe_float(
                    item.get("vol24h")
                ),
            }

            return symbol, record

    except Exception:

        return symbol, None


async def fetch_okx(session):

    tasks = [

        fetch_okx_symbol(
            session,
            symbol
        )

        for symbol in SYMBOLS
    ]

    results = await asyncio.gather(
        *tasks
    )

    result = {}

    for symbol, data in results:

        if data is not None:
            result[symbol] = data

    return result


# ============================================================
# DATA COLLECTION
# ============================================================

async def collect_all_data(session):

    results = await asyncio.gather(

        fetch_binance(session),

        fetch_bybit(session),

        fetch_okx(session),
    )

    market_data["BINANCE"] = results[0]
    market_data["BYBIT"] = results[1]
    market_data["OKX"] = results[2]


# ============================================================
# EXCHANGE STATUS
# ============================================================

def get_exchange_status(exchange):

    total = len(SYMBOLS)

    fresh = 0

    for symbol in SYMBOLS:

        data = market_data[
            exchange
        ].get(symbol)

        if data is None:
            continue

        if is_fresh(
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
# CLEAN REPORT
# ============================================================

def print_report():

    print("\n")

    print("=" * 72)

    print(
        "             PERSONAL QUANT SYSTEM — V1.1"
    )

    print(
        "                    DATA ENGINE"
    )

    print("=" * 72)

    print(
        f"UTC: {utc_time()}"
    )

    print()

    # --------------------------------------------------------
    # CONNECTION STATUS
    # --------------------------------------------------------

    print("CONNECTION STATUS")

    print("-" * 72)

    for exchange in [
        "BINANCE",
        "BYBIT",
        "OKX",
    ]:

        status, fresh, total = (
            get_exchange_status(
                exchange
            )
        )

        print(
            f"{exchange:<10} "
            f"{status:<8} "
            f"{fresh}/{total} fresh"
        )

    print()

    # --------------------------------------------------------
    # MARKET DATA
    # --------------------------------------------------------

    for symbol in SYMBOLS:

        print(
            "=" * 72
        )

        print(
            f"{symbol}"
        )

        print(
            "-" * 72
        )

        for exchange in [
            "BINANCE",
            "BYBIT",
            "OKX",
        ]:

            data = market_data[
                exchange
            ].get(symbol)

            if data is None:

                print(
                    f"{exchange:<10} "
                    f"NO DATA"
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
                f"{freshness:<7} "
                f"Price: "
                f"{format_price(data.get('price')):<18} "
                f"Funding: "
                f"{format_percent(data.get('funding_rate'))}"
            )

    print()

    print("=" * 72)

    print(
        "V1.1 STATUS: DATA COLLECTION ONLY"
    )

    print(
        "NO TRADING / NO ORDERS / NO SIGNALS"
    )

    print("=" * 72)


# ============================================================
# MAIN LOOP
# ============================================================

async def main():

    timeout = aiohttp.ClientTimeout(
        total=REQUEST_TIMEOUT
    )

    connector = aiohttp.TCPConnector(
        limit=30
    )

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector
    ) as session:

        while True:

            cycle_start = time.time()

            # ------------------------------------------------
            # Collect everything first
            # ------------------------------------------------

            await collect_all_data(
                session
            )

            # ------------------------------------------------
            # Print only after all collection is finished
            # ------------------------------------------------

            print_report()

            # ------------------------------------------------
            # Maintain approximately 10-second cycle
            # ------------------------------------------------

            elapsed = (
                time.time()
                - cycle_start
            )

            sleep_time = max(
                0,
                UPDATE_INTERVAL
                - elapsed
            )

            await asyncio.sleep(
                sleep_time
            )


# ============================================================
# PROGRAM ENTRY
# ============================================================

if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        print()
        print(
            "System stopped by user."
        )