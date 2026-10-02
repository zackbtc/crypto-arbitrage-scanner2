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
TRADE_SIZE_USD = 500
# Temporary fee assumptions.
# We will replace these with the real fee tiers later.
BINANCE_FEE = 0.001
OKX_FEE = 0.001
BYBIT_FEE = 0.001
# Opportunity threshold
MIN_NET_EDGE = 0.0005
# Data is considered stale after this many seconds
STALE_SECONDS = 2
# Market report interval
REPORT_INTERVAL = 30
BINANCE_URL = "wss://stream.binance.com:9443/stream"
OKX_URL = "wss://ws.okx.com/ws/v5/public"
BYBIT_URL = "wss://stream.bybit.com/v5/public/spot"
# =========================
# ORDER BOOK STORAGE
# =========================
books = {
    "binance": {},
    "okx": {},
    "bybit": {},
}
# Opportunity counters
opportunity_counts = {
    "binance_okx": 0,
    "okx_binance": 0,
    "binance_bybit": 0,
    "bybit_binance": 0,
    "okx_bybit": 0,
    "bybit_okx": 0,
}
# =========================
# VWAP
# =========================
def calculate_vwap(levels, usd_size):
    remaining_usd = usd_size
    total_base = 0.0
    total_usd = 0.0
    for price, quantity in levels:
        price = float(price)
        quantity = float(quantity)
        if price <= 0 or quantity <= 0:
            continue
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
# =========================
# BINANCE
# =========================
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
            print("Connecting to Binance...")
            async with websockets.connect(
                BINANCE_URL,
                ping_interval=20,
                ping_timeout=20
            ) as ws:
                await ws.send(json.dumps(params))
                print("Binance connected.")
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
# =========================
# OKX
# =========================
async def okx():
    while True:
        try:
            print("Connecting to OKX...")
            async with websockets.connect(
                OKX_URL,
                ping_interval=20,
                ping_timeout=20
            ) as ws:
                args = [
                    {
                        "channel": "books5",
                        "instId": symbol.replace("USDT", "-USDT")
                    }
                    for symbol in SYMBOLS
                ]
                subscribe = {
                    "op": "subscribe",
                    "args": args
                }
                await ws.send(json.dumps(subscribe))
                print("OKX connected.")
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
                    symbol = inst_id.replace("-", "")
                    books["okx"][symbol] = {
                        "bids": bids,
                        "asks": asks,
                        "timestamp": time.time()
                    }
        except Exception as e:
            print("OKX connection error:", e)
            await asyncio.sleep(3)
# =========================
# BYBIT
# =========================
async def bybit():
    while True:
        try:
            print("Connecting to Bybit...")
            async with websockets.connect(
                BYBIT_URL,
                ping_interval=20,
                ping_timeout=20
            ) as ws:
                topics = [
                    f"orderbook.50.{symbol}"
                    for symbol in SYMBOLS
                ]
                subscribe = {
                    "op": "subscribe",
                    "args": topics
                }
                await ws.send(json.dumps(subscribe))
                print("Bybit connected.")
                # Local order books
                local_books = {
                    symbol: {
                        "bids": {},
                        "asks": {},
                        "timestamp": 0
                    }
                    for symbol in SYMBOLS
                }
                async for message in ws:
                    data = json.loads(message)
                    if data.get("op") == "pong":
                        continue
                    topic = data.get("topic", "")
                    if not topic.startswith("orderbook.50."):
                        continue
                    symbol = topic.split(".")[-1]
                    if symbol not in SYMBOLS:
                        continue
                    msg_type = data.get("type")
                    payload = data.get("data", {})
                    bids = payload.get("b", [])
                    asks = payload.get("a", [])
                    # -------------------------
                    # SNAPSHOT
                    # -------------------------
                    if msg_type == "snapshot":
                        local_books[symbol]["bids"] = {
                            float(price): float(qty)
                            for price, qty in bids
                            if float(qty) > 0
                        }
                        local_books[symbol]["asks"] = {
                            float(price): float(qty)
                            for price, qty in asks
                            if float(qty) > 0
                        }
                    # -------------------------
                    # DELTA
                    # -------------------------
                    elif msg_type == "delta":
                        for price, qty in bids:
                            price = float(price)
                            qty = float(qty)
                            if qty == 0:
                                local_books[symbol]["bids"].pop(
                                    price,
                                    None
                                )
                            else:
                                local_books[symbol]["bids"][price] = qty
                        for price, qty in asks:
                            price = float(price)
                            qty = float(qty)
                            if qty == 0:
                                local_books[symbol]["asks"].pop(
                                    price,
                                    None
                                )
                            else:
                                local_books[symbol]["asks"][price] = qty
                    else:
                        continue
                    # Sort local order book
                    sorted_bids = sorted(
                        local_books[symbol]["bids"].items(),
                        key=lambda x: x[0],
                        reverse=True
                    )[:50]
                    sorted_asks = sorted(
                        local_books[symbol]["asks"].items(),
                        key=lambda x: x[0]
                    )[:50]
                    if not sorted_bids or not sorted_asks:
                        continue
                    local_books[symbol]["timestamp"] = time.time()
                    books["bybit"][symbol] = {
                        "bids": sorted_bids,
                        "asks": sorted_asks,
                        "timestamp": local_books[symbol]["timestamp"]
                    }
        except Exception as e:
            print("Bybit connection error:", e)
            await asyncio.sleep(3)
# =========================
# ARBITRAGE CALCULATION
# =========================
def evaluate_pair(
    buy_exchange,
    sell_exchange,
    symbol,
    fee_buy,
    fee_sell,
    counter_name
):
    buy_book = books[buy_exchange].get(symbol)
    sell_book = books[sell_exchange].get(symbol)
    if not buy_book or not sell_book:
        return None
    now = time.time()
    if now - buy_book["timestamp"] > STALE_SECONDS:
        return None
    if now - sell_book["timestamp"] > STALE_SECONDS:
        return None
    buy_vwap = calculate_vwap(
        buy_book["asks"],
        TRADE_SIZE_USD
    )
    sell_vwap = calculate_vwap(
        sell_book["bids"],
        TRADE_SIZE_USD
    )
    if not buy_vwap or not sell_vwap:
        return None
    gross_edge = (
        sell_vwap - buy_vwap
    ) / buy_vwap
    fees = fee_buy + fee_sell
    net_edge = gross_edge - fees
    expected_profit = TRADE_SIZE_USD * net_edge
    if net_edge >= MIN_NET_EDGE:
        opportunity_counts[counter_name] += 1
        print(
            f"\n🚨 NET ARBITRAGE\n"
            f"{symbol}\n"
            f"BUY  {buy_exchange.upper()}: "
            f"${buy_vwap:.6f}\n"
            f"SELL {sell_exchange.upper()}: "
            f"${sell_vwap:.6f}\n"
            f"\nGross: {gross_edge * 100:.3f}%"
            f"\nFees:  {fees * 100:.3f}%"
            f"\nNET:   {net_edge * 100:.3f}%"
            f"\n\nSize: ${TRADE_SIZE_USD}"
            f"\nEstimated P&L: ${expected_profit:.2f}\n"
        )
    return {
        "gross": gross_edge,
        "net": net_edge,
        "profit": expected_profit
    }
# =========================
# MARKET CHECK
# =========================
def check_all_markets():
    for symbol in SYMBOLS:
        evaluate_pair(
            "binance",
            "okx",
            symbol,
            BINANCE_FEE,
            OKX_FEE,
            "binance_okx"
        )
        evaluate_pair(
            "okx",
            "binance",
            symbol,
            OKX_FEE,
            BINANCE_FEE,
            "okx_binance"
        )
        evaluate_pair(
            "binance",
            "bybit",
            symbol,
            BINANCE_FEE,
            BYBIT_FEE,
            "binance_bybit"
        )
        evaluate_pair(
            "bybit",
            "binance",
            symbol,
            BYBIT_FEE,
            BINANCE_FEE,
            "bybit_binance"
        )
        evaluate_pair(
            "okx",
            "bybit",
            symbol,
            OKX_FEE,
            BYBIT_FEE,
            "okx_bybit"
        )
        evaluate_pair(
            "bybit",
            "okx",
            symbol,
            BYBIT_FEE,
            OKX_FEE,
            "bybit_okx"
        )
# =========================
# MARKET REPORT
# =========================
def market_report():
    print("\n" + "=" * 60)
    print("📊 MARKET REPORT")
    print("=" * 60)
    print(
        f"Trade size: ${TRADE_SIZE_USD}"
    )
    print(
        f"Net threshold: "
        f"{MIN_NET_EDGE * 100:.3f}%"
    )
    print("\nData status:")
    for exchange in books:
        ready = len(books[exchange])
        print(
            f"{exchange.upper():8} "
            f"{ready}/{len(SYMBOLS)} symbols"
        )
    print("\nOpportunity counts:")
    for name, count in opportunity_counts.items():
        print(
            f"{name:18} {count}"
        )
    print("=" * 60 + "\n")
# =========================
# MONITOR
# =========================
async def monitor():
    last_report = 0
    while True:
        check_all_markets()
        now = time.time()
        if now - last_report >= REPORT_INTERVAL:
            market_report()
            last_report = now
        await asyncio.sleep(0.25)
# =========================
# MAIN
# =========================
async def main():
    await asyncio.gather(
        binance(),
        okx(),
        bybit(),
        monitor()
    )
if __name__ == "__main__":
    asyncio.run(main())