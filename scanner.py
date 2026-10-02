import asyncio

import json

import time

import websockets

SYMBOLS = [

    "BTCUSDT",

    "ETHUSDT",

    "SOLUSDT",

    "XRPUSDT",

    "DOGEUSDT",

]

BINANCE_URL = "wss://stream.binance.com:9443/stream"

OKX_URL = "wss://ws.okx.com:8443/ws/v5/public"

async def binance():

    streams = [

        f"{symbol.lower()}@bookTicker"

        for symbol in SYMBOLS

    ]

    params = {

        "method": "SUBSCRIBE",

        "params": streams,

        "id": 1

    }

    async with websockets.connect(BINANCE_URL) as ws:

        await ws.send(json.dumps(params))

        async for message in ws:

            data = json.loads(message)

            if "data" not in data:

                continue

            ticker = data["data"]

            symbol = ticker["s"]

            bid = float(ticker["b"])

            ask = float(ticker["a"])

            prices["binance"][symbol] = {

                "bid": bid,

                "ask": ask,

                "time": time.time()

            }

async def okx():

    async with websockets.connect(OKX_URL) as ws:

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

        async for message in ws:

            if message == "pong":

                continue

            data = json.loads(message)

            if "data" not in data:

                continue

            if not data["data"]:

                continue

            book = data["data"][0]

            asks = book.get("asks", [])

            bids = book.get("bids", [])

            if not asks or not bids:

                continue

            symbol = data["arg"]["instId"].replace("-", "")

            ask = float(asks[0][0])

            bid = float(bids[0][0])

            prices["okx"][symbol] = {

                "bid": bid,

                "ask": ask,

                "time": time.time()

            }

prices = {

    "binance": {},

    "okx": {}

}

TRADE_SIZE_USD = 500

# Conservative starting assumption.
# We will replace this with the real fee tier later.
BINANCE_FEE = 0.001
OKX_FEE = 0.001

MIN_NET_EDGE = 0.0005


def check_arbitrage():

    for symbol in SYMBOLS:

        b = prices["binance"].get(symbol)
        o = prices["okx"].get(symbol)

        if not b or not o:
            continue

        # ==========================================
        # Binance -> OKX
        # ==========================================

        buy_price = b["ask"]
        sell_price = o["bid"]

        gross_edge = (sell_price - buy_price) / buy_price

        fees = BINANCE_FEE + OKX_FEE

        net_edge = gross_edge - fees

        expected_profit = TRADE_SIZE_USD * net_edge

        if net_edge >= MIN_NET_EDGE:

            print(
                f"\n🚨 NET ARBITRAGE {symbol}\n"
                f"BUY Binance: {buy_price:.4f}\n"
                f"SELL OKX:   {sell_price:.4f}\n"
                f"Gross edge: {gross_edge * 100:.3f}%\n"
                f"Fees:       {fees * 100:.3f}%\n"
                f"NET EDGE:   {net_edge * 100:.3f}%\n"
                f"Size:       ${TRADE_SIZE_USD}\n"
                f"Est. profit: ${expected_profit:.2f}\n"
            )

        # ==========================================
        # OKX -> Binance
        # ==========================================

        buy_price = o["ask"]
        sell_price = b["bid"]

        gross_edge = (sell_price - buy_price) / buy_price

        fees = OKX_FEE + BINANCE_FEE

        net_edge = gross_edge - fees

        expected_profit = TRADE_SIZE_USD * net_edge

        if net_edge >= MIN_NET_EDGE:

            print(
                f"\n🚨 NET ARBITRAGE {symbol}\n"
                f"BUY OKX:     {buy_price:.4f}\n"
                f"SELL Binance: {sell_price:.4f}\n"
                f"Gross edge: {gross_edge * 100:.3f}%\n"
                f"Fees:       {fees * 100:.3f}%\n"
                f"NET EDGE:   {net_edge * 100:.3f}%\n"
                f"Size:       ${TRADE_SIZE_USD}\n"
                f"Est. profit: ${expected_profit:.2f}\n"
            )


async def monitor():

    while True:

        check_arbitrage()

        await asyncio.sleep(0.1)

async def main():

    await asyncio.gather(

        binance(),

        okx(),

        monitor()

    )

if __name__ == "__main__":

    asyncio.run(main())
