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

def check_arbitrage():

    for symbol in SYMBOLS:

        b = prices["binance"].get(symbol)

        o = prices["okx"].get(symbol)

        if not b or not o:

            continue

        # Binance -> OKX

        buy_binance = b["ask"]

        sell_okx = o["bid"]

        spread_1 = (sell_okx - buy_binance) / buy_binance

        # OKX -> Binance

        buy_okx = o["ask"]

        sell_binance = b["bid"]

        spread_2 = (sell_binance - buy_okx) / buy_okx

        if spread_1 > 0.001:

            print(

                f"\n🚨 ARBITRAGE {symbol}\n"

                f"BUY Binance: {buy_binance}\n"

                f"SELL OKX:   {sell_okx}\n"

                f"Gross spread: {spread_1 * 100:.3f}%\n"

            )

        if spread_2 > 0.001:

            print(

                f"\n🚨 ARBITRAGE {symbol}\n"

                f"BUY OKX:     {buy_okx}\n"

                f"SELL Binance: {sell_binance}\n"

                f"Gross spread: {spread_2 * 100:.3f}%\n"

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
