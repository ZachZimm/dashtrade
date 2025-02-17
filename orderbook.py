import asyncio
import logging
import aiohttp
import time
import os
import asyncpg
from datetime import datetime

async def get_orderbook(symbol: str, count: int = 500) -> dict:
    endpoint = 'https://api.kraken.com/0/public/' + '/Depth' + f'?pair={symbol}&count={count}'
    payload = {}
    headers = {'Accept': 'application/json'}

    async with aiohttp.ClientSession() as session:
        async with session.get(endpoint, headers=headers, data=payload) as response:
            if response.status != 200:
                logging.error(f'Error getting orderbook: {await response.json()}')
                return {}
            response_json = await response.json()
            if response_json['error'] != []:
                logging.error(f'Error getting orderbook: {response_json["error"]}')
                return {}
            response_json['result']['symbol'] = symbol
            return response_json['result']

def format_orderbook(orderbook: dict) -> dict:
    xz_symbol = list(orderbook.keys())[0]
    symbol = orderbook['symbol']
    bids = orderbook[xz_symbol]['bids']
    asks = orderbook[xz_symbol]['asks']
    # bids and asks are lists of lists: [[pricestr, volumestr, timestampint], ...]
    # we want to convert the strings to floats and the timestamp to a datetime object

    for bid in bids:
        bid[0] = float(bid[0])
        bid[1] = float(bid[1])
        bid[2] = datetime.fromtimestamp(bid[2])

    for ask in asks:
        ask[0] = float(ask[0])
        ask[1] = float(ask[1])
        ask[2] = datetime.fromtimestamp(ask[2])
    orderbook = {symbol: {'bids': bids, 'asks': asks}}

    return orderbook

async def push_orderbook(orderbook: dict, conn):
    # Create the orderbook table (if it doesn't exist) using Postgres syntax
    # schema is {symbol: {bids: [[price (float), volume (float), timestamp (timestamp)], ...], asks: [[price (float), volume (float), timestamp (datetime)], ...]}}

    pass


async def listen_to_orderbooks(pairs, conn, shutdown_event):
    time_start = time.time()
    tasks = []
    async def get_and_push_orderbook(pair: str):
        orderbook = await get_orderbook(pair)
        orderbook = format_orderbook(orderbook)
        # saved = await push_orderbook(orderbook, conn)
        return orderbook
    # try:
    # while not shutdown_event.is_set():
    runtime = time.time()
    for pair in pairs:
        pair = pair.replace('/', '')
        tasks.append(asyncio.create_task(get_and_push_orderbook(pair)))

    orderbooks = await asyncio.gather(*tasks)

    runtime += 10
    # time_until_next_run = runtime - time.time()
    # asycnio.sleep(time_until_next_run)

    time_end = time.time()
    print(f'Time elapsed: {round(time_end - time_start,2)}')
