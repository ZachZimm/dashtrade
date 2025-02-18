import asyncio
import logging
import aiohttp
import time
import os
import asyncpg
from datetime import datetime

async def get_orderbook(symbol: str, count: int = 500) -> dict:
    endpoint = 'https://api.kraken.com/0/public/' + '/Depth' + f'?pair={symbol.replace('/','')}&count={count}'
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

async def format_orderbook(orderbook: dict) -> dict:
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

async def push_orderbook(orderbook: dict, conn: asyncpg.Connection):
    # Extract the symbol. Our orderbook is structured as:
    # schema is {symbol: {bids: [[price (float), volume (float), timestamp (timestamp)], ...], asks: [[price (float), volume (float), timestamp (datetime)], ...]}}
    # { symbol: { bids: [...], asks: [...] } }
    symbol = list(orderbook.keys())[0]
    bids = orderbook[symbol]['bids']
    asks = orderbook[symbol]['asks']

    # Set the snapshot time to now (or use a provided timestamp)
    snapshot_time = datetime.now()

    # Prepare a list of tuples to insert.
    # Each tuple: (symbol, side, price, volume, order_time, snapshot_time)
    entries = []
    for bid in bids:
        price, volume, order_time = bid
        entries.append((symbol, 'bid', price, volume, order_time, snapshot_time))
    for ask in asks:
        price, volume, order_time = ask
        entries.append((symbol, 'ask', price, volume, order_time, snapshot_time))

    # Create the table if it doesn't exist.
    await conn.execute('''
            CREATE TABLE IF NOT EXISTS orderbook_entries (
                id SERIAL PRIMARY KEY,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                price DOUBLE PRECISION NOT NULL,
                volume DOUBLE PRECISION NOT NULL,
                order_time TIMESTAMP NOT NULL,
                snapshot_time TIMESTAMP NOT NULL
            )
    ''')

    # Insert the entries using executemany for bulk insert.
    await conn.executemany('''
            INSERT INTO orderbook_entries(symbol, side, price, volume, order_time, snapshot_time)
            VALUES($1, $2, $3, $4, $5, $6)
    ''', entries)


async def listen_to_orderbooks(pairs: list, pool: asyncpg.Pool, shutdown_event):
    recording_frequency = 30

    tasks = []
    async def get_and_push_orderbook(pair: str, pool: asyncpg.Pool):
        try:
            orderbook = await get_orderbook(pair)
            orderbook = await format_orderbook(orderbook)
            async with pool.acquire() as conn:
                await push_orderbook(orderbook, conn)
            return orderbook
        except Exception as e:
            logging.error(f'Error in get_and_push_orderbook: {e}')
            return {}

    print(f'Waiting {round(recording_frequency - time.time() % recording_frequency,2)} seconds to start orderbook recording')
    await asyncio.sleep(recording_frequency - time.time() % recording_frequency)
    runtime = time.time()
    try:
        while not shutdown_event.is_set():
            for pair in pairs:
                tasks.append(asyncio.create_task(get_and_push_orderbook(pair, pool)))

            orderbooks = await asyncio.gather(*tasks)

            runtime += recording_frequency
            time_until_next_run = runtime - time.time()
            tasks = []
            await asyncio.sleep((time_until_next_run / 3))
            # Interrupt the sleep to allow for a restart within the frequency
            await asyncio.sleep(runtime - time.time())
    except Exception as e:
        logging.error(f'Error in listen_to_orderbooks: {e}')
