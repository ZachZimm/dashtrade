import asyncio
import logging
import aiohttp
import time
import os
import asyncpg
from datetime import datetime

async def get_orderbook(symbol: str, count: int = 500) -> dict:
    endpoint = 'https://api.kraken.com/0/public/Depth' + f'?pair={symbol.replace("/","")}&count={count}'
    payload = {}
    headers = {'Accept': 'application/json'}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(endpoint, headers=headers, data=payload, timeout=10) as response:
                if response.status != 200:
                    logging.warning(f'HTTP {response.status} when getting orderbook for {symbol}: {await response.text()}')
                    return {'error': f'HTTP {response.status}', 'symbol': symbol}

                response_json = await response.json()
                if response_json['error']:
                    logging.warning(f'API error getting orderbook for {symbol}: {response_json["error"]}')
                    return {'error': response_json['error'], 'symbol': symbol}

                response_json['result']['symbol'] = symbol
                return response_json['result']
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logging.warning(f'Network error getting orderbook for {symbol}: {str(e)}')
        return {'error': str(e), 'symbol': symbol}
    except Exception as e:
        logging.error(f'Unexpected error getting orderbook for {symbol}: {str(e)}')
        return {'error': str(e), 'symbol': symbol}

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
    recording_frequency = 15
    # Track pairs that repeatedly fail
    problem_pairs = {}

    async def get_and_push_orderbook(pair: str, pool: asyncpg.Pool):
        try:
            orderbook = await get_orderbook(pair)
            if 'error' in orderbook:
                # Count consecutive failures
                # problem_pairs[pair] = problem_pairs.get(pair, 0) + 1
                return {'error': orderbook['error'], 'symbol': pair}

            # Reset failure count on success
            if pair in problem_pairs:
                problem_pairs[pair] = 0

            orderbook = await format_orderbook(orderbook)
            async with pool.acquire() as conn:
                await push_orderbook(orderbook, conn)
            return orderbook
        except Exception as e:
            logging.error(f'Error in get_and_push_orderbook for {pair}: {e}')
            problem_pairs[pair] = problem_pairs.get(pair, 0) + 1
            return {'error': str(e), 'symbol': pair}

    print(f'Waiting {round(recording_frequency - time.time() % recording_frequency,2)} seconds to start orderbook recording')
    await asyncio.sleep(recording_frequency - time.time() % recording_frequency)
    runtime = time.time()

    while not shutdown_event.is_set():
        active_pairs = [p for p in pairs if problem_pairs.get(p, 0) < 5]
        removed_pairs = [p for p in pairs if problem_pairs.get(p, 0) >= 5]

        if removed_pairs:
            logging.warning(f"Temporarily skipping problematic pairs: {removed_pairs}")

        tasks = []
        for pair in active_pairs:
            tasks.append(asyncio.create_task(get_and_push_orderbook(pair, pool)))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Log any errors from the gather
        for result in results:
            if isinstance(result, Exception):
                logging.error(f"Task exception: {result}")

        # Reduce failure counts over time to eventually retry problematic pairs
        for pair in list(problem_pairs.keys()):
            if problem_pairs[pair] > 0:
                problem_pairs[pair] -= 0.2  # Slowly reduce the error count

        runtime += recording_frequency
        time_until_next_run = runtime - time.time()

        # Ensure we don't sleep for negative time
        if time_until_next_run > 0:
            await asyncio.sleep(time_until_next_run)
        else:
            # If we're behind schedule, reset the runtime clock
            logging.warning(f"Recording cycle took longer than {recording_frequency}s, resetting schedule")
            runtime = time.time() + recording_frequency
            await asyncio.sleep(1)  # Brief pause before next cycle
