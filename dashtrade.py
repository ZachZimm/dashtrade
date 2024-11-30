import os
import sys
import time
import json
import asyncio
import signal
import websockets
from websockets.exceptions import ConnectionClosedError
from dotenv import load_dotenv
from rest_websocket_token import get_token
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mpf
import datetime

# Load environment variables
load_dotenv()
ws_token = None

shutodwn_event = asyncio.Event()

def signal_handler(sig, frame):
    print("\nShutting down...")
    shutodwn_event.set()
    sys.exit(0)

def read_balances(data):
    total_balance = 0
    for asset in data:
        _bal = asset['balance']
        _asset = asset['asset']
        if _bal <= 1e-8:
            continue
        print(f"Token: {_asset}")
        print(f"Balance: {_bal}")
        if _asset[:2] == "US":
            total_balance += _bal

    return total_balance

subscribe_data = {
    "method": "subscribe",
    "params": {
        "channel": "trade",
        "symbol": ["XRP/USD", "ETH/USD", "BTC/USD", "ADA/USD", "SOL/USD", "DOGE/USD"],
    }
}

balance_subscribe_data = {
    "method": "subscribe",
    "params": {
        "channel": "balances",
        "token": ws_token
    }
}

# Create a websocket connection
uri = "wss://ws.kraken.com/v2"
auth_uri = "wss://ws-auth.kraken.com/v2"

async def connect_auth():
    global ws_token
    while not shutodwn_event.is_set():
        try:
            async with websockets.connect(auth_uri) as websocket:
                await websocket.send(json.dumps(balance_subscribe_data))
                while not shutodwn_event.is_set():
                    try:
                        response = await websocket.recv()
                        response = json.loads(response)
                        if 'channel' in response and response['channel'] == "heartbeat":
                            continue
                        if 'channel' in response and response['channel'] == "balances":
                            print(f"Total Balance: {read_balances(response['data'])}")
                    except ConnectionClosedError as e:
                        print(f"Auth ConnectionClosedError: {e}")
                        break
        except Exception as e:
            print(f"Auth Connection Exception: {e}")
            await asyncio.sleep(1)  # Wait before reconnecting

async def connect():
    while not shutodwn_event.is_set():
        try:
            # Setup database connection and create the trades table
            with sqlite3.connect('trades.db') as conn:  # Use context manager to ensure closure
                cursor = conn.cursor()
                cursor.execute('''CREATE TABLE IF NOT EXISTS trades (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    symbol TEXT,
                                    side TEXT,
                                    qty REAL,
                                    price REAL,
                                    ord_type TEXT,
                                    trade_id INTEGER,
                                    timestamp TEXT
                                )''')
                conn.commit()

                async with websockets.connect(uri) as websocket:
                    await websocket.send(json.dumps(subscribe_data))
                    while not shutodwn_event.is_set():
                        try:
                            response = await websocket.recv()
                            response = json.loads(response)
                            if 'channel' in response and response['channel'] == "heartbeat":
                                continue
                            if 'channel' in response and response['channel'] == "trade":
                                if 'data' in response:
                                    for trade in response['data']:
                                        # Extract fields from the trade data
                                        symbol = trade['symbol']
                                        side = trade['side']
                                        qty = trade['qty']
                                        price = trade['price']
                                        ord_type = trade['ord_type']
                                        trade_id = trade['trade_id']
                                        timestamp = trade['timestamp']
                                        # Insert the trade data into the database
                                        cursor.execute('''INSERT INTO trades (symbol, side, qty, price, ord_type, trade_id, timestamp)
                                                          VALUES (?, ?, ?, ?, ?, ?, ?)''',
                                                       (symbol, side, qty, price, ord_type, trade_id, timestamp))
                                        conn.commit()
                            else:
                                print(response)
                        except ConnectionClosedError as e:
                            print(f"Trade ConnectionClosedError: {e}")
                            break
        except Exception as e:
            print(f"Trade Connection Exception: {e}")
            await asyncio.sleep(1)  # Wait before reconnecting

async def main():
    global ws_token
    ws_token = get_token()
    signal.signal(signal.SIGINT, signal_handler)
    await asyncio.gather(
        connect(),
        connect_auth()
    )

def load_data_from_db(ticker, start_timestamp) -> list:
    conn = sqlite3.connect('trades.db')  # Connect to the SQLite database
    cursor = conn.cursor()
    
    # Format the start_timestamp for SQL query
    formatted_start_time = start_timestamp.isoformat()

    # Query to fetch data starting from the specified timestamp
    query = f"""
    SELECT symbol, side, qty, price, ord_type, trade_id, timestamp 
    FROM trades 
    WHERE symbol='{ticker}' AND timestamp >= '{formatted_start_time}'
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    
    # Convert each row to a dictionary
    data_points = []
    for row in rows:
        data_point = {
            'symbol': row[0],
            'side': row[1],
            'qty': row[2],
            'price': row[3],
            'ord_type': row[4],
            'trade_id': row[5],
            'timestamp': datetime.datetime.strptime(row[6], "%Y-%m-%dT%H:%M:%S.%fZ")
        }
        data_points.append(data_point)
    
    conn.close()
    return data_points

def create_dollar_bars(data, bar_size) -> pd.DataFrame:
    dollar_bars = []
    
    # Sort the data by timestamp
    sorted_data = sorted(data, key=lambda x: x['timestamp'])

    for trade in sorted_data:
        symbol = trade['symbol']
        price = trade['price']
        volume = trade['qty']

        if not dollar_bars or dollar_bars[-1]['dollar_volume'] >= bar_size:
            # Start a new bar
            new_bar = {
                'start_time': trade['timestamp'],
                'open': price,
                'high': price,
                'low': price,
                'close': price,
                'volume': volume,
                'dollar_volume': price * volume
            }
            dollar_bars.append(new_bar)
        else:
            # Update the current bar
            last_bar = dollar_bars[-1]
            last_bar['close'] = price
            last_bar['high'] = max(last_bar['high'], price)
            last_bar['low'] = min(last_bar['low'], price)

            remaining_dollar_volume = bar_size - last_bar['dollar_volume']
            trade_dollar_volume = price * volume

            if trade_dollar_volume <= remaining_dollar_volume:
                last_bar['volume'] += volume
                last_bar['dollar_volume'] += trade_dollar_volume
                last_bar['end_time'] = trade['timestamp']
            else:
                # Fill up the current bar
                partial_volume = remaining_dollar_volume / price
                last_bar['volume'] += partial_volume
                last_bar['dollar_volume'] = bar_size
                last_bar['end_time'] = trade['timestamp']

                # Start new bars with the remaining volume
                remaining_volume = volume - partial_volume
                remaining_dollar_volume = trade_dollar_volume - remaining_dollar_volume

                while remaining_dollar_volume > 0:
                    bar_volume = min(remaining_volume, bar_size / price)
                    bar_dollar_volume = bar_volume * price
                    new_bar = {
                        'start_time': trade['timestamp'],
                        'open': price,
                        'high': price,
                        'low': price,
                        'close': price,
                        'volume': bar_volume,
                        'dollar_volume': bar_dollar_volume,
                        'end_time': trade['timestamp']
                    }
                    dollar_bars.append(new_bar)
                    remaining_volume -= bar_volume
                    remaining_dollar_volume -= bar_dollar_volume

    # Convert list of dictionaries to DataFrame
    df = pd.DataFrame(dollar_bars)
    df.set_index('start_time', inplace=True)

    return df

def read_data(symbol, start_timestamp):
    print("Reading data from the database...")
    # Load data from the database
    bar_size = 500000
    data = load_data_from_db(symbol, start_timestamp)
    # Create dollar bars
    dollar_bars = create_dollar_bars(data, bar_size)
    print(dollar_bars.head())
    print(dollar_bars.tail())

    # Plot the OHLC chart
    mpf.plot(dollar_bars, type='candle', style='charles', title='charts/' + symbol.replace("/", "_") + ".png")
    plt.show()

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "record":
        asyncio.run(main())
    else:
        symbol = "XRP/USD"
        if len(sys.argv) > 2:
            symbol = sys.argv[2]
        read_data(symbol, datetime.datetime(2024, 11, 24))
