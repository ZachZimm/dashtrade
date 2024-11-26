import os
import sys
import time
import json
import asyncio
import signal
import threading
import websockets
from websockets.exceptions import ConnectionClosedError
from typing import Optional
from pydantic import BaseModel
import datetime
from dotenv import load_dotenv
from rest_websocket_token import get_token
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mpf

# Load environment variables
load_dotenv()
dollar_bar_size = 100000

shutodwn_event = threading.Event()

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

ws_token = get_token()

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
    async with websockets.connect(auth_uri) as websocket:
        # await websocket.send(ws_token)
        # await websocket.send(json.dumps(subscribe_data))
        await websocket.send(json.dumps(balance_subscribe_data))
        while not shutodwn_event.is_set():
            try:
                response = await websocket.recv()
                response = json.loads(response)
                if 'channel' in response.keys() and response['channel'] == "heartbeat":
                    continue
                if 'channel' in response.keys() and response['channel'] == "balances":
                    # print(response['data'])
                    print(f"Total Balance: {read_balances(response['data'])}")
                # print(response)
            except ConnectionClosedError as e:
                print(e)
                break


class Bar(BaseModel):
    start_time: datetime.datetime
    end_time: Optional[datetime.datetime] = None
    time_elapsed: float
    open: float
    high: float
    low: float
    close: float
    volume: float
    dollar_volume: float
    bar_size: int

    def __init__(self, start_time, open_price, volume=0, bar_size=dollar_bar_size):
        dollar_volume = volume * open_price
        super().__init__(
            start_time=start_time,
            end_time=None,
            time_elapsed=0,
            open=open_price,
            high=open_price,
            low=open_price,
            close=open_price,
            volume=volume,
            dollar_volume=dollar_volume,
            bar_size=bar_size
        )
        # print(f"Creating a new bar of ${bar_size}")


    def update(self, price, volume):
        self.close = price
        self.time_elapsed = (datetime.datetime.now() - self.start_time).total_seconds()
        dollar_vol = volume * price
        remaining_dollar_volume = self.bar_size - self.dollar_volume

        if dollar_vol <= remaining_dollar_volume:
            # Add entire trade to the current bar
            self.dollar_volume += dollar_vol
            self.volume += volume
            if price > self.high:
                self.high = price
            if price < self.low:
                self.low = price
            return 0  # No excess volume
        else:
            # Calculate the portion of the trade that fits in the current bar
            needed_volume = remaining_dollar_volume / price
            self.dollar_volume = self.bar_size
            self.volume += needed_volume
            if price > self.high:
                self.high = price
            if price < self.low:
                self.low = price
            self.end_time = datetime.datetime.now()

            # Calculate excess volume to pass to the next bar
            excess_volume = volume - needed_volume
            return excess_volume

def plot_ohlc_charts():
    while not shutodwn_event.is_set():
        with data_lock:
            _data_len = len(data)
            data_len = 1024
            data_len = min(data_len, _data_len)
            local_data = {symbol: data[symbol][-data_len:] for symbol in data}
        for symbol, bars in local_data.items():
            if not bars:
                continue
            # Create DataFrame from bars
            df = pd.DataFrame([{
                'Date': bar.start_time,
                'Open': bar.open,
                'High': bar.high,
                'Low': bar.low,
                'Close': bar.close,
                'Volume': bar.volume
            } for bar in bars])
            df.set_index('Date', inplace=True)
            # Plot the OHLC chart
            mpf.plot(df, type='candle', style='charles', title=symbol,
                     savefig=f'charts/{symbol.replace("/", "_")}.png')
            print(f"Plotted {symbol} chart")
        time.sleep(150)  # Sleep for 2.5 minutes
    
data = {}
data_lock = threading.Lock()

def handle_data(new_trade):
    global data
    symbol = new_trade['symbol']
    price = new_trade['price']
    volume = new_trade['volume']
    with data_lock:
        if symbol not in data:
            data[symbol] = []

        excess_volume = volume
        while excess_volume > 0:
            if len(data[symbol]) == 0 or data[symbol][-1].dollar_volume >= data[symbol][-1].bar_size:
                # Start a new bar
                data[symbol].append(Bar(datetime.datetime.now(), price))
            bar = data[symbol][-1]
            excess_volume = bar.update(price, excess_volume)



async def connect():
    # Setup database connection and create the trades table
    conn = sqlite3.connect('trades.db')  # Connect to the SQLite database
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
                if 'channel' in response.keys() and response['channel'] == "heartbeat":
                    continue
                if 'channel' in response.keys() and response['channel'] == "trade":
                    if 'data' in response.keys():
                        for trade in response['data']:
                            # Extract fields from the trade data
                            symbol = trade['symbol']
                            side = trade['side']
                            qty = trade['qty']
                            price = trade['price']
                            ord_type = trade['ord_type']
                            trade_id = trade['trade_id']
                            timestamp = trade['timestamp']
                            # Convert timestamp to datetime object if needed
                            timestamp_dt = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
                            # print(f"{timestamp_dt}:\t{side} {round(qty,3)} {symbol} @ {price} ")

                            # Insert the trade data into the database
                            cursor.execute('''INSERT INTO trades (symbol, side, qty, price, ord_type, trade_id, timestamp)
                                              VALUES (?, ?, ?, ?, ?, ?, ?)''',
                                           (symbol, side, qty, price, ord_type, trade_id, timestamp))
                            conn.commit()

                            trade['volume'] = trade['qty']
                            handle_data(trade)

                else:
                    print(response)
            except ConnectionClosedError as e:
                print(e)
                break

def main():
    try:
        while not shutodwn_event.is_set():
            shutodwn_event.wait(1)
    except KeyboardInterrupt:
        shutodwn_event.set()
        sys.exit(0)

def create_threads():
    signal.signal(signal.SIGINT, signal_handler)
    thread1 = threading.Thread(target=lambda: asyncio.run(connect_auth()), daemon=True)
    thread2 = threading.Thread(target=lambda: asyncio.run(connect()), daemon=True)
    thread3 = threading.Thread(target=plot_ohlc_charts, daemon=True)
    thread1.start()
    thread2.start()
    thread3.start()

    main()


if __name__ == "__main__":
    create_threads()