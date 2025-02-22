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
import asyncpg
import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mpf
import datetime
import logging

from orderbook import listen_to_orderbooks

# Load environment variables
load_dotenv()
ws_token = None

shutdown_event = asyncio.Event()

def signal_handler(sig, frame):
    print("\nShutting down...")
    shutdown_event.set()
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

pairs = ["XRP/USD", "XRP/BTC", "ETH/USD", "ETH/BTC", "BTC/USD", "ADA/USD", "SOL/USD", "SOL/BTC", "DOGE/USD", "JUP/USD", "FARTCOIN/USD", "TAO/USD"]

subscribe_data = {
    "method": "subscribe",
    "params": {
        "channel": "trade",
        "symbol": pairs,
    }
}



# Create a websocket connection
uri = "wss://ws.kraken.com/v2"
auth_uri = "wss://ws-auth.kraken.com/v2"

async def connect_auth():
    while not shutdown_event.is_set():
        try:
            ws_token = get_token()
            balance_subscribe_data = {
                "method": "subscribe",
                "params": {
                    "channel": "balances",
                    "token": ws_token
                }
            }
            async with websockets.connect(auth_uri) as websocket:
                await websocket.send(json.dumps(balance_subscribe_data))
                while not shutdown_event.is_set():
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
                        print(f"Auth Exception: {e}")
                        break
        except Exception as e:
            print(f"Auth Connection Exception: {e}")
            await asyncio.sleep(1)  # Wait before reconnecting

async def connect() -> asyncpg.Pool:
    # Retrieve DB credentials from .env
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = int(os.getenv('DB_PORT', 5432))
    db_user = os.getenv('DB_USER', 'postgres')
    db_pass = os.getenv('DB_PASS', 'password')
    db_name = os.getenv('DB_NAME', 'dashtrade')
    dsn = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    pool = await asyncpg.create_pool(dsn=dsn, min_size=5, max_size=20)

    # Create the trades table (if it doesn't exist) using Postgres syntax
    async with pool.acquire() as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                id SERIAL PRIMARY KEY,
                symbol TEXT,
                side TEXT,
                qty REAL,
                price REAL,
                ord_type TEXT,
                trade_id INTEGER,
                timestamp TIMESTAMPTZ
            )
        ''')

    return pool
    # try:
async def listen_and_push_trades(pool, shutdown_event):
    while not shutdown_event.is_set():
        try:
            async with websockets.connect(uri, ping_interval=20, ping_timeout=20) as websocket:
                await websocket.send(json.dumps(subscribe_data))
                while not shutdown_event.is_set():
                    try:
                        response = await websocket.recv()
                        response = json.loads(response)
                        if response.get('channel') == "heartbeat":
                            continue
                        if response.get('channel') == "trade" and 'data' in response:
                            for trade in response['data']:
                                # Extract trade fields
                                symbol = trade['symbol']
                                side = trade['side']
                                qty = trade['qty']
                                price = trade['price']
                                ord_type = trade['ord_type']
                                trade_id = trade['trade_id']
                                timestamp = trade['timestamp']
                                # ensure that timestamp is a datetime object
                                if isinstance(timestamp, str):
                                    timestamp = datetime.datetime.fromisoformat(timestamp)
                                # Asynchronously insert the trade data into the database
                                async with pool.acquire() as conn:
                                    await conn.execute('''
                                        INSERT INTO trades (symbol, side, qty, price, ord_type, trade_id, timestamp)
                                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                                    ''', symbol, side, qty, price, ord_type, trade_id, timestamp)
                        else:
                            print(response)
                    except json.JSONDecodeError as e:
                        print(f"Trade JSONDecodeError: {e}")
                        continue
                    except ConnectionClosedError as e:
                        print(f"Trade ConnectionClosedError: {e}")
                        break
                    except Exception as e:
                        print(f"Trade Exception: {e}")
                        break
        except Exception as e:
            print(f"Trade Connection Exception: {e}")
            await asyncio.sleep(1)


async def main(trades=True, orderbook=True):
    global ws_token
    signal.signal(signal.SIGINT, signal_handler)
    tasks = []
    print(f'Recording Options: Trades: {trades}, Orderbook: {orderbook}')

    if trades or orderbook:
        pool = await connect()
    else:
        print("No recording options selected.")
        return

    try:
        if trades:
            tasks.append(asyncio.create_task(listen_and_push_trades(pool, shutdown_event)))
        if orderbook:
            tasks.append(asyncio.create_task(listen_to_orderbooks(pairs, pool, shutdown_event)))

        await asyncio.gather(*tasks)
    except Exception as e:
        print(f"Main Exception: {e}")
    finally:
        await pool.close()
        print("Database connection closed.")


async def shutdown():
    print("\nShutting down...")
    shutdown_event.set()

# async def main():
#     loop = asyncio.get_event_loop()
#     loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(shutdown()))
#     loop.run_until_complete(asyncio.gather(connect(), connect_auth()))

async def load_data_from_db(ticker, start_timestamp) -> list:
    # Retrieve DB credentials from .env
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = int(os.getenv('DB_PORT', 5432))
    db_user = os.getenv('DB_USER', 'postgres')
    db_pass = os.getenv('DB_PASS', 'password')
    db_name = os.getenv('DB_NAME', 'dashtrade')

    # Establish an asynchronous connection to Postgres
    conn = await asyncpg.connect(
        host=db_host, port=db_port, user=db_user, password=db_pass, database=db_name
    )

    query = """
        SELECT symbol, side, qty, price, ord_type, trade_id, timestamp
        FROM trades
        WHERE symbol = $1 AND timestamp >= $2
        ORDER BY timestamp ASC
    """
    rows = await conn.fetch(query, ticker, start_timestamp)
    data_points = []
    for row in rows:
        # asyncpg returns Record objects that can be accessed like dicts.
        data_point = {
            'symbol': row['symbol'],
            'side': row['side'],
            'qty': row['qty'],
            'price': row['price'],
            'ord_type': row['ord_type'],
            'trade_id': row['trade_id'],
            'timestamp': row['timestamp']  # should already be a datetime object
        }
        data_points.append(data_point)
    await conn.close()
    return data_points


def create_dollar_bars(data, bar_size) -> pd.DataFrame:
    dollar_bars = []

    # Sort the data by timestamp
    sorted_data = sorted(data, key=lambda x: x['timestamp'])

    for trade in sorted_data:
        # symbol = trade['symbol']
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

                    if remaining_volume <= 0:
                        break

    # Convert list of dictionaries to DataFrame
    df = pd.DataFrame(dollar_bars)
    df.set_index('start_time', inplace=True)

    return df

def classify_trade_tick_rule(trade_price, previous_price, last_side):
    if trade_price > previous_price:
        return 'buy'
    elif trade_price < previous_price:
        return 'sell'
    else:
        # If price hasn't changed, repeat the last classification
        return last_side or 'buy'  # Default assumption

def create_dollar_imbalance_bars(data_points, theta) -> pd.DataFrame:
    cumulative_buy_dollar = 0
    cumulative_sell_dollar = 0
    delta = 0
    bars = []
    current_bar = {
        'open': None,
        'high': float('-inf'),
        'low': float('inf'),
        'close': None,
        'volume': 0,
        'trades': [],
        'num_trades': 0,
        'start_time': None,
        'end_time': None
    }
    previous_price = None
    last_side = None

    for idx, trade in enumerate(data_points):
        # Use 'side' if available, else classify
        if 'side' in trade and trade['side'] in ['buy', 'sell']:
            side = trade['side']
        else:
            if previous_price is not None:
                side = classify_trade_tick_rule(trade['price'], previous_price, last_side)
            else:
                side = 'buy'  # Default for the first trade
            trade['side'] = side  # Add side to trade data

        qty = trade['qty']
        price = trade['price']
        timestamp = trade['timestamp']
        dollar_value = qty * price

        # Initialize current bar's open price and start time
        if current_bar['open'] is None:
            current_bar['open'] = price
            current_bar['start_time'] = timestamp

        # Update high and low
        current_bar['high'] = max(current_bar['high'], price)
        current_bar['low'] = min(current_bar['low'], price)

        # Update volume and trades
        current_bar['volume'] += qty
        current_bar['trades'].append(trade)
        current_bar['num_trades'] += 1

        # Update cumulative dollar volumes
        if side == 'buy':
            cumulative_buy_dollar += dollar_value
        elif side == 'sell':
            cumulative_sell_dollar += dollar_value

        # Update dollar imbalance
        delta = cumulative_buy_dollar - cumulative_sell_dollar

        # Check if threshold is reached
        if abs(delta) >= theta:
            # Set close price and end time
            current_bar['close'] = price
            current_bar['end_time'] = timestamp

            # Add bar to list
            bars.append(current_bar.copy())

            # Reset for next bar
            cumulative_buy_dollar = 0
            cumulative_sell_dollar = 0
            delta = 0
            current_bar = {
                'open': None,
                'high': float('-inf'),
                'low': float('inf'),
                'close': None,
                'volume': 0,
                'trades': [],
                'num_trades': 0,
                'start_time': None,
                'end_time': None
            }

        # Update previous price and side for tick test
        previous_price = price
        last_side = side

    # Handle the last bar if needed
    if current_bar['trades']:
        current_bar['close'] = current_bar['trades'][-1]['price']
        current_bar['end_time'] = current_bar['trades'][-1]['timestamp']
        bars.append(current_bar)


    # Convert list of dictionaries to DataFrame
    df = pd.DataFrame(bars)
    df.set_index('start_time', inplace=True)

    print(f"Last bar imablance: {delta}")
    utc_tz = datetime.timezone.utc
    current_utc_time = datetime.datetime.now(tz=utc_tz)
    last_trade_time = current_bar['end_time'].replace(tzinfo=utc_tz)
    data_recency = current_utc_time - last_trade_time
    print(f"Data recency:\t{data_recency}")
    return df

def add_features(data):
    print("Data has rows " + str(data.shape[0]))
    # Calculate a moving average of the close prices
    data['close_ma_20'] = data['close'].rolling(window=20).mean()
    # Calculate the exponential moving average of the close prices
    data['close_ema_10'] = data['close'].ewm(span=10, adjust=True).mean()
    data['close_ema_20'] = data['close'].ewm(span=20, adjust=True).mean()
    data['close_ema_50'] = data['close'].ewm(span=50, adjust=True).mean()
    data['close_ema_100'] = data['close'].ewm(span=100, adjust=True).mean()
    data['ema_20/50'] = data['close_ema_20'] / data['close_ema_50']
    data['ema_20/100'] = data['close_ema_20'] / data['close_ema_100']
    data['ema_div_diff'] = data['ema_20/50'] - data['ema_20/100']

    data['ema_20-50'] = data['close_ema_20'] - data['close_ema_50']
    data['ema_20-100'] = data['close_ema_20'] - data['close_ema_100']

    data['ext_ema_10'] = ((data['close'] - data['close_ema_10']) / data['close_ema_10']) * 100
    data['ext_ema_20'] = ((data['close'] - data['close_ema_20']) / data['close_ema_20']) * 100
    data['ext_ema_50'] = ((data['close'] - data['close_ema_50']) / data['close_ema_50']) * 100
    data['ext_ema_100'] = ((data['close'] - data['close_ema_100']) / data['close_ema_100']) * 100


    # Calculate the relative strength index
    delta = data['close'].diff()
    rsi_period = 14
    gain = (delta.where(delta > 0, 0)).rolling(window=rsi_period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=rsi_period).mean()
    rs = gain / loss
    data['rsi'] = 100 - (100 / (1 + rs))

def plot_chart(data, symbol):
    # Plot the OHLC chart with the derived features (MA, EMA, RSI, MACD)
    num_bars_to_plot = 150
    # num_bars_to_plot = 650
    data = data.iloc[-num_bars_to_plot:]
    fig, ax = plt.subplots(4, 1, figsize=(12, 8), gridspec_kw={'height_ratios': [3, 1, 3, 3]})

    # Plot the OHLC chart
    mpf.plot(data, type='candle', ax=ax[0], style='charles')
    # Plot the moving averages
    # if 'close_ma_20' in data.columns:
    #     data = data.dropna(subset=['close_ma_20'])
    #     ax[0].plot(data.index, data['close_ma_20'], label='MA 20', color='blue', linewidth=1)
    # if 'close_ema_20' in data.columns:
    #     data = data.dropna(subset=['close_ema_20'])
    #     ax[2].plot(data.index, data['close_ema_20'], label='EMA 20', color='red', linewidth=1)
    if 'ema_20/50' in data.columns:
        data = data.dropna(subset=['ema_20/50'])
        ax[3].axhline(1, color='black', linestyle='--', linewidth=0.5)
        ax[3].plot(data.index, data['ema_20/50'], label='EMA 20/50', color='green', linewidth=1)
        ax[3].plot(data.index, data['ema_20/100'], label='EMA 20/100', color='blue', linewidth=1)
    if 'ext_ema_20' in data.columns:
        data = data.dropna(subset=['ext_ema_20'])
        ax[2].axhline(0, color='black', linestyle='--', linewidth=0.5)
        ax[2].axhline(2.5, color='green', linestyle='--', linewidth=0.5)
        ax[2].axhline(-2.5, color='green', linestyle='--', linewidth=0.5)
        ax[2].axhline(5, color='orange', linestyle='--', linewidth=0.5)
        ax[2].axhline(-5, color='orange', linestyle='--', linewidth=0.5)
        ax[2].axhline(10, color='red', linestyle='--', linewidth=0.5)
        ax[2].axhline(-10, color='red', linestyle='--', linewidth=0.5)
        ax[2].plot(data.index, data['ext_ema_10'], label='Ext EMA 10', color='purple', linewidth=1)
        ax[2].plot(data.index, data['ext_ema_20'], label='Ext EMA 20', color='red', linewidth=1)
        ax[2].plot(data.index, data['ext_ema_50'], label='Ext EMA 50', color='green', linewidth=1)
        ax[2].plot(data.index, data['ext_ema_100'], label='Ext EMA 100', color='blue', linewidth=1)

    # if 'ema_20-50' in data.columns:
    #     data = data.dropna(subset=['ema_20-50'])
    #     ax[3].axhline(0, color='black', linestyle='--', linewidth=0.5)
    #     ax[3].plot(data.index, data['ema_20-50'], label='EMA 20-50', color='green', linewidth=1)
    #     ax[3].plot(data.index, data['ema_20-100'], label='EMA 20-100', color='blue', linewidth=1)

    # if 'ema_div_diff' in data.columns:
    #     data = data.dropna(subset=['ema_div_diff'])
    #     ax[3].axhline(0, color='black', linestyle='--', linewidth=0.5)
    #     ax[3].plot(data.index, data['ema_div_diff'], label='EMA Div Diff', color='green', linewidth=1)

    # Add legend
    ax[0].legend()

    # Plot the RSI
    if 'rsi' in data.columns:
        ax[1].plot(data.index, data['rsi'], label='RSI', color='purple', linewidth=1)

        ax[1].axhline(80, color='orange', linestyle='--', linewidth=0.5)
        ax[1].axhline(70, color='red', linestyle='--', linewidth=0.5)
        ax[1].axhline(50, color='black', linestyle='--', linewidth=0.5)
        ax[1].axhline(30, color='green', linestyle='--', linewidth=0.5)
        ax[1].axhline(20, color='blue', linestyle='--', linewidth=0.5)
        ax[1].set_title('Relative Strength Index')
        ax[1].legend()

    # Plot the trades
    # ax[3].plot(data.index, data['num_trades'], color='gray', label='Trades')

    plt.tight_layout()
    plt.show()


async def read_data(symbol, start_timestamp, plot=False):
    print(f"Reading data after {start_timestamp.isoformat()} from the database...")
    # Load data from the database
    # bar_size = 500000
    # bar_size = 1000000
    imablance_theta = 1500000
    # imablance_theta = 1000000
    # imablance_theta = 750000
    data = await load_data_from_db(symbol, start_timestamp)
    # Create dollar bars
    # dollar_bars = create_dollar_bars(data, bar_size)
    # print(dollar_bars.head())
    # print(dollar_bars.tail())
    # add_features(dollar_bars)

    # plot_chart(dollar_bars, symbol)

    dib_bars = create_dollar_imbalance_bars(data, imablance_theta)
    add_features(dib_bars)
    if plot:
        plot_chart(dib_bars, symbol)
    # Plot the OHLC chart
    # mpf.plot(dollar_bars, type='candle', style='charles', title='charts/' + symbol.replace("/", "_") + ".png")
    # plt.show()

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "record":
        options = [False, False]
        if 'trades' in sys.argv:
            options[0] = True
        if 'orderbook' in sys.argv:
            options[1] = True

        asyncio.run(main(*options))

    elif 'read' in sys.argv:
        symbol = "BTC/USD"
        plot = False
        if 'plot' in sys.argv:
            plot = True
        if len(sys.argv) > 2:
            if sys.argv[2] != "plot":
                symbol = sys.argv[2]
            elif len(sys.argv) > 3:
                symbol = sys.argv[2]

        asyncio.run(read_data(symbol, datetime.datetime(2024, 11, 24), plot=plot))

    else:
        print("Usage: python dashtrade.py [record] [symbol] [plot]")
        print("Example: python dashtrade.py record")
        print("Example: python dashtrade.py read BTC/USD")
        print("Example: python dashtrade.py read BTC/USD plot")
