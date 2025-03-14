import sys
import asyncio
import time
import datetime
import os
import asyncpg
from dotenv import load_dotenv
from tqdm import tqdm

BATCH_SIZE = 10000  # Adjust as needed

async def create_tables_if_not_exist(conn: asyncpg.Connection):
    """
    Creates the necessary tables on the destination if they don't exist.
    (Schema remains the same: id SERIAL PRIMARY KEY, etc.)
    """
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

async def fix_sequence(conn: asyncpg.Connection, table_name: str, pk_column: str):
    """
    Ensures the destination table’s underlying SERIAL sequence is set
    to at least the current maximum ID, preventing any conflict or overlap.
    """
    # Find the max ID
    max_id = await conn.fetchval(f"SELECT COALESCE(MAX({pk_column}), 0) FROM {table_name}")
    # Find the actual sequence name behind SERIAL
    seq_name = await conn.fetchval(
        "SELECT pg_get_serial_sequence($1, $2)",
        table_name, pk_column
    )
    if seq_name:
        # Set the sequence to max_id + 1 (the next available)
        await conn.execute("SELECT setval($1, $2, false)", seq_name, max_id + 1)

async def copy_and_remove_table(table_name: str, key_column: str, snapshot_id: int,
                                src_conn: asyncpg.Connection, dest_conn: asyncpg.Connection):
    """
    Processes rows from the given table in batches:
      1) Select rows with key_column <= snapshot_id
      2) Insert *without* the source 'id' (so new ID is generated)
      3) Delete them from the source
    """
    total_processed = 0
    pbar = tqdm(desc=f"Processing {table_name}", unit="batch")

    while True:
        rows = await src_conn.fetch(
            f"SELECT * FROM {table_name} WHERE {key_column} <= $1 "
            f"ORDER BY {key_column} ASC LIMIT {BATCH_SIZE}",
            snapshot_id
        )
        if not rows:
            break

        # Build the insert statement but omit the primary key column
        columns = list(rows[0].keys())
        # Exclude the source 'id' column from insertion
        dest_columns = [col for col in columns if col != key_column]

        column_list = ', '.join(dest_columns)
        placeholders = ', '.join(f"${i+1}" for i in range(len(dest_columns)))
        insert_query = (
            f"INSERT INTO {table_name} ({column_list}) "
            f"VALUES ({placeholders})"
        )

        # Prepare the data (omit the source ID)
        values_list = [
            tuple(row[col] for col in dest_columns)
            for row in rows
        ]

        # Insert into destination
        try:
            async with dest_conn.transaction():
                await dest_conn.executemany(insert_query, values_list)
        except Exception as e:
            print(f"Failed to copy a batch from '{table_name}' to destination: {e}")
            break

        # Delete from source
        try:
            async with src_conn.transaction():
                id_list = [row[key_column] for row in rows]
                await src_conn.execute(
                    f"DELETE FROM {table_name} WHERE {key_column} = ANY($1::int[])",
                    id_list
                )
        except Exception as e:
            print(f"Failed to delete a batch from source for table '{table_name}': {e}")
            break

        total_processed += len(rows)
        pbar.update(1)

    pbar.close()
    print(f"Completed processing {table_name}: Total rows processed: {total_processed}")

async def main(loop=False):
    start_time = time.time()
    load_dotenv()  # Loads environment variables from a .env file if present
    sleep_hours = os.getenv("SLEEP_HOURS", 8)
    try:
        sleep_hours = int(sleep_hours)
    except ValueError:
        print("**Invalid value for SLEEP_HOURS**\nUsing default value of 8 hours.")
        sleep_hours = 8

    # Read source DB settings
    SRC_HOST = os.getenv("SRC_DB_HOST", "localhost")
    SRC_PORT = int(os.getenv("SRC_DB_PORT", 5432))
    SRC_USER = os.getenv("SRC_DB_USER", "zach")
    SRC_PASS = os.getenv("SRC_DB_PASS", "password")
    SRC_DB_NAME = os.getenv("SRC_DB_NAME", "source_db")

    # Read destination DB settings
    DEST_HOST = os.getenv("DEST_DB_HOST", "localhost")
    DEST_PORT = int(os.getenv("DEST_DB_PORT", 5432))
    DEST_USER = os.getenv("DEST_DB_USER", "zach")
    DEST_PASS = os.getenv("DEST_DB_PASS", "password")
    DEST_DB_NAME = os.getenv("DEST_DB_NAME", "dest_db")

    # Build DSNs
    src_dsn = f"postgresql://{SRC_USER}:{SRC_PASS}@{SRC_HOST}:{SRC_PORT}/{SRC_DB_NAME}"
    dest_dsn = f"postgresql://{DEST_USER}:{DEST_PASS}@{DEST_HOST}:{DEST_PORT}/{DEST_DB_NAME}"

    try:
        while True:
            run_start_time = time.time()

            # Separate connections for each table
            print("Connecting to source database for 'trades'...")
            trades_src_conn = await asyncpg.connect(src_dsn)
            print("Connecting to destination database for 'trades'...")
            trades_dest_conn = await asyncpg.connect(dest_dsn)

            print("Connecting to source database for 'orderbook_entries'...")
            orderbook_src_conn = await asyncpg.connect(src_dsn)
            print("Connecting to destination database for 'orderbook_entries'...")
            orderbook_dest_conn = await asyncpg.connect(dest_dsn)

            try:
                # Ensure the destination tables exist
                print("Ensuring destination tables exist...")
                await create_tables_if_not_exist(trades_dest_conn)

                # Bump the destination sequences so we never clash
                await fix_sequence(trades_dest_conn, "trades", "id")
                await fix_sequence(trades_dest_conn, "orderbook_entries", "id")

                # Also ensure orderbook_dest_conn sequence is up to date,
                # though they both connect to the same DB. It's harmless to do again:
                await fix_sequence(orderbook_dest_conn, "orderbook_entries", "id")
                await fix_sequence(orderbook_dest_conn, "trades", "id")

                # Determine snapshot boundaries using a separate connection
                common_src_conn = await asyncpg.connect(src_dsn)
                snapshot_trade_id = await common_src_conn.fetchval(
                    "SELECT COALESCE(MAX(id), 0) FROM trades"
                )
                snapshot_orderbook_id = await common_src_conn.fetchval(
                    "SELECT COALESCE(MAX(id), 0) FROM orderbook_entries"
                )
                await common_src_conn.close()

                print(f"Snapshot for 'trades': max id = {snapshot_trade_id}")
                print(f"Snapshot for 'orderbook_entries': max id = {snapshot_orderbook_id}")

                # Process both tables concurrently
                tasks = [
                    copy_and_remove_table(
                        "trades", "id", snapshot_trade_id,
                        trades_src_conn, trades_dest_conn
                    ),
                    copy_and_remove_table(
                        "orderbook_entries", "id", snapshot_orderbook_id,
                        orderbook_src_conn, orderbook_dest_conn
                    )
                ]
                await asyncio.gather(*tasks)
                print("Copy and remove process completed successfully.")

            except Exception as e:
                print(f"Error during copy-and-remove process: {e}")
            finally:
                await trades_src_conn.close()
                await trades_dest_conn.close()
                await orderbook_src_conn.close()
                await orderbook_dest_conn.close()

            if not loop:
                break
            else:
                sleep_time = sleep_hours * 60 * 60
                next_run = run_start_time + sleep_time
                time_to_sleep = next_run - time.time()  # Prevents drift
                time_to_sleep_hours = time_to_sleep / 60 / 60
                print(f"Sleeping for {time_to_sleep_hours:.2f} hours...")
                wake_datetime = datetime.datetime.fromtimestamp(next_run)
                print(f"Next run scheduled for: {wake_datetime}\n")
                if time_to_sleep > 0:
                    await asyncio.sleep(time_to_sleep)

    except KeyboardInterrupt:
        print("Shutting down...")
        print("Exiting...")
        sys.exit(0)
    except Exception as e:
        print(f"Error in main: {e}")
        sys.exit(1)

if __name__ == '__main__':
    loop = False
    if sys.argv[-1] == "loop":
        loop = True
    elif sys.argv[-1] in ["help", "h", "-h", "--h", "--help", "-help"]:
        print("To run once:")
        print("\tpython copy_and_delete_data.py")
        print("To run in a loop with a specified SLEEP_TIME:")
        print("\tpython copy_and_delete_data.py [loop]")
        sys.exit(0)
    asyncio.run(main(loop=loop))
