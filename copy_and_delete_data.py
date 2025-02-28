import sys
import asyncio
import time
import os
import asyncpg
from dotenv import load_dotenv
from tqdm import tqdm

BATCH_SIZE = 10000  # Adjust as needed

async def create_tables_if_not_exist(conn: asyncpg.Connection):
    """
    Creates the necessary tables on the destination if they don't exist.
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

async def copy_and_remove_table(table_name: str, key_column: str, snapshot_id: int,
                                src_conn: asyncpg.Connection, dest_conn: asyncpg.Connection):
    """
    Processes rows from the given table in batches.
    Each batch selects rows with key_column <= snapshot_id (ordered by key_column),
    copies them to the destination, and then deletes them from the source.

    If the copy to the destination fails, that batch is not deleted.
    """
    total_processed = 0
    pbar = tqdm(desc=f"Processing {table_name}", unit="batch")

    while True:
        # Fetch one batch ordered by key_column
        rows = await src_conn.fetch(
            f"SELECT * FROM {table_name} WHERE {key_column} <= $1 ORDER BY {key_column} ASC LIMIT {BATCH_SIZE}",
            snapshot_id
        )
        if not rows:
            break

        # Build the insert statement dynamically (assumes identical schema)
        columns = list(rows[0].keys())
        column_list = ', '.join(columns)
        placeholders = ', '.join(f"${i+1}" for i in range(len(columns)))
        insert_query = f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders})"
        values_list = [tuple(row[col] for col in columns) for row in rows]

        # Attempt to insert the batch into the destination within a transaction.
        try:
            async with dest_conn.transaction():
                await dest_conn.executemany(insert_query, values_list)
        except Exception as e:
            print(f"Failed to copy a batch from '{table_name}' to destination: {e}")
            break

        # Delete the copied rows from the source using a transaction.
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
            # Instead of using one connection for both tables, open separate connections.
            print("Connecting to source database for 'trades'...")
            trades_src_conn = await asyncpg.connect(src_dsn)
            print("Connecting to destination database for 'trades'...")
            trades_dest_conn = await asyncpg.connect(dest_dsn)

            print("Connecting to source database for 'orderbook_entries'...")
            orderbook_src_conn = await asyncpg.connect(src_dsn)
            print("Connecting to destination database for 'orderbook_entries'...")
            orderbook_dest_conn = await asyncpg.connect(dest_dsn)

            try:
                # Ensure the destination tables exist (using one of the destination connections)
                print("Ensuring destination tables exist...")
                await create_tables_if_not_exist(trades_dest_conn)

                # Determine snapshot boundaries using a separate connection.
                common_src_conn = await asyncpg.connect(src_dsn)
                snapshot_trade_id = await common_src_conn.fetchval("SELECT COALESCE(MAX(id), 0) FROM trades")
                snapshot_orderbook_id = await common_src_conn.fetchval("SELECT COALESCE(MAX(id), 0) FROM orderbook_entries")
                await common_src_conn.close()

                print(f"Snapshot for 'trades': max id = {snapshot_trade_id}")
                print(f"Snapshot for 'orderbook_entries': max id = {snapshot_orderbook_id}")

                # Process both tables concurrently, each with its own source and destination connections.
                tasks = [
                    copy_and_remove_table("trades", "id", snapshot_trade_id, trades_src_conn, trades_dest_conn),
                    copy_and_remove_table("orderbook_entries", "id", snapshot_orderbook_id, orderbook_src_conn, orderbook_dest_conn)
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
            if loop:
                sleep_time = sleep_hours * 60 * 60
                next_run = start_time + sleep_time
                time_to_sleep = next_run - time.time() # Prevents drift
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
