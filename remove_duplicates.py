import asyncio
import asyncpg

# Change these to match your DEST DB connection settings.
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "dest_db"
DB_USER = "postgres"
DB_PASS = "password"

async def find_and_delete_duplicates(table_name: str, conn: asyncpg.Connection):
    """
    1) Finds groups of rows that share all columns except 'id'.
    2) Within each group, keeps the lowest ID and marks the rest for deletion, 
       but only if the ID differs by >= 10 from that min ID.
    3) Returns the total number of sets found and total rows that would be deleted.
    """

    # Identify all potential duplicates by grouping on every column except 'id'.
    # We'll gather an array of IDs (ordered by ID) per group.
    # We also store count(*) so we know how many duplicates are in each group.
    duplicates_query = f"""
        WITH grouped AS (
            SELECT
                -- You can list columns that define "duplicate" content. For trades or orderbook_entries
                (row_to_json({table_name}) - 'id') AS row_data, 
                -- row_to_json(...) - 'id' effectively keeps everything except "id".
                array_agg(id ORDER BY id) AS id_list,
                count(*) AS cnt
            FROM {table_name}
            GROUP BY row_to_json({table_name}) - 'id'
            HAVING count(*) > 1
        )
        SELECT row_data, id_list, cnt
        FROM grouped;
    """

    rows = await conn.fetch(duplicates_query)
    # Each row in 'rows' has:
    #  row_data: the JSON-ified contents of each row (except id)
    #  id_list:  array of IDs with identical row_data
    #  cnt:      how many rows are in that group

    # We'll figure out which IDs to delete:
    delete_ids = []
    for r in rows:
        id_list = r["id_list"]
        # The IDs are sorted ascending. The "lowest" is the first in the list.
        min_id = id_list[0]
        # For each group, keep min_id, remove others, 
        # but only if they differ from min_id by >= 10.
        # So we skip id_list[0], remove everything else if (id - min_id >= 10).
        to_remove = [idx for idx in id_list[1:] if (idx - min_id) >= 10]
        if to_remove:
            delete_ids.extend(to_remove)

    # Just how many "duplicate sets" are we talking about?
    # We'll say a "set" is each row in 'rows' that had >1 in cnt.
    total_sets = len(rows)  
    total_rows_marked = len(delete_ids)

    return total_sets, total_rows_marked, delete_ids

async def main():
    conn = await asyncpg.connect(
        host=DB_HOST, port=DB_PORT,
        user=DB_USER, password=DB_PASS,
        database=DB_NAME
    )

    try:
        # ----- 1) Check 'trades' table duplicates
        print("Checking duplicates in 'trades' table...")
        t_sets, t_rows, t_delete_ids = await find_and_delete_duplicates("trades", conn)
        if t_sets == 0:
            print("No duplicate sets found in 'trades'.")
        else:
            print(f"Found {t_sets} duplicate sets in 'trades', totaling {t_rows} rows to remove.")

        # ----- 2) Check 'orderbook_entries' table duplicates
        print("\nChecking duplicates in 'orderbook_entries' table...")
        o_sets, o_rows, o_delete_ids = await find_and_delete_duplicates("orderbook_entries", conn)
        if o_sets == 0:
            print("No duplicate sets found in 'orderbook_entries'.")
        else:
            print(f"Found {o_sets} duplicate sets in 'orderbook_entries', totaling {o_rows} rows to remove.")

        # Summarize
        if t_sets == 0 and o_sets == 0:
            print("\nNo duplicates found in either table. Exiting.")
            return

        print("\nSummary:")
        print(f"  trades: {t_sets} sets, {t_rows} rows to delete.")
        print(f"  orderbook_entries: {o_sets} sets, {o_rows} rows to delete.")

        # ----- 3) Ask for confirmation
        confirm = input("\nDo you want to remove these duplicates? (y/N) ")
        if confirm.lower().startswith('y'):
            # Actually perform the deletion
            async with conn.transaction():
                if t_delete_ids:
                    await conn.execute(
                        "DELETE FROM trades WHERE id = ANY($1::bigint[])",
                        t_delete_ids
                    )
                if o_delete_ids:
                    await conn.execute(
                        "DELETE FROM orderbook_entries WHERE id = ANY($1::bigint[])",
                        o_delete_ids
                    )
            print("Duplicate rows removed successfully!")
        else:
            print("No changes made.")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
