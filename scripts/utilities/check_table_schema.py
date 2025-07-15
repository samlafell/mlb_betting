#!/usr/bin/env python3
"""
Check the actual table schema for sbr_parsed_games.
"""

import asyncio
import sys

sys.path.append(".")

from sportsbookreview.services.data_storage_service import DataStorageService


async def check_table_schema():
    """Check the actual schema of sbr_parsed_games table."""
    storage = DataStorageService()
    await storage.initialize_connection()

    try:
        print("🔍 Checking sbr_parsed_games table schema...")

        # Get column information
        columns = await storage.pool.fetch("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'sbr_parsed_games'
            ORDER BY ordinal_position
        """)

        print("\n📋 Table columns:")
        for col in columns:
            nullable = "NULL" if col["is_nullable"] == "YES" else "NOT NULL"
            print(f"  {col['column_name']}: {col['data_type']} ({nullable})")

        # Get a sample record to see the actual data
        sample = await storage.pool.fetchrow("""
            SELECT *
            FROM sbr_parsed_games 
            WHERE status = 'parsed'
            LIMIT 1
        """)

        if sample:
            print("\n📄 Sample record structure:")
            for key, value in sample.items():
                value_preview = str(value)[:100] if value else "NULL"
                print(f"  {key}: {value_preview}")

    finally:
        await storage.close_connection()


if __name__ == "__main__":
    asyncio.run(check_table_schema())
