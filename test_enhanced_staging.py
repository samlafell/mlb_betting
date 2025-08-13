#!/usr/bin/env python3
"""
Test script for enhanced staging zone processing with multi-bet type support.
"""
import asyncio
import asyncpg
import json
from src.core.config import get_settings

async def test_enhanced_staging():
    """Test the enhanced staging zone with real Action Network data."""
    
    # Get database connection
    config = get_settings()
    conn = await asyncpg.connect(
        host=config.database.host,
        port=config.database.port,
        user=config.database.user,
        password=config.database.password,
        database=config.database.database
    )
    
    print("=== ENHANCED STAGING ZONE TEST ===")
    
    # Get a sample raw record
    record = await conn.fetchrow("""
        SELECT id, raw_odds 
        FROM raw_data.action_network_odds 
        LIMIT 1
    """)
    
    if not record:
        print("No raw records found!")
        return
    
    print(f"Testing with record ID: {record['id']}")
    
    # Parse the raw_odds to see structure
    raw_odds_text = record['raw_odds']
    raw_odds = json.loads(raw_odds_text) if isinstance(raw_odds_text, str) else raw_odds_text
    print(f"Raw odds structure: {json.dumps(raw_odds, indent=2)[:500]}...")
    
    # Check what bet types are available
    bet_types_found = []
    for bet_type in ['moneyline', 'spread', 'total']:
        if bet_type in raw_odds and isinstance(raw_odds[bet_type], list) and raw_odds[bet_type]:
            bet_types_found.append(bet_type)
            print(f"Found {bet_type}: {len(raw_odds[bet_type])} entries")
    
    print(f"Available bet types: {bet_types_found}")
    
    # Calculate expected staging records
    total_expected_records = 0
    for bet_type in bet_types_found:
        if bet_type in raw_odds:
            total_expected_records += len(raw_odds[bet_type])
    
    print(f"Expected staging records from this raw record: {total_expected_records}")
    
    # Check current staging table counts
    moneylines_count = await conn.fetchval("SELECT COUNT(*) FROM staging.moneylines")
    spreads_count = await conn.fetchval("SELECT COUNT(*) FROM staging.spreads")
    totals_count = await conn.fetchval("SELECT COUNT(*) FROM staging.totals")
    
    print(f"Current staging table counts:")
    print(f"  Moneylines: {moneylines_count}")
    print(f"  Spreads: {spreads_count}")
    print(f"  Totals: {totals_count}")
    print(f"  Total staging records: {moneylines_count + spreads_count + totals_count}")
    
    # Calculate potential for all raw records
    total_raw_records = await conn.fetchval("SELECT COUNT(*) FROM raw_data.action_network_odds")
    print(f"\nTotal raw records available: {total_raw_records}")
    print(f"Potential staging records (if all processed): ~{total_raw_records * len(bet_types_found)} (assuming {len(bet_types_found)} bet types per record)")
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(test_enhanced_staging())