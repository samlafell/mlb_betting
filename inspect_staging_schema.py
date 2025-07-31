#!/usr/bin/env python3
"""
Inspect STAGING zone table schemas
"""
from src.data.database.connection import get_connection, initialize_connections
from src.core.config import get_settings
import asyncio

async def inspect_staging_schema():
    config = get_settings()
    initialize_connections(config)
    async with get_connection() as conn:
        # Check staging.action_network_games schema
        games_cols = await conn.fetch('''
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_schema = 'staging' AND table_name = 'action_network_games'
            ORDER BY ordinal_position
        ''')
        print('📊 staging.action_network_games columns:')
        for col in games_cols:
            print(f'  {col["column_name"]} ({col["data_type"]}) - nullable: {col["is_nullable"]}')
        
        print()
        
        # Check staging.action_network_odds_historical schema
        odds_cols = await conn.fetch('''
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_schema = 'staging' AND table_name = 'action_network_odds_historical'
            ORDER BY ordinal_position
        ''')
        print('📊 staging.action_network_odds_historical columns:')
        for col in odds_cols:
            print(f'  {col["column_name"]} ({col["data_type"]}) - nullable: {col["is_nullable"]}')
        
        print()
        
        # Check curated table schemas too
        curated_tables = ['enhanced_games', 'unified_betting_splits', 'ml_temporal_features']
        for table_name in curated_tables:
            curated_cols = await conn.fetch(f'''
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_schema = 'curated' AND table_name = '{table_name}'
                ORDER BY ordinal_position
            ''')
            print(f'📊 curated.{table_name} columns:')
            for col in curated_cols:
                print(f'  {col["column_name"]} ({col["data_type"]}) - nullable: {col["is_nullable"]}')
            print()

if __name__ == "__main__":
    asyncio.run(inspect_staging_schema())