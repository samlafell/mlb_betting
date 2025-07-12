#!/usr/bin/env python3
"""
Check totals records in the database.
"""

import asyncio
import sys
import json
sys.path.append('.')
from sportsbookreview.services.data_storage_service import DataStorageService

async def check_totals():
    storage = DataStorageService()
    await storage.initialize_connection()
    
    # Check parsed games with totals data
    result = await storage.pool.fetchrow('''
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN game_data->>'bet_type' = 'totals' THEN 1 END) as totals_records,
            COUNT(CASE WHEN game_data->>'bet_type' = 'totals' AND game_data->'odds_data' != '[]' THEN 1 END) as totals_with_odds
        FROM sbr_parsed_games 
        WHERE parsed_at::date = '2025-07-08'
    ''')
    
    print(f'Parsed Games Analysis (July 8th):')
    print(f'  Total records: {result["total_records"]}')
    print(f'  Totals records: {result["totals_records"]}')
    print(f'  Totals with odds data: {result["totals_with_odds"]}')
    
    # Check what bet types we have
    bet_types = await storage.pool.fetch('''
        SELECT game_data->>'bet_type' as bet_type, COUNT(*) as count
        FROM sbr_parsed_games 
        WHERE parsed_at::date = '2025-07-08'
        GROUP BY game_data->>'bet_type'
        ORDER BY count DESC
    ''')
    
    print(f'\nBet types breakdown:')
    for row in bet_types:
        print(f'  {row["bet_type"]}: {row["count"]} records')
    
    # Check a totals record with odds data
    totals_with_data = await storage.pool.fetchrow('''
        SELECT id, game_data
        FROM sbr_parsed_games 
        WHERE parsed_at::date = '2025-07-08' 
        AND game_data->>'bet_type' = 'totals' 
        AND game_data->'odds_data' != '[]'
        LIMIT 1
    ''')
    
    if totals_with_data:
        print(f'\nTotals record with odds data:')
        print(f'  ID: {totals_with_data["id"]}')
        
        game_data = json.loads(totals_with_data["game_data"]) if isinstance(totals_with_data["game_data"], str) else totals_with_data["game_data"]
        odds_data = game_data.get("odds_data", [])
        
        print(f'  Game: {game_data.get("away_team")} @ {game_data.get("home_team")}')
        print(f'  Bet Type: {game_data.get("bet_type")}')
        print(f'  Odds Data Count: {len(odds_data)}')
        
        if odds_data:
            first_odds = odds_data[0]
            print(f'  First Odds Record:')
            for key, value in first_odds.items():
                if value is not None:
                    print(f'    {key}: {value}')
    
    await storage.close_connection()

if __name__ == "__main__":
    asyncio.run(check_totals()) 