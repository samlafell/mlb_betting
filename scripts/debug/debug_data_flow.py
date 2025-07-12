#!/usr/bin/env python3
"""
Debug the complete data flow from parser to database.
"""

import asyncio
import sys
import json
sys.path.append('.')
from sportsbookreview.services.data_storage_service import DataStorageService

async def debug_data_flow():
    storage = DataStorageService()
    await storage.initialize_connection()
    
    # Get a totals record with odds data
    record = await storage.pool.fetchrow('''
        SELECT id, game_data
        FROM sbr_parsed_games 
        WHERE parsed_at::date = '2025-07-08' 
        AND game_data->>'bet_type' = 'totals' 
        AND game_data->'odds_data' != '[]'
        LIMIT 1
    ''')
    
    if record:
        print(f'🔍 Debugging data flow for record ID: {record["id"]}')
        
        game_data = json.loads(record["game_data"]) if isinstance(record["game_data"], str) else record["game_data"]
        odds_data = game_data.get("odds_data", [])
        
        print(f'\n📊 Raw Game Data:')
        print(f'  Bet Type: {game_data.get("bet_type")}')
        print(f'  Game: {game_data.get("away_team")} @ {game_data.get("home_team")}')
        print(f'  Odds Data Count: {len(odds_data)}')
        
        if odds_data:
            print(f'\n🎯 First Odds Record (RAW):')
            first_odds = odds_data[0]
            for key, value in first_odds.items():
                print(f'    {key}: {value}')
            
            # Simulate the collection orchestrator transformation
            print(f'\n🔄 Collection Orchestrator Transformation:')
            record_transform = {
                'bet_type': game_data.get('bet_type'),
                'sportsbook': first_odds.get('sportsbook'),
                'timestamp': game_data.get('scraped_at'),
            }
            
            # Map keys based on bet type (from collection_orchestrator.py line 226)
            if record_transform['bet_type'] in ('total', 'totals'):
                record_transform['total_line'] = first_odds.get('total_line')
                record_transform['over_price'] = first_odds.get('total_over') or first_odds.get('over_price')
                record_transform['under_price'] = first_odds.get('total_under') or first_odds.get('under_price')
            
            print(f'  Transformed Record:')
            for key, value in record_transform.items():
                print(f'    {key}: {value}')
            
            # Check what gets stored in the final betting table
            print(f'\n💾 Final Database Check:')
            
            # Look for this specific game in the betting data
            sbr_game_id = game_data.get('sbr_game_id')
            if sbr_game_id:
                # Check if there's a corresponding entry in the main games table
                game_entry = await storage.pool.fetchrow('''
                    SELECT id, sbr_game_id 
                    FROM games 
                    WHERE sbr_game_id = $1
                ''', sbr_game_id)
                
                if game_entry:
                    print(f'  ✅ Found game in main table: ID {game_entry["id"]}')
                    
                    # Now check betting data for this game
                    betting_records = await storage.pool.fetch('''
                        SELECT * FROM games_with_sportsbookreview_data 
                        WHERE game_id = $1 AND bet_type = 'totals'
                    ''', game_entry["id"])
                    
                    if betting_records:
                        print(f'  ✅ Found {len(betting_records)} totals betting records')
                        first_betting = betting_records[0]
                        print(f'  📊 First betting record:')
                        for key, value in dict(first_betting).items():
                            if value is not None:
                                print(f'    {key}: {value}')
                    else:
                        print(f'  ❌ No totals betting records found')
                else:
                    print(f'  ❌ Game not found in main table')
    
    await storage.close_connection()

if __name__ == "__main__":
    asyncio.run(debug_data_flow()) 