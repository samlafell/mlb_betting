#!/usr/bin/env python3
"""
Debug the field mapping between parser output and storage service input.
"""

import asyncio
import sys
import json
sys.path.append('.')
from sportsbookreview.services.data_storage_service import DataStorageService

async def debug_field_mapping():
    storage = DataStorageService()
    await storage.initialize_connection()
    
    # Get the newest parsed game record to see the raw data
    record = await storage.pool.fetchrow('''
        SELECT id, game_data, parsed_at
        FROM sbr_parsed_games 
        WHERE game_data->>'bet_type' = 'moneyline' 
        AND game_data->'odds_data' != '[]'
        ORDER BY parsed_at DESC
        LIMIT 1
    ''')
    
    if record:
        print(f'🔍 Debugging field mapping for record ID: {record["id"]}')
        print(f'📅 Parsed at: {record["parsed_at"]}')
        
        game_data = json.loads(record["game_data"]) if isinstance(record["game_data"], str) else record["game_data"]
        odds_data = game_data.get("odds_data", [])
        
        print(f'\n🎯 Raw odds data from parser:')
        for i, odds in enumerate(odds_data[:3]):  # Show first 3 records
            print(f'  Record {i+1}:')
            for key, value in odds.items():
                print(f'    {key}: {value}')
            print()
        
        # Show what the collection orchestrator transformation would produce
        print(f'🔄 Collection Orchestrator Transformation:')
        bet_type = game_data.get('bet_type')
        print(f'  bet_type: {bet_type}')
        
        for i, odds in enumerate(odds_data[:3]):
            print(f'  Transformed Record {i+1}:')
            record_transformed = {
                'bet_type': bet_type,
                'sportsbook': odds.get('sportsbook'),
                'timestamp': game_data.get('scraped_at'),
            }
            
            if bet_type == 'moneyline':
                record_transformed['home_ml'] = odds.get('moneyline_home') or odds.get('home_ml')
                record_transformed['away_ml'] = odds.get('moneyline_away') or odds.get('away_ml')
                print(f'    home_ml: {record_transformed["home_ml"]} (from moneyline_home: {odds.get("moneyline_home")}, home_ml: {odds.get("home_ml")})')
                print(f'    away_ml: {record_transformed["away_ml"]} (from moneyline_away: {odds.get("moneyline_away")}, away_ml: {odds.get("away_ml")})')
            
            for key, value in record_transformed.items():
                if key not in ['home_ml', 'away_ml']:  # Already printed above
                    print(f'    {key}: {value}')
            print()
        
        # Check what's actually in the mlb_betting.moneyline table for this time period
        print(f'🏦 Database Records for this time period:')
        db_records = await storage.pool.fetch('''
            SELECT id, game_id, sportsbook, home_ml, away_ml, created_at
            FROM mlb_betting.moneyline 
            WHERE created_at::date = $1
            ORDER BY created_at DESC
            LIMIT 5
        ''', record["parsed_at"].date())
        
        for i, db_record in enumerate(db_records):
            print(f'  DB Record {i+1}: ID={db_record["id"]}, Game={db_record["game_id"]}, Book={db_record["sportsbook"]}')
            print(f'    home_ml: {db_record["home_ml"]}, away_ml: {db_record["away_ml"]}')
            print(f'    created_at: {db_record["created_at"]}')
            print()
    
    await storage.close_connection()

asyncio.run(debug_field_mapping()) 