#!/usr/bin/env python3
"""
Debug the complete integration flow from staging to database.
"""

import asyncio
import sys
import json
from datetime import date
sys.path.append('.')

from sportsbookreview.services.data_storage_service import DataStorageService
from sportsbookreview.services.integration_service import IntegrationService

# Patch multiple methods to trace the flow
original_store_game_data = DataStorageService.store_game_data
original_store_betting_data = DataStorageService.store_betting_data
original_store_moneyline_data = DataStorageService.store_moneyline_data
original_integrate = IntegrationService.integrate

async def debug_store_game_data(self, game_data):
    """Debug wrapper for store_game_data."""
    print(f"🎯 store_game_data called:")
    print(f"  game_data keys: {list(game_data.keys())}")
    if 'betting_data' in game_data:
        betting_data = game_data['betting_data']
        print(f"  betting_data type: {type(betting_data)}")
        print(f"  betting_data length: {len(betting_data) if betting_data else 0}")
        if betting_data:
            print(f"  first betting record: {betting_data[0]}")
    print()
    
    # Call original method
    return await original_store_game_data(self, game_data)

async def debug_store_betting_data(self, game_id, betting_data):
    """Debug wrapper for store_betting_data."""
    print(f"🔄 store_betting_data called:")
    print(f"  game_id: {game_id}")
    print(f"  betting_data type: {type(betting_data)}")
    print(f"  betting_data length: {len(betting_data) if betting_data else 0}")
    if betting_data:
        for i, record in enumerate(betting_data[:3]):
            print(f"  Record {i+1}: {record}")
    print()
    
    # Call original method
    return await original_store_betting_data(self, game_id, betting_data)

async def debug_store_moneyline_data(self, game_id, data):
    """Debug wrapper for store_moneyline_data."""
    print(f"💰 store_moneyline_data called:")
    print(f"  game_id: {game_id}")
    print(f"  data: {data}")
    print(f"  home_ml: {data.get('home_ml')}")
    print(f"  away_ml: {data.get('away_ml')}")
    print()
    
    # Call original method
    return await original_store_moneyline_data(self, game_id, data)

async def debug_integrate(self, raw_games):
    """Debug wrapper for integrate."""
    print(f"🚀 integrate called:")
    print(f"  raw_games length: {len(raw_games) if raw_games else 0}")
    if raw_games:
        first_game = raw_games[0]
        print(f"  first game keys: {list(first_game.keys())}")
        if 'odds_data' in first_game:
            odds_data = first_game['odds_data']
            print(f"  first game odds_data length: {len(odds_data) if odds_data else 0}")
            if odds_data:
                print(f"  first odds record: {odds_data[0]}")
    print()
    
    # Call original method
    return await original_integrate(self, raw_games)

# Apply patches
DataStorageService.store_game_data = debug_store_game_data
DataStorageService.store_betting_data = debug_store_betting_data
DataStorageService.store_moneyline_data = debug_store_moneyline_data
IntegrationService.integrate = debug_integrate

async def test_integration_flow():
    """Test the integration flow with debug logging."""
    print("🚀 Testing integration flow with debug logging...")
    
    storage = DataStorageService()
    await storage.initialize_connection()
    
    # Get a 'new' record with odds data to process
    record = await storage.pool.fetchrow('''
        SELECT id, game_data
        FROM sbr_parsed_games 
        WHERE status = 'new'
        AND game_data->'odds_data' != '[]'
        LIMIT 1
    ''')
    
    if not record:
        print("❌ No 'new' records found to process")
        await storage.close_connection()
        return
    
    print(f"🔍 Processing record ID: {record['id']}")
    
    # Parse the game data
    game_dict = record['game_data']
    if isinstance(game_dict, str):
        game_dict = json.loads(game_dict)
    
    print(f"📊 Game data bet_type: {game_dict.get('bet_type')}")
    print(f"📊 Game data odds_data length: {len(game_dict.get('odds_data', []))}")
    
    # Build the betting data records like the collection orchestrator does
    odds_records = []
    for odds in game_dict.get('odds_data', []):
        record_data = {
            'bet_type': game_dict.get('bet_type'),
            'sportsbook': odds.get('sportsbook'),
            'timestamp': game_dict.get('scraped_at'),
        }
        
        # Map keys based on bet type
        if record_data['bet_type'] == 'moneyline':
            record_data['home_ml'] = odds.get('moneyline_home') or odds.get('home_ml')
            record_data['away_ml'] = odds.get('moneyline_away') or odds.get('away_ml')
            print(f"🔧 Transformed moneyline record: {record_data}")
        
        odds_records.append(record_data)
    
    # Prepare game data for integration
    from sportsbookreview.models.game import EnhancedGame
    allowed_fields = set(EnhancedGame.model_fields.keys())
    cleaned_game = {k: v for k, v in game_dict.items() if k in allowed_fields}
    
    if 'game_date' in game_dict:
        cleaned_game['game_datetime'] = game_dict['game_date']
    elif 'game_datetime' in game_dict:
        cleaned_game['game_datetime'] = game_dict['game_datetime']
    
    # Ensure bet_type is preserved for validator
    cleaned_game['bet_type'] = game_dict.get('bet_type')
    cleaned_game['odds_data'] = odds_records
    
    # Test the integration
    integrator = IntegrationService(storage)
    result = await integrator.integrate([cleaned_game])
    
    print(f"✅ Integration completed. Result: {result}")
    
    await storage.close_connection()

if __name__ == "__main__":
    asyncio.run(test_integration_flow()) 