#!/usr/bin/env python3

"""
Test script to trace bet_type values through the complete data flow.
"""

import asyncio
import logging
import json
from datetime import datetime
from pathlib import Path
import sys

# Add project paths
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def test_bet_type_flow():
    """Test the complete bet_type data flow."""
    
    print("🔍 Testing Bet Type Data Flow")
    print("=" * 60)
    
    try:
        import asyncpg
        
        # Connect to database
        conn = await asyncpg.connect("postgresql://samlafell@localhost:5432/mlb_betting")
        
        print("📊 Step 1: Get latest staging record")
        print("-" * 40)
        
        # Get the latest staging record
        staging_record = await conn.fetchrow("""
            SELECT id, game_data 
            FROM public.sbr_parsed_games 
            WHERE raw_html_id IN (4, 5, 6)
            ORDER BY id DESC 
            LIMIT 1
        """)
        
        if not staging_record:
            print("❌ No staging records found")
            return
            
        game_data = staging_record['game_data']
        
        # Parse if it's a string
        if isinstance(game_data, str):
            game_data = json.loads(game_data)
            
        print(f"✅ Found staging record ID: {staging_record['id']}")
        print(f"📋 Game bet_type: '{game_data.get('bet_type')}'")
        print(f"📋 Odds data count: {len(game_data.get('odds_data', []))}")
        
        print("\n📊 Step 2: Simulate Collection Orchestrator Processing")
        print("-" * 40)
        
        # Simulate the collection orchestrator logic
        odds_records = []
        for i, odds in enumerate(game_data.get('odds_data', [])):
            record = {
                'bet_type': game_data.get('bet_type'),
                'sportsbook': odds.get('sportsbook'),
                'timestamp': game_data.get('scraped_at'),
            }
            
            print(f"  🎯 Betting record {i+1}:")
            print(f"     bet_type: '{record['bet_type']}'")
            print(f"     sportsbook: '{record['sportsbook']}'")
            
            odds_records.append(record)
        
        print("\n📊 Step 3: Simulate Integration Service Processing")
        print("-" * 40)
        
        # Simulate the integration service
        from sportsbookreview.services.data_quality_service import DataQualityService
        from sportsbookreview.models.game import EnhancedGame
        import copy
        
        quality_service = DataQualityService()
        
        # Create a test game record like the collection orchestrator would
        test_game = {
            'sbr_game_id': game_data.get('sbr_game_id'),
            'home_team': game_data.get('home_team'),
            'away_team': game_data.get('away_team'),
            'game_datetime': datetime.now(),
            'bet_type': game_data.get('bet_type'),
            'source_url': game_data.get('source_url', 'test'),
            'odds_data': odds_records
        }
        
        print(f"🎯 Before quality processing:")
        print(f"   Game bet_type: '{test_game['bet_type']}'")
        print(f"   Odds records count: {len(test_game['odds_data'])}")
        if test_game['odds_data']:
            print(f"   First odds record bet_type: '{test_game['odds_data'][0]['bet_type']}'")
        
        # Process through quality service (like integration service does)
        validated_games = await quality_service.process_games([test_game])
        
        if not validated_games:
            print("❌ Quality service rejected the game")
            return
            
        print(f"✅ Quality service passed: {len(validated_games)} games")
        
        # Simulate integration service processing
        raw_game = validated_games[0]
        game_dict = copy.deepcopy(raw_game)
        
        print(f"🎯 After quality processing:")
        print(f"   Game bet_type: '{game_dict.get('bet_type')}'")
        print(f"   Odds records count: {len(game_dict.get('odds_data', []))}")
        if game_dict.get('odds_data'):
            print(f"   First odds record bet_type: '{game_dict['odds_data'][0].get('bet_type')}'")
        
        # Transform keys (like integration service does)
        if 'game_datetime' in game_dict:
            game_dict['game_date'] = game_dict.pop('game_datetime')
        
        # Extract odds_data (like integration service does)
        odds_records_final = game_dict.pop('odds_data', [])
        
        print(f"🎯 Final betting records to be stored:")
        for i, record in enumerate(odds_records_final):
            print(f"   Record {i+1}: bet_type='{record.get('bet_type')}', sportsbook='{record.get('sportsbook')}'")
        
        print("\n📊 Step 4: Simulate Storage Service Call")
        print("-" * 40)
        
        # Test the actual storage service
        from sportsbookreview.services.data_storage_service import DataStorageService
        
        storage = DataStorageService()
        await storage.initialize_connection()
        
        print(f"🎯 Calling store_betting_data with {len(odds_records_final)} records...")
        
        # Test with a known game ID
        test_game_id = 999999  # Use our test game ID
        
        for i, bet_record in enumerate(odds_records_final):
            bet_type = bet_record.get('bet_type', '').lower()
            print(f"   Processing record {i+1}: bet_type='{bet_type}', sportsbook='{bet_record.get('sportsbook')}'")
            
            if not bet_type:
                print(f"   ⚠️  Empty bet_type detected!")
                break
        
        await storage.close_connection()
        await conn.close()
        
        print("\n🎉 Test completed!")
        
    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(test_bet_type_flow()) 