#!/usr/bin/env python3
"""
Test the actual processing logic on staging data to find where the transformation fails.
"""

import asyncio
import sys
import json
from datetime import date
sys.path.append('.')

from sportsbookreview.services.data_storage_service import DataStorageService
from sportsbookreview.services.integration_service import IntegrationService

async def test_actual_processing():
    """Test the actual processing logic step by step."""
    storage = DataStorageService()
    await storage.initialize_connection()
    
    target_date = date(2025, 7, 9)
    
    try:
        print(f"🔍 Testing actual processing logic for {target_date}...")
        
        # Get one staging record
        async with storage.pool.acquire() as conn:
            row = await conn.fetchrow('''
                SELECT id, raw_html_id, game_data, parsed_at, status
                FROM sbr_parsed_games 
                WHERE DATE(parsed_at) = $1
                AND status = 'parsed'
                AND game_data IS NOT NULL
                LIMIT 1
            ''', target_date)
        
        if not row:
            print("❌ No staging record found")
            return
        
        print(f"\n📦 Processing staging record {row['id']}...")
        
        # Parse the game_data
        game_dict = row['game_data']
        if isinstance(game_dict, str):
            game_dict = json.loads(game_dict)
        
        print(f"  Original game_dict bet_type: {game_dict.get('bet_type')}")
        print(f"  Original odds_data count: {len(game_dict.get('odds_data', []))}")
        
        # Show original odds data
        original_odds = game_dict.get('odds_data', [])
        if original_odds:
            print(f"\n📊 Original odds data (first record):")
            first_odds = original_odds[0]
            for key, value in first_odds.items():
                if value is not None:
                    print(f"    {key}: {value}")
        
        # Now simulate the collection orchestrator's transformation logic
        print(f"\n⚙️  Applying collection orchestrator transformation...")
        
        # Build betting_data records from odds_data list (from collection_orchestrator.py lines 202-233)
        odds_records = []
        for odds in game_dict.get('odds_data', []):
            record = {
                'bet_type': game_dict.get('bet_type'),
                'sportsbook': odds.get('sportsbook'),
                'timestamp': game_dict.get('scraped_at'),
            }
            
            # Map keys based on bet type
            if record['bet_type'] == 'moneyline':
                record['home_ml'] = odds.get('moneyline_home') or odds.get('home_ml')
                record['away_ml'] = odds.get('moneyline_away') or odds.get('away_ml')
                print(f"    Moneyline mapping: moneyline_home={odds.get('moneyline_home')} -> home_ml={record['home_ml']}")
                print(f"    Moneyline mapping: moneyline_away={odds.get('moneyline_away')} -> away_ml={record['away_ml']}")
            elif record['bet_type'] == 'spread':
                record['home_spread'] = odds.get('spread_home') or odds.get('home_spread')
                record['away_spread'] = odds.get('spread_away') or odds.get('away_spread')
                record['home_spread_price'] = odds.get('home_spread_price') or odds.get('moneyline_home')
                record['away_spread_price'] = odds.get('away_spread_price') or odds.get('moneyline_away')
                print(f"    Spread mapping: spread_home={odds.get('spread_home')} -> home_spread={record['home_spread']}")
                print(f"    Spread mapping: spread_away={odds.get('spread_away')} -> away_spread={record['away_spread']}")
                print(f"    Spread price mapping: moneyline_home={odds.get('moneyline_home')} -> home_spread_price={record['home_spread_price']}")
                print(f"    Spread price mapping: moneyline_away={odds.get('moneyline_away')} -> away_spread_price={record['away_spread_price']}")
            elif record['bet_type'] in ('total', 'totals'):
                record['total_line'] = odds.get('total_line')
                record['over_price'] = odds.get('total_over') or odds.get('over_price')
                record['under_price'] = odds.get('total_under') or odds.get('under_price')
                print(f"    Total mapping: total_line={odds.get('total_line')} -> total_line={record['total_line']}")
                print(f"    Total mapping: total_over={odds.get('total_over')} -> over_price={record['over_price']}")
                print(f"    Total mapping: total_under={odds.get('total_under')} -> under_price={record['under_price']}")

            odds_records.append(record)
        
        print(f"\n📋 Transformed betting records:")
        for i, record in enumerate(odds_records):
            print(f"  Record {i+1}: {record}")
        
        # Test the integration service
        print(f"\n🔗 Testing integration service...")
        
        # Prepare game data for integration (from collection_orchestrator.py lines 235-252)
        from sportsbookreview.models.game import EnhancedGame
        allowed_fields = set(EnhancedGame.model_fields.keys())
        cleaned_game = {k: v for k, v in game_dict.items() if k in allowed_fields}
        
        # Ensure required validator fields are present
        if 'game_date' in game_dict:
            cleaned_game['game_datetime'] = game_dict['game_date']
        elif 'game_datetime' in game_dict:
            cleaned_game['game_datetime'] = game_dict['game_datetime']
        
        # Ensure bet_type is preserved for validator
        if 'bet_type' not in cleaned_game:
            cleaned_game['bet_type'] = game_dict.get('bet_type')
        
        # Replace the odds_data with our constructed betting records
        cleaned_game['odds_data'] = odds_records
        
        print(f"  Cleaned game data keys: {list(cleaned_game.keys())}")
        print(f"  Odds data count: {len(cleaned_game.get('odds_data', []))}")
        
        # Test integration
        integrator = IntegrationService(storage)
        try:
            inserted = await integrator.integrate([cleaned_game])
            print(f"  ✅ Integration result: {inserted} records inserted")
        except Exception as e:
            print(f"  ❌ Integration failed: {e}")
            import traceback
            traceback.print_exc()
        
    finally:
        await storage.close_connection()

if __name__ == "__main__":
    asyncio.run(test_actual_processing()) 