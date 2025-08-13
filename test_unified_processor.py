#!/usr/bin/env python3
"""
Test script for UnifiedStagingProcessor data quality improvements.
"""

import asyncio
import asyncpg
from src.core.config import get_settings

async def test_database_and_processor():
    """Test the improved UnifiedStagingProcessor with real data."""
    print("🧪 Testing team resolution with real database data...")
    
    # Connect directly to database
    config = get_settings()
    conn = await asyncpg.connect(
        host=config.database.host,
        port=config.database.port,
        user=config.database.user,
        password=config.database.password,
        database=config.database.database
    )
    
    # Test our team resolution logic
    print("\n🔍 Testing team resolution improvements...")
    
    # Check if team data exists for game 258890 (Cincinnati Reds)
    query = """
    SELECT external_game_id, home_team, away_team, game_date, start_time
    FROM raw_data.action_network_games 
    WHERE external_game_id = '258890'
    """
    
    game_row = await conn.fetchrow(query)
    if game_row:
        print(f"✅ Found game data for 258890:")
        print(f"   Home Team: {game_row['home_team']}")
        print(f"   Away Team: {game_row['away_team']}")
        print(f"   Game Date: {game_row['game_date']}")
        print(f"   Start Time: {game_row['start_time']}")
    else:
        print("❌ No game data found for 258890")
    
    # Check raw odds structure
    odds_query = """
    SELECT external_game_id, sportsbook_key, 
           raw_odds->'moneyline'->0->>'team_id' as home_team_id,
           raw_odds->'moneyline'->1->>'team_id' as away_team_id,
           raw_odds->'moneyline'->0->>'odds' as home_ml_odds,
           raw_odds->'moneyline'->1->>'odds' as away_ml_odds
    FROM raw_data.action_network_odds 
    WHERE external_game_id = '258890' 
    AND raw_odds IS NOT NULL
    LIMIT 1
    """
    
    odds_row = await conn.fetchrow(odds_query)
    if odds_row:
        print(f"\n💰 Found betting odds data:")
        print(f"   Home Team ID: {odds_row['home_team_id']}")
        print(f"   Away Team ID: {odds_row['away_team_id']}")
        print(f"   Home ML Odds: {odds_row['home_ml_odds']}")
        print(f"   Away ML Odds: {odds_row['away_ml_odds']}")
        
        # Test our team ID mapping
        if odds_row['home_team_id'] and odds_row['away_team_id']:
            home_id = int(odds_row['home_team_id'])
            away_id = int(odds_row['away_team_id'])
            
            # Updated mapping from our processor
            action_network_team_mapping = {
                202: "CIN",  # Cincinnati Reds
                212: "PHI",  # Philadelphia Phillies
                216: "WSH",  # Washington Nationals
                # Add more mappings as discovered from data
            }
            
            home_mapped = action_network_team_mapping.get(home_id, f"UNKNOWN_{home_id}")
            away_mapped = action_network_team_mapping.get(away_id, f"UNKNOWN_{away_id}")
            
            print(f"\n🔄 Team ID Mapping Test:")
            print(f"   {home_id} → {home_mapped}")
            print(f"   {away_id} → {away_mapped}")
            
            # Compare with database team names
            if game_row:
                from src.core.team_utils import normalize_team_name
                db_home = normalize_team_name(game_row['home_team']) if game_row['home_team'] else None
                db_away = normalize_team_name(game_row['away_team']) if game_row['away_team'] else None
                
                print(f"\n📊 Comparison:")
                print(f"   Database Home: {db_home}")
                print(f"   Mapped Home: {home_mapped}")
                print(f"   Database Away: {db_away}")
                print(f"   Mapped Away: {away_mapped}")
                
                if db_home == home_mapped and db_away == away_mapped:
                    print("✅ Perfect match! Team resolution working correctly.")
                elif db_home and db_away:
                    print("⚠️ Mismatch - need to update team ID mapping")
                else:
                    print("📝 Database teams available for mapping update")
    else:
        print("❌ No betting odds data found")
    
    # Test a few more games
    print(f"\n🎯 Testing multiple games...")
    multi_query = """
    SELECT DISTINCT g.external_game_id, g.home_team, g.away_team,
           EXISTS(SELECT 1 FROM raw_data.action_network_odds o 
                  WHERE o.external_game_id = g.external_game_id 
                  AND o.raw_odds IS NOT NULL) as has_odds_data
    FROM raw_data.action_network_games g
    WHERE g.home_team IS NOT NULL AND g.away_team IS NOT NULL
    LIMIT 5
    """
    
    multi_rows = await conn.fetch(multi_query)
    for row in multi_rows:
        print(f"   Game {row['external_game_id']}: {row['home_team']} vs {row['away_team']} (Odds: {row['has_odds_data']})")
    
    await conn.close()
    print("\n✅ Test completed!")

if __name__ == "__main__":
    asyncio.run(test_database_and_processor())