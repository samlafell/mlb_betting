#!/usr/bin/env python3
"""Test script for MLB API integration."""

import sys
import os
sys.path.append('src')

from datetime import date, datetime
from mlb_sharp_betting.services.mlb_api_service import MLBStatsAPIService

def test_mlb_api_service():
    """Test the MLB API service functionality."""
    print("🏀 Testing MLB Stats API Service Integration")
    print("=" * 50)
    
    # Initialize the service
    mlb_service = MLBStatsAPIService()
    
    # Test 1: Get games for today
    print("\n📅 Test 1: Getting today's MLB games")
    today = date.today()
    games = mlb_service.get_games_for_date(today)
    
    if games:
        print(f"✅ Found {len(games)} games for {today}")
        for game in games[:3]:  # Show first 3 games
            print(f"   🆔 Game {game.game_pk}: {game.away_team} @ {game.home_team}")
            print(f"      📅 {game.game_date.strftime('%Y-%m-%d %H:%M:%S UTC')}")
            print(f"      🏟️ {game.venue}")
            print(f"      📊 Status: {game.status}")
            print()
    else:
        print(f"ℹ️ No games found for {today} (might be off-season or rest day)")
    
    # Test 2: Team name normalization
    print("\n🏷️ Test 2: Team name normalization")
    test_teams = [
        "Yankees", "Red Sox", "Dodgers", "Giants", "Cubs",
        "NYY", "BOS", "LAD", "SF", "CHC",
        "New York Yankees", "Boston Red Sox"
    ]
    
    for team in test_teams:
        normalized = mlb_service.normalize_team_name(team)
        print(f"   '{team}' → '{normalized}'")
    
    # Test 3: Find game by teams (using common team names)
    print("\n🔍 Test 3: Finding games by team names")
    
    test_matchups = [
        ("Yankees", "Red Sox"),
        ("Dodgers", "Giants"), 
        ("New York Yankees", "Boston Red Sox"),
        ("NYY", "BOS")
    ]
    
    for home_team, away_team in test_matchups:
        print(f"\n   Searching for: {away_team} @ {home_team}")
        
        # Search today and tomorrow for games
        for search_date in [today, date.fromordinal(today.toordinal() + 1)]:
            game_info = mlb_service.find_game_by_teams(home_team, away_team, search_date)
            if game_info:
                print(f"   ✅ Found on {search_date}: Game {game_info.game_pk}")
                print(f"      📊 {game_info.away_team} @ {game_info.home_team}")
                print(f"      📅 {game_info.game_date.strftime('%Y-%m-%d %H:%M:%S')}")
                break
        else:
            print(f"   ℹ️ No game found for this matchup in next 2 days")
    
    # Test 4: Batch game ID lookup
    print("\n📦 Test 4: Batch game ID lookup")
    
    # Use team pairs from any games we found
    if games:
        team_pairs = []
        for game in games[:3]:  # Use first 3 games
            team_pairs.append((game.home_team, game.away_team, game.game_date))
        
        print(f"   Testing batch lookup for {len(team_pairs)} team pairs...")
        
        results = mlb_service.batch_get_game_ids(team_pairs)
        
        for (home_team, away_team), game_id in results.items():
            print(f"   {away_team} @ {home_team} → Game ID: {game_id}")
    
    print("\n🎯 Test 5: Get official game ID (main function)")
    
    if games:
        # Use the first game as a test
        test_game = games[0]
        official_id = mlb_service.get_official_game_id(
            home_team=test_game.home_team,
            away_team=test_game.away_team,
            game_datetime=test_game.game_date
        )
        
        print(f"   Input: {test_game.away_team} @ {test_game.home_team}")
        print(f"   Official Game ID: {official_id}")
        print(f"   Expected Game PK: {test_game.game_pk}")
        
        if official_id == str(test_game.game_pk):
            print("   ✅ Perfect match!")
        else:
            print("   ⚠️ ID mismatch - check logic")
    
    print("\n" + "=" * 50)
    print("🏁 MLB API Integration Test Complete!")

if __name__ == "__main__":
    test_mlb_api_service() 