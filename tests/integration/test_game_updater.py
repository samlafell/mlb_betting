#!/usr/bin/env python3
"""
Daily MLB Game Updater Script.

This script is designed to be run daily to fetch completed game results 
from the MLB API and store them in the database with real betting lines.

It checks:
- Yesterday's games (most likely to be completed)
- Today's games (in case any finished early)
"""

import asyncio
import sys
from datetime import date, timedelta

# Add the src directory to the path
sys.path.insert(0, "../../src")

from mlb_sharp_betting.services.game_updater import GameUpdater
from mlb_sharp_betting.db.repositories import get_game_outcome_repository
from mlb_sharp_betting.db.connection import get_db_manager


async def main():
    """Daily game update function."""
    print("🏀 Daily MLB Game Updater")
    print("=" * 50)
    print(f"📅 Running daily update on {date.today()}")
    
    try:
        # Initialize the game updater
        updater = GameUpdater()
        print("✅ GameUpdater initialized")
        
        all_outcomes = []
        
        # Update yesterday's games (most likely to be completed)
        yesterday = date.today() - timedelta(days=1)
        print(f"\n📅 Processing completed games for {yesterday}")
        
        yesterday_outcomes = await updater.update_game_outcomes_for_date(yesterday)
        
        if yesterday_outcomes:
            print(f"✅ Found {len(yesterday_outcomes)} completed games from {yesterday}")
            all_outcomes.extend(yesterday_outcomes)
            
            # Display yesterday's outcomes
            for outcome in yesterday_outcomes:
                print(f"\n🏆 {yesterday} Game: {outcome.away_team.value} @ {outcome.home_team.value}")
                print(f"   Score: {outcome.away_score} - {outcome.home_score}")
                print(f"   Winner: {'Home' if outcome.home_win else 'Away'}")
                print(f"   Total: {outcome.away_score + outcome.home_score} (Line: {outcome.total_line}, Over: {outcome.over})")
                print(f"   Home Spread: {outcome.home_spread_line} (Home covered: {outcome.home_cover_spread})")
        else:
            print(f"ℹ️  No completed games found for {yesterday}")
        
        # Update today's games (in case any finished early)
        today = date.today()
        print(f"\n📅 Processing completed games for {today}")
        
        today_outcomes = await updater.update_game_outcomes_for_date(today)
        
        if today_outcomes:
            print(f"✅ Found {len(today_outcomes)} completed games from {today}")
            all_outcomes.extend(today_outcomes)
            
            # Display today's outcomes
            for outcome in today_outcomes:
                print(f"\n🏆 {today} Game: {outcome.away_team.value} @ {outcome.home_team.value}")
                print(f"   Score: {outcome.away_score} - {outcome.home_score}")
                print(f"   Winner: {'Home' if outcome.home_win else 'Away'}")
                print(f"   Total: {outcome.away_score + outcome.home_score} (Line: {outcome.total_line}, Over: {outcome.over})")
                print(f"   Home Spread: {outcome.home_spread_line} (Home covered: {outcome.home_cover_spread})")
        else:
            print(f"ℹ️  No completed games found for {today}")
        
        # Summary
        print(f"\n📊 Daily Update Summary")
        print("=" * 30)
        print(f"Total games processed: {len(all_outcomes)}")
        print(f"Yesterday ({yesterday}): {len(yesterday_outcomes)} games")
        print(f"Today ({today}): {len(today_outcomes)} games")
        
        if all_outcomes:
            # Calculate some basic stats
            home_wins = sum(1 for outcome in all_outcomes if outcome.home_win)
            away_wins = len(all_outcomes) - home_wins
            overs = sum(1 for outcome in all_outcomes if outcome.over)
            unders = len(all_outcomes) - overs
            home_covers = sum(1 for outcome in all_outcomes if outcome.home_cover_spread)
            away_covers = len(all_outcomes) - home_covers
            
            print(f"\n📈 Betting Results Summary:")
            print(f"   Home vs Away: {home_wins}-{away_wins} ({home_wins/len(all_outcomes)*100:.1f}% home)")
            print(f"   Over vs Under: {overs}-{unders} ({overs/len(all_outcomes)*100:.1f}% over)")
            print(f"   Home Spread: {home_covers}-{away_covers} ({home_covers/len(all_outcomes)*100:.1f}% home cover)")
        
        # Check database status
        print(f"\n📊 Database Status")
        print("=" * 20)
        outcome_repo = get_game_outcome_repository()
        recent_outcomes = await outcome_repo.get_recent_outcomes(limit=10)
        
        if recent_outcomes:
            print(f"✅ Database contains {len(recent_outcomes)} recent game outcomes")
            latest_date = max(outcome.game_date for outcome in recent_outcomes)
            print(f"   Latest game date: {latest_date}")
        else:
            print("ℹ️  No game outcomes found in database")
        
        if all_outcomes:
            print(f"\n✅ Daily update completed successfully!")
        else:
            print(f"\nℹ️  Daily update completed - no new games to process")
            
    except Exception as e:
        print(f"❌ Daily update failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main()) 