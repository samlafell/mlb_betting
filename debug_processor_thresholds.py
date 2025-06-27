#!/usr/bin/env python3
"""
Debug script to test processor thresholds and identify why signals aren't being generated
"""

import asyncio
from datetime import datetime, timedelta
from src.mlb_sharp_betting.services.betting_signal_repository import BettingSignalRepository
from src.mlb_sharp_betting.models.betting_analysis import SignalProcessorConfig, ProfitableStrategy

async def debug_processor_thresholds():
    """Test each processor's data requirements and thresholds"""
    
    config = SignalProcessorConfig(minimum_differential=5.0)
    repo = BettingSignalRepository(config)
    
    # Test time window
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=24)
    
    print("="*60)
    print("PROCESSOR THRESHOLD DEBUGGING")
    print("="*60)
    
    # Test Book Conflict Data
    print("\n1. BOOK CONFLICT PROCESSOR:")
    try:
        multi_book_data = await repo.get_multi_book_data(start_time, end_time)
        print(f"   Raw data: {len(multi_book_data)} records")
        
        # Group by game to see conflicts
        games = {}
        for record in multi_book_data:
            key = (record['home_team'], record['away_team'], record['split_type'])
            if key not in games:
                games[key] = []
            games[key].append(record)
        
        # Check for multi-book games
        multi_book_games = {k: v for k, v in games.items() if len(v) >= 2}
        print(f"   Multi-book games: {len(multi_book_games)}")
        
        # Check for actual conflicts
        conflicts = 0
        for game, records in multi_book_games.items():
            diffs = [r['differential'] for r in records]
            if len(set([1 if d > 0 else -1 for d in diffs])) > 1:  # Opposing directions
                conflicts += 1
                max_diff = max([abs(d) for d in diffs])
                print(f"   CONFLICT: {game[0]} vs {game[1]} ({game[2]}) - max diff: {max_diff:.1f}%")
        
        print(f"   Actual conflicts found: {conflicts}")
        print(f"   Current threshold: 15% (likely TOO HIGH)")
        
    except Exception as e:
        print(f"   ERROR: {e}")
    
    # Test Public Fade Data  
    print("\n2. PUBLIC FADE PROCESSOR:")
    try:
        public_data = await repo.get_public_betting_data(start_time, end_time)
        print(f"   Raw data: {len(public_data)} records")
        
        # Check public betting percentages
        heavy_public = 0
        for record in public_data:
            bet_pct = record.get('home_or_over_bets_percentage', 50)
            if bet_pct > 70 or bet_pct < 30:
                heavy_public += 1
                print(f"   FADE OPPORTUNITY: {record['home_team']} vs {record['away_team']} - {bet_pct}% public")
        
        print(f"   Heavy public betting (>70% or <30%): {heavy_public}")
        print(f"   Current threshold: 75% (might be TOO HIGH)")
        
    except Exception as e:
        print(f"   ERROR: {e}")
    
    # Test Late Flip Data
    print("\n3. LATE FLIP PROCESSOR:")
    try:
        steam_data = await repo.get_steam_move_data(start_time, end_time)
        print(f"   Raw data: {len(steam_data)} records")
        
        # Group by game for timeline analysis
        games = {}
        for record in steam_data:
            key = (record['home_team'], record['away_team'], record['split_type'])
            if key not in games:
                games[key] = []
            games[key].append(record)
        
        # Check for games with multiple updates (needed for flip detection)
        multi_update_games = {k: v for k, v in games.items() if len(v) >= 3}
        print(f"   Games with 3+ updates: {len(multi_update_games)}")
        
        # This processor needs complex time-series analysis
        print(f"   Current requirement: 3+ updates + time-based flip detection")
        print(f"   Issue: Complex logic rarely triggered")
        
    except Exception as e:
        print(f"   ERROR: {e}")
    
    # Test Strategy Availability
    print("\n4. STRATEGY MATCHING:")
    try:
        strategies = await repo.get_profitable_strategies()
        print(f"   Total profitable strategies: {len(strategies)}")
        
        # Group by strategy name
        by_name = {}
        for s in strategies:
            name = s['strategy_name']
            if name not in by_name:
                by_name[name] = 0
            by_name[name] += 1
        
        print("   Strategy breakdown:")
        for name, count in by_name.items():
            print(f"     {name}: {count} variants")
        
        # Check for non-sharp strategies
        non_sharp = [name for name in by_name.keys() if 'sharp' not in name.lower()]
        if non_sharp:
            print(f"   Non-sharp strategies: {non_sharp}")
        else:
            print("   ❌ All strategies are sharp-related - other processors need strategies!")
            
    except Exception as e:
        print(f"   ERROR: {e}")
    
    print("\n" + "="*60)
    print("RECOMMENDATIONS:")
    print("="*60)
    print("1. LOWER BookConflict threshold from 15% to 8%")
    print("2. LOWER PublicFade threshold from 75% to 65%") 
    print("3. SIMPLIFY LateFlip logic or lower requirements")
    print("4. ADD profitable strategies for non-sharp processors")
    print("5. ADD debug logging to see exact failure points")

if __name__ == "__main__":
    asyncio.run(debug_processor_thresholds()) 