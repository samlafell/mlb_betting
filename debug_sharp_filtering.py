#!/usr/bin/env python3
"""
Debug script to understand why Sharp action signals are being filtered out.
All 17 raw signals are found but 0 final signals are produced.
"""

import asyncio
import sys
from datetime import datetime, timedelta

sys.path.append('/Users/samlafell/Documents/programming_projects/sports_betting_dime_splits')

from src.mlb_sharp_betting.services.betting_signal_repository import BettingSignalRepository

async def debug_sharp_filtering():
    """Debug why sharp action signals are being filtered out."""
    
    repository = BettingSignalRepository()
    
    try:
        await repository.connect()
        
        # Get profitable strategies
        strategies = await repository.get_profitable_strategies()
        print(f"📊 Total profitable strategies: {len(strategies)}")
        
        # Show strategies that could be related to sharp action
        print("\n🔪 Sharp Action Related Strategies:")
        sharp_keywords = ['sharp', 'vsin', 'sbd', 'draftkings', 'circa', 'merged']
        
        relevant_strategies = []
        for strategy in strategies:
            strategy_name_lower = strategy.strategy_name.lower()
            source_book_lower = strategy.source_book.lower()
            
            is_relevant = (
                any(keyword in strategy_name_lower for keyword in sharp_keywords) or
                any(keyword in source_book_lower for keyword in sharp_keywords)
            )
            
            if is_relevant:
                relevant_strategies.append(strategy)
                print(f"  • {strategy.strategy_name}")
                print(f"    Source/Book: {strategy.source_book}")
                print(f"    Split Type: {strategy.split_type}")  
                print(f"    Win Rate: {strategy.win_rate}%")
                print(f"    ROI: {strategy.roi}%")
                print(f"    Total Bets: {strategy.total_bets}")
                print()
        
        print(f"Found {len(relevant_strategies)} potentially relevant strategies")
        
        # Get raw signal data (same time window as the detect command)
        start_time = datetime.now() - timedelta(minutes=5)  
        end_time = datetime.now() + timedelta(minutes=300)
        
        raw_signals = await repository.get_sharp_signal_data(start_time, end_time)
        print(f"\n📡 Raw signals found: {len(raw_signals)}")
        
        if not raw_signals:
            print("❌ No raw signals found - exiting")
            return
        
        # Analyze raw signals
        print("\n🔍 RAW SIGNAL ANALYSIS:")
        print("=" * 60)
        
        differential_ranges = {
            '0-10%': 0, '10-15%': 0, '15-20%': 0, '20-25%': 0, '25%+': 0
        }
        
        sources = {}
        books = {}
        split_types = {}
        
        for i, row in enumerate(raw_signals[:10]):  # Show first 10 in detail
            print(f"\nSignal {i+1}:")
            print(f"  Game: {row.get('away_team')} @ {row.get('home_team')}")
            print(f"  Source: {row.get('source')}")
            print(f"  Book: {row.get('book')}")
            print(f"  Split Type: {row.get('split_type')}")
            differential = float(row.get('differential', 0))
            print(f"  Differential: {differential}%")
            print(f"  Game Time: {row.get('game_datetime')}")
            
            # Track statistics
            abs_diff = abs(differential)
            if abs_diff < 10:
                differential_ranges['0-10%'] += 1
            elif abs_diff < 15:
                differential_ranges['10-15%'] += 1
            elif abs_diff < 20:
                differential_ranges['15-20%'] += 1
            elif abs_diff < 25:
                differential_ranges['20-25%'] += 1
            else:
                differential_ranges['25%+'] += 1
            
            source = row.get('source', 'unknown')
            book = row.get('book', 'unknown')
            split_type = row.get('split_type', 'unknown')
            
            sources[source] = sources.get(source, 0) + 1
            books[book] = books.get(book, 0) + 1
            split_types[split_type] = split_types.get(split_type, 0) + 1
        
        # Show summary statistics
        print(f"\n📊 SIGNAL STATISTICS (all {len(raw_signals)} signals):")
        print(f"Differential Ranges: {differential_ranges}")
        print(f"Sources: {sources}")  
        print(f"Books: {books}")
        print(f"Split Types: {split_types}")
        
        # Check strategy matching patterns
        print(f"\n🔍 STRATEGY MATCHING ANALYSIS:")
        print("=" * 60)
        
        # Look at what book-strategy combinations we have vs what we need
        needed_combinations = set()
        available_combinations = set()
        
        for row in raw_signals:
            source = str(row.get('source', 'unknown')).upper()
            book = str(row.get('book', 'unknown')).lower()
            split_type = str(row.get('split_type', 'unknown')).lower()
            needed_combinations.add(f"{source}-{book}-{split_type}")
        
        for strategy in relevant_strategies:
            # Try to extract book information from strategy
            source_book = strategy.source_book.lower()
            if 'vsin-draftkings' in source_book or 'vsin-dk' in source_book:
                available_combinations.add(f"VSIN-draftkings-{strategy.split_type}")
            elif 'vsin-circa' in source_book:
                available_combinations.add(f"VSIN-circa-{strategy.split_type}")
            elif 'sbd' in source_book:
                available_combinations.add(f"SBD-unknown-{strategy.split_type}")
            else:
                # Try to infer from strategy name
                strategy_name_lower = strategy.strategy_name.lower()
                if 'vsin' in strategy_name_lower and 'dra' in strategy_name_lower:
                    available_combinations.add(f"VSIN-draftkings-{strategy.split_type}")
                elif 'vsin' in strategy_name_lower and 'cir' in strategy_name_lower:
                    available_combinations.add(f"VSIN-circa-{strategy.split_type}")
        
        print(f"Needed strategy combinations:")
        for combo in sorted(needed_combinations):
            print(f"  • {combo}")
        
        print(f"\nAvailable strategy combinations:")
        for combo in sorted(available_combinations):
            print(f"  • {combo}")
        
        print(f"\nMissing combinations:")
        missing = needed_combinations - available_combinations
        for combo in sorted(missing):
            print(f"  ❌ {combo}")
        
        print(f"\nMatching combinations:")
        matching = needed_combinations & available_combinations
        for combo in sorted(matching):
            print(f"  ✅ {combo}")
        
        # Look at thresholds
        print(f"\n🎯 THRESHOLD ANALYSIS:")
        print("=" * 60)
        
        # Check win rates of relevant strategies
        for strategy in relevant_strategies:
            wr = strategy.win_rate
            if wr >= 65:
                threshold = 15.0
            elif wr >= 60:
                threshold = 18.0  
            elif wr >= 55:
                threshold = 22.0
            else:
                threshold = 25.0
            
            print(f"Strategy: {strategy.strategy_name}")
            print(f"  Win Rate: {wr}% → Threshold: {threshold}%")
        
        print(f"\nSignals by differential strength:")
        strong_signals = [s for s in raw_signals if abs(float(s.get('differential', 0))) >= 25]
        medium_signals = [s for s in raw_signals if 15 <= abs(float(s.get('differential', 0))) < 25]
        weak_signals = [s for s in raw_signals if abs(float(s.get('differential', 0))) < 15]
        
        print(f"  Strong (≥25%): {len(strong_signals)} signals")
        print(f"  Medium (15-25%): {len(medium_signals)} signals")  
        print(f"  Weak (<15%): {len(weak_signals)} signals")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await repository.close()

if __name__ == "__main__":
    asyncio.run(debug_sharp_filtering()) 