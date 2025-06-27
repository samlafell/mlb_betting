#!/usr/bin/env python3
"""
Standalone debug script to identify why strategies aren't generating signals.
"""

import asyncio
import sys
import os
from pathlib import Path

# Add the src directory to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from mlb_sharp_betting.db.connection import get_db_manager
from typing import Dict, Any, List


class StrategySignalDebugger:
    """Debug utility to analyze why strategies aren't generating signals."""
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
    
    def debug_strategy_signal_generation(self, strategy_name: str) -> Dict[str, Any]:
        """
        Deep debug analysis for a specific strategy.
        """
        debug_info = {
            'strategy_name': strategy_name,
            'raw_data_available': False,
            'data_quality_issues': [],
            'signal_generation_steps': [],
            'potential_fixes': []
        }
        
        try:
            # Step 1: Check raw data availability
            with self.db_manager.get_cursor() as cursor:
                # Check betting splits data
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_splits,
                        COUNT(DISTINCT game_id) as unique_games,
                        MIN(last_updated) as oldest_data,
                        MAX(last_updated) as newest_data,
                        COUNT(DISTINCT sportsbook) as unique_books
                    FROM splits.raw_mlb_betting_splits 
                    WHERE last_updated >= NOW() - INTERVAL '7 days'
                """)
                
                data_stats = cursor.fetchone()
                if data_stats:
                    debug_info['raw_data_stats'] = data_stats
                    debug_info['raw_data_available'] = data_stats['total_splits'] > 0
                else:
                    debug_info['raw_data_available'] = False
                
                if not debug_info['raw_data_available']:
                    debug_info['potential_fixes'].append("No betting splits data found - run data collection")
                    return debug_info
                
                # Check game outcomes
                cursor.execute("""
                    SELECT COUNT(*) as total_outcomes
                    FROM public.game_outcomes 
                    WHERE updated_at >= NOW() - INTERVAL '7 days'
                """)
                
                outcomes_stats = cursor.fetchone()
                debug_info['outcomes_available'] = outcomes_stats['total_outcomes'] > 0 if outcomes_stats else False
                
                if not debug_info['outcomes_available']:
                    debug_info['potential_fixes'].append("No game outcomes data - outcomes may be missing")
                
                # Step 2: Check specific data quality for strategy
                self._check_strategy_specific_data_quality(cursor, strategy_name, debug_info)
                
                # Step 3: Test signal generation with sample data
                self._test_signal_generation_logic(cursor, strategy_name, debug_info)
                
        except Exception as e:
            debug_info['error'] = str(e)
            print(f"Debug failed for {strategy_name}: {e}")
        
        return debug_info
    
    def _check_strategy_specific_data_quality(self, cursor, strategy_name: str, debug_info: Dict):
        """Check data quality specific to strategy requirements."""
        
        # Different strategies have different data requirements
        strategy_data_requirements = {
            'opposing_markets': {
                'min_books': 2,
                'required_fields': ['moneyline_home', 'moneyline_away'],
                'description': 'Needs odds from multiple books for comparison'
            },
            'book_conflicts': {
                'min_books': 3,
                'required_fields': ['moneyline_home', 'moneyline_away'],
                'description': 'Needs odds conflicts between 3+ books'
            },
            'public_money_fade': {
                'min_books': 1,
                'required_fields': ['bet_percentage_home', 'bet_percentage_away'],
                'description': 'Needs public betting percentages'
            },
            'late_sharp_flip': {
                'min_books': 1,
                'required_fields': ['moneyline_home', 'moneyline_away', 'last_updated'],
                'description': 'Needs time-series odds data for line movement'
            },
            'consensus_moneyline': {
                'min_books': 3,
                'required_fields': ['moneyline_home', 'moneyline_away'],
                'description': 'Needs consensus from multiple books'
            },
            'underdog_ml_value': {
                'min_books': 1,
                'required_fields': ['moneyline_home', 'moneyline_away'],
                'description': 'Needs moneyline odds to identify underdogs'
            },
            'line_movement': {
                'min_books': 1,
                'required_fields': ['moneyline_home', 'moneyline_away', 'last_updated'],
                'description': 'Needs historical odds for movement analysis'
            }
        }
        
        requirements = strategy_data_requirements.get(strategy_name, {
            'min_books': 1,
            'required_fields': ['moneyline_home', 'moneyline_away'],
            'description': 'Basic moneyline requirements'
        })
        
        debug_info['strategy_requirements'] = requirements
        
        # Check if we have enough books
        cursor.execute("""
            SELECT COUNT(DISTINCT sportsbook) as book_count
            FROM splits.raw_mlb_betting_splits 
            WHERE last_updated >= NOW() - INTERVAL '7 days'
        """)
        
        book_result = cursor.fetchone()
        book_count = book_result['book_count'] if book_result else 0
        
        if book_count < requirements['min_books']:
            debug_info['data_quality_issues'].append(
                f"Insufficient sportsbooks: need {requirements['min_books']}, have {book_count}"
            )
            debug_info['potential_fixes'].append(
                f"Add more sportsbook data sources (need {requirements['min_books']} minimum)"
            )
        
        # Check required fields availability
        for field in requirements['required_fields']:
            try:
                cursor.execute(f"""
                    SELECT COUNT(*) as non_null_count
                    FROM splits.raw_mlb_betting_splits 
                    WHERE {field} IS NOT NULL 
                    AND last_updated >= NOW() - INTERVAL '7 days'
                """)
                
                field_result = cursor.fetchone()
                non_null_count = field_result['non_null_count'] if field_result else 0
                
                if non_null_count == 0:
                    debug_info['data_quality_issues'].append(f"Field '{field}' has no data")
                    debug_info['potential_fixes'].append(f"Ensure {field} is being collected from data sources")
            except Exception as e:
                debug_info['data_quality_issues'].append(f"Error checking field '{field}': {e}")
        
        # Check for specific patterns this strategy needs
        self._check_strategy_patterns(cursor, strategy_name, debug_info)
    
    def _check_strategy_patterns(self, cursor, strategy_name: str, debug_info: Dict):
        """Check for specific data patterns each strategy needs."""
        
        try:
            if strategy_name == 'opposing_markets':
                # Check for actual opposing market conditions
                cursor.execute("""
                    SELECT 
                        game_id,
                        COUNT(DISTINCT sportsbook) as book_count,
                        AVG(moneyline_home) as avg_home_odds,
                        STDDEV(moneyline_home) as home_odds_variance
                    FROM splits.raw_mlb_betting_splits 
                    WHERE moneyline_home IS NOT NULL 
                    AND last_updated >= NOW() - INTERVAL '7 days'
                    GROUP BY game_id
                    HAVING COUNT(DISTINCT sportsbook) >= 2
                    ORDER BY home_odds_variance DESC
                    LIMIT 5
                """)
                
                opposing_markets = cursor.fetchall()
                if not opposing_markets:
                    debug_info['data_quality_issues'].append("No games with opposing market conditions found")
                    debug_info['potential_fixes'].append("Need games with significant odds variance between books")
            
            elif strategy_name == 'public_money_fade':
                # Check for betting percentage data
                cursor.execute("""
                    SELECT COUNT(*) as games_with_percentages
                    FROM splits.raw_mlb_betting_splits 
                    WHERE (bet_percentage_home IS NOT NULL OR bet_percentage_away IS NOT NULL)
                    AND last_updated >= NOW() - INTERVAL '7 days'
                """)
                
                percentage_result = cursor.fetchone()
                if percentage_result and percentage_result['games_with_percentages'] == 0:
                    debug_info['data_quality_issues'].append("No betting percentage data available")
                    debug_info['potential_fixes'].append("Ensure VSIN or similar source provides public betting percentages")
            
            elif strategy_name == 'line_movement':
                # Check for time-series data (multiple entries per game)
                cursor.execute("""
                    SELECT 
                        game_id,
                        COUNT(*) as data_points,
                        MAX(last_updated) - MIN(last_updated) as time_span
                    FROM splits.raw_mlb_betting_splits 
                    WHERE last_updated >= NOW() - INTERVAL '7 days'
                    GROUP BY game_id
                    HAVING COUNT(*) > 1
                    ORDER BY data_points DESC
                    LIMIT 5
                """)
                
                movement_data = cursor.fetchall()
                if not movement_data:
                    debug_info['data_quality_issues'].append("No line movement data - only single odds snapshots per game")
                    debug_info['potential_fixes'].append("Collect odds at multiple time points to track line movement")
        except Exception as e:
            debug_info['data_quality_issues'].append(f"Error checking strategy patterns: {e}")
    
    def _test_signal_generation_logic(self, cursor, strategy_name: str, debug_info: Dict):
        """Test the actual signal generation logic with sample data."""
        
        try:
            # Get a small sample of data to test with
            cursor.execute("""
                SELECT 
                    game_id,
                    sportsbook,
                    moneyline_home,
                    moneyline_away,
                    bet_percentage_home,
                    bet_percentage_away,
                    last_updated
                FROM splits.raw_mlb_betting_splits 
                WHERE last_updated >= NOW() - INTERVAL '7 days'
                AND moneyline_home IS NOT NULL
                ORDER BY last_updated DESC
                LIMIT 10
            """)
            
            sample_data = cursor.fetchall()
            debug_info['sample_data_count'] = len(sample_data)
            
            if sample_data:
                debug_info['sample_data_preview'] = sample_data[:3]  # Show first 3 rows
                
                # Try to identify why signals aren't generated
                self._analyze_signal_generation_failure(sample_data, strategy_name, debug_info)
        except Exception as e:
            debug_info['data_quality_issues'].append(f"Error testing signal generation: {e}")
    
    def _analyze_signal_generation_failure(self, sample_data: List, strategy_name: str, debug_info: Dict):
        """Analyze why signal generation might be failing."""
        
        # Common signal generation failure patterns
        failure_patterns = []
        
        # Check for zero/null odds
        zero_odds = [row for row in sample_data if not row['moneyline_home'] or not row['moneyline_away']]
        if zero_odds:
            failure_patterns.append(f"{len(zero_odds)} rows have missing/zero odds")
        
        # Check for unrealistic odds
        unrealistic_odds = [
            row for row in sample_data 
            if (row['moneyline_home'] and (row['moneyline_home'] < -1000 or row['moneyline_home'] > 1000)) or
               (row['moneyline_away'] and (row['moneyline_away'] < -1000 or row['moneyline_away'] > 1000))
        ]
        if unrealistic_odds:
            failure_patterns.append(f"{len(unrealistic_odds)} rows have unrealistic odds")
        
        # Strategy-specific checks
        if strategy_name == 'public_money_fade':
            no_percentages = [
                row for row in sample_data 
                if not row['bet_percentage_home'] and not row['bet_percentage_away']
            ]
            if no_percentages:
                failure_patterns.append(f"{len(no_percentages)} rows missing betting percentages")
        
        debug_info['signal_generation_failures'] = failure_patterns
        
        if failure_patterns:
            debug_info['potential_fixes'].extend([
                "Check data validation rules in scrapers",
                "Verify data transformation logic",
                "Review strategy signal generation thresholds"
            ])


def debug_all_failing_strategies():
    """Debug all strategies that aren't generating signals."""
    
    db_manager = get_db_manager()
    debugger = StrategySignalDebugger(db_manager)
    
    # List of strategies that are failing (based on logs)
    failing_strategies = [
        'opposing_markets',
        'book_conflicts', 
        'public_money_fade',
        'late_sharp_flip',
        'consensus_moneyline',
        'underdog_ml_value',
        'line_movement'
    ]
    
    print("🔍 STRATEGY SIGNAL GENERATION DEBUG REPORT")
    print("=" * 80)
    
    for strategy in failing_strategies:
        print(f"\n🎯 DEBUGGING: {strategy}")
        print("-" * 40)
        
        debug_results = debugger.debug_strategy_signal_generation(strategy)
        
        # Print debug results
        if debug_results.get('raw_data_available'):
            stats = debug_results['raw_data_stats']
            print(f"✅ Raw data: {stats['total_splits']:,} splits from {stats['unique_books']} books")
            print(f"   📅 Data range: {stats['oldest_data']} to {stats['newest_data']}")
            print(f"   🎮 Games: {stats['unique_games']} unique games")
        else:
            print("❌ No raw data available")
        
        if debug_results.get('outcomes_available'):
            print("✅ Game outcomes data available")
        else:
            print("❌ No game outcomes data")
        
        if debug_results.get('strategy_requirements'):
            req = debug_results['strategy_requirements']
            print(f"📋 Requirements: {req['description']}")
            print(f"   📚 Min books: {req['min_books']}")
            print(f"   📊 Required fields: {', '.join(req['required_fields'])}")
        
        if debug_results['data_quality_issues']:
            print("❌ Data Quality Issues:")
            for issue in debug_results['data_quality_issues']:
                print(f"   • {issue}")
        
        if debug_results.get('signal_generation_failures'):
            print("🚫 Signal Generation Issues:")
            for failure in debug_results['signal_generation_failures']:
                print(f"   • {failure}")
        
        if debug_results['potential_fixes']:
            print("💡 Potential Fixes:")
            for fix in debug_results['potential_fixes']:
                print(f"   • {fix}")
        
        if debug_results.get('sample_data_count'):
            print(f"📊 Sample data: {debug_results['sample_data_count']} rows analyzed")
        
        if debug_results.get('error'):
            print(f"❌ Error: {debug_results['error']}")


if __name__ == "__main__":
    debug_all_failing_strategies() 