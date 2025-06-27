#!/usr/bin/env python3
"""
Synthetic Betting Data Generator for Testing All 10 Processors

This script creates realistic betting scenarios that will trigger each of the 10 implemented processors:
1. sharp_action - Large bet%/money% discrepancies  
2. opposing_markets - Conflicting signals across bet types
3. book_conflicts - Different lines across sportsbooks
4. public_money_fade - Heavy public betting to fade
5. late_sharp_flip - Recent dramatic line movement
6. consensus_moneyline - Strong consensus signals
7. underdog_ml_value - Value underdog opportunities
8. timing_based - Sharp action at key timing windows
9. line_movement - Significant line movement patterns
10. hybrid_line_sharp - Combined line movement + sharp action
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
import pytz

from src.mlb_sharp_betting.core.logging import get_logger
from src.mlb_sharp_betting.db.connection import get_db_manager
from src.mlb_sharp_betting.models.splits import BettingSplit
from src.mlb_sharp_betting.models.game import Game

logger = get_logger(__name__)

class BettingScenarioGenerator:
    """Generates synthetic betting data to trigger all processors"""
    
    def __init__(self):
        self.db_manager = None
        self.est = pytz.timezone('US/Eastern')
        
    async def initialize(self):
        """Initialize database connection"""
        self.db_manager = get_db_manager()
        logger.info("Scenario generator initialized")
    
    def create_test_scenarios(self) -> List[Dict[str, Any]]:
        """Create test scenarios for all 10 processors"""
        
        base_time = datetime.now(self.est)
        game_time_2h = base_time + timedelta(hours=2)  # Game in 2 hours
        game_time_30m = base_time + timedelta(minutes=30)  # Game in 30 minutes
        game_time_6h = base_time + timedelta(hours=6)  # Game in 6 hours
        
        scenarios = [
            # 1. SHARP ACTION TRIGGER - Yankees vs Red Sox
            {
                'processor': 'sharp_action',
                'description': 'Large discrepancy: 25% bets but 75% money (sharp money on Yankees)',
                'game': {
                    'home_team': 'BOS', 'away_team': 'NYY',
                    'game_time': game_time_2h,
                    'game_id': 'test_sharp_action_001'
                },
                'splits': [
                    {
                        'split_type': 'moneyline', 'sportsbook': 'draftkings',
                        'home_bets_pct': 75.0, 'away_bets_pct': 25.0,
                        'home_money_pct': 25.0, 'away_money_pct': 75.0,  # SHARP SIGNAL
                        'split_value': 'NYY -135 / BOS +115'
                    }
                ]
            },
            
            # 2. OPPOSING MARKETS TRIGGER - Dodgers vs Padres
            {
                'processor': 'opposing_markets',
                'description': 'Conflicting signals: ML favors LAD, but spread/total favor SD',
                'game': {
                    'home_team': 'SD', 'away_team': 'LAD',
                    'game_time': game_time_2h,
                    'game_id': 'test_opposing_markets_001'
                },
                'splits': [
                    # Moneyline favors LAD
                    {
                        'split_type': 'moneyline', 'sportsbook': 'draftkings',
                        'home_bets_pct': 30.0, 'away_bets_pct': 70.0,
                        'home_money_pct': 25.0, 'away_money_pct': 75.0,
                        'split_value': 'LAD -150 / SD +130'
                    },
                    # Spread favors SD
                    {
                        'split_type': 'spread', 'sportsbook': 'draftkings',
                        'home_bets_pct': 65.0, 'away_bets_pct': 35.0,
                        'home_money_pct': 70.0, 'away_money_pct': 30.0,
                        'split_value': 'SD +1.5 (-110) / LAD -1.5 (-110)'
                    }
                ]
            },
            
            # 3. BOOK CONFLICTS TRIGGER - Astros vs Rangers
            {
                'processor': 'book_conflicts',  
                'description': 'Different lines: DK has HOU -140, Circa has HOU -165',
                'game': {
                    'home_team': 'TEX', 'away_team': 'HOU',
                    'game_time': game_time_2h,
                    'game_id': 'test_book_conflicts_001'
                },
                'splits': [
                    # DraftKings line
                    {
                        'split_type': 'moneyline', 'sportsbook': 'draftkings',
                        'home_bets_pct': 45.0, 'away_bets_pct': 55.0,
                        'home_money_pct': 40.0, 'away_money_pct': 60.0,
                        'split_value': 'HOU -140 / TEX +120'  # Softer line
                    },
                    # Circa line (sharper book with different line)
                    {
                        'split_type': 'moneyline', 'sportsbook': 'circa',
                        'home_bets_pct': 35.0, 'away_bets_pct': 65.0,
                        'home_money_pct': 30.0, 'away_money_pct': 70.0,
                        'split_value': 'HOU -165 / TEX +145'  # Much sharper line
                    }
                ]
            },
            
            # 4. PUBLIC FADE TRIGGER - Mets vs Braves (popular public team)
            {
                'processor': 'public_money_fade',
                'description': 'Heavy public betting on Mets (80% bets) - fade opportunity',
                'game': {
                    'home_team': 'ATL', 'away_team': 'NYM',
                    'game_time': game_time_2h,
                    'game_id': 'test_public_fade_001'
                },
                'splits': [
                    {
                        'split_type': 'moneyline', 'sportsbook': 'draftkings',
                        'home_bets_pct': 20.0, 'away_bets_pct': 80.0,  # HEAVY PUBLIC on NYM
                        'home_money_pct': 35.0, 'away_money_pct': 65.0,
                        'split_value': 'NYM -125 / ATL +105'
                    }
                ]
            },
            
            # 5. LATE FLIP TRIGGER - Cubs vs Cardinals (game in 30 minutes)
            {
                'processor': 'late_sharp_flip',
                'description': 'Late sharp flip: Recent money flip on Cubs close to game time',
                'game': {
                    'home_team': 'STL', 'away_team': 'CHC',
                    'game_time': game_time_30m,  # Game in 30 minutes - LATE
                    'game_id': 'test_late_flip_001'
                },
                'splits': [
                    {
                        'split_type': 'moneyline', 'sportsbook': 'draftkings',
                        'home_bets_pct': 60.0, 'away_bets_pct': 40.0,
                        'home_money_pct': 35.0, 'away_money_pct': 65.0,  # FLIP: Money going opposite of bets
                        'split_value': 'CHC -110 / STL -110'
                    }
                ]
            },
            
            # 6. CONSENSUS TRIGGER - Giants vs Rockies
            {
                'processor': 'consensus_moneyline',
                'description': 'Strong consensus: All metrics align on Giants',
                'game': {
                    'home_team': 'COL', 'away_team': 'SF',
                    'game_time': game_time_2h,
                    'game_id': 'test_consensus_001'
                },
                'splits': [
                    {
                        'split_type': 'moneyline', 'sportsbook': 'draftkings',
                        'home_bets_pct': 25.0, 'away_bets_pct': 75.0,  # Bet consensus on SF
                        'home_money_pct': 20.0, 'away_money_pct': 80.0,  # Money consensus on SF
                        'split_value': 'SF -180 / COL +155'
                    }
                ]
            },
            
            # 7. UNDERDOG VALUE TRIGGER - Marlins vs Phillies
            {
                'processor': 'underdog_ml_value',
                'description': 'Underdog value: Marlins getting sharp money despite being dogs',
                'game': {
                    'home_team': 'PHI', 'away_team': 'MIA',
                    'game_time': game_time_2h,
                    'game_id': 'test_underdog_value_001'
                },
                'splits': [
                    {
                        'split_type': 'moneyline', 'sportsbook': 'draftkings',
                        'home_bets_pct': 70.0, 'away_bets_pct': 30.0,
                        'home_money_pct': 45.0, 'away_money_pct': 55.0,  # Sharp money on underdog MIA
                        'split_value': 'PHI -155 / MIA +135'  # MIA is underdog but getting sharp $
                    }
                ]
            },
            
            # 8. TIMING BASED TRIGGER - Angels vs Athletics (ultra late timing)
            {
                'processor': 'timing_based',
                'description': 'Ultra-late sharp action: Game in 30 min with sharp indicators',
                'game': {
                    'home_team': 'OAK', 'away_team': 'LAA',
                    'game_time': game_time_30m,  # ULTRA LATE timing
                    'game_id': 'test_timing_001'
                },
                'splits': [
                    {
                        'split_type': 'moneyline', 'sportsbook': 'circa',  # Sharp book
                        'home_bets_pct': 40.0, 'away_bets_pct': 60.0,
                        'home_money_pct': 25.0, 'away_money_pct': 75.0,  # Sharp action on LAA
                        'split_value': 'LAA -125 / OAK +105'
                    }
                ]
            },
            
            # 9. LINE MOVEMENT TRIGGER - Brewers vs Pirates
            {
                'processor': 'line_movement',
                'description': 'Significant line movement: Heavy action moving line',
                'game': {
                    'home_team': 'PIT', 'away_team': 'MIL',
                    'game_time': game_time_2h,
                    'game_id': 'test_line_movement_001'
                },
                'splits': [
                    {
                        'split_type': 'moneyline', 'sportsbook': 'draftkings',
                        'home_bets_pct': 35.0, 'away_bets_pct': 65.0,
                        'home_money_pct': 20.0, 'away_money_pct': 80.0,  # Heavy sharp action
                        'split_value': 'MIL -160 / PIT +140'  # Line has moved significantly
                    }
                ]
            },
            
            # 10. HYBRID SHARP TRIGGER - Orioles vs Guardians
            {
                'processor': 'hybrid_line_sharp',
                'description': 'Hybrid signal: Line movement + sharp action confirmation',
                'game': {
                    'home_team': 'CLE', 'away_team': 'BAL',
                    'game_time': game_time_2h,
                    'game_id': 'test_hybrid_001'
                },
                'splits': [
                    {
                        'split_type': 'moneyline', 'sportsbook': 'draftkings',
                        'home_bets_pct': 30.0, 'away_bets_pct': 70.0,
                        'home_money_pct': 15.0, 'away_money_pct': 85.0,  # Strong sharp action
                        'split_value': 'BAL -145 / CLE +125'
                    },
                    {
                        'split_type': 'spread', 'sportsbook': 'draftkings',
                        'home_bets_pct': 35.0, 'away_bets_pct': 65.0,
                        'home_money_pct': 20.0, 'away_money_pct': 80.0,  # Confirming action
                        'split_value': 'BAL -1.5 (-110) / CLE +1.5 (-110)'
                    }
                ]
            }
        ]
        
        return scenarios
    
    async def insert_test_data(self, scenarios: List[Dict[str, Any]]):
        """Insert test scenarios into database"""
        
        try:
            # Insert games first
            games_inserted = 0
            splits_inserted = 0
            
            for scenario in scenarios:
                # Create game
                game_data = scenario['game']
                
                # Insert game
                game_insert_query = """
                INSERT INTO splits.games (
                    id, game_id, home_team, away_team, game_datetime, 
                    created_at, updated_at
                ) VALUES (
                    %(game_id)s, %(game_id)s, %(home_team)s, %(away_team)s, %(game_datetime)s,
                    NOW(), NOW()
                ) ON CONFLICT (game_id) DO UPDATE SET
                    updated_at = NOW()
                """
                
                await self.db_manager.execute_query(
                    game_insert_query,
                    {
                        'game_id': game_data['game_id'],
                        'home_team': game_data['home_team'],
                        'away_team': game_data['away_team'],
                        'game_datetime': game_data['game_time']
                    }
                )
                games_inserted += 1
                
                # Insert betting splits
                for split_data in scenario['splits']:
                    split_insert_query = """
                    INSERT INTO splits.raw_mlb_betting_splits (
                        game_id, home_team, away_team, game_datetime,
                        split_type, book, source,
                        home_or_over_bets_percentage, away_or_under_bets_percentage,
                        home_or_over_stake_percentage, away_or_under_stake_percentage,
                        split_value, created_at
                    ) VALUES (
                        %(game_id)s, %(home_team)s, %(away_team)s, %(game_datetime)s,
                        %(split_type)s, %(book)s, 'SYNTHETIC_TEST',
                        %(home_bets_pct)s, %(away_bets_pct)s,
                        %(home_money_pct)s, %(away_money_pct)s,
                        %(split_value)s, NOW()
                    )
                    """
                    
                    await self.db_manager.execute_query(
                        split_insert_query,
                        {
                            'game_id': game_data['game_id'],
                            'home_team': game_data['home_team'],
                            'away_team': game_data['away_team'],
                            'game_datetime': game_data['game_time'],
                            'split_type': split_data['split_type'],
                            'book': split_data['sportsbook'],
                            'home_bets_pct': split_data['home_bets_pct'],
                            'away_bets_pct': split_data['away_bets_pct'],
                            'home_money_pct': split_data['home_money_pct'],
                            'away_money_pct': split_data['away_money_pct'],
                            'split_value': split_data['split_value']
                        }
                    )
                    splits_inserted += 1
            
            logger.info(f"✅ Test data inserted: {games_inserted} games, {splits_inserted} splits")
            
            # Print summary
            print(f"\n🎯 SYNTHETIC TEST DATA INSERTED")
            print(f"=" * 50)
            print(f"📊 Games: {games_inserted}")
            print(f"📈 Betting Splits: {splits_inserted}")
            print(f"🎪 Processors to trigger: {len(scenarios)}")
            
            print(f"\n🎭 SCENARIOS CREATED:")
            for i, scenario in enumerate(scenarios, 1):
                print(f"  {i:2d}. {scenario['processor']:20s} - {scenario['description']}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert test data: {e}")
            return False
    
    async def cleanup_test_data(self):
        """Clean up synthetic test data"""
        try:
            # Delete synthetic splits
            await self.db_manager.execute_query(
                "DELETE FROM splits.raw_mlb_betting_splits WHERE source = 'SYNTHETIC_TEST'"
            )
            
            # Delete synthetic games
            test_game_ids = [
                'test_sharp_action_001', 'test_opposing_markets_001', 'test_book_conflicts_001',
                'test_public_fade_001', 'test_late_flip_001', 'test_consensus_001',
                'test_underdog_value_001', 'test_timing_001', 'test_line_movement_001',
                'test_hybrid_001'
            ]
            
            for game_id in test_game_ids:
                await self.db_manager.execute_query(
                    "DELETE FROM splits.games WHERE game_id = %s",
                    (game_id,)
                )
            
            logger.info("✅ Test data cleaned up")
            print("\n🧹 SYNTHETIC TEST DATA CLEANED UP")
            
        except Exception as e:
            logger.error(f"Failed to cleanup test data: {e}")

async def main():
    """Main execution function"""
    
    print("🎯 SYNTHETIC BETTING SCENARIO GENERATOR")
    print("=" * 60)
    print("This script creates realistic betting data to trigger all 10 processors")
    
    generator = BettingScenarioGenerator()
    await generator.initialize()
    
    # Create scenarios
    scenarios = generator.create_test_scenarios()
    
    # Insert test data
    success = await generator.insert_test_data(scenarios)
    
    if success:
        print(f"\n✅ SUCCESS! Run these commands to test detection:")
        print(f"   uv run src/mlb_sharp_betting/cli.py detect opportunities --minutes 300")
        print(f"   uv run src/mlb_sharp_betting/cli.py backtesting run --mode single-run")
        print(f"\n🧹 To cleanup test data afterwards:")
        print(f"   uv run simulate_betting_scenarios.py --cleanup")
    else:
        print(f"\n❌ FAILED to insert test data")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--cleanup":
        # Cleanup mode
        async def cleanup():
            generator = BettingScenarioGenerator()
            await generator.initialize()
            await generator.cleanup_test_data()
        
        asyncio.run(cleanup())
    else:
        # Normal mode - insert test data
        asyncio.run(main()) 