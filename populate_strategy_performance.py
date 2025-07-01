#!/usr/bin/env python3
"""
Comprehensive Strategy Performance Population Script

This script populates the backtesting.strategy_performance table with comprehensive
data using all available betting splits, game outcomes, and strategy processors.
"""

import asyncio
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any
import traceback

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from mlb_sharp_betting.services.backtesting_engine import BacktestingEngine, get_backtesting_engine
from mlb_sharp_betting.db.connection import get_db_manager
from mlb_sharp_betting.core.logging import get_logger


class StrategyPerformancePopulator:
    """Comprehensive strategy performance data populator"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.db_manager = get_db_manager()
        self.backtesting_engine = None
        
        # Track results
        self.total_results_added = 0
        self.date_ranges_processed = []
        
        self.logger.info("🚀 Strategy Performance Populator initialized")
    
    async def initialize(self):
        """Initialize all required components"""
        self.logger.info("🔧 Initializing components...")
        
        # Initialize backtesting engine
        self.backtesting_engine = get_backtesting_engine()
        await self.backtesting_engine.initialize()
        
        self.logger.info("✅ All components initialized successfully")
    
    async def get_available_data_ranges(self) -> Dict[str, Any]:
        """Get available data ranges for backtesting"""
        with self.db_manager.get_cursor() as cursor:
            # Get betting splits data range
            cursor.execute("""
                SELECT 
                    MIN(last_updated::date) as min_splits_date,
                    MAX(last_updated::date) as max_splits_date,
                    COUNT(*) as splits_count
                FROM splits.raw_mlb_betting_splits
                WHERE home_or_over_bets_percentage IS NOT NULL 
                  AND home_or_over_stake_percentage IS NOT NULL
            """)
            splits_info = cursor.fetchone()
            
            # Get game outcomes data range
            cursor.execute("""
                SELECT 
                    MIN(game_date) as min_outcome_date,
                    MAX(game_date) as max_outcome_date,
                    COUNT(*) as outcomes_count
                FROM public.game_outcomes
                WHERE home_score IS NOT NULL 
                  AND away_score IS NOT NULL
            """)
            outcomes_info = cursor.fetchone()
            
            # Find overlap period where we have both splits and outcomes
            # Convert to dates for comparison
            splits_start = splits_info['min_splits_date']
            splits_end = splits_info['max_splits_date']
            outcomes_start = outcomes_info['min_outcome_date']
            outcomes_end = outcomes_info['max_outcome_date']
            
            # Handle datetime/date conversion
            if hasattr(splits_start, 'date'):
                splits_start = splits_start.date()
            if hasattr(splits_end, 'date'):
                splits_end = splits_end.date()
            if hasattr(outcomes_start, 'date'):
                outcomes_start = outcomes_start.date()
            if hasattr(outcomes_end, 'date'):
                outcomes_end = outcomes_end.date()
            
            overlap_start = max(splits_start, outcomes_start)
            overlap_end = min(splits_end, outcomes_end)
            
            return {
                'splits_range': {
                    'start': splits_info['min_splits_date'],
                    'end': splits_info['max_splits_date'],
                    'count': splits_info['splits_count']
                },
                'outcomes_range': {
                    'start': outcomes_info['min_outcome_date'],
                    'end': outcomes_info['max_outcome_date'],
                    'count': outcomes_info['outcomes_count']
                },
                'overlap_range': {
                    'start': overlap_start,
                    'end': overlap_end
                }
            }
    
    async def run_comprehensive_population(self):
        """Run comprehensive population of strategy performance data"""
        self.logger.info("🎯 Starting comprehensive strategy performance population")
        
        # Get available data ranges
        data_ranges = await self.get_available_data_ranges()
        self.logger.info(f"📊 Available data: {data_ranges}")
        
        overlap_start = data_ranges['overlap_range']['start']
        overlap_end = data_ranges['overlap_range']['end']
        
        if not overlap_start or not overlap_end:
            self.logger.error("❌ No overlapping data between splits and outcomes")
            return
        
        self.logger.info(f"📅 Processing overlap period: {overlap_start} to {overlap_end}")
        
        # Process data in weekly chunks for comprehensive coverage
        await self._populate_weekly_chunks(overlap_start, overlap_end)
        
        # Run comprehensive backtest across full date range
        await self._populate_full_range(overlap_start, overlap_end)
        
        # Generate summary report
        await self._generate_population_report()
    
    async def _populate_weekly_chunks(self, start_date, end_date):
        """Break down into weekly chunks for comprehensive coverage"""
        self.logger.info("📅 Processing data in weekly chunks for comprehensive coverage")
        
        current_date = start_date
        chunk_count = 0
        
        while current_date <= end_date:
            chunk_end = min(current_date + timedelta(days=6), end_date)
            
            try:
                self.logger.info(f"🔄 Processing chunk {chunk_count + 1}: {current_date} to {chunk_end}")
                
                result = await self.backtesting_engine.run_backtest(
                    start_date=current_date.strftime('%Y-%m-%d'),
                    end_date=chunk_end.strftime('%Y-%m-%d'),
                    include_diagnostics=False,
                    include_alignment=False
                )
                
                if 'backtest_results' in result and 'strategy_results' in result['backtest_results']:
                    chunk_results = result['backtest_results']['strategy_results']
                    
                    self.total_results_added += len(chunk_results)
                    self.logger.info(f"✅ Chunk {chunk_count + 1}: Added {len(chunk_results)} results")
                    
                    # Track date range processed
                    self.date_ranges_processed.append({
                        'start': current_date,
                        'end': chunk_end,
                        'results_count': len(chunk_results)
                    })
                
                chunk_count += 1
                current_date = chunk_end + timedelta(days=1)
                
            except Exception as e:
                self.logger.error(f"❌ Failed to process chunk {current_date} to {chunk_end}: {e}")
                current_date = chunk_end + timedelta(days=1)
                continue
    
    async def _populate_full_range(self, start_date, end_date):
        """Run comprehensive backtest across full date range"""
        self.logger.info(f"🔄 Running full range backtest: {start_date} to {end_date}")
        
        try:
            result = await self.backtesting_engine.run_backtest(
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d'),
                include_diagnostics=False,
                include_alignment=False
            )
            
            if 'backtest_results' in result and 'strategy_results' in result['backtest_results']:
                full_range_results = result['backtest_results']['strategy_results']
                self.total_results_added += len(full_range_results)
                self.logger.info(f"✅ Full range: Added {len(full_range_results)} strategy results")
                
        except Exception as e:
            self.logger.error(f"❌ Full range backtest failed: {e}")
    
    async def _generate_population_report(self):
        """Generate comprehensive population report"""
        
        # Get final counts from database
        with self.db_manager.get_cursor() as cursor:
            cursor.execute("SELECT COUNT(*) as total_count FROM backtesting.strategy_performance")
            final_count = cursor.fetchone()['total_count']
            
            cursor.execute("""
                SELECT COUNT(DISTINCT strategy_name) as unique_strategies,
                       COUNT(DISTINCT source_book_type) as unique_sources,
                       COUNT(DISTINCT split_type) as unique_split_types,
                       MIN(backtest_date) as earliest_date,
                       MAX(backtest_date) as latest_date
                FROM backtesting.strategy_performance
            """)
            summary_stats = cursor.fetchone()
            
            cursor.execute("""
                SELECT strategy_name, COUNT(*) as count, 
                       AVG(roi_per_100) as avg_roi,
                       AVG(win_rate) as avg_win_rate
                FROM backtesting.strategy_performance 
                GROUP BY strategy_name 
                ORDER BY count DESC
                LIMIT 15
            """)
            top_strategies = cursor.fetchall()
        
        # Generate comprehensive report
        report = f"""
🎯 STRATEGY PERFORMANCE POPULATION REPORT
{'=' * 60}

📊 POPULATION RESULTS:
- Total records in strategy_performance: {final_count:,}
- Unique strategies: {summary_stats['unique_strategies']}
- Unique source/book combinations: {summary_stats['unique_sources']}
- Unique split types: {summary_stats['unique_split_types']}
- Date range covered: {summary_stats['earliest_date']} to {summary_stats['latest_date']}

🏭 PROCESSING SUMMARY:
- Total strategy results generated this run: {self.total_results_added}
- Date ranges processed: {len(self.date_ranges_processed)}

📈 TOP 15 STRATEGIES BY RECORD COUNT:
"""
        
        for i, strategy in enumerate(top_strategies, 1):
            report += f"{i:2d}. {strategy['strategy_name']}: {strategy['count']} records "
            report += f"(Avg ROI: {strategy['avg_roi']:.1f}%, Win Rate: {strategy['avg_win_rate']:.1%})\n"
        
        report += f"\n📅 DATE RANGES PROCESSED THIS RUN:\n"
        for date_range in self.date_ranges_processed:
            report += f"- {date_range['start']} to {date_range['end']}: {date_range['results_count']} results\n"
        
        report += f"\n{'=' * 60}"
        
        self.logger.info(report)
        
        # Save report to file
        report_file = Path("strategy_performance_population_report.txt")
        with open(report_file, 'w') as f:
            f.write(report)
        
        self.logger.info(f"📝 Full report saved to: {report_file}")


async def main():
    """Main execution function"""
    print("🚀 Starting Strategy Performance Population")
    print("=" * 60)
    
    populator = StrategyPerformancePopulator()
    
    try:
        await populator.initialize()
        await populator.run_comprehensive_population()
        
        print("\n✅ Population completed successfully!")
        print("📊 Check the database and report file for detailed results")
        
    except Exception as e:
        print(f"\n❌ Population failed: {e}")
        print(f"🔍 Traceback: {traceback.format_exc()}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
