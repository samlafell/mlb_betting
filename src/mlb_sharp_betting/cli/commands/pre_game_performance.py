#!/usr/bin/env python3
"""
Pre-Game Performance Report Command

This command generates performance reports for betting recommendations that were
sent via pre-game email alerts. This tracks actual recommendations made to the user,
not general strategy backtesting.

Usage:
    uv run -m mlb_sharp_betting.cli.commands.pre_game_performance --days 7
    uv run -m mlb_sharp_betting.cli.commands.pre_game_performance --days 30
"""

import argparse
import asyncio
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from mlb_sharp_betting.services.pre_game_recommendation_tracker import PreGameRecommendationTracker
from mlb_sharp_betting.core.logging import get_logger

logger = get_logger(__name__)


async def generate_performance_report(days_back: int = 7, update_outcomes: bool = True):
    """Generate and display pre-game recommendation performance report."""
    
    print(f"\n🏈 Generating Pre-Game Performance Report ({days_back} days)")
    print("=" * 60)
    
    try:
        tracker = PreGameRecommendationTracker()
        
        if update_outcomes:
            print("📊 Updating game outcomes...")
            await tracker.update_recommendation_outcomes(lookback_days=days_back)
            print("✅ Game outcomes updated")
        
        print("📈 Generating performance report...")
        report_text = await tracker.generate_daily_report_text(days_back=days_back)
        
        print("\n" + report_text)
        
        print(f"\n{'='*60}")
        print("✅ Performance report generated successfully")
        
    except Exception as e:
        logger.error("Failed to generate performance report", error=str(e))
        print(f"\n❌ Error generating report: {str(e)}")
        return False
    
    return True


def main():
    """Main entry point for pre-game performance command."""
    parser = argparse.ArgumentParser(
        description="Generate performance report for pre-game betting recommendations"
    )
    parser.add_argument(
        '--days', '-d',
        type=int,
        default=7,
        help='Number of days to look back (default: 7)'
    )
    parser.add_argument(
        '--no-update',
        action='store_true',
        help='Skip updating game outcomes (faster but may be outdated)'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.days < 1 or args.days > 365:
        print("❌ Error: Days must be between 1 and 365")
        sys.exit(1)
    
    # Run the performance report
    success = asyncio.run(generate_performance_report(
        days_back=args.days,
        update_outcomes=not args.no_update
    ))
    
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main() 