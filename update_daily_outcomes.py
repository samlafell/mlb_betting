#!/usr/bin/env python3
"""
Daily Outcome Updater for Pre-Game Recommendations

This script should be run daily (ideally around midnight EST) to:
1. Update outcomes for completed games
2. Calculate profit/loss for recommendations 
3. Update the tracking.pre_game_recommendations table
4. Generate learning data for the adaptive system

Usage:
    uv run python update_daily_outcomes.py [--days-back N] [--dry-run]
"""

import asyncio
import argparse
from datetime import datetime, timezone
from src.mlb_sharp_betting.services.pre_game_recommendation_tracker import PreGameRecommendationTracker
from src.mlb_sharp_betting.core.logging import get_logger

logger = get_logger(__name__)

async def update_daily_outcomes(days_back: int = 7, dry_run: bool = False):
    """
    Update recommendation outcomes for completed games.
    
    Args:
        days_back: Number of days back to check for completed games
        dry_run: If True, show what would be updated without making changes
    """
    
    try:
        tracker = PreGameRecommendationTracker()
        
        print(f"🔄 Starting daily outcome update (looking back {days_back} days)")
        
        if dry_run:
            print("🧪 DRY RUN MODE - No changes will be made")
            
        # Update recommendation outcomes
        await tracker.update_recommendation_outcomes(lookback_days=days_back)
        
        # Generate performance report to show current status
        report = await tracker.generate_performance_report(days_back=30)
        
        print("📊 PERFORMANCE SUMMARY (Last 30 Days):")
        print(f"  Total Recommendations: {report.total_recommendations}")
        print(f"  Completed Games: {report.completed_games}")
        print(f"  Wins: {report.wins}")
        print(f"  Losses: {report.losses}")
        print(f"  Pending: {report.pending_games}")
        print(f"  Win Rate: {report.win_rate:.1%}")
        print(f"  Total P&L: ${report.total_profit_loss:.2f}")
        print(f"  ROI per 100 units: {report.roi_per_100_units:.1f}%")
        
        # Breakdown by signal source
        if report.by_signal_source:
            print("\n📈 PERFORMANCE BY SIGNAL SOURCE:")
            for source, data in report.by_signal_source.items():
                wins = data.get('wins', 0)
                total = data.get('total', 0)
                win_rate = wins / total if total > 0 else 0
                pnl = data.get('total_pnl', 0)
                print(f"  {source}: {wins}/{total} ({win_rate:.1%}) - ${pnl:.2f}")
        
        # Breakdown by bet type  
        if report.by_bet_type:
            print("\n🎯 PERFORMANCE BY BET TYPE:")
            for bet_type, data in report.by_bet_type.items():
                wins = data.get('wins', 0)
                total = data.get('total', 0)
                win_rate = wins / total if total > 0 else 0
                pnl = data.get('total_pnl', 0)
                print(f"  {bet_type}: {wins}/{total} ({win_rate:.1%}) - ${pnl:.2f}")
        
        print(f"\n✅ Daily outcome update completed at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
        
    except Exception as e:
        logger.error(f"Failed to update daily outcomes: {e}")
        print(f"❌ ERROR: {e}")
        raise

async def main():
    """Main entry point for daily outcome updater."""
    
    parser = argparse.ArgumentParser(description="Update daily recommendation outcomes")
    parser.add_argument("--days-back", type=int, default=7, 
                       help="Number of days back to check for completed games (default: 7)")
    parser.add_argument("--dry-run", action="store_true",
                       help="Show what would be updated without making changes")
    
    args = parser.parse_args()
    
    await update_daily_outcomes(days_back=args.days_back, dry_run=args.dry_run)

if __name__ == "__main__":
    asyncio.run(main()) 