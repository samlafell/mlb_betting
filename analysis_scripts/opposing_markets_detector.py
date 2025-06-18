#!/usr/bin/env python3
"""
OPPOSING MARKETS DETECTOR
========================

Detects games where moneyline and spread betting recommendations oppose each other.
This often indicates sharp vs public money splits and potential value betting opportunities.

Usage: uv run analysis_scripts/opposing_markets_detector.py --minutes 300
"""

import argparse
import duckdb
from datetime import datetime, timedelta
from decimal import Decimal
import pytz

class OpposingMarketsDetector:
    def __init__(self, db_path='data/raw/mlb_betting.duckdb'):
        self.conn = duckdb.connect(db_path)
        self.est = pytz.timezone('US/Eastern')
        
    def find_opposing_markets(self, minutes_ahead=60):
        """Find games where ML and spread recommendations oppose each other"""
        
        # Get current time in EST
        now_est = datetime.now(self.est)
        cutoff_time = now_est + timedelta(minutes=minutes_ahead)
        
        print(f"🔍 Looking for opposing market signals in games starting within {minutes_ahead} minutes...")
        print(f"📅 Current time: {now_est.strftime('%H:%M:%S %Z')}")
        print(f"🎯 Looking until: {cutoff_time.strftime('%H:%M:%S %Z')}")
        
        # Query for latest splits
        query = """
        WITH latest_splits AS (
            SELECT 
                home_team,
                away_team,
                game_datetime,
                split_type,
                split_value,
                home_or_over_stake_percentage as stake_pct,
                home_or_over_bets_percentage as bet_pct,
                (home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
                source,
                COALESCE(book, 'UNKNOWN') as book,
                last_updated,
                ROW_NUMBER() OVER (
                    PARTITION BY home_team, away_team, game_datetime, split_type, source, COALESCE(book, 'UNKNOWN')
                    ORDER BY last_updated DESC
                ) as rn
            FROM splits.raw_mlb_betting_splits
            WHERE home_or_over_stake_percentage IS NOT NULL 
              AND home_or_over_bets_percentage IS NOT NULL
              AND game_datetime IS NOT NULL
              AND split_type IN ('moneyline', 'spread')
        ),
        
        clean_splits AS (
            SELECT *
            FROM latest_splits
            WHERE rn = 1
        ),
        
        ml_signals AS (
            SELECT 
                home_team, away_team, game_datetime, source, book,
                differential as ml_diff,
                stake_pct as ml_stake_pct,
                bet_pct as ml_bet_pct,
                split_value as ml_split_value,
                CASE WHEN differential > 0 THEN home_team ELSE away_team END as ml_rec_team,
                ABS(differential) as ml_strength,
                last_updated as ml_updated
            FROM clean_splits
            WHERE split_type = 'moneyline'
        ),
        
        spread_signals AS (
            SELECT 
                home_team, away_team, game_datetime, source, book,
                differential as spread_diff,
                stake_pct as spread_stake_pct,
                bet_pct as spread_bet_pct,
                split_value as spread_split_value,
                CASE WHEN differential > 0 THEN home_team ELSE away_team END as spread_rec_team,
                ABS(differential) as spread_strength,
                last_updated as spread_updated
            FROM clean_splits
            WHERE split_type = 'spread'
        )
        
        SELECT 
            ml.home_team,
            ml.away_team,
            ml.game_datetime,
            ml.source,
            ml.book,
            
            -- ML data
            ml.ml_rec_team,
            ml.ml_diff,
            ml.ml_strength,
            ml.ml_stake_pct,
            ml.ml_bet_pct,
            ml.ml_split_value,
            ml.ml_updated,
            
            -- Spread data
            sp.spread_rec_team,
            sp.spread_diff,
            sp.spread_strength,
            sp.spread_stake_pct,
            sp.spread_bet_pct,
            sp.spread_split_value,
            sp.spread_updated,
            
            -- Analysis
            (ml.ml_strength + sp.spread_strength) / 2 as combined_strength,
            ABS(ml.ml_diff - sp.spread_diff) as opposition_strength,
            CASE 
                WHEN ml.ml_strength > sp.spread_strength THEN 'ML_STRONGER'
                WHEN sp.spread_strength > ml.ml_strength THEN 'SPREAD_STRONGER'
                ELSE 'EQUAL'
            END as dominant_market
            
        FROM ml_signals ml
        INNER JOIN spread_signals sp
            ON ml.home_team = sp.home_team
            AND ml.away_team = sp.away_team
            AND ml.game_datetime = sp.game_datetime
            AND ml.source = sp.source
            AND ml.book = sp.book
        WHERE ml.ml_rec_team != sp.spread_rec_team  -- Only opposing recommendations
        ORDER BY ml.game_datetime ASC, combined_strength DESC
        """
        
        results = self.conn.execute(query).fetchall()
        
        # Filter by time window and apply thresholds
        opposing_opportunities = []
        
        for row in results:
            (home, away, game_time, source, book, 
             ml_rec_team, ml_diff, ml_strength, ml_stake_pct, ml_bet_pct, ml_split_value, ml_updated,
             spread_rec_team, spread_diff, spread_strength, spread_stake_pct, spread_bet_pct, spread_split_value, spread_updated,
             combined_strength, opposition_strength, dominant_market) = row
            
            # Convert game_time to EST if needed
            if game_time.tzinfo is None:
                game_time_est = self.est.localize(game_time)
            else:
                game_time_est = game_time.astimezone(self.est)
            
            # Calculate time difference
            time_diff_minutes = (game_time_est - now_est).total_seconds() / 60
            
            # Only include upcoming games within time window
            if 0 <= time_diff_minutes <= minutes_ahead:
                
                # Apply signal strength thresholds
                is_valid_signal = False
                confidence_level = "LOW"
                
                # Require both signals to be meaningful
                min_signal_strength = 10.0  # Both ML and spread need at least 10% differential
                combined_min_strength = 20.0  # Combined strength needs to be meaningful
                
                if (ml_strength >= min_signal_strength and 
                    spread_strength >= min_signal_strength and 
                    combined_strength >= combined_min_strength):
                    
                    if source == 'VSIN':
                        if combined_strength >= 35:
                            is_valid_signal = True
                            confidence_level = "HIGH CONFIDENCE"
                        elif combined_strength >= 25:
                            is_valid_signal = True
                            confidence_level = "MODERATE CONFIDENCE"
                    elif source == 'SBD':
                        if combined_strength >= 40:
                            is_valid_signal = True
                            confidence_level = "MODERATE CONFIDENCE"
                
                if is_valid_signal:
                    # Determine recommendations based on different strategies
                    strategies = {
                        "FOLLOW_STRONGER": ml_rec_team if dominant_market == 'ML_STRONGER' else spread_rec_team,
                        "ML_PREFERENCE": ml_rec_team,
                        "SPREAD_PREFERENCE": spread_rec_team,
                        "CONTRARIAN": spread_rec_team if dominant_market == 'ML_STRONGER' else ml_rec_team
                    }
                    
                    opposing_opportunities.append({
                        'home_team': home,
                        'away_team': away,
                        'game_time': game_time_est,
                        'minutes_to_game': int(time_diff_minutes),
                        'source': source,
                        'book': book,
                        
                        # ML data
                        'ml_recommended_team': ml_rec_team,
                        'ml_differential': float(ml_diff),
                        'ml_strength': float(ml_strength),
                        'ml_stake_pct': float(ml_stake_pct),
                        'ml_bet_pct': float(ml_bet_pct),
                        'ml_split_value': ml_split_value,
                        'ml_updated': ml_updated,
                        
                        # Spread data  
                        'spread_recommended_team': spread_rec_team,
                        'spread_differential': float(spread_diff),
                        'spread_strength': float(spread_strength),
                        'spread_stake_pct': float(spread_stake_pct),
                        'spread_bet_pct': float(spread_bet_pct),
                        'spread_split_value': spread_split_value,
                        'spread_updated': spread_updated,
                        
                        # Analysis
                        'combined_strength': float(combined_strength),
                        'opposition_strength': float(opposition_strength),
                        'dominant_market': dominant_market,
                        'confidence_level': confidence_level,
                        'strategies': strategies
                    })
        
        return opposing_opportunities
    
    def display_opposing_opportunities(self, opportunities):
        """Display opposing market opportunities in a clear format"""
        
        if not opportunities:
            print(f"\n🚫 No opposing market signals found.")
            print(f"💡 This happens when ML and spread recommendations align, or when signals are too weak.")
            return
        
        print(f"\n⚡ {len(opportunities)} OPPOSING MARKET OPPORTUNITIES FOUND!")
        print("=" * 70)
        print("📊 When ML and Spread disagree, there's often edge to be found!")
        print()
        
        for i, opp in enumerate(sorted(opportunities, key=lambda x: x['game_time']), 1):
            print(f"🏟️  #{i}: {opp['away_team']} @ {opp['home_team']}")
            print(f"⏰ Starts in {opp['minutes_to_game']} minutes ({opp['game_time'].strftime('%H:%M')})")
            print(f"📍 Source: {opp['source']}-{opp['book']} | Confidence: {opp['confidence_level']}")
            print("-" * 70)
            
            # Show the disagreement
            print(f"🔄 MARKET DISAGREEMENT:")
            print(f"   💰 MONEYLINE recommends: {opp['ml_recommended_team']}")
            print(f"       📊 {opp['ml_stake_pct']:.1f}% money vs {opp['ml_bet_pct']:.1f}% bets ({opp['ml_differential']:+.1f}% diff)")
            print(f"       📈 Signal strength: {opp['ml_strength']:.1f}%")
            print()
            print(f"   🏈 SPREAD recommends: {opp['spread_recommended_team']}")
            print(f"       📊 {opp['spread_stake_pct']:.1f}% money vs {opp['spread_bet_pct']:.1f}% bets ({opp['spread_differential']:+.1f}% diff)")
            print(f"       📈 Signal strength: {opp['spread_strength']:.1f}%")
            print()
            
            # Strategy recommendations
            print(f"🎯 STRATEGY RECOMMENDATIONS:")
            for strategy, team in opp['strategies'].items():
                emoji = "🔥" if strategy == "FOLLOW_STRONGER" else "🤔"
                strategy_display = strategy.replace('_', ' ').title()
                print(f"   {emoji} {strategy_display}: BET {team}")
            
            print(f"📈 Combined Signal Strength: {opp['combined_strength']:.1f}%")
            print(f"⚡ Opposition Strength: {opp['opposition_strength']:.1f}%")
            print(f"🏆 Dominant Market: {opp['dominant_market'].replace('_', ' ')}")
            print()
        
        # Summary and insights
        print("=" * 70)
        print("🧠 OPPOSING MARKETS INSIGHTS:")
        print("• When ML and Spread disagree, it often indicates sharp vs public money")
        print("• The 'Follow Stronger' strategy typically has the best historical performance")
        print("• High opposition strength (>30%) signals stronger potential edge")
        print("• Consider market context: ML often reflects sharp money, spread reflects public")
        print("• These signals are time-sensitive - place bets quickly!")
        print()
        
        # Strategy performance note
        high_conf = len([o for o in opportunities if "HIGH" in o['confidence_level']])
        mod_conf = len([o for o in opportunities if "MODERATE" in o['confidence_level']])
        
        print(f"📊 CONFIDENCE DISTRIBUTION:")
        print(f"   🔥 High Confidence: {high_conf}")
        print(f"   🟡 Moderate Confidence: {mod_conf}")
        print(f"   💰 Total Opportunities: {len(opportunities)}")

def main():
    parser = argparse.ArgumentParser(description='Opposing Markets Detector')
    parser.add_argument('--minutes', type=int, default=60,
                       help='Look for games starting within N minutes (default: 60)')
    args = parser.parse_args()
    
    print("⚡ OPPOSING MARKETS DETECTOR")
    print("=" * 50)
    print("Finds games where Moneyline and Spread recommendations disagree")
    print("This often indicates sharp vs public money splits and betting edge")
    
    detector = OpposingMarketsDetector()
    opportunities = detector.find_opposing_markets(args.minutes)
    detector.display_opposing_opportunities(opportunities)

if __name__ == "__main__":
    main() 