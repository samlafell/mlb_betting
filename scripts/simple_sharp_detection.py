#!/usr/bin/env python3
"""
Simple Sharp Action Detection
Identifies clear sharp money indicators in betting splits
"""

import duckdb
import sys
import os

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import Config

config = Config()
DB_PATH = config.database_path

def analyze_sharp_action():
    """Analyze betting splits for sharp action indicators"""
    con = duckdb.connect(DB_PATH)
    
    # Get current splits with significant bet/stake discrepancies
    query = """
    SELECT 
        game_id,
        home_team,
        away_team,
        split_type,
        home_or_over_bets_percentage,
        home_or_over_stake_percentage,
        away_or_under_bets_percentage,
        away_or_under_stake_percentage,
        outcome,
        ABS(home_or_over_stake_percentage - home_or_over_bets_percentage) as home_discrepancy,
        ABS(away_or_under_stake_percentage - away_or_under_bets_percentage) as away_discrepancy
    FROM splits.raw_mlb_betting_splits 
    WHERE DATE(game_datetime) = CURRENT_DATE
    ORDER BY game_id, split_type
    """
    
    results = con.execute(query).fetchall()
    con.close()
    
    sharp_games = {}
    
    for row in results:
        (game_id, home_team, away_team, split_type, 
         home_bets_pct, home_stake_pct, away_bets_pct, away_stake_pct, 
         outcome, home_discrepancy, away_discrepancy) = row
        
        indicators = []
        
        # Indicator 1: Big handle %, low ticket % (15+ point discrepancy)
        if home_discrepancy > 15:
            if home_stake_pct > home_bets_pct:
                indicators.append(f"🔥 Sharp money on HOME/OVER: {home_bets_pct:.1f}% bets → {home_stake_pct:.1f}% money (+{home_discrepancy:.1f})")
        
        if away_discrepancy > 15:
            if away_stake_pct > away_bets_pct:
                indicators.append(f"🔥 Sharp money on AWAY/UNDER: {away_bets_pct:.1f}% bets → {away_stake_pct:.1f}% money (+{away_discrepancy:.1f})")
        
        # Indicator 2: Heavy sharp betting (60%+ money from 40%- bets)
        if home_stake_pct >= 60 and home_bets_pct <= 40:
            indicators.append(f"💰 Heavy sharp HOME/OVER: {home_bets_pct:.1f}% bets control {home_stake_pct:.1f}% money")
        
        if away_stake_pct >= 60 and away_bets_pct <= 40:
            indicators.append(f"💰 Heavy sharp AWAY/UNDER: {away_bets_pct:.1f}% bets control {away_stake_pct:.1f}% money")
        
        # Indicator 3: Public darling fade (75%+ tickets, <60% money)
        if home_bets_pct > 75 and home_stake_pct < 60:
            indicators.append(f"📉 Public darling fade HOME/OVER: {home_bets_pct:.1f}% tickets → only {home_stake_pct:.1f}% money")
        
        if away_bets_pct > 75 and away_stake_pct < 60:
            indicators.append(f"📉 Public darling fade AWAY/UNDER: {away_bets_pct:.1f}% tickets → only {away_stake_pct:.1f}% money")
        
        if indicators:
            if game_id not in sharp_games:
                sharp_games[game_id] = {
                    'home_team': home_team,
                    'away_team': away_team,
                    'outcome': outcome,
                    'splits': {}
                }
            
            sharp_games[game_id]['splits'][split_type] = indicators
    
    return sharp_games

def update_sharp_flags(sharp_games):
    """Update sharp_action flags in database"""
    con = duckdb.connect(DB_PATH)
    
    total_updates = 0
    
    for game_id, game_data in sharp_games.items():
        for split_type in game_data['splits'].keys():
            # Update sharp_action flag
            update_query = """
            UPDATE splits.raw_mlb_betting_splits 
            SET sharp_action = true
            WHERE game_id = ? AND split_type = ?
            """
            con.execute(update_query, [game_id, split_type])
            total_updates += 1
    
    con.close()
    return total_updates

def get_historical_sharp_performance():
    """Get historical sharp action performance statistics"""
    con = duckdb.connect(DB_PATH)
    
    query = """
    SELECT 
        COUNT(*) as total_sharp_plays,
        SUM(CASE WHEN sharp_success = true THEN 1 ELSE 0 END) as successful_plays,
        SUM(CASE WHEN sharp_success = false THEN 1 ELSE 0 END) as failed_plays,
        AVG(CASE WHEN sharp_success IS NOT NULL THEN CAST(sharp_success AS INTEGER) END) * 100 as success_rate
    FROM splits.raw_mlb_betting_splits 
    WHERE sharp_action = true 
    AND sharp_success IS NOT NULL
    """
    
    result = con.execute(query).fetchone()
    con.close()
    
    return result

def main():
    print("🔍 SHARP ACTION ANALYSIS")
    print("=" * 60)
    
    sharp_games = analyze_sharp_action()
    
    if sharp_games:
        print(f"🚨 SHARP ACTION DETECTED in {len(sharp_games)} games:\n")
        
        for game_id, game_data in sharp_games.items():
            outcome_str = f" ({game_data['outcome']})" if game_data['outcome'] else ""
            print(f"🎯 Game {game_id}: {game_data['away_team']} @ {game_data['home_team']}{outcome_str}")
            
            for split_type, indicators in game_data['splits'].items():
                print(f"   📊 {split_type.upper()} SPLIT:")
                for indicator in indicators:
                    print(f"      {indicator}")
            print()
        
        # Update database
        total_updates = update_sharp_flags(sharp_games)
        print(f"✅ Updated {total_updates} split entries with sharp action flags")
        
        # Show historical performance
        historical_stats = get_historical_sharp_performance()
        if historical_stats and historical_stats[0] > 0:
            total_plays, successful, failed, success_rate = historical_stats
            print(f"\n📈 HISTORICAL SHARP PERFORMANCE:")
            print(f"   Total Sharp Plays: {total_plays}")
            print(f"   Successful: {successful} | Failed: {failed}")
            print(f"   Success Rate: {success_rate:.1f}%")
            
            if success_rate > 52.4:
                print("   🔥 Sharp money is profitable!")
            elif success_rate > 50:
                print("   ⚖️  Sharp money slightly above random")
            else:
                print("   📉 Sharp money underperforming")
        
        print("\n" + "=" * 60)
        print("🧠 SHARP ACTION SUMMARY:")
        print("🔥 = Significant bet/stake discrepancy (sharp money)")
        print("💰 = Heavy sharp betting (few bets, big money)")
        print("📉 = Public darling fade (many tickets, little money)")
        
    else:
        print("ℹ️  No significant sharp action detected in today's games")
        print("   (Market appears efficient or needs more data points)")

if __name__ == "__main__":
    main() 