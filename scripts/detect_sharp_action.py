#!/usr/bin/env python3
"""
Detect sharp action in betting splits based on multiple indicators:
- Line moves counter to public money
- Big handle %, low ticket %
- Public/Sharp split discrepancy
- Line movement analysis
"""

import duckdb
import sys
import os
from datetime import datetime, timedelta

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import Config

config = Config()
DB_PATH = config.database_path

def get_betting_splits_with_history():
    """Get betting splits with previous entries for comparison"""
    con = duckdb.connect(DB_PATH)
    
    query = """
    WITH ranked_splits AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY game_id, split_type 
                   ORDER BY last_updated DESC
               ) as rn
        FROM splits.raw_mlb_betting_splits 
        WHERE DATE(game_datetime) = CURRENT_DATE
    ),
    current_splits AS (
        SELECT * FROM ranked_splits WHERE rn = 1
    ),
    previous_splits AS (
        SELECT * FROM ranked_splits WHERE rn = 2
    )
    SELECT 
        c.*,
        p.home_or_over_bets_percentage as prev_home_or_over_bets_pct,
        p.home_or_over_stake_percentage as prev_home_or_over_stake_pct,
        p.away_or_under_bets_percentage as prev_away_or_under_bets_pct,
        p.away_or_under_stake_percentage as prev_away_or_under_stake_pct,
        p.split_value as prev_split_value,
        p.last_updated as prev_last_updated
    FROM current_splits c
    LEFT JOIN previous_splits p ON c.game_id = p.game_id AND c.split_type = p.split_type
    ORDER BY c.game_id, c.split_type
    """
    
    result = con.execute(query).fetchall()
    con.close()
    
    return result

def detect_sharp_action_indicators(split_data):
    """Detect sharp action based on multiple indicators"""
    indicators = []
    
    # Extract current data based on the actual structure (25 fields)
    if len(split_data) < 25:
        return indicators, False
    
    # Extract the key fields we need (corrected indices)
    game_id = split_data[1]  # Actual game_id is at index 1
    home_team = split_data[2] 
    away_team = split_data[3]
    split_type = split_data[5]
    home_or_over_bets_pct = split_data[10]
    home_or_over_stake_pct = split_data[11]
    away_or_under_bets_pct = split_data[13]
    away_or_under_stake_pct = split_data[14]
    split_value = split_data[15]
    
    # Previous data (if available)
    prev_home_or_over_bets_pct = split_data[19] if len(split_data) > 19 else None
    prev_home_or_over_stake_pct = split_data[20] if len(split_data) > 20 else None
    prev_away_or_under_bets_pct = split_data[21] if len(split_data) > 21 else None
    prev_away_or_under_stake_pct = split_data[22] if len(split_data) > 22 else None
    prev_split_value = split_data[23] if len(split_data) > 23 else None
    prev_last_updated = split_data[24] if len(split_data) > 24 else None
    
    # Skip if we don't have previous data for comparison
    if prev_last_updated is None:
        return indicators, False
    
    # Indicator 1: Big handle %, low ticket % (Sharp side detection)
    # Look for significant discrepancy between bet percentage and stake percentage
    home_discrepancy = abs((home_or_over_stake_pct or 0) - (home_or_over_bets_pct or 0))
    away_discrepancy = abs((away_or_under_stake_pct or 0) - (away_or_under_bets_pct or 0))
    
    if home_discrepancy > 15:  # 15+ point discrepancy suggests sharp action
        if (home_or_over_stake_pct or 0) > (home_or_over_bets_pct or 0):
            indicators.append(f"Sharp money on HOME/OVER: {home_or_over_bets_pct:.1f}% bets, {home_or_over_stake_pct:.1f}% money")
    
    if away_discrepancy > 15:
        if (away_or_under_stake_pct or 0) > (away_or_under_bets_pct or 0):
            indicators.append(f"Sharp money on AWAY/UNDER: {away_or_under_bets_pct:.1f}% bets, {away_or_under_stake_pct:.1f}% money")
    
    # Indicator 2: Total money > total bets (on one side)
    # 70% of money from just 40% of bets = sharp action
    if (home_or_over_stake_pct or 0) >= 60 and (home_or_over_bets_pct or 0) <= 40:
        indicators.append(f"Heavy sharp betting HOME/OVER: {home_or_over_bets_pct:.1f}% bets control {home_or_over_stake_pct:.1f}% money")
    
    if (away_or_under_stake_pct or 0) >= 60 and (away_or_under_bets_pct or 0) <= 40:
        indicators.append(f"Heavy sharp betting AWAY/UNDER: {away_or_under_bets_pct:.1f}% bets control {away_or_under_stake_pct:.1f}% money")
    
    # Indicator 3: Line movement analysis (if we have previous split_value)
    if prev_split_value and split_value and prev_split_value != split_value:
        try:
            if split_type == 'spread':
                # Parse spread values like "-1.5/-1.5" or "+1.5/-1.5"
                current_parts = split_value.split('/')
                prev_parts = prev_split_value.split('/')
                
                if len(current_parts) == 2 and len(prev_parts) == 2:
                    # Look at home team spread (second value)
                    current_home_spread = float(current_parts[1])
                    prev_home_spread = float(prev_parts[1])
                    
                    spread_movement = current_home_spread - prev_home_spread
                    
                    # Check if line moved counter to public money
                    if spread_movement > 0 and (home_or_over_bets_pct or 0) > 60:
                        indicators.append(f"Line moved AGAINST public: spread +{spread_movement:.1f} despite {home_or_over_bets_pct:.1f}% on home")
                    elif spread_movement < 0 and (away_or_under_bets_pct or 0) > 60:
                        indicators.append(f"Line moved AGAINST public: spread {spread_movement:.1f} despite {away_or_under_bets_pct:.1f}% on away")
                        
            elif split_type == 'total':
                # Parse total values
                current_total = float(split_value)
                prev_total = float(prev_split_value)
                
                total_movement = current_total - prev_total
                
                if abs(total_movement) >= 0.5:  # Significant total movement
                    if total_movement < 0 and (home_or_over_bets_pct or 0) > 60:  # Total dropped despite over action
                        indicators.append(f"Total moved AGAINST public: {total_movement:.1f} despite {home_or_over_bets_pct:.1f}% on over")
                    elif total_movement > 0 and (away_or_under_bets_pct or 0) > 60:  # Total rose despite under action
                        indicators.append(f"Total moved AGAINST public: +{total_movement:.1f} despite {away_or_under_bets_pct:.1f}% on under")
                        
        except (ValueError, IndexError):
            # Skip if we can't parse the values
            pass
    
    # Indicator 4: Extreme public/sharp split
    # High % of public on one side but money is more balanced
    if (home_or_over_bets_pct or 0) > 75 and (home_or_over_stake_pct or 0) < 60:
        indicators.append(f"Public darling fade: {home_or_over_bets_pct:.1f}% tickets but only {home_or_over_stake_pct:.1f}% money")
    
    if (away_or_under_bets_pct or 0) > 75 and (away_or_under_stake_pct or 0) < 60:
        indicators.append(f"Public darling fade: {away_or_under_bets_pct:.1f}% tickets but only {away_or_under_stake_pct:.1f}% money")
    
    # Determine if this qualifies as sharp action
    sharp_detected = len(indicators) >= 1  # At least one strong indicator
    
    return indicators, sharp_detected

def update_sharp_action_flags():
    """Update sharp_action flags in the database"""
    splits_data = get_betting_splits_with_history()
    
    updates = []
    sharp_games = {}
    
    for split_data in splits_data:
        game_id = split_data[0]
        split_type = split_data[4]
        
        indicators, sharp_detected = detect_sharp_action_indicators(split_data)
        
        if sharp_detected:
            updates.append((game_id, split_type, True))
            
            if game_id not in sharp_games:
                sharp_games[game_id] = []
            sharp_games[game_id].append({
                'split_type': split_type,
                'indicators': indicators
            })
    
    # Update database
    if updates:
        con = duckdb.connect(DB_PATH)
        
        for game_id, split_type, sharp_flag in updates:
            update_query = """
            UPDATE splits.raw_mlb_betting_splits 
            SET sharp_action = ?
            WHERE game_id = ? AND split_type = ?
            """
            con.execute(update_query, [sharp_flag, game_id, split_type])
        
        con.close()
    
    return sharp_games, len(updates)

def main():
    print("🔍 Analyzing betting splits for sharp action indicators...")
    print("=" * 70)
    
    sharp_games, total_updates = update_sharp_action_flags()
    
    if sharp_games:
        print(f"🚨 SHARP ACTION DETECTED in {len(sharp_games)} games:")
        print()
        
        for game_id, splits in sharp_games.items():
            # Get game info
            con = duckdb.connect(DB_PATH)
            game_info = con.execute("""
                SELECT DISTINCT home_team, away_team, outcome 
                FROM splits.raw_mlb_betting_splits 
                WHERE game_id = ?
            """, [game_id]).fetchone()
            con.close()
            
            if game_info:
                home_team, away_team, outcome = game_info
                outcome_str = f" ({outcome})" if outcome else ""
                print(f"🎯 Game {game_id}: {away_team} @ {home_team}{outcome_str}")
                
                for split in splits:
                    print(f"   📊 {split['split_type'].upper()} SPLIT:")
                    for indicator in split['indicators']:
                        print(f"      • {indicator}")
                print()
        
        print(f"✅ Updated {total_updates} split entries with sharp action flags")
    else:
        print("ℹ️  No significant sharp action detected in today's games")
        print("   (This could mean the market is efficient or we need more data points)")

if __name__ == "__main__":
    main() 