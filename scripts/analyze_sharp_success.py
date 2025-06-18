#!/usr/bin/env python3
"""
Analyze Sharp Action Success Rates
Determines whether sharp money was correct for finished games
Updates database with sharp_success indicators
"""

import duckdb
import sys
import os

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import Config

config = Config()
DB_PATH = config.database_path

def analyze_sharp_success():
    """Analyze sharp action success for finished games"""
    con = duckdb.connect(DB_PATH)
    
    # Get all sharp action entries for finished games
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
        split_value,
        outcome,
        sharp_action
    FROM splits.raw_mlb_betting_splits 
    WHERE sharp_action = true 
    AND outcome IS NOT NULL 
    AND outcome NOT IN ('In Progress', 'N/A', '')
    ORDER BY game_id, split_type
    """
    
    results = con.execute(query).fetchall()
    con.close()
    
    sharp_analysis = {}
    
    for row in results:
        (game_id, home_team, away_team, split_type, 
         home_bets_pct, home_stake_pct, away_bets_pct, away_stake_pct, 
         split_value, outcome, sharp_action) = row
        
        # Determine where the sharp money was
        sharp_side = None
        sharp_confidence = 0
        
        # Calculate discrepancies to identify sharp side
        home_discrepancy = abs(home_stake_pct - home_bets_pct) if home_stake_pct and home_bets_pct else 0
        away_discrepancy = abs(away_stake_pct - away_bets_pct) if away_stake_pct and away_bets_pct else 0
        
        # Determine sharp side based on stake vs bet percentage
        if home_discrepancy > 15 and home_stake_pct > home_bets_pct:
            sharp_side = 'home_or_over'
            sharp_confidence = home_discrepancy
        elif away_discrepancy > 15 and away_stake_pct > away_bets_pct:
            sharp_side = 'away_or_under'
            sharp_confidence = away_discrepancy
        elif home_stake_pct >= 60 and home_bets_pct <= 40:
            sharp_side = 'home_or_over'
            sharp_confidence = home_stake_pct - home_bets_pct
        elif away_stake_pct >= 60 and away_bets_pct <= 40:
            sharp_side = 'away_or_under'
            sharp_confidence = away_stake_pct - away_bets_pct
        elif home_bets_pct > 75 and home_stake_pct < 60:
            sharp_side = 'away_or_under'  # Fade the public darling
            sharp_confidence = home_bets_pct - home_stake_pct
        elif away_bets_pct > 75 and away_stake_pct < 60:
            sharp_side = 'home_or_over'  # Fade the public darling
            sharp_confidence = away_bets_pct - away_stake_pct
        
        if sharp_side:
            # Determine if sharp money was correct based on outcome
            sharp_success = determine_sharp_success(split_type, sharp_side, outcome, home_team, away_team, split_value)
            
            key = f"{game_id}_{split_type}"
            sharp_analysis[key] = {
                'game_id': game_id,
                'home_team': home_team,
                'away_team': away_team,
                'split_type': split_type,
                'split_value': split_value,
                'sharp_side': sharp_side,
                'sharp_confidence': sharp_confidence,
                'outcome': outcome,
                'sharp_success': sharp_success
            }
    
    return sharp_analysis

def determine_sharp_success(split_type, sharp_side, outcome, home_team, away_team, split_value):
    """Determine if sharp money was correct based on game outcome"""
    
    # Parse outcome to determine winner and score
    if 'Home Win' in outcome:
        actual_winner = 'home'
        # Extract score if available
        score_part = outcome.replace('Home Win ', '')
        home_score, away_score = parse_score(score_part)
    elif 'Away Win' in outcome:
        actual_winner = 'away'
        score_part = outcome.replace('Away Win ', '')
        away_score, home_score = parse_score(score_part)  # Note: reversed for away wins
    else:
        return None  # Can't determine for ties or other outcomes
    
    if split_type.lower() == 'moneyline':
        # For moneyline, check if sharp side matches winner
        if sharp_side == 'home_or_over' and actual_winner == 'home':
            return True
        elif sharp_side == 'away_or_under' and actual_winner == 'away':
            return True
        else:
            return False
    
    elif split_type.lower() == 'spread':
        # For spread betting, use actual spread values from split_value
        if home_score is not None and away_score is not None and split_value and split_value != 'N/A/N/A':
            try:
                # Parse spread value like "+1.5/-1.5" or "-1.5/+1.5"
                if '/' in split_value:
                    spread_parts = split_value.split('/')
                    spread1 = float(spread_parts[0])
                    spread2 = float(spread_parts[1])
                    
                    # Determine which is away and which is home spread
                    # Away team gets positive spread (underdog), home team gets negative (favorite)
                    if spread1 > 0:  # First value is positive (away team)
                        away_spread = spread1
                        home_spread = spread2
                    else:  # First value is negative (home team)
                        home_spread = spread1
                        away_spread = spread2
                    
                    # Calculate actual margin (home_score - away_score)
                    margin = home_score - away_score
                    
                    # Check if sharp side covered the spread
                    if sharp_side == 'home_or_over':
                        # Sharp money on home team spread
                        # Home covers if they win by more than the spread (e.g., win by 2+ when spread is -1.5)
                        return margin > abs(home_spread)
                    elif sharp_side == 'away_or_under':
                        # Sharp money on away team spread  
                        # Away covers if they lose by less than spread or win outright
                        # (e.g., lose by 1 or win when getting +1.5)
                        return margin < abs(away_spread)
                        
            except (ValueError, IndexError):
                # Fall back to typical 1.5 runline if parsing fails
                margin = abs(home_score - away_score)
                if sharp_side == 'home_or_over':
                    return actual_winner == 'home' and margin >= 2
                elif sharp_side == 'away_or_under':
                    return actual_winner == 'away' or (actual_winner == 'home' and margin == 1)
        
        return None
    
    elif split_type.lower() == 'total':
        # For totals, use actual total values from split_value
        if home_score is not None and away_score is not None:
            total_score = home_score + away_score
            
            # Try to use actual total from split_value
            if split_value and split_value != 'N/A':
                try:
                    actual_total = float(split_value)
                    
                    if sharp_side == 'home_or_over':
                        # Sharp money on OVER
                        return total_score > actual_total
                    elif sharp_side == 'away_or_under':
                        # Sharp money on UNDER  
                        return total_score < actual_total
                        
                except ValueError:
                    pass  # Fall through to estimated logic
            
            # Fall back to estimated total if no actual value
            estimated_total = 8.5
            if total_score >= 12:
                estimated_total = 9.5
            elif total_score <= 5:
                estimated_total = 7.5
            
            if sharp_side == 'home_or_over':
                return total_score > estimated_total
            elif sharp_side == 'away_or_under':
                return total_score < estimated_total
        
        return None
    
    return None

def parse_score(score_str):
    """Parse score string like '8-2' into individual scores"""
    try:
        if '-' in score_str:
            parts = score_str.split('-')
            if len(parts) == 2:
                return int(parts[0]), int(parts[1])
    except (ValueError, IndexError):
        pass
    return None, None

def update_sharp_success_flags(sharp_analysis):
    """Update database with sharp success indicators"""
    con = duckdb.connect(DB_PATH)
    
    # First, add the sharp_success column if it doesn't exist
    try:
        con.execute("ALTER TABLE splits.raw_mlb_betting_splits ADD COLUMN sharp_success BOOLEAN DEFAULT NULL")
    except:
        pass  # Column might already exist
    
    total_updates = 0
    successful_sharps = 0
    
    for key, analysis in sharp_analysis.items():
        if analysis['sharp_success'] is not None:
            # Update sharp_success flag
            update_query = """
            UPDATE splits.raw_mlb_betting_splits 
            SET sharp_success = ?
            WHERE game_id = ? AND split_type = ?
            """
            con.execute(update_query, [analysis['sharp_success'], analysis['game_id'], analysis['split_type']])
            total_updates += 1
            
            if analysis['sharp_success']:
                successful_sharps += 1
    
    con.close()
    return total_updates, successful_sharps

def main():
    print("🎯 SHARP ACTION SUCCESS ANALYSIS")
    print("=" * 60)
    
    sharp_analysis = analyze_sharp_success()
    
    if sharp_analysis:
        print(f"📊 Analyzing {len(sharp_analysis)} sharp action plays from finished games:\n")
        
        successful_plays = []
        failed_plays = []
        
        for key, analysis in sharp_analysis.items():
            game_str = f"{analysis['away_team']} @ {analysis['home_team']}"
            split_str = analysis['split_type'].upper()
            sharp_side_str = "HOME/OVER" if analysis['sharp_side'] == 'home_or_over' else "AWAY/UNDER"
            confidence_str = f"({analysis['sharp_confidence']:.1f} pts)"
            
            if analysis['sharp_success'] is True:
                successful_plays.append(f"✅ {game_str} - {split_str}: Sharp {sharp_side_str} {confidence_str} - {analysis['outcome']}")
            elif analysis['sharp_success'] is False:
                failed_plays.append(f"❌ {game_str} - {split_str}: Sharp {sharp_side_str} {confidence_str} - {analysis['outcome']}")
        
        if successful_plays:
            print("🎯 SUCCESSFUL SHARP PLAYS:")
            for play in successful_plays:
                print(f"   {play}")
            print()
        
        if failed_plays:
            print("💸 FAILED SHARP PLAYS:")
            for play in failed_plays:
                print(f"   {play}")
            print()
        
        # Update database
        total_updates, successful_sharps = update_sharp_success_flags(sharp_analysis)
        
        # Calculate success rate
        total_analyzed = len([a for a in sharp_analysis.values() if a['sharp_success'] is not None])
        success_rate = (successful_sharps / total_analyzed * 100) if total_analyzed > 0 else 0
        
        print(f"📈 SHARP ACTION PERFORMANCE:")
        print(f"   Total Sharp Plays Analyzed: {total_analyzed}")
        print(f"   Successful Sharp Plays: {successful_sharps}")
        print(f"   Failed Sharp Plays: {total_analyzed - successful_sharps}")
        print(f"   Sharp Success Rate: {success_rate:.1f}%")
        print(f"   Database Updates: {total_updates} entries")
        
        print("\n" + "=" * 60)
        if success_rate > 52.4:  # Break-even point considering typical vig
            print("🔥 SHARP MONEY IS PROFITABLE! Following sharp action would be +EV")
        elif success_rate > 50:
            print("⚖️  Sharp money is slightly above random, but may not beat the vig")
        else:
            print("📉 Sharp money underperformed in this sample")
            
    else:
        print("ℹ️  No sharp action found in finished games to analyze")

if __name__ == "__main__":
    main() 