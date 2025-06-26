#!/usr/bin/env python3
"""
Simple script to check if a game had betting recommendations and their results.

Usage:
    python check_game_results.py TOR CLE 2025-06-24
    
This will show:
- What sharp signals existed for the game
- What the system would have recommended  
- Whether those recommendations won or lost
"""

import sys
from datetime import datetime
from src.mlb_sharp_betting.db.connection import DatabaseManager

def check_game_results(away_team, home_team, game_date):
    """Check betting recommendations and results for a specific game."""
    
    db_mgr = DatabaseManager()
    
    print(f'🎯 CHECKING GAME: {away_team} @ {home_team} on {game_date}')
    print('=' * 80)
    
    # Get all betting splits for the game
    splits_query = '''
        SELECT 
            split_type, split_value,
            home_or_over_stake_percentage as stake_pct,
            home_or_over_bets_percentage as bets_pct,
            (home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
            source, book, last_updated
        FROM splits.raw_mlb_betting_splits
        WHERE home_team = %s AND away_team = %s
        AND DATE(game_datetime) = %s
        AND ROW_NUMBER() OVER (
            PARTITION BY split_type, source, book 
            ORDER BY last_updated DESC
        ) = 1
        ORDER BY ABS(home_or_over_stake_percentage - home_or_over_bets_percentage) DESC
    '''
    
    splits = db_mgr.execute_query(splits_query, (home_team, away_team, game_date))
    
    # Get game outcome
    outcome_query = '''
        SELECT 
            away_score, home_score,
            away_team, home_team,
            game_date, status
        FROM game_outcomes
        WHERE home_team = %s AND away_team = %s
        AND DATE(game_date) = %s
    '''
    
    try:
        outcomes = db_mgr.execute_query(outcome_query, (home_team, away_team, game_date))
        outcome = outcomes[0] if outcomes else None
    except:
        outcome = None
    
    if not splits:
        print('❌ No betting data found for this game')
        return
    
    print(f'📊 Found {len(splits)} betting signals:')
    print()
    
    # Analyze each signal
    recommendations = []
    
    for split in splits:
        diff = split['differential']
        if abs(diff) >= 10:  # Sharp signal threshold
            signal_strength = '🔥 VERY STRONG' if abs(diff) >= 20 else '📈 STRONG'
            
            # Determine recommendation
            if split['split_type'] == 'moneyline':
                rec = f"{'Home' if diff > 0 else 'Away'} ML"
            elif split['split_type'] == 'spread':
                rec = f"{'Home' if diff > 0 else 'Away'} {split['split_value']}"
            elif split['split_type'] == 'total':
                rec = f"{'Over' if diff > 0 else 'Under'} {split['split_value']}"
            
            print(f'{signal_strength} {split["split_type"].upper()} SIGNAL ({split["source"]}-{split["book"]}):')
            print(f'   🎯 Recommendation: {rec}')
            print(f'   📊 Differential: {diff:+.1f}% (Stake: {split["stake_pct"]}%, Bets: {split["bets_pct"]}%)')
            
            # Check if recommendation won (if we have outcome data)
            if outcome:
                result = check_recommendation_result(split, outcome, away_team, home_team)
                print(f'   {result}')
                recommendations.append({
                    'type': split['split_type'],
                    'recommendation': rec,
                    'differential': diff,
                    'result': result
                })
            else:
                print(f'   ⏳ Result: Game outcome not available')
            
            print()
    
    # Summary
    if outcome and recommendations:
        wins = len([r for r in recommendations if '✅ WIN' in r['result']])
        total = len(recommendations)
        win_pct = (wins / total * 100) if total > 0 else 0
        
        print('📈 RECOMMENDATION SUMMARY:')
        print(f'   🏆 Performance: {wins}W-{total-wins}L ({win_pct:.1f}% win rate)')
        print(f'   🎯 Game Result: {away_team} {outcome["away_score"]}, {home_team} {outcome["home_score"]}')
    
    elif outcome:
        print('📈 GAME RESULT:')
        print(f'   🎯 Final Score: {away_team} {outcome["away_score"]}, {home_team} {outcome["home_score"]}')
        print('   ℹ️  No strong signals (≥10% differential) found')
    
    else:
        print('⏳ Game outcome data not available yet')

def check_recommendation_result(split, outcome, away_team, home_team):
    """Check if a recommendation won or lost."""
    
    away_score = outcome['away_score']
    home_score = outcome['home_score']
    total_score = away_score + home_score
    diff = split['differential']
    
    if split['split_type'] == 'moneyline':
        # Positive diff = sharp money on home, negative = sharp money on away
        if diff > 0:  # Recommended home team
            return '✅ WIN' if home_score > away_score else '❌ LOSS'
        else:  # Recommended away team
            return '✅ WIN' if away_score > home_score else '❌ LOSS'
    
    elif split['split_type'] == 'total':
        line = float(split['split_value'])
        # Positive diff = sharp money on over, negative = sharp money on under
        if diff > 0:  # Recommended over
            return '✅ WIN' if total_score > line else '❌ LOSS'
        else:  # Recommended under
            return '✅ WIN' if total_score < line else '❌ LOSS'
    
    elif split['split_type'] == 'spread':
        line = float(split['split_value'])
        # Positive diff = sharp money on home, negative = sharp money on away
        if diff > 0:  # Recommended home team
            home_covered = (home_score - away_score) > line
            return '✅ WIN' if home_covered else '❌ LOSS'
        else:  # Recommended away team  
            away_covered = (away_score - home_score) > line
            return '✅ WIN' if away_covered else '❌ LOSS'
    
    return '❓ UNKNOWN'

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print('Usage: python check_game_results.py <away_team> <home_team> <date>')
        print('Example: python check_game_results.py TOR CLE 2025-06-24')
        sys.exit(1)
    
    away_team = sys.argv[1].upper()
    home_team = sys.argv[2].upper()
    game_date = sys.argv[3]
    
    try:
        # Validate date format
        datetime.strptime(game_date, '%Y-%m-%d')
        check_game_results(away_team, home_team, game_date)
    except ValueError:
        print('❌ Invalid date format. Use YYYY-MM-DD')
        sys.exit(1)
    except Exception as e:
        print(f'❌ Error: {e}')
        sys.exit(1) 