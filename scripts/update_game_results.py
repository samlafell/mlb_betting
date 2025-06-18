#!/usr/bin/env python3
"""
Update game results from MLB Stats API
- Fetch today's games and their results
- Match with database entries by team names and date
- Update database with MLB game IDs and outcomes
"""

import duckdb
import statsapi
from datetime import datetime, date
import pytz
import sys
import os

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import Config

# Load configuration
config = Config()
DB_PATH = config.database_path

def get_todays_games_from_db():
    """Get today's games from the database"""
    con = duckdb.connect(DB_PATH)
    
    query = """
    SELECT DISTINCT 
        game_id, 
        home_team, 
        away_team, 
        game_datetime,
        outcome
    FROM splits.raw_mlb_betting_splits 
    WHERE DATE(game_datetime) = CURRENT_DATE 
    ORDER BY game_datetime
    """
    
    result = con.execute(query).fetchall()
    con.close()
    
    return result

def get_todays_mlb_games():
    """Get today's games from MLB Stats API"""
    today = date.today().strftime('%Y-%m-%d')
    
    # Get today's schedule
    schedule = statsapi.schedule(date=today)
    
    games = []
    for game in schedule:
        games.append({
            'game_pk': game['game_id'],
            'home_team': game['home_name'],
            'away_team': game['away_name'],
            'game_date': game['game_date'],
            'status': game['status'],
            'home_score': game.get('home_score', None),
            'away_score': game.get('away_score', None),
            'winning_team': game.get('winning_team', None),
            'losing_team': game.get('losing_team', None)
        })
    
    return games

def normalize_team_name(team_name):
    """Normalize team names for matching"""
    # Common variations in team names
    name_mappings = {
        'D-backs': 'Diamondbacks',
        'Indians': 'Guardians',  # SBD still uses old Cleveland name
        'Guardians': 'Guardians',  # MLB API uses current name
        'White Sox': 'White Sox',
        'Red Sox': 'Red Sox',
        'Blue Jays': 'Blue Jays',
        'Rays': 'Rays',
        'A\'s': 'Athletics',
        'Athletics': 'Athletics',
        'Nats': 'Nationals'
    }
    
    return name_mappings.get(team_name, team_name)

def match_games(db_games, mlb_games):
    """Match database games with MLB API games"""
    matches = []
    
    for db_game in db_games:
        db_game_id, db_home, db_away, db_datetime, db_outcome = db_game
        
        # Normalize team names
        db_home_norm = normalize_team_name(db_home)
        db_away_norm = normalize_team_name(db_away)
        
        for mlb_game in mlb_games:
            mlb_home_norm = normalize_team_name(mlb_game['home_team'])
            mlb_away_norm = normalize_team_name(mlb_game['away_team'])
            
            # Match by team names - try multiple approaches
            home_match = (db_home_norm == mlb_home_norm or 
                         db_home in mlb_game['home_team'] or 
                         mlb_game['home_team'] in db_home or
                         db_home_norm in mlb_game['home_team'] or
                         mlb_home_norm in db_home)
            
            away_match = (db_away_norm == mlb_away_norm or 
                         db_away in mlb_game['away_team'] or 
                         mlb_game['away_team'] in db_away or
                         db_away_norm in mlb_game['away_team'] or
                         mlb_away_norm in db_away)
            
            if home_match and away_match:
                
                # Determine outcome
                outcome = None
                if mlb_game['status'] == 'Final':
                    if mlb_game['home_score'] is not None and mlb_game['away_score'] is not None:
                        if mlb_game['home_score'] > mlb_game['away_score']:
                            outcome = f"Home Win {mlb_game['home_score']}-{mlb_game['away_score']}"
                        else:
                            outcome = f"Away Win {mlb_game['away_score']}-{mlb_game['home_score']}"
                elif mlb_game['status'] in ['In Progress', 'Live']:
                    outcome = 'In Progress'
                elif mlb_game['status'] in ['Scheduled', 'Pre-Game']:
                    outcome = 'Scheduled'
                else:
                    outcome = mlb_game['status']
                
                matches.append({
                    'db_game_id': db_game_id,
                    'mlb_game_pk': mlb_game['game_pk'],
                    'home_team': db_home,
                    'away_team': db_away,
                    'outcome': outcome,
                    'status': mlb_game['status'],
                    'home_score': mlb_game.get('home_score'),
                    'away_score': mlb_game.get('away_score')
                })
                break
    
    return matches

def update_database(matches):
    """Update database with MLB game IDs and outcomes"""
    con = duckdb.connect(DB_PATH)
    
    updated_count = 0
    
    for match in matches:
        # Update all rows for this game
        update_query = """
        UPDATE splits.raw_mlb_betting_splits 
        SET 
            game_id = ?,
            outcome = ?
        WHERE game_id = ?
        """
        
        result = con.execute(update_query, [
            str(match['mlb_game_pk']),  # New MLB game ID
            match['outcome'],           # Game outcome
            match['db_game_id']         # Original game ID to match
        ])
        
        rows_affected = result.fetchone()[0] if result else 0
        updated_count += rows_affected
        
        print(f"Updated {rows_affected} rows for {match['away_team']} @ {match['home_team']}")
        print(f"  Old ID: {match['db_game_id']}")
        print(f"  New ID: {match['mlb_game_pk']}")
        print(f"  Outcome: {match['outcome']}")
        print()
    
    con.close()
    return updated_count

def main():
    print("Fetching today's games from database...")
    db_games = get_todays_games_from_db()
    print(f"Found {len(db_games)} unique games in database")
    
    print("\nFetching today's games from MLB Stats API...")
    mlb_games = get_todays_mlb_games()
    print(f"Found {len(mlb_games)} games from MLB API")
    
    print("\nMatching games...")
    matches = match_games(db_games, mlb_games)
    print(f"Successfully matched {len(matches)} games")
    
    if matches:
        print("\nUpdating database...")
        updated_count = update_database(matches)
        print(f"\nTotal rows updated: {updated_count}")
        
        print("\nGame Results Summary:")
        print("-" * 60)
        for match in matches:
            status_emoji = "✅" if match['status'] == 'Final' else "🔄" if match['status'] in ['In Progress', 'Live'] else "⏰"
            print(f"{status_emoji} {match['away_team']} @ {match['home_team']}: {match['outcome']}")
    else:
        print("No games matched. Check team name mappings.")

if __name__ == "__main__":
    main() 