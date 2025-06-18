import requests
import json
import duckdb
from datetime import datetime
import pytz
from dateutil import parser
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from examples.python_classes import SpreadSplit, TotalSplit, MoneylineSplit, SplitType, Source
from config.settings import config

# Configuration from TOML file
URL = config.sbd_api_url
DB_PATH = config.database_path
SOURCE = config.sbd_source
BOOK = None     # NULL for SBD source



def parse_splits_from_api():
    """Fetch and parse betting splits from SportsBettingDime API"""
    response = requests.get(URL)
    response.raise_for_status()
    data = response.json()
    
    all_splits = []
    games = data.get('games', [])
    
    # Set up timezone objects
    utc = pytz.UTC
    eastern = pytz.timezone('US/Eastern')
    
    for game in games:
        # Extract game information using proper API fields
        game_id = game.get('_id')  # Use the actual _id from API
        if not game_id:
            print(f"Warning: No _id found for game, skipping...")
            continue
            
        # Use the proper date field from API and convert to Eastern Time
        game_datetime_str = game.get('date')
        if game_datetime_str:
            game_datetime_utc = parser.isoparse(game_datetime_str)
            # Ensure it's UTC aware
            if game_datetime_utc.tzinfo is None:
                game_datetime_utc = utc.localize(game_datetime_utc)
            # Convert to Eastern Time
            game_datetime = game_datetime_utc.astimezone(eastern)
        else:
            print(f"Warning: No date found for game {game_id}, using current time")
            game_datetime = datetime.now(eastern)
        
        # Extract team information
        home_team = game.get('home', {}).get('team', 'Unknown')
        away_team = game.get('away', {}).get('team', 'Unknown')
        
        # Format the date for display in Eastern Time
        date_display = game_datetime.strftime('%Y-%m-%d %I:%M %p %Z')
        
        print(f"Processing: {away_team} @ {home_team} on {date_display}")
        print(f"  Game ID: {game_id}")
        
        splits = game.get('bettingSplits', {})
        
        # Process each split type
        for split_type in ['spread', 'total', 'moneyline']:
            split_data = splits.get(split_type, {})
            if not split_data:
                continue
                
            # Parse last updated time and convert to Eastern Time
            updated = split_data.get('updated', 'N/A')
            if updated and updated != 'N/A':
                last_updated_utc = parser.isoparse(updated)
                # Ensure it's UTC aware
                if last_updated_utc.tzinfo is None:
                    last_updated_utc = utc.localize(last_updated_utc)
                # Convert to Eastern Time
                last_updated = last_updated_utc.astimezone(eastern)
            else:
                last_updated = datetime.now(eastern)
            
            # Create appropriate split object based on type
            if split_type == 'spread':
                home_data = split_data.get('home', {})
                away_data = split_data.get('away', {})
                spread_value = f"{away_data.get('spread', 'N/A')}/{home_data.get('spread', 'N/A')}"
                
                split_obj = SpreadSplit(
                    game_id=game_id,
                    home_team=home_team,
                    away_team=away_team,
                    game_datetime=game_datetime,
                    last_updated=last_updated,
                    spread_value=spread_value,
                    home_team_bets=home_data.get('bets'),
                    home_team_bets_percentage=home_data.get('betsPercentage'),
                    home_team_stake_percentage=home_data.get('stakePercentage'),
                    away_team_bets=away_data.get('bets'),
                    away_team_bets_percentage=away_data.get('betsPercentage'),
                    away_team_stake_percentage=away_data.get('stakePercentage'),
                    source=SOURCE,
                    book=BOOK,
                    sharp_action=False,  # Could be enhanced to detect sharp action
                    outcome=None  # Could be enhanced to determine outcome
                )
                
            elif split_type == 'total':
                over_data = split_data.get('over', {})
                under_data = split_data.get('under', {})
                total_value = over_data.get('total', 'N/A')
                
                split_obj = TotalSplit(
                    game_id=game_id,
                    home_team=home_team,
                    away_team=away_team,
                    game_datetime=game_datetime,
                    last_updated=last_updated,
                    total_value=str(total_value),
                    over_bets=over_data.get('bets'),
                    over_bets_percentage=over_data.get('betsPercentage'),
                    over_stake_percentage=over_data.get('stakePercentage'),
                    under_bets=under_data.get('bets'),
                    under_bets_percentage=under_data.get('betsPercentage'),
                    under_stake_percentage=under_data.get('stakePercentage'),
                    source=SOURCE,
                    book=BOOK,
                    sharp_action=False,
                    outcome=None
                )
                
            elif split_type == 'moneyline':
                home_data = split_data.get('home', {})
                away_data = split_data.get('away', {})
                moneyline_value = f"{home_data.get('moneyline', 'N/A')}/{away_data.get('moneyline', 'N/A')}"
                
                split_obj = MoneylineSplit(
                    game_id=game_id,
                    home_team=home_team,
                    away_team=away_team,
                    game_datetime=game_datetime,
                    last_updated=last_updated,
                    moneyline_value=moneyline_value,
                    home_team_bets=home_data.get('bets'),
                    home_team_bets_percentage=home_data.get('betsPercentage'),
                    home_team_stake_percentage=home_data.get('stakePercentage'),
                    away_team_bets=away_data.get('bets'),
                    away_team_bets_percentage=away_data.get('betsPercentage'),
                    away_team_stake_percentage=away_data.get('stakePercentage'),
                    source=SOURCE,
                    book=BOOK,
                    sharp_action=False,
                    outcome=None
                )
            
            all_splits.append(split_obj)
            
            # Print formatted information with Eastern Time
            formatted_time = last_updated.strftime('%B %d, %Y at %I:%M %p %Z')
            print(f"  {split_type.capitalize()} Splits (Last updated: {formatted_time})")
            if split_type == 'spread':
                print(f"    Home: {home_data}")
                print(f"    Away: {away_data}")
            elif split_type == 'total':
                print(f"    Over: {over_data}")
                print(f"    Under: {under_data}")
            elif split_type == 'moneyline':
                print(f"    Home: {home_data}")
                print(f"    Away: {away_data}")
            print()
    
    return all_splits

def save_splits_to_duckdb(splits):
    """Save splits to DuckDB database"""
    con = duckdb.connect(DB_PATH)
    
    # Read and execute schema SQL
    with open('sql/schema.sql', 'r') as f:
        schema_sql = f.read()
    con.execute(schema_sql)
    
    for split in splits:
        # Get the unified INSERT query for all split types
        query = config.get_insert_query(split.split_type.value)
        
        # All split types now use the same long format approach
        con.execute(query, [
            split.game_id, split.home_team, split.away_team, split.game_datetime,
            split.split_type.value, split.last_updated, split.source, split.book,
            split.home_or_over_bets, split.home_or_over_bets_percentage, split.home_or_over_stake_percentage,
            split.away_or_under_bets, split.away_or_under_bets_percentage, split.away_or_under_stake_percentage,
            split.split_value, split.sharp_action, split.outcome
        ])
    
    # Query and print summary
    table_name = config.full_table_name
    print(f'\nSaved {len(splits)} splits to {table_name} table.')
    
    # Show recent entries
    print(f'\nRecent entries in {table_name}:')
    results = con.execute(f'''
        SELECT game_id, home_team, away_team, split_type, source, book, last_updated 
        FROM {table_name} 
        ORDER BY last_updated DESC 
        LIMIT 10
    ''').fetchall()
    
    for row in results:
        print(f"  {row[0]} | {row[2]} @ {row[1]} | {row[3]} | {row[4]} | {row[5]} | {row[6]}")
    
    con.close()

def main():
    """Main function to parse and save betting splits"""
    print("Fetching betting splits from SportsBettingDime API...")
    splits = parse_splits_from_api()
    
    print(f"\nParsed {len(splits)} splits from {len(set(s.game_id for s in splits))} games.")
    
    print("\nSaving to DuckDB...")
    save_splits_to_duckdb(splits)
    
    print("\nProcess completed successfully!")

if __name__ == '__main__':
    main() 