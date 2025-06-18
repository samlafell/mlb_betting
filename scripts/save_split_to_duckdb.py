import duckdb
from datetime import datetime
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from examples.python_classes import SpreadSplit, TotalSplit, MoneylineSplit, SplitType, Source

# Path to DuckDB file
db_path = 'data/raw/mlb_betting.duckdb'

# Example data for the game
example_game_id = '20250613.06-MLB-PIT@CHC'
home_team = 'Cubs'
away_team = 'Pirates'
game_datetime = datetime(2025, 6, 13, 18, 20)
last_updated = datetime(2025, 6, 13, 17, 3)  # Example EST time

# Example splits using new classes - these directly map to the database schema
splits = [
    SpreadSplit(
        game_id=example_game_id,
        home_team=home_team,
        away_team=away_team,
        game_datetime=game_datetime,
        last_updated=last_updated,
        spread_value='-1.5/+1.5',
        home_team_bets=1504,
        home_team_bets_percentage=76.66,
        home_team_stake_percentage=65.69,
        away_team_bets=458,
        away_team_bets_percentage=23.34,
        away_team_stake_percentage=34.31,
        outcome='Away',
        sharp_action=True
    ),
    TotalSplit(
        game_id=example_game_id,
        home_team=home_team,
        away_team=away_team,
        game_datetime=game_datetime,
        last_updated=last_updated,
        total_value='6.5',
        over_bets=8703,
        over_bets_percentage=78.03,
        over_stake_percentage=45.02,
        under_bets=2450,
        under_bets_percentage=21.97,
        under_stake_percentage=54.98,
        outcome='Under',
        sharp_action=True
    ),
    MoneylineSplit(
        game_id=example_game_id,
        home_team=home_team,
        away_team=away_team,
        game_datetime=game_datetime,
        last_updated=last_updated,
        moneyline_value='-118/-102',
        home_team_bets=13315,
        home_team_bets_percentage=66.01,
        home_team_stake_percentage=52.49,
        away_team_bets=6855,
        away_team_bets_percentage=33.99,
        away_team_stake_percentage=47.51,
        outcome='Away',
        sharp_action=True
    ),
]

# Save to DuckDB and print all splits
def save_splits(splits):
    con = duckdb.connect(db_path)
    # Read and execute schema SQL
    with open('sql/schema.sql', 'r') as f:
        schema_sql = f.read()
    con.execute(schema_sql)
    
    for split in splits:
        # The new classes directly map to the database schema
        if isinstance(split, SpreadSplit):
            con.execute('''
                INSERT INTO SPLITS (
                    game_id, home_team, away_team, game_datetime, split_type, last_updated,
                    home_team_bets, home_team_bets_percentage, home_team_stake_percentage,
                    away_team_bets, away_team_bets_percentage, away_team_stake_percentage,
                    home_team_spread_bets, home_team_spread_bets_percentage, home_team_spread_stake_percentage,
                    away_team_spread_bets, away_team_spread_bets_percentage, away_team_spread_stake_percentage,
                    spread_value, sharp_action, outcome
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', [
                split.game_id, split.home_team, split.away_team, split.game_datetime,
                split.split_type.value, split.last_updated,
                split.home_team_bets, split.home_team_bets_percentage, split.home_team_stake_percentage,
                split.away_team_bets, split.away_team_bets_percentage, split.away_team_stake_percentage,
                split.home_team_bets, split.home_team_bets_percentage, split.home_team_stake_percentage,  # spread columns
                split.away_team_bets, split.away_team_bets_percentage, split.away_team_stake_percentage,  # spread columns
                split.spread_value, split.sharp_action, split.outcome
            ])
        elif isinstance(split, TotalSplit):
            con.execute('''
                INSERT INTO SPLITS (
                    game_id, home_team, away_team, game_datetime, split_type, last_updated,
                    over_bets, over_bets_percentage, over_stake_percentage,
                    under_bets, under_bets_percentage, under_stake_percentage,
                    home_team_total_bets, total_value, sharp_action, outcome
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', [
                split.game_id, split.home_team, split.away_team, split.game_datetime,
                split.split_type.value, split.last_updated,
                split.over_bets, split.over_bets_percentage, split.over_stake_percentage,
                split.under_bets, split.under_bets_percentage, split.under_stake_percentage,
                split.over_bets,  # home_team_total_bets maps to over_bets
                split.total_value, split.sharp_action, split.outcome
            ])
        elif isinstance(split, MoneylineSplit):
            con.execute('''
                INSERT INTO SPLITS (
                    game_id, home_team, away_team, game_datetime, split_type, last_updated,
                    home_team_bets, home_team_bets_percentage, home_team_stake_percentage,
                    away_team_bets, away_team_bets_percentage, away_team_stake_percentage,
                    moneyline_value, sharp_action, outcome
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', [
                split.game_id, split.home_team, split.away_team, split.game_datetime,
                split.split_type.value, split.last_updated,
                split.home_team_bets, split.home_team_bets_percentage, split.home_team_stake_percentage,
                split.away_team_bets, split.away_team_bets_percentage, split.away_team_stake_percentage,
                split.moneyline_value, split.sharp_action, split.outcome
            ])
    
    # Query and print all splits
    print('Current SPLITS table:')
    results = con.execute('SELECT * FROM SPLITS').fetchall()
    for row in results:
        print(row)
    con.close()

if __name__ == '__main__':
    save_splits(splits)
    print('Splits saved to DuckDB.') 