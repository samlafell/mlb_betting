import requests
import json
from datetime import datetime
import pytz
from dateutil import parser

URL = "https://srfeeds.sportsbettingdime.com/v2/matchups/mlb/betting-splits?books=betmgm,bet365,fanatics,draftkings,caesars,fanduel"

# Helper to get ordinal suffix for day
def ordinal(n):
    if 10 <= n % 100 <= 20:
        suffix = 'th'
    else:
        suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')
    return str(n) + suffix

# Convert UTC ISO string to EST and format
def format_est_time(iso_str):
    if not iso_str or iso_str == 'N/A':
        return 'N/A'
    dt_utc = parser.isoparse(iso_str)
    eastern = pytz.timezone('US/Eastern')
    dt_est = dt_utc.astimezone(eastern)
    month = dt_est.strftime('%B')
    day = ordinal(dt_est.day)
    year = dt_est.year
    time = dt_est.strftime('%-I:%M %p')
    return f"{month} {day}, {year} at {time}"

def main():
    response = requests.get(URL)
    response.raise_for_status()
    data = response.json()

    games = data.get('games', [])
    for game in games:
        date = game.get('date', 'Unknown date')
        home_team = game.get('home', {}).get('team', 'Unknown')
        away_team = game.get('away', {}).get('team', 'Unknown')
        print(f"Game: {away_team} @ {home_team} on {date}\n")

        splits = game.get('bettingSplits', {})
        for split_type in ['spread', 'total', 'moneyline']:
            split = splits.get(split_type, {})
            updated = split.get('updated', 'N/A')
            formatted_time = format_est_time(updated)
            print(f"{split_type.capitalize()} Splits (Last updated: {formatted_time}):")
            if split_type == 'spread':
                print(f"  Home: {split.get('home', {})}")
                print(f"  Away: {split.get('away', {})}")
            elif split_type == 'total':
                print(f"  Over: {split.get('over', {})}")
                print(f"  Under: {split.get('under', {})}")
            elif split_type == 'moneyline':
                print(f"  Home: {split.get('home', {})}")
                print(f"  Away: {split.get('away', {})}")
            print()

if __name__ == '__main__':
    main() 