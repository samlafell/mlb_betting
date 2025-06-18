#!/usr/bin/env python3
"""
Fetch Current Betting Lines from VSIN
Gets actual spread, total, and moneyline values from VSIN
Updates database with real line values instead of 'N/A'
"""

import duckdb
import sys
import os
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import Config

config = Config()
DB_PATH = config.database_path

# VSIN configuration - Using Circa data
VSIN_BASE_URL = "https://data.vsin.com"
VSIN_MLB_URL = f"{VSIN_BASE_URL}/mlb/betting-splits/?view=circa"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

def fetch_vsin_html():
    """Fetch HTML content from VSIN MLB betting splits page"""
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://data.vsin.com/",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0",
    }
    
    try:
        print(f"🌐 Fetching VSIN MLB data from {VSIN_MLB_URL}")
        response = requests.get(VSIN_MLB_URL, headers=headers, timeout=30)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching VSIN data: {e}")
        return None

def parse_vsin_betting_data(html_content):
    """Parse betting data from VSIN HTML content"""
    if not html_content:
        return []
    
    # Fix broken HTML if needed
    if not html_content.startswith('<'):
        html_content = '<table>' + html_content
    
    soup = BeautifulSoup(html_content, 'lxml')
    rows = soup.find_all('tr')
    
    if not rows:
        print("❌ Could not find any table rows in VSIN HTML")
        return []
    
    games_data = []
    
    for tr in rows:
        if 'div_dkdark' in tr.get('class', []):  # Skip header rows
            continue
            
        cells = tr.find_all('td')
        if not cells or len(cells) < 5:
            continue
        
        game_data = {}
        
        # Extract team names from first cell
        team_cell = cells[0]
        team_text = team_cell.get_text(strip=True, separator='\n')
        teams = team_text.split('\n')
        team_names = []
        
        for item in teams:
            # Clean team names
            clean_item = re.sub(r'\(\d+\)', '', item).strip()
            clean_item = re.sub(r'History|VSiN Pro Picks|\d+ VSiN Pro Picks', '', clean_item).strip()
            if clean_item and len(clean_item) > 2:
                team_names.append(clean_item)
        
        if len(team_names) < 2:
            continue
            
        game_data['away_team'] = team_names[0]
        game_data['home_team'] = team_names[1]
        
        # For MLB on Circa view: Moneyline (col 1), Total (col 4), Spread (col 7)
        ml_col, total_col, spread_col = 1, 4, 7
        
        # Extract moneyline odds (Circa format: numbers like 131, -146)
        if len(cells) > ml_col:
            ml_text = cells[ml_col].get_text(strip=True, separator='\n')
            ml_values = re.findall(r'[+-]?\d+', ml_text)
            if len(ml_values) >= 2:
                # First value is away team, second is home team
                # Add + sign if positive number doesn't have one (Circa format)
                away_ml = ml_values[0] if ml_values[0].startswith(('+', '-')) else '+' + ml_values[0]
                home_ml = ml_values[1] if ml_values[1].startswith(('+', '-')) else '+' + ml_values[1]
                game_data['away_ml'] = away_ml
                game_data['home_ml'] = home_ml
        
        # Extract total (over/under)
        if len(cells) > total_col:
            total_text = cells[total_col].get_text(strip=True, separator='\n')
            total_values = re.findall(r'\d+\.?\d*', total_text)
            if len(total_values) >= 1:
                game_data['total'] = float(total_values[0])
        
        # Extract spread/runline
        if len(cells) > spread_col:
            spread_text = cells[spread_col].get_text(strip=True, separator='\n')
            spread_values = re.findall(r'[+-]?\d+\.?\d*', spread_text)
            if len(spread_values) >= 2:
                # First value is away spread, second is home spread
                game_data['away_spread'] = float(spread_values[0])
                game_data['home_spread'] = float(spread_values[1])
        
        # Only add if we have meaningful data
        if any(key in game_data for key in ['away_ml', 'total', 'away_spread']):
            games_data.append(game_data)
    
    return games_data

def normalize_team_name(team_name):
    """Normalize team names to match database format"""
    # Common team name mappings
    team_mappings = {
        'Arizona Diamondbacks': 'Diamondbacks',
        'Atlanta Braves': 'Braves',
        'Baltimore Orioles': 'Orioles',
        'Boston Red Sox': 'Red Sox',
        'Chicago Cubs': 'Cubs',
        'Chicago White Sox': 'White Sox',
        'Cincinnati Reds': 'Reds',
        'Cleveland Guardians': 'Guardians',
        'Cleveland Indians': 'Guardians',  # Handle old name
        'Colorado Rockies': 'Rockies',
        'Detroit Tigers': 'Tigers',
        'Houston Astros': 'Astros',
        'Kansas City Royals': 'Royals',
        'Los Angeles Angels': 'Angels',
        'Los Angeles Dodgers': 'Dodgers',
        'Miami Marlins': 'Marlins',
        'Milwaukee Brewers': 'Brewers',
        'Minnesota Twins': 'Twins',
        'New York Mets': 'Mets',
        'New York Yankees': 'Yankees',
        'Oakland Athletics': 'Athletics',
        'Philadelphia Phillies': 'Phillies',
        'Pittsburgh Pirates': 'Pirates',
        'San Diego Padres': 'Padres',
        'San Francisco Giants': 'Giants',
        'Seattle Mariners': 'Mariners',
        'St. Louis Cardinals': 'Cardinals',
        'ST Louis Cardinals': 'Cardinals',
        'Tampa Bay Rays': 'Rays',
        'Texas Rangers': 'Rangers',
        'Toronto Blue Jays': 'Blue Jays',
        'Washington Nationals': 'Nationals'
    }
    
    # Try exact match first
    if team_name in team_mappings:
        return team_mappings[team_name]
    
    # Try partial matches
    for full_name, short_name in team_mappings.items():
        if team_name in full_name or full_name in team_name:
            return short_name
    
    return team_name

def update_database_with_vsin_lines(games_data):
    """Update database with actual VSIN line values"""
    con = duckdb.connect(DB_PATH)
    
    updates = 0
    matched_games = 0
    
    print(f"\n📊 Processing {len(games_data)} games from VSIN...")
    
    for game in games_data:
        away_team = normalize_team_name(game['away_team'])
        home_team = normalize_team_name(game['home_team'])
        
        print(f"\n🔍 Looking for: {away_team} @ {home_team}")
        
        # Find matching games in database
        query = """
            SELECT DISTINCT game_id, away_team, home_team 
            FROM splits.raw_mlb_betting_splits 
            WHERE (away_team LIKE ? OR away_team LIKE ?) 
            AND (home_team LIKE ? OR home_team LIKE ?)
        """
        
        results = con.execute(query, [
            f'%{away_team}%', f'%{game["away_team"]}%',
            f'%{home_team}%', f'%{game["home_team"]}%'
        ]).fetchall()
        
        if not results:
            print(f"   ❌ No match found in database")
            continue
        
        for game_id, db_away, db_home in results:
            print(f"   ✅ Matched: {db_away} @ {db_home} (ID: {game_id})")
            matched_games += 1
            
            # Update spread values
            if 'away_spread' in game and 'home_spread' in game:
                spread_value = f"{game['away_spread']:+.1f}/{game['home_spread']:+.1f}"
                con.execute("""
                    UPDATE splits.raw_mlb_betting_splits 
                    SET split_value = ?
                    WHERE game_id = ? AND split_type = 'Spread' AND split_value IN ('N/A/N/A', 'N/A')
                """, [spread_value, game_id])
                print(f"      📈 Spread: {spread_value}")
                updates += 1
            
            # Update total values
            if 'total' in game:
                total_value = str(game['total'])
                con.execute("""
                    UPDATE splits.raw_mlb_betting_splits 
                    SET split_value = ?
                    WHERE game_id = ? AND split_type = 'Total' AND split_value IN ('N/A', 'N/A/N/A')
                """, [total_value, game_id])
                print(f"      🎯 Total: {total_value}")
                updates += 1
            
            # Update moneyline values
            if 'away_ml' in game and 'home_ml' in game:
                ml_value = f"{game['home_ml']}/{game['away_ml']}"
                con.execute("""
                    UPDATE splits.raw_mlb_betting_splits 
                    SET split_value = ?
                    WHERE game_id = ? AND split_type = 'Moneyline' AND split_value IN ('N/A/N/A', 'N/A')
                """, [ml_value, game_id])
                print(f"      💰 Moneyline: {ml_value}")
                updates += 1
    
    con.close()
    return updates, matched_games

def main():
    print("🎯 FETCHING CURRENT MLB BETTING LINES FROM VSIN")
    print("=" * 60)
    
    # Fetch VSIN data
    html_content = fetch_vsin_html()
    if not html_content:
        print("❌ Failed to fetch VSIN data")
        return
    
    # Parse betting data
    print("📊 Parsing VSIN betting data...")
    games_data = parse_vsin_betting_data(html_content)
    
    if not games_data:
        print("❌ No games found in VSIN data")
        return
    
    print(f"✅ Found {len(games_data)} games with betting lines")
    
    # Show sample data
    print("\n📋 Sample VSIN Lines:")
    for i, game in enumerate(games_data[:3]):
        print(f"   Game {i+1}: {game['away_team']} @ {game['home_team']}")
        if 'away_spread' in game:
            print(f"     Spread: {game['away_spread']:+.1f}/{game['home_spread']:+.1f}")
        if 'total' in game:
            print(f"     Total: {game['total']}")
        if 'away_ml' in game:
            print(f"     Moneyline: {game['home_ml']}/{game['away_ml']}")
    
    # Update database
    print(f"\n💾 Updating database with VSIN line values...")
    updates, matched_games = update_database_with_vsin_lines(games_data)
    
    print(f"\n" + "=" * 60)
    print(f"✅ VSIN Integration Complete!")
    print(f"   📊 Games from VSIN: {len(games_data)}")
    print(f"   🎯 Games matched: {matched_games}")
    print(f"   💾 Database updates: {updates}")
    print(f"\n🎯 Now run sharp analysis with real VSIN lines!")

if __name__ == "__main__":
    main() 