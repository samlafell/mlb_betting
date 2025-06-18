#!/usr/bin/env python3
"""
VSIN HTML Parser

This script parses the sports betting data from VSIN HTML files and displays it in a tabular format.
The script extracts information about teams, spreads, betting percentages, and other relevant betting data.
Supports multiple sports: NFL, NBA, MLB, NHL, CBB (College Basketball), CFB (College Football), etc.
"""
import os
import sys
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import re
import argparse
from datetime import datetime
import pathlib


def create_output_directory(directory):
    """
    Create the output directory if it doesn't exist.
    
    Args:
        directory (str): The path to the output directory
    """
    if not os.path.exists(directory):
        try:
            os.makedirs(directory)
            print(f"Created directory: {directory}")
        except OSError as e:
            print(f"Error creating directory {directory}: {e}")
            sys.exit(1)


def read_html_file(file_path):
    """
    Read the contents of an HTML file.
    
    Args:
        file_path (str): The path to the HTML file
        
    Returns:
        str: The HTML content as a string
        
    Raises:
        FileNotFoundError: If the specified file does not exist
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.read()
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        sys.exit(1)


def parse_betting_data(html_content, file_path=None):
    """
    Parse the betting data from the HTML content.
    
    Args:
        html_content (str): The HTML content to parse
        file_path (str): The path to the HTML file for sport/book detection
        
    Returns:
        list: A list of dictionaries containing the parsed betting data
    """
    # Fix broken HTML - the file appears to be malformed at the beginning
    if not html_content.startswith('<'):
        html_content = '<table>' + html_content
    
    soup = BeautifulSoup(html_content, 'lxml')
    
    # Look for table rows directly
    rows = soup.find_all('tr')
    
    if not rows:
        print("Error: Could not find any table rows in the HTML.")
        return []
    
    # Process the data rows
    data = []
    
    for tr in rows:
        if 'div_dkdark' in tr.get('class', []):  # This is a header row
            continue
            
        row_data = {}
        
        # Process all cells in the row
        cells = tr.find_all('td')
        if not cells or len(cells) < 5:
            continue  # Skip rows with insufficient data
        
        # Extract team names
        team_cell = cells[0]
        team_text = team_cell.get_text(strip=True, separator='\n')
        teams = team_text.split('\n')
        team_names = []
        
        for item in teams:
            # Look for team names (removing numbers, extra spaces, and special words)
            clean_item = re.sub(r'\(\d+\)', '', item).strip()
            clean_item = re.sub(r'History|VSiN Pro Picks|\d+ VSiN Pro Picks', '', clean_item).strip()
            if clean_item and len(clean_item) > 2:  # Ignore very short strings
                team_names.append(clean_item)
        
        if len(team_names) >= 2:
            row_data['Team1'] = team_names[0]
            row_data['Team2'] = team_names[1]
        else:
            continue  # Skip rows where we couldn't extract team names
        
        # Determine sport-specific column structure
        # Get sport info from the file path
        sport, book_name = extract_sport_and_book_name(file_path) if file_path else ('unknown', 'unknown')
        
        # Different sports have different column arrangements, even within the same sportsbook
        if book_name.lower() == 'circa':
            if sport.lower() in ['nba', 'nfl', 'nhl', 'cbb', 'cfb', 'wnba']:
                # NBA/NFL/NHL/Basketball format: Spread (col 1), Total (col 4), Moneyline (col 7)
                spread_col, total_col, ml_col = 1, 4, 7
            else:
                # MLB/Baseball format: Moneyline (col 1), Total (col 4), Spread (col 7)  
                spread_col, total_col, ml_col = 7, 4, 1
        else:
            # DraftKings and others: Moneyline (col 1), Total (col 4), Spread (col 7)
            spread_col, total_col, ml_col = 7, 4, 1
        
        # Extract moneyline data (variable column)
        if len(cells) > ml_col:
            ml_text = cells[ml_col].get_text(strip=True, separator='\n')
            # For Circa: numbers like 131, -146 (need to add + for positive)
            # For DraftKings: numbers like +130, -155 (already formatted)
            ml_values = re.findall(r'[+-]?\d+', ml_text)
            if len(ml_values) >= 1:
                val1 = ml_values[0]
                # Add + sign if positive number doesn't have one
                if not val1.startswith(('+', '-')) and val1.isdigit():
                    val1 = '+' + val1
                row_data['Moneyline1'] = val1
            if len(ml_values) >= 2:
                row_data['Moneyline2'] = ml_values[1]
        
        # Extract handle percentages for moneyline (column after moneyline)
        if len(cells) > ml_col + 1:
            ml_handle_text = cells[ml_col + 1].get_text(strip=True, separator='\n')
            ml_handle_values = re.findall(r'\d+%', ml_handle_text)
            if len(ml_handle_values) >= 1:
                row_data['MoneylineHandle1'] = ml_handle_values[0]
            if len(ml_handle_values) >= 2:
                row_data['MoneylineHandle2'] = ml_handle_values[1]
        
        # Extract bet percentages for moneyline (column after handle)
        if len(cells) > ml_col + 2:
            ml_bets_text = cells[ml_col + 2].get_text(strip=True, separator='\n')
            ml_bets_values = re.findall(r'\d+%', ml_bets_text)
            if len(ml_bets_values) >= 1:
                row_data['MoneylineBets1'] = ml_bets_values[0]
            if len(ml_bets_values) >= 2:
                row_data['MoneylineBets2'] = ml_bets_values[1]
        
        # Extract total (over/under) data (variable column)
        if len(cells) > total_col:
            total_text = cells[total_col].get_text(strip=True, separator='\n')
            total_values = re.findall(r'\d+\.?\d*', total_text)
            if len(total_values) >= 1:
                row_data['Total'] = total_values[0]
        
        # Extract handle percentages for total (column after total)
        if len(cells) > total_col + 1:
            total_handle_text = cells[total_col + 1].get_text(strip=True, separator='\n')
            total_handle_values = re.findall(r'\d+%', total_handle_text)
            if len(total_handle_values) >= 1:
                row_data['TotalOverHandle'] = total_handle_values[0]
            if len(total_handle_values) >= 2:
                row_data['TotalUnderHandle'] = total_handle_values[1]
        
        # Extract bet percentages for total (column after handle)
        if len(cells) > total_col + 2:
            total_bets_text = cells[total_col + 2].get_text(strip=True, separator='\n')
            total_bets_values = re.findall(r'\d+%', total_bets_text)
            if len(total_bets_values) >= 1:
                row_data['TotalOverBets'] = total_bets_values[0]
            if len(total_bets_values) >= 2:
                row_data['TotalUnderBets'] = total_bets_values[1]
        
        # Extract spread/run line data (variable column)
        if len(cells) > spread_col:
            spread_text = cells[spread_col].get_text(strip=True, separator='\n')
            spread_values = re.findall(r'[+-]?\d+\.?\d*', spread_text)
            if len(spread_values) >= 1:
                row_data['Spread1'] = spread_values[0]
            if len(spread_values) >= 2:
                row_data['Spread2'] = spread_values[1]
        
        # Extract handle percentages for spread (column after spread)
        if len(cells) > spread_col + 1:
            spread_handle_text = cells[spread_col + 1].get_text(strip=True, separator='\n')
            spread_handle_values = re.findall(r'\d+%', spread_handle_text)
            if len(spread_handle_values) >= 1:
                row_data['SpreadHandle1'] = spread_handle_values[0]
            if len(spread_handle_values) >= 2:
                row_data['SpreadHandle2'] = spread_handle_values[1]
        
        # Extract bet percentages for spread (column after handle)
        if len(cells) > spread_col + 2:
            spread_bets_text = cells[spread_col + 2].get_text(strip=True, separator='\n')
            spread_bets_values = re.findall(r'\d+%', spread_bets_text)
            if len(spread_bets_values) >= 1:
                row_data['SpreadBets1'] = spread_bets_values[0]
            if len(spread_bets_values) >= 2:
                row_data['SpreadBets2'] = spread_bets_values[1]
        
        if row_data and 'Team1' in row_data and 'Team2' in row_data:  # Only add if we have team names
            data.append(row_data)
    
    return data


def display_data_as_table(data):
    """
    Display the parsed data as a formatted table using pandas.
    
    Args:
        data (list): List of dictionaries containing the betting data
        
    Returns:
        pandas.DataFrame: The data formatted as a pandas DataFrame
    """
    if not data:
        print("No data to display.")
        return None
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Clean up data by removing percentage signs and converting to numeric where appropriate
    for col in df.columns:
        if col.endswith(('Handle1', 'Handle2', 'Bets1', 'Bets2')):
            df[col] = df[col].str.replace('%', '').str.replace('▲', '').str.strip()
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Rename columns for better readability
    columns_map = {
        'Team1': 'Team 1',
        'Team2': 'Team 2',
        'Spread1': 'Spread 1',
        'Spread2': 'Spread 2',
        'SpreadHandle1': 'Spread Handle 1 (%)',
        'SpreadHandle2': 'Spread Handle 2 (%)',
        'SpreadBets1': 'Spread Bets 1 (%)',
        'SpreadBets2': 'Spread Bets 2 (%)',
        'Total': 'Total Line',
        'TotalOverHandle': 'Over Handle (%)',
        'TotalUnderHandle': 'Under Handle (%)',
        'TotalOverBets': 'Over Bets (%)',
        'TotalUnderBets': 'Under Bets (%)',
        'Moneyline1': 'ML 1',
        'Moneyline2': 'ML 2',
        'MoneylineHandle1': 'ML Handle 1 (%)',
        'MoneylineHandle2': 'ML Handle 2 (%)',
        'MoneylineBets1': 'ML Bets 1 (%)',
        'MoneylineBets2': 'ML Bets 2 (%)'
    }
    df = df.rename(columns=columns_map)
    
    # Create a more readable display by organizing columns into groups
    team_cols = ['Team 1', 'Team 2']
    spread_cols = ['Spread 1', 'Spread 2', 'Spread Handle 1 (%)', 'Spread Handle 2 (%)', 
                  'Spread Bets 1 (%)', 'Spread Bets 2 (%)']
    total_cols = ['Total Line', 'Over Handle (%)', 'Under Handle (%)', 'Over Bets (%)', 'Under Bets (%)']
    ml_cols = ['ML 1', 'ML 2', 'ML Handle 1 (%)', 'ML Handle 2 (%)', 'ML Bets 1 (%)', 'ML Bets 2 (%)']
    
    # Reorder columns
    display_cols = team_cols + spread_cols + total_cols + ml_cols
    display_cols = [col for col in display_cols if col in df.columns]
    df = df[display_cols]
    
    # Format the DataFrame for display
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.colheader_justify', 'center')
    
    return df


def extract_sport_and_book_name(file_path):
    """
    Extract the sport and sportsbook name from the input filename.
    
    Args:
        file_path (str): The path to the HTML file
        
    Returns:
        tuple: (sport, sportsbook) or ('unknown', 'unknown') if not found
    """
    filename = os.path.basename(file_path)
    
    # Try to extract sport and sportsbook using regex - new format: vsin_html_sport_sportsbook_timestamp.html
    match = re.search(r'vsin_html_(\w+)_(\w+)_\d{8}_\d{6}\.html', filename)
    if match:
        return match.group(1), match.group(2)
    
    # Fallback for old format: vsin_html_sportsbook_timestamp.html
    match = re.search(r'vsin_html_(\w+)_\d{8}_\d{6}\.html', filename)
    if match:
        return 'cbb', match.group(1)  # Assume college basketball for old format
    
    return "unknown", "unknown"


def main():
    """
    Main function to run the script.
    """
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description='Parse VSIN betting data from HTML files for multiple sports',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python vsin_parser.py -i examples/vsin_html_nfl_circa_20250330_133425.html
  python vsin_parser.py -i examples/vsin_html_nba_dk_20250330_140000.html
  python vsin_parser.py  # Uses default file
        """
    )
    parser.add_argument('-i', '--input', 
                        help='Path to the HTML file to parse (default: examples/vsin_html.html)',
                        default='examples/vsin_html.html')
    args = parser.parse_args()
    
    # Read the HTML file
    html_content = read_html_file(args.input)
    
    # Extract sport and book name from file
    sport, book_name = extract_sport_and_book_name(args.input)
    
    # Parse the betting data
    betting_data = parse_betting_data(html_content)
    
    # Display the data as a table
    df = display_data_as_table(betting_data)
    
    if df is not None:
        # Sport-specific icons
        sport_icons = {
            'nfl': '🏈', 'nba': '🏀', 'mlb': '⚾', 'nhl': '🏒',
            'cbb': '🏀', 'cfb': '🏈', 'wnba': '🏀', 'ufc': '🥊',
            'pga': '⛳', 'tennis': '🎾', 'epl': '⚽', 'ufl': '🏈'
        }
        icon = sport_icons.get(sport.lower(), '🎯')
        
        print(f"\n{icon} VSIN {sport.upper()} Betting Data ({len(df)} games found, {book_name.upper()})")
        print("=" * 100)
        
        # Set pandas display options for better formatting
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', 15)
        pd.set_option('display.precision', 1)
        
        # Add data freshness and timing analysis
        current_time = datetime.now()
        
        # Determine likely game times based on sport and day
        def get_typical_game_times(sport, current_time):
            """Get typical start times for different sports"""
            sport_times = {
                'nfl': {'sun': [13, 16, 20], 'mon': [20], 'thu': [20], 'other': []},
                'nba': {'daily': [19, 20, 21, 22]}, 
                'mlb': {'daily': [13, 19, 20]},
                'nhl': {'daily': [19, 20, 21]},
                'cbb': {'daily': [18, 19, 20, 21]},
                'cfb': {'sat': [12, 15, 18, 20], 'fri': [20], 'other': []},
                'wnba': {'daily': [19, 20]},
                'ufc': {'sat': [22], 'other': []},
                'pga': {'daily': [8, 13]},
                'tennis': {'daily': [11, 18]},
                'epl': {'sat': [7, 10, 12], 'sun': [9, 11], 'other': [15]},
                'ufl': {'sat': [13, 16], 'sun': [15], 'other': []}
            }
            
            day_name = current_time.strftime('%a').lower()
            sport_schedule = sport_times.get(sport.lower(), {'daily': [19, 20]})
            
            if 'daily' in sport_schedule:
                return sport_schedule['daily']
            elif day_name in sport_schedule:
                return sport_schedule[day_name]
            else:
                return sport_schedule.get('other', [])
        
        typical_times = get_typical_game_times(sport, current_time)
        
        # Print timing analysis
        if typical_times:
            next_games = []
            for hour in typical_times:
                game_time = current_time.replace(hour=hour, minute=0, second=0, microsecond=0)
                if game_time < current_time:
                    game_time += pd.Timedelta(days=1)  # Next day if time has passed
                time_diff = game_time - current_time
                
                if time_diff.total_seconds() <= 30 * 60:  # Within 30 minutes
                    next_games.append(f"🚨 GAME STARTING SOON (~{int(time_diff.total_seconds()/60)} min)")
                elif time_diff.total_seconds() > 6 * 3600:  # More than 6 hours
                    next_games.append(f"⏰ Early data: Games likely {time_diff.total_seconds()/3600:.1f}+ hours away")
            
            if next_games:
                print(f"\n⏱️  TIMING: {next_games[0]}")
        
        # Print game-by-game analysis for better readability
        for i, row in df.iterrows():
            print(f"\n🎯 Game {i+1}: {row['Team 1']} vs {row['Team 2']}")
            print("-" * 60)
            
            # Spread Analysis
            if 'Spread 1' in row and 'Spread 2' in row:
                spread_line = f"{row['Spread 1']}/{row['Spread 2']}"
                spread_handle = f"{row.get('Spread Handle 1 (%)', '-')}/{row.get('Spread Handle 2 (%)', '-')}"
                spread_bets = f"{row.get('Spread Bets 1 (%)', '-')}/{row.get('Spread Bets 2 (%)', '-')}"
                print(f"📊 SPREAD {spread_line:>12} | Money: {spread_handle:>8}% | Bets: {spread_bets:>8}%")
            
            # Total Analysis  
            if 'Total Line' in row:
                total_line = f"O/U {row['Total Line']}"
                total_handle = f"{row.get('Over Handle (%)', '-')}/{row.get('Under Handle (%)', '-')}"
                total_bets = f"{row.get('Over Bets (%)', '-')}/{row.get('Under Bets (%)', '-')}"
                print(f"📈 TOTAL {total_line:>13} | Money: {total_handle:>8}% | Bets: {total_bets:>8}%")
            
            # Moneyline Analysis
            if 'ML 1' in row and 'ML 2' in row:
                ml_line = f"{row['ML 1']}/{row['ML 2']}"
                ml_handle = f"{row.get('ML Handle 1 (%)', '-')}/{row.get('ML Handle 2 (%)', '-')}"
                ml_bets = f"{row.get('ML Bets 1 (%)', '-')}/{row.get('ML Bets 2 (%)', '-')}"
                print(f"💰 MONEYLINE {ml_line:>8} | Money: {ml_handle:>8}% | Bets: {ml_bets:>8}%")
            
            # Sharp vs Public Analysis with fallback for small sample sizes
            def analyze_sharp_public(handle_1, bets_1, handle_2, bets_2, bet_type, team_name):
                """Analyze sharp vs public money with small sample size detection"""
                try:
                    h1, b1, h2, b2 = float(handle_1 or 0), float(bets_1 or 0), float(handle_2 or 0), float(bets_2 or 0)
                    
                    # Check for small sample size indicators (0% or 100%)
                    if any(x in [0, 100] for x in [h1, b1, h2, b2]):
                        return f"⚠️  SMALL SAMPLE: {bet_type} data limited, use caution"
                    
                    if h1 > 0 and b1 > 0:
                        if abs(h1 - b1) >= 10:
                            if h1 > b1:
                                return f"🔥 SHARP MONEY: More money than bets on {team_name} {bet_type.lower()}"
                            else:
                                return f"👥 PUBLIC MONEY: More bets than money on {team_name} {bet_type.lower()}"
                    return None
                except (ValueError, TypeError):
                    return None
            
            # Try spread analysis first
            spread_analysis = analyze_sharp_public(
                row.get('Spread Handle 1 (%)', 0), row.get('Spread Bets 1 (%)', 0),
                row.get('Spread Handle 2 (%)', 0), row.get('Spread Bets 2 (%)', 0),
                "SPREAD", row['Team 1']
            )
            
            # If spread has small sample, fall back to moneyline
            if spread_analysis and "SMALL SAMPLE" in spread_analysis:
                ml_analysis = analyze_sharp_public(
                    row.get('ML Handle 1 (%)', 0), row.get('ML Bets 1 (%)', 0),
                    row.get('ML Handle 2 (%)', 0), row.get('ML Bets 2 (%)', 0),
                    "MONEYLINE", row['Team 1']
                )
                if ml_analysis and "SMALL SAMPLE" not in ml_analysis:
                    print(ml_analysis + " (spread data limited)")
                else:
                    print(spread_analysis)
            elif spread_analysis:
                print(spread_analysis)
        
        print("\n" + "=" * 100)
        print(f"✅ Data successfully parsed from {args.input}")
        print(f"💡 Tip: Use 'python vsin_scraper.py {sport} {book_name}' to get fresh data")


if __name__ == "__main__":
    main()