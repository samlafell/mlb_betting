#!/usr/bin/env python3
"""
One-time script to extract all MLB team data from Action Network scoreboard API.

This script follows the path structure:
- /games/0/teams/0/ (first game, home team)
- /games/0/teams/1/ (first game, away team)  
- /games/1/teams/0/ (second game, home team)
- /games/1/teams/1/ (second game, away team)
- etc...

Saves all team attributes to JSON and CSV files.
"""

import requests
import json
import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

# Simple logging since structlog might not be available
def log_info(message, **kwargs):
    timestamp = datetime.now().strftime("%H:%M:%S")
    extra = " ".join([f"{k}={v}" for k, v in kwargs.items()])
    print(f"[{timestamp}] INFO: {message} {extra}")

def log_error(message, **kwargs):
    timestamp = datetime.now().strftime("%H:%M:%S")
    extra = " ".join([f"{k}={v}" for k, v in kwargs.items()])
    print(f"[{timestamp}] ERROR: {message} {extra}")


class ActionNetworkTeamExtractor:
    """Extract all team data from Action Network scoreboard API."""
    
    def __init__(self):
        self.session = self._setup_session()
        self.base_url = "https://api.actionnetwork.com/web/v2/scoreboard/proreport/mlb"
        self.params = {
            'bookIds': '15,30,2194,2292,2888,2889,2890,3118,3120,2891,281',
            'periods': 'event',
            'date': '20250701'  # July 1, 2025
        }
        
    def _setup_session(self) -> requests.Session:
        """Setup session with Firefox headers."""
        session = requests.Session()
        
        # Firefox headers from user's actual request
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:139.0) Gecko/20100101 Firefox/139.0',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Referer': 'https://www.actionnetwork.com/mlb/sharp-report',
            'purpose': 'prefetch',
            'x-middleware-prefetch': '1',
            'x-nextjs-data': '1',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-GPC': '1',
            'Priority': 'u=4'
        }
        
        session.headers.update(headers)
        log_info("Session setup complete with Firefox headers")
        return session
    
    def fetch_scoreboard_data(self) -> Dict[str, Any]:
        """Fetch scoreboard data from Action Network API."""
        try:
            # Establish session first
            try:
                self.session.get('https://www.actionnetwork.com/mlb/sharp-report', timeout=10)
                log_info("Session established via sharp report page")
            except:
                log_info("Failed to establish session, continuing anyway")
            
            # Fetch scoreboard data
            response = self.session.get(self.base_url, params=self.params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            games_count = len(data.get('games', []))
            
            log_info("Successfully fetched scoreboard data", 
                    games_count=games_count, data_size=len(str(data)))
            
            return data
            
        except requests.RequestException as e:
            log_error("Failed to fetch scoreboard data", error=str(e))
            raise
    
    def extract_team_data(self, scoreboard_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract all team data following the path structure:
        /games/{game_index}/teams/{team_index}/
        """
        games = scoreboard_data.get('games', [])
        extracted_teams = []
        
        log_info("Starting team data extraction", total_games=len(games))
        
        for game_index, game in enumerate(games):
            game_id = game.get('id')
            game_status = game.get('status')
            start_time = game.get('start_time')
            
            teams = game.get('teams', [])
            
            for team_index, team in enumerate(teams):
                # Create the hierarchical path
                path = f"/games/{game_index}/teams/{team_index}/"
                
                # Extract all team attributes
                team_data = {
                    # Path information
                    'path': path,
                    'game_index': game_index,
                    'team_index': team_index,
                    'team_role': 'home' if team_index == 0 else 'away',
                    
                    # Game information
                    'game_id': game_id,
                    'game_status': game_status,
                    'game_start_time': start_time,
                    
                    # Team attributes (matching the image structure)
                    'id': team.get('id'),
                    'full_name': team.get('full_name'),
                    'display_name': team.get('display_name'),
                    'short_name': team.get('short_name'),
                    'location': team.get('location'),
                    'abbr': team.get('abbr'),
                    'logo': team.get('logo'),
                    'primary_color': team.get('primary_color'),
                    'secondary_color': team.get('secondary_color'),
                    'conference_type': team.get('conference_type'),
                    'division_type': team.get('division_type'),
                    'url_slug': team.get('url_slug'),
                    
                    # Additional data
                    'standings_win': team.get('standings', {}).get('win'),
                    'standings_loss': team.get('standings', {}).get('loss'),
                    'extracted_at': datetime.now().isoformat()
                }
                
                extracted_teams.append(team_data)
        
        log_info("Team data extraction complete", total_teams=len(extracted_teams))
        return extracted_teams
    
    def save_data(self, team_data: List[Dict[str, Any]], output_dir: str = "output"):
        """Save extracted team data to JSON and CSV files."""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save as JSON
        json_file = output_path / f"mlb_teams_data_{timestamp}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(team_data, f, indent=2, ensure_ascii=False)
        
        # Save as CSV
        csv_file = output_path / f"mlb_teams_data_{timestamp}.csv"
        if team_data:
            fieldnames = team_data[0].keys()
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(team_data)
        
        log_info("Data saved successfully", 
                json_file=str(json_file), csv_file=str(csv_file), teams_count=len(team_data))
        
        return json_file, csv_file
    
    def run(self):
        """Run the complete extraction process."""
        print("🏟️  MLB Team Data Extractor")
        print("=" * 50)
        print(f"📅 Extracting data for July 1, 2025")
        print()
        
        try:
            # Fetch data
            print("📡 Fetching scoreboard data...")
            scoreboard_data = self.fetch_scoreboard_data()
            
            # Extract teams
            print("🔍 Extracting team data...")
            team_data = self.extract_team_data(scoreboard_data)
            
            # Save results
            print("💾 Saving data...")
            json_file, csv_file = self.save_data(team_data)
            
            # Summary
            print("\n✅ Extraction Complete!")
            print(f"   📊 Total teams: {len(team_data)}")
            print(f"   🎯 Total games: {len(team_data) // 2}")
            print(f"   📄 JSON file: {json_file}")
            print(f"   📊 CSV file: {csv_file}")
            
            # Show sample data
            print(f"\n🔍 Sample team paths:")
            for team in team_data[:10]:  # Show first 10
                print(f"   {team['path']} → {team['full_name']} ({team['abbr']})")
            
            if len(team_data) > 10:
                print(f"   ... and {len(team_data) - 10} more teams")
            
            # Show path structure
            print(f"\n📁 Path Structure Examples:")
            games_found = set()
            for team in team_data:
                game_idx = team['game_index']
                if game_idx not in games_found and game_idx < 3:  # Show first 3 games
                    print(f"   Game {game_idx}:")
                    home_team = next(t for t in team_data if t['game_index'] == game_idx and t['team_index'] == 0)
                    away_team = next(t for t in team_data if t['game_index'] == game_idx and t['team_index'] == 1)
                    print(f"     {home_team['path']} → {home_team['full_name']} (HOME)")
                    print(f"     {away_team['path']} → {away_team['full_name']} (AWAY)")
                    games_found.add(game_idx)
            
            return team_data
            
        except Exception as e:
            log_error("Extraction failed", error=str(e))
            print(f"❌ Extraction failed: {e}")
            raise


def main():
    """Main function to run the extractor."""
    extractor = ActionNetworkTeamExtractor()
    return extractor.run()


if __name__ == "__main__":
    main() 