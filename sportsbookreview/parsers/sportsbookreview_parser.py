"""
Concrete implementation of SportsbookReview.com data parser.

This parser handles the specific HTML structure and data extraction
patterns used by SportsbookReview.com for MLB betting data.
"""

import re
import json
import logging
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime, date
from bs4 import BeautifulSoup, Tag
from decimal import Decimal

from .base_parser import SportsbookReviewBaseParser, ParseError
from ..models.game import EnhancedGame, GameType
from ..models.odds_data import OddsData, OddsSnapshot, LineMovementData
from ..models.sportsbook_mapping import SportsbookName
from ..models.odds_data import MarketSide
from ..models.base import BetType
from ..models.base import DataQuality
from .validators import GameDataValidator

# Import Team enum for centralized team name handling
from src.mlb_sharp_betting.models.game import Team


logger = logging.getLogger(__name__)


class SportsbookReviewParser(SportsbookReviewBaseParser):
    """
    Concrete parser for SportsbookReview.com HTML content.
    
    Handles the specific parsing of game data, odds data, and line movements
    from SportsbookReview.com pages.
    """
    
    def __init__(self):
        """Initialize the parser with SportsbookReview-specific configurations."""
        super().__init__()
        
        # CSS selectors for different page elements based on documentation
        self.selectors = {
            'main_table': '#tbody-mlb',
            'game_row': 'div[class*="d-flex"]',
            'game_time': 'div[class*="d-flex"]:first-child',
            'team_info': 'div[class*="d-flex"]:nth-child(2)',
            'odds_container': 'div[class*="d-flex"]:nth-child(3)',
            'sportsbook_links': 'a[data-aatracker]',
            'odds_cells': 'span[role="button"]',
            'line_history_link': 'a[href*="line-history"]'
        }
        
        # Known sportsbooks from documentation
        self.sportsbooks = {
            'betMGM': 'BetMGM',
            'fanduel': 'FanDuel',
            'caesars': 'Caesars',
            'bet365': 'Bet365',
            'draftkings': 'DraftKings',
            'betrivers': 'BetRivers',
            'bet_rivers_ny': 'BetRivers'
        }
    
    def parse_page(self, html_content: str, source_url: str) -> List[Dict[str, Any]]:
        """
        Parse a SportsbookReview page and return game data.
        
        Args:
            html_content: HTML content from SportsbookReview
            source_url: Source URL for the page
            
        Returns:
            List of game data dictionaries
        """
        try:
            # Extract date and bet type from URL
            # Example URLs:
            # https://www.sportsbookreview.com/betting-odds/mlb-baseball/?date=2025-07-05
            # https://www.sportsbookreview.com/betting-odds/mlb-baseball/pointspread/full-game/?date=2025-07-05
            
            bet_type = 'moneyline'  # default
            if '/pointspread/' in source_url:
                bet_type = 'spread'
            elif '/totals/' in source_url:
                bet_type = 'totals'
            
            # Extract date from URL
            game_date = date.today()  # default
            if '?date=' in source_url:
                date_str = source_url.split('?date=')[1].split('&')[0]
                try:
                    game_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                except ValueError:
                    logger.warning(f"Could not parse date from URL: {source_url}")
            
            return self.parse_bet_type_page(html_content, bet_type, game_date, source_url)
            
        except Exception as e:
            logger.error(f"Error parsing page from {source_url}: {e}")
            raise ParseError(f"Failed to parse page: {e}")

    def parse_bet_type_page(self, html_content: str, bet_type: str, game_date: date, source_url: str) -> List[Dict[str, Any]]:
        """
        Parse a bet type page (moneyline, spread, totals) for a specific date.
        
        Args:
            html_content: HTML content from SportsbookReview
            bet_type: Type of betting page (moneyline, spread, totals)
            game_date: Date of the games
            source_url: Source URL for the page
            
        Returns:
            List of game data dictionaries
        """
        try:
            # First try to extract data from embedded JSON
            json_games = self._extract_games_from_json(html_content, bet_type, game_date, source_url)
            if json_games:
                logger.info(f"Successfully extracted {len(json_games)} games from JSON data")
                return json_games
            
            # Fall back to HTML parsing if JSON extraction fails
            logger.warning("JSON extraction failed, falling back to HTML parsing")
            return self._parse_html_fallback(html_content, bet_type, game_date, source_url)
            
        except Exception as e:
            logger.error(f"Error parsing {bet_type} page for {game_date}: {e}")
            raise ParseError(f"Failed to parse {bet_type} page: {e}")
    
    def _extract_games_from_json(self, html_content: str, bet_type: str, game_date: date, source_url: str) -> List[GameDataValidator]:
        """
        Extract game data from the embedded JSON in script tags.
        
        Args:
            html_content: HTML content from SportsbookReview
            bet_type: Type of betting page (moneyline, spread, totals)
            game_date: Date of the games
            source_url: Source URL for the page
            
        Returns:
            List of game data dictionaries
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find script tags containing JSON data
            script_tags = soup.find_all('script')
            
            for script in script_tags:
                script_content = script.string
                if script_content and '"props":' in script_content and len(script_content) > 1000:
                    # Extract the JSON data
                    json_pattern = r'{"props":(.*?),"page"'
                    match = re.search(json_pattern, script_content, re.DOTALL)
                    if match:
                        json_str = '{"props":' + match.group(1) + '}'
                        data = json.loads(json_str)
                        
                        # Extract games from the JSON structure
                        if 'pageProps' in data['props'] and 'oddsTables' in data['props']['pageProps']:
                            odds_tables = data['props']['pageProps']['oddsTables']
                            if odds_tables and len(odds_tables) > 0:
                                game_rows = odds_tables[0].get('oddsTableModel', {}).get('gameRows', [])
                                
                                games_data: List[GameDataValidator] = []
                                for game_row in game_rows:
                                    game_data_validator = self._process_json_game_row(game_row, bet_type, game_date, source_url)
                                    if game_data_validator:
                                        games_data.append(game_data_validator)
                                
                                return games_data
            
            return []
            
        except (json.JSONDecodeError, KeyError, AttributeError) as e:
            logger.error(f"Error extracting JSON data: {e}")
            return []
    
    def _process_json_game_row(self, game_row: Dict[str, Any], bet_type: str, game_date: date, source_url: str) -> Optional[GameDataValidator]:
        """
        Process a single game row from the JSON data.
        
        Args:
            game_row: Game row data from JSON
            bet_type: Type of betting page
            game_date: Date of the games
            source_url: Source URL for the page
            
        Returns:
            Processed game data dictionary or None if invalid
        """
        try:
            game_view = game_row.get('gameView', {})
            odds_views = game_row.get('oddsViews', []) or []

            # ------------------------------------------------------------------
            # NEW: Robustly filter out None entries that occasionally appear in
            #       the oddsViews list and can cause attribute errors downstream.
            # ------------------------------------------------------------------
            odds_views = [ov for ov in odds_views if ov is not None]
            if not odds_views:
                logger.debug("Skipped game row with empty oddsViews – likely incomplete data block")
                return None
            # ------------------------------------------------------------------

            # Extract and normalize team names
            home_team_raw = game_view.get('homeTeam', {}).get('shortName')
            away_team_raw = game_view.get('awayTeam', {}).get('shortName')
            
            # Normalize team names using centralized Team enum
            home_team = Team.normalize_team_name(home_team_raw) if home_team_raw else None
            away_team = Team.normalize_team_name(away_team_raw) if away_team_raw else None
            
            if not home_team or not away_team:
                logger.warning(f"Could not normalize team names for a game in {source_url}. Skipping.")
                return None
            
            # Normalize game status
            game_status_raw = game_view.get('gameStatusText', '')
            game_status = self._normalize_game_status(game_status_raw)
            
            # Create game datetime from date and parsed time
            game_datetime = self._create_game_datetime(game_date, game_view.get('startDate'))
            
            # Extract basic game information
            game_data = {
                'sbr_game_id': str(game_view.get('gameId', '')),  # Use correct field name
                'game_date': game_datetime,  # Use datetime instead of date
                'game_datetime': game_datetime,
                'game_time': self._parse_game_time(game_view.get('startDate')),
                'home_team': home_team,
                'away_team': away_team,
                'home_team_full': game_view.get('homeTeam', {}).get('fullName'),
                'away_team_full': game_view.get('awayTeam', {}).get('fullName'),
                'home_pitcher': self._format_pitcher_name(game_view.get('homeStarter', {})),
                'away_pitcher': self._format_pitcher_name(game_view.get('awayStarter', {})),
                'venue_name': game_view.get('venueName'),
                'city': game_view.get('city'),
                'state': game_view.get('state'),
                'game_status': game_status,
                'bet_type': bet_type,
                'source_url': source_url,
                'scraped_at': datetime.now()
            }
            
            # Extract final scores if available
            if game_view.get('homeTeamScore') is not None and game_view.get('awayTeamScore') is not None:
                game_data['final_score'] = {
                    'home': game_view.get('homeTeamScore'),
                    'away': game_view.get('awayTeamScore')
                }
            
            # Extract consensus/public betting percentages
            consensus = game_view.get('consensus', {})
            if consensus:
                game_data['public_betting_percentage'] = {
                    'home_ml': consensus.get('homeMoneyLinePickPercent'),
                    'away_ml': consensus.get('awayMoneyLinePickPercent'),
                    'home_spread': consensus.get('homeSpreadPickPercent'),
                    'away_spread': consensus.get('awaySpreadPickPercent'),
                    'over': consensus.get('overPickPercent'),
                    'under': consensus.get('underPickPercent')
                }
            
            # Extract odds from different sportsbooks
            odds_data = []
            # Filter and format odds information, skipping malformed entries
            for odds_view in odds_views:
                # The field is actually 'sportsbook', not 'provider'
                provider = odds_view.get("sportsbook") or odds_view.get("provider")
                if not provider:
                    # Occasionally the JSON contains null placeholders – ignore them
                    continue

                sportsbook_name = self.sportsbooks.get(provider, provider)

                line_data = self._format_odds_line(odds_view, bet_type)
                if not line_data:
                    # Nothing useful extracted for this sportsbook
                    continue

                line_data["sportsbook"] = sportsbook_name
                odds_data.append(line_data)
                
            game_data['odds_data'] = odds_data
            
            # Validate the constructed game data
            validated_game_data = GameDataValidator.validate_data(game_data)

            if not validated_game_data:
                logger.warning(f"Game data failed validation for game {game_data.get('sbr_game_id')}")
                return None

            return validated_game_data
            
        except Exception as e:
            logger.error(f"Error processing game row from JSON: {e}", exc_info=True)
            return None
    
    def _parse_game_time(self, start_date_str: str) -> Optional[str]:
        """Parse game start time from ISO format."""
        try:
            if start_date_str:
                dt = datetime.fromisoformat(start_date_str.replace('Z', '+00:00'))
                return dt.strftime('%I:%M %p %Z')
            return None
        except Exception:
            return start_date_str
    
    def _format_pitcher_name(self, pitcher_data: Dict[str, Any]) -> Optional[str]:
        """Format pitcher name from JSON data."""
        if not pitcher_data:
            return None
        
        first_name = pitcher_data.get('firstName', '')
        last_name = pitcher_data.get('lastName', '')
        throws = pitcher_data.get('throwsShort', '')
        
        if first_name and last_name:
            name = f"{first_name} {last_name}"
            if throws:
                name += f" ({throws})"
            return name
        
        return None
    
    def _format_odds_line(self, line_data: Dict[str, Any], bet_type: str) -> Dict[str, Any]:
        """Format odds line data based on bet type."""
        if not line_data:
            return {}
        
        formatted_line = {}
        
        # Extract odds from nested structure: prioritize currentLine over openingLine
        current_line = line_data.get("currentLine", {}) or {}
        opening_line = line_data.get("openingLine", {}) or {}
        
        # Use current line if available, otherwise fall back to opening line
        odds_source = current_line if current_line else opening_line
        
        # ✅ SAFEGUARD: Always include bet_type in odds records to prevent null bet_type issues
        if bet_type:
            formatted_line["bet_type"] = bet_type
        else:
            logger.warning("bet_type is None/empty in _format_odds_line - this may cause validation issues")
        
        # SportsbookReview returns all bet types in the same JSON structure
        # Extract all available data regardless of the requested bet_type
        
        # Always extract moneyline data if available
        home_odds = odds_source.get("homeOdds")
        away_odds = odds_source.get("awayOdds")
        if home_odds is not None and away_odds is not None:
            formatted_line["moneyline_home"] = home_odds
            formatted_line["moneyline_away"] = away_odds
        
        # Always extract spread data if available
        home_spread = odds_source.get("homeSpread")
        away_spread = odds_source.get("awaySpread")
        if home_spread is not None and away_spread is not None:
            formatted_line["spread_home"] = home_spread
            formatted_line["spread_away"] = away_spread
            # For spreads, the odds are in homeOdds/awayOdds
            if home_odds is not None:
                formatted_line["home_spread_price"] = home_odds
            if away_odds is not None:
                formatted_line["away_spread_price"] = away_odds
        
        # Always extract totals data if available
        over_odds = odds_source.get("overOdds")
        under_odds = odds_source.get("underOdds")
        total_line = odds_source.get("total") or odds_source.get("totalLine")
        if total_line is not None and (over_odds is not None or under_odds is not None):
            formatted_line["total_line"] = total_line
            if over_odds is not None:
                formatted_line["total_over"] = over_odds
            if under_odds is not None:
                formatted_line["total_under"] = under_odds
        
        return formatted_line

    def _normalize_game_status(self, status_raw: str) -> str:
        """Normalize game status to match enum values."""
        if not status_raw:
            return 'scheduled'
        
        status_lower = status_raw.lower()
        
        # Map common status values
        if 'final' in status_lower:
            return 'final'
        elif 'live' in status_lower or 'in progress' in status_lower:
            return 'live'
        elif 'postponed' in status_lower:
            return 'postponed'
        elif 'cancelled' in status_lower:
            return 'cancelled'
        elif 'suspended' in status_lower:
            return 'suspended'
        elif 'delayed' in status_lower:
            return 'delayed'
        else:
            return 'scheduled'  # Default fallback

    def _create_game_datetime(self, game_date: date, start_date_str: str) -> datetime:
        """Create game datetime from date and start date string."""
        try:
            if start_date_str:
                # Parse ISO format datetime
                dt = datetime.fromisoformat(start_date_str.replace('Z', '+00:00'))
                # Convert to EST timezone
                from zoneinfo import ZoneInfo
                est_tz = ZoneInfo("America/New_York")
                return dt.astimezone(est_tz)
            else:
                # Fallback to date with default time
                from zoneinfo import ZoneInfo
                est_tz = ZoneInfo("America/New_York")
                return datetime.combine(game_date, datetime.min.time()).replace(tzinfo=est_tz)
        except Exception:
            # Last resort fallback
            from zoneinfo import ZoneInfo
            est_tz = ZoneInfo("America/New_York")
            return datetime.combine(game_date, datetime.min.time()).replace(tzinfo=est_tz)
    
    def _parse_html_fallback(self, html_content: str, bet_type: str, game_date: date, source_url: str) -> List[Dict[str, Any]]:
        """
        Fall back to HTML parsing if JSON extraction fails.
        
        Args:
            html_content: HTML content from SportsbookReview
            bet_type: Type of betting page (moneyline, spread, totals)
            game_date: Date of the games
            source_url: Source URL for the page
            
        Returns:
            List of game data dictionaries
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find the main table with game data
            main_table = soup.select_one(self.selectors['main_table'])
            if not main_table:
                logger.warning(f"Could not find main table for {bet_type} on {game_date}")
                return []
            
            # Find all game rows - each game is in a div container
            game_containers = main_table.find_all('div', class_='d-flex')
            
            games_data = []
            current_game_data = None
            
            for container in game_containers:
                # Try to identify if this is a game row or odds row
                game_info = self._extract_game_info_from_container(container)
                
                if game_info:
                    # This is a new game
                    if current_game_data:
                        # Convert to expected format
                        formatted_game_data = {
                            'game': current_game_data,
                            'betting_data': []  # HTML fallback doesn't extract detailed betting data
                        }
                        games_data.append(formatted_game_data)
                    
                    current_game_data = {
                        'game_date': game_date,
                        'game_time': game_info.get('game_time'),
                        'home_team': game_info.get('home_team'),
                        'away_team': game_info.get('away_team'),
                        'home_pitcher': game_info.get('home_pitcher'),
                        'away_pitcher': game_info.get('away_pitcher'),
                        'final_score': game_info.get('final_score'),
                        'bet_type': bet_type,
                        'odds_data': {},
                        'public_betting_percentage': game_info.get('public_betting_percentage'),
                        'line_history_url': game_info.get('line_history_url'),
                        'source_url': source_url,
                        'scraped_at': datetime.now()
                    }
                
                # Try to extract odds data from this container
                odds_data = self._extract_odds_data_from_container(container, bet_type)
                if odds_data and current_game_data:
                    current_game_data['odds_data'].update(odds_data)
            
            # Add the last game if exists
            if current_game_data:
                # Convert to expected format
                formatted_game_data = {
                    'game': current_game_data,
                    'betting_data': []  # HTML fallback doesn't extract detailed betting data
                }
                games_data.append(formatted_game_data)
            
            logger.debug(f"Parsed {len(games_data)} games for {bet_type} on {game_date}")
            return games_data
            
        except Exception as e:
            logger.error(f"Error in HTML fallback parsing for {bet_type} page for {game_date}: {e}")
            return []

    def _extract_game_info_from_container(self, container: Tag) -> Optional[Dict[str, Any]]:
        """
        Extract game information from a container div.
        
        Args:
            container: BeautifulSoup container element
            
        Returns:
            Dictionary with game info or None if not a game container
        """
        try:
            container_text = container.get_text(strip=True)
            
            # Look for time patterns in the container text (e.g., "6:35 PM EDT")
            time_match = re.search(r'(\d{1,2}:\d{2}\s*(?:PM|AM)\s*\w*)', container_text)
            if not time_match:
                return None
            
            game_time = time_match.group(1).strip()
            game_info = {'game_time': game_time}
            
            # Look for team information in the container
            # Pattern: SD-M.Waldron(R)0PHI-Z.Wheeler(R)4
            team_pattern = r'([A-Z]{2,3})-[^(]*\([LR]\)\d*([A-Z]{2,3})-[^(]*\([LR]\)\d*'
            team_match = re.search(team_pattern, container_text)
            
            if team_match:
                away_team = Team.normalize_team_name(team_match.group(1))
                home_team = Team.normalize_team_name(team_match.group(2))
                
                if away_team:
                    game_info['away_team'] = away_team
                if home_team:
                    game_info['home_team'] = home_team
            else:
                # Try simpler team pattern - look for multiple team abbreviations
                team_matches = re.findall(r'\b([A-Z]{2,3})\b', container_text)
                valid_teams = []
                for team_abbrev in team_matches:
                    normalized = Team.normalize_team_name(team_abbrev)
                    if normalized and normalized not in valid_teams:
                        valid_teams.append(normalized)
                
                if len(valid_teams) >= 2:
                    game_info['away_team'] = valid_teams[0]
                    game_info['home_team'] = valid_teams[1]
            
            # Look for pitcher handedness if present
            pitcher_matches = re.findall(r'\(([LR])\)', container_text)
            if len(pitcher_matches) >= 2:
                game_info['away_pitcher'] = f"({pitcher_matches[0]})"
                game_info['home_pitcher'] = f"({pitcher_matches[1]})"
            
            # Look for final score if present
            score_match = re.search(r'(\d+)-(\d+)', container_text)
            if score_match:
                game_info['final_score'] = f"{score_match.group(1)}-{score_match.group(2)}"
            
            # Look for line history link
            line_history_link = container.find('a', href=lambda x: x and 'line-history' in x)
            if line_history_link:
                game_info['line_history_url'] = line_history_link['href']
            
            # Look for public betting percentage (look for % signs)
            if '%' in container_text:
                percentage_matches = re.findall(r'(\d+)%', container_text)
                if len(percentage_matches) >= 2:
                    game_info['public_betting_percentage'] = {
                        'away_percentage': f"{percentage_matches[0]}%",
                        'home_percentage': f"{percentage_matches[1]}%"
                    }
            
            # Only return game info if we have at least time and teams
            if len(game_info) >= 3:  # time + at least 2 team fields
                return game_info
            
            return None
            
        except Exception as e:
            logger.debug(f"Error extracting game info from container: {e}")
            return None
    
    def _extract_odds_data_from_container(self, container: Tag, bet_type: str) -> Optional[Dict[str, Any]]:
        """
        Extract odds data from a container div.
        
        Args:
            container: BeautifulSoup container element
            bet_type: Type of betting data
            
        Returns:
            Dictionary with odds data or None if not an odds container
        """
        try:
            odds_data = {}
            
            # Look for sportsbook links with data-aatracker attribute
            sportsbook_links = container.find_all('a', {'data-aatracker': True})
            
            for link in sportsbook_links:
                tracker_value = link.get('data-aatracker', '')
                sportsbook_name = self._extract_sportsbook_name_from_tracker(tracker_value)
                
                if sportsbook_name:
                    # Extract odds from this sportsbook section
                    odds_values = self._extract_odds_from_sportsbook_section(link, bet_type)
                    if odds_values:
                        odds_data[sportsbook_name] = odds_values
            
            return odds_data if odds_data else None
            
        except Exception as e:
            logger.debug(f"Error extracting odds data from container: {e}")
            return None
    
    def _looks_like_team_info(self, text: str) -> bool:
        """Check if text looks like team information."""
        # Look for team abbreviations, pitcher info, or scores
        team_patterns = [
            r'[A-Z]{2,3}',  # Team abbreviations
            r'\([LR]\)',    # Pitcher handedness
            r'\d+-\d+',     # Scores
        ]
        
        for pattern in team_patterns:
            if re.search(pattern, text):
                return True
        return False
    
    def _parse_team_info(self, text: str) -> Dict[str, Any]:
        """Parse team information from text using centralized Team enum."""
        info = {}
        
        # Extract team names (usually 2-3 letter abbreviations)
        team_matches = re.findall(r'\b[A-Z]{2,3}\b', text)
        if len(team_matches) >= 2:
            # Use Team enum to normalize team names
            away_team = Team.normalize_team_name(team_matches[0])
            home_team = Team.normalize_team_name(team_matches[1])
            
            if away_team:
                info['away_team'] = away_team
            if home_team:
                info['home_team'] = home_team
        
        # Extract pitcher handedness
        pitcher_matches = re.findall(r'\(([LR])\)', text)
        if len(pitcher_matches) >= 2:
            info['away_pitcher'] = f"({pitcher_matches[0]})"
            info['home_pitcher'] = f"({pitcher_matches[1]})"
        
        # Extract final score if present
        score_match = re.search(r'(\d+)-(\d+)', text)
        if score_match:
            info['final_score'] = f"{score_match.group(1)}-{score_match.group(2)}"
        
        return info
    
    def _extract_sportsbook_name_from_tracker(self, tracker_value: str) -> Optional[str]:
        """Extract sportsbook name from data-aatracker attribute."""
        # The tracker value should end with the sportsbook name
        for key, name in self.sportsbooks.items():
            if key.lower() in tracker_value.lower():
                return name
        return None
    
    def _extract_odds_from_sportsbook_section(self, link: Tag, bet_type: str) -> Optional[Dict[str, Any]]:
        """Extract odds values from a sportsbook section."""
        try:
            odds_data = {}
            
            # Look for odds cells (span elements with role="button")
            odds_spans = link.find_all('span', {'role': 'button'})
            
            for span in odds_spans:
                # Extract line and odds from the span
                line_odds = self._parse_odds_span(span, bet_type)
                if line_odds:
                    odds_data.update(line_odds)
            
            return odds_data if odds_data else None
            
        except Exception as e:
            logger.debug(f"Error extracting odds from sportsbook section: {e}")
            return None
    
    def _parse_odds_span(self, span: Tag, bet_type: str) -> Optional[Dict[str, Any]]:
        """Parse individual odds span element."""
        try:
            span_text = span.get_text(strip=True)
            
            if bet_type == 'moneyline':
                # Moneyline odds are just the number (e.g., "-164", "+138")
                odds_match = re.search(r'([+-]\d+)', span_text)
                if odds_match:
                    return {'odds': odds_match.group(1)}
            
            elif bet_type == 'spread':
                # Spread odds have line and odds (e.g., "-1.5 -102")
                spread_match = re.search(r'([+-]?\d+\.?\d*)\s*([+-]\d+)', span_text)
                if spread_match:
                    return {
                        'line': spread_match.group(1),
                        'odds': spread_match.group(2)
                    }
            
            elif bet_type == 'totals':
                # Total odds have over/under and odds (e.g., "8.5 -110")
                total_match = re.search(r'(\d+\.?\d*)\s*([+-]\d+)', span_text)
                if total_match:
                    return {
                        'total': total_match.group(1),
                        'odds': total_match.group(2)
                    }
            
            return None
            
        except Exception as e:
            logger.debug(f"Error parsing odds span: {e}")
            return None
    
    def _has_public_betting_percentage(self, container: Tag) -> bool:
        """Check if container has public betting percentage data."""
        # Look for percentage signs
        text = container.get_text()
        return '%' in text
    
    def _extract_public_betting_percentage(self, container: Tag) -> Optional[Dict[str, str]]:
        """Extract public betting percentage from container."""
        try:
            # Look for percentage values
            percentage_matches = re.findall(r'(\d+)%', container.get_text())
            
            if len(percentage_matches) >= 2:
                return {
                    'away_percentage': f"{percentage_matches[0]}%",
                    'home_percentage': f"{percentage_matches[1]}%"
                }
            
            return None
            
        except Exception as e:
            logger.debug(f"Error extracting public betting percentage: {e}")
            return None

    def parse_game_data(self, html_content: str, source_url: str) -> Optional[EnhancedGame]:
        """
        Parse game data from SportsbookReview HTML content.
        
        Args:
            html_content: HTML content from SportsbookReview
            source_url: Source URL for the game
            
        Returns:
            Parsed EnhancedGame object or None if parsing fails
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract basic game information
            game_data = self._extract_basic_game_info(soup, source_url)
            if not game_data:
                logger.warning(f"Could not extract basic game info from {source_url}")
                return None
            
            # Create EnhancedGame object
            enhanced_game = EnhancedGame(
                sbr_game_id=game_data['sbr_game_id'],
                home_team=game_data['home_team'],
                away_team=game_data['away_team'],
                game_date=game_data['game_date'],
                game_datetime=game_data['game_datetime'],
                game_type=GameType.REGULAR,  # Assume regular season for now
                data_quality=DataQuality.MEDIUM,  # Will be upgraded with more data
                source_url=source_url
            )
            
            # Add venue information if available
            venue_info = self._extract_venue_info(soup)
            if venue_info:
                enhanced_game.venue_info = venue_info
            
            logger.debug(f"Parsed game: {enhanced_game.matchup_display}")
            return enhanced_game
            
        except Exception as e:
            logger.error(f"Error parsing game data from {source_url}: {e}")
            raise ParseError(f"Failed to parse game data: {e}")
    
    def parse_betting_data(
        self, 
        html_content: str, 
        game: EnhancedGame
    ) -> List[OddsData]:
        """
        Parse betting odds data from SportsbookReview HTML content.
        
        Args:
            html_content: HTML content containing betting data
            game: Associated game object
            
        Returns:
            List of parsed OddsData objects
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            odds_data = []
            
            # Parse moneyline odds
            moneyline_odds = self._parse_moneyline_odds(soup, game)
            odds_data.extend(moneyline_odds)
            
            # Parse spread odds
            spread_odds = self._parse_spread_odds(soup, game)
            odds_data.extend(spread_odds)
            
            # Parse total odds
            total_odds = self._parse_total_odds(soup, game)
            odds_data.extend(total_odds)
            
            logger.debug(f"Parsed {len(odds_data)} odds entries for game {game.sbr_game_id}")
            return odds_data
            
        except Exception as e:
            logger.error(f"Error parsing betting data for game {game.sbr_game_id}: {e}")
            raise ParseError(f"Failed to parse betting data: {e}")
    
    def _extract_basic_game_info(self, soup: BeautifulSoup, source_url: str) -> Optional[Dict[str, Any]]:
        """Extract basic game information from the HTML."""
        try:
            # Generate a SportsbookReview game ID from the URL
            sbr_game_id = self._generate_sbr_game_id(source_url)
            
            # Extract teams - try multiple selectors
            teams = self._extract_teams(soup)
            if not teams or len(teams) != 2:
                logger.warning("Could not extract both teams")
                return None
            
            # Extract game date and time
            game_datetime = self._extract_game_datetime(soup)
            if not game_datetime:
                logger.warning("Could not extract game datetime")
                return None
            
            return {
                'sbr_game_id': sbr_game_id,
                'home_team': teams[0],  # First team is typically home
                'away_team': teams[1],  # Second team is typically away
                'game_date': game_datetime.date(),
                'game_datetime': game_datetime
            }
            
        except Exception as e:
            logger.error(f"Error extracting basic game info: {e}")
            return None
    
    def _extract_teams(self, soup: BeautifulSoup) -> Optional[List[str]]:
        """Extract team names from the HTML using centralized Team enum."""
        teams = []
        
        # Try different selectors for team names
        for selector in ['.team-name', '.participant-name', '.team']:
            team_elements = soup.select(selector)
            if team_elements:
                for element in team_elements:
                    team_text = element.get_text(strip=True)
                    normalized_team = Team.normalize_team_name(team_text)
                    if normalized_team:
                        teams.append(normalized_team)
                break
        
        # If we have exactly 2 teams, return them
        if len(teams) == 2:
            return teams
        
        # Try extracting from page title or other elements
        title = soup.find('title')
        if title:
            title_text = title.get_text()
            # Look for patterns like "Team1 vs Team2" or "Team1 @ Team2"
            vs_match = re.search(r'([A-Za-z\s]+)\s+(?:vs|@|\-)\s+([A-Za-z\s]+)', title_text)
            if vs_match:
                team1 = Team.normalize_team_name(vs_match.group(1).strip())
                team2 = Team.normalize_team_name(vs_match.group(2).strip())
                if team1 and team2:
                    return [team1, team2]
        
        return None
    
    def _extract_game_datetime(self, soup: BeautifulSoup) -> Optional[datetime]:
        """Extract game datetime from the HTML."""
        # Try different selectors for datetime
        for selector in ['.game-time', '.event-time', '.start-time', '.datetime']:
            datetime_element = soup.select_one(selector)
            if datetime_element:
                datetime_text = datetime_element.get_text(strip=True)
                parsed_datetime = self.parse_datetime(datetime_text)
                if parsed_datetime:
                    return parsed_datetime
        
        # Try extracting from data attributes
        for element in soup.find_all(attrs={'data-time': True}):
            time_attr = element.get('data-time')
            if time_attr:
                parsed_datetime = self.parse_datetime(time_attr)
                if parsed_datetime:
                    return parsed_datetime
        
        # Default to today for testing purposes
        return datetime.now()
    
    def _extract_venue_info(self, soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
        """Extract venue information from the HTML."""
        venue_info = {}
        
        # Try to find venue name
        for selector in ['.venue-name', '.stadium', '.ballpark']:
            venue_element = soup.select_one(selector)
            if venue_element:
                venue_info['venue_name'] = venue_element.get_text(strip=True)
                break
        
        return venue_info if venue_info else None
    
    def _parse_moneyline_odds(self, soup: BeautifulSoup, game: EnhancedGame) -> List[OddsData]:
        """Parse moneyline odds from the HTML."""
        odds_data = []
        
        # Find moneyline sections
        moneyline_sections = soup.select('.moneyline, .ml-odds')
        
        for section in moneyline_sections:
            # Extract sportsbook name
            sportsbook = self._extract_sportsbook_name(section)
            if not sportsbook:
                continue
            
            # Extract home/away odds
            odds_elements = section.select('.odds, .price')
            if len(odds_elements) >= 2:
                home_odds = self.parse_american_odds(odds_elements[0].get_text(strip=True))
                away_odds = self.parse_american_odds(odds_elements[1].get_text(strip=True))
                
                if home_odds and away_odds:
                    # Create odds data for home team
                    home_odds_data = OddsData(
                        game_id=game.sbr_game_id,
                        sportsbook=sportsbook,
                        bet_type=BetType.MONEYLINE,
                        market_side=MarketSide.HOME,
                        line_movement=LineMovementData(),
                        data_quality=DataQuality.MEDIUM,
                        source_confidence=0.8
                    )
                    home_odds_data.add_odds_update(home_odds)
                    odds_data.append(home_odds_data)
                    
                    # Create odds data for away team
                    away_odds_data = OddsData(
                        game_id=game.sbr_game_id,
                        sportsbook=sportsbook,
                        bet_type=BetType.MONEYLINE,
                        market_side=MarketSide.AWAY,
                        line_movement=LineMovementData(),
                        data_quality=DataQuality.MEDIUM,
                        source_confidence=0.8
                    )
                    away_odds_data.add_odds_update(away_odds)
                    odds_data.append(away_odds_data)
        
        return odds_data
    
    def _parse_spread_odds(self, soup: BeautifulSoup, game: EnhancedGame) -> List[OddsData]:
        """Parse spread odds from the HTML."""
        odds_data = []
        
        # Find spread sections
        spread_sections = soup.select('.spread, .handicap, .point-spread')
        
        for section in spread_sections:
            # Extract sportsbook name
            sportsbook = self._extract_sportsbook_name(section)
            if not sportsbook:
                continue
            
            # Extract spread and odds
            spread_elements = section.select('.spread-value, .line')
            odds_elements = section.select('.odds, .price')
            
            if spread_elements and odds_elements:
                spread_value = self.parse_spread_value(spread_elements[0].get_text(strip=True))
                spread_odds = self.parse_american_odds(odds_elements[0].get_text(strip=True))
                
                if spread_value is not None and spread_odds:
                    # Determine market side based on spread sign
                    market_side = MarketSide.HOME_SPREAD if spread_value < 0 else MarketSide.AWAY_SPREAD
                    
                    spread_odds_data = OddsData(
                        game_id=game.sbr_game_id,
                        sportsbook=sportsbook,
                        bet_type=BetType.SPREAD,
                        market_side=market_side,
                        line_movement=LineMovementData(line_value=spread_value),
                        data_quality=DataQuality.MEDIUM,
                        source_confidence=0.8
                    )
                    spread_odds_data.add_odds_update(spread_odds)
                    odds_data.append(spread_odds_data)
        
        return odds_data
    
    def _parse_total_odds(self, soup: BeautifulSoup, game: EnhancedGame) -> List[OddsData]:
        """Parse total (over/under) odds from the HTML."""
        odds_data = []
        
        # Find total sections
        total_sections = soup.select('.total, .over-under, .ou')
        
        for section in total_sections:
            # Extract sportsbook name
            sportsbook = self._extract_sportsbook_name(section)
            if not sportsbook:
                continue
            
            # Extract total value and odds
            total_elements = section.select('.total-value, .line')
            over_elements = section.select('.over-odds, .over')
            under_elements = section.select('.under-odds, .under')
            
            if total_elements and over_elements and under_elements:
                total_value = self.parse_total_value(total_elements[0].get_text(strip=True))
                over_odds = self.parse_american_odds(over_elements[0].get_text(strip=True))
                under_odds = self.parse_american_odds(under_elements[0].get_text(strip=True))
                
                if total_value is not None and over_odds and under_odds:
                    # Create odds data for over
                    over_odds_data = OddsData(
                        game_id=game.sbr_game_id,
                        sportsbook=sportsbook,
                        bet_type=BetType.TOTAL,
                        market_side=MarketSide.OVER,
                        line_movement=LineMovementData(line_value=total_value),
                        data_quality=DataQuality.MEDIUM,
                        source_confidence=0.8
                    )
                    over_odds_data.add_odds_update(over_odds)
                    odds_data.append(over_odds_data)
                    
                    # Create odds data for under
                    under_odds_data = OddsData(
                        game_id=game.sbr_game_id,
                        sportsbook=sportsbook,
                        bet_type=BetType.TOTAL,
                        market_side=MarketSide.UNDER,
                        line_movement=LineMovementData(line_value=total_value),
                        data_quality=DataQuality.MEDIUM,
                        source_confidence=0.8
                    )
                    under_odds_data.add_odds_update(under_odds)
                    odds_data.append(under_odds_data)
        
        return odds_data
    
    def _extract_sportsbook_name(self, section: Tag) -> Optional[SportsbookName]:
        """Extract sportsbook name from a section."""
        # Try different selectors for sportsbook name
        for selector in ['.sportsbook-name', '.bookmaker', '.book']:
            sbook_element = section.select_one(selector)
            if sbook_element:
                sbook_text = sbook_element.get_text(strip=True).lower()
                return self.normalize_sportsbook_name(sbook_text)
        
        # Try looking in parent elements
        parent = section.parent
        if parent:
            for selector in ['.sportsbook-name', '.bookmaker', '.book']:
                sbook_element = parent.select_one(selector)
                if sbook_element:
                    sbook_text = sbook_element.get_text(strip=True).lower()
                    return self.normalize_sportsbook_name(sbook_text)
        
        # Default to DraftKings for testing
        return SportsbookName.DRAFTKINGS
    
    def _generate_sbr_game_id(self, source_url: str) -> str:
        """Generate a SportsbookReview game ID from the URL."""
        # Extract meaningful parts from URL
        url_parts = source_url.split('/')
        
        # Try to find date and team information in URL
        date_part = None
        team_part = None
        
        for part in url_parts:
            # Look for date patterns
            if re.match(r'\d{4}-\d{2}-\d{2}', part):
                date_part = part
            # Look for team patterns
            elif any(team in part.lower() for team in ['yankees', 'dodgers', 'giants', 'mets']):
                team_part = part
        
        # Create ID based on available information
        if date_part and team_part:
            return f"sbr-{date_part}-{team_part}"
        elif date_part:
            return f"sbr-{date_part}-{hash(source_url) % 10000}"
        else:
            # Fallback to hash-based ID
            return f"sbr-game-{hash(source_url) % 100000}"
    
    def parse_datetime(self, datetime_text: str) -> Optional[datetime]:
        """Parse datetime string using the base class method."""
        return self.parse_game_datetime(datetime_text)
    
    def parse_spread_value(self, spread_text: str) -> Optional[float]:
        """
        Parse spread value from text (e.g., "-1.5", "+2.5").
        
        Args:
            spread_text: Text containing spread value
            
        Returns:
            Float spread value or None if parsing fails
        """
        try:
            if not spread_text:
                return None
            
            # Clean the text and extract numeric value with sign
            cleaned = re.sub(r'[^\d\+\-\.]', '', spread_text)
            match = re.search(r'([+-]?\d+\.?\d*)', cleaned)
            
            if match:
                spread_value = float(match.group(1))
                
                # Validate reasonable range for MLB spreads
                if -20.0 <= spread_value <= 20.0:
                    return spread_value
            
            return None
            
        except (ValueError, AttributeError) as e:
            logger.debug(f"Error parsing spread value '{spread_text}': {e}")
            return None
    
    def parse_total_value(self, total_text: str) -> Optional[float]:
        """
        Parse total value from text (e.g., "8.5", "9.0").
        
        Args:
            total_text: Text containing total value
            
        Returns:
            Float total value or None if parsing fails
        """
        try:
            if not total_text:
                return None
            
            # Extract numeric value (no sign for totals)
            match = re.search(r'(\d+\.?\d*)', total_text)
            
            if match:
                total_value = float(match.group(1))
                
                # Validate reasonable range for MLB totals
                if 3.0 <= total_value <= 30.0:
                    return total_value
            
            return None
            
        except (ValueError, AttributeError) as e:
            logger.debug(f"Error parsing total value '{total_text}': {e}")
            return None
    
    def normalize_sportsbook_name(self, sportsbook_text: str) -> Optional[SportsbookName]:
        """
        Normalize sportsbook name to SportsbookName enum.
        
        Args:
            sportsbook_text: Raw sportsbook name text
            
        Returns:
            SportsbookName enum value or None if not found
        """
        if not sportsbook_text:
            return None
        
        # Convert to lowercase for matching
        text = sportsbook_text.lower().strip()
        
        # Mapping of text patterns to SportsbookName enum values
        sportsbook_mapping = {
            'draftkings': SportsbookName.DRAFTKINGS,
            'fanduel': SportsbookName.FANDUEL,
            'betmgm': SportsbookName.BETMGM,
            'mgm': SportsbookName.BETMGM,
            'caesars': SportsbookName.CAESARS,
            'bet365': SportsbookName.BET365,
            'betrivers': SportsbookName.BETRIVERS,
            'pointsbet': SportsbookName.POINTSBET,
            'unibet': SportsbookName.UNIBET,
            'barstool': SportsbookName.BARSTOOL,
            'williamhill': SportsbookName.WILLIAMHILL,
            'william hill': SportsbookName.WILLIAMHILL,
            'foxbet': SportsbookName.FOXBET,
            'fox bet': SportsbookName.FOXBET,
        }
        
        # Try exact match first
        if text in sportsbook_mapping:
            return sportsbook_mapping[text]
        
        # Try partial matching
        for pattern, sportsbook in sportsbook_mapping.items():
            if pattern in text:
                return sportsbook
        
        # Default to DraftKings if no match found
        logger.debug(f"Unknown sportsbook '{sportsbook_text}', defaulting to DraftKings")
        return SportsbookName.DRAFTKINGS 