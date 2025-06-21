"""
VSIN HTML scraper for betting splits data.

This module provides functionality to scrape betting splits data from VSIN
with proper error handling, retry logic, and data validation.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import structlog
from bs4 import BeautifulSoup, Tag

from .base import HTMLScraper, ScrapingResult, RateLimitConfig, RetryConfig
from ..core.exceptions import ScrapingError, ValidationError
from ..models.splits import BookType, DataSource

logger = structlog.get_logger(__name__)


class VSINScraper(HTMLScraper):
    """
    VSIN HTML scraper for betting splits data.
    
    Scrapes betting splits data from VSIN's web interface using
    Circa as the primary data source (per memory constraint).
    """
    
    # Sports URL mappings - VSIN uses sport-specific paths
    SPORTS_URLS = {
        'nfl': 'nfl',
        'nba': 'nba', 
        'mlb': 'mlb',
        'nhl': 'nhl',
        'cbb': 'cbb',
        'cfb': 'cfb',
        'wnba': 'wnba',
        'ufc': 'ufc',
        'pga': 'pga',
        'tennis': 'tennis',
        'epl': 'epl',
        'ufl': 'ufl'
    }
    
    # Sportsbook view parameters
    SPORTSBOOK_VIEWS = {
        'circa': 'circa',
        'dk': 'dk',
        'fanduel': 'fanduel',
        'mgm': 'mgm',
        'caesars': 'caesars'
    }
    
    def __init__(
        self,
        base_url: str = "https://data.vsin.com",
        default_sportsbook: str = "circa",  # Use Circa per memory constraint
        rate_limit_config: Optional[RateLimitConfig] = None,
        retry_config: Optional[RetryConfig] = None
    ) -> None:
        """
        Initialize VSIN scraper.
        
        Args:
            base_url: Base URL for VSIN
            default_sportsbook: Default sportsbook to use
            rate_limit_config: Rate limiting configuration
            retry_config: Retry behavior configuration
        """
        # VSIN-specific rate limiting (be respectful)
        vsin_rate_config = rate_limit_config or RateLimitConfig(
            requests_per_second=0.5,  # 2 seconds between requests
            requests_per_minute=15,   # Conservative limit
            burst_size=3
        )
        
        super().__init__(
            source_name="VSIN",
            rate_limit_config=vsin_rate_config,
            retry_config=retry_config,
            timeout=30.0
        )
        
        self.base_url = base_url
        self.default_sportsbook = default_sportsbook
        
        # Validate default sportsbook
        if default_sportsbook not in self.SPORTSBOOK_VIEWS:
            raise ValueError(f"Invalid sportsbook: {default_sportsbook}")
    
    def build_url(self, sport: str, sportsbook: Optional[str] = None) -> str:
        """
        Build the complete URL for scraping VSIN data.
        
        Args:
            sport: The sport to scrape (mlb, nfl, etc.)
            sportsbook: The sportsbook view (defaults to Circa)
            
        Returns:
            Complete URL for scraping
            
        Raises:
            ValueError: If sport is not supported
        """
        sport_lower = sport.lower()
        sportsbook = sportsbook or self.default_sportsbook
        
        sport_param = self.SPORTS_URLS.get(sport_lower)
        if not sport_param:
            raise ValueError(
                f"Sport '{sport}' not supported. "
                f"Available sports: {', '.join(self.SPORTS_URLS.keys())}"
            )
        
        sportsbook_param = self.SPORTSBOOK_VIEWS.get(sportsbook.lower())
        if not sportsbook_param:
            raise ValueError(
                f"Sportsbook '{sportsbook}' not supported. "
                f"Available sportsbooks: {', '.join(self.SPORTSBOOK_VIEWS.keys())}"
            )
        
        # Build URL with updated VSIN format from vsin_new_url_and_info.md
        # Both DK and Circa now use the format: /betting-splits/?bookid={book}&view={sport}
        url = f"{self.base_url}/betting-splits/?bookid={sportsbook_param}&view={sport_param}"
        
        self.logger.debug("Built VSIN URL", 
                         sport=sport, sportsbook=sportsbook, url=url)
        
        return url
    
    async def scrape_sport(
        self, 
        sport: str = "mlb",
        sportsbook: Optional[str] = None
    ) -> ScrapingResult:
        """
        Scrape betting splits for a specific sport.
        
        Args:
            sport: Sport to scrape (defaults to MLB)
            sportsbook: Sportsbook to use (defaults to Circa)
            
        Returns:
            ScrapingResult with scraped data
        """
        return await self.scrape(sport=sport, sportsbook=sportsbook)
    
    async def scrape(self, **kwargs: Any) -> ScrapingResult:
        """
        Scrape VSIN betting splits data.
        
        Args:
            sport: Sport to scrape (default: mlb)
            sportsbook: Sportsbook to use (default: circa)
            
        Returns:
            ScrapingResult containing scraped betting splits
        """
        sport = kwargs.get('sport', 'mlb')
        sportsbook = kwargs.get('sportsbook', self.default_sportsbook)
        
        errors = []
        data = []
        request_count = 0
        total_response_time = 0.0
        
        try:
            # Build URL
            url = self.build_url(sport, sportsbook)
            
            # Make request and get HTML
            self.logger.info("Starting VSIN scrape", 
                           sport=sport, sportsbook=sportsbook, url=url)
            
            start_time = datetime.now()
            soup = await self._get_soup(url)
            response_time = (datetime.now() - start_time).total_seconds() * 1000
            
            request_count = 1
            total_response_time = response_time
            
            # Extract main content
            main_content = self._extract_main_content(soup)
            if not main_content:
                error_msg = "Could not find main content container"
                errors.append(error_msg)
                self.logger.warning(error_msg)
                
                return self._create_result(
                    success=False,
                    data=[],
                    errors=errors,
                    metadata={"sport": sport, "sportsbook": sportsbook, "url": url},
                    request_count=request_count,
                    response_time_ms=total_response_time
                )
            
            # Parse betting splits from HTML
            betting_splits = self._parse_betting_splits(main_content, sport, sportsbook)
            
            if betting_splits:
                data = betting_splits
                self.logger.info("Successfully scraped VSIN data",
                               sport=sport,
                               sportsbook=sportsbook,
                               splits_count=len(betting_splits))
            else:
                error_msg = "No betting splits data found"
                errors.append(error_msg)
                self.logger.warning(error_msg, sport=sport, sportsbook=sportsbook)
            
            success = len(data) > 0
            
            return self._create_result(
                success=success,
                data=data,
                errors=errors,
                metadata={
                    "sport": sport,
                    "sportsbook": sportsbook,
                    "url": url,
                    "html_length": len(str(soup)),
                    "source": DataSource.VSIN.value
                },
                request_count=request_count,
                response_time_ms=total_response_time
            )
            
        except Exception as e:
            error_msg = f"VSIN scraping failed: {str(e)}"
            errors.append(error_msg)
            self.logger.error("VSIN scraping failed", 
                            sport=sport, sportsbook=sportsbook, error=str(e))
            
            return self._create_result(
                success=False,
                data=[],
                errors=errors,
                metadata={"sport": sport, "sportsbook": sportsbook},
                request_count=request_count,
                response_time_ms=total_response_time
            )
    
    def _extract_main_content(self, soup: BeautifulSoup) -> Optional[Tag]:
        """
        Extract the main content using the updated XPath structure for VSIN.
        
        Updated XPath based on user feedback:
        /html/body/div[6]/div[2]/div/div[3]/div[1]/div/div[1]/div[4]/main/div/table/tbody
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            Table element containing betting data or None if not found
        """
        try:
            # Navigate through the DOM structure based on the updated XPath
            body = soup.find('body')
            if not body:
                self.logger.warning("No body element found")
                return None
            
            # Get all direct div children of body and select the 6th one (index 5)
            body_divs = body.find_all('div', recursive=False)
            if len(body_divs) < 6:
                self.logger.debug(f"Not enough body divs found: {len(body_divs)}")
                # Fallback: look for any table with betting data
                return self._find_betting_table_fallback(soup)
            
            # Navigate: div[6]/div[2]/div/div[3]/div[1]/div/div[1]/div[4]/main/div/table
            current = body_divs[5]  # div[6] (0-indexed)
            
            # div[2]
            divs = current.find_all('div', recursive=False)
            if len(divs) < 2:
                return self._find_betting_table_fallback(soup)
            current = divs[1]
            
            # div
            div_child = current.find('div', recursive=False)
            if not div_child:
                return self._find_betting_table_fallback(soup)
            current = div_child
            
            # div[3]
            divs = current.find_all('div', recursive=False)
            if len(divs) < 3:
                return self._find_betting_table_fallback(soup)
            current = divs[2]
            
            # div[1]
            div_child = current.find('div', recursive=False)
            if not div_child:
                return self._find_betting_table_fallback(soup)
            current = div_child
            
            # div
            div_child = current.find('div', recursive=False)
            if not div_child:
                return self._find_betting_table_fallback(soup)
            current = div_child
            
            # div[1]
            div_child = current.find('div', recursive=False)
            if not div_child:
                return self._find_betting_table_fallback(soup)
            current = div_child
            
            # div[4]  # Updated from div[2] to div[4] based on new XPath
            divs = current.find_all('div', recursive=False)
            if len(divs) < 4:
                return self._find_betting_table_fallback(soup)
            current = divs[3]  # div[4] (0-indexed)
            
            # main
            main = current.find('main', recursive=False)
            if not main:
                return self._find_betting_table_fallback(soup)
            current = main
            
            # div
            div_child = current.find('div', recursive=False)
            if not div_child:
                return self._find_betting_table_fallback(soup)
            current = div_child
            
            # table
            table = current.find('table', recursive=False)
            if table:
                self.logger.debug("Found betting table using updated XPath navigation")
                return table
            
            # If we didn't find the table, try fallback
            return self._find_betting_table_fallback(soup)
            
        except Exception as e:
            self.logger.debug(f"Error navigating updated XPath structure: {e}")
            return self._find_betting_table_fallback(soup)
    
    def _find_betting_table_fallback(self, soup: BeautifulSoup) -> Optional[Tag]:
        """
        Fallback method to find betting table when XPath navigation fails.
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            Table element or None
        """
        # Look for tables with common VSIN classes
        table_selectors = [
            'table.freezetable',
            'table[class*="table"]',
            'table[class*="betting"]',
            'table[class*="splits"]'
        ]
        
        for selector in table_selectors:
            table = soup.select_one(selector)
            if table:
                self.logger.debug(f"Found table using fallback selector: {selector}")
                return table
        
        # Last resort: find any table with betting-like content
        tables = soup.find_all('table')
        for table in tables:
            table_text = table.get_text().lower()
            if any(keyword in table_text for keyword in ['%', 'line', 'odds', 'bet', 'handle']):
                self.logger.debug("Found table with betting content")
                return table
        
        self.logger.warning("No betting table found with any method")
        return None
    
    def _parse_betting_splits(
        self, 
        content: Tag, 
        sport: str, 
        sportsbook: str
    ) -> List[Dict[str, Any]]:
        """
        Parse betting splits data from HTML content.
        
        Args:
            content: HTML content element
            sport: Sport being parsed
            sportsbook: Sportsbook source
            
        Returns:
            List of parsed betting splits data
        """
        splits_data = []
        
        try:
            # Check if the content itself is a table
            tables = []
            if content.name == 'table':
                tables = [content]
            else:
                # Look for betting data tables within content
                tables = content.find_all('table')
            
            for table in tables:
                # Check if this looks like a betting table
                if not self._is_betting_table(table):
                    continue
                
                # Try two different parsing approaches:
                # 1. tbody structure (each game in separate row)
                # 2. freezetable structure (all games in one row)
                
                # Approach 1: Check for tbody with individual game rows
                tbody_splits = self._parse_tbody_structure(table, sport, sportsbook)
                if tbody_splits:
                    splits_data.extend(tbody_splits)
                    continue
                
                # Approach 2: Parse freezetable structure (all games in one row)
                freezetable_splits = self._parse_freezetable_structure(table, sport, sportsbook)
                if freezetable_splits:
                    splits_data.extend(freezetable_splits)
                    continue
            
            # If no tables found, look for alternative structures
            if not splits_data:
                splits_data = self._parse_alternative_structure(content, sport, sportsbook)
            
        except Exception as e:
            self.logger.error("Error parsing betting splits", error=str(e))
        
        return splits_data
    
    def _parse_tbody_structure(self, table: Tag, sport: str, sportsbook: str) -> List[Dict[str, Any]]:
        """
        Parse tbody structure where each game is in a separate row.
        
        Args:
            table: Table element
            sport: Sport being parsed
            sportsbook: Sportsbook source
            
        Returns:
            List of parsed betting splits data
        """
        splits_data = []
        
        try:
            # Look for tbody elements
            tbodies = table.find_all('tbody')
            
            for tbody in tbodies:
                rows = tbody.find_all('tr')
                
                # Parse each row as a separate game
                for row in rows:
                    split_data = self._parse_betting_row(row, [], sport, sportsbook)
                    if split_data:
                        splits_data.append(split_data)
            
        except Exception as e:
            self.logger.debug("Error parsing tbody structure", error=str(e))
        
        return splits_data
    
    def _parse_freezetable_structure(self, table: Tag, sport: str, sportsbook: str) -> List[Dict[str, Any]]:
        """
        Parse freezetable structure where all games are in one row with many cells.
        
        Args:
            table: Table element
            sport: Sport being parsed
            sportsbook: Sportsbook source
            
        Returns:
            List of parsed betting splits data
        """
        splits_data = []
        
        try:
            # Check if this is a freezetable
            table_classes = table.get('class', [])
            if 'freezetable' not in table_classes:
                return splits_data
            
            # Get all rows
            rows = table.find_all('tr')
            
            # Find the row with the most team links (contains all game data)
            best_row = None
            max_team_links = 0
            
            for row in rows:
                team_links = row.find_all('a', href=True)
                mlb_team_links = [link for link in team_links if '/mlb/teams/' in link.get('href', '')]
                
                if len(mlb_team_links) > max_team_links:
                    max_team_links = len(mlb_team_links)
                    best_row = row
            
            if not best_row or max_team_links < 4:  # Need at least 2 games (4 teams)
                return splits_data
            
            self.logger.info("Found freezetable data row", 
                           team_links=max_team_links, 
                           expected_games=max_team_links // 2)
            
            # Parse multiple games from this single row
            splits_data = self._parse_multiple_games_from_row(best_row, sport, sportsbook)
            
        except Exception as e:
            self.logger.debug("Error parsing freezetable structure", error=str(e))
        
        return splits_data
    
    def _parse_multiple_games_from_row(self, row: Tag, sport: str, sportsbook: str) -> List[Dict[str, Any]]:
        """
        Parse multiple games from a single freezetable row.
        
        In VSIN's freezetable, all games are in one row with each game occupying
        approximately 10 cells (based on analysis showing games at cell 0, 10, 20, 30, etc.)
        
        Args:
            row: Table row element containing multiple games
            sport: Sport being parsed  
            sportsbook: Sportsbook source
            
        Returns:
            List of parsed betting splits data for all games
        """
        splits_data = []
        
        try:
            cells = row.find_all(['td', 'th'])
            
            # Find all cells that contain team links
            game_cells = []
            for i, cell in enumerate(cells):
                team_links = cell.find_all('a', href=True)
                mlb_team_links = [link for link in team_links if '/mlb/teams/' in link.get('href', '')]
                
                if len(mlb_team_links) >= 2:  # Cell contains a complete game (2 teams)
                    game_cells.append((i, cell, mlb_team_links))
            
            self.logger.info("Found game cells in freezetable row", 
                           total_cells=len(cells),
                           game_cells=len(game_cells))
            
            # Parse each game
            for cell_index, cell, team_links in game_cells:
                try:
                    # Extract team names
                    team_names = [link.get_text(strip=True) for link in team_links[:2]]
                    away_team = team_names[0]
                    home_team = team_names[1]
                    
                    # For freezetable, we need to extract data from multiple cells
                    # Based on analysis: game data spans ~10 cells starting from the team cell
                    game_data = self._extract_game_data_from_freezetable(
                        cells, cell_index, away_team, home_team, sport, sportsbook
                    )
                    
                    if game_data:
                        splits_data.append(game_data)
                        
                except Exception as e:
                    self.logger.debug("Error parsing individual game from freezetable", 
                                    cell_index=cell_index, error=str(e))
                    continue
            
        except Exception as e:
            self.logger.error("Error parsing multiple games from row", error=str(e))
        
        return splits_data
    
    def _extract_game_data_from_freezetable(
        self, 
        cells: List[Tag], 
        start_cell: int, 
        away_team: str, 
        home_team: str,
        sport: str,
        sportsbook: str
    ) -> Optional[Dict[str, Any]]:
        """
        Extract betting data for a single game from freezetable cells.
        
        Args:
            cells: All cells in the row
            start_cell: Index of cell containing team names
            away_team: Away team name
            home_team: Home team name
            sport: Sport being parsed
            sportsbook: Sportsbook source
            
        Returns:
            Parsed betting data or None if extraction fails
        """
        try:
            # Based on analysis, each game spans exactly 10 cells:
            # Cell 0: Teams, Cell 1: Moneyline, Cell 2: ML Handle %, Cell 3: ML Bet %
            # Cell 4: Total line, Cell 5: Total Handle %, Cell 6: Total Bet %
            # Cell 7: Spread/RL, Cell 8: Spread Handle %, Cell 9: Spread Bet %
            end_cell = min(start_cell + 10, len(cells))
            game_cells = cells[start_cell:end_cell]
            
            # Extract moneyline odds (cell 1 relative to team cell)
            away_line = None
            home_line = None
            
            if len(game_cells) > 1:
                moneyline_cell = game_cells[1]
                
                # Try to find odds in links (for DK) or divs (for Circa)
                odds_links = moneyline_cell.find_all('a')
                if len(odds_links) >= 2:
                    away_line = odds_links[0].get_text(strip=True)
                    home_line = odds_links[1].get_text(strip=True)
                else:
                    odds_divs = moneyline_cell.find_all('div', class_='scorebox_highlight')
                    if len(odds_divs) >= 2:
                        away_line = odds_divs[0].get_text(strip=True)
                        home_line = odds_divs[1].get_text(strip=True)
                
                # Fix missing + signs for positive odds
                if away_line and away_line.isdigit():
                    away_line = '+' + away_line
                if home_line and home_line.isdigit():
                    home_line = '+' + home_line
            
            # Extract handle percentages (cell 2)
            away_handle = None
            home_handle = None
            
            if len(game_cells) > 2:
                handle_cell = game_cells[2]
                handle_text = handle_cell.get_text(strip=True)
                
                import re
                # Handle text format: "19%81%-"
                handle_matches = re.findall(r'(\d+)%', handle_text)
                if len(handle_matches) >= 2:
                    away_handle = handle_matches[0] + '%'
                    home_handle = handle_matches[1] + '%'
            
            # Extract bet percentages (cell 3)
            away_bets = None
            home_bets = None
            
            if len(game_cells) > 3:
                bet_cell = game_cells[3]
                bet_text = bet_cell.get_text(strip=True)
                
                # Bet text format: "22%78%-"
                bet_matches = re.findall(r'(\d+)%', bet_text)
                if len(bet_matches) >= 2:
                    away_bets = bet_matches[0] + '%'
                    home_bets = bet_matches[1] + '%'
            
            # Extract total line (cell 4)
            total_line = None
            
            if len(game_cells) > 4:
                total_cell = game_cells[4]
                
                # Look for total in scorebox_highlight div
                total_div = total_cell.find('div', class_='scorebox_highlight')
                if total_div:
                    total_text = total_div.get_text(strip=True)
                    # Total should be a number like 8.5, 9, 10.5, etc.
                    total_match = re.search(r'(\d+(?:\.\d+)?)', total_text)
                    if total_match:
                        total_line = total_match.group(1)
                        self.logger.debug("Found total line in freezetable", total=total_line, cell_text=total_text)
            
            # Extract total handle percentages (cell 5) 
            over_handle = None
            under_handle = None
            
            if len(game_cells) > 5:
                total_handle_cell = game_cells[5]
                total_handle_text = total_handle_cell.get_text(strip=True)
                
                # Handle text format: "75%25%-"
                total_handle_matches = re.findall(r'(\d+)%', total_handle_text)
                if len(total_handle_matches) >= 2:
                    over_handle = total_handle_matches[0] + '%'
                    under_handle = total_handle_matches[1] + '%'
            
            # Extract total bet percentages (cell 6)
            over_bets = None
            under_bets = None
            
            if len(game_cells) > 6:
                total_bet_cell = game_cells[6]
                total_bet_text = total_bet_cell.get_text(strip=True)
                
                # Bet text format: "80%20%-"
                total_bet_matches = re.findall(r'(\d+)%', total_bet_text)
                if len(total_bet_matches) >= 2:
                    over_bets = total_bet_matches[0] + '%'
                    under_bets = total_bet_matches[1] + '%'
            
            # Extract spread data (cell 7)
            away_spread = None
            home_spread = None
            
            if len(game_cells) > 7:
                spread_cell = game_cells[7]
                
                # Try to find spread in links (for DK) or divs (for Circa)
                spread_links = spread_cell.find_all('a')
                if len(spread_links) >= 2:
                    away_spread = spread_links[0].get_text(strip=True)
                    home_spread = spread_links[1].get_text(strip=True)
                else:
                    spread_divs = spread_cell.find_all('div', class_='scorebox_highlight')
                    if len(spread_divs) >= 2:
                        away_spread = spread_divs[0].get_text(strip=True)
                        home_spread = spread_divs[1].get_text(strip=True)
            
            # Extract spread handle percentages (cell 8)
            away_spread_handle = None
            home_spread_handle = None
            
            if len(game_cells) > 8:
                spread_handle_cell = game_cells[8]
                spread_handle_text = spread_handle_cell.get_text(strip=True)
                
                # Handle text format: "4%96%-"
                spread_handle_matches = re.findall(r'(\d+)%', spread_handle_text)
                if len(spread_handle_matches) >= 2:
                    away_spread_handle = spread_handle_matches[0] + '%'
                    home_spread_handle = spread_handle_matches[1] + '%'
            
            # Extract spread bet percentages (cell 9)
            away_spread_bets = None
            home_spread_bets = None
            
            if len(game_cells) > 9:
                spread_bet_cell = game_cells[9]
                spread_bet_text = spread_bet_cell.get_text(strip=True)
                
                # Bet text format: "25%75%-"
                spread_bet_matches = re.findall(r'(\d+)%', spread_bet_text)
                if len(spread_bet_matches) >= 2:
                    away_spread_bets = spread_bet_matches[0] + '%'
                    home_spread_bets = spread_bet_matches[1] + '%'
            
            # Create betting data structure
            betting_data = {
                'Game': f"{away_team} @ {home_team}",
                'Away Team': away_team,
                'Home Team': home_team,
                'source': DataSource.VSIN.value,
                'book': self._normalize_sportsbook(sportsbook),
                'sport': sport,
                'scraped_at': datetime.now().isoformat()
            }
            
            # Add moneyline data if available
            if away_line:
                betting_data['Away Line'] = away_line
            if home_line:
                betting_data['Home Line'] = home_line
            if away_handle:
                betting_data['Away Handle %'] = away_handle
            if home_handle:
                betting_data['Home Handle %'] = home_handle
            if away_bets:
                betting_data['Away Bets %'] = away_bets
            if home_bets:
                betting_data['Home Bets %'] = home_bets
            
            # Add total data if available
            if total_line:
                betting_data['Total'] = total_line
            if over_handle:
                betting_data['Over Handle %'] = over_handle
            if under_handle:
                betting_data['Under Handle %'] = under_handle
            if over_bets:
                betting_data['Over Bets %'] = over_bets
            if under_bets:
                betting_data['Under Bets %'] = under_bets
            
            # Add spread data if available
            if away_spread:
                betting_data['Away Spread'] = away_spread
            if home_spread:
                betting_data['Home Spread'] = home_spread
            if away_spread_handle:
                betting_data['Away Spread Handle %'] = away_spread_handle
            if home_spread_handle:
                betting_data['Home Spread Handle %'] = home_spread_handle
            if away_spread_bets:
                betting_data['Away Spread Bets %'] = away_spread_bets
            if home_spread_bets:
                betting_data['Home Spread Bets %'] = home_spread_bets
            
            # Only return if we have meaningful betting data
            has_meaningful_data = any([away_line, home_line, away_handle, home_handle, away_spread, home_spread, total_line])
            
            if has_meaningful_data:
                self.logger.debug("Successfully extracted freezetable game data", 
                                away_team=away_team, home_team=home_team,
                                away_line=away_line, home_line=home_line,
                                away_handle=away_handle, home_handle=home_handle,
                                away_spread=away_spread, home_spread=home_spread,
                                total_line=total_line)
            
            return betting_data if has_meaningful_data else None
            
        except Exception as e:
            self.logger.debug("Error extracting game data from freezetable", error=str(e))
            return None
    
    def _is_betting_table(self, table: Tag) -> bool:
        """
        Check if a table contains betting data.
        
        Args:
            table: Table element
            
        Returns:
            True if table appears to contain betting data
        """
        # Look for VSIN-specific betting table classes
        table_classes = table.get('class', [])
        
        # VSIN uses "freezetable table table-sm mb-0" for their betting splits table
        if 'freezetable' in table_classes and 'table' in table_classes:
            return True
        
        # Fallback: check for other betting-related classes
        betting_classes = ['betting', 'splits', 'freeze', 'odds']
        if any(cls in str(table_classes).lower() for cls in betting_classes):
            return True
        
        # Check for links to team pages (strong indicator of betting table)
        team_links = table.find_all('a', href=True)
        mlb_team_links = [link for link in team_links if '/mlb/teams/' in link.get('href', '')]
        if len(mlb_team_links) >= 4:  # Should have multiple team links
            return True
        
        # Check table content for betting-related terms and team names
        table_text = table.get_text().lower()
        betting_terms = ['spread', 'total', 'moneyline', 'bets', 'stake', '%', 'handle']
        team_indicators = ['yankees', 'dodgers', 'red sox', 'giants', 'cubs', 'cardinals', 
                          'phillies', 'marlins', 'pirates', 'tigers']
        
        has_betting_terms = any(term in table_text for term in betting_terms)
        has_team_data = any(team in table_text for team in team_indicators)
        
        return has_betting_terms and has_team_data
    
    def _extract_headers(self, header_row: Optional[Tag]) -> List[str]:
        """
        Extract column headers from table header row.
        
        Args:
            header_row: Header row element
            
        Returns:
            List of column header names
        """
        if not header_row:
            return []
        
        headers = []
        for cell in header_row.find_all(['th', 'td']):
            header_text = cell.get_text(strip=True)
            headers.append(header_text)
        
        return headers
    
    def _parse_betting_row(
        self, 
        row: Tag, 
        headers: List[str], 
        sport: str, 
        sportsbook: str
    ) -> Optional[Dict[str, Any]]:
        """
        Parse a single betting data row from VSIN table.
        
        Updated structure based on vsin_new_url_and_info.md:
        - Cell 0: Team names and links with <hr> separator
        - Cell 1: Moneyline odds (away/home) with <hr> separator
        - Cell 2: Handle percentages with <hr> separator  
        - Cell 3: Bet percentages with <hr> separator
        - Additional cells for spreads, totals, etc.
        
        Args:
            row: Table row element
            headers: Column headers
            sport: Sport being parsed
            sportsbook: Sportsbook source
            
        Returns:
            Parsed betting data or None if invalid
        """
        try:
            cells = row.find_all(['td', 'th'])
            
            # Skip header rows - data rows should have at least 10 cells
            if len(cells) < 10:
                return None
            
            # Extract team information from first cell (cell 0)
            first_cell = cells[0]
            team_links = first_cell.find_all('a', href=True)
            
            # Find team names from links that have "/mlb/teams/" in href
            team_names = []
            for link in team_links:
                href = link.get('href', '')
                if '/mlb/teams/' in href:
                    link_text = link.get_text(strip=True)
                    if link_text and len(link_text) > 3:
                        team_names.append(link_text)
            
            if len(team_names) < 2:
                self.logger.debug("Could not find two team names", team_names=team_names)
                return None
            
            away_team = team_names[0]  # First team is away
            home_team = team_names[1]  # Second team is home
            
            # Extract moneyline odds from second cell (cell 1)
            moneyline_cell = cells[1]
            
            # Look for odds within divs or links, separated by <hr>
            away_line = None
            home_line = None
            
            # Try to find odds in links first (for DK)
            odds_links = moneyline_cell.find_all('a')
            if len(odds_links) >= 2:
                away_line = odds_links[0].get_text(strip=True)
                home_line = odds_links[1].get_text(strip=True)
            else:
                # Try to find odds in divs (for Circa)
                odds_divs = moneyline_cell.find_all('div', class_='scorebox_highlight')
                if len(odds_divs) >= 2:
                    away_line = odds_divs[0].get_text(strip=True)
                    home_line = odds_divs[1].get_text(strip=True)
            
            # Extract handle percentages from third cell (cell 2)
            handle_cell = cells[2]
            handle_divs = handle_cell.find_all('div')
            
            import re
            away_handle = None
            home_handle = None
            
            if len(handle_divs) >= 2:
                # First div is typically away team, second is home team
                away_handle_text = handle_divs[0].get_text(strip=True)
                home_handle_text = handle_divs[1].get_text(strip=True)
                
                away_handle_match = re.search(r'(\d+)%', away_handle_text)
                home_handle_match = re.search(r'(\d+)%', home_handle_text)
                
                if away_handle_match:
                    away_handle = away_handle_match.group(1) + '%'
                if home_handle_match:
                    home_handle = home_handle_match.group(1) + '%'
            
            # Extract bet percentages from fourth cell (cell 3)
            bet_cell = cells[3] if len(cells) > 3 else None
            away_bets = None
            home_bets = None
            
            if bet_cell:
                bet_divs = bet_cell.find_all('div')
                if len(bet_divs) >= 2:
                    away_bets_text = bet_divs[0].get_text(strip=True)
                    home_bets_text = bet_divs[1].get_text(strip=True)
                    
                    away_bets_match = re.search(r'(\d+)%', away_bets_text)
                    home_bets_match = re.search(r'(\d+)%', home_bets_text)
                    
                    if away_bets_match:
                        away_bets = away_bets_match.group(1) + '%'
                    if home_bets_match:
                        home_bets = home_bets_match.group(1) + '%'
            
            # Extract total (over/under) data from fifth cell (cell 4, index 4)
            # Based on user HTML: <div class="scorebox_highlight text-center game_highlight_dark">9.5</div>
            total_cell = cells[4] if len(cells) > 4 else None
            total_line = None
            
            if total_cell:
                # Look for the total line in scorebox_highlight div
                total_div = total_cell.find('div', class_='scorebox_highlight')
                if total_div:
                    total_text = total_div.get_text(strip=True)
                    # Total should be a number like 8.5, 9, 10.5, etc.
                    total_match = re.search(r'(\d+(?:\.\d+)?)', total_text)
                    if total_match:
                        total_line = total_match.group(1)
                        self.logger.debug("Found total line", total=total_line, cell_text=total_text)
            
            # Extract total handle percentages from sixth cell (cell 5, index 5) 
            total_handle_cell = cells[5] if len(cells) > 5 else None
            over_handle = None
            under_handle = None
            
            if total_handle_cell:
                total_handle_divs = total_handle_cell.find_all('div')
                if len(total_handle_divs) >= 2:
                    over_handle_text = total_handle_divs[0].get_text(strip=True)
                    under_handle_text = total_handle_divs[1].get_text(strip=True)
                    
                    over_handle_match = re.search(r'(\d+)%', over_handle_text)
                    under_handle_match = re.search(r'(\d+)%', under_handle_text)
                    
                    if over_handle_match:
                        over_handle = over_handle_match.group(1) + '%'
                    if under_handle_match:
                        under_handle = under_handle_match.group(1) + '%'
            
            # Extract total bet percentages from seventh cell (cell 6, index 6)
            total_bet_cell = cells[6] if len(cells) > 6 else None
            over_bets = None
            under_bets = None
            
            if total_bet_cell:
                total_bet_divs = total_bet_cell.find_all('div')
                if len(total_bet_divs) >= 2:
                    over_bets_text = total_bet_divs[0].get_text(strip=True)
                    under_bets_text = total_bet_divs[1].get_text(strip=True)
                    
                    over_bets_match = re.search(r'(\d+)%', over_bets_text)
                    under_bets_match = re.search(r'(\d+)%', under_bets_text)
                    
                    if over_bets_match:
                        over_bets = over_bets_match.group(1) + '%'
                    if under_bets_match:
                        under_bets = under_bets_match.group(1) + '%'
            
            # Extract spread/run line data from eighth cell (cell 7, index 7)
            # Based on HTML structure: RL column contains spread like -1.5/+1.5
            spread_cell = cells[7] if len(cells) > 7 else None
            away_spread = None
            home_spread = None
            
            if spread_cell:
                spread_divs = spread_cell.find_all('div', class_='scorebox_highlight')
                if len(spread_divs) >= 2:
                    away_spread = spread_divs[0].get_text(strip=True)
                    home_spread = spread_divs[1].get_text(strip=True)
            
            # Extract spread handle percentages from ninth cell (cell 8, index 8)
            spread_handle_cell = cells[8] if len(cells) > 8 else None
            away_spread_handle = None
            home_spread_handle = None
            
            if spread_handle_cell:
                spread_handle_divs = spread_handle_cell.find_all('div')
                if len(spread_handle_divs) >= 2:
                    away_spread_handle_text = spread_handle_divs[0].get_text(strip=True)
                    home_spread_handle_text = spread_handle_divs[1].get_text(strip=True)
                    
                    away_spread_handle_match = re.search(r'(\d+)%', away_spread_handle_text)
                    home_spread_handle_match = re.search(r'(\d+)%', home_spread_handle_text)
                    
                    if away_spread_handle_match:
                        away_spread_handle = away_spread_handle_match.group(1) + '%'
                    if home_spread_handle_match:
                        home_spread_handle = home_spread_handle_match.group(1) + '%'
            
            # Extract spread bet percentages from tenth cell (cell 9, index 9)
            spread_bet_cell = cells[9] if len(cells) > 9 else None
            away_spread_bets = None
            home_spread_bets = None
            
            if spread_bet_cell:
                spread_bet_divs = spread_bet_cell.find_all('div')
                if len(spread_bet_divs) >= 2:
                    away_spread_bets_text = spread_bet_divs[0].get_text(strip=True)
                    home_spread_bets_text = spread_bet_divs[1].get_text(strip=True)
                    
                    away_spread_bets_match = re.search(r'(\d+)%', away_spread_bets_text)
                    home_spread_bets_match = re.search(r'(\d+)%', home_spread_bets_text)
                    
                    if away_spread_bets_match:
                        away_spread_bets = away_spread_bets_match.group(1) + '%'
                    if home_spread_bets_match:
                        home_spread_bets = home_spread_bets_match.group(1) + '%'
            
            # Create betting data structure
            betting_data = {
                'Game': f"{away_team} @ {home_team}",
                'Away Team': away_team,
                'Home Team': home_team,
                'source': DataSource.VSIN.value,
                'book': self._normalize_sportsbook(sportsbook),
                'sport': sport,
                'scraped_at': datetime.now().isoformat()
            }
            
            # Add moneyline data if available
            if away_line:
                betting_data['Away Line'] = away_line
            if home_line:
                betting_data['Home Line'] = home_line
            
            # Add handle data if available
            if away_handle:
                betting_data['Away Handle %'] = away_handle
            if home_handle:
                betting_data['Home Handle %'] = home_handle
            
            # Add bet percentage data if available
            if away_bets:
                betting_data['Away Bets %'] = away_bets
            if home_bets:
                betting_data['Home Bets %'] = home_bets
            
            # Add spread data if available
            if away_spread:
                betting_data['Away Spread'] = away_spread
            if home_spread:
                betting_data['Home Spread'] = home_spread
            
            # Add spread handle data if available
            if away_spread_handle:
                betting_data['Away Spread Handle %'] = away_spread_handle
            if home_spread_handle:
                betting_data['Home Spread Handle %'] = home_spread_handle
            
            # Add total data if available
            if total_line:
                betting_data['Total'] = total_line
            if over_handle:
                betting_data['Over Handle %'] = over_handle
            if under_handle:
                betting_data['Under Handle %'] = under_handle
            if over_bets:
                betting_data['Over Bets %'] = over_bets
            if under_bets:
                betting_data['Under Bets %'] = under_bets
            
            # Add spread bet percentage data if available
            if away_spread_bets:
                betting_data['Away Spread Bets %'] = away_spread_bets
            if home_spread_bets:
                betting_data['Home Spread Bets %'] = home_spread_bets
            
            # Only return if we have meaningful betting data
            has_meaningful_data = any([away_line, home_line, away_handle, home_handle, away_spread, home_spread, total_line])
            
            if has_meaningful_data:
                self.logger.debug("Successfully parsed VSIN row", 
                                away_team=away_team, home_team=home_team,
                                away_line=away_line, home_line=home_line,
                                away_handle=away_handle, home_handle=home_handle,
                                away_spread=away_spread, home_spread=home_spread,
                                total_line=total_line)
            
            return betting_data if has_meaningful_data else None
            
        except Exception as e:
            self.logger.debug("Error parsing VSIN betting row", error=str(e))
            return None
    
    def _parse_alternative_structure(
        self, 
        content: Tag, 
        sport: str, 
        sportsbook: str
    ) -> List[Dict[str, Any]]:
        """
        Parse betting data from alternative HTML structures.
        
        Args:
            content: HTML content element
            sport: Sport being parsed
            sportsbook: Sportsbook source
            
        Returns:
            List of parsed betting data
        """
        # This method can be expanded to handle different HTML structures
        # For now, return empty list if standard table parsing fails
        self.logger.debug("Attempting alternative structure parsing")
        return []
    
    def _normalize_sportsbook(self, sportsbook: str) -> str:
        """
        Normalize sportsbook name to BookType enum value.
        
        Args:
            sportsbook: Raw sportsbook name
            
        Returns:
            Normalized sportsbook name
        """
        sportsbook_mapping = {
            'circa': BookType.CIRCA.value,
            'dk': BookType.DRAFTKINGS.value,
            'fanduel': BookType.FANDUEL.value,
            'mgm': BookType.BETMGM.value,
            'caesars': BookType.CAESARS.value,
        }
        
        return sportsbook_mapping.get(sportsbook.lower(), sportsbook)
    
    async def get_available_sports(self) -> List[str]:
        """
        Get list of available sports for scraping.
        
        Returns:
            List of available sport codes
        """
        return list(self.SPORTS_URLS.keys())
    
    async def get_available_sportsbooks(self) -> List[str]:
        """
        Get list of available sportsbooks.
        
        Returns:
            List of available sportsbook codes
        """
        return list(self.SPORTSBOOK_VIEWS.keys())
    
    async def validate_sport_sportsbook(self, sport: str, sportsbook: str) -> bool:
        """
        Validate sport and sportsbook combination.
        
        Args:
            sport: Sport code
            sportsbook: Sportsbook code
            
        Returns:
            True if combination is valid
        """
        return (sport.lower() in self.SPORTS_URLS and 
                sportsbook.lower() in self.SPORTSBOOK_VIEWS)


# Convenience function
async def scrape_vsin_mlb(sportsbook: str = "circa") -> ScrapingResult:
    """
    Convenience function to scrape MLB data from VSIN.
    
    Args:
        sportsbook: Sportsbook to use (defaults to Circa per memory constraint)
        
    Returns:
        ScrapingResult with MLB betting splits
    """
    async with VSINScraper() as scraper:
        return await scraper.scrape_sport("mlb", sportsbook) 