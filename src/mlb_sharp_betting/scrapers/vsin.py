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
        
        # Build URL with VSIN format
        # DK uses base URL without view parameter, others use ?view={sportsbook}
        if sportsbook_param == 'dk':
            url = f"{self.base_url}/{sport_param}/betting-splits/"
        else:
            url = f"{self.base_url}/{sport_param}/betting-splits/?view={sportsbook_param}"
        
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
        Extract the main content using the specific XPath structure for VSIN.
        
        XPath: /html/body/div[6]/div[2]/div/div[3]/div[1]/div/div[1]/div[2]/div/table/tbody[1]/tr[1]
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            Table element containing betting data or None if not found
        """
        # Convert XPath to CSS selector approach
        # /html/body/div[6]/div[2]/div/div[3]/div[1]/div/div[1]/div[2]/div/table/tbody[1]
        
        try:
            # Navigate through the DOM structure based on the XPath
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
            
            # Navigate: div[6]/div[2]/div/div[3]/div[1]/div/div[1]/div[2]/div/table
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
            
            # table
            table = current.find('table', recursive=False)
            if table:
                self.logger.debug("Found betting table using XPath navigation")
                return table
            
            # If we didn't find the table, try fallback
            return self._find_betting_table_fallback(soup)
            
        except Exception as e:
            self.logger.debug(f"Error navigating XPath structure: {e}")
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
                
                # Parse rows from this table
                rows = table.find_all('tr')
                
                # For VSIN freezetable, skip the first 2 header rows
                data_rows = []
                for i, row in enumerate(rows):
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 100:  # Data rows have 100+ cells
                        data_rows.append(row)
                
                # Parse data rows
                for row in data_rows:
                    split_data = self._parse_betting_row(row, [], sport, sportsbook)
                    if split_data:
                        splits_data.append(split_data)
            
            # If no tables found, look for alternative structures
            if not splits_data:
                splits_data = self._parse_alternative_structure(content, sport, sportsbook)
            
        except Exception as e:
            self.logger.error("Error parsing betting splits", error=str(e))
        
        return splits_data
    
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
        
        # Check table content for betting-related terms and team names
        table_text = table.get_text().lower()
        betting_terms = ['spread', 'total', 'moneyline', 'bets', 'stake', '%', 'handle']
        team_indicators = ['yankees', 'dodgers', 'red sox', 'giants', 'cubs', 'cardinals']
        
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
        Parse a single betting data row from VSIN freezetable.
        
        VSIN structure:
        - Cell 1: "TeamATeamBHistory..." with team links
        - Cell 2: Moneyline odds like "-175+143" 
        - Cell 3: Handle percentages like "87%13%-"
        - Additional cells contain more betting data
        
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
            
            # Skip header rows (first two rows have different structure)
            if len(cells) < 100:  # Data rows have 250+ cells
                return None
            
            # Extract team information from first cell
            first_cell = cells[0]
            team_links = first_cell.find_all('a')
            
            # Find team names from links (skip empty links)
            team_names = []
            for link in team_links:
                link_text = link.get_text(strip=True)
                if link_text and len(link_text) > 3 and link_text not in ['History', 'VSiN Picks']:
                    # Filter out non-team links
                    if not any(keyword in link_text.lower() for keyword in ['history', 'pick', 'vsin']):
                        team_names.append(link_text)
            
            if len(team_names) < 2:
                return None
            
            away_team = team_names[0]
            home_team = team_names[1]
            
            # Extract moneyline odds from second cell
            moneyline_cell = cells[1]
            moneyline_links = moneyline_cell.find_all('a')
            
            away_line = None
            home_line = None
            
            if len(moneyline_links) >= 2:
                away_line = moneyline_links[0].get_text(strip=True)
                home_line = moneyline_links[1].get_text(strip=True)
            
            # Extract handle percentages from third cell
            handle_cell = cells[2]
            handle_text = handle_cell.get_text(strip=True)
            
            # Parse handle percentages (format: "87%13%-")
            import re
            handle_percentages = re.findall(r'(\d+)%', handle_text)
            
            away_handle = None
            home_handle = None
            
            if len(handle_percentages) >= 2:
                home_handle = f"{handle_percentages[0]}%"  # First percentage is usually home
                away_handle = f"{handle_percentages[1]}%"  # Second is away
            
            # Look for bet percentages in subsequent cells
            away_bets = None
            home_bets = None
            
            # Check cells 3-10 for bet percentages
            for i in range(3, min(10, len(cells))):
                cell_text = cells[i].get_text(strip=True)
                if '%' in cell_text:
                    bet_percentages = re.findall(r'(\d+)%', cell_text)
                    if len(bet_percentages) >= 2:
                        home_bets = f"{bet_percentages[0]}%"
                        away_bets = f"{bet_percentages[1]}%"
                        break
            
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
            
            # Only return if we have meaningful betting data
            has_meaningful_data = any([away_line, home_line, away_handle, home_handle])
            
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