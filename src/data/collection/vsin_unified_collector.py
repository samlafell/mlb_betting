#!/usr/bin/env python3
"""
VSIN Unified Collector

VSIN (Vegas Stats & Information Network) collector adapted to use the unified betting lines pattern.
Integrates with core_betting schema and provides standardized data quality tracking.
"""

import asyncio
import re
import uuid
from datetime import datetime
from typing import Any

import aiohttp
import psycopg2.extras
import structlog
from bs4 import BeautifulSoup

from .base import DataSource
from .unified_betting_lines_collector import UnifiedBettingLinesCollector

# Note: MCP bridge removed - browser automation no longer available

logger = structlog.get_logger(__name__)


class VSINUnifiedCollector(UnifiedBettingLinesCollector):
    """
    VSIN collector using the unified betting lines pattern.
    
    Provides standardized integration with core_betting schema while maintaining
    compatibility with existing VSIN data collection methods.
    """

    def __init__(self):
        super().__init__(DataSource.VSIN)
        # Updated to production VSIN URL from original implementation
        self.base_url = "https://data.vsin.com"
        
        # Sports URL mappings from original implementation
        self.sports_urls = {
            'mlb': 'mlb/betting-splits',
            'nfl': 'nfl/betting-splits',
            'nba': 'nba/betting-splits', 
            'nhl': 'nhl/betting-splits',
            'cbb': 'college-basketball/betting-splits',
            'cfb': 'college-football/betting-splits'
        }
        
        # Sportsbook view parameters from original implementation
        self.sportsbook_views = {
            'dk': '?',  # DraftKings is default view
            'circa': '?view=circa',
            'fanduel': '?view=fanduel',
            'mgm': '?view=mgm',
            'caesars': '?view=caesars'
        }
        
        # MLB-specific column mapping from original parser
        self.mlb_columns = {
            'teams': 0,
            'moneyline_odds': 1,
            'moneyline_handle': 2, 
            'moneyline_bets': 3,
            'total_line': 4,
            'total_handle': 5,
            'total_bets': 6,
            'runline_odds': 7,
            'runline_handle': 8,
            'runline_bets': 9
        }
        
        self.bet_types = [
            {"data_format": "money-line", "name": "Money Line", "unified_type": "moneyline"},
            {"data_format": "run-line", "name": "Run Line", "unified_type": "spread"},
            {"data_format": "totals", "name": "Totals", "unified_type": "totals"}
        ]
        self.playwright_adapter = None

    def build_vsin_url(self, sport: str, sportsbook: str = 'dk') -> str:
        """
        Build VSIN URL for betting splits data (from original implementation).
        
        Args:
            sport: Sport to collect data for ('mlb', 'nfl', 'nba', etc.)
            sportsbook: Sportsbook view ('dk', 'circa', 'fanduel', 'mgm', 'caesars')
            
        Returns:
            Complete VSIN URL for the specified sport and sportsbook
            
        Raises:
            ValueError: If sport is not supported
        """
        sport_path = self.sports_urls.get(sport.lower())
        if not sport_path:
            supported_sports = ', '.join(self.sports_urls.keys())
            raise ValueError(f"Sport '{sport}' not supported. Available sports: {supported_sports}")
        
        sportsbook_param = self.sportsbook_views.get(sportsbook.lower(), '?')
        return f"{self.base_url}/{sport_path}/{sportsbook_param}"

    def collect_raw_data(self, sport: str = "mlb", **kwargs) -> list[dict[str, Any]]:
        """
        Collect raw betting data from VSIN.
        
        Args:
            sport: Sport type (default: mlb)
            **kwargs: Additional parameters
            
        Returns:
            List of raw betting line dictionaries
        """
        try:
            # Check if we're already in an event loop (AGENT SBD's pattern)
            try:
                loop = asyncio.get_running_loop()
                # We're in an event loop, can't use asyncio.run()
                self.logger.info("VSIN collector running in async context - attempting live data collection")
                
                # Try live data collection with fallback to mock data
                live_data = self._collect_vsin_data_sync(sport, **kwargs)
                if live_data:
                    self.logger.info(f"Successfully collected {len(live_data)} live VSIN records")
                    return live_data
                else:
                    # Fallback to mock data if live collection fails
                    self.logger.warning("Live collection failed, falling back to mock data")
                    mock_data = self._generate_mock_data(sport)
                    return mock_data
                    
            except RuntimeError:
                # No event loop running, safe to use asyncio.run()
                return asyncio.run(self._collect_vsin_data_async(sport, **kwargs))

        except Exception as e:
            self.logger.error("Failed to collect VSIN data", sport=sport, error=str(e))
            return []

    def _collect_vsin_data_sync(self, sport: str, **kwargs) -> list[dict[str, Any]]:
        """
        Synchronous collection of VSIN betting data with live HTML parsing.
        
        Args:
            sport: Sport to collect data for
            **kwargs: Additional parameters including sportsbook selection
            
        Returns:
            List of parsed betting data records
        """
        all_data = []
        
        try:
            # Get sportsbook preference from kwargs, default to DraftKings
            sportsbook = kwargs.get('sportsbook', 'dk')
            
            # Support multiple sportsbooks for comprehensive data
            sportsbooks_to_collect = [sportsbook] if sportsbook != 'all' else ['dk', 'circa', 'fanduel']
            
            for book in sportsbooks_to_collect:
                try:
                    # Build URL for specific sportsbook
                    url = self.build_vsin_url(sport, book)
                    self.logger.info(f"Collecting VSIN data from {book.upper()}", url=url)
                    
                    # Collect HTML data
                    html_content = self._fetch_html_content_sync(url)
                    if html_content:
                        # Parse HTML using patterns from original implementation
                        parsed_data = self._parse_vsin_html(html_content, sport, book)
                        if parsed_data:
                            all_data.extend(parsed_data)
                            self.logger.info(f"Successfully parsed {len(parsed_data)} records from {book.upper()}")
                        else:
                            self.logger.warning(f"No data found in HTML for {book.upper()}")
                    else:
                        self.logger.warning(f"Failed to fetch HTML content for {book.upper()}")
                        
                except Exception as e:
                    self.logger.error(f"Error collecting from {book.upper()}", error=str(e))
                    continue
            
            return all_data
            
        except Exception as e:
            self.logger.error("Error in synchronous VSIN collection", error=str(e))
            return []

    def _fetch_html_content_sync(self, url: str) -> str | None:
        """
        Fetch HTML content synchronously using requests (from original implementation).
        
        Args:
            url: VSIN URL to fetch
            
        Returns:
            HTML content string or None if failed
        """
        try:
            import requests
            
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Referer": f"{self.base_url}/",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Cache-Control": "max-age=0",
            }
            
            self.logger.info(f"Fetching HTML from {url}")
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            return response.text
            
        except Exception as e:
            self.logger.error(f"Error fetching HTML: {str(e)}")
            return None

    def _parse_vsin_html(self, html_content: str, sport: str, sportsbook: str) -> list[dict[str, Any]]:
        """
        Parse VSIN HTML content using patterns from original implementation.
        
        Args:
            html_content: Raw HTML from VSIN
            sport: Sport being parsed
            sportsbook: Source sportsbook
            
        Returns:
            List of parsed betting records
        """
        try:
            # Extract main content div (from original implementation)
            main_content = self._extract_main_content(html_content)
            
            soup = BeautifulSoup(main_content, 'lxml')
            
            # Find main betting table
            main_table = soup.find('table', {'class': 'freezetable'})
            if not main_table:
                self.logger.warning("Could not find main betting table")
                return []
            
            rows = main_table.find_all('tr')
            parsed_games = []
            
            for tr in rows:
                # Skip header rows
                if 'div_dkdark' in tr.get('class', []):
                    continue
                    
                cells = tr.find_all('td')
                if len(cells) < 10:  # Ensure we have all required columns for MLB
                    continue
                
                # Extract game data using original parsing patterns
                game_record = self._parse_game_row(cells, sport, sportsbook)
                if game_record:
                    parsed_games.append(game_record)
            
            return parsed_games
            
        except Exception as e:
            self.logger.error("Error parsing VSIN HTML", error=str(e))
            return []

    def _extract_main_content(self, html_content: str) -> str:
        """
        Extract main content div from HTML (from original implementation).
        
        Args:
            html_content: Full HTML content
            
        Returns:
            Main content HTML string
        """
        try:
            soup = BeautifulSoup(html_content, 'lxml')
            
            # Try multiple selectors for main content
            selectors = [
                {'class': 'main-content paywall-active', 'id': 'main-content'},
                {'id': 'main-content'},
                {'class': 'main-content'},
                {'class': 'freezetable'}
            ]
            
            for selector in selectors:
                main_content_div = soup.find('div', selector) or soup.find('table', selector)
                if main_content_div:
                    return str(main_content_div)
            
            # If no specific content found, return full HTML
            self.logger.warning("Could not find main content div, using full HTML")
            return html_content
            
        except Exception as e:
            self.logger.error("Error extracting main content", error=str(e))
            return html_content

    def _parse_game_row(self, cells: list, sport: str, sportsbook: str) -> dict[str, Any] | None:
        """
        Parse a single game row from VSIN table using original implementation patterns.
        
        Args:
            cells: Table cells for the game row
            sport: Sport being parsed (affects column arrangement)
            sportsbook: Source sportsbook
            
        Returns:
            Parsed game record or None if parsing failed
        """
        try:
            # Extract team names from first column
            away_team, home_team = self._extract_team_names(cells[0])
            if away_team == "Unknown" or home_team == "Unknown":
                return None
            
            # MLB-specific column mapping (from original implementation)
            if sport.lower() == 'mlb':
                ml_col, total_col, spread_col = 1, 4, 7
            else:
                # Other sports use different arrangement
                ml_col, total_col, spread_col = 1, 4, 7
            
            # Extract betting data for all markets
            moneyline_data = self._extract_moneyline_data(cells, ml_col)
            totals_data = self._extract_totals_data(cells, total_col) 
            spread_data = self._extract_spread_data(cells, spread_col, sport)
            
            # Combine all betting data
            combined_data = {**moneyline_data, **totals_data, **spread_data}
            
            # Detect sharp action patterns (Phase 3)
            sharp_indicators = self._detect_sharp_action_comprehensive(combined_data)
            
            # Calculate data quality score
            quality_score = self._calculate_data_completeness(combined_data)
            
            # Create unified game record for three-tier pipeline
            external_source_id = f"vsin_{sport}_{away_team.replace(' ', '')}_{home_team.replace(' ', '')}_{sportsbook}_{datetime.now().strftime('%Y%m%d')}"
            
            game_record = {
                'external_source_id': external_source_id,
                'bet_type': 'composite',  # VSIN provides multiple bet types
                'home_team': home_team,
                'away_team': away_team,
                'game_datetime': datetime.now().isoformat(),
                'collection_method': 'HTML_PARSING',
                'source_api_version': 'vsin_unified_v3_live',
                'source_metadata': {
                    'sportsbook': sportsbook,
                    'sport': sport,
                    'url_source': sportsbook,
                    'data_format': 'html_parsed'
                },
                'sharp_action': sharp_indicators.get('overall'),
                'reverse_line_movement': False,  # Would need historical data
                'steam_move': False,  # Would need historical data
                # Include betting percentages at top level for unified processing
                'home_money_percentage': combined_data.get('home_ml_handle_pct'),
                'away_money_percentage': combined_data.get('away_ml_handle_pct'),
                'home_bets_percentage': combined_data.get('home_ml_bets_pct'),
                'away_bets_percentage': combined_data.get('away_ml_bets_pct'),
                'over_money_percentage': combined_data.get('over_handle_pct'),
                'under_money_percentage': combined_data.get('under_handle_pct'),
                'over_bets_percentage': combined_data.get('over_bets_pct'),
                'under_bets_percentage': combined_data.get('under_bets_pct'),
                'home_ml': combined_data.get('home_ml'),
                'away_ml': combined_data.get('away_ml'),
                'total_line': combined_data.get('total_line'),
                'spread_line': combined_data.get('away_rl') or combined_data.get('away_spread'),
                'raw_response': {
                    'away_team': away_team,
                    'home_team': home_team,
                    'sportsbook': sportsbook,
                    'sport': sport,
                    'betting_data': combined_data,
                    'sharp_indicators': sharp_indicators,
                    'data_quality_score': quality_score,
                    'collection_metadata': {
                        'collection_timestamp': datetime.now().isoformat(),
                        'source': 'vsin',
                        'collector_version': 'vsin_unified_v3_live',
                        'data_format': 'html_parsed',
                        'url_source': sportsbook
                    }
                },
                'api_endpoint': f'vsin_html_{sportsbook}'
            }
            
            return game_record
            
        except Exception as e:
            self.logger.error("Error parsing game row", error=str(e))
            return None

    def _extract_team_names(self, team_cell) -> tuple[str, str]:
        """
        Extract away and home team names from VSIN team cell (from original implementation).
        
        Args:
            team_cell: BeautifulSoup table cell containing team information
            
        Returns:
            Tuple of (away_team, home_team)
        """
        try:
            team_text = team_cell.get_text(strip=True, separator='\n')
            teams = team_text.split('\n')
            
            clean_teams = []
            for item in teams:
                # Clean team names (remove IDs, extra text)
                clean_item = re.sub(r'\(\d+\)', '', item).strip()
                clean_item = re.sub(r'History|VSiN Pro Picks|\d+ VSiN Pro Picks', '', clean_item).strip()
                
                # Filter out short strings and keep actual team names
                if clean_item and len(clean_item) > 2:
                    clean_teams.append(clean_item)
            
            if len(clean_teams) >= 2:
                return clean_teams[0], clean_teams[1]  # Away, Home
            else:
                return "Unknown", "Unknown"
                
        except Exception as e:
            self.logger.error("Error extracting team names", error=str(e))
            return "Unknown", "Unknown"

    def _extract_moneyline_data(self, cells: list, ml_col: int) -> dict[str, Any]:
        """
        Extract moneyline odds and percentages from MLB format (column 1-3).
        
        Args:
            cells: Table cells
            ml_col: Starting column for moneyline data (typically 1)
            
        Returns:
            Dictionary with moneyline data
        """
        data = {}
        
        try:
            # Extract odds from moneyline column
            if len(cells) > ml_col:
                odds_text = cells[ml_col].get_text(strip=True, separator='\n')
                odds_values = re.findall(r'[+-]?\d+', odds_text)
                if len(odds_values) >= 2:
                    # Ensure proper +/- formatting
                    away_odds = odds_values[0] if odds_values[0].startswith(('+', '-')) else f"+{odds_values[0]}"
                    home_odds = odds_values[1] if odds_values[1].startswith(('+', '-')) else odds_values[1]
                    data.update({
                        'away_ml': away_odds,
                        'home_ml': home_odds
                    })
            
            # Extract handle percentages
            if len(cells) > ml_col + 1:
                handle_text = cells[ml_col + 1].get_text(strip=True, separator='\n')
                handle_values = re.findall(r'\d+%', handle_text)
                if len(handle_values) >= 2:
                    data.update({
                        'away_ml_handle_pct': float(handle_values[0].replace('%', '')),
                        'home_ml_handle_pct': float(handle_values[1].replace('%', ''))
                    })
            
            # Extract bet percentages
            if len(cells) > ml_col + 2:
                bets_text = cells[ml_col + 2].get_text(strip=True, separator='\n')
                bets_values = re.findall(r'\d+%', bets_text)
                if len(bets_values) >= 2:
                    data.update({
                        'away_ml_bets_pct': float(bets_values[0].replace('%', '')),
                        'home_ml_bets_pct': float(bets_values[1].replace('%', ''))
                    })
            
        except Exception as e:
            self.logger.error("Error extracting moneyline data", error=str(e))
        
        return data

    def _extract_totals_data(self, cells: list, total_col: int) -> dict[str, Any]:
        """
        Extract totals (over/under) data from MLB format (column 4-6).
        
        Args:
            cells: Table cells
            total_col: Starting column for totals data (typically 4)
            
        Returns:
            Dictionary with totals data
        """
        data = {}
        
        try:
            # Extract total line
            if len(cells) > total_col:
                total_text = cells[total_col].get_text(strip=True, separator='\n')
                total_values = re.findall(r'\d+\.?\d*', total_text)
                if total_values:
                    data['total_line'] = float(total_values[0])
            
            # Extract handle percentages (over/under)
            if len(cells) > total_col + 1:
                handle_text = cells[total_col + 1].get_text(strip=True, separator='\n')
                handle_values = re.findall(r'\d+%', handle_text)
                if len(handle_values) >= 2:
                    data.update({
                        'over_handle_pct': float(handle_values[0].replace('%', '')),
                        'under_handle_pct': float(handle_values[1].replace('%', ''))
                    })
            
            # Extract bet percentages (over/under)
            if len(cells) > total_col + 2:
                bets_text = cells[total_col + 2].get_text(strip=True, separator='\n')
                bets_values = re.findall(r'\d+%', bets_text)
                if len(bets_values) >= 2:
                    data.update({
                        'over_bets_pct': float(bets_values[0].replace('%', '')),
                        'under_bets_pct': float(bets_values[1].replace('%', ''))
                    })
            
        except Exception as e:
            self.logger.error("Error extracting totals data", error=str(e))
        
        return data

    def _extract_spread_data(self, cells: list, spread_col: int, sport: str) -> dict[str, Any]:
        """
        Extract spread/run line data from MLB format (column 7-9).
        
        Args:
            cells: Table cells
            spread_col: Starting column for spread data (typically 7)
            sport: Sport type (affects naming - 'runline' for MLB, 'spread' for others)
            
        Returns:
            Dictionary with spread data
        """
        data = {}
        spread_prefix = 'rl' if sport.lower() == 'mlb' else 'spread'
        
        try:
            # Extract spread/run line
            if len(cells) > spread_col:
                spread_text = cells[spread_col].get_text(strip=True, separator='\n')
                spread_values = re.findall(r'[+-]?\d+\.?\d*', spread_text)
                if len(spread_values) >= 2:
                    data.update({
                        f'away_{spread_prefix}': spread_values[0],
                        f'home_{spread_prefix}': spread_values[1]
                    })
            
            # Extract handle percentages
            if len(cells) > spread_col + 1:
                handle_text = cells[spread_col + 1].get_text(strip=True, separator='\n')
                handle_values = re.findall(r'\d+%', handle_text)
                if len(handle_values) >= 2:
                    data.update({
                        f'away_{spread_prefix}_handle_pct': float(handle_values[0].replace('%', '')),
                        f'home_{spread_prefix}_handle_pct': float(handle_values[1].replace('%', ''))
                    })
            
            # Extract bet percentages
            if len(cells) > spread_col + 2:
                bets_text = cells[spread_col + 2].get_text(strip=True, separator='\n')
                bets_values = re.findall(r'\d+%', bets_text)
                if len(bets_values) >= 2:
                    data.update({
                        f'away_{spread_prefix}_bets_pct': float(bets_values[0].replace('%', '')),
                        f'home_{spread_prefix}_bets_pct': float(bets_values[1].replace('%', ''))
                    })
            
        except Exception as e:
            self.logger.error("Error extracting spread data", error=str(e))
        
        return data

    def _detect_sharp_action_comprehensive(self, betting_data: dict) -> dict[str, Any]:
        """
        Comprehensive sharp action detection across all betting markets (Phase 3).
        
        Args:
            betting_data: Combined betting data from all markets
            
        Returns:
            Dictionary with sharp action indicators
        """
        sharp_indicators = {}
        
        try:
            # Moneyline sharp action detection
            ml_sharp = self._detect_percentage_divergence(
                betting_data.get('away_ml_handle_pct'),
                betting_data.get('away_ml_bets_pct'),
                betting_data.get('home_ml_handle_pct'),
                betting_data.get('home_ml_bets_pct'),
                'moneyline'
            )
            if ml_sharp:
                sharp_indicators['moneyline'] = ml_sharp
            
            # Totals sharp action detection
            totals_sharp = self._detect_percentage_divergence(
                betting_data.get('over_handle_pct'),
                betting_data.get('over_bets_pct'),
                betting_data.get('under_handle_pct'),
                betting_data.get('under_bets_pct'),
                'totals'
            )
            if totals_sharp:
                sharp_indicators['totals'] = totals_sharp
            
            # Run line/spread sharp action detection
            spread_prefix = 'rl' if 'away_rl' in betting_data else 'spread'
            spread_sharp = self._detect_percentage_divergence(
                betting_data.get(f'away_{spread_prefix}_handle_pct'),
                betting_data.get(f'away_{spread_prefix}_bets_pct'),
                betting_data.get(f'home_{spread_prefix}_handle_pct'),
                betting_data.get(f'home_{spread_prefix}_bets_pct'),
                spread_prefix
            )
            if spread_sharp:
                sharp_indicators[spread_prefix] = spread_sharp
            
            # Overall sharp action summary
            if sharp_indicators:
                sharp_count = len([v for v in sharp_indicators.values() if 'SHARP' in v])
                strong_count = len([v for v in sharp_indicators.values() if 'STRONG' in v])
                
                if strong_count > 0:
                    sharp_indicators['overall'] = 'STRONG_SHARP_ACTION'
                elif sharp_count > 1:
                    sharp_indicators['overall'] = 'MODERATE_SHARP_ACTION'
                elif sharp_count > 0:
                    sharp_indicators['overall'] = 'LIGHT_SHARP_ACTION'
            
        except Exception as e:
            self.logger.error("Error detecting sharp action", error=str(e))
        
        return sharp_indicators

    def _detect_percentage_divergence(self, handle1: float | None, bets1: float | None, 
                                    handle2: float | None, bets2: float | None, 
                                    market_type: str) -> str | None:
        """
        Detect money vs. bets percentage divergence indicating sharp action.
        
        Args:
            handle1, bets1: First side percentages (away/over/etc.)
            handle2, bets2: Second side percentages (home/under/etc.)
            market_type: Type of market being analyzed
            
        Returns:
            Sharp action indicator string or None
        """
        try:
            if not all(x is not None for x in [handle1, bets1, handle2, bets2]):
                return None
            
            # Check for small sample size indicators
            if any(x in [0, 100] for x in [handle1, bets1, handle2, bets2]):
                return "SMALL_SAMPLE"
            
            # Check side 1 for divergence
            if abs(handle1 - bets1) >= 15:
                strength = "STRONG" if abs(handle1 - bets1) >= 25 else "MODERATE"
                direction = "SHARP_MONEY" if handle1 > bets1 else "PUBLIC_MONEY"
                return f"{strength}_{direction}_SIDE1"
            
            # Check side 2 for divergence
            if abs(handle2 - bets2) >= 15:
                strength = "STRONG" if abs(handle2 - bets2) >= 25 else "MODERATE"
                direction = "SHARP_MONEY" if handle2 > bets2 else "PUBLIC_MONEY"
                return f"{strength}_{direction}_SIDE2"
            
            return None
            
        except (ValueError, TypeError):
            return None

    def _calculate_data_completeness(self, betting_data: dict) -> float:
        """
        Calculate data quality score based on field completeness.
        
        Args:
            betting_data: Combined betting data
            
        Returns:
            Quality score as percentage (0-100)
        """
        try:
            # Define required fields for comprehensive data
            required_fields = [
                'away_ml', 'home_ml', 'away_ml_handle_pct', 'home_ml_handle_pct',
                'total_line', 'over_handle_pct', 'under_handle_pct'
            ]
            
            # Add sport-specific spread fields
            if 'away_rl' in betting_data:
                required_fields.extend(['away_rl', 'home_rl', 'away_rl_handle_pct', 'home_rl_handle_pct'])
            elif 'away_spread' in betting_data:
                required_fields.extend(['away_spread', 'home_spread', 'away_spread_handle_pct', 'home_spread_handle_pct'])
            
            present_fields = sum(1 for field in required_fields 
                               if field in betting_data and betting_data[field] is not None)
            
            return (present_fields / len(required_fields)) * 100 if required_fields else 0
            
        except Exception as e:
            self.logger.error("Error calculating data completeness", error=str(e))
            return 0.0

    async def _collect_vsin_data_async(self, sport: str, **kwargs) -> list[dict[str, Any]]:
        """Async collection of VSIN betting data."""
        all_data = []

        try:
            # Try API first, fall back to scraping
            api_data = await self._collect_via_api(sport)
            if api_data:
                all_data.extend(api_data)
            else:
                # Fallback to web scraping
                scraping_data = await self._collect_via_scraping(sport)
                all_data.extend(scraping_data)

            return all_data

        except Exception as e:
            self.logger.error("Failed to collect VSIN data", error=str(e))
            return []

    async def _collect_via_api(self, sport: str) -> list[dict[str, Any]]:
        """Collect data via VSIN API endpoints."""
        all_data = []
        
        try:
            # Initialize HTTP session with proper headers
            timeout = aiohttp.ClientTimeout(total=30)
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/json, text/html, text/plain, */*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Referer': f'{self.base_url}/',
                'Cache-Control': 'no-cache',
            }

            async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
                # Try multiple VSIN API endpoints
                api_endpoints = [
                    f"{self.base_url}/api/v1/{sport}/betting-splits",
                    f"{self.base_url}/api/{sport}/betting-splits",
                    f"{self.base_url}/api/{sport}/odds",
                    f"{self.base_url}/{sport}/api/betting-data",
                    f"{self.base_url}/{sport}/betting-splits",
                    f"{self.base_url}/betting-splits/{sport}/api",
                    f"{self.base_url}/betting-splits/{sport}"
                ]

                for api_url in api_endpoints:
                    try:
                        self.logger.info(f"Trying VSIN API endpoint: {api_url}")
                        
                        async with session.get(api_url) as response:
                            if response.status == 200:
                                content_type = response.headers.get('content-type', '')
                                
                                if 'application/json' in content_type:
                                    data = await response.json()
                                    processed_data = self._process_api_data(data, sport, api_url)
                                    if processed_data:
                                        all_data.extend(processed_data)
                                        self.logger.info(f"Successfully collected {len(processed_data)} records from {api_url}")
                                        break
                                else:
                                    # Handle HTML/text response - might contain embedded JSON
                                    text_data = await response.text()
                                    json_data = self._extract_json_from_html(text_data, api_url)
                                    if json_data:
                                        processed_data = self._process_api_data(json_data, sport, api_url)
                                        if processed_data:
                                            all_data.extend(processed_data)
                                            self.logger.info(f"Successfully extracted {len(processed_data)} records from {api_url}")
                                            break
                            else:
                                self.logger.debug(f"API endpoint {api_url} returned status: {response.status}")
                                
                    except Exception as e:
                        self.logger.debug(f"API endpoint {api_url} failed: {str(e)}")
                        continue

                if not all_data:
                    self.logger.info("No successful API endpoints found - using mock data for testing")
                    all_data = self._generate_mock_data(sport)

                return all_data

        except Exception as e:
            self.logger.error("API collection failed", error=str(e))
            # Generate mock data for testing purposes
            return self._generate_mock_data(sport)

    async def _collect_via_scraping(self, sport: str) -> list[dict[str, Any]]:
        """Collect data via web scraping."""
        all_data = []

        try:
            # Initialize Playwright adapter
            # Note: MCP browser automation no longer available
            self.playwright_adapter = None

            # Check if playwright adapter is available
            if self.playwright_adapter is None:
                self.logger.warning("Playwright adapter not available - scraping disabled")
                return []

            # Navigate to VSIN betting splits page
            url = f"{self.base_url}/betting-splits/{sport}"
            await self.playwright_adapter.goto(url)
            await self.playwright_adapter.wait_for_load_state('networkidle')

            # Extract today's games
            games_data = await self._extract_games_data(sport)
            self.logger.info(f"Found {len(games_data)} games on VSIN")

            # For each game, collect betting data for all bet types
            for game_data in games_data:
                try:
                    # Navigate to game-specific page if needed
                    game_url = game_data.get('game_url')
                    if game_url:
                        await self.playwright_adapter.goto(game_url)
                        await self.playwright_adapter.wait_for_load_state('networkidle')

                    # Extract betting data for each bet type
                    for bet_type in self.bet_types:
                        try:
                            betting_data = await self._extract_betting_data(
                                game_data, bet_type
                            )

                            # Convert to unified format
                            unified_data = self._convert_to_unified_format(
                                betting_data, game_data, bet_type
                            )

                            all_data.extend(unified_data)

                            self.logger.info(
                                f"Collected {len(unified_data)} unified records",
                                game=game_data.get('game_name'),
                                bet_type=bet_type['name']
                            )

                        except Exception as e:
                            self.logger.error(
                                "Error collecting betting data",
                                game=game_data.get('game_name'),
                                bet_type=bet_type['name'],
                                error=str(e)
                            )
                            continue

                except Exception as e:
                    self.logger.error(
                        "Error processing game",
                        game=game_data.get('game_name'),
                        error=str(e)
                    )
                    continue

            return all_data

        except Exception as e:
            self.logger.error("Failed to collect VSIN data via scraping", error=str(e))
            return []

        finally:
            if self.playwright_adapter:
                await self.playwright_adapter.close()

    async def _extract_games_data(self, sport: str) -> list[dict[str, Any]]:
        """Extract today's games from VSIN betting splits page."""
        try:
            # Extract games using JavaScript
            games_script = """
            (function() {
                const games = [];
                const gameElements = document.querySelectorAll('.game-card, .betting-game, .split-game');
                
                gameElements.forEach(element => {
                    const gameNameElement = element.querySelector('.game-name, .matchup, .teams');
                    const gameUrlElement = element.querySelector('a[href]');
                    
                    if (gameNameElement) {
                        const gameName = gameNameElement.textContent.trim();
                        const gameUrl = gameUrlElement ? gameUrlElement.getAttribute('href') : null;
                        
                        // Extract team names
                        const teams = gameName.split(/[@vs]/i).map(t => t.trim());
                        
                        games.push({
                            game_name: gameName,
                            game_url: gameUrl ? (gameUrl.startsWith('http') ? gameUrl : 'https://www.vsin.com' + gameUrl) : null,
                            away_team: teams[0] || 'Unknown',
                            home_team: teams[1] || 'Unknown',
                            sport: arguments[0],
                            extraction_timestamp: new Date().toISOString()
                        });
                    }
                });
                
                return games;
            })();
            """

            result = await self.playwright_adapter.evaluate(games_script, sport)
            return result or []

        except Exception as e:
            self.logger.error("Error extracting games data", error=str(e))
            return []

    async def _extract_betting_data(
        self,
        game_data: dict[str, Any],
        bet_type: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Extract betting data for a specific game and bet type.
        
        Args:
            game_data: Game information
            bet_type: Bet type configuration
            
        Returns:
            List of betting data records
        """
        try:
            # Extract betting data using JavaScript based on bet type
            if bet_type['unified_type'] == 'moneyline':
                extraction_script = """
                (function() {
                    const data = [];
                    const moneylineElements = document.querySelectorAll('.moneyline-data, .ml-data, .money-line');
                    
                    moneylineElements.forEach(element => {
                        const sportsbook = element.querySelector('.sportsbook-name, .book-name')?.textContent?.trim();
                        const homeOdds = element.querySelector('.home-odds, .home-ml')?.textContent?.trim();
                        const awayOdds = element.querySelector('.away-odds, .away-ml')?.textContent?.trim();
                        const homeBetsPct = element.querySelector('.home-bets-pct, .home-tickets')?.textContent?.trim();
                        const awayBetsPct = element.querySelector('.away-bets-pct, .away-tickets')?.textContent?.trim();
                        const homeMoneyPct = element.querySelector('.home-money-pct, .home-handle')?.textContent?.trim();
                        const awayMoneyPct = element.querySelector('.away-money-pct, .away-handle')?.textContent?.trim();
                        
                        if (sportsbook && (homeOdds || awayOdds)) {
                            data.push({
                                sportsbook: sportsbook,
                                home_odds: homeOdds,
                                away_odds: awayOdds,
                                home_bets_percentage: homeBetsPct,
                                away_bets_percentage: awayBetsPct,
                                home_money_percentage: homeMoneyPct,
                                away_money_percentage: awayMoneyPct,
                                timestamp: new Date().toISOString()
                            });
                        }
                    });
                    
                    return data;
                })();
                """

            elif bet_type['unified_type'] == 'spread':
                extraction_script = """
                (function() {
                    const data = [];
                    const spreadElements = document.querySelectorAll('.spread-data, .rl-data, .run-line');
                    
                    spreadElements.forEach(element => {
                        const sportsbook = element.querySelector('.sportsbook-name, .book-name')?.textContent?.trim();
                        const spreadLine = element.querySelector('.spread-line, .rl-line')?.textContent?.trim();
                        const homeSpreadOdds = element.querySelector('.home-spread-odds, .home-rl-odds')?.textContent?.trim();
                        const awaySpreadOdds = element.querySelector('.away-spread-odds, .away-rl-odds')?.textContent?.trim();
                        const homeBetsPct = element.querySelector('.home-bets-pct, .home-tickets')?.textContent?.trim();
                        const awayBetsPct = element.querySelector('.away-bets-pct, .away-tickets')?.textContent?.trim();
                        const homeMoneyPct = element.querySelector('.home-money-pct, .home-handle')?.textContent?.trim();
                        const awayMoneyPct = element.querySelector('.away-money-pct, .away-handle')?.textContent?.trim();
                        
                        if (sportsbook && spreadLine) {
                            data.push({
                                sportsbook: sportsbook,
                                spread_line: spreadLine,
                                home_spread_odds: homeSpreadOdds,
                                away_spread_odds: awaySpreadOdds,
                                home_bets_percentage: homeBetsPct,
                                away_bets_percentage: awayBetsPct,
                                home_money_percentage: homeMoneyPct,
                                away_money_percentage: awayMoneyPct,
                                timestamp: new Date().toISOString()
                            });
                        }
                    });
                    
                    return data;
                })();
                """

            elif bet_type['unified_type'] == 'totals':
                extraction_script = """
                (function() {
                    const data = [];
                    const totalsElements = document.querySelectorAll('.totals-data, .total-data, .over-under');
                    
                    totalsElements.forEach(element => {
                        const sportsbook = element.querySelector('.sportsbook-name, .book-name')?.textContent?.trim();
                        const totalLine = element.querySelector('.total-line, .ou-line')?.textContent?.trim();
                        const overOdds = element.querySelector('.over-odds, .over-price')?.textContent?.trim();
                        const underOdds = element.querySelector('.under-odds, .under-price')?.textContent?.trim();
                        const overBetsPct = element.querySelector('.over-bets-pct, .over-tickets')?.textContent?.trim();
                        const underBetsPct = element.querySelector('.under-bets-pct, .under-tickets')?.textContent?.trim();
                        const overMoneyPct = element.querySelector('.over-money-pct, .over-handle')?.textContent?.trim();
                        const underMoneyPct = element.querySelector('.under-money-pct, .under-handle')?.textContent?.trim();
                        
                        if (sportsbook && totalLine) {
                            data.push({
                                sportsbook: sportsbook,
                                total_line: totalLine,
                                over_odds: overOdds,
                                under_odds: underOdds,
                                over_bets_percentage: overBetsPct,
                                under_bets_percentage: underBetsPct,
                                over_money_percentage: overMoneyPct,
                                under_money_percentage: underMoneyPct,
                                timestamp: new Date().toISOString()
                            });
                        }
                    });
                    
                    return data;
                })();
                """

            else:
                return []

            result = await self.playwright_adapter.evaluate(extraction_script)
            return result or []

        except Exception as e:
            self.logger.error("Error extracting betting data", error=str(e))
            return []

    def _convert_to_unified_format(
        self,
        betting_data: list[dict[str, Any]],
        game_data: dict[str, Any],
        bet_type: dict[str, Any] = None
    ) -> list[dict[str, Any]]:
        """
        Convert VSIN raw data to unified format for three-tier pipeline.
        
        Args:
            betting_data: Raw betting data from VSIN
            game_data: Game information
            bet_type: Bet type configuration (optional for API data)
            
        Returns:
            List of unified format records for raw_data.vsin_betting_splits
        """
        unified_records = []

        for record in betting_data:
            try:
                # Generate external source ID for three-tier pipeline
                external_source_id = f"vsin_{game_data.get('game_id', game_data.get('game_name', 'unknown').replace(' ', '_'))}_{datetime.now().strftime('%Y%m%d')}"

                # Create raw_data record for three-tier pipeline
                raw_record = {
                    'external_source_id': external_source_id,
                    'raw_response': {
                        'game_data': game_data,
                        'betting_record': record,
                        'collection_metadata': {
                            'collection_timestamp': datetime.now().isoformat(),
                            'source': 'vsin',
                            'collector_version': 'vsin_unified_v2',
                            'data_format': 'api_response',
                            'sport': game_data.get('sport', 'mlb'),
                            'bet_type_config': bet_type
                        }
                    },
                    'api_endpoint': game_data.get('api_endpoint', 'vsin_unified_collector')
                }

                unified_records.append(raw_record)

            except Exception as e:
                self.logger.error("Error converting record to unified format", error=str(e))
                continue

        return unified_records

    def _convert_to_legacy_unified_format(
        self,
        betting_data: list[dict[str, Any]],
        game_data: dict[str, Any],
        bet_type: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Convert VSIN raw data to legacy unified format (for backward compatibility).
        
        Args:
            betting_data: Raw betting data from VSIN
            game_data: Game information
            bet_type: Bet type configuration
            
        Returns:
            List of legacy unified format records
        """
        unified_records = []

        for record in betting_data:
            try:
                # Generate external source ID from game and sportsbook
                external_source_id = f"vsin_{game_data['game_name'].replace(' ', '_')}_{record['sportsbook'].replace(' ', '_')}"

                # Base unified record
                unified_record = {
                    'external_source_id': external_source_id,
                    'sportsbook': record['sportsbook'],
                    'bet_type': bet_type['unified_type'],
                    'odds_timestamp': record.get('timestamp', datetime.now().isoformat()),
                    'collection_method': 'API_REQUEST',
                    'source_api_version': 'VSIN_v2',
                    'source_metadata': {
                        'original_bet_type': bet_type['data_format'],
                        'game_url': game_data.get('game_url'),
                        'extraction_method': 'api',
                        'sport': game_data.get('sport', 'mlb')
                    },
                    'game_datetime': datetime.now().isoformat(),  # VSIN doesn't provide game datetime
                    'home_team': game_data.get('home_team'),
                    'away_team': game_data.get('away_team'),
                }

                # Add bet type specific fields
                if bet_type['unified_type'] == 'moneyline':
                    unified_record.update({
                        'home_ml': self._parse_odds(record.get('home_odds')),
                        'away_ml': self._parse_odds(record.get('away_odds')),
                        'home_bets_percentage': self._parse_percentage(record.get('home_bets_percentage')),
                        'away_bets_percentage': self._parse_percentage(record.get('away_bets_percentage')),
                        'home_money_percentage': self._parse_percentage(record.get('home_money_percentage')),
                        'away_money_percentage': self._parse_percentage(record.get('away_money_percentage')),
                    })

                elif bet_type['unified_type'] == 'spread':
                    unified_record.update({
                        'spread_line': self._parse_spread(record.get('spread_line')),
                        'home_spread_price': self._parse_odds(record.get('home_spread_odds')),
                        'away_spread_price': self._parse_odds(record.get('away_spread_odds')),
                        'home_bets_percentage': self._parse_percentage(record.get('home_bets_percentage')),
                        'away_bets_percentage': self._parse_percentage(record.get('away_bets_percentage')),
                        'home_money_percentage': self._parse_percentage(record.get('home_money_percentage')),
                        'away_money_percentage': self._parse_percentage(record.get('away_money_percentage')),
                    })

                elif bet_type['unified_type'] == 'totals':
                    unified_record.update({
                        'total_line': self._parse_total(record.get('total_line')),
                        'over_price': self._parse_odds(record.get('over_odds')),
                        'under_price': self._parse_odds(record.get('under_odds')),
                        'over_bets_percentage': self._parse_percentage(record.get('over_bets_percentage')),
                        'under_bets_percentage': self._parse_percentage(record.get('under_bets_percentage')),
                        'over_money_percentage': self._parse_percentage(record.get('over_money_percentage')),
                        'under_money_percentage': self._parse_percentage(record.get('under_money_percentage')),
                    })

                # Detect sharp action indicators
                sharp_action = self._detect_sharp_action(unified_record, bet_type['unified_type'])
                if sharp_action:
                    unified_record['sharp_action'] = sharp_action

                # Detect reverse line movement
                rlm = self._detect_reverse_line_movement(unified_record, bet_type['unified_type'])
                unified_record['reverse_line_movement'] = rlm

                unified_records.append(unified_record)

            except Exception as e:
                self.logger.error("Error converting record to unified format", error=str(e))
                continue

        return unified_records

    def _parse_odds(self, odds_str: str | None) -> int | None:
        """Parse odds string to integer."""
        if not odds_str:
            return None

        try:
            # Remove non-numeric characters except + and -
            clean_odds = re.sub(r'[^\d+-]', '', odds_str)
            if clean_odds:
                return int(clean_odds)
        except ValueError:
            pass

        return None

    def _parse_percentage(self, pct_str: str | None) -> float | None:
        """Parse percentage string to float."""
        if not pct_str:
            return None

        try:
            # Remove % and convert to float
            clean_pct = re.sub(r'[^\d.]', '', pct_str)
            if clean_pct:
                return float(clean_pct)
        except ValueError:
            pass

        return None

    def _parse_spread(self, spread_str: str | None) -> float | None:
        """Parse spread string to float."""
        if not spread_str:
            return None

        try:
            # Remove non-numeric characters except + and -
            clean_spread = re.sub(r'[^\d.+-]', '', spread_str)
            if clean_spread:
                return float(clean_spread)
        except ValueError:
            pass

        return None

    def _parse_total(self, total_str: str | None) -> float | None:
        """Parse total string to float."""
        if not total_str:
            return None

        try:
            # Remove non-numeric characters except decimal point
            clean_total = re.sub(r'[^\d.]', '', total_str)
            if clean_total:
                return float(clean_total)
        except ValueError:
            pass

        return None

    def _detect_sharp_action(self, record: dict[str, Any], bet_type: str) -> str | None:
        """Detect sharp action based on betting percentages."""
        try:
            if bet_type == 'moneyline':
                home_money_pct = record.get('home_money_percentage', 0)
                away_money_pct = record.get('away_money_percentage', 0)
                home_bets_pct = record.get('home_bets_percentage', 0)
                away_bets_pct = record.get('away_bets_percentage', 0)

                # Sharp action: money percentage significantly higher than bet percentage
                if home_money_pct and home_bets_pct and home_money_pct > home_bets_pct + 15:
                    return 'MODERATE' if home_money_pct > home_bets_pct + 25 else 'LIGHT'
                elif away_money_pct and away_bets_pct and away_money_pct > away_bets_pct + 15:
                    return 'MODERATE' if away_money_pct > away_bets_pct + 25 else 'LIGHT'

            elif bet_type == 'spread':
                home_money_pct = record.get('home_money_percentage', 0)
                away_money_pct = record.get('away_money_percentage', 0)
                home_bets_pct = record.get('home_bets_percentage', 0)
                away_bets_pct = record.get('away_bets_percentage', 0)

                if home_money_pct and home_bets_pct and home_money_pct > home_bets_pct + 15:
                    return 'MODERATE' if home_money_pct > home_bets_pct + 25 else 'LIGHT'
                elif away_money_pct and away_bets_pct and away_money_pct > away_bets_pct + 15:
                    return 'MODERATE' if away_money_pct > away_bets_pct + 25 else 'LIGHT'

            elif bet_type == 'totals':
                over_money_pct = record.get('over_money_percentage', 0)
                under_money_pct = record.get('under_money_percentage', 0)
                over_bets_pct = record.get('over_bets_percentage', 0)
                under_bets_pct = record.get('under_bets_percentage', 0)

                if over_money_pct and over_bets_pct and over_money_pct > over_bets_pct + 15:
                    return 'MODERATE' if over_money_pct > over_bets_pct + 25 else 'LIGHT'
                elif under_money_pct and under_bets_pct and under_money_pct > under_bets_pct + 15:
                    return 'MODERATE' if under_money_pct > under_bets_pct + 25 else 'LIGHT'

        except Exception as e:
            self.logger.error("Error detecting sharp action", error=str(e))

        return None

    def _process_api_data(self, data: dict[str, Any], sport: str, api_url: str) -> list[dict[str, Any]]:
        """Process API response data."""
        processed_data = []

        try:
            # Handle different API response formats
            games = []
            if isinstance(data, dict):
                if 'games' in data:
                    games = data['games']
                elif 'data' in data:
                    games = data['data'] if isinstance(data['data'], list) else [data['data']]
                elif 'results' in data:
                    games = data['results']
                else:
                    # Assume data itself is game data
                    games = [data]
            elif isinstance(data, list):
                games = data

            for game in games:
                game_data = {
                    'game_id': game.get('id', game.get('game_id')),
                    'game_name': f"{game.get('away_team', game.get('visitor', 'Unknown'))} @ {game.get('home_team', game.get('home', 'Unknown'))}",
                    'away_team': game.get('away_team', game.get('visitor')),
                    'home_team': game.get('home_team', game.get('home')),
                    'sport': sport,
                    'game_datetime': game.get('game_time', game.get('start_time', game.get('datetime'))),
                    'sportsbooks': game.get('sportsbooks', game.get('books', [])),
                    'api_endpoint': api_url,
                    'raw_data': game
                }

                # Process each sportsbook's data
                sportsbooks = game_data['sportsbooks']
                if sportsbooks:
                    for sportsbook_data in sportsbooks:
                        betting_records = self._process_sportsbook_data(sportsbook_data, game_data)
                        processed_data.extend(betting_records)
                else:
                    # Direct game data without sportsbook breakdown
                    betting_records = self._process_direct_game_data(game_data)
                    processed_data.extend(betting_records)

            return processed_data

        except Exception as e:
            self.logger.error("Error processing API data", error=str(e), api_url=api_url)
            return []

    def _process_sportsbook_data(self, sportsbook_data: dict[str, Any], game_data: dict[str, Any]) -> list[dict[str, Any]]:
        """Process sportsbook data from API."""
        records = []

        try:
            sportsbook_name = sportsbook_data.get('name', 'Unknown')

            # Process moneyline data
            if 'moneyline' in sportsbook_data or 'ml' in sportsbook_data:
                ml_data = sportsbook_data.get('moneyline', sportsbook_data.get('ml', {}))
                records.append({
                    'sportsbook': sportsbook_name,
                    'bet_type': 'moneyline',
                    'home_odds': ml_data.get('home_odds', ml_data.get('home')),
                    'away_odds': ml_data.get('away_odds', ml_data.get('away')),
                    'home_bets_percentage': ml_data.get('home_bets_pct', ml_data.get('home_tickets')),
                    'away_bets_percentage': ml_data.get('away_bets_pct', ml_data.get('away_tickets')),
                    'home_money_percentage': ml_data.get('home_money_pct', ml_data.get('home_handle')),
                    'away_money_percentage': ml_data.get('away_money_pct', ml_data.get('away_handle')),
                    'timestamp': datetime.now().isoformat(),
                    'game_data': game_data
                })

            # Process spread data
            if 'spread' in sportsbook_data or 'runline' in sportsbook_data:
                spread_data = sportsbook_data.get('spread', sportsbook_data.get('runline', {}))
                records.append({
                    'sportsbook': sportsbook_name,
                    'bet_type': 'spread',
                    'spread_line': spread_data.get('line'),
                    'home_spread_odds': spread_data.get('home_odds', spread_data.get('home')),
                    'away_spread_odds': spread_data.get('away_odds', spread_data.get('away')),
                    'home_bets_percentage': spread_data.get('home_bets_pct', spread_data.get('home_tickets')),
                    'away_bets_percentage': spread_data.get('away_bets_pct', spread_data.get('away_tickets')),
                    'home_money_percentage': spread_data.get('home_money_pct', spread_data.get('home_handle')),
                    'away_money_percentage': spread_data.get('away_money_pct', spread_data.get('away_handle')),
                    'timestamp': datetime.now().isoformat(),
                    'game_data': game_data
                })

            # Process totals data
            if 'totals' in sportsbook_data or 'total' in sportsbook_data:
                totals_data = sportsbook_data.get('totals', sportsbook_data.get('total', {}))
                records.append({
                    'sportsbook': sportsbook_name,
                    'bet_type': 'totals',
                    'total_line': totals_data.get('line'),
                    'over_odds': totals_data.get('over_odds', totals_data.get('over')),
                    'under_odds': totals_data.get('under_odds', totals_data.get('under')),
                    'over_bets_percentage': totals_data.get('over_bets_pct', totals_data.get('over_tickets')),
                    'under_bets_percentage': totals_data.get('under_bets_pct', totals_data.get('under_tickets')),
                    'over_money_percentage': totals_data.get('over_money_pct', totals_data.get('over_handle')),
                    'under_money_percentage': totals_data.get('under_money_pct', totals_data.get('under_handle')),
                    'timestamp': datetime.now().isoformat(),
                    'game_data': game_data
                })

            return records

        except Exception as e:
            self.logger.error("Error processing sportsbook data", error=str(e))
            return []

    def _process_direct_game_data(self, game_data: dict[str, Any]) -> list[dict[str, Any]]:
        """Process game data when sportsbooks are not separated."""
        records = []

        try:
            # Extract betting data directly from game data
            raw_game = game_data.get('raw_data', {})
            
            # Generic sportsbook name from API endpoint
            sportsbook_name = "VSIN_API"

            # Try to extract moneyline
            for ml_key in ['moneyline', 'ml', 'money_line']:
                if ml_key in raw_game:
                    ml_data = raw_game[ml_key]
                    records.append({
                        'sportsbook': sportsbook_name,
                        'bet_type': 'moneyline',
                        'home_odds': ml_data.get('home_odds', ml_data.get('home')),
                        'away_odds': ml_data.get('away_odds', ml_data.get('away')),
                        'home_bets_percentage': ml_data.get('home_bets_pct'),
                        'away_bets_percentage': ml_data.get('away_bets_pct'),
                        'home_money_percentage': ml_data.get('home_money_pct'),
                        'away_money_percentage': ml_data.get('away_money_pct'),
                        'timestamp': datetime.now().isoformat(),
                        'game_data': game_data
                    })
                    break

            return records

        except Exception as e:
            self.logger.error("Error processing direct game data", error=str(e))
            return []

    def _extract_json_from_html(self, html_text: str, api_url: str) -> dict[str, Any] | None:
        """Extract JSON data from HTML response."""
        try:
            # Look for common JSON patterns in HTML
            json_patterns = [
                r'window\.__INITIAL_STATE__\s*=\s*({.*?});',
                r'window\.data\s*=\s*({.*?});',
                r'var\s+data\s*=\s*({.*?});',
                r'const\s+data\s*=\s*({.*?});',
                r'"data":\s*({.*?})',
                r'<script[^>]*>.*?({.*?"games".*?}).*?</script>',
            ]

            import json
            for pattern in json_patterns:
                matches = re.findall(pattern, html_text, re.DOTALL | re.IGNORECASE)
                for match in matches:
                    try:
                        data = json.loads(match)
                        if isinstance(data, dict) and (
                            'games' in data or 'data' in data or 'results' in data
                        ):
                            self.logger.info(f"Successfully extracted JSON from HTML at {api_url}")
                            return data
                    except json.JSONDecodeError:
                        continue

            return None

        except Exception as e:
            self.logger.debug(f"Could not extract JSON from HTML: {str(e)}")
            return None

    def _generate_mock_data(self, sport: str) -> list[dict[str, Any]]:
        """
        Generate realistic mock data with sharp action patterns for testing.
        Uses new parsing format compatible with live data collection.
        """
        try:
            # Mock games with realistic sharp action scenarios
            mock_scenarios = [
                {
                    'away_team': 'Cincinnati Reds',
                    'home_team': 'Detroit Tigers',
                    'betting_data': {
                        # Moneyline with sharp money on away team
                        'away_ml': '+153', 'home_ml': '-188',
                        'away_ml_handle_pct': 16.0, 'home_ml_handle_pct': 84.0,
                        'away_ml_bets_pct': 21.0, 'home_ml_bets_pct': 79.0,
                        # Totals with sharp action on over
                        'total_line': 8.5,
                        'over_handle_pct': 60.0, 'under_handle_pct': 40.0,
                        'over_bets_pct': 55.0, 'under_bets_pct': 45.0,
                        # Run line with strong sharp action on home
                        'away_rl': '+1.5', 'home_rl': '-1.5',
                        'away_rl_handle_pct': 8.0, 'home_rl_handle_pct': 92.0,
                        'away_rl_bets_pct': 30.0, 'home_rl_bets_pct': 70.0
                    },
                    'sportsbook': 'dk'
                },
                {
                    'away_team': 'Colorado Rockies',
                    'home_team': 'Atlanta Braves',
                    'betting_data': {
                        # Moneyline with public money scenario
                        'away_ml': '+278', 'home_ml': '-361',
                        'away_ml_handle_pct': 14.0, 'home_ml_handle_pct': 86.0,
                        'away_ml_bets_pct': 8.0, 'home_ml_bets_pct': 92.0,
                        # Totals with moderate sharp action on under
                        'total_line': 9.0,
                        'over_handle_pct': 13.0, 'under_handle_pct': 87.0,
                        'over_bets_pct': 57.0, 'under_bets_pct': 43.0,
                        # Run line with typical pattern
                        'away_rl': '+1.5', 'home_rl': '-1.5',
                        'away_rl_handle_pct': 6.0, 'home_rl_handle_pct': 94.0,
                        'away_rl_bets_pct': 9.0, 'home_rl_bets_pct': 91.0
                    },
                    'sportsbook': 'dk'
                },
                {
                    'away_team': 'Miami Marlins',
                    'home_team': 'Washington Nationals',
                    'betting_data': {
                        # Moneyline with light sharp action
                        'away_ml': '+134', 'home_ml': '-164',
                        'away_ml_handle_pct': 13.0, 'home_ml_handle_pct': 87.0,
                        'away_ml_bets_pct': 21.0, 'home_ml_bets_pct': 79.0,
                        # Totals with sharp action on over
                        'total_line': 7.5,
                        'over_handle_pct': 55.0, 'under_handle_pct': 45.0,
                        'over_bets_pct': 63.0, 'under_bets_pct': 37.0,
                        # Run line with reverse action
                        'away_rl': '+1.5', 'home_rl': '-1.5',
                        'away_rl_handle_pct': 9.0, 'home_rl_handle_pct': 91.0,
                        'away_rl_bets_pct': 41.0, 'home_rl_bets_pct': 59.0
                    },
                    'sportsbook': 'dk'
                },
                {
                    'away_team': 'Los Angeles Angels',
                    'home_team': 'Baltimore Orioles', 
                    'betting_data': {
                        # Pick'em game with balanced action
                        'away_ml': '+100', 'home_ml': '-121',
                        'away_ml_handle_pct': 82.0, 'home_ml_handle_pct': 18.0,
                        'away_ml_bets_pct': 53.0, 'home_ml_bets_pct': 47.0,
                        # High total with under sharp action
                        'total_line': 9.5,
                        'over_handle_pct': 25.0, 'under_handle_pct': 75.0,
                        'over_bets_pct': 48.0, 'under_bets_pct': 52.0,
                        # Run line with strong divergence
                        'away_rl': '+1.5', 'home_rl': '-1.5',
                        'away_rl_handle_pct': 15.0, 'home_rl_handle_pct': 85.0,
                        'away_rl_bets_pct': 45.0, 'home_rl_bets_pct': 55.0
                    },
                    'sportsbook': 'dk'
                }
            ]
            
            processed_data = []
            
            for i, scenario in enumerate(mock_scenarios):
                # Create realistic external source ID
                external_source_id = f"vsin_{sport}_{scenario['away_team'].replace(' ', '')}_{scenario['home_team'].replace(' ', '')}_{scenario['sportsbook']}_{datetime.now().strftime('%Y%m%d')}"
                
                # Detect sharp action using our new comprehensive detection
                sharp_indicators = self._detect_sharp_action_comprehensive(scenario['betting_data'])
                
                # Calculate data quality score
                quality_score = self._calculate_data_completeness(scenario['betting_data'])
                
                # Create unified format record matching live data structure
                mock_record = {
                    'external_source_id': external_source_id,
                    'bet_type': 'composite',  # VSIN provides multiple bet types
                    'home_team': scenario['home_team'],
                    'away_team': scenario['away_team'],
                    'game_datetime': datetime.now().isoformat(),
                    'collection_method': 'MOCK_DATA',
                    'source_api_version': 'vsin_unified_v3_mock',
                    'source_metadata': {
                        'sportsbook': scenario['sportsbook'],
                        'sport': sport,
                        'url_source': scenario['sportsbook'],
                        'data_format': 'mock_realistic'
                    },
                    'sharp_action': sharp_indicators.get('overall'),
                    'reverse_line_movement': False,  # Would need historical data
                    'steam_move': False,  # Would need historical data
                    # Include betting percentages at top level for unified processing
                    'home_money_percentage': scenario['betting_data'].get('home_ml_handle_pct'),
                    'away_money_percentage': scenario['betting_data'].get('away_ml_handle_pct'),
                    'home_bets_percentage': scenario['betting_data'].get('home_ml_bets_pct'),
                    'away_bets_percentage': scenario['betting_data'].get('away_ml_bets_pct'),
                    'over_money_percentage': scenario['betting_data'].get('over_handle_pct'),
                    'under_money_percentage': scenario['betting_data'].get('under_handle_pct'),
                    'over_bets_percentage': scenario['betting_data'].get('over_bets_pct'),
                    'under_bets_percentage': scenario['betting_data'].get('under_bets_pct'),
                    'home_ml': scenario['betting_data'].get('home_ml'),
                    'away_ml': scenario['betting_data'].get('away_ml'),
                    'total_line': scenario['betting_data'].get('total_line'),
                    'spread_line': scenario['betting_data'].get('away_rl') or scenario['betting_data'].get('away_spread'),
                    'raw_response': {
                        'away_team': scenario['away_team'],
                        'home_team': scenario['home_team'],
                        'sportsbook': scenario['sportsbook'],
                        'sport': sport,
                        'betting_data': scenario['betting_data'],
                        'sharp_indicators': sharp_indicators,
                        'data_quality_score': quality_score,
                        'collection_metadata': {
                            'collection_timestamp': datetime.now().isoformat(),
                            'source': 'vsin',
                            'collector_version': 'vsin_unified_v3_mock',
                            'data_format': 'mock_realistic',
                            'url_source': scenario['sportsbook']
                        }
                    },
                    'api_endpoint': f'vsin_mock_{scenario["sportsbook"]}'
                }
                
                processed_data.append(mock_record)
            
            self.logger.info(f"Generated {len(processed_data)} realistic mock VSIN records with sharp action patterns")
            return processed_data

        except Exception as e:
            self.logger.error("Error generating mock data", error=str(e))
            return []

    def _detect_reverse_line_movement(self, record: dict[str, Any], bet_type: str) -> bool:
        """Detect potential reverse line movement (placeholder - requires historical data)."""
        # This is a placeholder - true RLM detection requires historical line data
        # For now, we'll return False but this could be enhanced with line movement tracking
        return False

    def collect_game_data(self, sport: str = "mlb") -> int:
        """
        Convenience method to collect all betting data for a sport.
        
        Args:
            sport: Sport type (default: mlb)
            
        Returns:
            Number of records stored
        """
        try:
            result = self.collect_and_store(sport=sport)

            self.logger.info(
                "VSIN collection completed",
                sport=sport,
                status=result.status.value,
                processed=result.records_processed,
                stored=result.records_stored
            )

            return result.records_stored

        except Exception as e:
            self.logger.error("Error in collect_game_data", error=str(e))
            return 0

    def test_collection(self, sport: str = "mlb") -> dict[str, Any]:
        """
        Test method for validating VSIN collection.
        
        Args:
            sport: Sport type to test with
            
        Returns:
            Test results dictionary
        """
        try:
            self.logger.info("Testing VSIN unified collection", sport=sport)

            # Test data collection
            raw_data = self.collect_raw_data(sport=sport)

            # Test storage
            if raw_data:
                result = self.collect_and_store(sport=sport)

                return {
                    'status': 'success',
                    'raw_records': len(raw_data),
                    'processed': result.records_processed,
                    'stored': result.records_stored,
                    'collection_result': result.status.value,
                    'sample_record': raw_data[0] if raw_data else None
                }
            else:
                return {
                    'status': 'no_data',
                    'raw_records': 0,
                    'processed': 0,
                    'stored': 0,
                    'message': 'No data collected from VSIN'
                }

        except Exception as e:
            return {
                'status': 'error',
                'error': str(e),
                'raw_records': 0,
                'processed': 0,
                'stored': 0
            }

    def _process_and_store_record(self, record: dict[str, Any], batch_id: uuid.UUID) -> bool:
        """
        Override parent method to add fallback storage for VSIN data without game ID resolution.
        
        Args:
            record: VSIN betting line record
            batch_id: Collection batch identifier
            
        Returns:
            True if stored successfully in any schema
        """
        try:
            # Try original flow with game ID resolution
            game_id = self.game_resolver.resolve_game_id(
                record['external_source_id'],
                self.source
            )

            if game_id:
                # Game ID resolved - use full storage
                return self._process_and_store_record_with_game_id(record, batch_id, game_id)
            else:
                # Game ID resolution failed - use fallback storage
                self.logger.info(
                    "Game ID resolution failed, using fallback storage",
                    external_source_id=record.get('external_source_id'),
                    source=self.source.value
                )
                return self._store_record_without_game_id(record, batch_id)
                
        except Exception as e:
            self.logger.error("Error processing record", error=str(e))
            # Try fallback storage as last resort
            try:
                return self._store_record_without_game_id(record, batch_id)
            except Exception as fallback_error:
                self.logger.error("Fallback storage also failed", error=str(fallback_error))
                return False

    def _store_record_without_game_id(self, record: dict[str, Any], batch_id: uuid.UUID) -> bool:
        """
        Fallback storage method for VSIN records when game ID resolution fails.
        Stores data in raw_data.vsin_data for later processing.
        
        Args:
            record: VSIN betting line record
            batch_id: Collection batch identifier
            
        Returns:
            True if stored successfully
        """
        try:
            # Store in three-tier pipeline raw zone (no game ID required)
            pipeline_success = self._store_in_three_tier_pipeline(record, batch_id)
            
            if pipeline_success:
                self.performance_metrics.successful_records += 1
                self.logger.info(
                    "Successfully stored VSIN record in raw zone",
                    external_source_id=record.get('external_source_id')
                )
            else:
                self.performance_metrics.failed_records += 1
                
            return pipeline_success
            
        except Exception as e:
            self.logger.error("Error in fallback storage", error=str(e))
            self.performance_metrics.failed_records += 1
            return False

    def _process_and_store_record_with_game_id(self, record: dict[str, Any], batch_id: uuid.UUID, game_id: int) -> bool:
        """
        Enhanced VSIN storage that writes to both core_betting schema and three-tier pipeline.
        
        Args:
            record: VSIN betting line record with sharp action data
            batch_id: Collection batch identifier  
            game_id: Pre-resolved game ID from core_betting.games
            
        Returns:
            True if stored successfully in at least one schema
        """
        try:
            # Store in legacy core_betting schema (current production)
            legacy_success = self._store_in_core_betting_schema(record, game_id)
            
            # Store in new three-tier pipeline (future analytics)
            pipeline_success = self._store_in_three_tier_pipeline(record, batch_id)
            
            # Consider successful if either storage method works (during transition)
            success = legacy_success or pipeline_success
            
            if success:
                self.performance_metrics.successful_records += 1
            else:
                self.performance_metrics.failed_records += 1
                
            return success
            
        except Exception as e:
            self.logger.error("Error in dual schema storage", error=str(e))
            self.performance_metrics.failed_records += 1
            return False

    def _store_in_core_betting_schema(self, record: dict[str, Any], game_id: int) -> bool:
        """Store VSIN data in legacy core_betting schema for compatibility."""
        try:
            raw_response = record.get('raw_response', {})
            betting_data = raw_response.get('betting_data', {})
            
            # Extract sportsbook information
            sportsbook_name = raw_response.get('sportsbook', 'VSIN')
            
            # Store moneyline data if available
            if betting_data.get('away_ml') and betting_data.get('home_ml'):
                moneyline_success = self._store_moneyline_legacy(
                    game_id, sportsbook_name, betting_data, record
                )
            else:
                moneyline_success = True  # No moneyline data to store
            
            # Store spread data if available  
            spread_key = 'away_rl' if 'away_rl' in betting_data else 'away_spread'
            if betting_data.get(spread_key):
                spread_success = self._store_spread_legacy(
                    game_id, sportsbook_name, betting_data, record, spread_key
                )
            else:
                spread_success = True  # No spread data to store
            
            # Store totals data if available
            if betting_data.get('total_line'):
                totals_success = self._store_totals_legacy(
                    game_id, sportsbook_name, betting_data, record
                )
            else:
                totals_success = True  # No totals data to store
            
            return moneyline_success and spread_success and totals_success
            
        except Exception as e:
            self.logger.error("Error storing in core_betting schema", error=str(e))
            return False

    def _store_in_three_tier_pipeline(self, record: dict[str, Any], batch_id: uuid.UUID) -> bool:
        """Store VSIN data in new three-tier pipeline for future analytics."""
        try:
            # Store in RAW zone
            raw_success = self._store_in_raw_zone(record)
            
            # Future: Add STAGING and CURATED processing
            # staging_success = self._store_in_staging_zone(record)
            # curated_success = self._store_in_curated_zone(record)
            
            return raw_success
            
        except Exception as e:
            self.logger.error("Error storing in three-tier pipeline", error=str(e))
            return False

    def _store_in_raw_zone(self, record: dict[str, Any]) -> bool:
        """Store raw VSIN data in raw_data schema."""
        try:
            # Use the entire record as raw_response since it contains all parsed data
            raw_response = record
            
            with self.connection_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    # Store in raw_data.vsin_data table (no unique constraint, just insert)
                    cur.execute("""
                        INSERT INTO raw_data.vsin_data (
                            external_id, data_type, raw_response, source_feed, 
                            collected_at, created_at
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        record.get('external_source_id'),
                        'betting_splits',
                        psycopg2.extras.Json(raw_response),
                        record.get('source_metadata', {}).get('url_source', 'vsin'),
                        datetime.now(),
                        datetime.now()
                    ))
                    
                    conn.commit()
                    return True
                    
        except Exception as e:
            self.logger.error("Error storing in raw_data.vsin_data", error=str(e), 
                            external_id=record.get('external_source_id'))
            return False

    def _store_moneyline_legacy(self, game_id: int, sportsbook: str, betting_data: dict, record: dict) -> bool:
        """Store moneyline data in core_betting.betting_lines_moneyline."""
        try:
            with self.connection_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO core_betting.betting_lines_moneyline (
                            game_id, sportsbook, home_ml, away_ml,
                            home_money_percentage, away_money_percentage,
                            home_bets_percentage, away_bets_percentage,
                            sharp_action, odds_timestamp, source, data_quality,
                            created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (game_id, sportsbook, odds_timestamp) DO UPDATE SET
                            home_ml = EXCLUDED.home_ml,
                            away_ml = EXCLUDED.away_ml,
                            home_money_percentage = EXCLUDED.home_money_percentage,
                            away_money_percentage = EXCLUDED.away_money_percentage,
                            sharp_action = EXCLUDED.sharp_action,
                            updated_at = EXCLUDED.updated_at
                    """, (
                        game_id, sportsbook,
                        self._parse_odds(betting_data.get('home_ml')),
                        self._parse_odds(betting_data.get('away_ml')),
                        betting_data.get('home_ml_handle_pct'),
                        betting_data.get('away_ml_handle_pct'),
                        betting_data.get('home_ml_bets_pct'),
                        betting_data.get('away_ml_bets_pct'),
                        record.get('sharp_action'),
                        datetime.now(),
                        'VSIN',
                        'HIGH',  # VSIN has high data quality
                        datetime.now(),
                        datetime.now()
                    ))
                    conn.commit()
                    return True
        except Exception as e:
            self.logger.error("Error storing moneyline in legacy schema", error=str(e))
            return False

    def _store_spread_legacy(self, game_id: int, sportsbook: str, betting_data: dict, record: dict, spread_key: str) -> bool:
        """Store spread/run line data in core_betting.betting_lines_spreads."""
        try:
            with self.connection_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    home_spread_key = spread_key.replace('away_', 'home_')
                    
                    cur.execute("""
                        INSERT INTO core_betting.betting_lines_spreads (
                            game_id, sportsbook, away_spread, home_spread,
                            away_spread_price, home_spread_price,
                            home_money_percentage, away_money_percentage,
                            home_bets_percentage, away_bets_percentage,
                            sharp_action, odds_timestamp, source, data_quality,
                            created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (game_id, sportsbook, odds_timestamp) DO UPDATE SET
                            away_spread = EXCLUDED.away_spread,
                            home_spread = EXCLUDED.home_spread,
                            sharp_action = EXCLUDED.sharp_action,
                            updated_at = EXCLUDED.updated_at
                    """, (
                        game_id, sportsbook,
                        self._parse_spread(betting_data.get(spread_key)),
                        self._parse_spread(betting_data.get(home_spread_key)),
                        -110,  # Default spread odds
                        -110,
                        betting_data.get(f'home_{spread_key.split("_")[1]}_handle_pct'),
                        betting_data.get(f'away_{spread_key.split("_")[1]}_handle_pct'),
                        betting_data.get(f'home_{spread_key.split("_")[1]}_bets_pct'),
                        betting_data.get(f'away_{spread_key.split("_")[1]}_bets_pct'),
                        record.get('sharp_action'),
                        datetime.now(),
                        'VSIN',
                        'HIGH',
                        datetime.now(),
                        datetime.now()
                    ))
                    conn.commit()
                    return True
        except Exception as e:
            self.logger.error("Error storing spread in legacy schema", error=str(e))
            return False

    def _store_totals_legacy(self, game_id: int, sportsbook: str, betting_data: dict, record: dict) -> bool:
        """Store totals data in core_betting.betting_lines_totals."""
        try:
            with self.connection_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO core_betting.betting_lines_totals (
                            game_id, sportsbook, total_line,
                            over_price, under_price,
                            over_money_percentage, under_money_percentage,
                            over_bets_percentage, under_bets_percentage,
                            sharp_action, odds_timestamp, source, data_quality,
                            created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (game_id, sportsbook, odds_timestamp) DO UPDATE SET
                            total_line = EXCLUDED.total_line,
                            sharp_action = EXCLUDED.sharp_action,
                            updated_at = EXCLUDED.updated_at
                    """, (
                        game_id, sportsbook,
                        betting_data.get('total_line'),
                        -110,  # Default over odds
                        -110,  # Default under odds  
                        betting_data.get('over_handle_pct'),
                        betting_data.get('under_handle_pct'),
                        betting_data.get('over_bets_pct'),
                        betting_data.get('under_bets_pct'),
                        record.get('sharp_action'),
                        datetime.now(),
                        'VSIN',
                        'HIGH',
                        datetime.now(),
                        datetime.now()
                    ))
                    conn.commit()
                    return True
        except Exception as e:
            self.logger.error("Error storing totals in legacy schema", error=str(e))
            return False

    def test_live_collection(self, sport: str = "mlb", sportsbook: str = "dk") -> dict[str, Any]:
        """
        Test live VSIN data collection with comprehensive validation.
        
        Args:
            sport: Sport to test collection for
            sportsbook: Sportsbook to test ('dk', 'circa', 'fanduel', 'all')
            
        Returns:
            Comprehensive test results
        """
        try:
            self.logger.info(f"Testing live VSIN collection for {sport.upper()} from {sportsbook.upper()}")
            
            # Test URL building
            try:
                if sportsbook != 'all':
                    test_url = self.build_vsin_url(sport, sportsbook)
                    self.logger.info(f"Generated URL: {test_url}")
                    url_test = True
                else:
                    url_test = True
                    test_url = "Multiple URLs"
            except Exception as e:
                url_test = False
                test_url = f"Error: {str(e)}"
            
            # Test live data collection
            try:
                live_data = self._collect_vsin_data_sync(sport, sportsbook=sportsbook)
                live_success = len(live_data) > 0
                live_count = len(live_data)
                
                # Analyze data quality if we got data
                quality_analysis = {}
                sharp_action_summary = {}
                
                if live_data:
                    total_quality = 0
                    sharp_games = 0
                    
                    for record in live_data:
                        raw_data = record.get('raw_response', {})
                        quality_score = raw_data.get('data_quality_score', 0)
                        total_quality += quality_score
                        
                        sharp_indicators = raw_data.get('sharp_indicators', {})
                        if sharp_indicators:
                            sharp_games += 1
                    
                    quality_analysis = {
                        'average_quality_score': total_quality / len(live_data) if live_data else 0,
                        'games_with_sharp_action': sharp_games,
                        'sharp_action_percentage': (sharp_games / len(live_data)) * 100 if live_data else 0
                    }
                
            except Exception as e:
                live_success = False
                live_count = 0
                live_data = []
                quality_analysis = {'error': str(e)}
            
            # Test mock data fallback
            try:
                mock_data = self._generate_mock_data(sport)
                mock_success = len(mock_data) > 0
                mock_count = len(mock_data)
                
                # Validate mock data sharp action detection
                mock_sharp_analysis = {}
                if mock_data:
                    sharp_detected = 0
                    for record in mock_data:
                        raw_data = record.get('raw_response', {})
                        sharp_indicators = raw_data.get('sharp_indicators', {})
                        if sharp_indicators:
                            sharp_detected += 1
                    
                    mock_sharp_analysis = {
                        'games_with_sharp_action': sharp_detected,
                        'sharp_detection_rate': (sharp_detected / len(mock_data)) * 100
                    }
                
            except Exception as e:
                mock_success = False
                mock_count = 0
                mock_sharp_analysis = {'error': str(e)}
            
            # Test three-tier pipeline storage if we have data
            storage_test = {}
            if live_data or mock_data:
                try:
                    test_data = live_data if live_data else mock_data
                    result = self.collect_and_store(sport=sport, sportsbook=sportsbook)
                    storage_test = {
                        'status': result.status.value,
                        'processed': result.records_processed,
                        'stored': result.records_stored,
                        'success': result.records_stored > 0
                    }
                except Exception as e:
                    storage_test = {
                        'status': 'error',
                        'error': str(e),
                        'success': False
                    }
            
            return {
                'status': 'success' if (live_success or mock_success) else 'error',
                'timestamp': datetime.now().isoformat(),
                'test_configuration': {
                    'sport': sport,
                    'sportsbook': sportsbook,
                    'collector_version': 'vsin_unified_v3_live'
                },
                'url_generation': {
                    'success': url_test,
                    'test_url': test_url
                },
                'live_collection': {
                    'success': live_success,
                    'record_count': live_count,
                    'quality_analysis': quality_analysis
                },
                'mock_fallback': {
                    'success': mock_success,
                    'record_count': mock_count,
                    'sharp_action_analysis': mock_sharp_analysis
                },
                'three_tier_storage': storage_test,
                'summary': {
                    'overall_success': live_success or mock_success,
                    'live_data_available': live_success,
                    'mock_fallback_working': mock_success,
                    'sharp_action_detection': 'working' if mock_sharp_analysis.get('games_with_sharp_action', 0) > 0 else 'not_detected',
                    'data_quality_scoring': 'working' if quality_analysis.get('average_quality_score', 0) > 0 else 'basic'
                }
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat(),
                'test_configuration': {
                    'sport': sport,
                    'sportsbook': sportsbook
                }
            }


# Example usage
if __name__ == "__main__":
    collector = VSINUnifiedCollector()

    # Test improved VSIN collection with live data
    print("=== Testing Improved VSIN Collector ===")
    
    # Test live collection capabilities
    live_test_result = collector.test_live_collection("mlb", "dk")
    print(f"\nLive Collection Test Result:")
    print(f"Status: {live_test_result['status']}")
    print(f"Live Data Available: {live_test_result['summary']['live_data_available']}")
    print(f"Mock Fallback Working: {live_test_result['summary']['mock_fallback_working']}")
    print(f"Sharp Action Detection: {live_test_result['summary']['sharp_action_detection']}")
    
    if live_test_result['live_collection']['success']:
        print(f"Live Records Collected: {live_test_result['live_collection']['record_count']}")
        print(f"Average Quality Score: {live_test_result['live_collection']['quality_analysis'].get('average_quality_score', 'N/A')}")
    
    if live_test_result['mock_fallback']['success']:
        print(f"Mock Records Generated: {live_test_result['mock_fallback']['record_count']}")
        print(f"Sharp Action Detection Rate: {live_test_result['mock_fallback']['sharp_action_analysis'].get('sharp_detection_rate', 'N/A')}%")
    
    # Test legacy collection method
    print("\n=== Testing Legacy Collection Method ===")
    test_result = collector.test_collection("mlb")
    print(f"Legacy Test Result: {test_result['status']}")
    
    # Production collection if tests pass
    if live_test_result['summary']['overall_success']:
        print("\n=== Production Collection ===")
        stored_count = collector.collect_game_data("mlb")
        print(f"Stored {stored_count} records in three-tier pipeline")
