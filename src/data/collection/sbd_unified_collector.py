#!/usr/bin/env python3
"""
SBD Unified Collector

Sports Betting Dime (SBD) collector adapted to use the unified betting lines pattern.
Integrates with core_betting schema and provides standardized data quality tracking.
"""

import asyncio
import re
from datetime import datetime
from typing import Any

import aiohttp
import structlog

from .base import DataSource
from .unified_betting_lines_collector import UnifiedBettingLinesCollector

# Note: MCP bridge removed - browser automation no longer available

logger = structlog.get_logger(__name__)


class SBDUnifiedCollector(UnifiedBettingLinesCollector):
    """
    SBD (Sports Betting Dime) collector using the unified betting lines pattern.
    
    Provides standardized integration with core_betting schema while maintaining
    compatibility with existing SBD data collection methods.
    """

    def __init__(self):
        super().__init__(DataSource.SPORTS_BETTING_DIME)
        self.base_url = "https://sportsbettingdime.com"
        self.bet_types = [
            {"data_format": "moneyline", "name": "Moneyline", "unified_type": "moneyline"},
            {"data_format": "spread", "name": "Spread", "unified_type": "spread"},
            {"data_format": "totals", "name": "Totals", "unified_type": "totals"}
        ]
        self.playwright_adapter = None
        self.session = None

    def collect_raw_data(self, sport: str = "mlb", **kwargs) -> list[dict[str, Any]]:
        """
        Collect raw betting data from SBD.
        
        Args:
            sport: Sport type (default: mlb)
            **kwargs: Additional parameters
            
        Returns:
            List of raw betting line dictionaries
        """
        try:
            # Use async context for data collection
            return asyncio.run(self._collect_sbd_data_async(sport))

        except Exception as e:
            self.logger.error("Failed to collect SBD data", sport=sport, error=str(e))
            return []

    async def _collect_sbd_data_async(self, sport: str) -> list[dict[str, Any]]:
        """Async collection of SBD betting data."""
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
            self.logger.error("Failed to collect SBD data", error=str(e))
            return []

    async def _collect_via_api(self, sport: str) -> list[dict[str, Any]]:
        """Collect data via SBD API if available."""
        try:
            # Initialize HTTP session
            timeout = aiohttp.ClientTimeout(total=30)
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Referer': f'{self.base_url}/',
            }

            async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
                self.session = session

                # Try to find API endpoints
                api_url = f"{self.base_url}/api/v1/{sport}/betting-splits"

                async with session.get(api_url) as response:
                    if response.status == 200:
                        data = await response.json()
                        return self._process_api_data(data, sport)
                    else:
                        self.logger.info(f"API not available, status: {response.status}")
                        return []

        except Exception as e:
            self.logger.info("API collection failed, will try scraping", error=str(e))
            return []

    async def _collect_via_scraping(self, sport: str) -> list[dict[str, Any]]:
        """Collect data via web scraping."""
        all_data = []

        try:
            # Initialize Playwright adapter
            # Note: MCP browser automation no longer available
            self.playwright_adapter = None

            # Navigate to SBD betting splits page
            url = f"{self.base_url}/{sport}/betting-splits"
            await self.playwright_adapter.goto(url)
            await self.playwright_adapter.wait_for_load_state('networkidle')

            # Extract today's games
            games_data = await self._extract_games_data(sport)
            self.logger.info(f"Found {len(games_data)} games on SBD")

            # For each game, collect betting data
            for game_data in games_data:
                try:
                    # Extract betting data for all bet types
                    betting_data = await self._extract_betting_data(game_data)

                    # Convert to unified format
                    unified_data = self._convert_to_unified_format(
                        betting_data, game_data
                    )

                    all_data.extend(unified_data)

                    self.logger.info(
                        f"Collected {len(unified_data)} unified records",
                        game=game_data.get('game_name')
                    )

                except Exception as e:
                    self.logger.error(
                        "Error processing game",
                        game=game_data.get('game_name'),
                        error=str(e)
                    )
                    continue

            return all_data

        except Exception as e:
            self.logger.error("Failed to collect SBD data via scraping", error=str(e))
            return []

        finally:
            if self.playwright_adapter:
                await self.playwright_adapter.close()

    def _process_api_data(self, data: dict[str, Any], sport: str) -> list[dict[str, Any]]:
        """Process API response data."""
        processed_data = []

        try:
            games = data.get('games', [])

            for game in games:
                game_data = {
                    'game_id': game.get('id'),
                    'game_name': f"{game.get('away_team', 'Unknown')} @ {game.get('home_team', 'Unknown')}",
                    'away_team': game.get('away_team'),
                    'home_team': game.get('home_team'),
                    'sport': sport,
                    'game_datetime': game.get('game_time'),
                    'sportsbooks': game.get('sportsbooks', [])
                }

                # Process each sportsbook's data
                for sportsbook_data in game_data['sportsbooks']:
                    betting_records = self._process_sportsbook_data(sportsbook_data, game_data)
                    processed_data.extend(betting_records)

            return processed_data

        except Exception as e:
            self.logger.error("Error processing API data", error=str(e))
            return []

    def _process_sportsbook_data(self, sportsbook_data: dict[str, Any], game_data: dict[str, Any]) -> list[dict[str, Any]]:
        """Process sportsbook data from API."""
        records = []

        try:
            sportsbook_name = sportsbook_data.get('name', 'Unknown')

            # Process moneyline data
            if 'moneyline' in sportsbook_data:
                ml_data = sportsbook_data['moneyline']
                records.append({
                    'sportsbook': sportsbook_name,
                    'bet_type': 'moneyline',
                    'home_odds': ml_data.get('home_odds'),
                    'away_odds': ml_data.get('away_odds'),
                    'home_bets_percentage': ml_data.get('home_bets_pct'),
                    'away_bets_percentage': ml_data.get('away_bets_pct'),
                    'home_money_percentage': ml_data.get('home_money_pct'),
                    'away_money_percentage': ml_data.get('away_money_pct'),
                    'timestamp': datetime.now().isoformat(),
                    'game_data': game_data
                })

            # Process spread data
            if 'spread' in sportsbook_data:
                spread_data = sportsbook_data['spread']
                records.append({
                    'sportsbook': sportsbook_name,
                    'bet_type': 'spread',
                    'spread_line': spread_data.get('line'),
                    'home_spread_odds': spread_data.get('home_odds'),
                    'away_spread_odds': spread_data.get('away_odds'),
                    'home_bets_percentage': spread_data.get('home_bets_pct'),
                    'away_bets_percentage': spread_data.get('away_bets_pct'),
                    'home_money_percentage': spread_data.get('home_money_pct'),
                    'away_money_percentage': spread_data.get('away_money_pct'),
                    'timestamp': datetime.now().isoformat(),
                    'game_data': game_data
                })

            # Process totals data
            if 'totals' in sportsbook_data:
                totals_data = sportsbook_data['totals']
                records.append({
                    'sportsbook': sportsbook_name,
                    'bet_type': 'totals',
                    'total_line': totals_data.get('line'),
                    'over_odds': totals_data.get('over_odds'),
                    'under_odds': totals_data.get('under_odds'),
                    'over_bets_percentage': totals_data.get('over_bets_pct'),
                    'under_bets_percentage': totals_data.get('under_bets_pct'),
                    'over_money_percentage': totals_data.get('over_money_pct'),
                    'under_money_percentage': totals_data.get('under_money_pct'),
                    'timestamp': datetime.now().isoformat(),
                    'game_data': game_data
                })

            return records

        except Exception as e:
            self.logger.error("Error processing sportsbook data", error=str(e))
            return []

    async def _extract_games_data(self, sport: str) -> list[dict[str, Any]]:
        """Extract today's games from SBD betting splits page."""
        try:
            # Extract games using JavaScript
            games_script = """
            (function() {
                const games = [];
                const gameElements = document.querySelectorAll('.game-card, .betting-game, .matchup-card, .game-row');
                
                gameElements.forEach(element => {
                    const gameNameElement = element.querySelector('.game-name, .matchup, .teams, .game-title');
                    const gameTimeElement = element.querySelector('.game-time, .start-time, .datetime');
                    const gameIdElement = element.querySelector('[data-game-id]');
                    
                    if (gameNameElement) {
                        const gameName = gameNameElement.textContent.trim();
                        const gameTime = gameTimeElement ? gameTimeElement.textContent.trim() : null;
                        const gameId = gameIdElement ? gameIdElement.getAttribute('data-game-id') : null;
                        
                        // Extract team names
                        const teams = gameName.split(/[@vs]/i).map(t => t.trim());
                        
                        games.push({
                            game_id: gameId,
                            game_name: gameName,
                            away_team: teams[0] || 'Unknown',
                            home_team: teams[1] || 'Unknown',
                            game_time: gameTime,
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

    async def _extract_betting_data(self, game_data: dict[str, Any]) -> list[dict[str, Any]]:
        """Extract betting data for a specific game."""
        try:
            # Extract betting data using JavaScript
            extraction_script = """
            (function() {
                const gameId = arguments[0];
                const data = [];
                
                // Find the game container
                const gameContainer = document.querySelector(`[data-game-id="${gameId}"]`) || 
                                    document.querySelector('.game-card, .betting-game, .matchup-card');
                
                if (!gameContainer) return data;
                
                // Extract sportsbook data
                const sportsbookElements = gameContainer.querySelectorAll('.sportsbook-row, .book-data, .odds-row');
                
                sportsbookElements.forEach(element => {
                    const sportsbook = element.querySelector('.sportsbook-name, .book-name, .book-logo')?.textContent?.trim() ||
                                     element.querySelector('img')?.getAttribute('alt') || 'Unknown';
                    
                    // Extract moneyline data
                    const mlHomeOdds = element.querySelector('.ml-home, .home-ml, .moneyline-home')?.textContent?.trim();
                    const mlAwayOdds = element.querySelector('.ml-away, .away-ml, .moneyline-away')?.textContent?.trim();
                    const mlHomeBets = element.querySelector('.ml-home-bets, .home-bets-pct')?.textContent?.trim();
                    const mlAwayBets = element.querySelector('.ml-away-bets, .away-bets-pct')?.textContent?.trim();
                    const mlHomeMoney = element.querySelector('.ml-home-money, .home-money-pct')?.textContent?.trim();
                    const mlAwayMoney = element.querySelector('.ml-away-money, .away-money-pct')?.textContent?.trim();
                    
                    if (mlHomeOdds || mlAwayOdds) {
                        data.push({
                            sportsbook: sportsbook,
                            bet_type: 'moneyline',
                            home_odds: mlHomeOdds,
                            away_odds: mlAwayOdds,
                            home_bets_percentage: mlHomeBets,
                            away_bets_percentage: mlAwayBets,
                            home_money_percentage: mlHomeMoney,
                            away_money_percentage: mlAwayMoney,
                            timestamp: new Date().toISOString()
                        });
                    }
                    
                    // Extract spread data
                    const spreadLine = element.querySelector('.spread-line, .spread-number')?.textContent?.trim();
                    const spreadHomeOdds = element.querySelector('.spread-home, .home-spread-odds')?.textContent?.trim();
                    const spreadAwayOdds = element.querySelector('.spread-away, .away-spread-odds')?.textContent?.trim();
                    const spreadHomeBets = element.querySelector('.spread-home-bets, .home-spread-bets')?.textContent?.trim();
                    const spreadAwayBets = element.querySelector('.spread-away-bets, .away-spread-bets')?.textContent?.trim();
                    const spreadHomeMoney = element.querySelector('.spread-home-money, .home-spread-money')?.textContent?.trim();
                    const spreadAwayMoney = element.querySelector('.spread-away-money, .away-spread-money')?.textContent?.trim();
                    
                    if (spreadLine) {
                        data.push({
                            sportsbook: sportsbook,
                            bet_type: 'spread',
                            spread_line: spreadLine,
                            home_spread_odds: spreadHomeOdds,
                            away_spread_odds: spreadAwayOdds,
                            home_bets_percentage: spreadHomeBets,
                            away_bets_percentage: spreadAwayBets,
                            home_money_percentage: spreadHomeMoney,
                            away_money_percentage: spreadAwayMoney,
                            timestamp: new Date().toISOString()
                        });
                    }
                    
                    // Extract totals data
                    const totalLine = element.querySelector('.total-line, .total-number, .ou-line')?.textContent?.trim();
                    const overOdds = element.querySelector('.over-odds, .over-price')?.textContent?.trim();
                    const underOdds = element.querySelector('.under-odds, .under-price')?.textContent?.trim();
                    const overBets = element.querySelector('.over-bets, .over-bets-pct')?.textContent?.trim();
                    const underBets = element.querySelector('.under-bets, .under-bets-pct')?.textContent?.trim();
                    const overMoney = element.querySelector('.over-money, .over-money-pct')?.textContent?.trim();
                    const underMoney = element.querySelector('.under-money, .under-money-pct')?.textContent?.trim();
                    
                    if (totalLine) {
                        data.push({
                            sportsbook: sportsbook,
                            bet_type: 'totals',
                            total_line: totalLine,
                            over_odds: overOdds,
                            under_odds: underOdds,
                            over_bets_percentage: overBets,
                            under_bets_percentage: underBets,
                            over_money_percentage: overMoney,
                            under_money_percentage: underMoney,
                            timestamp: new Date().toISOString()
                        });
                    }
                });
                
                return data;
            })();
            """

            result = await self.playwright_adapter.evaluate(extraction_script, game_data.get('game_id'))
            return result or []

        except Exception as e:
            self.logger.error("Error extracting betting data", error=str(e))
            return []

    def _convert_to_unified_format(
        self,
        betting_data: list[dict[str, Any]],
        game_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Convert SBD raw data to unified format.
        
        Args:
            betting_data: Raw betting data from SBD
            game_data: Game information
            
        Returns:
            List of unified format records
        """
        unified_records = []

        for record in betting_data:
            try:
                # Generate external source ID
                external_source_id = f"sbd_{game_data.get('game_id', game_data['game_name'].replace(' ', '_'))}_{record['sportsbook'].replace(' ', '_')}"

                # Base unified record
                unified_record = {
                    'external_source_id': external_source_id,
                    'sportsbook': record['sportsbook'],
                    'bet_type': record['bet_type'],
                    'odds_timestamp': record.get('timestamp', datetime.now().isoformat()),
                    'collection_method': 'WEB_SCRAPING',
                    'source_api_version': 'SBD_v1',
                    'source_metadata': {
                        'game_id': game_data.get('game_id'),
                        'extraction_method': 'playwright',
                        'sport': game_data.get('sport', 'mlb')
                    },
                    'game_datetime': game_data.get('game_time') or datetime.now().isoformat(),
                    'home_team': game_data.get('home_team'),
                    'away_team': game_data.get('away_team'),
                }

                # Add bet type specific fields
                if record['bet_type'] == 'moneyline':
                    unified_record.update({
                        'home_ml': self._parse_odds(record.get('home_odds')),
                        'away_ml': self._parse_odds(record.get('away_odds')),
                        'home_bets_percentage': self._parse_percentage(record.get('home_bets_percentage')),
                        'away_bets_percentage': self._parse_percentage(record.get('away_bets_percentage')),
                        'home_money_percentage': self._parse_percentage(record.get('home_money_percentage')),
                        'away_money_percentage': self._parse_percentage(record.get('away_money_percentage')),
                    })

                elif record['bet_type'] == 'spread':
                    unified_record.update({
                        'spread_line': self._parse_spread(record.get('spread_line')),
                        'home_spread_price': self._parse_odds(record.get('home_spread_odds')),
                        'away_spread_price': self._parse_odds(record.get('away_spread_odds')),
                        'home_bets_percentage': self._parse_percentage(record.get('home_bets_percentage')),
                        'away_bets_percentage': self._parse_percentage(record.get('away_bets_percentage')),
                        'home_money_percentage': self._parse_percentage(record.get('home_money_percentage')),
                        'away_money_percentage': self._parse_percentage(record.get('away_money_percentage')),
                    })

                elif record['bet_type'] == 'totals':
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
                sharp_action = self._detect_sharp_action(unified_record, record['bet_type'])
                if sharp_action:
                    unified_record['sharp_action'] = sharp_action

                # Detect reverse line movement
                rlm = self._detect_reverse_line_movement(unified_record, record['bet_type'])
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

    def _detect_reverse_line_movement(self, record: dict[str, Any], bet_type: str) -> bool:
        """Detect potential reverse line movement (placeholder - requires historical data)."""
        # This is a placeholder - true RLM detection requires historical line data
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
                "SBD collection completed",
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
        Test method for validating SBD collection.
        
        Args:
            sport: Sport type to test with
            
        Returns:
            Test results dictionary
        """
        try:
            self.logger.info("Testing SBD unified collection", sport=sport)

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
                    'message': 'No data collected from SBD'
                }

        except Exception as e:
            return {
                'status': 'error',
                'error': str(e),
                'raw_records': 0,
                'processed': 0,
                'stored': 0
            }


# Example usage
if __name__ == "__main__":
    collector = SBDUnifiedCollector()

    # Test collection
    test_result = collector.test_collection("mlb")
    print(f"Test result: {test_result}")

    # Production collection
    if test_result['status'] == 'success':
        stored_count = collector.collect_game_data("mlb")
        print(f"Stored {stored_count} records")
