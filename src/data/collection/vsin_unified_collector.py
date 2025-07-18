#!/usr/bin/env python3
"""
VSIN Unified Collector

VSIN (Vegas Stats & Information Network) collector adapted to use the unified betting lines pattern.
Integrates with core_betting schema and provides standardized data quality tracking.
"""

import asyncio
import re
from datetime import datetime
from typing import Any

import structlog

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
        self.base_url = "https://www.vsin.com"
        self.bet_types = [
            {"data_format": "money-line", "name": "Money Line", "unified_type": "moneyline"},
            {"data_format": "run-line", "name": "Run Line", "unified_type": "spread"},
            {"data_format": "totals", "name": "Totals", "unified_type": "totals"}
        ]
        self.playwright_adapter = None

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
            # Use async context for web scraping
            return asyncio.run(self._collect_vsin_data_async(sport))

        except Exception as e:
            self.logger.error("Failed to collect VSIN data", sport=sport, error=str(e))
            return []

    async def _collect_vsin_data_async(self, sport: str) -> list[dict[str, Any]]:
        """Async collection of VSIN betting data."""
        all_data = []

        try:
            # Initialize Playwright adapter
            # Note: MCP browser automation no longer available
            self.playwright_adapter = None

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
            self.logger.error("Failed to collect VSIN data", error=str(e))
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
        bet_type: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Convert VSIN raw data to unified format.
        
        Args:
            betting_data: Raw betting data from VSIN
            game_data: Game information
            bet_type: Bet type configuration
            
        Returns:
            List of unified format records
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
                    'collection_method': 'WEB_SCRAPING',
                    'source_api_version': 'VSIN_v1',
                    'source_metadata': {
                        'original_bet_type': bet_type['data_format'],
                        'game_url': game_data.get('game_url'),
                        'extraction_method': 'playwright',
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


# Example usage
if __name__ == "__main__":
    collector = VSINUnifiedCollector()

    # Test collection
    test_result = collector.test_collection("mlb")
    print(f"Test result: {test_result}")

    # Production collection
    if test_result['status'] == 'success':
        stored_count = collector.collect_game_data("mlb")
        print(f"Stored {stored_count} records")
