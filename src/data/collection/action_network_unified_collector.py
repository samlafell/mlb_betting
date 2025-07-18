#!/usr/bin/env python3
"""
Action Network Unified Collector

Action Network collector enhanced to use the unified betting lines pattern.
Integrates with core_betting schema and provides comprehensive betting data collection.
"""

import asyncio
from typing import Any

import aiohttp
import structlog

from ...core.datetime_utils import (
    collection_timestamp,
    prepare_for_postgres,
)
from .base import DataSource
from .unified_betting_lines_collector import UnifiedBettingLinesCollector

logger = structlog.get_logger(__name__)


class ActionNetworkUnifiedCollector(UnifiedBettingLinesCollector):
    """
    Action Network collector using the unified betting lines pattern.
    
    Enhanced version that provides comprehensive betting data collection
    with standardized integration with core_betting schema.
    """

    def __init__(self):
        super().__init__(DataSource.ACTION_NETWORK)
        self.api_base = "https://api.actionnetwork.com"
        self.web_base = "https://www.actionnetwork.com"
        self.session = None

        # Headers for Action Network API requests
        self.headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.actionnetwork.com/",
            "Origin": "https://www.actionnetwork.com",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }

    def collect_raw_data(self, date: str = None, sport: str = "mlb", **kwargs) -> list[dict[str, Any]]:
        """
        Collect raw betting data from Action Network.
        
        Args:
            date: Date in YYYYMMDD format (default: today)
            sport: Sport type (default: mlb)
            **kwargs: Additional parameters
            
        Returns:
            List of raw betting line dictionaries
        """
        try:
            # Use async context for API calls
            return asyncio.run(self._collect_action_network_data_async(date, sport, **kwargs))

        except Exception as e:
            self.logger.error("Failed to collect Action Network data", date=date, sport=sport, error=str(e))
            return []

    async def _collect_action_network_data_async(self, date: str, sport: str, **kwargs) -> list[dict[str, Any]]:
        """Async collection of Action Network betting data."""
        all_data = []

        try:
            # Initialize HTTP session
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(headers=self.headers, timeout=timeout) as session:
                self.session = session

                # Get today's date if not provided
                if not date:
                    date = collection_timestamp().strftime("%Y%m%d")

                # Step 1: Get today's games
                games_data = await self._fetch_games_data(date, sport)
                self.logger.info(f"Found {len(games_data)} games from Action Network")

                # Step 2: For each game, collect comprehensive betting data
                for game_data in games_data:
                    try:
                        # Collect betting lines for all bet types
                        betting_data = await self._collect_game_betting_data(game_data)

                        # Convert to unified format
                        unified_data = self._convert_to_unified_format(betting_data, game_data)

                        all_data.extend(unified_data)

                        self.logger.info(
                            f"Collected {len(unified_data)} unified records",
                            game_id=game_data.get('id'),
                            matchup=f"{game_data.get('away_team', {}).get('abbreviation', 'UNK')} @ {game_data.get('home_team', {}).get('abbreviation', 'UNK')}"
                        )

                    except Exception as e:
                        self.logger.error(
                            "Error collecting betting data for game",
                            game_id=game_data.get('id'),
                            error=str(e)
                        )
                        continue

                return all_data

        except Exception as e:
            self.logger.error("Failed to collect Action Network data", error=str(e))
            return []

    async def _fetch_games_data(self, date: str, sport: str) -> list[dict[str, Any]]:
        """Fetch today's games from Action Network API."""
        try:
            # Use the correct Action Network API endpoint
            url = f"{self.api_base}/web/v2/scoreboard/publicbetting/{sport}"
            params = {
                "bookIds": "15,30,75,123,69,68,972,71,247,79",  # Major sportsbooks
                "date": date,
                "periods": "event",
            }

            self.logger.info("Fetching games from Action Network", url=url, params=params)

            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    games = data.get("games", [])

                    # Process games with enhanced data
                    processed_games = []
                    for game in games:
                        processed_game = await self._process_game_data(game)
                        if processed_game:
                            processed_games.append(processed_game)

                    return processed_games
                else:
                    self.logger.error(f"API request failed with status {response.status}")
                    return []

        except Exception as e:
            self.logger.error("Error fetching games", error=str(e))
            return []

    async def _process_game_data(self, game: dict[str, Any]) -> dict[str, Any] | None:
        """Process a single game from Action Network API response."""
        try:
            game_id = game.get("id")
            if not game_id:
                return None

            # Extract teams
            teams = game.get("teams", [])
            if len(teams) != 2:
                return None

            # Action Network API returns teams in [away, home] order
            away_team = teams[0]
            home_team = teams[1]

            # Extract game timing
            start_time = game.get("start_time")
            game_date = start_time.split("T")[0] if start_time else "unknown"

            # Extract betting data
            betting_data = self._extract_betting_indicators(game)

            processed_game = {
                "id": game_id,
                "home_team": {
                    "name": home_team.get("full_name", home_team.get("display_name", "Unknown")),
                    "abbreviation": home_team.get("abbreviation", "UNK"),
                    "id": home_team.get("id"),
                },
                "away_team": {
                    "name": away_team.get("full_name", away_team.get("display_name", "Unknown")),
                    "abbreviation": away_team.get("abbreviation", "UNK"),
                    "id": away_team.get("id"),
                },
                "game_date": game_date,
                "start_time": start_time,
                "status": game.get("status", "unknown"),
                "betting_data": betting_data,
                "raw_game_data": game,  # Store raw data for reference
            }

            return processed_game

        except Exception as e:
            self.logger.error("Error processing game data", game_id=game.get("id"), error=str(e))
            return None

    def _extract_betting_indicators(self, game: dict[str, Any]) -> dict[str, Any]:
        """Extract all betting indicators from game data."""
        betting_data = {
            "moneyline": {},
            "spread": {},
            "total": {},
            "public_betting": {},
            "sharp_indicators": {},
            "reverse_line_movement": [],
        }

        try:
            # Extract odds data
            odds = game.get("odds", {})

            # Process moneyline
            if "moneyline" in odds:
                ml_data = odds["moneyline"]
                betting_data["moneyline"] = {
                    "home_odds": ml_data.get("home_odds"),
                    "away_odds": ml_data.get("away_odds"),
                    "movement": ml_data.get("movement", {}),
                    "opening_odds": ml_data.get("opening_odds", {}),
                    "current_odds": ml_data.get("current_odds", {}),
                }

            # Process spread
            if "spread" in odds:
                spread_data = odds["spread"]
                betting_data["spread"] = {
                    "home_spread": spread_data.get("home_spread"),
                    "away_spread": spread_data.get("away_spread"),
                    "home_odds": spread_data.get("home_odds"),
                    "away_odds": spread_data.get("away_odds"),
                    "movement": spread_data.get("movement", {}),
                    "opening_line": spread_data.get("opening_line", {}),
                    "current_line": spread_data.get("current_line", {}),
                }

            # Process total
            if "total" in odds:
                total_data = odds["total"]
                betting_data["total"] = {
                    "total_points": total_data.get("total"),
                    "over_odds": total_data.get("over_odds"),
                    "under_odds": total_data.get("under_odds"),
                    "movement": total_data.get("movement", {}),
                    "opening_total": total_data.get("opening_total", {}),
                    "current_total": total_data.get("current_total", {}),
                }

            # Extract public betting percentages
            public_betting = game.get("public_betting", {})
            betting_data["public_betting"] = {
                "moneyline": {
                    "home_bets_pct": public_betting.get("moneyline_home_bets_pct"),
                    "away_bets_pct": public_betting.get("moneyline_away_bets_pct"),
                    "home_money_pct": public_betting.get("moneyline_home_money_pct"),
                    "away_money_pct": public_betting.get("moneyline_away_money_pct"),
                },
                "spread": {
                    "home_bets_pct": public_betting.get("spread_home_bets_pct"),
                    "away_bets_pct": public_betting.get("spread_away_bets_pct"),
                    "home_money_pct": public_betting.get("spread_home_money_pct"),
                    "away_money_pct": public_betting.get("spread_away_money_pct"),
                },
                "total": {
                    "over_bets_pct": public_betting.get("total_over_bets_pct"),
                    "under_bets_pct": public_betting.get("total_under_bets_pct"),
                    "over_money_pct": public_betting.get("total_over_money_pct"),
                    "under_money_pct": public_betting.get("total_under_money_pct"),
                },
            }

            # Detect sharp indicators
            betting_data["sharp_indicators"] = self._detect_sharp_indicators(betting_data)

        except Exception as e:
            self.logger.error("Error extracting betting indicators", error=str(e))

        return betting_data

    def _detect_sharp_indicators(self, betting_data: dict[str, Any]) -> dict[str, Any]:
        """Detect sharp money indicators and reverse line movement."""
        indicators = {
            "reverse_line_movement": [],
            "steam_moves": [],
            "sharp_money_signals": [],
            "public_fade_opportunities": [],
        }

        try:
            public = betting_data.get("public_betting", {})

            # Check for RLM in moneyline
            ml_public = public.get("moneyline", {})
            ml_data = betting_data.get("moneyline", {})
            if ml_public.get("home_money_pct") and ml_data.get("movement"):
                home_money_pct = ml_public.get("home_money_pct", 0)
                if home_money_pct > 60 and ml_data.get("movement", {}).get("direction") == "home":
                    indicators["sharp_money_signals"].append({
                        "market": "moneyline",
                        "team": "home",
                        "public_money_pct": home_money_pct,
                        "line_movement": "favorable",
                        "signal_strength": "strong" if home_money_pct > 70 else "moderate",
                    })

            # Check for RLM in spread
            spread_public = public.get("spread", {})
            spread_data = betting_data.get("spread", {})

            # Check for RLM in totals
            total_public = public.get("total", {})
            total_data = betting_data.get("total", {})
            if total_public.get("over_money_pct") and total_data.get("movement"):
                over_money_pct = total_public.get("over_money_pct", 0)
                movement = total_data.get("movement", {})

                if over_money_pct > 55 and movement.get("direction") == "over":
                    signal_strength = "weak" if over_money_pct < 65 else "moderate"
                    indicators["reverse_line_movement"].append({
                        "market": "total",
                        "direction": "over",
                        "public_money_pct": over_money_pct,
                        "odds_movement": movement,
                        "signal_strength": signal_strength,
                        "note": "Potential RLM - line moved with public money",
                    })

            # Detect public fade opportunities
            for market in ["moneyline", "spread", "total"]:
                market_public = public.get(market, {})
                # Look for heavily public sides (>75%) for fade opportunities
                # This logic could be expanded based on historical performance

        except Exception as e:
            self.logger.error("Error detecting sharp indicators", error=str(e))

        return indicators

    async def _collect_game_betting_data(self, game_data: dict[str, Any]) -> list[dict[str, Any]]:
        """Collect comprehensive betting data for a single game."""
        betting_records = []

        try:
            game_id = game_data.get("id")
            betting_data = game_data.get("betting_data", {})

            # Extract moneyline data
            if "moneyline" in betting_data:
                ml_data = betting_data["moneyline"]
                public_ml = betting_data.get("public_betting", {}).get("moneyline", {})

                moneyline_record = {
                    "game_id": game_id,
                    "bet_type": "moneyline",
                    "sportsbook": "Action Network Consensus",
                    "home_odds": ml_data.get("home_odds"),
                    "away_odds": ml_data.get("away_odds"),
                    "opening_home_odds": ml_data.get("opening_odds", {}).get("home"),
                    "opening_away_odds": ml_data.get("opening_odds", {}).get("away"),
                    "current_home_odds": ml_data.get("current_odds", {}).get("home"),
                    "current_away_odds": ml_data.get("current_odds", {}).get("away"),
                    "home_bets_percentage": public_ml.get("home_bets_pct"),
                    "away_bets_percentage": public_ml.get("away_bets_pct"),
                    "home_money_percentage": public_ml.get("home_money_pct"),
                    "away_money_percentage": public_ml.get("away_money_pct"),
                    "movement": ml_data.get("movement", {}),
                    "timestamp": collection_timestamp(),
                }
                betting_records.append(moneyline_record)

            # Extract spread data
            if "spread" in betting_data:
                spread_data = betting_data["spread"]
                public_spread = betting_data.get("public_betting", {}).get("spread", {})

                spread_record = {
                    "game_id": game_id,
                    "bet_type": "spread",
                    "sportsbook": "Action Network Consensus",
                    "spread_line": spread_data.get("home_spread"),
                    "home_spread_odds": spread_data.get("home_odds"),
                    "away_spread_odds": spread_data.get("away_odds"),
                    "opening_spread": spread_data.get("opening_line", {}).get("home"),
                    "opening_home_odds": spread_data.get("opening_line", {}).get("home_odds"),
                    "opening_away_odds": spread_data.get("opening_line", {}).get("away_odds"),
                    "current_spread": spread_data.get("current_line", {}).get("home"),
                    "current_home_odds": spread_data.get("current_line", {}).get("home_odds"),
                    "current_away_odds": spread_data.get("current_line", {}).get("away_odds"),
                    "home_bets_percentage": public_spread.get("home_bets_pct"),
                    "away_bets_percentage": public_spread.get("away_bets_pct"),
                    "home_money_percentage": public_spread.get("home_money_pct"),
                    "away_money_percentage": public_spread.get("away_money_pct"),
                    "movement": spread_data.get("movement", {}),
                    "timestamp": collection_timestamp(),
                }
                betting_records.append(spread_record)

            # Extract totals data
            if "total" in betting_data:
                total_data = betting_data["total"]
                public_total = betting_data.get("public_betting", {}).get("total", {})

                total_record = {
                    "game_id": game_id,
                    "bet_type": "totals",
                    "sportsbook": "Action Network Consensus",
                    "total_line": total_data.get("total_points"),
                    "over_odds": total_data.get("over_odds"),
                    "under_odds": total_data.get("under_odds"),
                    "opening_total": total_data.get("opening_total", {}).get("total"),
                    "opening_over_odds": total_data.get("opening_total", {}).get("over_odds"),
                    "opening_under_odds": total_data.get("opening_total", {}).get("under_odds"),
                    "current_total": total_data.get("current_total", {}).get("total"),
                    "current_over_odds": total_data.get("current_total", {}).get("over_odds"),
                    "current_under_odds": total_data.get("current_total", {}).get("under_odds"),
                    "over_bets_percentage": public_total.get("over_bets_pct"),
                    "under_bets_percentage": public_total.get("under_bets_pct"),
                    "over_money_percentage": public_total.get("over_money_pct"),
                    "under_money_percentage": public_total.get("under_money_pct"),
                    "movement": total_data.get("movement", {}),
                    "timestamp": collection_timestamp(),
                }
                betting_records.append(total_record)

            return betting_records

        except Exception as e:
            self.logger.error("Error collecting game betting data", error=str(e))
            return []

    def _convert_to_unified_format(
        self,
        betting_data: list[dict[str, Any]],
        game_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Convert Action Network raw data to unified format.
        
        Args:
            betting_data: Raw betting data from Action Network
            game_data: Game information
            
        Returns:
            List of unified format records
        """
        unified_records = []

        for record in betting_data:
            try:
                # Generate external source ID
                external_source_id = f"action_network_{record['game_id']}_{record['sportsbook'].replace(' ', '_')}"

                # Base unified record
                unified_record = {
                    'external_source_id': external_source_id,
                    'sportsbook': record['sportsbook'],
                    'bet_type': record['bet_type'],
                    'odds_timestamp': prepare_for_postgres(record.get('timestamp', collection_timestamp())),
                    'collection_method': 'API',
                    'source_api_version': 'ActionNetwork_v2',
                    'source_metadata': {
                        'game_id': record['game_id'],
                        'api_endpoint': f"{self.api_base}/web/v2/scoreboard/publicbetting/mlb",
                        'raw_movement_data': record.get('movement', {}),
                        'sharp_indicators': game_data.get('betting_data', {}).get('sharp_indicators', {})
                    },
                    'game_datetime': game_data.get('start_time'),
                    'home_team': game_data.get('home_team', {}).get('name'),
                    'away_team': game_data.get('away_team', {}).get('name'),
                }

                # Add bet type specific fields
                if record['bet_type'] == 'moneyline':
                    unified_record.update({
                        'home_ml': record.get('home_odds'),
                        'away_ml': record.get('away_odds'),
                        'opening_home_ml': record.get('opening_home_odds'),
                        'opening_away_ml': record.get('opening_away_odds'),
                        'closing_home_ml': record.get('current_home_odds'),
                        'closing_away_ml': record.get('current_away_odds'),
                        'home_bets_percentage': record.get('home_bets_percentage'),
                        'away_bets_percentage': record.get('away_bets_percentage'),
                        'home_money_percentage': record.get('home_money_percentage'),
                        'away_money_percentage': record.get('away_money_percentage'),
                    })

                elif record['bet_type'] == 'spread':
                    unified_record.update({
                        'spread_line': record.get('spread_line'),
                        'home_spread_price': record.get('home_spread_odds'),
                        'away_spread_price': record.get('away_spread_odds'),
                        'opening_spread': record.get('opening_spread'),
                        'opening_home_price': record.get('opening_home_odds'),
                        'opening_away_price': record.get('opening_away_odds'),
                        'closing_spread': record.get('current_spread'),
                        'closing_home_price': record.get('current_home_odds'),
                        'closing_away_price': record.get('current_away_odds'),
                        'home_bets_percentage': record.get('home_bets_percentage'),
                        'away_bets_percentage': record.get('away_bets_percentage'),
                        'home_money_percentage': record.get('home_money_percentage'),
                        'away_money_percentage': record.get('away_money_percentage'),
                    })

                elif record['bet_type'] == 'totals':
                    unified_record.update({
                        'total_line': record.get('total_line'),
                        'over_price': record.get('over_odds'),
                        'under_price': record.get('under_odds'),
                        'opening_total': record.get('opening_total'),
                        'opening_over_price': record.get('opening_over_odds'),
                        'opening_under_price': record.get('opening_under_odds'),
                        'closing_total': record.get('current_total'),
                        'closing_over_price': record.get('current_over_odds'),
                        'closing_under_price': record.get('current_under_odds'),
                        'over_bets_percentage': record.get('over_bets_percentage'),
                        'under_bets_percentage': record.get('under_bets_percentage'),
                        'over_money_percentage': record.get('over_money_percentage'),
                        'under_money_percentage': record.get('under_money_percentage'),
                    })

                # Detect sharp action
                sharp_action = self._detect_sharp_action_from_record(unified_record, record['bet_type'])
                if sharp_action:
                    unified_record['sharp_action'] = sharp_action

                # Detect reverse line movement
                rlm = self._detect_reverse_line_movement_from_record(unified_record, record.get('movement', {}))
                unified_record['reverse_line_movement'] = rlm

                # Add steam move detection
                steam_move = self._detect_steam_move(record.get('movement', {}))
                unified_record['steam_move'] = steam_move

                unified_records.append(unified_record)

            except Exception as e:
                self.logger.error("Error converting record to unified format", error=str(e))
                continue

        return unified_records

    def _detect_sharp_action_from_record(self, record: dict[str, Any], bet_type: str) -> str | None:
        """Detect sharp action from unified record."""
        try:
            if bet_type == 'moneyline':
                home_money_pct = record.get('home_money_percentage', 0)
                away_money_pct = record.get('away_money_percentage', 0)
                home_bets_pct = record.get('home_bets_percentage', 0)
                away_bets_pct = record.get('away_bets_percentage', 0)

                # Sharp action: money percentage significantly higher than bet percentage
                if home_money_pct and home_bets_pct and home_money_pct > home_bets_pct + 15:
                    return 'HEAVY' if home_money_pct > home_bets_pct + 30 else 'MODERATE'
                elif away_money_pct and away_bets_pct and away_money_pct > away_bets_pct + 15:
                    return 'HEAVY' if away_money_pct > away_bets_pct + 30 else 'MODERATE'

            elif bet_type == 'spread':
                home_money_pct = record.get('home_money_percentage', 0)
                away_money_pct = record.get('away_money_percentage', 0)
                home_bets_pct = record.get('home_bets_percentage', 0)
                away_bets_pct = record.get('away_bets_percentage', 0)

                if home_money_pct and home_bets_pct and home_money_pct > home_bets_pct + 15:
                    return 'HEAVY' if home_money_pct > home_bets_pct + 30 else 'MODERATE'
                elif away_money_pct and away_bets_pct and away_money_pct > away_bets_pct + 15:
                    return 'HEAVY' if away_money_pct > away_bets_pct + 30 else 'MODERATE'

            elif bet_type == 'totals':
                over_money_pct = record.get('over_money_percentage', 0)
                under_money_pct = record.get('under_money_percentage', 0)
                over_bets_pct = record.get('over_bets_percentage', 0)
                under_bets_pct = record.get('under_bets_percentage', 0)

                if over_money_pct and over_bets_pct and over_money_pct > over_bets_pct + 15:
                    return 'HEAVY' if over_money_pct > over_bets_pct + 30 else 'MODERATE'
                elif under_money_pct and under_bets_pct and under_money_pct > under_bets_pct + 15:
                    return 'HEAVY' if under_money_pct > under_bets_pct + 30 else 'MODERATE'

        except Exception as e:
            self.logger.error("Error detecting sharp action", error=str(e))

        return None

    def _detect_reverse_line_movement_from_record(self, record: dict[str, Any], movement: dict[str, Any]) -> bool:
        """Detect reverse line movement from movement data."""
        try:
            # This is a simplified implementation
            # In reality, this would require more sophisticated analysis
            direction = movement.get('direction')
            magnitude = movement.get('magnitude', 0)

            # Basic RLM detection based on movement direction and public betting
            if direction and magnitude > 0:
                # More sophisticated logic would go here
                return True

        except Exception as e:
            self.logger.error("Error detecting reverse line movement", error=str(e))

        return False

    def _detect_steam_move(self, movement: dict[str, Any]) -> bool:
        """Detect steam moves from movement data."""
        try:
            # Steam moves are typically characterized by rapid, large line movements
            magnitude = movement.get('magnitude', 0)
            velocity = movement.get('velocity', 0)

            # Simple steam move detection
            if magnitude > 0.5 and velocity > 10:  # Adjust thresholds as needed
                return True

        except Exception as e:
            self.logger.error("Error detecting steam move", error=str(e))

        return False

    def collect_game_data(self, date: str = None, sport: str = "mlb") -> int:
        """
        Convenience method to collect all betting data for a date.
        
        Args:
            date: Date in YYYYMMDD format (default: today)
            sport: Sport type (default: mlb)
            
        Returns:
            Number of records stored
        """
        try:
            result = self.collect_and_store(date=date, sport=sport)

            self.logger.info(
                "Action Network collection completed",
                date=date,
                sport=sport,
                status=result.status.value,
                processed=result.records_processed,
                stored=result.records_stored
            )

            return result.records_stored

        except Exception as e:
            self.logger.error("Error in collect_game_data", error=str(e))
            return 0

    def test_collection(self, date: str = None, sport: str = "mlb") -> dict[str, Any]:
        """
        Test method for validating Action Network collection.
        
        Args:
            date: Date in YYYYMMDD format (default: today)
            sport: Sport type to test with
            
        Returns:
            Test results dictionary
        """
        try:
            self.logger.info("Testing Action Network unified collection", date=date, sport=sport)

            # Test data collection
            raw_data = self.collect_raw_data(date=date, sport=sport)

            # Test storage
            if raw_data:
                result = self.collect_and_store(date=date, sport=sport)

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
                    'message': 'No data collected from Action Network'
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
    collector = ActionNetworkUnifiedCollector()

    # Test collection
    test_result = collector.test_collection()
    print(f"Test result: {test_result}")

    # Production collection
    if test_result['status'] == 'success':
        stored_count = collector.collect_game_data()
        print(f"Stored {stored_count} records")
