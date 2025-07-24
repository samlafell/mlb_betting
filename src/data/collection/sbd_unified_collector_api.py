#!/usr/bin/env python3
"""
SBD Unified Collector - API Edition

Sports Betting Dime (SBD) collector using their WordPress JSON API for real betting data.
Collects comprehensive odds and betting splits from 9+ major sportsbooks.
Provides rich data including sharp action indicators and line movement tracking.

This replaces the web scraping approach with direct API access for higher reliability.
Refactored to use BaseCollector and proper Pydantic models.
"""

import asyncio
import json
from datetime import datetime
from typing import Any

import aiohttp
import structlog

from .base import (
    BaseCollector,
    CollectorConfig, 
    CollectionRequest,
    CollectionResult,
    DataSource
)

logger = structlog.get_logger(__name__)


class SBDUnifiedCollectorAPI(BaseCollector):
    """
    SBD (Sports Betting Dime) collector using WordPress JSON API.

    Provides direct access to SBD's comprehensive betting data including:
    - Odds from 9+ major sportsbooks (DraftKings, FanDuel, MGM, etc.)
    - Real-time betting splits (bet % vs money %)
    - Opening and current lines for line movement tracking
    - Best odds identification across all books
    """

    def __init__(self, config: CollectorConfig):
        super().__init__(config)
        # Use config.base_url or fallback to default
        self.base_url = config.base_url or "https://www.sportsbettingdime.com"
        # Use config.params for API path or fallback to default
        api_path = config.params.get("api_path", "/wp-json/adpt/v1/mlb-odds")
        self.api_url = f"{self.base_url}{api_path}"

        # Major sportsbook IDs discovered from API
        self.sportsbook_mapping = {
            "sr:book:6": "Unibet",
            "sr:book:7612": "Betway2",
            "sr:book:17324": "MGM",
            "sr:book:18149": "DraftKings",
            "sr:book:18186": "FanDuel",
            "sr:book:27447": "SugarHouseNJ",
            "sr:book:27769": "PointsBet",
            "sr:book:28901": "Bet365NewJersey",
            "sr:book:32219": "WilliamHillNewJersey",
        }

        self.sportsbook_ids = list(self.sportsbook_mapping.keys())

        self.bet_types = [
            {
                "data_format": "moneyline",
                "name": "Moneyline",
                "unified_type": "moneyline",
            },
            {"data_format": "spread", "name": "Spread", "unified_type": "spread"},
            {"data_format": "totals", "name": "Totals", "unified_type": "totals"},
        ]

    async def collect_data(self, request: CollectionRequest) -> list[dict[str, Any]]:
        """
        Collect raw betting data from SBD API.

        Args:
            request: Collection request with parameters

        Returns:
            List of raw betting line dictionaries
        """
        try:
            sport = request.additional_params.get("sport", "mlb")
            self.logger.info(
                "Starting SBD API data collection",
                sport=sport,
                dry_run=request.dry_run
            )

            # Collect real data using SBD API
            real_data = await self._collect_with_api(sport)
            if real_data:
                # Convert to three-tier format and store if not dry run
                if not request.dry_run:
                    for game_data in real_data:
                        # Process each real game
                        raw_records = self._convert_to_unified_format(
                            [game_data], game_data
                        )
                        if raw_records:
                            stored_count = self._store_in_raw_data(raw_records)
                            self.logger.info(
                                f"Stored {stored_count} real SBD API records in raw_data"
                            )

            return real_data

        except Exception as e:
            self.logger.error(
                "Failed to collect SBD API data", sport=sport, error=str(e)
            )
            raise

    async def _collect_with_api(self, sport: str) -> list[dict[str, Any]]:
        """Collect data using SBD WordPress JSON API."""
        try:
            # Build API URL with all sportsbook IDs and US format
            books_param = ",".join(self.sportsbook_ids)
            api_url = f"{self.api_url}?books={books_param}&format=us"

            self.logger.info(f"Fetching data from SBD API: {api_url}")

            # Set up headers to mimic browser request
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:140.0) Gecko/20100101 Firefox/140.0",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br",
                "Referer": "https://www.sportsbettingdime.com/mlb/public-betting-trends/",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "DNT": "1",
            }

            # Make API request using async HTTP session
            async with self.session.get(api_url, headers=headers) as response:
                response.raise_for_status()
                api_data = await response.json()

            # API data already parsed above

            if not api_data or "data" not in api_data:
                self.logger.warning("No data found in SBD API response")
                return []

            # Process each game from API
            processed_games = []
            for game in api_data["data"]:
                try:
                    processed_game = self._process_api_game(game, sport)
                    if processed_game:
                        processed_games.append(processed_game)

                except Exception as e:
                    self.logger.error(
                        f"Error processing game {game.get('id', 'unknown')}: {str(e)}"
                    )
                    continue

            self.logger.info(
                f"Successfully processed {len(processed_games)} games from SBD API"
            )
            return processed_games

        except Exception as e:
            self.logger.error(f"SBD API collection failed: {str(e)}")
            return []

    def _process_api_game(self, game: dict[str, Any], sport: str) -> dict[str, Any]:
        """Process a single game from the SBD API response."""
        try:
            # Extract basic game info
            game_id = game.get("id", "unknown")
            scheduled_time = game.get("scheduled", datetime.now().isoformat())

            # Extract team info
            competitors = game.get("competitors", {})
            home_team = competitors.get("home", {})
            away_team = competitors.get("away", {})

            # Create base game data with enhanced team information
            game_data = {
                "game_id": f"sbd_api_{game_id}",
                "external_game_id": game_id,
                "game_name": f"{away_team.get('abbreviation', 'UNK')} @ {home_team.get('abbreviation', 'UNK')}",
                "away_team": away_team.get("name", "Unknown"),
                "away_team_abbr": away_team.get("abbreviation", "UNK"),
                "away_team_id": away_team.get("id", "unknown"),
                "home_team": home_team.get("name", "Unknown"),
                "home_team_abbr": home_team.get("abbreviation", "UNK"),
                "home_team_id": home_team.get("id", "unknown"),
                "sport": sport,
                "game_datetime": scheduled_time,
                "game_status": game.get("status", "scheduled"),
                "api_endpoint": "sbd_wordpress_api",
                "extraction_method": "api_real_data",
                "extraction_timestamp": datetime.now().isoformat(),
                "betting_splits": game.get("bettingSplits", {}),
                "competitors": competitors,  # Store complete competitors data
                "betting_records": [],
                "raw_data": game,
            }

            # Process markets (moneyline, spread, totals)
            markets = game.get("markets", {})

            # Process moneyline
            if "moneyline" in markets:
                moneyline_records = self._process_market_data(
                    markets["moneyline"], "moneyline", game_data
                )
                game_data["betting_records"].extend(moneyline_records)

            # Process spread
            if "spread" in markets:
                spread_records = self._process_market_data(
                    markets["spread"], "spread", game_data
                )
                game_data["betting_records"].extend(spread_records)

            # Process totals
            if "total" in markets:
                total_records = self._process_market_data(
                    markets["total"], "totals", game_data
                )
                game_data["betting_records"].extend(total_records)

            return game_data

        except Exception as e:
            self.logger.error(f"Error processing API game: {str(e)}")
            return None

    def _process_market_data(
        self, market: dict[str, Any], bet_type: str, game_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Process betting market data for a specific bet type."""
        records = []

        try:
            books = market.get("books", [])

            for book in books:
                book_id = book.get("id")
                book_name = book.get(
                    "name", self.sportsbook_mapping.get(book_id, "Unknown")
                )

                # Create base record with team information
                record = {
                    "sportsbook": book_name,
                    "sportsbook_id": book_id,
                    "bet_type": bet_type,
                    "timestamp": datetime.now().isoformat(),
                    "extraction_method": "sbd_api_real",
                    "game_id": game_data["external_game_id"],
                    "home_team": game_data.get("home_team"),
                    "away_team": game_data.get("away_team"),
                    "home_team_abbr": game_data.get("home_team_abbr"),
                    "away_team_abbr": game_data.get("away_team_abbr"),
                    "home_team_id": game_data.get("home_team_id"),
                    "away_team_id": game_data.get("away_team_id"),
                    "game_name": game_data.get("game_name"),
                }

                # Add bet type specific data
                if bet_type == "moneyline":
                    record.update(
                        {
                            "home_odds": book.get("home", {}).get("odds"),
                            "away_odds": book.get("away", {}).get("odds"),
                            "home_opening_odds": book.get("home", {}).get(
                                "opening_odds"
                            ),
                            "away_opening_odds": book.get("away", {}).get(
                                "opening_odds"
                            ),
                            "home_best": book.get("home", {}).get("best", False),
                            "away_best": book.get("away", {}).get("best", False),
                        }
                    )

                elif bet_type == "spread":
                    record.update(
                        {
                            "home_spread": book.get("home", {}).get("spread"),
                            "away_spread": book.get("away", {}).get("spread"),
                            "home_spread_odds": book.get("home", {}).get("odds"),
                            "away_spread_odds": book.get("away", {}).get("odds"),
                            "home_opening_spread": book.get("home", {}).get(
                                "opening_spread"
                            ),
                            "away_opening_spread": book.get("away", {}).get(
                                "opening_spread"
                            ),
                            "home_opening_odds": book.get("home", {}).get(
                                "opening_odds"
                            ),
                            "away_opening_odds": book.get("away", {}).get(
                                "opening_odds"
                            ),
                            "home_best": book.get("home", {}).get("best", False),
                            "away_best": book.get("away", {}).get("best", False),
                        }
                    )

                elif bet_type == "totals":
                    record.update(
                        {
                            "total_line": book.get("total"),
                            "opening_total": book.get("opening_total"),
                            "over_odds": book.get("over", {}).get("odds"),
                            "under_odds": book.get("under", {}).get("odds"),
                            "over_opening_odds": book.get("over", {}).get(
                                "opening_odds"
                            ),
                            "under_opening_odds": book.get("under", {}).get(
                                "opening_odds"
                            ),
                            "over_best": book.get("over", {}).get("best", False),
                            "under_best": book.get("under", {}).get("best", False),
                        }
                    )

                # Add betting splits data if available
                betting_splits = game_data.get("betting_splits", {})
                if bet_type in betting_splits:
                    splits = betting_splits[bet_type]
                    record.update(
                        {
                            "splits_updated": splits.get("updated"),
                            "home_bets_percentage": splits.get("home", {}).get(
                                "betsPercentage"
                            ),
                            "home_money_percentage": splits.get("home", {}).get(
                                "stakePercentage"
                            ),
                            "away_bets_percentage": splits.get("away", {}).get(
                                "betsPercentage"
                            ),
                            "away_money_percentage": splits.get("away", {}).get(
                                "stakePercentage"
                            ),
                            "over_bets_percentage": splits.get("over", {}).get(
                                "betsPercentage"
                            ),
                            "over_money_percentage": splits.get("over", {}).get(
                                "stakePercentage"
                            ),
                            "under_bets_percentage": splits.get("under", {}).get(
                                "betsPercentage"
                            ),
                            "under_money_percentage": splits.get("under", {}).get(
                                "stakePercentage"
                            ),
                        }
                    )

                    # Calculate sharp action indicators
                    sharp_action = self._detect_sharp_action_api(record, bet_type)
                    if sharp_action:
                        record["sharp_action"] = sharp_action
                        record["sharp_action_detected"] = True

                records.append(record)

            return records

        except Exception as e:
            self.logger.error(f"Error processing {bet_type} market data: {str(e)}")
            return []

    def _detect_sharp_action_api(
        self, record: dict[str, Any], bet_type: str
    ) -> str | None:
        """Detect sharp action using betting splits from API data."""
        try:
            if bet_type == "moneyline":
                home_money = record.get("home_money_percentage", 0)
                home_bets = record.get("home_bets_percentage", 0)
                away_money = record.get("away_money_percentage", 0)
                away_bets = record.get("away_bets_percentage", 0)

                # Sharp action: money percentage significantly higher than bet percentage
                if home_money and home_bets and home_money > home_bets + 15:
                    return "MODERATE" if home_money > home_bets + 25 else "LIGHT"
                elif away_money and away_bets and away_money > away_bets + 15:
                    return "MODERATE" if away_money > away_bets + 25 else "LIGHT"

            elif bet_type == "totals":
                over_money = record.get("over_money_percentage", 0)
                over_bets = record.get("over_bets_percentage", 0)
                under_money = record.get("under_money_percentage", 0)
                under_bets = record.get("under_bets_percentage", 0)

                if over_money and over_bets and over_money > over_bets + 15:
                    return "MODERATE" if over_money > over_bets + 25 else "LIGHT"
                elif under_money and under_bets and under_money > under_bets + 15:
                    return "MODERATE" if under_money > under_bets + 25 else "LIGHT"

            return None

        except Exception as e:
            self.logger.error(f"Error detecting sharp action: {str(e)}")
            return None

    def _convert_to_unified_format(
        self, betting_data: list[dict[str, Any]], game_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Convert SBD API data to unified format for three-tier pipeline.

        Args:
            betting_data: Real betting data from SBD API
            game_data: Game information

        Returns:
            List of unified format records for raw_data.sbd_betting_splits
        """
        unified_records = []

        # Handle the case where betting_data is embedded in game_data
        if "betting_records" in game_data:
            betting_records = game_data["betting_records"]
        else:
            betting_records = betting_data

        for record in betting_records:
            try:
                # Generate external matchup ID for three-tier pipeline
                external_matchup_id = f"sbd_api_{game_data.get('external_game_id', 'unknown')}_{record.get('sportsbook_id', 'unknown')}_{record.get('bet_type', 'unknown')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

                # Create raw_data record for three-tier pipeline
                raw_record = {
                    "external_matchup_id": external_matchup_id,
                    "raw_response": {
                        "game_data": game_data,
                        "betting_record": record,
                        "collection_metadata": {
                            "collection_timestamp": datetime.now().isoformat(),
                            "source": "sbd",
                            "collector_version": "sbd_api_v1",
                            "data_format": "wordpress_api_response",
                            "sport": game_data.get("sport", "mlb"),
                            "extraction_method": record.get(
                                "extraction_method", "sbd_api_real"
                            ),
                            "is_real_data": True,
                            "api_endpoint": game_data.get("api_endpoint"),
                            "sportsbook_count": len(self.sportsbook_ids),
                            "has_betting_splits": bool(game_data.get("betting_splits")),
                        },
                    },
                    "api_endpoint": game_data.get("api_endpoint", "sbd_wordpress_api"),
                }

                unified_records.append(raw_record)

            except Exception as e:
                self.logger.error(
                    "Error converting record to unified format", error=str(e)
                )
                continue

        return unified_records

    def _store_in_raw_data(self, records: list[dict[str, Any]]) -> int:
        """Store records in raw_data.sbd_betting_splits table."""
        stored_count = 0

        try:
            # Import database connection here to avoid circular imports
            import os

            import psycopg2
            import psycopg2.extras

            # Use environment variables or construct connection string
            db_url = os.getenv(
                "DATABASE_URL", "postgresql://samlafell@localhost:5432/mlb_betting"
            )

            with psycopg2.connect(db_url) as conn:
                with conn.cursor() as cursor:
                    for record in records:
                        try:
                            # Extract team info from raw_response for direct storage
                            game_data = record["raw_response"].get("game_data", {})

                            # Insert into raw_data.sbd_betting_splits with team information
                            insert_sql = """
                                INSERT INTO raw_data.sbd_betting_splits
                                (external_matchup_id, raw_response, api_endpoint,
                                 home_team, away_team, home_team_abbr, away_team_abbr,
                                 home_team_id, away_team_id, game_name)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """

                            cursor.execute(
                                insert_sql,
                                (
                                    record["external_matchup_id"],
                                    psycopg2.extras.Json(record["raw_response"]),  # Use JSONB instead of JSON string
                                    record["api_endpoint"],
                                    game_data.get("home_team"),
                                    game_data.get("away_team"),
                                    game_data.get("home_team_abbr"),
                                    game_data.get("away_team_abbr"),
                                    game_data.get("home_team_id"),
                                    game_data.get("away_team_id"),
                                    game_data.get("game_name"),
                                ),
                            )
                            stored_count += 1

                        except Exception as e:
                            self.logger.error(f"Failed to store record: {str(e)}")
                            continue

                    conn.commit()
                    self.logger.info(
                        f"Successfully stored {stored_count} real SBD API records in raw_data"
                    )

        except Exception as e:
            self.logger.error(f"Database storage failed: {str(e)}")

        return stored_count

    def validate_record(self, record: dict[str, Any]) -> bool:
        """
        Validate SBD record structure.
        
        Args:
            record: Raw data record from SBD API
            
        Returns:
            True if valid, False otherwise
        """
        required_fields = [
            "external_game_id", "game_name", "away_team", "home_team",
            "game_datetime", "betting_records"
        ]
        
        # Check basic structure
        if not all(field in record for field in required_fields):
            return False
            
        # Validate betting records exist
        betting_records = record.get("betting_records", [])
        if not isinstance(betting_records, list) or len(betting_records) == 0:
            return False
            
        # Validate at least one betting record has required fields
        for betting_record in betting_records:
            if not isinstance(betting_record, dict):
                continue
            if all(field in betting_record for field in ["sportsbook", "bet_type", "timestamp"]):
                return True
                
        return False
        
    def normalize_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """
        Normalize SBD record to standard format.
        
        Args:
            record: Raw data record
            
        Returns:
            Normalized data record with collection metadata
        """
        normalized = record.copy()
        
        # Add standardized metadata
        normalized["source"] = self.source.value
        normalized["collected_at_est"] = datetime.now().isoformat()
        normalized["collector_version"] = "sbd_api_v2_unified"
        
        # Ensure consistent team name formatting
        if "away_team" in normalized:
            normalized["away_team_normalized"] = str(normalized["away_team"]).strip()
        if "home_team" in normalized:
            normalized["home_team_normalized"] = str(normalized["home_team"]).strip()
            
        # Add data quality indicators
        normalized["has_betting_splits"] = bool(normalized.get("betting_splits"))
        normalized["betting_record_count"] = len(normalized.get("betting_records", []))
        normalized["sportsbook_count"] = len(set(
            br.get("sportsbook") for br in normalized.get("betting_records", [])
            if br.get("sportsbook")
        ))
        
        return normalized

    async def collect_game_data(self, sport: str = "mlb") -> int:
        """
        Convenience method to collect all betting data for a sport using API.

        Args:
            sport: Sport type (default: mlb)

        Returns:
            Number of records stored
        """
        try:
            # Create collection request
            request = CollectionRequest(
                source=self.source,
                sport=sport,
                additional_params={"sport": sport}
            )
            
            # Use standardized collection interface
            raw_data = await self.collect_data(request)
            
            if raw_data:
                self.logger.info(
                    "SBD API collection completed",
                    sport=sport,
                    status="success",
                    processed=len(raw_data),
                    stored=0,  # Storage is handled in collect_raw_data
                )
                return len(raw_data)
            else:
                return 0

        except Exception as e:
            self.logger.error("Error in collect_game_data", error=str(e))
            return 0

    async def test_collection(self, sport: str = "mlb") -> dict[str, Any]:
        """
        Test method for validating SBD API collection.

        Args:
            sport: Sport type to test with

        Returns:
            Test results dictionary
        """
        try:
            self.logger.info("Testing SBD API collection", sport=sport)

            # Create test collection request
            test_request = CollectionRequest(
                source=self.source,
                sport=sport,
                dry_run=True,
                additional_params={"sport": sport}
            )
            
            # Test data collection using the standardized interface
            raw_data = await self.collect_data(test_request)

            # Test validation on sample records
            valid_count = sum(1 for record in raw_data if self.validate_record(record))
            
            if raw_data:
                return {
                    "status": "success",
                    "data_source": "real_sbd_api",
                    "raw_records": len(raw_data),
                    "valid_records": valid_count,
                    "validation_rate": valid_count / len(raw_data) if raw_data else 0,
                    "collection_result": "success",
                    "sample_record": self.normalize_record(raw_data[0]) if raw_data else None,
                    "is_real_data": True,
                    "sportsbooks_included": len(self.sportsbook_ids),
                    "api_endpoint": self.api_url,
                }
            else:
                return {
                    "status": "no_data",
                    "data_source": "sbd_api",
                    "raw_records": 0,
                    "valid_records": 0,
                    "validation_rate": 0,
                    "message": "No data collected from SBD API",
                }

        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "raw_records": 0,
                "valid_records": 0,
                "validation_rate": 0,
            }


# Example usage
if __name__ == "__main__":
    from .base import CollectorConfig
    
    # Create configuration using the standardized CollectorConfig
    config = CollectorConfig(
        source=DataSource.SBD,
        base_url="https://www.sportsbettingdime.com",
        rate_limit_per_minute=60,
        timeout_seconds=30,
        params={"api_path": "/wp-json/adpt/v1/mlb-odds"}
    )
    
    collector = SBDUnifiedCollectorAPI(config)

    # Test collection using async context
    async def main():
        async with collector:
            test_result = await collector.test_collection("mlb")
            print(f"Test result: {test_result}")

            # Production collection
            if test_result["status"] == "success":
                stored_count = await collector.collect_game_data("mlb")
                print(f"Stored {stored_count} real API records")
    
    asyncio.run(main())
