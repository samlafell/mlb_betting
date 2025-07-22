#!/usr/bin/env python3
"""
SBD Unified Collector - API Edition

Sports Betting Dime (SBD) collector using their WordPress JSON API for real betting data.
Collects comprehensive odds and betting splits from 9+ major sportsbooks.
Provides rich data including sharp action indicators and line movement tracking.

This replaces the web scraping approach with direct API access for higher reliability.
"""

import asyncio
import json
import requests
from datetime import datetime
from typing import Any

import structlog

from .base import DataSource
from .unified_betting_lines_collector import UnifiedBettingLinesCollector

logger = structlog.get_logger(__name__)


class SBDUnifiedCollectorAPI(UnifiedBettingLinesCollector):
    """
    SBD (Sports Betting Dime) collector using WordPress JSON API.
    
    Provides direct access to SBD's comprehensive betting data including:
    - Odds from 9+ major sportsbooks (DraftKings, FanDuel, MGM, etc.)
    - Real-time betting splits (bet % vs money %)
    - Opening and current lines for line movement tracking
    - Best odds identification across all books
    """

    def __init__(self):
        super().__init__(DataSource.SPORTS_BETTING_DIME)
        self.base_url = "https://www.sportsbettingdime.com"
        self.api_url = f"{self.base_url}/wp-json/adpt/v1/mlb-odds"
        
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
            "sr:book:32219": "WilliamHillNewJersey"
        }
        
        self.sportsbook_ids = list(self.sportsbook_mapping.keys())
        
        self.bet_types = [
            {"data_format": "moneyline", "name": "Moneyline", "unified_type": "moneyline"},
            {"data_format": "spread", "name": "Spread", "unified_type": "spread"},
            {"data_format": "totals", "name": "Totals", "unified_type": "totals"}
        ]

    def collect_raw_data(self, sport: str = "mlb", **kwargs) -> list[dict[str, Any]]:
        """
        Collect raw betting data from SBD API.
        
        Args:
            sport: Sport type (default: mlb)
            **kwargs: Additional parameters
            
        Returns:
            List of raw betting line dictionaries
        """
        try:
            # Check if we're already in an event loop
            try:
                loop = asyncio.get_running_loop()
                # We're in an event loop - collect real data using API
                self.logger.info("SBD API collector running in async context - using real SBD API")
                
                # Collect real data using SBD API
                real_data = self._collect_with_api(sport)
                if real_data:
                    # Convert to three-tier format and store
                    for game_data in real_data:
                        # Process each real game
                        raw_records = self._convert_to_unified_format([game_data], game_data)
                        if raw_records:
                            stored_count = self._store_in_raw_data(raw_records)
                            self.logger.info(f"Stored {stored_count} real SBD API records in raw_data")
                
                return real_data
                
            except RuntimeError:
                # No event loop running, collect real data directly
                return self._collect_with_api(sport)

        except Exception as e:
            self.logger.error("Failed to collect SBD API data", sport=sport, error=str(e))
            return []

    def _collect_with_api(self, sport: str) -> list[dict[str, Any]]:
        """Collect data using SBD WordPress JSON API."""
        try:
            # Build API URL with all sportsbook IDs and US format
            books_param = ",".join(self.sportsbook_ids)
            api_url = f"{self.api_url}?books={books_param}&format=us"
            
            self.logger.info(f"Fetching data from SBD API: {api_url}")
            
            # Set up headers to mimic browser request
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:140.0) Gecko/20100101 Firefox/140.0',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'Referer': 'https://www.sportsbettingdime.com/mlb/public-betting-trends/',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
                'DNT': '1'
            }
            
            # Make API request
            response = requests.get(api_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Parse JSON response
            api_data = response.json()
            
            if not api_data or 'data' not in api_data:
                self.logger.warning("No data found in SBD API response")
                return []
            
            # Process each game from API
            processed_games = []
            for game in api_data['data']:
                try:
                    processed_game = self._process_api_game(game, sport)
                    if processed_game:
                        processed_games.append(processed_game)
                        
                except Exception as e:
                    self.logger.error(f"Error processing game {game.get('id', 'unknown')}: {str(e)}")
                    continue
            
            self.logger.info(f"Successfully processed {len(processed_games)} games from SBD API")
            return processed_games
            
        except Exception as e:
            self.logger.error(f"SBD API collection failed: {str(e)}")
            return []

    def _process_api_game(self, game: dict[str, Any], sport: str) -> dict[str, Any]:
        """Process a single game from the SBD API response."""
        try:
            # Extract basic game info
            game_id = game.get('id', 'unknown')
            scheduled_time = game.get('scheduled', datetime.now().isoformat())
            
            # Extract team info
            competitors = game.get('competitors', {})
            home_team = competitors.get('home', {})
            away_team = competitors.get('away', {})
            
            # Create base game data
            game_data = {
                'game_id': f"sbd_api_{game_id}",
                'external_game_id': game_id,
                'game_name': f"{away_team.get('abbreviation', 'UNK')} @ {home_team.get('abbreviation', 'UNK')}",
                'away_team': away_team.get('name', 'Unknown'),
                'away_team_abbr': away_team.get('abbreviation', 'UNK'),
                'home_team': home_team.get('name', 'Unknown'),
                'home_team_abbr': home_team.get('abbreviation', 'UNK'),
                'sport': sport,
                'game_datetime': scheduled_time,
                'game_status': game.get('status', 'scheduled'),
                'api_endpoint': 'sbd_wordpress_api',
                'extraction_method': 'api_real_data',
                'extraction_timestamp': datetime.now().isoformat(),
                'betting_splits': game.get('bettingSplits', {}),
                'betting_records': [],
                'raw_data': game
            }
            
            # Process markets (moneyline, spread, totals)
            markets = game.get('markets', {})
            
            # Process moneyline
            if 'moneyline' in markets:
                moneyline_records = self._process_market_data(
                    markets['moneyline'], 'moneyline', game_data
                )
                game_data['betting_records'].extend(moneyline_records)
            
            # Process spread
            if 'spread' in markets:
                spread_records = self._process_market_data(
                    markets['spread'], 'spread', game_data
                )
                game_data['betting_records'].extend(spread_records)
            
            # Process totals
            if 'total' in markets:
                total_records = self._process_market_data(
                    markets['total'], 'totals', game_data
                )
                game_data['betting_records'].extend(total_records)
            
            return game_data
            
        except Exception as e:
            self.logger.error(f"Error processing API game: {str(e)}")
            return None

    def _process_market_data(self, market: dict[str, Any], bet_type: str, 
                           game_data: dict[str, Any]) -> list[dict[str, Any]]:
        """Process betting market data for a specific bet type."""
        records = []
        
        try:
            books = market.get('books', [])
            
            for book in books:
                book_id = book.get('id')
                book_name = book.get('name', self.sportsbook_mapping.get(book_id, 'Unknown'))
                
                # Create base record
                record = {
                    'sportsbook': book_name,
                    'sportsbook_id': book_id,
                    'bet_type': bet_type,
                    'timestamp': datetime.now().isoformat(),
                    'extraction_method': 'sbd_api_real',
                    'game_id': game_data['external_game_id']
                }
                
                # Add bet type specific data
                if bet_type == 'moneyline':
                    record.update({
                        'home_odds': book.get('home', {}).get('odds'),
                        'away_odds': book.get('away', {}).get('odds'),
                        'home_opening_odds': book.get('home', {}).get('opening_odds'),
                        'away_opening_odds': book.get('away', {}).get('opening_odds'),
                        'home_best': book.get('home', {}).get('best', False),
                        'away_best': book.get('away', {}).get('best', False)
                    })
                    
                elif bet_type == 'spread':
                    record.update({
                        'home_spread': book.get('home', {}).get('spread'),
                        'away_spread': book.get('away', {}).get('spread'),
                        'home_spread_odds': book.get('home', {}).get('odds'),
                        'away_spread_odds': book.get('away', {}).get('odds'),
                        'home_opening_spread': book.get('home', {}).get('opening_spread'),
                        'away_opening_spread': book.get('away', {}).get('opening_spread'),
                        'home_opening_odds': book.get('home', {}).get('opening_odds'),
                        'away_opening_odds': book.get('away', {}).get('opening_odds'),
                        'home_best': book.get('home', {}).get('best', False),
                        'away_best': book.get('away', {}).get('best', False)
                    })
                    
                elif bet_type == 'totals':
                    record.update({
                        'total_line': book.get('total'),
                        'opening_total': book.get('opening_total'),
                        'over_odds': book.get('over', {}).get('odds'),
                        'under_odds': book.get('under', {}).get('odds'),
                        'over_opening_odds': book.get('over', {}).get('opening_odds'),
                        'under_opening_odds': book.get('under', {}).get('opening_odds'),
                        'over_best': book.get('over', {}).get('best', False),
                        'under_best': book.get('under', {}).get('best', False)
                    })
                
                # Add betting splits data if available
                betting_splits = game_data.get('betting_splits', {})
                if bet_type in betting_splits:
                    splits = betting_splits[bet_type]
                    record.update({
                        'splits_updated': splits.get('updated'),
                        'home_bets_percentage': splits.get('home', {}).get('betsPercentage'),
                        'home_money_percentage': splits.get('home', {}).get('stakePercentage'),
                        'away_bets_percentage': splits.get('away', {}).get('betsPercentage'),
                        'away_money_percentage': splits.get('away', {}).get('stakePercentage'),
                        'over_bets_percentage': splits.get('over', {}).get('betsPercentage'),
                        'over_money_percentage': splits.get('over', {}).get('stakePercentage'),
                        'under_bets_percentage': splits.get('under', {}).get('betsPercentage'),
                        'under_money_percentage': splits.get('under', {}).get('stakePercentage')
                    })
                    
                    # Calculate sharp action indicators
                    sharp_action = self._detect_sharp_action_api(record, bet_type)
                    if sharp_action:
                        record['sharp_action'] = sharp_action
                        record['sharp_action_detected'] = True
                    
                records.append(record)
            
            return records
            
        except Exception as e:
            self.logger.error(f"Error processing {bet_type} market data: {str(e)}")
            return []

    def _detect_sharp_action_api(self, record: dict[str, Any], bet_type: str) -> str | None:
        """Detect sharp action using betting splits from API data."""
        try:
            if bet_type == 'moneyline':
                home_money = record.get('home_money_percentage', 0)
                home_bets = record.get('home_bets_percentage', 0)
                away_money = record.get('away_money_percentage', 0)
                away_bets = record.get('away_bets_percentage', 0)
                
                # Sharp action: money percentage significantly higher than bet percentage
                if home_money and home_bets and home_money > home_bets + 15:
                    return 'MODERATE' if home_money > home_bets + 25 else 'LIGHT'
                elif away_money and away_bets and away_money > away_bets + 15:
                    return 'MODERATE' if away_money > away_bets + 25 else 'LIGHT'
                    
            elif bet_type == 'totals':
                over_money = record.get('over_money_percentage', 0)
                over_bets = record.get('over_bets_percentage', 0)
                under_money = record.get('under_money_percentage', 0)
                under_bets = record.get('under_bets_percentage', 0)
                
                if over_money and over_bets and over_money > over_bets + 15:
                    return 'MODERATE' if over_money > over_bets + 25 else 'LIGHT'
                elif under_money and under_bets and under_money > under_bets + 15:
                    return 'MODERATE' if under_money > under_bets + 25 else 'LIGHT'
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error detecting sharp action: {str(e)}")
            return None

    def _convert_to_unified_format(
        self,
        betting_data: list[dict[str, Any]],
        game_data: dict[str, Any]
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
        if 'betting_records' in game_data:
            betting_records = game_data['betting_records']
        else:
            betting_records = betting_data

        for record in betting_records:
            try:
                # Generate external matchup ID for three-tier pipeline
                external_matchup_id = f"sbd_api_{game_data.get('external_game_id', 'unknown')}_{record.get('sportsbook_id', 'unknown')}_{record.get('bet_type', 'unknown')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

                # Create raw_data record for three-tier pipeline
                raw_record = {
                    'external_matchup_id': external_matchup_id,
                    'raw_response': {
                        'game_data': game_data,
                        'betting_record': record,
                        'collection_metadata': {
                            'collection_timestamp': datetime.now().isoformat(),
                            'source': 'sbd',
                            'collector_version': 'sbd_api_v1',
                            'data_format': 'wordpress_api_response',
                            'sport': game_data.get('sport', 'mlb'),
                            'extraction_method': record.get('extraction_method', 'sbd_api_real'),
                            'is_real_data': True,
                            'api_endpoint': game_data.get('api_endpoint'),
                            'sportsbook_count': len(self.sportsbook_ids),
                            'has_betting_splits': bool(game_data.get('betting_splits'))
                        }
                    },
                    'api_endpoint': game_data.get('api_endpoint', 'sbd_wordpress_api')
                }

                unified_records.append(raw_record)

            except Exception as e:
                self.logger.error("Error converting record to unified format", error=str(e))
                continue

        return unified_records

    def _store_in_raw_data(self, records: list[dict[str, Any]]) -> int:
        """Store records in raw_data.sbd_betting_splits table."""
        stored_count = 0
        
        try:
            # Import database connection here to avoid circular imports
            import psycopg2
            import os
            
            # Use environment variables or construct connection string
            db_url = os.getenv('DATABASE_URL', 'postgresql://samlafell@localhost:5432/mlb_betting')
            
            with psycopg2.connect(db_url) as conn:
                with conn.cursor() as cursor:
                    for record in records:
                        try:
                            # Insert into raw_data.sbd_betting_splits
                            insert_sql = """
                                INSERT INTO raw_data.sbd_betting_splits 
                                (external_matchup_id, raw_response, api_endpoint)
                                VALUES (%s, %s, %s)
                            """
                            
                            cursor.execute(insert_sql, (
                                record['external_matchup_id'],
                                json.dumps(record['raw_response']),
                                record['api_endpoint']
                            ))
                            stored_count += 1
                            
                        except Exception as e:
                            self.logger.error(f"Failed to store record: {str(e)}")
                            continue
                    
                    conn.commit()
                    self.logger.info(f"Successfully stored {stored_count} real SBD API records in raw_data")
                    
        except Exception as e:
            self.logger.error(f"Database storage failed: {str(e)}")
            
        return stored_count

    def collect_game_data(self, sport: str = "mlb") -> int:
        """
        Convenience method to collect all betting data for a sport using API.
        
        Args:
            sport: Sport type (default: mlb)
            
        Returns:
            Number of records stored
        """
        try:
            result = self.collect_and_store(sport=sport)

            self.logger.info(
                "SBD API collection completed",
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
        Test method for validating SBD API collection.
        
        Args:
            sport: Sport type to test with
            
        Returns:
            Test results dictionary
        """
        try:
            self.logger.info("Testing SBD API collection", sport=sport)

            # Test data collection
            raw_data = self.collect_raw_data(sport=sport)

            # Test storage
            if raw_data:
                result = self.collect_and_store(sport=sport)

                return {
                    'status': 'success',
                    'data_source': 'real_sbd_api',
                    'raw_records': len(raw_data),
                    'processed': result.records_processed,
                    'stored': result.records_stored,
                    'collection_result': result.status.value,
                    'sample_record': raw_data[0] if raw_data else None,
                    'is_real_data': True,
                    'sportsbooks_included': len(self.sportsbook_ids),
                    'api_endpoint': self.api_url
                }
            else:
                return {
                    'status': 'no_data',
                    'data_source': 'sbd_api',
                    'raw_records': 0,
                    'processed': 0,
                    'stored': 0,
                    'message': 'No data collected from SBD API'
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
    collector = SBDUnifiedCollectorAPI()

    # Test collection
    test_result = collector.test_collection("mlb")
    print(f"Test result: {test_result}")

    # Production collection
    if test_result['status'] == 'success':
        stored_count = collector.collect_game_data("mlb")
        print(f"Stored {stored_count} real API records")