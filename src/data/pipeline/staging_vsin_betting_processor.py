#!/usr/bin/env python3
"""
VSIN Betting Data Processor - Sharp Action Detection with MLB API Integration

Processes raw VSIN HTML/JSON data to extract betting splits, consensus data,
and sharp action indicators while resolving MLB Stats API game IDs.

Key Features:
1. Parses VSIN betting splits data (handle % and bet count %)
2. Detects sharp action based on handle vs bets discrepancy
3. Resolves MLB Stats API game IDs for cross-system integration
4. Provides comprehensive data quality scoring
5. Supports multiple sportsbook views (DK, Circa, FanDuel, etc.)

Data Source: raw_data.vsin_raw_data table with HTML content
Output: staging.vsin_betting_data with comprehensive betting analysis
"""

import asyncio
import re
from datetime import date
from decimal import Decimal
from typing import Any
from uuid import uuid4

import asyncpg
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field, field_validator

from ...core.config import get_settings
from ...core.datetime_utils import now_est
from ...core.logging import LogComponent, get_logger
from ...core.team_utils import normalize_team_name
from ...services.mlb_stats_api_game_resolution_service import (
    DataSource,
    MLBStatsAPIGameResolutionService,
)

logger = get_logger(__name__, LogComponent.CORE)


class VSINBettingRecord(BaseModel):
    """Single VSIN betting splits record with sharp action detection."""

    # Game and source identification
    external_matchup_id: str | None = None
    mlb_stats_api_game_id: str | None = None

    # Game details
    home_team: str
    away_team: str
    home_team_normalized: str | None = None
    away_team_normalized: str | None = None
    game_date: date | None = None
    game_time: str | None = None

    # Sportsbook information
    sportsbook_name: str
    sportsbook_id: int | None = None

    # Moneyline betting data
    moneyline_home_odds: int | None = None
    moneyline_away_odds: int | None = None
    moneyline_home_handle_percent: Decimal | None = None
    moneyline_away_handle_percent: Decimal | None = None
    moneyline_home_bets_percent: Decimal | None = None
    moneyline_away_bets_percent: Decimal | None = None

    # Totals (over/under) betting data
    total_line: Decimal | None = None
    total_over_odds: int | None = None
    total_under_odds: int | None = None
    total_over_handle_percent: Decimal | None = None
    total_under_handle_percent: Decimal | None = None
    total_over_bets_percent: Decimal | None = None
    total_under_bets_percent: Decimal | None = None

    # Runline (spread) betting data
    runline_spread: Decimal | None = None
    runline_home_odds: int | None = None
    runline_away_odds: int | None = None
    runline_home_handle_percent: Decimal | None = None
    runline_away_handle_percent: Decimal | None = None
    runline_home_bets_percent: Decimal | None = None
    runline_away_bets_percent: Decimal | None = None

    # Sharp action indicators
    moneyline_sharp_side: str | None = None  # home, away, or null
    total_sharp_side: str | None = None  # over, under, or null
    runline_sharp_side: str | None = None  # home, away, or null
    sharp_confidence: Decimal = Field(default=Decimal('0.0'), ge=0.0, le=1.0)

    # Reverse Line Movement indicators
    moneyline_rlm_detected: bool = False
    total_rlm_detected: bool = False
    runline_rlm_detected: bool = False

    # Data quality and lineage
    data_quality_score: float = Field(ge=0.0, le=1.0, default=1.0)
    validation_status: str = "valid"
    parsing_errors: list[str] = Field(default_factory=list)
    source_url: str | None = None
    vsin_view: str | None = None
    raw_data_id: int | None = None

    @field_validator("sportsbook_name")
    @classmethod
    def validate_sportsbook_name(cls, v):
        valid_books = ["dk", "circa", "fanduel", "mgm", "caesars"]
        if v.lower() not in valid_books:
            raise ValueError(f"Invalid sportsbook_name: {v}")
        return v.lower()

    @field_validator("moneyline_sharp_side", "runline_sharp_side")
    @classmethod
    def validate_team_sharp_side(cls, v):
        if v is not None and v not in ["home", "away"]:
            raise ValueError(f"Invalid team sharp side: {v}")
        return v

    @field_validator("total_sharp_side")
    @classmethod
    def validate_total_sharp_side(cls, v):
        if v is not None and v not in ["over", "under"]:
            raise ValueError(f"Invalid total sharp side: {v}")
        return v


class VSINBettingProcessor:
    """
    Processor for extracting VSIN betting splits data with sharp action detection.
    
    Processes raw_data.vsin_raw_data to extract betting consensus and sharp action
    indicators while resolving MLB Stats API game IDs for cross-system integration.
    """

    def __init__(self):
        self.settings = get_settings()
        self.mlb_resolver = MLBStatsAPIGameResolutionService()
        self.processing_batch_id = str(uuid4())

        # Sharp action detection threshold (handle % vs bets % difference)
        self.sharp_action_threshold = 10.0  # 10% difference indicates sharp action

    def _get_db_config(self) -> dict[str, Any]:
        """Get database configuration from centralized settings."""
        settings = get_settings()
        return {
            "host": settings.database.host,
            "port": settings.database.port,
            "database": settings.database.database,
            "user": settings.database.user,
            "password": settings.database.password,
        }

    async def initialize(self):
        """Initialize processor services."""
        await self.mlb_resolver.initialize()
        logger.info(
            "VSINBettingProcessor initialized with MLB Stats API integration"
        )

    async def cleanup(self):
        """Cleanup processor resources."""
        await self.mlb_resolver.cleanup()

    async def process_vsin_data(self, limit: int = 10) -> dict[str, Any]:
        """
        Process raw VSIN data to extract betting splits with sharp action detection.
        
        This method:
        1. Retrieves unprocessed raw VSIN HTML data
        2. Parses betting splits and consensus data
        3. Detects sharp action indicators
        4. Resolves MLB Stats API game IDs
        5. Stores processed data in staging table
        """
        logger.info(
            "Starting VSIN data processing",
            batch_id=self.processing_batch_id,
            limit=limit,
        )

        try:
            conn = await asyncpg.connect(**self._get_db_config())

            # Get unprocessed raw VSIN data
            raw_vsin_data = await conn.fetch(
                """
                SELECT id, source_url, raw_content, content_type, scrape_timestamp
                FROM raw_data.vsin_raw_data 
                WHERE id NOT IN (
                    SELECT DISTINCT raw_data_id 
                    FROM staging.vsin_betting_data
                    WHERE raw_data_id IS NOT NULL
                )
                ORDER BY scrape_timestamp DESC
                LIMIT $1
            """,
                limit,
            )

            if not raw_vsin_data:
                logger.info("No unprocessed VSIN data found")
                return {
                    "betting_records_processed": 0,
                    "betting_records_valid": 0,
                    "mlb_games_resolved": 0,
                }

            logger.info(f"Found {len(raw_vsin_data)} unprocessed VSIN records")

            processed_count = 0
            valid_count = 0
            mlb_resolved_count = 0

            for raw_vsin_record in raw_vsin_data:
                try:
                    # Parse VSIN HTML/JSON data
                    betting_records = await self._parse_vsin_content(
                        raw_vsin_record, conn
                    )

                    for betting_record in betting_records:
                        # Resolve MLB Stats API game ID
                        mlb_game_id = await self._resolve_mlb_game_id(
                            betting_record, conn
                        )
                        if mlb_game_id:
                            betting_record.mlb_stats_api_game_id = mlb_game_id
                            mlb_resolved_count += 1

                        # Detect sharp action
                        self._detect_sharp_action(betting_record)

                        # Calculate data quality score
                        betting_record.data_quality_score = self._calculate_quality_score(betting_record)

                        # Insert betting record
                        await self._insert_betting_record(betting_record, conn)
                        processed_count += 1

                        if betting_record.validation_status == "valid":
                            valid_count += 1

                except Exception as e:
                    logger.error(
                        f"Error processing VSIN record {raw_vsin_record['id']}: {e}"
                    )
                    continue

            await conn.close()

            result = {
                "betting_records_processed": processed_count,
                "betting_records_valid": valid_count,
                "mlb_games_resolved": mlb_resolved_count,
                "processing_batch_id": self.processing_batch_id,
                "structure_type": "VSIN betting splits with sharp action detection",
            }

            logger.info("VSIN data processing completed", **result)
            return result

        except Exception as e:
            logger.error(f"Error in VSIN data processing: {e}")
            raise

    async def _parse_vsin_content(
        self, raw_vsin: dict, conn: asyncpg.Connection
    ) -> list[VSINBettingRecord]:
        """Parse VSIN HTML content to extract betting splits data."""
        records = []

        try:
            raw_content = raw_vsin["raw_content"]
            source_url = raw_vsin["source_url"]

            # Extract sportsbook view from URL
            vsin_view = self._extract_vsin_view(source_url)

            # Parse HTML content
            soup = BeautifulSoup(raw_content, 'html.parser')

            # Find the main betting table
            main_table = soup.find("table", {"class": "freezetable"})
            if not main_table:
                logger.warning("Could not find main betting table in VSIN data")
                return records

            rows = main_table.find_all("tr")

            for row in rows:
                # Skip header rows
                if "div_dkdark" in row.get("class", []):
                    continue

                cells = row.find_all("td")
                if len(cells) < 10:  # Need at least 10 columns for full MLB data
                    continue

                # Parse game row
                betting_record = self._parse_game_row(
                    cells, vsin_view, source_url, raw_vsin["id"]
                )
                if betting_record:
                    records.append(betting_record)

            return records

        except Exception as e:
            logger.error(f"Error parsing VSIN content: {e}")
            return []

    def _extract_vsin_view(self, source_url: str) -> str:
        """Extract sportsbook view from VSIN URL."""
        if not source_url:
            return "dk"  # Default to DraftKings

        if "view=circa" in source_url:
            return "circa"
        elif "view=fanduel" in source_url:
            return "fanduel"
        elif "view=mgm" in source_url:
            return "mgm"
        elif "view=caesars" in source_url:
            return "caesars"
        else:
            return "dk"  # Default

    def _parse_game_row(
        self, cells: list, vsin_view: str, source_url: str, raw_data_id: int
    ) -> VSINBettingRecord | None:
        """Parse individual game row from VSIN table."""
        try:
            # VSIN MLB column mapping (from collector)
            if len(cells) < 10:
                return None

            # Extract team names from first column
            teams_cell = cells[0].get_text(strip=True)
            teams_match = re.search(r'(.+?)\s+@\s+(.+)', teams_cell)
            if not teams_match:
                return None

            away_team = teams_match.group(1).strip()
            home_team = teams_match.group(2).strip()

            # Create external matchup ID
            external_matchup_id = f"vsin_{away_team.replace(' ', '_')}_{home_team.replace(' ', '_')}_{vsin_view}"

            # Parse betting data
            record = VSINBettingRecord(
                external_matchup_id=external_matchup_id,
                home_team=home_team,
                away_team=away_team,
                home_team_normalized=normalize_team_name(home_team),
                away_team_normalized=normalize_team_name(away_team),
                sportsbook_name=vsin_view,
                source_url=source_url,
                vsin_view=vsin_view,
                raw_data_id=raw_data_id,
            )

            # Parse moneyline data (columns 1-3)
            if len(cells) > 3:
                record.moneyline_home_odds = self._safe_int(cells[1].get_text(strip=True))
                record.moneyline_home_handle_percent = self._safe_decimal(cells[2].get_text(strip=True))
                record.moneyline_home_bets_percent = self._safe_decimal(cells[3].get_text(strip=True))

            # Parse totals data (columns 4-6)
            if len(cells) > 6:
                total_text = cells[4].get_text(strip=True)
                record.total_line = self._extract_total_line(total_text)
                record.total_over_handle_percent = self._safe_decimal(cells[5].get_text(strip=True))
                record.total_over_bets_percent = self._safe_decimal(cells[6].get_text(strip=True))

            # Parse runline data (columns 7-9)
            if len(cells) > 9:
                runline_text = cells[7].get_text(strip=True)
                record.runline_spread = self._extract_runline_spread(runline_text)
                record.runline_home_handle_percent = self._safe_decimal(cells[8].get_text(strip=True))
                record.runline_home_bets_percent = self._safe_decimal(cells[9].get_text(strip=True))

            # Calculate complementary percentages
            if record.moneyline_home_handle_percent:
                record.moneyline_away_handle_percent = Decimal('100.0') - record.moneyline_home_handle_percent
            if record.moneyline_home_bets_percent:
                record.moneyline_away_bets_percent = Decimal('100.0') - record.moneyline_home_bets_percent
            if record.total_over_handle_percent:
                record.total_under_handle_percent = Decimal('100.0') - record.total_over_handle_percent
            if record.total_over_bets_percent:
                record.total_under_bets_percent = Decimal('100.0') - record.total_over_bets_percent
            if record.runline_home_handle_percent:
                record.runline_away_handle_percent = Decimal('100.0') - record.runline_home_handle_percent
            if record.runline_home_bets_percent:
                record.runline_away_bets_percent = Decimal('100.0') - record.runline_home_bets_percent

            return record

        except Exception as e:
            logger.error(f"Error parsing VSIN game row: {e}")
            return None

    def _extract_total_line(self, text: str) -> Decimal | None:
        """Extract total line from VSIN text."""
        try:
            # Look for patterns like "O 8.5", "U 8.5", etc.
            match = re.search(r'[OU]\s*(\d+\.?\d*)', text)
            if match:
                return Decimal(match.group(1))
            return None
        except:
            return None

    def _extract_runline_spread(self, text: str) -> Decimal | None:
        """Extract runline spread from VSIN text."""
        try:
            # Look for patterns like "+1.5", "-1.5"
            match = re.search(r'[+-]?\s*(\d+\.?\d*)', text)
            if match:
                return Decimal(match.group(1))
            return None
        except:
            return None

    def _safe_int(self, value: str) -> int | None:
        """Safely convert string to integer."""
        if not value or value in ['-', 'N/A', '']:
            return None
        try:
            # Remove any non-numeric characters except +/- and convert
            clean_value = re.sub(r'[^\d+-]', '', value)
            return int(clean_value) if clean_value else None
        except:
            return None

    def _safe_decimal(self, value: str) -> Decimal | None:
        """Safely convert string to Decimal."""
        if not value or value in ['-', 'N/A', '']:
            return None
        try:
            # Remove % sign and other non-numeric chars except decimal point
            clean_value = re.sub(r'[^\d.]', '', value)
            return Decimal(clean_value) if clean_value else None
        except:
            return None

    def _detect_sharp_action(self, record: VSINBettingRecord):
        """Detect sharp action based on handle vs bets discrepancy."""
        sharp_indicators = 0
        total_markets = 0

        # Check moneyline for sharp action
        if (record.moneyline_home_handle_percent is not None and
            record.moneyline_home_bets_percent is not None):
            total_markets += 1

            handle_diff = abs(record.moneyline_home_handle_percent - record.moneyline_home_bets_percent)
            if handle_diff >= self.sharp_action_threshold:
                sharp_indicators += 1
                # Determine which side has sharp action
                if record.moneyline_home_handle_percent > record.moneyline_home_bets_percent:
                    record.moneyline_sharp_side = "home"
                else:
                    record.moneyline_sharp_side = "away"

        # Check totals for sharp action
        if (record.total_over_handle_percent is not None and
            record.total_over_bets_percent is not None):
            total_markets += 1

            handle_diff = abs(record.total_over_handle_percent - record.total_over_bets_percent)
            if handle_diff >= self.sharp_action_threshold:
                sharp_indicators += 1
                # Determine which side has sharp action
                if record.total_over_handle_percent > record.total_over_bets_percent:
                    record.total_sharp_side = "over"
                else:
                    record.total_sharp_side = "under"

        # Check runline for sharp action
        if (record.runline_home_handle_percent is not None and
            record.runline_home_bets_percent is not None):
            total_markets += 1

            handle_diff = abs(record.runline_home_handle_percent - record.runline_home_bets_percent)
            if handle_diff >= self.sharp_action_threshold:
                sharp_indicators += 1
                # Determine which side has sharp action
                if record.runline_home_handle_percent > record.runline_home_bets_percent:
                    record.runline_sharp_side = "home"
                else:
                    record.runline_sharp_side = "away"

        # Calculate sharp confidence
        if total_markets > 0:
            record.sharp_confidence = Decimal(str(round(sharp_indicators / total_markets, 2)))

    def _calculate_quality_score(self, record: VSINBettingRecord) -> float:
        """Calculate data quality score for VSIN record."""
        score = 1.0

        # Check for required fields
        if not record.home_team or not record.away_team:
            score -= 0.5

        # Check for betting data completeness
        markets_with_data = 0
        total_markets = 3  # moneyline, totals, runline

        if record.moneyline_home_handle_percent is not None:
            markets_with_data += 1
        if record.total_over_handle_percent is not None:
            markets_with_data += 1
        if record.runline_home_handle_percent is not None:
            markets_with_data += 1

        # Reduce score based on missing market data
        data_completeness = markets_with_data / total_markets
        score *= data_completeness

        # Check for parsing errors
        if record.parsing_errors:
            score -= 0.1 * len(record.parsing_errors)

        return max(0.0, min(1.0, score))

    async def _resolve_mlb_game_id(
        self, record: VSINBettingRecord, conn: asyncpg.Connection
    ) -> str | None:
        """Enhanced VSIN game to MLB Stats API game ID resolution with multiple strategies."""
        try:
            if not record.home_team_normalized or not record.away_team_normalized:
                return None

            # Strategy 1: Use enhanced VSIN-specific resolver
            resolution_result = await self.mlb_resolver.resolve_vsin_game_id(
                external_game_id=record.external_matchup_id or "",
                home_team=record.home_team_normalized,
                away_team=record.away_team_normalized,
                game_date=record.game_date,
            )

            if resolution_result.mlb_game_id:
                logger.info(
                    f"Resolved MLB game ID for VSIN game {record.external_matchup_id}: {resolution_result.mlb_game_id} (confidence: {resolution_result.confidence})"
                )
                return resolution_result.mlb_game_id

            # Strategy 2: Fallback to generic resolver with team names
            if record.home_team_normalized and record.away_team_normalized:
                logger.debug(f"Trying generic resolver for VSIN game {record.external_matchup_id}")
                fallback_result = await self.mlb_resolver.resolve_game_id(
                    external_id=record.external_matchup_id or "",
                    source=DataSource.VSIN,
                    home_team=record.home_team_normalized,
                    away_team=record.away_team_normalized,
                    game_date=record.game_date,
                )

                if fallback_result.mlb_game_id:
                    logger.info(
                        f"Resolved MLB game ID via fallback for VSIN game {record.external_matchup_id}: {fallback_result.mlb_game_id} "
                        f"({record.away_team_normalized} @ {record.home_team_normalized}) - confidence: {fallback_result.confidence}"
                    )
                    return fallback_result.mlb_game_id

            logger.warning(
                f"Failed to resolve MLB game ID for VSIN game {record.external_matchup_id} using all strategies"
            )
            return None

        except Exception as e:
            logger.error(
                f"Error resolving MLB game ID for VSIN record {record.external_matchup_id}: {e}"
            )
            return None

    async def _insert_betting_record(
        self, record: VSINBettingRecord, conn: asyncpg.Connection
    ) -> None:
        """Insert VSIN betting record into database."""
        await conn.execute(
            """
            INSERT INTO staging.vsin_betting_data (
                external_matchup_id, mlb_stats_api_game_id, home_team, away_team,
                home_team_normalized, away_team_normalized, game_date, game_time,
                sportsbook_name, sportsbook_id,
                moneyline_home_odds, moneyline_away_odds,
                moneyline_home_handle_percent, moneyline_away_handle_percent,
                moneyline_home_bets_percent, moneyline_away_bets_percent,
                total_line, total_over_odds, total_under_odds,
                total_over_handle_percent, total_under_handle_percent,
                total_over_bets_percent, total_under_bets_percent,
                runline_spread, runline_home_odds, runline_away_odds,
                runline_home_handle_percent, runline_away_handle_percent,
                runline_home_bets_percent, runline_away_bets_percent,
                moneyline_sharp_side, total_sharp_side, runline_sharp_side,
                sharp_confidence, moneyline_rlm_detected, total_rlm_detected, runline_rlm_detected,
                data_quality_score, validation_status, parsing_errors,
                source_url, vsin_view, raw_data_id, processed_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
                $31, $32, $33, $34, $35, $36, $37, $38, $39, $40,
                $41, $42, $43, $44
            )
            ON CONFLICT (external_matchup_id, sportsbook_name, game_date, processed_at) 
            DO UPDATE SET
                mlb_stats_api_game_id = EXCLUDED.mlb_stats_api_game_id,
                sharp_confidence = EXCLUDED.sharp_confidence,
                data_quality_score = EXCLUDED.data_quality_score,
                updated_at = NOW()
        """,
            record.external_matchup_id,
            record.mlb_stats_api_game_id,
            record.home_team,
            record.away_team,
            record.home_team_normalized,
            record.away_team_normalized,
            record.game_date,
            record.game_time,
            record.sportsbook_name,
            record.sportsbook_id,
            record.moneyline_home_odds,
            record.moneyline_away_odds,
            record.moneyline_home_handle_percent,
            record.moneyline_away_handle_percent,
            record.moneyline_home_bets_percent,
            record.moneyline_away_bets_percent,
            record.total_line,
            record.total_over_odds,
            record.total_under_odds,
            record.total_over_handle_percent,
            record.total_under_handle_percent,
            record.total_over_bets_percent,
            record.total_under_bets_percent,
            record.runline_spread,
            record.runline_home_odds,
            record.runline_away_odds,
            record.runline_home_handle_percent,
            record.runline_away_handle_percent,
            record.runline_home_bets_percent,
            record.runline_away_bets_percent,
            record.moneyline_sharp_side,
            record.total_sharp_side,
            record.runline_sharp_side,
            record.sharp_confidence,
            record.moneyline_rlm_detected,
            record.total_rlm_detected,
            record.runline_rlm_detected,
            record.data_quality_score,
            record.validation_status,
            record.parsing_errors,
            record.source_url,
            record.vsin_view,
            record.raw_data_id,
            now_est(),
        )


# CLI entry point
async def main():
    """Run VSIN data processing from command line."""
    processor = VSINBettingProcessor()
    await processor.initialize()

    try:
        result = await processor.process_vsin_data(limit=5)
        print(f"VSIN processing completed: {result}")
    finally:
        await processor.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
