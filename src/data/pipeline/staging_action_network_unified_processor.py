#!/usr/bin/env python3
"""
Action Network Unified Staging Processor

Consolidated processor that handles both action_network_odds and action_network_history tables.
Replaces staging_action_network_historical_processor.py and staging_action_network_history_processor.py
with unified logic and improved efficiency.

Key Features:
1. Processes both raw_data.action_network_odds and raw_data.action_network_history
2. Unified historical record extraction with exact timestamps
3. Single codebase for all Action Network staging operations
4. Improved performance through batch processing
5. Better error handling and logging
"""

import asyncio
import json
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional
from uuid import uuid4

import asyncpg
from pydantic import BaseModel, Field, field_validator

from ...core.config import get_settings
from ...core.datetime_utils import now_est
from ...core.logging import get_logger, LogComponent
from ...core.sportsbook_utils import SportsbookResolver
from ...services.mlb_stats_api_game_resolution_service import (
    MLBStatsAPIGameResolutionService, 
    DataSource
)

logger = get_logger(__name__, LogComponent.CORE)


class HistoricalOddsRecord(BaseModel):
    """Single historical odds record with exact timestamp."""
    
    # Game and sportsbook identifiers
    external_game_id: str
    mlb_stats_api_game_id: Optional[str] = None
    sportsbook_external_id: str
    sportsbook_id: Optional[int] = None
    sportsbook_name: Optional[str] = None
    
    # Market and side identification
    market_type: str  # moneyline, spread, total
    side: str         # home, away, over, under
    
    # Odds data
    odds: int
    line_value: Optional[Decimal] = None
    
    # Critical timing information
    updated_at: datetime
    data_collection_time: Optional[datetime] = None
    data_processing_time: Optional[datetime] = None
    
    # Line status and metadata
    line_status: Optional[str] = "normal"
    is_current_odds: bool = False
    
    # Action Network metadata
    market_id: Optional[int] = None
    outcome_id: Optional[int] = None
    period: str = "event"
    
    # Data quality
    data_quality_score: float = Field(ge=0.0, le=1.0, default=1.0)
    validation_status: str = "valid"
    
    # Lineage
    raw_data_id: int
    
    @field_validator('market_type')
    @classmethod
    def validate_market_type(cls, v):
        if v not in ['moneyline', 'spread', 'total']:
            raise ValueError(f"Invalid market_type: {v}")
        return v
    
    @field_validator('side')
    @classmethod  
    def validate_side(cls, v):
        if v not in ['home', 'away', 'over', 'under']:
            raise ValueError(f"Invalid side: {v}")
        return v


class ActionNetworkUnifiedStagingProcessor:
    """
    Unified processor for Action Network staging operations.
    
    Processes both raw_data.action_network_odds and raw_data.action_network_history
    to extract complete historical odds data with timestamps.
    """
    
    def __init__(self):
        self.settings = get_settings()
        self.sportsbook_resolver = SportsbookResolver(self._get_db_config())
        self.mlb_resolver = MLBStatsAPIGameResolutionService()
        self.processing_batch_id = str(uuid4())
        
    def _get_db_config(self) -> Dict[str, Any]:
        """Get database configuration."""
        return {
            "host": "localhost",
            "port": 5432,
            "database": "mlb_betting",
            "user": "samlafell"
        }
    
    async def initialize(self):
        """Initialize processor services."""
        await self.mlb_resolver.initialize()
        logger.info("ActionNetworkUnifiedStagingProcessor initialized")
    
    async def cleanup(self):
        """Cleanup processor resources."""
        await self.mlb_resolver.cleanup()
    
    async def process_all_sources(self, limit: int = 100) -> Dict[str, Any]:
        """
        Process data from both Action Network sources.
        
        Args:
            limit: Maximum records to process per source
            
        Returns:
            Combined processing results
        """
        logger.info(f"Starting unified Action Network processing", 
                   batch_id=self.processing_batch_id, limit=limit)
        
        results = {
            "odds_table_results": {},
            "history_table_results": {},
            "combined_totals": {
                "historical_records_processed": 0,
                "historical_records_valid": 0,
                "mlb_games_resolved": 0
            }
        }
        
        try:
            # Process odds table
            odds_results = await self.process_odds_table(limit)
            results["odds_table_results"] = odds_results
            
            # Process history table
            history_results = await self.process_history_table(limit)
            results["history_table_results"] = history_results
            
            # Combine totals
            results["combined_totals"]["historical_records_processed"] = (
                odds_results.get("historical_records_processed", 0) + 
                history_results.get("historical_records_processed", 0)
            )
            results["combined_totals"]["historical_records_valid"] = (
                odds_results.get("historical_records_valid", 0) + 
                history_results.get("historical_records_valid", 0)
            )
            results["combined_totals"]["mlb_games_resolved"] = (
                odds_results.get("mlb_games_resolved", 0) + 
                history_results.get("mlb_games_resolved", 0)
            )
            
            results["processing_batch_id"] = self.processing_batch_id
            results["structure_type"] = "Unified - complete temporal records from both sources"
            
            logger.info("Unified Action Network processing completed", **results["combined_totals"])
            return results
            
        except Exception as e:
            logger.error(f"Error in unified processing: {e}")
            raise
    
    async def process_odds_table(self, limit: int = 100) -> Dict[str, Any]:
        """Process raw_data.action_network_odds table."""
        try:
            conn = await asyncpg.connect(**self._get_db_config())
            
            # Get unprocessed raw odds
            raw_odds = await conn.fetch("""
                SELECT id, external_game_id, sportsbook_key, raw_odds, collected_at
                FROM raw_data.action_network_odds 
                WHERE id NOT IN (
                    SELECT raw_data_id FROM staging.action_network_odds_historical
                    WHERE raw_data_id IS NOT NULL
                )
                ORDER BY collected_at DESC
                LIMIT $1
            """, limit)
            
            if not raw_odds:
                logger.info("No unprocessed odds found")
                return {"historical_records_processed": 0, "historical_records_valid": 0, "mlb_games_resolved": 0}
            
            return await self._process_records(conn, raw_odds, 'action_network_odds')
            
        except Exception as e:
            logger.error(f"Error processing odds table: {e}")
            raise
        finally:
            await conn.close()
    
    async def process_history_table(self, limit: int = 10) -> Dict[str, Any]:
        """Process raw_data.action_network_history table."""
        try:
            conn = await asyncpg.connect(**self._get_db_config())
            
            # Get unprocessed raw history
            raw_history = await conn.fetch("""
                SELECT id, external_game_id, raw_history, collected_at
                FROM raw_data.action_network_history 
                WHERE id NOT IN (
                    SELECT DISTINCT raw_data_id 
                    FROM staging.action_network_odds_historical
                    WHERE raw_data_id IS NOT NULL
                )
                ORDER BY collected_at DESC
                LIMIT $1
            """, limit)
            
            if not raw_history:
                logger.info("No unprocessed history found")
                return {"historical_records_processed": 0, "historical_records_valid": 0, "mlb_games_resolved": 0}
            
            return await self._process_records(conn, raw_history, 'action_network_history')
            
        except Exception as e:
            logger.error(f"Error processing history table: {e}")
            raise
        finally:
            await conn.close()
    
    async def _process_records(
        self, 
        conn: asyncpg.Connection, 
        raw_records: List[Dict], 
        source_table: str
    ) -> Dict[str, Any]:
        """Process records from either source table."""
        processed_count = 0
        valid_count = 0
        mlb_resolved_count = 0
        
        for raw_record in raw_records:
            try:
                # Extract historical records based on source table
                if source_table == 'action_network_odds':
                    historical_records = await self._extract_from_odds(raw_record, conn)
                else:  # action_network_history
                    historical_records = await self._extract_from_history(raw_record, conn)
                
                for historical_record in historical_records:
                    
                    # Resolve MLB Stats API game ID if needed
                    if not historical_record.mlb_stats_api_game_id:
                        mlb_game_id = await self._resolve_mlb_game_id(historical_record, conn)
                        if mlb_game_id:
                            historical_record.mlb_stats_api_game_id = mlb_game_id
                            mlb_resolved_count += 1
                    
                    # Insert historical record
                    await self._insert_historical_odds_record(historical_record, conn)
                    processed_count += 1
                    
                    if historical_record.validation_status == "valid":
                        valid_count += 1
                        
            except Exception as e:
                logger.error(f"Error processing {source_table} record {raw_record['id']}: {e}")
                continue
        
        return {
            "historical_records_processed": processed_count,
            "historical_records_valid": valid_count,
            "mlb_games_resolved": mlb_resolved_count
        }
    
    async def _extract_from_odds(self, raw_odds: Dict, conn: asyncpg.Connection) -> List[HistoricalOddsRecord]:
        """Extract historical records from action_network_odds format."""
        records = []
        
        try:
            raw_odds_data = raw_odds['raw_odds']
            if isinstance(raw_odds_data, str):
                odds_data = json.loads(raw_odds_data)
            else:
                odds_data = raw_odds_data
            
            external_game_id = raw_odds['external_game_id']
            sportsbook_key = raw_odds['sportsbook_key']
            collection_time = raw_odds['collected_at']
            
            # Resolve sportsbook
            sportsbook_mapping = await self.sportsbook_resolver.resolve_action_network_id(int(sportsbook_key))
            sportsbook_id = sportsbook_mapping[0] if sportsbook_mapping else None
            sportsbook_name = sportsbook_mapping[1] if sportsbook_mapping else f"Sportsbook_{sportsbook_key}"
            
            # Process each market type
            for market_type in ['moneyline', 'spread', 'total']:
                market_data = odds_data.get(market_type, [])
                if not market_data:
                    continue
                
                records.extend(await self._process_market_data(
                    market_data, market_type, external_game_id, sportsbook_key,
                    sportsbook_id, sportsbook_name, collection_time, raw_odds['id']
                ))
            
            return records
            
        except Exception as e:
            logger.error(f"Error extracting from odds: {e}")
            return []
    
    async def _extract_from_history(self, raw_history: Dict, conn: asyncpg.Connection) -> List[HistoricalOddsRecord]:
        """Extract historical records from action_network_history format."""
        records = []
        
        try:
            raw_history_data = raw_history['raw_history']
            if isinstance(raw_history_data, str):
                history_data = json.loads(raw_history_data)
            else:
                history_data = raw_history_data
            
            external_game_id = raw_history['external_game_id']
            collection_time = raw_history['collected_at']
            
            # Process each sportsbook in history
            for sportsbook_key, sportsbook_data in history_data.items():
                if not isinstance(sportsbook_data, dict) or 'event' not in sportsbook_data:
                    continue
                
                try:
                    sportsbook_id_int = int(sportsbook_key)
                    sportsbook_mapping = await self.sportsbook_resolver.resolve_action_network_id(sportsbook_id_int)
                    sportsbook_id = sportsbook_mapping[0] if sportsbook_mapping else None
                    sportsbook_name = sportsbook_mapping[1] if sportsbook_mapping else f"Sportsbook_{sportsbook_key}"
                except (ValueError, TypeError):
                    logger.warning(f"Invalid sportsbook key: {sportsbook_key}")
                    continue
                
                event_data = sportsbook_data['event']
                
                # Process each market type
                for market_type in ['moneyline', 'spread', 'total']:
                    market_data = event_data.get(market_type, [])
                    if not market_data:
                        continue
                    
                    records.extend(await self._process_market_data(
                        market_data, market_type, external_game_id, sportsbook_key,
                        sportsbook_id, sportsbook_name, collection_time, raw_history['id']
                    ))
            
            return records
            
        except Exception as e:
            logger.error(f"Error extracting from history: {e}")
            return []
    
    async def _process_market_data(
        self, 
        market_data: List[Dict], 
        market_type: str, 
        external_game_id: str,
        sportsbook_key: str,
        sportsbook_id: Optional[int],
        sportsbook_name: str,
        collection_time: datetime,
        raw_data_id: int
    ) -> List[HistoricalOddsRecord]:
        """Process market data to extract historical records."""
        records = []
        
        for side_data in market_data:
            side = side_data.get('side')
            if not side:
                continue
            
            current_odds = side_data.get('odds')
            line_value = self._safe_decimal(side_data.get('value'))
            market_id = side_data.get('market_id')
            outcome_id = side_data.get('outcome_id')
            
            # Process historical data
            history = side_data.get('history', [])
            current_odds_in_history = False
            
            for hist_entry in history:
                updated_at_str = hist_entry.get('updated_at')
                if not updated_at_str:
                    continue
                
                try:
                    updated_at = self._parse_timestamp(updated_at_str)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Invalid timestamp: {updated_at_str}, error: {e}")
                    continue
                
                hist_odds = hist_entry.get('odds')
                is_current = (hist_odds == current_odds)
                if is_current:
                    current_odds_in_history = True
                
                # Determine line value
                hist_line_value = self._safe_decimal(hist_entry.get('value'))
                final_line_value = None if market_type == 'moneyline' else (hist_line_value or line_value)
                
                # Create historical record
                historical_record = HistoricalOddsRecord(
                    external_game_id=external_game_id,
                    sportsbook_external_id=sportsbook_key,
                    sportsbook_id=sportsbook_id,
                    sportsbook_name=sportsbook_name,
                    market_type=market_type,
                    side=side,
                    odds=hist_odds,
                    line_value=final_line_value,
                    updated_at=updated_at,
                    data_collection_time=collection_time,
                    data_processing_time=now_est(),
                    line_status=hist_entry.get('line_status', 'normal'),
                    is_current_odds=is_current,
                    market_id=market_id,
                    outcome_id=outcome_id,
                    raw_data_id=raw_data_id
                )
                
                records.append(historical_record)
            
            # Add current odds if not in history
            if not current_odds_in_history and current_odds is not None:
                current_line_value = None if market_type == 'moneyline' else line_value
                
                current_record = HistoricalOddsRecord(
                    external_game_id=external_game_id,
                    sportsbook_external_id=sportsbook_key,
                    sportsbook_id=sportsbook_id,
                    sportsbook_name=sportsbook_name,
                    market_type=market_type,
                    side=side,
                    odds=current_odds,
                    line_value=current_line_value,
                    updated_at=collection_time,
                    data_collection_time=collection_time,
                    data_processing_time=now_est(),
                    line_status='normal',
                    is_current_odds=True,
                    market_id=market_id,
                    outcome_id=outcome_id,
                    raw_data_id=raw_data_id
                )
                
                records.append(current_record)
        
        return records
    
    def _parse_timestamp(self, timestamp_str: str) -> datetime:
        """Parse timestamp with microsecond handling."""
        timestamp_clean = timestamp_str.replace('Z', '+00:00')
        
        # Handle microseconds with more than 6 digits
        if '.' in timestamp_clean and '+' in timestamp_clean:
            date_part, time_with_tz = timestamp_clean.split('+')
            if '.' in date_part:
                time_base, microseconds = date_part.rsplit('.', 1)
                # Truncate microseconds to 6 digits
                microseconds = microseconds[:6].ljust(6, '0')
                timestamp_clean = f"{time_base}.{microseconds}+{time_with_tz}"
        
        return datetime.fromisoformat(timestamp_clean)
    
    async def _resolve_mlb_game_id(self, record: HistoricalOddsRecord, conn: asyncpg.Connection) -> Optional[str]:
        """Resolve MLB Stats API game ID."""
        try:
            game_info = await conn.fetchrow("""
                SELECT home_team_normalized, away_team_normalized, game_date
                FROM staging.action_network_games 
                WHERE external_game_id = $1
            """, record.external_game_id)
            
            if not game_info:
                return None
            
            resolution_result = await self.mlb_resolver.resolve_game_id(
                external_id=record.external_game_id,
                source=DataSource.ACTION_NETWORK,
                home_team=game_info['home_team_normalized'],
                away_team=game_info['away_team_normalized'],
                game_date=game_info['game_date']
            )
            
            if resolution_result.mlb_game_id:
                await conn.execute("""
                    UPDATE staging.action_network_games 
                    SET mlb_stats_api_game_id = $1, updated_at = NOW()
                    WHERE external_game_id = $2 AND mlb_stats_api_game_id IS NULL
                """, resolution_result.mlb_game_id, record.external_game_id)
                
                return resolution_result.mlb_game_id
            
            return None
            
        except Exception as e:
            logger.error(f"Error resolving MLB game ID: {e}")
            return None
    
    async def _insert_historical_odds_record(self, record: HistoricalOddsRecord, conn: asyncpg.Connection) -> None:
        """Insert historical odds record with source table tracking."""
        await conn.execute("""
            INSERT INTO staging.action_network_odds_historical (
                external_game_id, mlb_stats_api_game_id, sportsbook_external_id,
                sportsbook_id, sportsbook_name, market_type, side,
                odds, line_value, updated_at, data_collection_time, data_processing_time,
                line_status, is_current_odds, market_id, outcome_id, period,
                data_quality_score, validation_status, raw_data_id, processed_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21
            )
            ON CONFLICT (external_game_id, sportsbook_external_id, market_type, side, updated_at) 
            DO UPDATE SET
                mlb_stats_api_game_id = EXCLUDED.mlb_stats_api_game_id,
                is_current_odds = EXCLUDED.is_current_odds,
                data_processing_time = EXCLUDED.data_processing_time,
                updated_at_record = NOW()
        """,
        record.external_game_id, record.mlb_stats_api_game_id, record.sportsbook_external_id,
        record.sportsbook_id, record.sportsbook_name, record.market_type, record.side,
        record.odds, record.line_value, record.updated_at, record.data_collection_time, 
        record.data_processing_time, record.line_status, record.is_current_odds,
        record.market_id, record.outcome_id, record.period,
        record.data_quality_score, record.validation_status, record.raw_data_id, now_est()
        )
    
    def _safe_decimal(self, value: Any) -> Optional[Decimal]:
        """Safely convert value to Decimal."""
        if value is None:
            return None
        try:
            return Decimal(str(value))
        except (ValueError, TypeError):
            return None


# CLI entry point
async def main():
    """Run unified staging processing from command line."""
    processor = ActionNetworkUnifiedStagingProcessor()
    await processor.initialize()
    
    try:
        result = await processor.process_all_sources(limit=10)
        print(f"Unified processing completed: {result}")
    finally:
        await processor.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
