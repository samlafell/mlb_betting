# Enhanced Action Network History Processor with Betting Percentage Support
# Purpose: Process Action Network history data and extract betting percentages
# Extends: staging_action_network_history_processor.py

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

import asyncpg

from ...core.config import get_settings
from ...core.logging import get_logger, LogComponent

logger = get_logger(__name__, LogComponent.DATA_PIPELINE)


@dataclass
class EnhancedHistoricalOddsRecord:
    """Enhanced historical odds record with betting percentage data."""
    # Core odds data
    external_game_id: str
    mlb_stats_api_game_id: Optional[str]
    sportsbook_external_id: str
    sportsbook_id: Optional[int]
    sportsbook_name: Optional[str]
    market_type: str
    side: str
    odds: int
    line_value: Optional[float]
    updated_at: datetime
    
    # Metadata
    data_collection_time: Optional[datetime]
    data_processing_time: Optional[datetime]
    line_status: Optional[str]
    market_id: Optional[int]
    outcome_id: Optional[int]
    period: str = "event"
    data_quality_score: float = 1.0
    validation_status: str = "valid"
    raw_data_id: Optional[int] = None
    
    # NEW: Betting percentage data
    bet_percent_tickets: Optional[int] = None      # Ticket percentage (0-100)
    bet_percent_money: Optional[int] = None        # Money percentage (0-100)
    bet_value_tickets: Optional[int] = None        # Actual ticket count
    bet_value_money: Optional[int] = None          # Actual money amount
    bet_info_available: bool = False               # Flag indicating betting data exists
    
    # Derived betting analysis fields
    betting_divergence: Optional[float] = None     # |tickets% - money%|
    sharp_action_indicator: Optional[str] = None   # Classification of betting pattern


class EnhancedActionNetworkBettingProcessor:
    """Enhanced processor for Action Network data with betting percentage extraction."""
    
    def __init__(self):
        self.settings = get_settings()
        self.db_config = self.settings.database.model_dump()
        self.processing_batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
    async def extract_bet_info_from_line_data(self, line_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract betting percentage information from Action Network line data.
        
        Args:
            line_data: Individual line data from Action Network history response
            
        Returns:
            Dict containing extracted betting information
        """
        bet_info = line_data.get("bet_info", {})
        
        # Initialize with default values
        extracted_data = {
            "bet_percent_tickets": None,
            "bet_percent_money": None,
            "bet_value_tickets": None,
            "bet_value_money": None,
            "bet_info_available": False,
            "betting_divergence": None,
            "sharp_action_indicator": None
        }
        
        if not bet_info or not isinstance(bet_info, dict):
            logger.debug("No bet_info available in line data")
            return extracted_data
        
        try:
            # Extract ticket data
            tickets_data = bet_info.get("tickets", {})
            if isinstance(tickets_data, dict):
                tickets_percent = tickets_data.get("percent")
                tickets_value = tickets_data.get("value")
                
                # Validate and store ticket percentage
                if tickets_percent is not None and isinstance(tickets_percent, (int, float)):
                    if 0 <= tickets_percent <= 100:
                        extracted_data["bet_percent_tickets"] = int(tickets_percent)
                    else:
                        logger.warning(f"Invalid ticket percentage: {tickets_percent}")
                
                # Store ticket value if meaningful
                if tickets_value is not None and isinstance(tickets_value, (int, float)) and tickets_value > 0:
                    extracted_data["bet_value_tickets"] = int(tickets_value)
            
            # Extract money data
            money_data = bet_info.get("money", {})
            if isinstance(money_data, dict):
                money_percent = money_data.get("percent")
                money_value = money_data.get("value")
                
                # Validate and store money percentage
                if money_percent is not None and isinstance(money_percent, (int, float)):
                    if 0 <= money_percent <= 100:
                        extracted_data["bet_percent_money"] = int(money_percent)
                    else:
                        logger.warning(f"Invalid money percentage: {money_percent}")
                
                # Store money value if meaningful
                if money_value is not None and isinstance(money_value, (int, float)) and money_value > 0:
                    extracted_data["bet_value_money"] = int(money_value)
            
            # Determine if betting info is available
            betting_data_exists = (
                extracted_data["bet_percent_tickets"] is not None or 
                extracted_data["bet_percent_money"] is not None
            )
            extracted_data["bet_info_available"] = betting_data_exists
            
            # Calculate derived metrics if both percentages available
            if (extracted_data["bet_percent_tickets"] is not None and 
                extracted_data["bet_percent_money"] is not None):
                
                # Calculate divergence
                divergence = abs(
                    extracted_data["bet_percent_tickets"] - extracted_data["bet_percent_money"]
                )
                extracted_data["betting_divergence"] = divergence
                
                # Classify sharp action
                tickets_pct = extracted_data["bet_percent_tickets"]
                money_pct = extracted_data["bet_percent_money"]
                
                if money_pct > tickets_pct + 15:
                    extracted_data["sharp_action_indicator"] = "Sharp Money Heavy"
                elif tickets_pct > money_pct + 15:
                    extracted_data["sharp_action_indicator"] = "Public Heavy"
                elif divergence <= 5:
                    extracted_data["sharp_action_indicator"] = "Aligned"
                else:
                    extracted_data["sharp_action_indicator"] = "Moderate Divergence"
            
            if betting_data_exists:
                logger.debug(
                    f"Extracted betting data - Tickets: {extracted_data['bet_percent_tickets']}%, "
                    f"Money: {extracted_data['bet_percent_money']}%, "
                    f"Divergence: {extracted_data['betting_divergence']}, "
                    f"Indicator: {extracted_data['sharp_action_indicator']}"
                )
                
        except Exception as e:
            logger.error(f"Error extracting bet_info: {e}", exc_info=True)
            # Return default values on error
            pass
        
        return extracted_data
    
    async def create_enhanced_historical_record(
        self, 
        base_record_data: Dict[str, Any], 
        line_data: Dict[str, Any],
        history_entry: Dict[str, Any]
    ) -> EnhancedHistoricalOddsRecord:
        """Create enhanced historical record with betting percentage data.
        
        Args:
            base_record_data: Base record information (game_id, sportsbook, etc.)
            line_data: Current line data with bet_info
            history_entry: Individual historical entry
            
        Returns:
            EnhancedHistoricalOddsRecord with betting data populated
        """
        # Extract betting information
        betting_data = await self.extract_bet_info_from_line_data(line_data)
        
        # Create enhanced record
        record = EnhancedHistoricalOddsRecord(
            # Core data from base_record_data and history_entry
            external_game_id=base_record_data["external_game_id"],
            mlb_stats_api_game_id=base_record_data.get("mlb_stats_api_game_id"),
            sportsbook_external_id=str(base_record_data["sportsbook_id"]),
            sportsbook_id=base_record_data.get("sportsbook_id_int"),
            sportsbook_name=base_record_data.get("sportsbook_name"),
            market_type=base_record_data["market_type"],
            side=base_record_data["side"],
            odds=history_entry["odds"],
            line_value=history_entry.get("value"),
            updated_at=datetime.fromisoformat(history_entry["updated_at"].replace("Z", "+00:00")),
            line_status=history_entry.get("line_status", "normal"),
            
            # Metadata
            data_collection_time=base_record_data.get("collection_time"),
            data_processing_time=datetime.now(),
            market_id=line_data.get("market_id"),
            outcome_id=line_data.get("outcome_id"),
            raw_data_id=base_record_data.get("raw_data_id"),
            
            # Enhanced betting data
            bet_percent_tickets=betting_data["bet_percent_tickets"],
            bet_percent_money=betting_data["bet_percent_money"],
            bet_value_tickets=betting_data["bet_value_tickets"],
            bet_value_money=betting_data["bet_value_money"],
            bet_info_available=betting_data["bet_info_available"],
            betting_divergence=betting_data["betting_divergence"],
            sharp_action_indicator=betting_data["sharp_action_indicator"]
        )
        
        return record
    
    async def insert_enhanced_historical_record(
        self, 
        record: EnhancedHistoricalOddsRecord, 
        conn: asyncpg.Connection
    ) -> None:
        """Insert enhanced historical odds record with betting percentage data.
        
        Args:
            record: Enhanced historical odds record to insert
            conn: Database connection
        """
        try:
            await conn.execute(
                """
                INSERT INTO staging.action_network_odds_historical (
                    external_game_id, mlb_stats_api_game_id, sportsbook_external_id,
                    sportsbook_id, sportsbook_name, market_type, side, odds, line_value,
                    updated_at, data_collection_time, data_processing_time, line_status,
                    market_id, outcome_id, period, data_quality_score, validation_status,
                    raw_data_id,
                    -- Enhanced betting percentage columns
                    bet_percent_tickets, bet_percent_money, bet_value_tickets, 
                    bet_value_money, bet_info_available
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, 
                    $16, $17, $18, $19, $20, $21, $22, $23, $24, $25
                )
                ON CONFLICT (external_game_id, sportsbook_external_id, market_type, side, updated_at) 
                DO UPDATE SET
                    -- Update core data
                    odds = EXCLUDED.odds,
                    line_value = EXCLUDED.line_value,
                    line_status = EXCLUDED.line_status,
                    -- Update betting percentage data
                    bet_percent_tickets = EXCLUDED.bet_percent_tickets,
                    bet_percent_money = EXCLUDED.bet_percent_money,
                    bet_value_tickets = EXCLUDED.bet_value_tickets,
                    bet_value_money = EXCLUDED.bet_value_money,
                    bet_info_available = EXCLUDED.bet_info_available,
                    -- Update metadata
                    data_processing_time = EXCLUDED.data_processing_time,
                    updated_at_record = NOW()
                """,
                # Core record data
                record.external_game_id,
                record.mlb_stats_api_game_id,
                record.sportsbook_external_id,
                record.sportsbook_id,
                record.sportsbook_name,
                record.market_type,
                record.side,
                record.odds,
                record.line_value,
                record.updated_at,
                record.data_collection_time,
                record.data_processing_time,
                record.line_status,
                record.market_id,
                record.outcome_id,
                record.period,
                record.data_quality_score,
                record.validation_status,
                record.raw_data_id,
                # Enhanced betting data
                record.bet_percent_tickets,
                record.bet_percent_money,
                record.bet_value_tickets,
                record.bet_value_money,
                record.bet_info_available
            )
            
            # Log betting data if available for debugging
            if record.bet_info_available:
                logger.debug(
                    f"Inserted betting data for {record.external_game_id} "
                    f"{record.sportsbook_name} {record.market_type} {record.side}: "
                    f"T:{record.bet_percent_tickets}% M:{record.bet_percent_money}% "
                    f"Div:{record.betting_divergence} Action:{record.sharp_action_indicator}"
                )
                
        except Exception as e:
            logger.error(
                f"Error inserting enhanced historical record: {e}",
                extra={
                    "game_id": record.external_game_id,
                    "sportsbook": record.sportsbook_name,
                    "market": record.market_type,
                    "betting_data_available": record.bet_info_available
                }
            )
            raise
    
    async def validate_betting_data_quality(self, conn: asyncpg.Connection) -> Dict[str, Any]:
        """Validate quality of betting percentage data after processing.
        
        Args:
            conn: Database connection
            
        Returns:
            Dict containing validation metrics
        """
        try:
            # Get validation results
            validation_result = await conn.fetchrow(
                "SELECT * FROM staging.validate_betting_percentage_data()"
            )
            
            if validation_result:
                validation_data = dict(validation_result)
                logger.info(
                    "Betting percentage data quality validation completed",
                    **validation_data
                )
                return validation_data
            else:
                logger.warning("No validation results returned")
                return {}
                
        except Exception as e:
            logger.error(f"Error validating betting data quality: {e}")
            return {"error": str(e)}


# Integration helper functions for existing processor
def enhance_existing_historical_record_with_betting_data(
    existing_record: Any,  # Your existing HistoricalOddsRecord class
    line_data: Dict[str, Any]
) -> Any:
    """Helper function to enhance existing historical records with betting data.
    
    This function can be integrated into your existing staging processor
    to add betting percentage support without major refactoring.
    """
    processor = EnhancedActionNetworkBettingProcessor()
    
    # Extract betting data
    import asyncio
    betting_data = asyncio.run(processor.extract_bet_info_from_line_data(line_data))
    
    # Add betting fields to existing record
    existing_record.bet_percent_tickets = betting_data["bet_percent_tickets"]
    existing_record.bet_percent_money = betting_data["bet_percent_money"]
    existing_record.bet_value_tickets = betting_data["bet_value_tickets"]
    existing_record.bet_value_money = betting_data["bet_value_money"]
    existing_record.bet_info_available = betting_data["bet_info_available"]
    
    return existing_record


async def update_existing_insert_query_with_betting_columns(
    existing_insert_function: callable,
    record: Any,
    conn: asyncpg.Connection
) -> None:
    """Helper to update existing insert queries with betting columns.
    
    This is a wrapper that can be used to enhance existing insert functions
    without completely rewriting them.
    """
    # This would be customized based on your existing insert function
    # The key is to add the 5 new betting columns to the INSERT statement
    pass