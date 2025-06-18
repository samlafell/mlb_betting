"""
Pinnacle API parser for betting odds data.

This module provides functionality to parse Pinnacle API data into
validated BettingSplit model instances with comprehensive error handling.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Type
import re

import structlog

from .base import BaseParser, ValidationResult, ValidationConfig
from ..models.splits import BettingSplit, SplitType, BookType, DataSource
from ..models.game import Team
from ..models.pinnacle import PinnacleMarket, PinnacleMarketType, PriceDesignation
from ..core.exceptions import ValidationError
from ..services.mlb_api_service import MLBStatsAPIService

logger = structlog.get_logger(__name__)


class PinnacleParser(BaseParser):
    """
    Parser for Pinnacle API odds data.
    
    Transforms raw Pinnacle API data into validated BettingSplit model 
    instances following the existing patterns in the system.
    """
    
    def __init__(self, validation_config: Optional[ValidationConfig] = None) -> None:
        """
        Initialize Pinnacle parser.
        
        Args:
            validation_config: Validation configuration
        """
        super().__init__(
            parser_name="Pinnacle", 
            validation_config=validation_config
        )
        
        # Initialize MLB Stats API service for official game IDs
        self.mlb_api_service = MLBStatsAPIService()
        
        # Data source mapping
        self.data_source = DataSource.VSIN  # Using VSIN as closest match for now
        self.book_type = BookType.CIRCA     # Using Circa as it's the most respected book
    
    @property
    def target_model_class(self) -> Type[BettingSplit]:
        """Get the target model class for this parser."""
        return BettingSplit
    
    async def parse_raw_data(self, raw_data: Dict[str, Any]) -> Optional[BettingSplit]:
        """
        Parse a single raw Pinnacle market data item into a BettingSplit model.
        
        Args:
            raw_data: Raw data dictionary from Pinnacle API
            
        Returns:
            Parsed BettingSplit instance or None if parsing fails
        """
        try:
            # Extract market data
            if "market" not in raw_data:
                return None
                
            market = raw_data["market"]
            if not isinstance(market, PinnacleMarket):
                return None
            
            # Convert market to BettingSplit based on market type
            if market.market_type == PinnacleMarketType.MONEYLINE:
                return self._parse_moneyline_split(market)
            elif market.market_type == PinnacleMarketType.SPREAD:
                return self._parse_spread_split(market)
            elif market.market_type == PinnacleMarketType.TOTAL:
                return self._parse_total_split(market)
            else:
                self.logger.debug("Unsupported market type", market_type=market.market_type)
                return None
                
        except Exception as e:
            self.logger.error("Failed to parse Pinnacle data", error=str(e))
            return None
    
    def _parse_moneyline_split(self, market: PinnacleMarket) -> Optional[BettingSplit]:
        """
        Parse moneyline market into BettingSplit.
        
        Args:
            market: PinnacleMarket object
            
        Returns:
            BettingSplit for moneyline or None
        """
        try:
            home_price = market.get_home_price()
            away_price = market.get_away_price()
            
            if not home_price or not away_price:
                return None
            
            # Calculate normalized implied probabilities
            home_prob = home_price.implied_probability
            away_prob = away_price.implied_probability
            total_prob = home_prob + away_prob
            
            if total_prob > 0:
                home_normalized = (home_prob / total_prob) * 100
                away_normalized = (away_prob / total_prob) * 100
            else:
                home_normalized = 50.0
                away_normalized = 50.0
            
            # Generate game ID
            game_id = self._generate_game_id(market)
            
            betting_split = BettingSplit(
                game_id=game_id,
                home_team=market.home_team,
                away_team=market.away_team,
                game_datetime=market.game_datetime,
                split_type=SplitType.MONEYLINE,
                split_value=None,
                source=self.data_source,
                book=self.book_type,
                last_updated=market.last_updated,
                home_or_over_bets_percentage=home_normalized,
                away_or_under_bets_percentage=away_normalized,
            )
            
            return betting_split
            
        except Exception as e:
            self.logger.error("Failed to parse moneyline split", error=str(e))
            return None
    
    def _parse_spread_split(self, market: PinnacleMarket) -> Optional[BettingSplit]:
        """
        Parse spread market into BettingSplit.
        
        Args:
            market: PinnacleMarket object
            
        Returns:
            BettingSplit for spread or None
        """
        try:
            home_price = market.get_home_price()
            away_price = market.get_away_price()
            
            if not home_price or not away_price or market.line_value is None:
                return None
            
            # Calculate normalized implied probabilities
            home_prob = home_price.implied_probability
            away_prob = away_price.implied_probability
            total_prob = home_prob + away_prob
            
            if total_prob > 0:
                home_normalized = (home_prob / total_prob) * 100
                away_normalized = (away_prob / total_prob) * 100
            else:
                home_normalized = 50.0
                away_normalized = 50.0
            
            game_id = self._generate_game_id(market)
            
            betting_split = BettingSplit(
                game_id=game_id,
                home_team=market.home_team,
                away_team=market.away_team,
                game_datetime=market.game_datetime,
                split_type=SplitType.SPREAD,
                split_value=market.line_value,
                source=self.data_source,
                book=self.book_type,
                last_updated=market.last_updated,
                home_or_over_bets_percentage=home_normalized,
                away_or_under_bets_percentage=away_normalized,
            )
            
            return betting_split
            
        except Exception as e:
            self.logger.error("Failed to parse spread split", error=str(e))
            return None
    
    def _parse_total_split(self, market: PinnacleMarket) -> Optional[BettingSplit]:
        """
        Parse total market into BettingSplit.
        
        Args:
            market: PinnacleMarket object
            
        Returns:
            BettingSplit for total or None
        """
        try:
            over_price = market.get_over_price()
            under_price = market.get_under_price()
            
            if not over_price or not under_price or market.line_value is None:
                return None
            
            # Calculate normalized implied probabilities
            over_prob = over_price.implied_probability
            under_prob = under_price.implied_probability
            total_prob = over_prob + under_prob
            
            if total_prob > 0:
                over_normalized = (over_prob / total_prob) * 100
                under_normalized = (under_prob / total_prob) * 100
            else:
                over_normalized = 50.0
                under_normalized = 50.0
            
            game_id = self._generate_game_id(market)
            
            betting_split = BettingSplit(
                game_id=game_id,
                home_team=market.home_team,
                away_team=market.away_team,
                game_datetime=market.game_datetime,
                split_type=SplitType.TOTAL,
                split_value=market.line_value,
                source=self.data_source,
                book=self.book_type,
                last_updated=market.last_updated,
                home_or_over_bets_percentage=over_normalized,
                away_or_under_bets_percentage=under_normalized,
            )
            
            return betting_split
            
        except Exception as e:
            self.logger.error("Failed to parse total split", error=str(e))
            return None
    
    def _generate_game_id(self, market: PinnacleMarket) -> str:
        """
        Generate a game ID for the split.
        
        Args:
            market: PinnacleMarket object
            
        Returns:
            Generated game ID
        """
        try:
            # Try to get official game ID from MLB API
            official_game_id = self.mlb_api_service.get_official_game_id(
                market.home_team.value,
                market.away_team.value,
                market.game_datetime
            )
            
            if official_game_id:
                return official_game_id
            
            # Fallback to generated ID
            date_str = market.game_datetime.strftime("%Y%m%d")
            return f"{market.away_team.value}_{market.home_team.value}_{date_str}"
            
        except Exception as e:
            self.logger.debug("Failed to generate official game ID, using fallback", error=str(e))
            date_str = market.game_datetime.strftime("%Y%m%d")
            return f"{market.away_team.value}_{market.home_team.value}_{date_str}"
    
    async def parse_pinnacle_markets(self, markets: List[PinnacleMarket]) -> List[BettingSplit]:
        """
        Parse list of Pinnacle markets into BettingSplit objects.
        
        Args:
            markets: List of PinnacleMarket objects
            
        Returns:
            List of BettingSplit objects
        """
        splits = []
        
        for market in markets:
            try:
                raw_data = {"market": market}
                split = await self.parse_raw_data(raw_data)
                
                if split:
                    splits.append(split)
                    
            except Exception as e:
                self.logger.warning("Failed to parse market", error=str(e))
                continue
        
        self.logger.info("Successfully parsed Pinnacle markets", 
                        input_count=len(markets),
                        output_count=len(splits))
        
        return splits
    
    async def _custom_validation(self, item: BettingSplit) -> ValidationResult:
        """
        Perform custom validation for Pinnacle-derived splits.
        
        Args:
            item: BettingSplit to validate
            
        Returns:
            ValidationResult
        """
        errors = []
        warnings = []
        
        # Check that implied probability percentages are reasonable
        if item.home_or_over_bets_percentage is not None:
            if item.home_or_over_bets_percentage < 5 or item.home_or_over_bets_percentage > 95:
                warnings.append(f"Unusual home/over percentage: {item.home_or_over_bets_percentage}")
        
        if item.away_or_under_bets_percentage is not None:
            if item.away_or_under_bets_percentage < 5 or item.away_or_under_bets_percentage > 95:
                warnings.append(f"Unusual away/under percentage: {item.away_or_under_bets_percentage}")
        
        # Check split value is reasonable for spread/total
        if item.split_type == SplitType.SPREAD and item.split_value is not None:
            if abs(item.split_value) > 20:
                warnings.append(f"Large spread value: {item.split_value}")
        
        if item.split_type == SplitType.TOTAL and item.split_value is not None:
            if item.split_value < 4 or item.split_value > 20:
                warnings.append(f"Unusual total value: {item.split_value}")
        
        # Check that teams are valid
        if item.home_team == item.away_team:
            errors.append("Home and away teams cannot be the same")
        
        is_valid = len(errors) == 0
        
        return ValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings
        )


async def parse_pinnacle_data(
    markets: List[PinnacleMarket], 
    validation_config: Optional[ValidationConfig] = None
) -> List[BettingSplit]:
    """
    Parse Pinnacle market data into BettingSplit objects.
    
    Args:
        markets: List of PinnacleMarket objects from Pinnacle API
        validation_config: Optional validation configuration
        
    Returns:
        List of validated BettingSplit objects
    """
    parser = PinnacleParser(validation_config=validation_config)
    return await parser.parse_pinnacle_markets(markets) 