"""
Base Signal Processor - Common Interface for All Signal Types

Provides shared functionality and establishes a consistent interface
for all signal processing implementations.
"""

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
import pytz

from ..models.betting_analysis import (
    BettingSignal, SignalType, ConfidenceLevel, SignalProcessorConfig, 
    ProfitableStrategy
)
from ..services.betting_signal_repository import BettingSignalRepository
from ..services.strategy_validator import StrategyValidator
from ..services.juice_filter_service import get_juice_filter_service
from ..services.confidence_scorer import get_confidence_scorer
from ..core.logging import get_logger


class BaseSignalProcessor(ABC):
    """Base class for all signal processors"""
    
    def __init__(self, repository: BettingSignalRepository, 
                 validator: StrategyValidator, config: SignalProcessorConfig):
        self.repository = repository
        self.validator = validator
        self.config = config
        self.juice_filter = get_juice_filter_service()
        self.confidence_scorer = get_confidence_scorer()
        self.logger = get_logger(__name__)
        self.est = pytz.timezone('US/Eastern')
    
    @abstractmethod
    async def process(self, minutes_ahead: int, 
                     profitable_strategies: List[ProfitableStrategy]) -> List[BettingSignal]:
        """Process signals for this specific signal type"""
        pass
    
    @abstractmethod
    def get_signal_type(self) -> SignalType:
        """Return the signal type this processor handles"""
        pass
    
    def _create_time_window(self, minutes_ahead: int) -> tuple[datetime, datetime]:
        """Create time window for analysis"""
        now_est = datetime.now(self.est)
        end_time = now_est + timedelta(minutes=minutes_ahead)
        return now_est, end_time
    
    def _normalize_game_time(self, game_time: datetime) -> datetime:
        """Normalize game time to EST timezone"""
        if game_time.tzinfo is None:
            return self.est.localize(game_time)
        else:
            return game_time.astimezone(self.est)
    
    def _calculate_minutes_to_game(self, game_time: datetime, current_time: datetime) -> int:
        """Calculate minutes until game starts"""
        return int((game_time - current_time).total_seconds() / 60)
    
    def _should_filter_juice(self, split_type: str, split_value: Optional[str], 
                            recommended_team: str, home_team: str, away_team: str) -> bool:
        """Check if bet should be filtered due to juice"""
        if split_type == 'moneyline' and split_value:
            return self.juice_filter.should_filter_bet(
                split_value, recommended_team, home_team, away_team, 
                self.get_signal_type().value.lower()
            )
        return False
    
    def _get_recommendation(self, split_type: str, differential: float, 
                          home_team: str, away_team: str) -> str:
        """Generate betting recommendation based on differential"""
        if split_type in ['moneyline', 'spread']:
            return f"BET {home_team}" if differential > 0 else f"BET {away_team}"
        elif split_type == 'total':
            return "BET OVER" if differential > 0 else "BET UNDER"
        return "UNKNOWN"
    
    def _calculate_confidence(self, differential: float, source: str, book: Optional[str],
                            split_type: str, strategy_name: str, last_updated: datetime,
                            game_datetime: datetime, **kwargs) -> Dict[str, Any]:
        """Calculate comprehensive confidence score for a signal"""
        confidence_result = self.confidence_scorer.calculate_confidence(
            signal_differential=float(differential),
            source=source,
            book=book or 'UNKNOWN',
            split_type=split_type,
            strategy_name=strategy_name.lower(),
            last_updated=last_updated,
            game_datetime=game_datetime,
            **kwargs
        )
        
        return {
            'confidence_score': confidence_result.overall_confidence,
            'confidence_level': self._map_confidence_level(confidence_result.confidence_level),
            'confidence_explanation': confidence_result.explanation,
            'recommendation_strength': confidence_result.recommendation_strength
        }
    
    def _map_confidence_level(self, level_str: str) -> ConfidenceLevel:
        """Map confidence level string to enum"""
        level_mapping = {
            'VERY_LOW': ConfidenceLevel.VERY_LOW,
            'LOW': ConfidenceLevel.LOW,
            'MODERATE': ConfidenceLevel.MODERATE,
            'HIGH': ConfidenceLevel.HIGH,
            'VERY_HIGH': ConfidenceLevel.VERY_HIGH
        }
        return level_mapping.get(level_str.upper(), ConfidenceLevel.MODERATE)
    
    def _create_betting_signal(self, raw_data: Dict[str, Any], strategy: ProfitableStrategy,
                              confidence_data: Dict[str, Any], metadata: Dict[str, Any] = None) -> BettingSignal:
        """Create a standardized BettingSignal from raw data"""
        game_time = self._normalize_game_time(raw_data['game_datetime'])
        now_est = datetime.now(self.est)
        
        return BettingSignal(
            signal_type=self.get_signal_type(),
            home_team=raw_data['home_team'],
            away_team=raw_data['away_team'],
            game_time=game_time,
            minutes_to_game=self._calculate_minutes_to_game(game_time, now_est),
            split_type=raw_data['split_type'],
            split_value=raw_data.get('split_value'),
            source=raw_data['source'],
            book=raw_data.get('book'),
            differential=float(raw_data['differential']),
            signal_strength=abs(float(raw_data['differential'])),
            confidence_score=confidence_data['confidence_score'],
            confidence_level=confidence_data['confidence_level'],
            confidence_explanation=confidence_data['confidence_explanation'],
            recommendation=self._get_recommendation(
                raw_data['split_type'], 
                raw_data['differential'], 
                raw_data['home_team'], 
                raw_data['away_team']
            ),
            recommendation_strength=confidence_data['recommendation_strength'],
            last_updated=raw_data['last_updated'],
            strategy_name=strategy.strategy_name,
            win_rate=strategy.win_rate,
            roi=strategy.roi,
            total_bets=strategy.total_bets,
            metadata=metadata or {}
        )
    
    def _log_processing_summary(self, signal_count: int, strategies_count: int, 
                               raw_data_count: int):
        """Log processing summary for debugging"""
        self.logger.info(
            f"{self.__class__.__name__}: Processed {raw_data_count} raw signals, "
            f"found {signal_count} valid signals using {strategies_count} strategies"
        ) 