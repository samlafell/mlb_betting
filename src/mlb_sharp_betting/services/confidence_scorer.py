#!/usr/bin/env python3
"""
CONFIDENCE SCORING SERVICE
=========================

Calculates confidence scores for betting recommendations based on:
1. Signal Strength (differential between money% and bets%)
2. Source Reliability (historical performance of source/book combination)
3. Strategy Performance (historical win rate and ROI of the specific strategy)
4. Data Quality (recency, cross-validation, sample size)
5. Market Context (time to game, market conditions)

Confidence Score Range: 0-100
- 90-100: VERY HIGH CONFIDENCE (bet with confidence)
- 75-89:  HIGH CONFIDENCE (strong recommendation)
- 60-74:  MODERATE CONFIDENCE (good bet)
- 45-59:  LOW CONFIDENCE (proceed with caution)
- 0-44:   VERY LOW CONFIDENCE (avoid or minimal bet)
"""

import math
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import pytz

from .database_coordinator import get_database_coordinator


@dataclass
class ConfidenceComponents:
    """Individual components that make up the confidence score"""
    signal_strength_score: float
    source_reliability_score: float
    strategy_performance_score: float
    data_quality_score: float
    market_context_score: float
    
    # Raw values for transparency
    signal_differential: float
    source_win_rate: Optional[float]
    source_roi: Optional[float]
    strategy_win_rate: Optional[float]
    strategy_roi: Optional[float]
    data_age_minutes: float
    minutes_to_game: float


@dataclass
class ConfidenceResult:
    """Complete confidence scoring result"""
    overall_confidence: float
    confidence_level: str
    components: ConfidenceComponents
    explanation: str
    recommendation_strength: str


class ConfidenceScorer:
    """Calculates confidence scores for betting recommendations"""
    
    def __init__(self):
        self.coordinator = get_database_coordinator()
        self.est = pytz.timezone('US/Eastern')
        
        # Scoring weights (must sum to 1.0) - ADJUSTED for better signal detection
        self.weights = {
            'signal_strength': 0.50,      # INCREASED from 0.40 - More emphasis on actual signal
            'source_reliability': 0.25,   # DECREASED from 0.30 - Less weight on historical performance
            'strategy_performance': 0.15, # DECREASED from 0.20 - Less weight on strategy history
            'data_quality': 0.05,         # Same - Data freshness and quality
            'market_context': 0.05        # Same - Time and market conditions
        }
    
    def calculate_confidence(
        self,
        signal_differential: float,
        source: str,
        book: str,
        split_type: str,
        strategy_name: str,
        last_updated: datetime,
        game_datetime: datetime,
        cross_validation_sources: int = 1
    ) -> ConfidenceResult:
        """
        Calculate overall confidence score for a betting recommendation
        
        Args:
            signal_differential: The money% - bets% differential
            source: Data source (e.g., 'VSIN', 'SBD')
            book: Sportsbook (e.g., 'draftkings', 'circa')
            split_type: Type of bet ('moneyline', 'spread', 'total')
            strategy_name: Name of the strategy generating the recommendation
            last_updated: When the data was last updated
            game_datetime: When the game starts
            cross_validation_sources: Number of sources confirming the signal
        
        Returns:
            ConfidenceResult with detailed scoring breakdown
        """
        
        # Calculate individual component scores
        signal_score = self._calculate_signal_strength_score(signal_differential)
        
        source_score, source_win_rate, source_roi = self._calculate_source_reliability_score(
            source, book, split_type
        )
        
        strategy_score, strategy_win_rate, strategy_roi = self._calculate_strategy_performance_score(
            strategy_name, source, book, split_type
        )
        
        data_score, data_age_minutes = self._calculate_data_quality_score(
            last_updated, cross_validation_sources
        )
        
        context_score, minutes_to_game = self._calculate_market_context_score(
            game_datetime, last_updated
        )
        
        # Calculate weighted overall confidence
        overall_confidence = (
            signal_score * self.weights['signal_strength'] +
            source_score * self.weights['source_reliability'] +
            strategy_score * self.weights['strategy_performance'] +
            data_score * self.weights['data_quality'] +
            context_score * self.weights['market_context']
        )
        
        # Create components object
        components = ConfidenceComponents(
            signal_strength_score=signal_score,
            source_reliability_score=source_score,
            strategy_performance_score=strategy_score,
            data_quality_score=data_score,
            market_context_score=context_score,
            signal_differential=signal_differential,
            source_win_rate=source_win_rate,
            source_roi=source_roi,
            strategy_win_rate=strategy_win_rate,
            strategy_roi=strategy_roi,
            data_age_minutes=data_age_minutes,
            minutes_to_game=minutes_to_game
        )
        
        # Determine confidence level and explanation
        confidence_level = self._get_confidence_level(overall_confidence)
        explanation = self._generate_explanation(overall_confidence, components)
        recommendation_strength = self._get_recommendation_strength(overall_confidence)
        
        return ConfidenceResult(
            overall_confidence=overall_confidence,
            confidence_level=confidence_level,
            components=components,
            explanation=explanation,
            recommendation_strength=recommendation_strength
        )
    
    def _calculate_signal_strength_score(self, differential: float) -> float:
        """
        Calculate score based on signal strength (differential)
        
        RECALIBRATED SIGNAL STRENGTH SCORING (Based on actual data analysis):
        - 25%+ differential: 90-100 points (elite edge)
        - 18-24% differential: 80-89 points (very strong) 
        - 12-17% differential: 65-79 points (strong)
        - 8-11% differential: 50-64 points (moderate) 
        - 5-7% differential: 25-49 points (weak but tradeable)
        - 3-4% differential: 10-24 points (very weak)
        - <3% differential: 0-9 points (negligible - should not bet)
        """
        abs_diff = abs(differential)
        
        if abs_diff >= 25:
            return min(100, 90 + (abs_diff - 25) * 0.8)  # 90-100 points (LOWERED from 30%)
        elif abs_diff >= 18:  # LOWERED from 22%
            return 80 + (abs_diff - 18) * 1.5   # 80-89 points
        elif abs_diff >= 12:  # LOWERED from 15%
            return 65 + (abs_diff - 12) * 2.5   # 65-79 points  
        elif abs_diff >= 8:   # LOWERED from 10%
            return 50 + (abs_diff - 8) * 3.75   # 50-64 points
        elif abs_diff >= 5:   # Same
            return 25 + (abs_diff - 5) * 8.33   # 25-49 points
        elif abs_diff >= 3:   # LOWERED from 2%
            return 10 + (abs_diff - 3) * 7      # 10-24 points
        else:
            return max(0, abs_diff * 3.33)      # 0-9 points for <3%
    
    def _calculate_source_reliability_score(
        self, source: str, book: str, split_type: str
    ) -> Tuple[float, Optional[float], Optional[float]]:
        """
        Calculate score based on historical source/book performance
        
        Source Reliability Scoring:
        - 70%+ win rate: 100 points
        - 65-69% win rate: 85-99 points
        - 60-64% win rate: 70-84 points
        - 55-59% win rate: 55-69 points
        - 52.4-54% win rate: 40-54 points (break-even)
        - <52.4% win rate: 0-39 points
        """
        
        # Try different source/book combinations
        source_book_patterns = [
            f"{source}-{book}",
            f"{source}-{book}-{split_type}",
            source,
            f"{source}-{split_type}"
        ]
        
        for pattern in source_book_patterns:
            query = """
            SELECT 
                AVG(win_rate) as avg_win_rate,
                SUM(total_bets) as total_bets,
                AVG(roi_per_100) as avg_database_roi
            FROM backtesting.strategy_performance
            WHERE source_book_type LIKE ? 
              AND (split_type = ? OR split_type = 'opposing_markets')
              AND total_bets >= 5
            """
            
            results = self.coordinator.execute_read(query, (f"%{pattern}%", split_type))
            if results and results[0]['avg_win_rate'] is not None:
                win_rate = float(results[0]['avg_win_rate'])
                total_bets = results[0]['total_bets']
                avg_database_roi = results[0]['avg_database_roi']
                
                # Use the more conservative database ROI directly instead of recalculating
                roi = float(avg_database_roi) if avg_database_roi is not None else 0
                
                # Convert to percentage if needed
                if win_rate <= 1.0:
                    win_rate *= 100
                
                # Calculate score based on win rate
                if win_rate >= 70:
                    score = 100
                elif win_rate >= 65:
                    score = 85 + (win_rate - 65) * 3
                elif win_rate >= 60:
                    score = 70 + (win_rate - 60) * 3
                elif win_rate >= 55:
                    score = 55 + (win_rate - 55) * 3
                elif win_rate >= 52.4:
                    score = 40 + (win_rate - 52.4) * 5.77
                else:
                    score = max(0, win_rate * 0.75)
                
                # Boost score for high sample size
                if total_bets >= 50:
                    score = min(100, score * 1.1)
                elif total_bets >= 20:
                    score = min(100, score * 1.05)
                
                return score, win_rate, roi
        
        # Default score for unknown sources
        return 50.0, None, None
    
    def _calculate_strategy_performance_score(
        self, strategy_name: str, source: str, book: str, split_type: str
    ) -> Tuple[float, Optional[float], Optional[float]]:
        """
        Calculate score based on strategy-specific historical performance
        """
        
        strategy_patterns = [
            strategy_name,
            f"{strategy_name}_{split_type}",
            f"{source}_{strategy_name}",
            strategy_name.replace('_', '%')  # Fuzzy matching
        ]
        
        for pattern in strategy_patterns:
            query = """
            SELECT 
                win_rate * 100 as win_rate_pct,
                total_bets,
                roi_per_100 as database_roi
            FROM backtesting.strategy_performance
            WHERE strategy_name LIKE %s
              AND (split_type = %s OR split_type = 'opposing_markets')
              AND total_bets >= 5
            ORDER BY total_bets DESC
            LIMIT 1
            """
            
            results = self.coordinator.execute_read(query, (f"%{pattern}%", split_type))
            if results:
                win_rate = float(results[0]['win_rate_pct'])
                total_bets = results[0]['total_bets']
                database_roi = results[0]['database_roi']
                
                # Use the more conservative database ROI directly instead of recalculating
                roi = float(database_roi) if database_roi is not None else 0
                
                # Convert decimal values to float for arithmetic
                roi = float(roi)
                win_rate = float(win_rate)
                total_bets = int(total_bets)
                
                # Score based on ROI and win rate
                if roi >= 50:
                    score = 100
                elif roi >= 30:
                    score = 85 + (roi - 30) * 0.75
                elif roi >= 15:
                    score = 70 + (roi - 15) * 1.0
                elif roi >= 5:
                    score = 55 + (roi - 5) * 1.5
                elif roi >= 0:
                    score = 40 + roi * 3
                else:
                    score = max(0, 40 + roi * 2)  # Negative ROI penalty
                
                # Adjust for sample size
                if total_bets >= 30:
                    score = min(100, score * 1.1)
                elif total_bets >= 15:
                    score = min(100, score * 1.05)
                elif total_bets < 10:
                    score *= 0.9  # Reduce confidence for small samples
                
                return score, win_rate, roi
        
        # Default for unknown strategies
        return 50.0, None, None
    
    def _calculate_data_quality_score(
        self, last_updated: datetime, cross_validation_sources: int
    ) -> Tuple[float, float]:
        """
        Calculate score based on data freshness and cross-validation
        """
        now = datetime.now(self.est)
        if last_updated.tzinfo is None:
            last_updated = self.est.localize(last_updated)
        else:
            last_updated = last_updated.astimezone(self.est)
        
        age_minutes = (now - last_updated).total_seconds() / 60
        
        # Freshness score (data age)
        if age_minutes <= 15:
            freshness_score = 100
        elif age_minutes <= 30:
            freshness_score = 85
        elif age_minutes <= 60:
            freshness_score = 70
        elif age_minutes <= 120:
            freshness_score = 55
        elif age_minutes <= 240:
            freshness_score = 40
        else:
            freshness_score = max(20, 100 - age_minutes * 0.2)
        
        # Cross-validation bonus
        cv_bonus = min(20, (cross_validation_sources - 1) * 10)
        
        total_score = min(100, freshness_score + cv_bonus)
        
        return total_score, age_minutes
    
    def _calculate_market_context_score(
        self, game_datetime: datetime, last_updated: datetime
    ) -> Tuple[float, float]:
        """
        Calculate score based on timing and market context
        """
        now = datetime.now(self.est)
        if game_datetime.tzinfo is None:
            game_datetime = self.est.localize(game_datetime)
        else:
            game_datetime = game_datetime.astimezone(self.est)
        
        minutes_to_game = (game_datetime - now).total_seconds() / 60
        
        # Optimal timing: 2-6 hours before game
        if 120 <= minutes_to_game <= 360:  # 2-6 hours
            score = 100
        elif 60 <= minutes_to_game < 120:   # 1-2 hours
            score = 85
        elif 30 <= minutes_to_game < 60:    # 30min-1hr
            score = 70
        elif 15 <= minutes_to_game < 30:    # 15-30min
            score = 55
        elif 0 <= minutes_to_game < 15:     # <15min
            score = 40
        elif minutes_to_game > 360:         # >6 hours
            score = max(60, 100 - (minutes_to_game - 360) * 0.1)
        else:
            score = 20  # Game already started
        
        return score, minutes_to_game
    
    def _get_confidence_level(self, score: float) -> str:
        """Convert numeric score to confidence level"""
        if score >= 90:
            return "VERY HIGH"
        elif score >= 75:
            return "HIGH"
        elif score >= 60:
            return "MODERATE"
        elif score >= 45:
            return "LOW"
        else:
            return "VERY LOW"
    
    def _get_recommendation_strength(self, score: float) -> str:
        """Convert numeric score to recommendation strength"""
        if score >= 90:
            return "STRONG BUY"
        elif score >= 75:
            return "BUY"
        elif score >= 60:
            return "LEAN BUY"
        elif score >= 45:
            return "WEAK BUY"
        else:
            return "AVOID"
    
    def _generate_explanation(self, score: float, components: ConfidenceComponents) -> str:
        """Generate human-readable explanation of the confidence score"""
        explanations = []
        
        # Signal strength
        if components.signal_strength_score >= 85:
            explanations.append(f"Very strong signal ({components.signal_differential:+.1f}% differential)")
        elif components.signal_strength_score >= 70:
            explanations.append(f"Strong signal ({components.signal_differential:+.1f}% differential)")
        elif components.signal_strength_score >= 55:
            explanations.append(f"Moderate signal ({components.signal_differential:+.1f}% differential)")
        else:
            explanations.append(f"Weak signal ({components.signal_differential:+.1f}% differential)")
        
        # Source reliability
        if components.source_win_rate:
            if components.source_reliability_score >= 85:
                explanations.append(f"Highly reliable source ({components.source_win_rate:.1f}% win rate)")
            elif components.source_reliability_score >= 70:
                explanations.append(f"Reliable source ({components.source_win_rate:.1f}% win rate)")
            else:
                explanations.append(f"Average source reliability ({components.source_win_rate:.1f}% win rate)")
        
        # Strategy performance
        if components.strategy_roi:
            if components.strategy_performance_score >= 85:
                explanations.append(f"Excellent strategy performance ({components.strategy_roi:+.1f}% ROI)")
            elif components.strategy_performance_score >= 70:
                explanations.append(f"Good strategy performance ({components.strategy_roi:+.1f}% ROI)")
            else:
                explanations.append(f"Average strategy performance ({components.strategy_roi:+.1f}% ROI)")
        
        # Data quality
        if components.data_age_minutes <= 30:
            explanations.append("Fresh data")
        elif components.data_age_minutes <= 120:
            explanations.append("Recent data")
        else:
            explanations.append(f"Older data ({components.data_age_minutes:.0f} min old)")
        
        return " • ".join(explanations)

    def _get_source_reliability_score(self, source_book_type: str, split_type: str) -> Tuple[float, str]:
        """Calculate source reliability score based on historical win rate."""
        try:
            # Get historical performance for this source/book combination
            query = """
                SELECT AVG(win_rate) as avg_win_rate, COUNT(*) as strategy_count,
                       AVG(total_bets) as avg_sample_size
                FROM backtesting.strategy_performance 
                WHERE source_book_type = %s 
                  AND split_type = %s
                  AND total_bets >= 10
            """
            
            result = self.coordinator.execute_read(query, (source_book_type, split_type))
            
            if result and result[0]['avg_win_rate'] is not None:
                avg_win_rate = float(result[0]['avg_win_rate'])
                strategy_count = int(result[0]['strategy_count'])
                avg_sample_size = float(result[0]['avg_sample_size'])
                
                # Convert win rate to percentage if it's in decimal format
                if avg_win_rate <= 1.0:
                    avg_win_rate *= 100
                
                # Calculate score based on win rate
                if avg_win_rate >= 70:
                    score = 100
                elif avg_win_rate >= 65:
                    score = 85 + (avg_win_rate - 65) * 3  # 85-99
                elif avg_win_rate >= 60:
                    score = 70 + (avg_win_rate - 60) * 3  # 70-84
                elif avg_win_rate >= 55:
                    score = 55 + (avg_win_rate - 55) * 3  # 55-69
                elif avg_win_rate >= 52.4:  # Break-even at -110 odds
                    score = 40 + (avg_win_rate - 52.4) * 5.77  # 40-54
                else:
                    score = max(0, 40 * (avg_win_rate / 52.4))  # 0-39
                
                # Adjust for sample size confidence
                if avg_sample_size < 20:
                    score *= 0.8  # Reduce confidence for small samples
                elif strategy_count < 3:
                    score *= 0.9  # Reduce confidence for few strategies
                
                explanation = f"{avg_win_rate:.1f}% avg win rate across {strategy_count} strategies"
                return (min(100, max(0, score)), explanation)
            
            # No historical data
            return (25.0, "No historical performance data")
            
        except Exception as e:
            self.logger.error("Failed to calculate source reliability score", error=str(e))
            return (25.0, "Error calculating reliability")
    
    def _get_strategy_performance_score(self, strategy_name: str, source_book_type: str, split_type: str) -> Tuple[float, str]:
        """Calculate strategy performance score based on historical ROI."""
        try:
            # Get historical performance for this specific strategy
            query = """
                SELECT total_bets, ROUND(win_rate * total_bets) as wins, AVG(total_bets) as avg_bets
                FROM backtesting.strategy_performance 
                WHERE strategy_name LIKE %s 
                  AND source_book_type = %s
                  AND split_type = %s
                  AND total_bets >= 5
            """
            
            # Use LIKE to match strategy variants (e.g., "sharp_action_detector_STRONG_SHARP_AWAY_UNDER")
            strategy_pattern = f"{strategy_name.split('_')[0]}%"
            result = self.coordinator.execute_read(query, (strategy_pattern, source_book_type, split_type))
            
            if result and result[0]['total_bets'] is not None:
                # Calculate ROI correctly from wins and total bets
                total_strategies = len(result)
                total_bets_sum = sum(int(row['total_bets']) for row in result)
                total_wins_sum = sum(int(row['wins']) for row in result)
                
                if total_bets_sum > 0:
                    # Correct ROI calculation: ((wins * 100) - (losses * 110)) / (total_bets * 110) * 100
                    total_losses = total_bets_sum - total_wins_sum
                    win_profit = total_wins_sum * 100
                    loss_cost = total_losses * 110
                    net_profit = win_profit - loss_cost
                    total_risked = total_bets_sum * 110
                    roi_per_100 = (net_profit / total_risked) * 100
                    
                    # Convert to float for arithmetic operations
                    roi_per_100 = float(roi_per_100)
                    
                    # Calculate score based on ROI
                    if roi_per_100 >= 50:
                        score = 100
                    elif roi_per_100 >= 30:
                        score = 85 + (roi_per_100 - 30) * 0.75  # 85-99
                    elif roi_per_100 >= 15:
                        score = 70 + (roi_per_100 - 15) * 1.0   # 70-84
                    elif roi_per_100 >= 5:
                        score = 55 + (roi_per_100 - 5) * 1.5    # 55-69
                    elif roi_per_100 >= 0:
                        score = 40 + (roi_per_100 * 3)          # 40-54
                    else:
                        # Penalty for negative ROI
                        score = max(0, 40 + (roi_per_100 * 0.5))  # Gradual penalty
                    
                    # Adjust for sample size
                    if total_bets_sum < 20:
                        score *= 0.9
                    
                    explanation = f"{roi_per_100:+.1f}% ROI across {total_strategies} variants ({total_bets_sum} total bets)"
                    return (min(100, max(0, score)), explanation)
            
            # No historical data
            return (40.0, "No historical performance data")
            
        except Exception as e:
            self.logger.error("Failed to calculate strategy performance score", error=str(e))
            return (40.0, "Error calculating performance")


def get_confidence_scorer() -> ConfidenceScorer:
    """Get singleton instance of ConfidenceScorer"""
    return ConfidenceScorer() 