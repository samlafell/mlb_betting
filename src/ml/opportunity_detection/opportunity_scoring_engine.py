"""
AI-Powered Opportunity Scoring Engine

Advanced multi-factor scoring algorithm that combines:
- Historical strategy performance
- ML prediction confidence
- Market conditions analysis
- Real-time data quality assessment
- Risk-adjusted opportunity ranking

This engine provides intelligent opportunity discovery by analyzing multiple
data sources and applying sophisticated scoring algorithms to identify
the highest-value betting opportunities.

Part of Issue #59: AI-Powered Betting Opportunity Discovery
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from decimal import Decimal
from dataclasses import dataclass, field
from enum import Enum

import numpy as np
from pydantic import BaseModel, Field

from src.analysis.models.unified_models import UnifiedBettingSignal, ConfidenceLevel, SignalType
from src.ml.services.prediction_service import PredictionService
from src.core.config import get_unified_config
from src.core.logging import get_logger


class OpportunityTier(str, Enum):
    """Opportunity tier classification"""
    PREMIUM = "premium"      # Top 5% opportunities
    HIGH_VALUE = "high_value"  # Top 15% opportunities  
    STANDARD = "standard"    # Top 40% opportunities
    LOW_GRADE = "low_grade"  # Remaining opportunities


class RiskProfile(str, Enum):
    """Risk profile for opportunity assessment"""
    CONSERVATIVE = "conservative"
    MODERATE = "moderate"
    AGGRESSIVE = "aggressive"


@dataclass
class ScoringFactors:
    """Multi-factor scoring components"""
    strategy_performance: float = 0.0  # Historical strategy success rate
    ml_confidence: float = 0.0         # ML prediction confidence
    market_efficiency: float = 0.0     # Market condition assessment
    data_quality: float = 0.0          # Data completeness/freshness
    consensus_strength: float = 0.0    # Cross-strategy consensus
    timing_factor: float = 0.0         # Time-to-game multiplier
    value_potential: float = 0.0       # Expected value assessment
    risk_adjustment: float = 0.0       # Risk-adjusted scoring


@dataclass
class OpportunityScore:
    """Comprehensive opportunity scoring result"""
    opportunity_id: str
    game_id: str
    signal_type: SignalType
    composite_score: float              # Final weighted score (0-100)
    tier: OpportunityTier
    confidence_level: ConfidenceLevel
    expected_value: float
    kelly_fraction: float
    risk_profile: RiskProfile
    
    # Detailed factor breakdown
    scoring_factors: ScoringFactors
    factor_weights: Dict[str, float]
    
    # Supporting data
    contributing_signals: List[str] = field(default_factory=list)
    ml_predictions: Dict[str, Any] = field(default_factory=dict)
    market_data: Dict[str, Any] = field(default_factory=dict)
    explanation_summary: str = ""
    
    # Metadata
    scored_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: datetime = field(default_factory=lambda: datetime.utcnow() + timedelta(hours=2))


class OpportunityScoringEngine:
    """
    AI-Powered Opportunity Scoring Engine
    
    Provides intelligent opportunity discovery through multi-factor analysis:
    
    1. Strategy Performance Analysis - Historical success rates and trends
    2. ML Prediction Integration - Leverage trained models for confidence scoring  
    3. Market Condition Assessment - Real-time market efficiency analysis
    4. Data Quality Scoring - Assess completeness and freshness
    5. Consensus Analysis - Cross-strategy signal alignment
    6. Risk-Adjusted Ranking - Kelly Criterion and risk profiling
    """
    
    def __init__(self, 
                 prediction_service: PredictionService,
                 config: Optional[Dict[str, Any]] = None):
        """
        Initialize the opportunity scoring engine
        
        Args:
            prediction_service: ML prediction service for model integration
            config: Engine configuration parameters
        """
        self.prediction_service = prediction_service
        self.config = config or {}
        self.logger = get_logger(__name__)
        
        # Scoring weights (configurable)
        self.factor_weights = self.config.get('scoring_weights', {
            'strategy_performance': 0.25,  # Historical strategy success
            'ml_confidence': 0.20,         # ML prediction confidence
            'market_efficiency': 0.15,     # Market condition analysis
            'data_quality': 0.10,          # Data completeness/freshness  
            'consensus_strength': 0.15,    # Cross-strategy consensus
            'timing_factor': 0.08,         # Time-based multipliers
            'value_potential': 0.07        # Expected value assessment
        })
        
        # Tier thresholds
        self.tier_thresholds = self.config.get('tier_thresholds', {
            'premium': 85.0,     # Top 5% - exceptional opportunities
            'high_value': 70.0,  # Top 15% - strong opportunities
            'standard': 50.0,    # Top 40% - solid opportunities
            'minimum': 35.0      # Minimum viable opportunity
        })
        
        # Risk profiles
        self.risk_profiles = {
            'conservative': {'max_kelly': 0.05, 'min_confidence': 0.75, 'min_ev': 0.03},
            'moderate': {'max_kelly': 0.10, 'min_confidence': 0.60, 'min_ev': 0.02},
            'aggressive': {'max_kelly': 0.20, 'min_confidence': 0.50, 'min_ev': 0.01}
        }
        
        # Cache for performance data
        self._performance_cache = {}
        self._cache_expiry = {}
        
        self.logger.info("OpportunityScoringEngine initialized with weights: %s", self.factor_weights)
    
    async def score_opportunity(self, 
                               signals: List[UnifiedBettingSignal],
                               game_id: str,
                               user_risk_profile: RiskProfile = RiskProfile.MODERATE) -> Optional[OpportunityScore]:
        """
        Score a betting opportunity based on multiple signals
        
        Args:
            signals: List of unified betting signals for the game
            game_id: Target game identifier
            user_risk_profile: User's risk tolerance
            
        Returns:
            Comprehensive opportunity score or None if insufficient data
        """
        try:
            if not signals:
                self.logger.warning(f"No signals provided for game {game_id}")
                return None
            
            self.logger.info(f"Scoring opportunity for game {game_id} with {len(signals)} signals")
            
            # Initialize scoring factors
            factors = ScoringFactors()
            
            # 1. Strategy Performance Analysis
            factors.strategy_performance = await self._calculate_strategy_performance(signals)
            
            # 2. ML Prediction Integration
            factors.ml_confidence = await self._calculate_ml_confidence(game_id, signals)
            
            # 3. Market Condition Assessment
            factors.market_efficiency = await self._calculate_market_efficiency(signals)
            
            # 4. Data Quality Assessment
            factors.data_quality = await self._calculate_data_quality(signals)
            
            # 5. Consensus Analysis
            factors.consensus_strength = await self._calculate_consensus_strength(signals)
            
            # 6. Timing Factor
            factors.timing_factor = await self._calculate_timing_factor(signals)
            
            # 7. Value Potential
            factors.value_potential = await self._calculate_value_potential(signals)
            
            # Calculate composite score
            composite_score = self._calculate_composite_score(factors)
            
            # Risk adjustment
            factors.risk_adjustment = self._apply_risk_adjustment(
                composite_score, user_risk_profile, signals
            )
            final_score = composite_score * factors.risk_adjustment
            
            # Determine tier and confidence
            tier = self._determine_tier(final_score)
            confidence_level = self._determine_confidence_level(final_score, factors)
            
            # Calculate financial metrics
            expected_value = await self._calculate_expected_value(signals, factors)
            kelly_fraction = self._calculate_kelly_fraction(expected_value, signals, user_risk_profile)
            
            # Get ML predictions for context
            ml_predictions = await self._get_ml_predictions(game_id)
            
            # Create opportunity score
            opportunity_score = OpportunityScore(
                opportunity_id=f"opp_{game_id}_{int(datetime.utcnow().timestamp())}",
                game_id=game_id,
                signal_type=self._get_primary_signal_type(signals),
                composite_score=final_score,
                tier=tier,
                confidence_level=confidence_level,
                expected_value=expected_value,
                kelly_fraction=kelly_fraction,
                risk_profile=user_risk_profile,
                scoring_factors=factors,
                factor_weights=self.factor_weights,
                contributing_signals=[s.signal_id for s in signals],
                ml_predictions=ml_predictions or {},
                explanation_summary=self._generate_explanation_summary(factors, signals, final_score)
            )
            
            self.logger.info(
                f"Scored opportunity {opportunity_score.opportunity_id}: "
                f"score={final_score:.1f}, tier={tier}, EV={expected_value:.3f}"
            )
            
            return opportunity_score
            
        except Exception as e:
            self.logger.error(f"Error scoring opportunity for game {game_id}: {e}", exc_info=True)
            return None
    
    async def score_all_opportunities(self, 
                                    signals_by_game: Dict[str, List[UnifiedBettingSignal]],
                                    user_risk_profile: RiskProfile = RiskProfile.MODERATE,
                                    min_score: float = 35.0) -> List[OpportunityScore]:
        """
        Score all opportunities across multiple games
        
        Args:
            signals_by_game: Dictionary mapping game IDs to their signals
            user_risk_profile: User's risk tolerance
            min_score: Minimum score threshold for inclusion
            
        Returns:
            List of opportunity scores sorted by composite score (descending)
        """
        try:
            self.logger.info(f"Scoring opportunities for {len(signals_by_game)} games")
            
            # Score opportunities in parallel
            tasks = []
            for game_id, signals in signals_by_game.items():
                task = self.score_opportunity(signals, game_id, user_risk_profile)
                tasks.append(task)
            
            # Execute with reasonable concurrency limit
            results = []
            batch_size = 5
            for i in range(0, len(tasks), batch_size):
                batch_tasks = tasks[i:i + batch_size]
                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                
                for result in batch_results:
                    if isinstance(result, Exception):
                        self.logger.error(f"Batch scoring error: {result}")
                    elif result is not None and result.composite_score >= min_score:
                        results.append(result)
            
            # Sort by composite score (descending)
            results.sort(key=lambda x: x.composite_score, reverse=True)
            
            self.logger.info(
                f"Scored {len(results)} qualifying opportunities "
                f"(min_score={min_score}, profile={user_risk_profile})"
            )
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error in batch opportunity scoring: {e}", exc_info=True)
            return []
    
    async def get_top_opportunities(self,
                                  signals_by_game: Dict[str, List[UnifiedBettingSignal]],
                                  limit: int = 10,
                                  tier_filter: Optional[OpportunityTier] = None,
                                  user_risk_profile: RiskProfile = RiskProfile.MODERATE) -> List[OpportunityScore]:
        """
        Get top-ranked opportunities with optional filtering
        
        Args:
            signals_by_game: Dictionary mapping game IDs to their signals
            limit: Maximum number of opportunities to return
            tier_filter: Optional tier filter (PREMIUM, HIGH_VALUE, etc.)
            user_risk_profile: User's risk tolerance
            
        Returns:
            Top opportunities meeting the criteria
        """
        try:
            # Score all opportunities
            opportunities = await self.score_all_opportunities(
                signals_by_game, user_risk_profile, min_score=0.0
            )
            
            # Apply tier filter if specified
            if tier_filter:
                opportunities = [opp for opp in opportunities if opp.tier == tier_filter]
            
            # Return top N opportunities
            result = opportunities[:limit]
            
            self.logger.info(
                f"Retrieved {len(result)} top opportunities "
                f"(tier_filter={tier_filter}, limit={limit})"
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error getting top opportunities: {e}", exc_info=True)
            return []
    
    # Private scoring methods
    
    async def _calculate_strategy_performance(self, signals: List[UnifiedBettingSignal]) -> float:
        """Calculate weighted strategy performance score (0-100)"""
        try:
            if not signals:
                return 0.0
            
            total_weight = 0.0
            weighted_performance = 0.0
            
            for signal in signals:
                # Get cached performance or calculate
                performance = await self._get_strategy_performance(signal.signal_type.value)
                weight = signal.confidence_score * signal.signal_strength
                
                weighted_performance += performance * weight
                total_weight += weight
            
            if total_weight == 0:
                return 50.0  # Neutral score
            
            return min(100.0, weighted_performance / total_weight)
            
        except Exception as e:
            self.logger.error(f"Error calculating strategy performance: {e}")
            return 50.0
    
    async def _calculate_ml_confidence(self, game_id: str, signals: List[UnifiedBettingSignal]) -> float:
        """Calculate ML prediction confidence score (0-100)"""
        try:
            # Get ML predictions for the game
            ml_prediction = await self.prediction_service.get_prediction(game_id, include_explanation=False)
            
            if not ml_prediction:
                return 25.0  # Low confidence without ML data
            
            # Extract confidence scores from ML predictions
            confidences = []
            
            # Check different prediction types
            for field in ['home_ml_confidence', 'total_over_confidence', 'total_runs_confidence']:
                if field in ml_prediction and ml_prediction[field] is not None:
                    confidences.append(ml_prediction[field])
            
            if not confidences:
                return 25.0
            
            # Calculate weighted average confidence
            avg_confidence = sum(confidences) / len(confidences)
            
            # Convert to 0-100 scale
            return min(100.0, avg_confidence * 100)
            
        except Exception as e:
            self.logger.error(f"Error calculating ML confidence: {e}")
            return 25.0
    
    async def _calculate_market_efficiency(self, signals: List[UnifiedBettingSignal]) -> float:
        """Assess market efficiency based on signal dispersion (0-100)"""
        try:
            if not signals:
                return 50.0
            
            # Calculate signal strength variance (higher variance = less efficient market)
            strengths = [signal.signal_strength for signal in signals]
            
            if len(strengths) < 2:
                return 60.0  # Slightly above neutral for single signal
            
            variance = np.var(strengths)
            mean_strength = np.mean(strengths)
            
            # High variance with high mean strength indicates inefficient market (good for us)
            efficiency_score = min(100.0, (variance / (mean_strength + 0.01)) * 200)
            
            return efficiency_score
            
        except Exception as e:
            self.logger.error(f"Error calculating market efficiency: {e}")
            return 50.0
    
    async def _calculate_data_quality(self, signals: List[UnifiedBettingSignal]) -> float:
        """Assess data quality based on signal metadata (0-100)"""
        try:
            if not signals:
                return 0.0
            
            quality_scores = []
            
            for signal in signals:
                # Base quality from signal quality score
                quality = signal.quality_score * 100
                
                # Adjust for validation status
                if signal.validation_passed:
                    quality *= 1.1
                else:
                    quality *= 0.8
                
                # Adjust for data freshness (signals should be recent)
                now = datetime.utcnow()
                if hasattr(signal, 'created_at') and signal.created_at:
                    age_hours = (now - signal.created_at).total_seconds() / 3600
                    freshness_factor = max(0.5, 1.0 - (age_hours / 24))  # Decay over 24 hours
                    quality *= freshness_factor
                
                quality_scores.append(min(100.0, quality))
            
            return sum(quality_scores) / len(quality_scores)
            
        except Exception as e:
            self.logger.error(f"Error calculating data quality: {e}")
            return 50.0
    
    async def _calculate_consensus_strength(self, signals: List[UnifiedBettingSignal]) -> float:
        """Calculate cross-strategy consensus strength (0-100)"""
        try:
            if len(signals) < 2:
                return 30.0  # Low consensus with single signal
            
            # Group signals by recommended side
            side_groups = {}
            for signal in signals:
                side = signal.recommended_side
                if side not in side_groups:
                    side_groups[side] = []
                side_groups[side].append(signal)
            
            # Calculate consensus strength
            if len(side_groups) == 1:
                # All signals agree
                consensus_strength = 90.0
                
                # Bonus for multiple high-confidence signals
                high_conf_signals = sum(1 for s in signals if s.confidence_score > 0.7)
                consensus_strength += min(10.0, high_conf_signals * 2)
                
            else:
                # Mixed signals - calculate dominant side strength
                max_group_size = max(len(group) for group in side_groups.values())
                max_group_weight = max(
                    sum(s.confidence_score * s.signal_strength for s in group)
                    for group in side_groups.values()
                )
                
                total_weight = sum(
                    s.confidence_score * s.signal_strength for s in signals
                )
                
                if total_weight > 0:
                    consensus_strength = (max_group_weight / total_weight) * 80
                else:
                    consensus_strength = 40.0
            
            return min(100.0, consensus_strength)
            
        except Exception as e:
            self.logger.error(f"Error calculating consensus strength: {e}")
            return 50.0
    
    async def _calculate_timing_factor(self, signals: List[UnifiedBettingSignal]) -> float:
        """Calculate timing-based multiplier (0-100)"""
        try:
            if not signals:
                return 50.0
            
            # Get average minutes to game
            avg_minutes = sum(signal.minutes_to_game for signal in signals) / len(signals)
            
            # Apply timing multipliers based on time to game
            if avg_minutes <= 60:  # Ultra late
                timing_score = 95.0
            elif avg_minutes <= 120:  # Closing hour
                timing_score = 85.0
            elif avg_minutes <= 240:  # Closing 2 hours
                timing_score = 75.0
            elif avg_minutes <= 480:  # Late afternoon
                timing_score = 65.0
            elif avg_minutes <= 1440:  # Same day
                timing_score = 55.0
            elif avg_minutes <= 2880:  # Early 24h
                timing_score = 45.0
            else:  # Very early
                timing_score = 35.0
            
            return timing_score
            
        except Exception as e:
            self.logger.error(f"Error calculating timing factor: {e}")
            return 50.0
    
    async def _calculate_value_potential(self, signals: List[UnifiedBettingSignal]) -> float:
        """Calculate expected value potential (0-100)"""
        try:
            if not signals:
                return 0.0
            
            # Calculate value based on signal expected values
            values = []
            for signal in signals:
                if signal.expected_value is not None:
                    values.append(signal.expected_value)
                else:
                    # Estimate value from confidence and odds
                    if signal.odds and signal.confidence_score:
                        implied_prob = 1 / signal.odds if signal.odds > 0 else 0.5
                        estimated_value = (signal.confidence_score - implied_prob) * signal.signal_strength
                        values.append(estimated_value)
            
            if not values:
                return 40.0  # Neutral when no value data
            
            # Convert to 0-100 scale (positive values are good)
            avg_value = sum(values) / len(values)
            value_score = min(100.0, max(0.0, (avg_value + 0.1) * 250))  # Scale to 0-100
            
            return value_score
            
        except Exception as e:
            self.logger.error(f"Error calculating value potential: {e}")
            return 40.0
    
    def _calculate_composite_score(self, factors: ScoringFactors) -> float:
        """Calculate weighted composite score from all factors"""
        try:
            weighted_sum = (
                factors.strategy_performance * self.factor_weights['strategy_performance'] +
                factors.ml_confidence * self.factor_weights['ml_confidence'] +
                factors.market_efficiency * self.factor_weights['market_efficiency'] +
                factors.data_quality * self.factor_weights['data_quality'] +
                factors.consensus_strength * self.factor_weights['consensus_strength'] +
                factors.timing_factor * self.factor_weights['timing_factor'] +
                factors.value_potential * self.factor_weights['value_potential']
            )
            
            return min(100.0, max(0.0, weighted_sum))
            
        except Exception as e:
            self.logger.error(f"Error calculating composite score: {e}")
            return 0.0
    
    def _apply_risk_adjustment(self, 
                              composite_score: float,
                              risk_profile: RiskProfile,
                              signals: List[UnifiedBettingSignal]) -> float:
        """Apply risk-based adjustments to the composite score"""
        try:
            profile_config = self.risk_profiles[risk_profile.value]
            
            # Base risk adjustment
            risk_adjustment = 1.0
            
            # Adjust based on signal confidence
            avg_confidence = sum(s.confidence_score for s in signals) / len(signals)
            if avg_confidence < profile_config['min_confidence']:
                risk_adjustment *= 0.8  # Penalize low confidence for conservative profiles
            
            # Adjust based on consensus
            if len(set(s.recommended_side for s in signals)) > 1:
                # Mixed signals - apply risk adjustment
                if risk_profile == RiskProfile.CONSERVATIVE:
                    risk_adjustment *= 0.7
                elif risk_profile == RiskProfile.MODERATE:
                    risk_adjustment *= 0.85
            
            return risk_adjustment
            
        except Exception as e:
            self.logger.error(f"Error applying risk adjustment: {e}")
            return 1.0
    
    def _determine_tier(self, final_score: float) -> OpportunityTier:
        """Determine opportunity tier based on final score"""
        if final_score >= self.tier_thresholds['premium']:
            return OpportunityTier.PREMIUM
        elif final_score >= self.tier_thresholds['high_value']:
            return OpportunityTier.HIGH_VALUE
        elif final_score >= self.tier_thresholds['standard']:
            return OpportunityTier.STANDARD
        else:
            return OpportunityTier.LOW_GRADE
    
    def _determine_confidence_level(self, final_score: float, factors: ScoringFactors) -> ConfidenceLevel:
        """Determine confidence level based on score and factors"""
        # Consider both score and consensus strength
        combined_confidence = (final_score + factors.consensus_strength) / 2
        
        if combined_confidence >= 85:
            return ConfidenceLevel.VERY_HIGH
        elif combined_confidence >= 70:
            return ConfidenceLevel.HIGH
        elif combined_confidence >= 50:
            return ConfidenceLevel.MEDIUM
        elif combined_confidence >= 30:
            return ConfidenceLevel.LOW
        else:
            return ConfidenceLevel.VERY_LOW
    
    async def _calculate_expected_value(self, 
                                      signals: List[UnifiedBettingSignal],
                                      factors: ScoringFactors) -> float:
        """Calculate expected value for the opportunity"""
        try:
            # Use existing signal expected values if available
            signal_evs = [s.expected_value for s in signals if s.expected_value is not None]
            
            if signal_evs:
                return sum(signal_evs) / len(signal_evs)
            
            # Estimate EV from confidence and implied probability
            estimated_evs = []
            for signal in signals:
                if signal.odds and signal.confidence_score:
                    # Convert American odds to decimal
                    decimal_odds = signal.odds + 1 if signal.odds > 0 else (-100 / signal.odds) + 1
                    implied_prob = 1 / decimal_odds
                    
                    # Simple EV calculation
                    win_prob = signal.confidence_score
                    payout = decimal_odds - 1
                    
                    ev = (win_prob * payout) - ((1 - win_prob) * 1)
                    estimated_evs.append(ev)
            
            if estimated_evs:
                return sum(estimated_evs) / len(estimated_evs)
            
            # Fallback: estimate from composite factors
            normalized_score = factors.strategy_performance / 100
            return (normalized_score - 0.5) * 0.1  # Conservative EV estimate
            
        except Exception as e:
            self.logger.error(f"Error calculating expected value: {e}")
            return 0.0
    
    def _calculate_kelly_fraction(self, 
                                 expected_value: float,
                                 signals: List[UnifiedBettingSignal],
                                 risk_profile: RiskProfile) -> float:
        """Calculate Kelly Criterion fraction for optimal bet sizing"""
        try:
            if expected_value <= 0:
                return 0.0
            
            # Get average odds and win probability
            valid_odds = [s.odds for s in signals if s.odds and s.odds > 0]
            if not valid_odds:
                return 0.0
            
            avg_odds = sum(valid_odds) / len(valid_odds)
            avg_confidence = sum(s.confidence_score for s in signals) / len(signals)
            
            # Convert to decimal odds
            decimal_odds = avg_odds + 1 if avg_odds > 0 else (-100 / avg_odds) + 1
            
            # Kelly formula: (bp - q) / b
            b = decimal_odds - 1  # Net odds
            p = avg_confidence     # Win probability
            q = 1 - p             # Loss probability
            
            kelly_fraction = (b * p - q) / b
            
            # Apply risk profile limits
            profile_config = self.risk_profiles[risk_profile.value]
            max_kelly = profile_config['max_kelly']
            
            # Conservative Kelly (reduce by half for safety)
            conservative_kelly = kelly_fraction * 0.5
            
            return max(0.0, min(conservative_kelly, max_kelly))
            
        except Exception as e:
            self.logger.error(f"Error calculating Kelly fraction: {e}")
            return 0.0
    
    async def _get_ml_predictions(self, game_id: str) -> Optional[Dict[str, Any]]:
        """Get ML predictions for context"""
        try:
            return await self.prediction_service.get_cached_prediction(game_id)
        except Exception as e:
            self.logger.error(f"Error getting ML predictions for {game_id}: {e}")
            return None
    
    def _get_primary_signal_type(self, signals: List[UnifiedBettingSignal]) -> SignalType:
        """Get the primary signal type from the list"""
        if not signals:
            return SignalType.CONSENSUS
        
        # Count signal types
        type_counts = {}
        for signal in signals:
            signal_type = signal.signal_type
            type_counts[signal_type] = type_counts.get(signal_type, 0) + signal.signal_strength
        
        # Return the type with highest weighted count
        return max(type_counts.items(), key=lambda x: x[1])[0]
    
    def _generate_explanation_summary(self, 
                                    factors: ScoringFactors,
                                    signals: List[UnifiedBettingSignal],
                                    final_score: float) -> str:
        """Generate human-readable explanation summary"""
        try:
            # Identify top contributing factors
            factor_scores = [
                ('Strategy Performance', factors.strategy_performance),
                ('ML Confidence', factors.ml_confidence),
                ('Market Efficiency', factors.market_efficiency),
                ('Data Quality', factors.data_quality),
                ('Consensus Strength', factors.consensus_strength),
                ('Timing Factor', factors.timing_factor),
                ('Value Potential', factors.value_potential),
            ]
            
            # Sort by score
            factor_scores.sort(key=lambda x: x[1], reverse=True)
            top_factors = factor_scores[:3]
            
            # Build explanation
            explanation_parts = []
            
            # Overall assessment
            if final_score >= 85:
                explanation_parts.append("Exceptional opportunity with strong indicators across multiple factors.")
            elif final_score >= 70:
                explanation_parts.append("High-value opportunity with solid supporting factors.")
            elif final_score >= 50:
                explanation_parts.append("Standard opportunity with reasonable potential.")
            else:
                explanation_parts.append("Lower-grade opportunity - proceed with caution.")
            
            # Top factors
            factor_descriptions = []
            for factor_name, score in top_factors:
                if score >= 75:
                    factor_descriptions.append(f"Strong {factor_name.lower()}")
                elif score >= 50:
                    factor_descriptions.append(f"Moderate {factor_name.lower()}")
            
            if factor_descriptions:
                explanation_parts.append(f"Key strengths: {', '.join(factor_descriptions)}.")
            
            # Signal context
            signal_types = list(set(s.signal_type.value for s in signals))
            if len(signal_types) > 1:
                explanation_parts.append(f"Based on {len(signals)} signals including {', '.join(signal_types)}.")
            else:
                explanation_parts.append(f"Based on {len(signals)} {signal_types[0]} signal(s).")
            
            return " ".join(explanation_parts)
            
        except Exception as e:
            self.logger.error(f"Error generating explanation: {e}")
            return "Opportunity scored based on multiple factors - see detailed breakdown for specifics."
    
    async def _get_strategy_performance(self, strategy_name: str) -> float:
        """Get cached strategy performance or calculate from database"""
        try:
            # Check cache first
            cache_key = f"performance_{strategy_name}"
            if (cache_key in self._performance_cache and 
                cache_key in self._cache_expiry and
                datetime.utcnow() < self._cache_expiry[cache_key]):
                return self._performance_cache[cache_key]
            
            # TODO: Implement actual database lookup for strategy performance
            # For now, return estimated performance based on strategy type
            performance_estimates = {
                'sharp_action': 68.0,
                'book_conflict': 62.0,
                'line_movement': 58.0,
                'hybrid_sharp': 65.0,
                'consensus': 55.0,
                'timing_based': 60.0,
                'public_fade': 52.0,
                'underdog_value': 48.0,
                'late_flip': 70.0,
                'opposing_markets': 50.0,
            }
            
            performance = performance_estimates.get(strategy_name, 55.0)
            
            # Cache the result
            self._performance_cache[cache_key] = performance
            self._cache_expiry[cache_key] = datetime.utcnow() + timedelta(hours=4)
            
            return performance
            
        except Exception as e:
            self.logger.error(f"Error getting strategy performance for {strategy_name}: {e}")
            return 55.0  # Default neutral performance