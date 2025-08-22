"""
AI-Powered Opportunity Detection Service

Main service that orchestrates the complete opportunity detection system:
- Integrates scoring engine, pattern recognition, and explanation engine
- Provides real-time opportunity discovery
- Handles user personalization
- Manages opportunity caching and performance tracking
- Coordinates with ML prediction service

This is the primary interface for the AI-powered betting opportunity discovery system.

Part of Issue #59: AI-Powered Betting Opportunity Discovery
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import json

from src.ml.opportunity_detection.opportunity_scoring_engine import (
    OpportunityScoringEngine, OpportunityScore, OpportunityTier, RiskProfile, ScoringFactors
)
from src.ml.opportunity_detection.pattern_recognition import (
    MLPatternRecognition, DetectedPattern, PatternType, PatternConfidence
)
from src.ml.opportunity_detection.explanation_engine import (
    NaturalLanguageExplanationEngine, ExplanationStyle, ExplanationFormat, UserProfile
)
from src.ml.services.prediction_service import PredictionService
from src.analysis.models.unified_models import UnifiedBettingSignal, SignalType
from src.core.config import get_settings
from src.core.logging import get_logger


@dataclass
class OpportunityDiscoveryResult:
    """Complete opportunity discovery result with all components"""
    opportunity_id: str
    game_id: str
    
    # Core scoring
    opportunity_score: OpportunityScore
    detected_patterns: List[DetectedPattern] = field(default_factory=list)
    
    # Natural language explanation
    explanation: Dict[str, Any] = field(default_factory=dict)
    
    # Supporting data
    contributing_signals: List[UnifiedBettingSignal] = field(default_factory=list)
    ml_predictions: Dict[str, Any] = field(default_factory=dict)
    market_context: Dict[str, Any] = field(default_factory=dict)
    
    # Performance tracking
    discovery_time_ms: float = 0.0
    cache_hit: bool = False
    
    # Metadata
    discovered_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: datetime = field(default_factory=lambda: datetime.utcnow() + timedelta(hours=2))


class OpportunityDetectionService:
    """
    AI-Powered Opportunity Detection Service
    
    Orchestrates the complete opportunity discovery pipeline:
    
    1. Signal Collection - Gathers betting signals from various strategies
    2. ML Pattern Recognition - Detects complex patterns in market data
    3. Opportunity Scoring - Multi-factor scoring with ML integration
    4. Natural Language Explanation - Generates user-friendly explanations
    5. User Personalization - Adapts to user experience and preferences
    6. Real-time Updates - Provides fresh opportunities as conditions change
    """
    
    def __init__(self, 
                 prediction_service: PredictionService,
                 config: Optional[Dict[str, Any]] = None):
        """
        Initialize the opportunity detection service
        
        Args:
            prediction_service: ML prediction service for integration
            config: Service configuration parameters
        """
        self.prediction_service = prediction_service
        self.config = config or {}
        self.logger = get_logger(__name__)
        
        # Initialize component engines
        self.scoring_engine = OpportunityScoringEngine(
            prediction_service=prediction_service,
            config=self.config.get('scoring_engine', {})
        )
        
        self.pattern_recognition = MLPatternRecognition(
            config=self.config.get('pattern_recognition', {})
        )
        
        self.explanation_engine = NaturalLanguageExplanationEngine(
            config=self.config.get('explanation_engine', {})
        )
        
        # Service configuration
        self.cache_ttl_hours = self.config.get('cache_ttl_hours', 2)
        self.min_opportunity_score = self.config.get('min_opportunity_score', 35.0)
        self.max_opportunities_per_request = self.config.get('max_opportunities_per_request', 50)
        self.pattern_detection_enabled = self.config.get('pattern_detection_enabled', True)
        self.explanation_generation_enabled = self.config.get('explanation_generation_enabled', True)
        
        # Performance tracking
        self.performance_metrics = {
            'opportunities_discovered': 0,
            'opportunities_cached': 0,
            'patterns_detected': 0,
            'explanations_generated': 0,
            'average_discovery_time_ms': 0.0,
            'cache_hit_rate': 0.0
        }
        
        # Opportunity cache (in production, this would be Redis/database)
        self._opportunity_cache: Dict[str, OpportunityDiscoveryResult] = {}
        self._cache_timestamps: Dict[str, datetime] = {}
        
        self.logger.info("OpportunityDetectionService initialized with config keys: %s", 
                        list(self.config.keys()))
    
    async def discover_opportunities(self,
                                   signals_by_game: Dict[str, List[UnifiedBettingSignal]],
                                   user_profile: Optional[UserProfile] = None,
                                   market_data: Optional[Dict[str, Dict[str, Any]]] = None,
                                   force_refresh: bool = False) -> List[OpportunityDiscoveryResult]:
        """
        Discover betting opportunities across multiple games
        
        Args:
            signals_by_game: Dictionary mapping game IDs to their betting signals
            user_profile: User profile for personalization (optional)
            market_data: Additional market context data (optional)
            force_refresh: Force fresh analysis ignoring cache
            
        Returns:
            List of discovered opportunities ranked by score
        """
        try:
            start_time = datetime.utcnow()
            
            if not user_profile:
                user_profile = UserProfile()  # Use defaults
            
            self.logger.info(
                f"Discovering opportunities for {len(signals_by_game)} games "
                f"(user_profile: {user_profile.experience_level}, force_refresh: {force_refresh})"
            )
            
            opportunities = []
            cache_hits = 0
            
            # Process games in batches for performance
            batch_size = 10
            game_ids = list(signals_by_game.keys())
            
            for i in range(0, len(game_ids), batch_size):
                batch_game_ids = game_ids[i:i + batch_size]
                batch_tasks = []
                
                for game_id in batch_game_ids:
                    signals = signals_by_game[game_id]
                    game_market_data = market_data.get(game_id, {}) if market_data else {}
                    
                    task = self._discover_single_opportunity(
                        game_id, signals, user_profile, game_market_data, force_refresh
                    )
                    batch_tasks.append(task)
                
                # Execute batch
                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                
                for result in batch_results:
                    if isinstance(result, Exception):
                        self.logger.error(f"Batch opportunity discovery error: {result}")
                    elif result is not None:
                        opportunities.append(result)
                        if result.cache_hit:
                            cache_hits += 1
            
            # Filter and rank opportunities
            qualified_opportunities = [
                opp for opp in opportunities 
                if opp.opportunity_score.composite_score >= self.min_opportunity_score
            ]
            
            # Sort by composite score (descending)
            qualified_opportunities.sort(
                key=lambda x: x.opportunity_score.composite_score, reverse=True
            )
            
            # Limit results
            final_opportunities = qualified_opportunities[:self.max_opportunities_per_request]
            
            # Update performance metrics
            discovery_time_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
            self._update_performance_metrics(
                len(final_opportunities), cache_hits, len(opportunities), discovery_time_ms
            )
            
            self.logger.info(
                f"Discovered {len(final_opportunities)} qualifying opportunities "
                f"({len(qualified_opportunities)} total above threshold) "
                f"in {discovery_time_ms:.1f}ms (cache hits: {cache_hits})"
            )
            
            return final_opportunities
            
        except Exception as e:
            self.logger.error(f"Error discovering opportunities: {e}", exc_info=True)
            return []
    
    async def discover_top_opportunities(self,
                                       signals_by_game: Dict[str, List[UnifiedBettingSignal]],
                                       limit: int = 10,
                                       tier_filter: Optional[OpportunityTier] = None,
                                       user_profile: Optional[UserProfile] = None) -> List[OpportunityDiscoveryResult]:
        """
        Discover top-ranked opportunities with filtering
        
        Args:
            signals_by_game: Dictionary mapping game IDs to their betting signals
            limit: Maximum number of opportunities to return
            tier_filter: Optional tier filter (PREMIUM, HIGH_VALUE, etc.)
            user_profile: User profile for personalization
            
        Returns:
            Top opportunities meeting the criteria
        """
        try:
            # Discover all opportunities
            all_opportunities = await self.discover_opportunities(
                signals_by_game, user_profile
            )
            
            # Apply tier filter if specified
            if tier_filter:
                filtered_opportunities = [
                    opp for opp in all_opportunities 
                    if opp.opportunity_score.tier == tier_filter
                ]
            else:
                filtered_opportunities = all_opportunities
            
            # Return top N
            result = filtered_opportunities[:limit]
            
            self.logger.info(
                f"Retrieved {len(result)} top opportunities "
                f"(tier_filter={tier_filter}, limit={limit})"
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error discovering top opportunities: {e}", exc_info=True)
            return []
    
    async def get_opportunity_details(self,
                                    game_id: str,
                                    signals: List[UnifiedBettingSignal],
                                    user_profile: Optional[UserProfile] = None,
                                    include_patterns: bool = True,
                                    include_explanation: bool = True) -> Optional[OpportunityDiscoveryResult]:
        """
        Get detailed analysis for a specific opportunity
        
        Args:
            game_id: Target game identifier
            signals: Betting signals for the game
            user_profile: User profile for personalization
            include_patterns: Whether to include pattern detection
            include_explanation: Whether to generate explanation
            
        Returns:
            Detailed opportunity analysis or None if insufficient data
        """
        try:
            self.logger.info(f"Getting opportunity details for game {game_id}")
            
            if not user_profile:
                user_profile = UserProfile()
            
            # Force fresh analysis for detail requests
            result = await self._discover_single_opportunity(
                game_id=game_id,
                signals=signals,
                user_profile=user_profile,
                market_data={},
                force_refresh=True,
                include_patterns=include_patterns,
                include_explanation=include_explanation
            )
            
            if result:
                self.logger.info(f"Generated detailed analysis for game {game_id}")
            else:
                self.logger.warning(f"Could not generate analysis for game {game_id}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error getting opportunity details for {game_id}: {e}", exc_info=True)
            return None
    
    async def refresh_opportunity(self, 
                                opportunity_id: str,
                                signals: List[UnifiedBettingSignal],
                                user_profile: Optional[UserProfile] = None) -> Optional[OpportunityDiscoveryResult]:
        """
        Refresh an existing opportunity with new data
        
        Args:
            opportunity_id: Existing opportunity identifier
            signals: Updated betting signals
            user_profile: User profile for personalization
            
        Returns:
            Refreshed opportunity analysis
        """
        try:
            # Extract game_id from opportunity_id (assumes format: "opp_{game_id}_{timestamp}")
            parts = opportunity_id.split('_')
            if len(parts) >= 3:
                game_id = parts[1]
            else:
                self.logger.error(f"Invalid opportunity_id format: {opportunity_id}")
                return None
            
            self.logger.info(f"Refreshing opportunity {opportunity_id} for game {game_id}")
            
            # Force fresh analysis
            result = await self._discover_single_opportunity(
                game_id=game_id,
                signals=signals,
                user_profile=user_profile,
                market_data={},
                force_refresh=True
            )
            
            # Remove old cache entry
            if opportunity_id in self._opportunity_cache:
                del self._opportunity_cache[opportunity_id]
                del self._cache_timestamps[opportunity_id]
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error refreshing opportunity {opportunity_id}: {e}", exc_info=True)
            return None
    
    # Performance and monitoring methods
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get service performance metrics"""
        return {
            **self.performance_metrics,
            'cache_size': len(self._opportunity_cache),
            'service_uptime': 'N/A',  # Would be calculated from initialization time
            'component_status': {
                'scoring_engine': 'active',
                'pattern_recognition': 'active' if self.pattern_detection_enabled else 'disabled',
                'explanation_engine': 'active' if self.explanation_generation_enabled else 'disabled'
            }
        }
    
    def get_cache_status(self) -> Dict[str, Any]:
        """Get cache status and statistics"""
        now = datetime.utcnow()
        expired_count = sum(
            1 for timestamp in self._cache_timestamps.values()
            if (now - timestamp).total_seconds() > (self.cache_ttl_hours * 3600)
        )
        
        return {
            'total_cached': len(self._opportunity_cache),
            'expired_entries': expired_count,
            'cache_hit_rate': self.performance_metrics['cache_hit_rate'],
            'ttl_hours': self.cache_ttl_hours
        }
    
    async def clear_cache(self, expired_only: bool = True) -> int:
        """
        Clear opportunity cache
        
        Args:
            expired_only: If True, only clear expired entries
            
        Returns:
            Number of entries cleared
        """
        try:
            if expired_only:
                now = datetime.utcnow()
                expired_keys = [
                    key for key, timestamp in self._cache_timestamps.items()
                    if (now - timestamp).total_seconds() > (self.cache_ttl_hours * 3600)
                ]
                
                for key in expired_keys:
                    del self._opportunity_cache[key]
                    del self._cache_timestamps[key]
                
                cleared_count = len(expired_keys)
                self.logger.info(f"Cleared {cleared_count} expired cache entries")
            else:
                cleared_count = len(self._opportunity_cache)
                self._opportunity_cache.clear()
                self._cache_timestamps.clear()
                self.logger.info(f"Cleared all {cleared_count} cache entries")
            
            return cleared_count
            
        except Exception as e:
            self.logger.error(f"Error clearing cache: {e}")
            return 0
    
    # Private implementation methods
    
    async def _discover_single_opportunity(self,
                                         game_id: str,
                                         signals: List[UnifiedBettingSignal],
                                         user_profile: UserProfile,
                                         market_data: Dict[str, Any],
                                         force_refresh: bool = False,
                                         include_patterns: bool = True,
                                         include_explanation: bool = True) -> Optional[OpportunityDiscoveryResult]:
        """Discover opportunity for a single game"""
        try:
            start_time = datetime.utcnow()
            
            # Check cache first (unless force refresh)
            cache_key = f"{game_id}_{user_profile.experience_level}_{user_profile.risk_tolerance}"
            if not force_refresh and cache_key in self._opportunity_cache:
                cached_result = self._opportunity_cache[cache_key]
                cache_timestamp = self._cache_timestamps[cache_key]
                
                # Check if cache is still valid
                if (start_time - cache_timestamp).total_seconds() < (self.cache_ttl_hours * 3600):
                    cached_result.cache_hit = True
                    return cached_result
            
            if not signals:
                self.logger.warning(f"No signals provided for game {game_id}")
                return None
            
            # 1. Score the opportunity
            opportunity_score = await self.scoring_engine.score_opportunity(
                signals=signals,
                game_id=game_id,
                user_risk_profile=user_profile.risk_tolerance
            )
            
            if not opportunity_score:
                self.logger.warning(f"Could not score opportunity for game {game_id}")
                return None
            
            # 2. Detect patterns (if enabled)
            detected_patterns = []
            if include_patterns and self.pattern_detection_enabled:
                try:
                    detected_patterns = await self.pattern_recognition.detect_patterns(
                        signals=signals,
                        game_id=game_id,
                        market_data=market_data
                    )
                    self.performance_metrics['patterns_detected'] += len(detected_patterns)
                except Exception as e:
                    self.logger.error(f"Pattern detection failed for {game_id}: {e}")
            
            # 3. Generate explanation (if enabled)
            explanation = {}
            if include_explanation and self.explanation_generation_enabled:
                try:
                    explanation = await self.explanation_engine.generate_opportunity_explanation(
                        opportunity=opportunity_score,
                        detected_patterns=detected_patterns,
                        signals=signals,
                        user_profile=user_profile
                    )
                    self.performance_metrics['explanations_generated'] += 1
                except Exception as e:
                    self.logger.error(f"Explanation generation failed for {game_id}: {e}")
            
            # 4. Get ML predictions context
            ml_predictions = {}
            try:
                ml_predictions = await self.prediction_service.get_cached_prediction(game_id) or {}
            except Exception as e:
                self.logger.error(f"Failed to get ML predictions for {game_id}: {e}")
            
            # 5. Build result
            discovery_time_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
            
            result = OpportunityDiscoveryResult(
                opportunity_id=opportunity_score.opportunity_id,
                game_id=game_id,
                opportunity_score=opportunity_score,
                detected_patterns=detected_patterns,
                explanation=explanation,
                contributing_signals=signals,
                ml_predictions=ml_predictions,
                market_context=market_data,
                discovery_time_ms=discovery_time_ms,
                cache_hit=False
            )
            
            # Cache the result
            self._opportunity_cache[cache_key] = result
            self._cache_timestamps[cache_key] = start_time
            self.performance_metrics['opportunities_cached'] += 1
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error discovering opportunity for game {game_id}: {e}", exc_info=True)
            return None
    
    def _update_performance_metrics(self,
                                  opportunities_count: int,
                                  cache_hits: int,
                                  total_processed: int,
                                  discovery_time_ms: float):
        """Update performance tracking metrics"""
        try:
            self.performance_metrics['opportunities_discovered'] += opportunities_count
            
            # Update average discovery time (exponential moving average)
            if self.performance_metrics['average_discovery_time_ms'] == 0:
                self.performance_metrics['average_discovery_time_ms'] = discovery_time_ms
            else:
                alpha = 0.1  # Smoothing factor
                current_avg = self.performance_metrics['average_discovery_time_ms']
                self.performance_metrics['average_discovery_time_ms'] = (
                    alpha * discovery_time_ms + (1 - alpha) * current_avg
                )
            
            # Update cache hit rate
            total_requests = self.performance_metrics['opportunities_discovered']
            total_cache_hits = self.performance_metrics['opportunities_cached'] + cache_hits
            if total_requests > 0:
                self.performance_metrics['cache_hit_rate'] = total_cache_hits / total_requests
            
        except Exception as e:
            self.logger.error(f"Error updating performance metrics: {e}")
    
    async def cleanup(self):
        """Cleanup resources"""
        try:
            # Clear caches
            await self.clear_cache(expired_only=False)
            
            # Cleanup component engines if needed
            # (Add cleanup methods to engines as needed)
            
            self.logger.info("OpportunityDetectionService cleanup completed")
            
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")


# Convenience factory function
async def create_opportunity_detection_service(config: Optional[Dict[str, Any]] = None) -> OpportunityDetectionService:
    """
    Factory function to create and initialize an OpportunityDetectionService
    
    Args:
        config: Service configuration
        
    Returns:
        Initialized opportunity detection service
    """
    try:
        # Initialize prediction service
        prediction_service = PredictionService()
        await prediction_service.initialize()
        
        # Create and return opportunity detection service
        service = OpportunityDetectionService(
            prediction_service=prediction_service,
            config=config
        )
        
        return service
        
    except Exception as e:
        logger = get_logger(__name__)
        logger.error(f"Error creating opportunity detection service: {e}", exc_info=True)
        raise