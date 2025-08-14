"""
Integration tests for the complete AI Opportunity Detection System

Tests end-to-end functionality including:
- Service orchestration
- Component integration
- Performance benchmarks
- Error handling and resilience
"""

import asyncio
import pytest
import time
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timedelta
from typing import List, Dict, Any

from src.ml.opportunity_detection.opportunity_detection_service import (
    OpportunityDetectionService,
    OpportunityDiscoveryResult,
    create_opportunity_detection_service
)
from src.ml.opportunity_detection.explanation_engine import UserProfile, ExplanationLevel
from src.ml.opportunity_detection.opportunity_scoring_engine import RiskProfile, OpportunityTier
from src.analysis.models.unified_models import UnifiedBettingSignal, SignalType


@pytest.mark.integration
class TestOpportunityDetectionIntegration:
    """Integration test suite for the complete opportunity detection system"""
    
    @pytest.fixture
    def mock_prediction_service(self):
        """Mock ML prediction service"""
        service = Mock()
        service.initialize = AsyncMock()
        service.get_prediction = AsyncMock(return_value={
            'win_probability': 0.65,
            'confidence': 0.8,
            'model_version': 'v1.2.3'
        })
        service.get_cached_prediction = AsyncMock(return_value={
            'cached_prediction': {'win_probability': 0.63},
            'cache_timestamp': datetime.utcnow()
        })
        return service
    
    @pytest.fixture
    def service_config(self):
        """Complete service configuration"""
        return {
            'cache_ttl_hours': 1,
            'min_opportunity_score': 30.0,
            'max_opportunities_per_request': 20,
            'pattern_detection_enabled': True,
            'explanation_generation_enabled': True,
            'scoring_engine': {
                'weights': {
                    'sharp_action': 0.25,
                    'line_movement': 0.20,
                    'consensus_divergence': 0.15,
                    'historical_patterns': 0.15,
                    'timing_factors': 0.10,
                    'market_efficiency': 0.10,
                    'confidence_level': 0.05
                }
            },
            'pattern_recognition': {
                'anomaly_detection': {'contamination': 0.1}
            },
            'explanation_engine': {
                'supported_languages': ['en'],
                'default_language': 'en'
            }
        }
    
    @pytest.fixture
    def sample_signals_data(self):
        """Sample signals data for multiple games"""
        signals_by_game = {}
        
        for game_id in ['game_001', 'game_002', 'game_003']:
            signals = []
            base_time = datetime.utcnow()
            
            for i in range(15):  # 15 signals per game
                signal = UnifiedBettingSignal(
                    signal_id=f"{game_id}_signal_{i}",
                    game_id=game_id,
                    signal_type=SignalType.SHARP_ACTION if i % 3 == 0 else 
                               (SignalType.LINE_MOVEMENT if i % 3 == 1 else SignalType.CONSENSUS),
                    strength=0.4 + (i * 0.03) % 0.5,  # Vary 0.4-0.9
                    confidence=0.5 + (i * 0.025) % 0.4,  # Vary 0.5-0.9
                    timestamp=base_time - timedelta(minutes=i * 3)
                )
                signals.append(signal)
            
            signals_by_game[game_id] = signals
        
        return signals_by_game
    
    @pytest.fixture
    def user_profiles(self):
        """Different user profiles for testing"""
        return {
            'beginner': UserProfile(
                user_id='test_beginner',
                experience_level=ExplanationLevel.BEGINNER,
                risk_tolerance=RiskProfile.CONSERVATIVE
            ),
            'expert': UserProfile(
                user_id='test_expert',
                experience_level=ExplanationLevel.EXPERT,
                risk_tolerance=RiskProfile.AGGRESSIVE
            )
        }
    
    @pytest.fixture
    async def opportunity_service(self, mock_prediction_service, service_config):
        """Initialize opportunity detection service with mocks"""
        service = OpportunityDetectionService(
            prediction_service=mock_prediction_service,
            config=service_config
        )
        yield service
        await service.cleanup()
    
    @pytest.mark.asyncio
    async def test_end_to_end_opportunity_discovery(
        self, opportunity_service, sample_signals_data, user_profiles
    ):
        """Test complete end-to-end opportunity discovery workflow"""
        user_profile = user_profiles['beginner']
        
        # Discover opportunities
        opportunities = await opportunity_service.discover_opportunities(
            signals_by_game=sample_signals_data,
            user_profile=user_profile
        )
        
        # Verify results
        assert isinstance(opportunities, list)
        assert len(opportunities) <= opportunity_service.max_opportunities_per_request
        
        for opportunity in opportunities:
            assert isinstance(opportunity, OpportunityDiscoveryResult)
            assert opportunity.game_id in sample_signals_data
            assert opportunity.opportunity_score.composite_score >= opportunity_service.min_opportunity_score
            assert len(opportunity.contributing_signals) > 0
            
            # Should have explanations for beginner user
            if opportunity_service.explanation_generation_enabled:
                assert opportunity.explanation
                assert isinstance(opportunity.explanation, dict)
            
            # Should have patterns if enabled
            if opportunity_service.pattern_detection_enabled:
                assert isinstance(opportunity.detected_patterns, list)
    
    @pytest.mark.asyncio
    async def test_service_performance_benchmarks(
        self, opportunity_service, sample_signals_data, user_profiles
    ):
        """Test service performance against benchmarks"""
        user_profile = user_profiles['expert']
        
        # Measure discovery performance
        start_time = time.time()
        
        opportunities = await opportunity_service.discover_opportunities(
            signals_by_game=sample_signals_data,
            user_profile=user_profile
        )
        
        end_time = time.time()
        duration_ms = (end_time - start_time) * 1000
        
        # Performance assertions
        assert duration_ms < 2000, f"Discovery took {duration_ms:.1f}ms, expected <2000ms for 3 games"
        
        # Each opportunity should meet performance targets
        for opportunity in opportunities:
            assert opportunity.discovery_time_ms < 500, f"Individual discovery took {opportunity.discovery_time_ms:.1f}ms"
    
    @pytest.mark.asyncio
    async def test_concurrent_discovery_requests(
        self, opportunity_service, sample_signals_data, user_profiles
    ):
        """Test concurrent opportunity discovery requests"""
        user_profile = user_profiles['expert']
        
        # Create multiple concurrent requests
        tasks = []
        for i in range(3):
            task = opportunity_service.discover_opportunities(
                signals_by_game=sample_signals_data,
                user_profile=user_profile,
                force_refresh=True  # Avoid cache hits
            )
            tasks.append(task)
        
        # Execute concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # All should succeed
        assert len(results) == 3
        for result in results:
            assert not isinstance(result, Exception)
            assert isinstance(result, list)
            assert all(isinstance(opp, OpportunityDiscoveryResult) for opp in result)
    
    @pytest.mark.asyncio
    async def test_caching_functionality(
        self, opportunity_service, sample_signals_data, user_profiles
    ):
        """Test opportunity caching and cache performance"""
        user_profile = user_profiles['beginner']
        
        # First request (cache miss)
        start_time = time.time()
        opportunities1 = await opportunity_service.discover_opportunities(
            signals_by_game=sample_signals_data,
            user_profile=user_profile
        )
        cache_miss_time = time.time() - start_time
        
        # Second request (should hit cache)
        start_time = time.time()
        opportunities2 = await opportunity_service.discover_opportunities(
            signals_by_game=sample_signals_data,
            user_profile=user_profile
        )
        cache_hit_time = time.time() - start_time
        
        # Cache hit should be significantly faster
        assert cache_hit_time < cache_miss_time * 0.5  # At least 50% faster
        
        # Verify cache hits
        cache_hits = sum(1 for opp in opportunities2 if opp.cache_hit)
        assert cache_hits > 0, "Expected cache hits on second request"
        
        # Results should be equivalent
        assert len(opportunities1) == len(opportunities2)
    
    @pytest.mark.asyncio
    async def test_top_opportunities_filtering(
        self, opportunity_service, sample_signals_data, user_profiles
    ):
        """Test top opportunities retrieval with filtering"""
        user_profile = user_profiles['expert']
        
        # Get top opportunities with tier filter
        top_high_value = await opportunity_service.discover_top_opportunities(
            signals_by_game=sample_signals_data,
            limit=5,
            tier_filter=OpportunityTier.HIGH_VALUE,
            user_profile=user_profile
        )
        
        # Verify filtering
        for opportunity in top_high_value:
            assert opportunity.opportunity_score.tier == OpportunityTier.HIGH_VALUE
        
        assert len(top_high_value) <= 5
        
        # Test without filter
        top_all = await opportunity_service.discover_top_opportunities(
            signals_by_game=sample_signals_data,
            limit=10,
            user_profile=user_profile
        )
        
        assert len(top_all) <= 10
        
        # Should be sorted by score (descending)
        if len(top_all) > 1:
            for i in range(len(top_all) - 1):
                assert (top_all[i].opportunity_score.composite_score >= 
                       top_all[i + 1].opportunity_score.composite_score)
    
    @pytest.mark.asyncio
    async def test_opportunity_detail_analysis(
        self, opportunity_service, sample_signals_data, user_profiles
    ):
        """Test detailed opportunity analysis"""
        user_profile = user_profiles['expert']
        game_id = 'game_001'
        signals = sample_signals_data[game_id]
        
        # Get detailed analysis
        detailed_opportunity = await opportunity_service.get_opportunity_details(
            game_id=game_id,
            signals=signals,
            user_profile=user_profile,
            include_patterns=True,
            include_explanation=True
        )
        
        assert detailed_opportunity is not None
        assert isinstance(detailed_opportunity, OpportunityDiscoveryResult)
        assert detailed_opportunity.game_id == game_id
        
        # Should have comprehensive data
        assert len(detailed_opportunity.contributing_signals) > 0
        assert detailed_opportunity.explanation  # Should have explanation
        assert isinstance(detailed_opportunity.detected_patterns, list)
        
        # Should have ML predictions
        assert detailed_opportunity.ml_predictions
        assert isinstance(detailed_opportunity.ml_predictions, dict)
    
    @pytest.mark.asyncio
    async def test_opportunity_refresh(
        self, opportunity_service, sample_signals_data, user_profiles
    ):
        """Test opportunity refresh functionality"""
        user_profile = user_profiles['beginner']
        
        # First discovery
        opportunities = await opportunity_service.discover_opportunities(
            signals_by_game=sample_signals_data,
            user_profile=user_profile
        )
        
        assert len(opportunities) > 0
        opportunity_to_refresh = opportunities[0]
        
        # Update signals (simulate new data)
        updated_signals = sample_signals_data[opportunity_to_refresh.game_id] + [
            UnifiedBettingSignal(
                signal_id="refresh_signal",
                game_id=opportunity_to_refresh.game_id,
                signal_type=SignalType.SHARP_ACTION,
                strength=0.9,  # High strength new signal
                confidence=0.95,
                timestamp=datetime.utcnow()
            )
        ]
        
        # Refresh the opportunity
        refreshed_opportunity = await opportunity_service.refresh_opportunity(
            opportunity_id=opportunity_to_refresh.opportunity_id,
            signals=updated_signals,
            user_profile=user_profile
        )
        
        assert refreshed_opportunity is not None
        assert refreshed_opportunity.game_id == opportunity_to_refresh.game_id
        assert not refreshed_opportunity.cache_hit  # Should be fresh analysis
        
        # Score might be different due to new signal
        # (exact comparison depends on scoring algorithm)
        assert isinstance(refreshed_opportunity.opportunity_score.composite_score, (int, float))
    
    @pytest.mark.asyncio
    async def test_user_personalization(
        self, opportunity_service, sample_signals_data, user_profiles
    ):
        """Test user personalization across different user types"""
        # Get opportunities for beginner user
        beginner_opportunities = await opportunity_service.discover_opportunities(
            signals_by_game=sample_signals_data,
            user_profile=user_profiles['beginner'],
            force_refresh=True
        )
        
        # Get opportunities for expert user
        expert_opportunities = await opportunity_service.discover_opportunities(
            signals_by_game=sample_signals_data,
            user_profile=user_profiles['expert'],
            force_refresh=True
        )
        
        # Both should return results
        assert len(beginner_opportunities) > 0
        assert len(expert_opportunities) > 0
        
        # Explanations should be different
        if beginner_opportunities and expert_opportunities:
            beginner_explanation = beginner_opportunities[0].explanation
            expert_explanation = expert_opportunities[0].explanation
            
            if beginner_explanation and expert_explanation:
                # Should be personalized differently
                assert beginner_explanation != expert_explanation
    
    @pytest.mark.asyncio
    async def test_error_resilience(
        self, opportunity_service, sample_signals_data, user_profiles
    ):
        """Test error handling and service resilience"""
        user_profile = user_profiles['beginner']
        
        # Test with ML service failure
        opportunity_service.prediction_service.get_prediction.side_effect = Exception("ML service down")
        
        opportunities = await opportunity_service.discover_opportunities(
            signals_by_game=sample_signals_data,
            user_profile=user_profile
        )
        
        # Should still return results despite ML failure
        assert isinstance(opportunities, list)
        # May be empty or reduced, but should not crash
        
        # Reset ML service
        opportunity_service.prediction_service.get_prediction.side_effect = None
        opportunity_service.prediction_service.get_prediction.return_value = {'win_probability': 0.6}
        
        # Test with invalid signals
        invalid_signals_data = {
            'invalid_game': [Mock(strength=None, confidence="invalid")]
        }
        
        opportunities = await opportunity_service.discover_opportunities(
            signals_by_game=invalid_signals_data,
            user_profile=user_profile
        )
        
        # Should handle gracefully
        assert isinstance(opportunities, list)
    
    @pytest.mark.asyncio
    async def test_performance_metrics_tracking(
        self, opportunity_service, sample_signals_data, user_profiles
    ):
        """Test performance metrics tracking"""
        user_profile = user_profiles['expert']
        
        # Get initial metrics
        initial_metrics = opportunity_service.get_performance_metrics()
        
        # Perform discovery
        await opportunity_service.discover_opportunities(
            signals_by_game=sample_signals_data,
            user_profile=user_profile
        )
        
        # Get updated metrics
        final_metrics = opportunity_service.get_performance_metrics()
        
        # Metrics should be updated
        assert final_metrics['opportunities_discovered'] >= initial_metrics['opportunities_discovered']
        assert final_metrics['average_discovery_time_ms'] > 0
        
        # Component status should be tracked
        assert 'component_status' in final_metrics
        assert final_metrics['component_status']['scoring_engine'] == 'active'
    
    @pytest.mark.asyncio
    async def test_cache_management(
        self, opportunity_service, sample_signals_data, user_profiles
    ):
        """Test cache management functionality"""
        user_profile = user_profiles['beginner']
        
        # Fill cache with discoveries
        await opportunity_service.discover_opportunities(
            signals_by_game=sample_signals_data,
            user_profile=user_profile
        )
        
        # Check cache status
        cache_status = opportunity_service.get_cache_status()
        assert cache_status['total_cached'] > 0
        assert isinstance(cache_status['cache_hit_rate'], (int, float))
        
        # Clear cache
        cleared_count = await opportunity_service.clear_cache(expired_only=False)
        assert cleared_count > 0
        
        # Verify cache is empty
        post_clear_status = opportunity_service.get_cache_status()
        assert post_clear_status['total_cached'] == 0
    
    @pytest.mark.asyncio
    async def test_market_data_integration(
        self, opportunity_service, sample_signals_data, user_profiles
    ):
        """Test integration of market data into opportunity discovery"""
        user_profile = user_profiles['expert']
        
        market_data = {
            'game_001': {
                'public_betting_percentage': 65,
                'total_volume': 1500000,
                'spread_movement': -1.5,
                'weather': 'clear'
            },
            'game_002': {
                'public_betting_percentage': 45,
                'total_volume': 800000,
                'spread_movement': 0.5,
                'weather': 'rain'
            }
        }
        
        opportunities = await opportunity_service.discover_opportunities(
            signals_by_game=sample_signals_data,
            user_profile=user_profile,
            market_data=market_data
        )
        
        # Should integrate market context
        for opportunity in opportunities:
            if opportunity.game_id in market_data:
                assert opportunity.market_context
                assert isinstance(opportunity.market_context, dict)
                assert len(opportunity.market_context) > 0


@pytest.mark.asyncio
async def test_factory_function():
    """Test the factory function for creating opportunity detection service"""
    with patch('src.ml.services.prediction_service.PredictionService') as MockPredictionService:
        mock_service = Mock()
        mock_service.initialize = AsyncMock()
        MockPredictionService.return_value = mock_service
        
        # Create service using factory
        config = {'test_config': True}
        service = await create_opportunity_detection_service(config)
        
        assert isinstance(service, OpportunityDetectionService)
        assert service.config == config
        mock_service.initialize.assert_called_once()
        
        # Cleanup
        await service.cleanup()


@pytest.mark.asyncio
async def test_large_scale_performance():
    """Test performance with larger data sets"""
    from src.ml.services.prediction_service import PredictionService
    
    # Mock prediction service for performance test
    prediction_service = Mock()
    prediction_service.get_prediction = AsyncMock(return_value={'win_probability': 0.65})
    prediction_service.get_cached_prediction = AsyncMock(return_value={})
    
    service = OpportunityDetectionService(
        prediction_service=prediction_service,
        config={'max_opportunities_per_request': 100}
    )
    
    # Generate large dataset
    signals_by_game = {}
    for i in range(20):  # 20 games
        game_id = f"large_game_{i}"
        signals = []
        
        for j in range(25):  # 25 signals per game
            signal = UnifiedBettingSignal(
                signal_id=f"{game_id}_signal_{j}",
                game_id=game_id,
                signal_type=SignalType.SHARP_ACTION,
                strength=0.5 + (j * 0.015) % 0.4,
                confidence=0.6 + (j * 0.012) % 0.3,
                timestamp=datetime.utcnow() - timedelta(minutes=j * 2)
            )
            signals.append(signal)
        
        signals_by_game[game_id] = signals
    
    # Measure performance
    start_time = time.time()
    
    opportunities = await service.discover_opportunities(
        signals_by_game=signals_by_game,
        user_profile=UserProfile()
    )
    
    end_time = time.time()
    duration_ms = (end_time - start_time) * 1000
    
    # Performance targets for large scale
    assert duration_ms < 10000, f"Large scale discovery took {duration_ms:.1f}ms, expected <10s"
    assert len(opportunities) > 0
    assert len(opportunities) <= 100  # Respects limit
    
    # Cleanup
    await service.cleanup()


if __name__ == "__main__":
    pytest.main([__file__])