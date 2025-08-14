"""
Unit tests for OpportunityScoringEngine

Tests all critical functionality including:
- Multi-factor scoring algorithm
- Database integration (mocked)
- Configuration handling
- Error conditions
- Performance validation
"""

import asyncio
import pytest
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timedelta
from typing import List, Dict, Any

from src.ml.opportunity_detection.opportunity_scoring_engine import (
    OpportunityScoringEngine,
    OpportunityScore,
    OpportunityTier,
    RiskProfile,
    ScoringFactors
)
from src.analysis.models.unified_models import UnifiedBettingSignal, SignalType


class TestOpportunityScoringEngine:
    """Test suite for OpportunityScoringEngine"""
    
    @pytest.fixture
    def mock_prediction_service(self):
        """Mock prediction service"""
        service = Mock()
        service.get_prediction = AsyncMock(return_value={'win_probability': 0.65})
        service.get_cached_prediction = AsyncMock(return_value={'cached': True})
        return service
    
    @pytest.fixture
    def scoring_config(self):
        """Standard scoring configuration"""
        return {
            'weights': {
                'sharp_action': 0.25,
                'line_movement': 0.20,
                'consensus_divergence': 0.15,
                'historical_patterns': 0.15,
                'timing_factors': 0.10,
                'market_efficiency': 0.10,
                'confidence_level': 0.05
            },
            'thresholds': {
                'premium_threshold': 75.0,
                'high_value_threshold': 60.0,
                'moderate_threshold': 40.0
            }
        }
    
    @pytest.fixture
    def sample_signals(self):
        """Sample betting signals for testing"""
        return [
            UnifiedBettingSignal(
                signal_id="test_1",
                game_id="game_123",
                signal_type=SignalType.SHARP_ACTION,
                strength=0.8,
                confidence=0.9,
                timestamp=datetime.utcnow()
            ),
            UnifiedBettingSignal(
                signal_id="test_2",
                game_id="game_123",
                signal_type=SignalType.LINE_MOVEMENT,
                strength=0.7,
                confidence=0.8,
                timestamp=datetime.utcnow()
            )
        ]
    
    @pytest.fixture
    def scoring_engine(self, mock_prediction_service, scoring_config):
        """Initialize scoring engine with mocks"""
        return OpportunityScoringEngine(
            prediction_service=mock_prediction_service,
            config=scoring_config
        )
    
    def test_initialization(self, mock_prediction_service, scoring_config):
        """Test proper initialization with configuration"""
        engine = OpportunityScoringEngine(
            prediction_service=mock_prediction_service,
            config=scoring_config
        )
        
        assert engine.prediction_service == mock_prediction_service
        assert engine.config == scoring_config
        assert engine.weights == scoring_config['weights']
        assert engine.thresholds == scoring_config['thresholds']
        assert isinstance(engine.performance_cache, dict)
    
    def test_initialization_with_defaults(self, mock_prediction_service):
        """Test initialization with default configuration"""
        engine = OpportunityScoringEngine(
            prediction_service=mock_prediction_service
        )
        
        # Should have default weights
        assert 'sharp_action' in engine.weights
        assert 'line_movement' in engine.weights
        assert sum(engine.weights.values()) == pytest.approx(1.0, rel=0.01)
    
    @pytest.mark.asyncio
    async def test_score_opportunity_basic(self, scoring_engine, sample_signals):
        """Test basic opportunity scoring"""
        game_id = "game_123"
        risk_profile = RiskProfile.MODERATE
        
        result = await scoring_engine.score_opportunity(
            signals=sample_signals,
            game_id=game_id,
            user_risk_profile=risk_profile
        )
        
        assert isinstance(result, OpportunityScore)
        assert result.game_id == game_id
        assert result.risk_profile == risk_profile
        assert isinstance(result.scoring_factors, ScoringFactors)
        assert 0 <= result.composite_score <= 100
        assert result.opportunity_id.startswith("opp_")
    
    @pytest.mark.asyncio
    async def test_score_opportunity_empty_signals(self, scoring_engine):
        """Test scoring with empty signals list"""
        result = await scoring_engine.score_opportunity(
            signals=[],
            game_id="game_123",
            user_risk_profile=RiskProfile.MODERATE
        )
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_score_opportunity_invalid_game_id(self, scoring_engine, sample_signals):
        """Test scoring with invalid game ID"""
        result = await scoring_engine.score_opportunity(
            signals=sample_signals,
            game_id="",  # Invalid
            user_risk_profile=RiskProfile.MODERATE
        )
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_tier_assignment(self, scoring_engine, sample_signals):
        """Test opportunity tier assignment based on score"""
        # Test with signals that should produce different tiers
        with patch.object(scoring_engine, '_calculate_composite_score') as mock_calc:
            # Test PREMIUM tier
            mock_calc.return_value = 80.0
            result = await scoring_engine.score_opportunity(
                sample_signals, "game_123", RiskProfile.MODERATE
            )
            assert result.tier == OpportunityTier.PREMIUM
            
            # Test HIGH_VALUE tier
            mock_calc.return_value = 65.0
            result = await scoring_engine.score_opportunity(
                sample_signals, "game_123", RiskProfile.MODERATE
            )
            assert result.tier == OpportunityTier.HIGH_VALUE
            
            # Test MODERATE tier
            mock_calc.return_value = 45.0
            result = await scoring_engine.score_opportunity(
                sample_signals, "game_123", RiskProfile.MODERATE
            )
            assert result.tier == OpportunityTier.MODERATE
            
            # Test LOW tier
            mock_calc.return_value = 25.0
            result = await scoring_engine.score_opportunity(
                sample_signals, "game_123", RiskProfile.MODERATE
            )
            assert result.tier == OpportunityTier.LOW
    
    @pytest.mark.asyncio
    async def test_risk_profile_adjustment(self, scoring_engine, sample_signals):
        """Test that risk profile affects scoring"""
        game_id = "game_123"
        
        # Get baseline score
        conservative_result = await scoring_engine.score_opportunity(
            sample_signals, game_id, RiskProfile.CONSERVATIVE
        )
        
        aggressive_result = await scoring_engine.score_opportunity(
            sample_signals, game_id, RiskProfile.AGGRESSIVE
        )
        
        # Aggressive should have higher risk tolerance
        assert conservative_result.composite_score != aggressive_result.composite_score
    
    def test_calculate_sharp_action_score(self, scoring_engine, sample_signals):
        """Test sharp action scoring component"""
        sharp_signals = [s for s in sample_signals if s.signal_type == SignalType.SHARP_ACTION]
        
        score = scoring_engine._calculate_sharp_action_score(sharp_signals)
        
        assert 0 <= score <= 100
        assert isinstance(score, (int, float))
    
    def test_calculate_line_movement_score(self, scoring_engine, sample_signals):
        """Test line movement scoring component"""
        movement_signals = [s for s in sample_signals if s.signal_type == SignalType.LINE_MOVEMENT]
        
        score = scoring_engine._calculate_line_movement_score(movement_signals)
        
        assert 0 <= score <= 100
        assert isinstance(score, (int, float))
    
    def test_calculate_consensus_divergence_score(self, scoring_engine, sample_signals):
        """Test consensus divergence scoring"""
        score = scoring_engine._calculate_consensus_divergence_score(sample_signals)
        
        assert 0 <= score <= 100
        assert isinstance(score, (int, float))
    
    @pytest.mark.asyncio
    async def test_get_historical_performance(self, scoring_engine):
        """Test historical performance lookup"""
        game_id = "game_123"
        
        # Mock database query
        with patch('asyncpg.connect') as mock_connect:
            mock_conn = AsyncMock()
            mock_connect.return_value.__aenter__.return_value = mock_conn
            mock_conn.fetchrow.return_value = {
                'sharp_action': 68.0,
                'line_movement': 72.0,
                'consensus_plays': 65.0
            }
            
            performance = await scoring_engine._get_historical_performance(game_id)
            
            assert isinstance(performance, dict)
            assert 'sharp_action' in performance
            assert performance['sharp_action'] == 68.0
    
    @pytest.mark.asyncio
    async def test_get_historical_performance_fallback(self, scoring_engine):
        """Test fallback to cached data when database fails"""
        game_id = "game_123"
        
        # Mock database failure
        with patch('asyncpg.connect', side_effect=Exception("Database error")):
            performance = await scoring_engine._get_historical_performance(game_id)
            
            # Should return cached/default values
            assert isinstance(performance, dict)
            assert 'sharp_action' in performance
    
    @pytest.mark.asyncio
    async def test_performance_caching(self, scoring_engine):
        """Test performance data caching"""
        game_id = "game_123"
        
        # First call should hit database
        with patch('asyncpg.connect') as mock_connect:
            mock_conn = AsyncMock()
            mock_connect.return_value.__aenter__.return_value = mock_conn
            mock_conn.fetchrow.return_value = {'sharp_action': 68.0}
            
            perf1 = await scoring_engine._get_historical_performance(game_id)
            perf2 = await scoring_engine._get_historical_performance(game_id)
            
            # Should only call database once due to caching
            assert mock_connect.call_count == 1
            assert perf1 == perf2
    
    @pytest.mark.asyncio
    async def test_ml_integration(self, scoring_engine, sample_signals):
        """Test integration with ML prediction service"""
        game_id = "game_123"
        
        # Mock ML service response
        scoring_engine.prediction_service.get_prediction.return_value = {
            'win_probability': 0.75,
            'confidence': 0.85
        }
        
        result = await scoring_engine.score_opportunity(
            sample_signals, game_id, RiskProfile.MODERATE
        )
        
        # Should have called ML service
        scoring_engine.prediction_service.get_prediction.assert_called_once_with(game_id)
        assert result is not None
    
    @pytest.mark.asyncio
    async def test_ml_service_failure_handling(self, scoring_engine, sample_signals):
        """Test handling of ML service failures"""
        game_id = "game_123"
        
        # Mock ML service failure
        scoring_engine.prediction_service.get_prediction.side_effect = Exception("ML service error")
        
        result = await scoring_engine.score_opportunity(
            sample_signals, game_id, RiskProfile.MODERATE
        )
        
        # Should still return result despite ML failure
        assert result is not None
        assert isinstance(result, OpportunityScore)
    
    def test_timing_factors_calculation(self, scoring_engine, sample_signals):
        """Test timing factors scoring"""
        # Create signals with different timestamps
        recent_signal = UnifiedBettingSignal(
            signal_id="recent",
            game_id="game_123",
            signal_type=SignalType.SHARP_ACTION,
            strength=0.8,
            timestamp=datetime.utcnow() - timedelta(minutes=5)
        )
        
        old_signal = UnifiedBettingSignal(
            signal_id="old",
            game_id="game_123",
            signal_type=SignalType.SHARP_ACTION,
            strength=0.8,
            timestamp=datetime.utcnow() - timedelta(hours=2)
        )
        
        recent_score = scoring_engine._calculate_timing_factors([recent_signal])
        old_score = scoring_engine._calculate_timing_factors([old_signal])
        
        # Recent signals should score higher
        assert recent_score >= old_score
    
    def test_market_efficiency_scoring(self, scoring_engine, sample_signals):
        """Test market efficiency component"""
        score = scoring_engine._calculate_market_efficiency_score(sample_signals)
        
        assert 0 <= score <= 100
        assert isinstance(score, (int, float))
    
    def test_confidence_level_calculation(self, scoring_engine, sample_signals):
        """Test confidence level aggregation"""
        score = scoring_engine._calculate_confidence_level_score(sample_signals)
        
        assert 0 <= score <= 100
        assert isinstance(score, (int, float))
    
    @pytest.mark.asyncio
    async def test_performance_metrics_tracking(self, scoring_engine, sample_signals):
        """Test that performance metrics are tracked"""
        initial_metrics = scoring_engine.get_performance_metrics()
        
        await scoring_engine.score_opportunity(
            sample_signals, "game_123", RiskProfile.MODERATE
        )
        
        final_metrics = scoring_engine.get_performance_metrics()
        
        # Should have incremented opportunities scored
        assert final_metrics['opportunities_scored'] > initial_metrics['opportunities_scored']
    
    @pytest.mark.asyncio
    async def test_concurrent_scoring(self, scoring_engine, sample_signals):
        """Test concurrent opportunity scoring"""
        tasks = []
        for i in range(5):
            task = scoring_engine.score_opportunity(
                sample_signals, f"game_{i}", RiskProfile.MODERATE
            )
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)
        
        # All should succeed
        assert len(results) == 5
        assert all(r is not None for r in results)
        assert all(isinstance(r, OpportunityScore) for r in results)
    
    def test_configuration_validation(self, mock_prediction_service):
        """Test configuration validation"""
        # Test invalid weights (don't sum to 1.0)
        invalid_config = {
            'weights': {
                'sharp_action': 0.5,
                'line_movement': 0.3  # Sum = 0.8, should be 1.0
            }
        }
        
        engine = OpportunityScoringEngine(
            prediction_service=mock_prediction_service,
            config=invalid_config
        )
        
        # Should normalize weights automatically
        assert abs(sum(engine.weights.values()) - 1.0) < 0.01
    
    def test_edge_cases(self, scoring_engine):
        """Test edge cases and boundary conditions"""
        # Test with single signal
        single_signal = [UnifiedBettingSignal(
            signal_id="single",
            game_id="game_123",
            signal_type=SignalType.SHARP_ACTION,
            strength=1.0,
            confidence=1.0,
            timestamp=datetime.utcnow()
        )]
        
        score = scoring_engine._calculate_sharp_action_score(single_signal)
        assert 0 <= score <= 100
        
        # Test with zero strength signals
        zero_signal = [UnifiedBettingSignal(
            signal_id="zero",
            game_id="game_123",
            signal_type=SignalType.SHARP_ACTION,
            strength=0.0,
            confidence=0.0,
            timestamp=datetime.utcnow()
        )]
        
        score = scoring_engine._calculate_sharp_action_score(zero_signal)
        assert score >= 0
    
    @pytest.mark.asyncio
    async def test_error_handling(self, scoring_engine, sample_signals):
        """Test error handling in various scenarios"""
        # Test with None signals
        result = await scoring_engine.score_opportunity(
            None, "game_123", RiskProfile.MODERATE
        )
        assert result is None
        
        # Test with invalid risk profile
        with patch('src.ml.opportunity_detection.opportunity_scoring_engine.RiskProfile') as mock_risk:
            mock_risk.MODERATE = None
            result = await scoring_engine.score_opportunity(
                sample_signals, "game_123", None
            )
            # Should handle gracefully
            assert result is None or isinstance(result, OpportunityScore)


@pytest.mark.asyncio
async def test_scoring_engine_performance():
    """Test scoring engine performance benchmarks"""
    from src.ml.services.prediction_service import PredictionService
    import time
    
    # Create mock prediction service
    prediction_service = Mock()
    prediction_service.get_prediction = AsyncMock(return_value={'win_probability': 0.65})
    
    engine = OpportunityScoringEngine(prediction_service=prediction_service)
    
    # Create test signals
    signals = [
        UnifiedBettingSignal(
            signal_id=f"test_{i}",
            game_id="perf_test",
            signal_type=SignalType.SHARP_ACTION,
            strength=0.7 + (i * 0.1) % 0.3,
            confidence=0.8 + (i * 0.05) % 0.2,
            timestamp=datetime.utcnow()
        )
        for i in range(10)
    ]
    
    # Measure performance
    start_time = time.time()
    
    result = await engine.score_opportunity(
        signals=signals,
        game_id="perf_test",
        user_risk_profile=RiskProfile.MODERATE
    )
    
    end_time = time.time()
    duration_ms = (end_time - start_time) * 1000
    
    # Should meet <100ms target
    assert duration_ms < 100, f"Scoring took {duration_ms:.1f}ms, expected <100ms"
    assert result is not None
    assert isinstance(result, OpportunityScore)


if __name__ == "__main__":
    pytest.main([__file__])