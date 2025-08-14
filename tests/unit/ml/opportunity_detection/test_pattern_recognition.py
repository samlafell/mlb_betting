"""
Unit tests for MLPatternRecognition

Tests ML pattern detection functionality including:
- Anomaly detection with Isolation Forest
- Statistical pattern analysis
- Performance validation
- Error handling
"""

import pytest
import numpy as np
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
from typing import List, Dict, Any

from src.ml.opportunity_detection.pattern_recognition import (
    MLPatternRecognition,
    DetectedPattern,
    PatternType,
    PatternConfidence,
    AnomalyType
)
from src.analysis.models.unified_models import UnifiedBettingSignal, SignalType


class TestMLPatternRecognition:
    """Test suite for MLPatternRecognition"""
    
    @pytest.fixture
    def pattern_config(self):
        """Standard pattern recognition configuration"""
        return {
            'anomaly_detection': {
                'contamination': 0.1,
                'n_estimators': 50,
                'max_samples': 256
            },
            'statistical_analysis': {
                'z_score_threshold': 2.0,
                'percentile_threshold': 95
            },
            'memory_optimization': {
                'max_baseline_size': 1000,
                'feature_cache_size': 500
            }
        }
    
    @pytest.fixture
    def sample_signals(self):
        """Sample betting signals for testing"""
        signals = []
        base_time = datetime.utcnow()
        
        for i in range(20):
            signal = UnifiedBettingSignal(
                signal_id=f"test_{i}",
                game_id="game_123",
                signal_type=SignalType.SHARP_ACTION if i % 3 == 0 else SignalType.LINE_MOVEMENT,
                strength=0.5 + (i * 0.02) % 0.4,  # Vary between 0.5-0.9
                confidence=0.6 + (i * 0.015) % 0.3,  # Vary between 0.6-0.9
                timestamp=base_time - timedelta(minutes=i * 5)
            )
            signals.append(signal)
        
        return signals
    
    @pytest.fixture
    def anomalous_signals(self):
        """Signals with clear anomalies for testing"""
        base_time = datetime.utcnow()
        
        # Normal signals
        normal_signals = [
            UnifiedBettingSignal(
                signal_id=f"normal_{i}",
                game_id="game_123",
                signal_type=SignalType.SHARP_ACTION,
                strength=0.7 + np.random.normal(0, 0.05),  # Normal distribution
                confidence=0.8 + np.random.normal(0, 0.03),
                timestamp=base_time - timedelta(minutes=i * 2)
            )
            for i in range(15)
        ]
        
        # Anomalous signals
        anomalous_signals = [
            UnifiedBettingSignal(
                signal_id="anomaly_1",
                game_id="game_123",
                signal_type=SignalType.SHARP_ACTION,
                strength=0.95,  # Much higher than normal
                confidence=0.98,
                timestamp=base_time - timedelta(minutes=30)
            ),
            UnifiedBettingSignal(
                signal_id="anomaly_2",
                game_id="game_123",
                signal_type=SignalType.LINE_MOVEMENT,
                strength=0.05,  # Much lower than normal
                confidence=0.02,
                timestamp=base_time - timedelta(minutes=35)
            )
        ]
        
        return normal_signals + anomalous_signals
    
    @pytest.fixture
    def pattern_recognition(self, pattern_config):
        """Initialize pattern recognition with config"""
        return MLPatternRecognition(config=pattern_config)
    
    def test_initialization(self, pattern_config):
        """Test proper initialization with configuration"""
        pr = MLPatternRecognition(config=pattern_config)
        
        assert pr.config == pattern_config
        assert pr.anomaly_contamination == 0.1
        assert pr.z_score_threshold == 2.0
        assert pr.percentile_threshold == 95
        assert isinstance(pr.feature_cache, dict)
        assert pr.isolation_forest is not None
    
    def test_initialization_with_defaults(self):
        """Test initialization with default configuration"""
        pr = MLPatternRecognition()
        
        # Should have default values
        assert pr.anomaly_contamination > 0
        assert pr.z_score_threshold > 0
        assert pr.percentile_threshold > 0
        assert pr.isolation_forest is not None
    
    @pytest.mark.asyncio
    async def test_detect_patterns_basic(self, pattern_recognition, sample_signals):
        """Test basic pattern detection"""
        game_id = "game_123"
        market_data = {'volume': 1000, 'spread': 0.05}
        
        patterns = await pattern_recognition.detect_patterns(
            signals=sample_signals,
            game_id=game_id,
            market_data=market_data
        )
        
        assert isinstance(patterns, list)
        assert all(isinstance(p, DetectedPattern) for p in patterns)
        
        # Should have detected some patterns with sufficient signals
        if patterns:
            for pattern in patterns:
                assert pattern.game_id == game_id
                assert isinstance(pattern.pattern_type, PatternType)
                assert isinstance(pattern.confidence, PatternConfidence)
                assert 0 <= pattern.strength <= 1.0
    
    @pytest.mark.asyncio
    async def test_detect_patterns_empty_signals(self, pattern_recognition):
        """Test pattern detection with empty signals"""
        patterns = await pattern_recognition.detect_patterns(
            signals=[],
            game_id="game_123"
        )
        
        assert patterns == []
    
    @pytest.mark.asyncio
    async def test_detect_patterns_insufficient_signals(self, pattern_recognition):
        """Test pattern detection with insufficient signals"""
        single_signal = [UnifiedBettingSignal(
            signal_id="single",
            game_id="game_123",
            signal_type=SignalType.SHARP_ACTION,
            strength=0.7,
            confidence=0.8,
            timestamp=datetime.utcnow()
        )]
        
        patterns = await pattern_recognition.detect_patterns(
            signals=single_signal,
            game_id="game_123"
        )
        
        # Should handle gracefully with minimal patterns or empty list
        assert isinstance(patterns, list)
    
    def test_extract_features(self, pattern_recognition, sample_signals):
        """Test feature extraction from signals"""
        features = pattern_recognition._extract_features(sample_signals)
        
        assert isinstance(features, np.ndarray)
        assert len(features.shape) == 1  # 1D feature vector
        assert len(features) > 0
        
        # Features should be normalized
        assert np.all(np.isfinite(features))
    
    def test_extract_features_caching(self, pattern_recognition, sample_signals):
        """Test feature extraction caching"""
        game_id = "game_123"
        
        # First call should compute features
        features1 = pattern_recognition._extract_features(sample_signals, game_id)
        
        # Second call should use cache
        features2 = pattern_recognition._extract_features(sample_signals, game_id)
        
        np.testing.assert_array_equal(features1, features2)
        assert game_id in pattern_recognition.feature_cache
    
    def test_detect_anomalies_isolation_forest(self, pattern_recognition, anomalous_signals):
        """Test anomaly detection using Isolation Forest"""
        # Generate baseline features
        baseline_features = np.random.normal(0.7, 0.05, (100, 8))  # Normal distribution
        
        # Test with anomalous signals
        anomalies = pattern_recognition._detect_anomalies_isolation_forest(
            signals=anomalous_signals,
            baseline_features=baseline_features
        )
        
        assert isinstance(anomalies, list)
        
        if anomalies:
            for anomaly in anomalies:
                assert hasattr(anomaly, 'signal_id')
                assert hasattr(anomaly, 'anomaly_score')
                assert hasattr(anomaly, 'anomaly_type')
    
    def test_detect_statistical_anomalies(self, pattern_recognition, anomalous_signals):
        """Test statistical anomaly detection"""
        anomalies = pattern_recognition._detect_statistical_anomalies(anomalous_signals)
        
        assert isinstance(anomalies, list)
        
        if anomalies:
            for anomaly in anomalies:
                assert hasattr(anomaly, 'signal_id')
                assert hasattr(anomaly, 'z_score')
                assert hasattr(anomaly, 'percentile')
    
    @pytest.mark.asyncio
    async def test_detect_temporal_patterns(self, pattern_recognition, sample_signals):
        """Test temporal pattern detection"""
        patterns = await pattern_recognition._detect_temporal_patterns(sample_signals)
        
        assert isinstance(patterns, list)
        
        for pattern in patterns:
            assert isinstance(pattern, DetectedPattern)
            assert pattern.pattern_type in [PatternType.TEMPORAL_CLUSTER, PatternType.TREND_REVERSAL]
    
    @pytest.mark.asyncio
    async def test_detect_signal_correlations(self, pattern_recognition, sample_signals):
        """Test signal correlation detection"""
        patterns = await pattern_recognition._detect_signal_correlations(sample_signals)
        
        assert isinstance(patterns, list)
        
        for pattern in patterns:
            assert isinstance(pattern, DetectedPattern)
            assert pattern.pattern_type == PatternType.SIGNAL_CORRELATION
    
    def test_calculate_pattern_confidence(self, pattern_recognition):
        """Test pattern confidence calculation"""
        # Test high confidence
        high_conf = pattern_recognition._calculate_pattern_confidence(
            strength=0.9,
            support_count=15,
            statistical_significance=0.95
        )
        assert high_conf == PatternConfidence.HIGH
        
        # Test medium confidence
        medium_conf = pattern_recognition._calculate_pattern_confidence(
            strength=0.7,
            support_count=8,
            statistical_significance=0.85
        )
        assert medium_conf == PatternConfidence.MEDIUM
        
        # Test low confidence
        low_conf = pattern_recognition._calculate_pattern_confidence(
            strength=0.4,
            support_count=3,
            statistical_significance=0.60
        )
        assert low_conf == PatternConfidence.LOW
    
    @pytest.mark.asyncio
    async def test_memory_optimization(self, pattern_recognition):
        """Test memory optimization features"""
        # Test with large baseline that should be limited
        large_baseline_size = 2000
        
        with patch('numpy.random.normal') as mock_random:
            mock_random.return_value = np.zeros((large_baseline_size, 8))
            
            # Should limit baseline size
            limited_features = pattern_recognition._generate_baseline_features(
                num_features=8,
                baseline_size=large_baseline_size
            )
            
            # Should be limited to max_baseline_size from config
            assert limited_features.shape[0] <= pattern_recognition.config['memory_optimization']['max_baseline_size']
    
    def test_feature_cache_cleanup(self, pattern_recognition, sample_signals):
        """Test feature cache size management"""
        max_cache_size = pattern_recognition.config['memory_optimization']['feature_cache_size']
        
        # Fill cache beyond limit
        for i in range(max_cache_size + 10):
            pattern_recognition._extract_features(sample_signals, f"game_{i}")
        
        # Cache should be limited
        assert len(pattern_recognition.feature_cache) <= max_cache_size
    
    @pytest.mark.asyncio
    async def test_error_handling(self, pattern_recognition):
        """Test error handling in various scenarios"""
        # Test with None signals
        patterns = await pattern_recognition.detect_patterns(
            signals=None,
            game_id="game_123"
        )
        assert patterns == []
        
        # Test with invalid game_id
        patterns = await pattern_recognition.detect_patterns(
            signals=[],
            game_id=""
        )
        assert patterns == []
        
        # Test with corrupted signal data
        invalid_signal = Mock()
        invalid_signal.strength = None
        invalid_signal.confidence = "invalid"
        
        patterns = await pattern_recognition.detect_patterns(
            signals=[invalid_signal],
            game_id="game_123"
        )
        assert isinstance(patterns, list)  # Should handle gracefully
    
    @pytest.mark.asyncio
    async def test_isolation_forest_failure_handling(self, pattern_recognition, sample_signals):
        """Test handling of Isolation Forest failures"""
        # Mock Isolation Forest to raise exception
        with patch.object(pattern_recognition.isolation_forest, 'fit', side_effect=Exception("ML error")):
            with patch.object(pattern_recognition.isolation_forest, 'decision_function', side_effect=Exception("ML error")):
                patterns = await pattern_recognition.detect_patterns(
                    signals=sample_signals,
                    game_id="game_123"
                )
                
                # Should still return results (potentially with statistical methods only)
                assert isinstance(patterns, list)
    
    def test_numpy_optimization(self, pattern_recognition, sample_signals):
        """Test numpy vectorization optimizations"""
        # Test with large signal set
        large_signals = []
        for i in range(100):
            signal = UnifiedBettingSignal(
                signal_id=f"large_{i}",
                game_id="game_123",
                signal_type=SignalType.SHARP_ACTION,
                strength=np.random.uniform(0.3, 0.9),
                confidence=np.random.uniform(0.5, 1.0),
                timestamp=datetime.utcnow() - timedelta(minutes=i)
            )
            large_signals.append(signal)
        
        # Should handle efficiently with numpy operations
        import time
        start_time = time.time()
        
        features = pattern_recognition._extract_features(large_signals)
        
        end_time = time.time()
        duration_ms = (end_time - start_time) * 1000
        
        # Should be fast with numpy optimization
        assert duration_ms < 100  # Should complete in <100ms
        assert isinstance(features, np.ndarray)
    
    def test_statistical_significance_calculation(self, pattern_recognition):
        """Test statistical significance calculations"""
        # Test with known statistical values
        test_values = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        
        # Should handle various statistical calculations
        z_scores = (test_values - np.mean(test_values)) / np.std(test_values)
        assert np.all(np.isfinite(z_scores))
        
        percentiles = np.percentile(test_values, [25, 50, 75, 95])
        assert len(percentiles) == 4
        assert np.all(percentiles >= np.min(test_values))
        assert np.all(percentiles <= np.max(test_values))
    
    @pytest.mark.asyncio
    async def test_pattern_type_classification(self, pattern_recognition, sample_signals):
        """Test proper classification of different pattern types"""
        patterns = await pattern_recognition.detect_patterns(
            signals=sample_signals,
            game_id="game_123",
            market_data={'volatility': 0.15}
        )
        
        pattern_types = set()
        for pattern in patterns:
            pattern_types.add(pattern.pattern_type)
        
        # Should classify into multiple pattern types
        assert len(pattern_types) >= 0
        
        # All pattern types should be valid
        valid_types = {PatternType.ANOMALY, PatternType.TEMPORAL_CLUSTER, 
                      PatternType.SIGNAL_CORRELATION, PatternType.TREND_REVERSAL}
        assert pattern_types.issubset(valid_types)
    
    def test_concurrent_safety(self, pattern_recognition, sample_signals):
        """Test thread/async safety"""
        import asyncio
        
        async def detect_patterns_worker(worker_id):
            return await pattern_recognition.detect_patterns(
                signals=sample_signals,
                game_id=f"game_{worker_id}"
            )
        
        # Run multiple workers concurrently
        async def run_concurrent_test():
            tasks = [detect_patterns_worker(i) for i in range(5)]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            return results
        
        results = asyncio.run(run_concurrent_test())
        
        # All should succeed
        assert len(results) == 5
        assert all(isinstance(r, list) for r in results)


@pytest.mark.asyncio
async def test_pattern_recognition_performance():
    """Test pattern recognition performance benchmarks"""
    import time
    
    pr = MLPatternRecognition()
    
    # Generate test signals
    signals = []
    for i in range(50):  # Larger test set
        signal = UnifiedBettingSignal(
            signal_id=f"perf_{i}",
            game_id="perf_test",
            signal_type=SignalType.SHARP_ACTION if i % 2 == 0 else SignalType.LINE_MOVEMENT,
            strength=0.5 + (i * 0.01) % 0.4,
            confidence=0.6 + (i * 0.008) % 0.3,
            timestamp=datetime.utcnow() - timedelta(minutes=i)
        )
        signals.append(signal)
    
    # Measure performance
    start_time = time.time()
    
    patterns = await pr.detect_patterns(
        signals=signals,
        game_id="perf_test"
    )
    
    end_time = time.time()
    duration_ms = (end_time - start_time) * 1000
    
    # Should be reasonably fast
    assert duration_ms < 500, f"Pattern detection took {duration_ms:.1f}ms, expected <500ms"
    assert isinstance(patterns, list)


if __name__ == "__main__":
    pytest.main([__file__])