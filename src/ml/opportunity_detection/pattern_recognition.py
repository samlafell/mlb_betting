"""
ML Pattern Recognition for Betting Opportunities

Advanced pattern recognition system that identifies profitable betting patterns using:
- Time series analysis of line movements
- Market sentiment clustering
- Behavioral pattern detection
- Anomaly detection in betting data
- Historical pattern matching

This module enhances opportunity detection by recognizing complex patterns
that traditional rule-based systems might miss.

Part of Issue #59: AI-Powered Betting Opportunity Discovery
"""

import asyncio
import logging
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, field
from enum import Enum
import warnings
warnings.filterwarnings('ignore', category=FutureWarning)

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans, DBSCAN
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest
from sklearn.decomposition import PCA
from sklearn.metrics.pairwise import cosine_similarity
from scipy import stats
from scipy.signal import find_peaks
import joblib

from src.analysis.models.unified_models import UnifiedBettingSignal, SignalType
from src.core.logging import get_logger


class PatternType(str, Enum):
    """Types of patterns that can be detected"""
    LINE_MOVEMENT_ANOMALY = "line_movement_anomaly"
    SHARP_MONEY_INFLUX = "sharp_money_influx"
    PUBLIC_FADE_SETUP = "public_fade_setup"
    CONTRARIAN_CLUSTER = "contrarian_cluster"
    STEAM_MOVE_PATTERN = "steam_move_pattern"
    REVERSE_LINE_MOVEMENT = "reverse_line_movement"
    CONSENSUS_BREAKDOWN = "consensus_breakdown"
    VALUE_DRIFT_PATTERN = "value_drift_pattern"
    TIMING_ANOMALY = "timing_anomaly"
    MULTI_BOOK_DIVERGENCE = "multi_book_divergence"


class PatternConfidence(str, Enum):
    """Pattern confidence levels"""
    VERY_HIGH = "very_high"  # 90%+
    HIGH = "high"            # 75-89%
    MEDIUM = "medium"        # 60-74%
    LOW = "low"              # 40-59%
    VERY_LOW = "very_low"    # <40%


@dataclass
class DetectedPattern:
    """A detected betting pattern with metadata"""
    pattern_id: str
    pattern_type: PatternType
    confidence: PatternConfidence
    confidence_score: float
    game_id: str
    
    # Pattern characteristics
    feature_vector: np.ndarray
    pattern_strength: float
    anomaly_score: float
    
    # Supporting data
    contributing_signals: List[str] = field(default_factory=list)
    historical_matches: List[str] = field(default_factory=list)
    market_conditions: Dict[str, Any] = field(default_factory=dict)
    
    # Pattern explanation
    description: str = ""
    key_indicators: List[str] = field(default_factory=list)
    risk_factors: List[str] = field(default_factory=list)
    
    # Timing and context
    detected_at: datetime = field(default_factory=datetime.utcnow)
    time_to_game: int = 0
    market_phase: str = "unknown"


class MLPatternRecognition:
    """
    ML-Powered Pattern Recognition System
    
    Uses multiple machine learning techniques to identify betting patterns:
    
    1. Anomaly Detection - Identifies unusual market behavior
    2. Clustering Analysis - Groups similar market conditions
    3. Time Series Patterns - Detects temporal betting patterns
    4. Historical Matching - Finds similar past scenarios
    5. Feature Engineering - Extracts meaningful pattern indicators
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the pattern recognition system"""
        self.config = config or {}
        self.logger = get_logger(__name__)
        
        # ML models (will be trained/loaded)
        self.anomaly_detector = None
        self.market_clusterer = None
        self.scaler = StandardScaler()
        
        # Pattern recognition parameters
        self.anomaly_threshold = self.config.get('anomaly_threshold', -0.1)
        self.cluster_eps = self.config.get('cluster_eps', 0.5)
        self.min_cluster_samples = self.config.get('min_cluster_samples', 3)
        self.historical_lookback_days = self.config.get('historical_lookback_days', 30)
        
        # Feature importance weights
        self.feature_weights = self.config.get('feature_weights', {
            'line_movement_velocity': 0.25,
            'volume_patterns': 0.20,
            'timing_patterns': 0.15,
            'consensus_deviation': 0.15,
            'market_sentiment': 0.12,
            'book_spread_patterns': 0.08,
            'historical_similarity': 0.05
        })
        
        # Pattern history for learning
        self.pattern_history = []
        self.performance_tracking = {}
        
        self.logger.info("MLPatternRecognition initialized with config: %s", 
                        {k: v for k, v in self.config.items() if k != 'feature_weights'})
    
    async def detect_patterns(self, 
                            signals: List[UnifiedBettingSignal],
                            game_id: str,
                            market_data: Optional[Dict[str, Any]] = None) -> List[DetectedPattern]:
        """
        Detect patterns in betting signals and market data
        
        Args:
            signals: List of betting signals to analyze
            game_id: Game identifier
            market_data: Additional market context data
            
        Returns:
            List of detected patterns with confidence scores
        """
        try:
            if not signals:
                self.logger.warning(f"No signals provided for pattern detection in game {game_id}")
                return []
            
            self.logger.info(f"Detecting patterns for game {game_id} with {len(signals)} signals")
            
            # Extract features from signals
            feature_matrix = await self._extract_pattern_features(signals, market_data or {})
            
            if feature_matrix.shape[0] == 0:
                self.logger.warning(f"No features extracted for game {game_id}")
                return []
            
            patterns = []
            
            # 1. Anomaly detection
            anomaly_patterns = await self._detect_anomaly_patterns(
                feature_matrix, signals, game_id
            )
            patterns.extend(anomaly_patterns)
            
            # 2. Clustering analysis
            cluster_patterns = await self._detect_cluster_patterns(
                feature_matrix, signals, game_id
            )
            patterns.extend(cluster_patterns)
            
            # 3. Time series pattern detection
            temporal_patterns = await self._detect_temporal_patterns(
                signals, game_id
            )
            patterns.extend(temporal_patterns)
            
            # 4. Market divergence patterns
            divergence_patterns = await self._detect_divergence_patterns(
                signals, game_id
            )
            patterns.extend(divergence_patterns)
            
            # 5. Historical pattern matching
            historical_patterns = await self._match_historical_patterns(
                feature_matrix, signals, game_id
            )
            patterns.extend(historical_patterns)
            
            # Deduplicate and rank patterns
            unique_patterns = self._deduplicate_patterns(patterns)
            ranked_patterns = self._rank_patterns_by_confidence(unique_patterns)
            
            self.logger.info(
                f"Detected {len(ranked_patterns)} unique patterns for game {game_id} "
                f"(from {len(patterns)} total detections)"
            )
            
            return ranked_patterns
            
        except Exception as e:
            self.logger.error(f"Error detecting patterns for game {game_id}: {e}", exc_info=True)
            return []
    
    async def _extract_pattern_features(self, 
                                      signals: List[UnifiedBettingSignal],
                                      market_data: Dict[str, Any]) -> np.ndarray:
        """Extract feature matrix for pattern recognition"""
        try:
            features = []
            
            for signal in signals:
                signal_features = []
                
                # Basic signal features
                signal_features.extend([
                    signal.confidence_score,
                    signal.signal_strength,
                    signal.minutes_to_game,
                    signal.quality_score,
                    len(signal.book_sources) if signal.book_sources else 0,
                ])
                
                # Signal type encoding (one-hot)
                signal_type_features = [0] * len(SignalType)
                if hasattr(SignalType, signal.signal_type.value.upper()):
                    type_index = list(SignalType).index(signal.signal_type)
                    signal_type_features[type_index] = 1
                signal_features.extend(signal_type_features)
                
                # Strategy-specific features
                strategy_data = signal.strategy_data or {}
                
                # Line movement features
                line_movement_velocity = strategy_data.get('line_movement_velocity', 0.0)
                line_movement_magnitude = strategy_data.get('line_movement_magnitude', 0.0)
                signal_features.extend([line_movement_velocity, line_movement_magnitude])
                
                # Volume features
                money_percentage = strategy_data.get('money_percentage', 50.0)
                bet_percentage = strategy_data.get('bet_percentage', 50.0)
                volume_differential = abs(money_percentage - bet_percentage)
                signal_features.extend([money_percentage, bet_percentage, volume_differential])
                
                # Timing features
                timing_category = signal.timing_category
                timing_features = self._encode_timing_category(timing_category)
                signal_features.extend(timing_features)
                
                # Consensus features
                consensus_strength = strategy_data.get('consensus_strength', 0.5)
                consensus_deviation = abs(0.5 - consensus_strength)
                signal_features.extend([consensus_strength, consensus_deviation])
                
                # Market features from additional data
                market_efficiency = market_data.get('market_efficiency', 0.5)
                book_spread = market_data.get('average_book_spread', 0.0)
                signal_features.extend([market_efficiency, book_spread])
                
                features.append(signal_features)
            
            # Convert to numpy array
            feature_matrix = np.array(features)
            
            # Handle any NaN values
            feature_matrix = np.nan_to_num(feature_matrix, nan=0.0)
            
            self.logger.debug(f"Extracted feature matrix shape: {feature_matrix.shape}")
            return feature_matrix
            
        except Exception as e:
            self.logger.error(f"Error extracting pattern features: {e}", exc_info=True)
            return np.array([])
    
    async def _detect_anomaly_patterns(self, 
                                     feature_matrix: np.ndarray,
                                     signals: List[UnifiedBettingSignal],
                                     game_id: str) -> List[DetectedPattern]:
        """Detect anomalous patterns using isolation forest"""
        try:
            if feature_matrix.shape[0] < 2:
                return []
            
            # Initialize anomaly detector if needed
            if self.anomaly_detector is None:
                self.anomaly_detector = IsolationForest(
                    contamination=0.1,
                    random_state=42,
                    n_estimators=100
                )
                
                # For single-sample scenarios, we need to create a baseline
                # In production, this would be trained on historical data
                baseline_features = self._create_baseline_features(feature_matrix)
                extended_matrix = np.vstack([feature_matrix, baseline_features])
                self.anomaly_detector.fit(extended_matrix)
            
            # Detect anomalies
            anomaly_scores = self.anomaly_detector.decision_function(feature_matrix)
            anomaly_labels = self.anomaly_detector.predict(feature_matrix)
            
            patterns = []
            
            for i, (score, label, signal) in enumerate(zip(anomaly_scores, anomaly_labels, signals)):
                if label == -1:  # Anomaly detected
                    # Determine pattern type based on signal characteristics
                    pattern_type = self._classify_anomaly_pattern(signal, score)
                    
                    # Calculate confidence based on anomaly score
                    confidence_score = min(0.95, abs(score) * 2)  # Scale to 0-1
                    confidence_level = self._score_to_confidence_level(confidence_score)
                    
                    pattern = DetectedPattern(
                        pattern_id=f"anomaly_{game_id}_{i}_{int(datetime.utcnow().timestamp())}",
                        pattern_type=pattern_type,
                        confidence=confidence_level,
                        confidence_score=confidence_score,
                        game_id=game_id,
                        feature_vector=feature_matrix[i],
                        pattern_strength=abs(score),
                        anomaly_score=score,
                        contributing_signals=[signal.signal_id],
                        description=self._generate_anomaly_description(pattern_type, signal, score),
                        key_indicators=self._extract_anomaly_indicators(signal, feature_matrix[i]),
                        time_to_game=signal.minutes_to_game,
                        market_phase=self._determine_market_phase(signal.minutes_to_game)
                    )
                    
                    patterns.append(pattern)
            
            self.logger.debug(f"Detected {len(patterns)} anomaly patterns for game {game_id}")
            return patterns
            
        except Exception as e:
            self.logger.error(f"Error detecting anomaly patterns: {e}", exc_info=True)
            return []
    
    async def _detect_cluster_patterns(self, 
                                     feature_matrix: np.ndarray,
                                     signals: List[UnifiedBettingSignal],
                                     game_id: str) -> List[DetectedPattern]:
        """Detect clustering patterns in market behavior"""
        try:
            if feature_matrix.shape[0] < 3:  # Need at least 3 points for clustering
                return []
            
            # Normalize features
            normalized_features = self.scaler.fit_transform(feature_matrix)
            
            # Apply DBSCAN clustering
            clusterer = DBSCAN(eps=self.cluster_eps, min_samples=self.min_cluster_samples)
            cluster_labels = clusterer.fit_predict(normalized_features)
            
            patterns = []
            unique_clusters = set(cluster_labels)
            
            for cluster_id in unique_clusters:
                if cluster_id == -1:  # Noise points
                    continue
                
                cluster_mask = cluster_labels == cluster_id
                cluster_signals = [s for i, s in enumerate(signals) if cluster_mask[i]]
                
                if len(cluster_signals) < 2:
                    continue
                
                # Analyze cluster characteristics
                cluster_features = normalized_features[cluster_mask]
                cluster_center = np.mean(cluster_features, axis=0)
                cluster_spread = np.std(cluster_features, axis=0).mean()
                
                # Determine pattern type based on cluster characteristics
                pattern_type = self._classify_cluster_pattern(cluster_signals, cluster_center)
                
                # Calculate confidence based on cluster cohesion
                confidence_score = max(0.1, 1.0 - cluster_spread)
                confidence_level = self._score_to_confidence_level(confidence_score)
                
                pattern = DetectedPattern(
                    pattern_id=f"cluster_{game_id}_{cluster_id}_{int(datetime.utcnow().timestamp())}",
                    pattern_type=pattern_type,
                    confidence=confidence_level,
                    confidence_score=confidence_score,
                    game_id=game_id,
                    feature_vector=cluster_center,
                    pattern_strength=1.0 - cluster_spread,
                    anomaly_score=0.0,
                    contributing_signals=[s.signal_id for s in cluster_signals],
                    description=self._generate_cluster_description(pattern_type, cluster_signals),
                    key_indicators=self._extract_cluster_indicators(cluster_signals),
                    time_to_game=int(np.mean([s.minutes_to_game for s in cluster_signals])),
                    market_phase=self._determine_market_phase(
                        int(np.mean([s.minutes_to_game for s in cluster_signals]))
                    )
                )
                
                patterns.append(pattern)
            
            self.logger.debug(f"Detected {len(patterns)} cluster patterns for game {game_id}")
            return patterns
            
        except Exception as e:
            self.logger.error(f"Error detecting cluster patterns: {e}", exc_info=True)
            return []
    
    async def _detect_temporal_patterns(self, 
                                      signals: List[UnifiedBettingSignal],
                                      game_id: str) -> List[DetectedPattern]:
        """Detect time-based patterns in signal sequence"""
        try:
            if len(signals) < 3:
                return []
            
            # Sort signals by time to game (descending - most recent first)
            sorted_signals = sorted(signals, key=lambda x: x.minutes_to_game, reverse=True)
            
            patterns = []
            
            # Extract time series of key metrics
            time_points = [s.minutes_to_game for s in sorted_signals]
            confidence_series = [s.confidence_score for s in sorted_signals]
            strength_series = [s.signal_strength for s in sorted_signals]
            
            # 1. Detect momentum patterns
            momentum_patterns = self._detect_momentum_patterns(
                time_points, confidence_series, strength_series, sorted_signals, game_id
            )
            patterns.extend(momentum_patterns)
            
            # 2. Detect reversal patterns  
            reversal_patterns = self._detect_reversal_patterns(
                time_points, confidence_series, strength_series, sorted_signals, game_id
            )
            patterns.extend(reversal_patterns)
            
            # 3. Detect acceleration patterns
            acceleration_patterns = self._detect_acceleration_patterns(
                time_points, confidence_series, strength_series, sorted_signals, game_id
            )
            patterns.extend(acceleration_patterns)
            
            self.logger.debug(f"Detected {len(patterns)} temporal patterns for game {game_id}")
            return patterns
            
        except Exception as e:
            self.logger.error(f"Error detecting temporal patterns: {e}", exc_info=True)
            return []
    
    async def _detect_divergence_patterns(self, 
                                        signals: List[UnifiedBettingSignal],
                                        game_id: str) -> List[DetectedPattern]:
        """Detect market divergence patterns"""
        try:
            if len(signals) < 2:
                return []
            
            patterns = []
            
            # Group signals by recommended side
            side_groups = {}
            for signal in signals:
                side = signal.recommended_side
                if side not in side_groups:
                    side_groups[side] = []
                side_groups[side].append(signal)
            
            # If we have opposing sides, analyze divergence
            if len(side_groups) >= 2:
                sides = list(side_groups.keys())
                for i in range(len(sides)):
                    for j in range(i + 1, len(sides)):
                        side_a, side_b = sides[i], sides[j]
                        group_a, group_b = side_groups[side_a], side_groups[side_b]
                        
                        # Calculate divergence metrics
                        avg_conf_a = np.mean([s.confidence_score for s in group_a])
                        avg_conf_b = np.mean([s.confidence_score for s in group_b])
                        avg_strength_a = np.mean([s.signal_strength for s in group_a])
                        avg_strength_b = np.mean([s.signal_strength for s in group_b])
                        
                        confidence_divergence = abs(avg_conf_a - avg_conf_b)
                        strength_divergence = abs(avg_strength_a - avg_strength_b)
                        
                        # If divergence is significant, create pattern
                        if confidence_divergence > 0.2 or strength_divergence > 0.3:
                            pattern_strength = (confidence_divergence + strength_divergence) / 2
                            
                            pattern = DetectedPattern(
                                pattern_id=f"divergence_{game_id}_{i}_{j}_{int(datetime.utcnow().timestamp())}",
                                pattern_type=PatternType.MULTI_BOOK_DIVERGENCE,
                                confidence=self._score_to_confidence_level(pattern_strength),
                                confidence_score=pattern_strength,
                                game_id=game_id,
                                feature_vector=np.array([confidence_divergence, strength_divergence]),
                                pattern_strength=pattern_strength,
                                anomaly_score=0.0,
                                contributing_signals=[s.signal_id for s in group_a + group_b],
                                description=f"Market divergence between {side_a} and {side_b} signals",
                                key_indicators=[
                                    f"Confidence divergence: {confidence_divergence:.2f}",
                                    f"Strength divergence: {strength_divergence:.2f}",
                                    f"Signal count: {len(group_a)} vs {len(group_b)}"
                                ],
                                time_to_game=int(np.mean([s.minutes_to_game for s in signals])),
                                market_phase=self._determine_market_phase(
                                    int(np.mean([s.minutes_to_game for s in signals]))
                                )
                            )
                            
                            patterns.append(pattern)
            
            self.logger.debug(f"Detected {len(patterns)} divergence patterns for game {game_id}")
            return patterns
            
        except Exception as e:
            self.logger.error(f"Error detecting divergence patterns: {e}", exc_info=True)
            return []
    
    async def _match_historical_patterns(self, 
                                       feature_matrix: np.ndarray,
                                       signals: List[UnifiedBettingSignal],
                                       game_id: str) -> List[DetectedPattern]:
        """Match current patterns against historical successful patterns"""
        try:
            # For now, implement a simplified version
            # In production, this would query a database of historical patterns
            patterns = []
            
            if feature_matrix.shape[0] == 0 or not hasattr(self, 'historical_patterns'):
                return patterns
            
            # TODO: Implement actual historical pattern matching
            # This would involve:
            # 1. Loading historical successful patterns from database
            # 2. Computing similarity scores (cosine similarity, euclidean distance)
            # 3. Identifying high-similarity matches
            # 4. Validating pattern performance in similar market conditions
            
            self.logger.debug(f"Historical pattern matching not yet implemented for game {game_id}")
            return patterns
            
        except Exception as e:
            self.logger.error(f"Error matching historical patterns: {e}", exc_info=True)
            return []
    
    # Helper methods
    
    def _encode_timing_category(self, timing_category: str) -> List[float]:
        """Encode timing category as features"""
        timing_categories = [
            "ULTRA_LATE", "CLOSING_HOUR", "CLOSING_2H", "LATE_AFTERNOON", 
            "SAME_DAY", "EARLY_24H", "OPENING_48H", "VERY_EARLY"
        ]
        
        features = [0.0] * len(timing_categories)
        if timing_category in timing_categories:
            index = timing_categories.index(timing_category)
            features[index] = 1.0
        
        return features
    
    def _create_baseline_features(self, feature_matrix: np.ndarray) -> np.ndarray:
        """Create baseline feature patterns for anomaly detection training"""
        # Create synthetic "normal" patterns based on the input data
        num_features = feature_matrix.shape[1]
        baseline_size = max(10, feature_matrix.shape[0] * 5)  # At least 10 baseline samples
        
        # Generate normal patterns around the mean with reasonable variation
        mean_features = np.mean(feature_matrix, axis=0)
        std_features = np.std(feature_matrix, axis=0)
        
        # Create baseline by adding small random variations to the mean
        baseline_features = np.random.normal(
            loc=mean_features,
            scale=std_features * 0.5,  # Smaller variation for "normal" patterns
            size=(baseline_size, num_features)
        )
        
        return baseline_features
    
    def _classify_anomaly_pattern(self, signal: UnifiedBettingSignal, anomaly_score: float) -> PatternType:
        """Classify the type of anomaly pattern based on signal characteristics"""
        # Simple classification logic - in production this would be more sophisticated
        if signal.signal_type == SignalType.LINE_MOVEMENT:
            return PatternType.LINE_MOVEMENT_ANOMALY
        elif signal.signal_type == SignalType.SHARP_ACTION:
            return PatternType.SHARP_MONEY_INFLUX
        elif signal.signal_type == SignalType.PUBLIC_FADE:
            return PatternType.PUBLIC_FADE_SETUP
        elif signal.minutes_to_game < 120:  # Within 2 hours of game
            return PatternType.STEAM_MOVE_PATTERN
        else:
            return PatternType.TIMING_ANOMALY
    
    def _classify_cluster_pattern(self, cluster_signals: List[UnifiedBettingSignal], cluster_center: np.ndarray) -> PatternType:
        """Classify cluster pattern based on signal characteristics"""
        # Analyze signal types in cluster
        signal_types = [s.signal_type for s in cluster_signals]
        
        if all(st == SignalType.SHARP_ACTION for st in signal_types):
            return PatternType.SHARP_MONEY_INFLUX
        elif SignalType.PUBLIC_FADE in signal_types:
            return PatternType.CONTRARIAN_CLUSTER
        elif all(s.recommended_side == cluster_signals[0].recommended_side for s in cluster_signals):
            return PatternType.CONSENSUS_BREAKDOWN
        else:
            return PatternType.MULTI_BOOK_DIVERGENCE
    
    def _detect_momentum_patterns(self, 
                                time_points: List[int],
                                confidence_series: List[float],
                                strength_series: List[float],
                                signals: List[UnifiedBettingSignal],
                                game_id: str) -> List[DetectedPattern]:
        """Detect momentum patterns in time series"""
        patterns = []
        
        if len(confidence_series) < 3:
            return patterns
        
        # Calculate momentum (rate of change)
        conf_momentum = np.diff(confidence_series)
        strength_momentum = np.diff(strength_series)
        
        # Detect sustained momentum (3+ consecutive increases)
        for i in range(len(conf_momentum) - 2):
            conf_trend = conf_momentum[i:i+3]
            strength_trend = strength_momentum[i:i+3]
            
            # Check for consistent positive momentum
            if all(x > 0.05 for x in conf_trend) and all(x > 0.05 for x in strength_trend):
                pattern_strength = np.mean(conf_trend + strength_trend)
                
                pattern = DetectedPattern(
                    pattern_id=f"momentum_{game_id}_{i}_{int(datetime.utcnow().timestamp())}",
                    pattern_type=PatternType.SHARP_MONEY_INFLUX,
                    confidence=self._score_to_confidence_level(pattern_strength),
                    confidence_score=pattern_strength,
                    game_id=game_id,
                    feature_vector=np.array(conf_trend + strength_trend),
                    pattern_strength=pattern_strength,
                    anomaly_score=0.0,
                    contributing_signals=[signals[i+j].signal_id for j in range(3)],
                    description="Sustained momentum pattern detected in confidence and strength",
                    key_indicators=["Sustained 3-period momentum increase"],
                    time_to_game=time_points[i+1],
                    market_phase=self._determine_market_phase(time_points[i+1])
                )
                
                patterns.append(pattern)
        
        return patterns
    
    def _detect_reversal_patterns(self, 
                                time_points: List[int],
                                confidence_series: List[float],
                                strength_series: List[float],
                                signals: List[UnifiedBettingSignal],
                                game_id: str) -> List[DetectedPattern]:
        """Detect reversal patterns in time series"""
        patterns = []
        
        if len(confidence_series) < 4:
            return patterns
        
        # Look for V-shaped or inverted V-shaped patterns
        for i in range(1, len(confidence_series) - 2):
            # Get 4-point window
            window = confidence_series[i-1:i+3]
            
            # Check for V-pattern (down then up)
            if (window[0] > window[1] and window[1] < window[2] and window[2] < window[3]):
                reversal_strength = (window[3] - window[1]) + (window[0] - window[1])
                
                pattern = DetectedPattern(
                    pattern_id=f"reversal_{game_id}_{i}_{int(datetime.utcnow().timestamp())}",
                    pattern_type=PatternType.REVERSE_LINE_MOVEMENT,
                    confidence=self._score_to_confidence_level(reversal_strength),
                    confidence_score=reversal_strength,
                    game_id=game_id,
                    feature_vector=np.array(window),
                    pattern_strength=reversal_strength,
                    anomaly_score=0.0,
                    contributing_signals=[signals[i+j-1].signal_id for j in range(4)],
                    description="Market reversal pattern - sentiment shift detected",
                    key_indicators=["V-shaped confidence pattern"],
                    time_to_game=time_points[i+1],
                    market_phase=self._determine_market_phase(time_points[i+1])
                )
                
                patterns.append(pattern)
        
        return patterns
    
    def _detect_acceleration_patterns(self, 
                                    time_points: List[int],
                                    confidence_series: List[float],
                                    strength_series: List[float],
                                    signals: List[UnifiedBettingSignal],
                                    game_id: str) -> List[DetectedPattern]:
        """Detect acceleration patterns (increasing rate of change)"""
        patterns = []
        
        if len(confidence_series) < 4:
            return patterns
        
        # Calculate first and second derivatives
        first_diff = np.diff(confidence_series)
        second_diff = np.diff(first_diff)
        
        # Look for acceleration (positive second derivative)
        for i in range(len(second_diff)):
            if second_diff[i] > 0.02:  # Significant acceleration
                acceleration_strength = second_diff[i]
                
                pattern = DetectedPattern(
                    pattern_id=f"acceleration_{game_id}_{i}_{int(datetime.utcnow().timestamp())}",
                    pattern_type=PatternType.STEAM_MOVE_PATTERN,
                    confidence=self._score_to_confidence_level(acceleration_strength),
                    confidence_score=acceleration_strength,
                    game_id=game_id,
                    feature_vector=np.array([first_diff[i], second_diff[i]]),
                    pattern_strength=acceleration_strength,
                    anomaly_score=0.0,
                    contributing_signals=[signals[i+j].signal_id for j in range(3)],
                    description="Accelerating market movement detected",
                    key_indicators=["Positive acceleration in confidence"],
                    time_to_game=time_points[i+2],
                    market_phase=self._determine_market_phase(time_points[i+2])
                )
                
                patterns.append(pattern)
        
        return patterns
    
    def _score_to_confidence_level(self, score: float) -> PatternConfidence:
        """Convert numerical score to confidence level"""
        if score >= 0.90:
            return PatternConfidence.VERY_HIGH
        elif score >= 0.75:
            return PatternConfidence.HIGH
        elif score >= 0.60:
            return PatternConfidence.MEDIUM
        elif score >= 0.40:
            return PatternConfidence.LOW
        else:
            return PatternConfidence.VERY_LOW
    
    def _determine_market_phase(self, minutes_to_game: int) -> str:
        """Determine market phase based on time to game"""
        if minutes_to_game <= 60:
            return "closing"
        elif minutes_to_game <= 240:
            return "late"
        elif minutes_to_game <= 1440:
            return "same_day"
        else:
            return "early"
    
    def _generate_anomaly_description(self, 
                                    pattern_type: PatternType,
                                    signal: UnifiedBettingSignal,
                                    anomaly_score: float) -> str:
        """Generate human-readable description for anomaly pattern"""
        descriptions = {
            PatternType.LINE_MOVEMENT_ANOMALY: f"Unusual line movement detected with anomaly score {abs(anomaly_score):.2f}",
            PatternType.SHARP_MONEY_INFLUX: f"Sharp money pattern anomaly - atypical betting behavior",
            PatternType.PUBLIC_FADE_SETUP: f"Contrarian setup with unusual characteristics",
            PatternType.STEAM_MOVE_PATTERN: f"Steam move anomaly detected close to game time",
            PatternType.TIMING_ANOMALY: f"Unusual timing pattern in {signal.timing_category} period"
        }
        
        return descriptions.get(pattern_type, f"Anomalous {pattern_type.value} pattern detected")
    
    def _generate_cluster_description(self, 
                                    pattern_type: PatternType,
                                    cluster_signals: List[UnifiedBettingSignal]) -> str:
        """Generate description for cluster pattern"""
        signal_count = len(cluster_signals)
        signal_types = list(set(s.signal_type.value for s in cluster_signals))
        
        return f"Cluster of {signal_count} similar signals ({', '.join(signal_types)}) indicating {pattern_type.value}"
    
    def _extract_anomaly_indicators(self, 
                                  signal: UnifiedBettingSignal,
                                  feature_vector: np.ndarray) -> List[str]:
        """Extract key indicators for anomaly pattern"""
        indicators = []
        
        indicators.append(f"Signal type: {signal.signal_type.value}")
        indicators.append(f"Confidence: {signal.confidence_score:.2f}")
        indicators.append(f"Strength: {signal.signal_strength:.2f}")
        indicators.append(f"Time to game: {signal.minutes_to_game} minutes")
        
        if signal.strategy_data:
            data = signal.strategy_data
            if 'money_percentage' in data and 'bet_percentage' in data:
                differential = abs(data['money_percentage'] - data['bet_percentage'])
                indicators.append(f"Money/bet differential: {differential:.1f}%")
        
        return indicators
    
    def _extract_cluster_indicators(self, cluster_signals: List[UnifiedBettingSignal]) -> List[str]:
        """Extract key indicators for cluster pattern"""
        indicators = []
        
        indicators.append(f"Cluster size: {len(cluster_signals)} signals")
        
        # Consensus indicators
        sides = [s.recommended_side for s in cluster_signals]
        unique_sides = list(set(sides))
        if len(unique_sides) == 1:
            indicators.append(f"Unanimous consensus: {unique_sides[0]}")
        else:
            indicators.append(f"Mixed signals: {len(unique_sides)} different sides")
        
        # Confidence statistics
        confidences = [s.confidence_score for s in cluster_signals]
        indicators.append(f"Avg confidence: {np.mean(confidences):.2f}")
        indicators.append(f"Confidence range: {np.min(confidences):.2f} - {np.max(confidences):.2f}")
        
        return indicators
    
    def _deduplicate_patterns(self, patterns: List[DetectedPattern]) -> List[DetectedPattern]:
        """Remove duplicate or very similar patterns"""
        if len(patterns) <= 1:
            return patterns
        
        unique_patterns = []
        
        for pattern in patterns:
            is_duplicate = False
            
            for existing in unique_patterns:
                # Check if patterns are too similar
                if (pattern.pattern_type == existing.pattern_type and
                    abs(pattern.confidence_score - existing.confidence_score) < 0.1 and
                    abs(pattern.time_to_game - existing.time_to_game) < 30):
                    
                    # Keep the pattern with higher confidence
                    if pattern.confidence_score > existing.confidence_score:
                        unique_patterns.remove(existing)
                    else:
                        is_duplicate = True
                    break
            
            if not is_duplicate:
                unique_patterns.append(pattern)
        
        return unique_patterns
    
    def _rank_patterns_by_confidence(self, patterns: List[DetectedPattern]) -> List[DetectedPattern]:
        """Rank patterns by confidence score (descending)"""
        return sorted(patterns, key=lambda p: p.confidence_score, reverse=True)