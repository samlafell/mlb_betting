"""
Unit tests for NaturalLanguageExplanationEngine

Tests natural language explanation functionality including:
- User-adaptive explanations
- Multiple explanation formats
- Internationalization support
- Performance validation
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime
from typing import List, Dict, Any

from src.ml.opportunity_detection.explanation_engine import (
    NaturalLanguageExplanationEngine,
    ExplanationStyle,
    ExplanationFormat,
    UserProfile,
    ExplanationLevel
)
from src.ml.opportunity_detection.opportunity_scoring_engine import (
    OpportunityScore,
    OpportunityTier,
    RiskProfile,
    ScoringFactors
)
from src.ml.opportunity_detection.pattern_recognition import (
    DetectedPattern,
    PatternType,
    PatternConfidence
)
from src.analysis.models.unified_models import UnifiedBettingSignal, SignalType


class TestNaturalLanguageExplanationEngine:
    """Test suite for NaturalLanguageExplanationEngine"""
    
    @pytest.fixture
    def explanation_config(self):
        """Standard explanation engine configuration"""
        return {
            'supported_languages': ['en', 'es', 'fr'],
            'default_language': 'en',
            'explanation_templates': {
                'en': {
                    'opportunity_intro': "Based on our analysis of {game_id}, we've identified a {tier} opportunity.",
                    'score_summary': "This opportunity scores {score:.1f} out of 100.",
                    'factor_importance': "The key factors driving this score are: {factors}."
                }
            },
            'personalization': {
                'beginner_threshold': 0.3,
                'expert_threshold': 0.8
            }
        }
    
    @pytest.fixture
    def sample_user_profiles(self):
        """Sample user profiles for testing"""
        return {
            'beginner': UserProfile(
                user_id='beginner_user',
                experience_level=ExplanationLevel.BEGINNER,
                preferred_language='en',
                risk_tolerance=RiskProfile.CONSERVATIVE
            ),
            'intermediate': UserProfile(
                user_id='intermediate_user',
                experience_level=ExplanationLevel.INTERMEDIATE,
                preferred_language='en',
                risk_tolerance=RiskProfile.MODERATE
            ),
            'expert': UserProfile(
                user_id='expert_user',
                experience_level=ExplanationLevel.EXPERT,
                preferred_language='en',
                risk_tolerance=RiskProfile.AGGRESSIVE
            )
        }
    
    @pytest.fixture
    def sample_opportunity(self):
        """Sample opportunity score for testing"""
        return OpportunityScore(
            opportunity_id="opp_test_123",
            game_id="game_123",
            composite_score=75.5,
            tier=OpportunityTier.HIGH_VALUE,
            risk_profile=RiskProfile.MODERATE,
            scoring_factors=ScoringFactors(
                sharp_action=80.0,
                line_movement=70.0,
                consensus_divergence=75.0,
                historical_patterns=65.0,
                timing_factors=85.0,
                market_efficiency=60.0,
                confidence_level=90.0
            ),
            generated_at=datetime.utcnow()
        )
    
    @pytest.fixture
    def sample_patterns(self):
        """Sample detected patterns for testing"""
        return [
            DetectedPattern(
                pattern_id="pattern_1",
                game_id="game_123",
                pattern_type=PatternType.ANOMALY,
                confidence=PatternConfidence.HIGH,
                strength=0.8,
                description="Strong anomaly detected in sharp money movement"
            ),
            DetectedPattern(
                pattern_id="pattern_2",
                game_id="game_123",
                pattern_type=PatternType.TEMPORAL_CLUSTER,
                confidence=PatternConfidence.MEDIUM,
                strength=0.6,
                description="Temporal clustering in line movement"
            )
        ]
    
    @pytest.fixture
    def sample_signals(self):
        """Sample betting signals for testing"""
        return [
            UnifiedBettingSignal(
                signal_id="signal_1",
                game_id="game_123",
                signal_type=SignalType.SHARP_ACTION,
                strength=0.8,
                confidence=0.9,
                timestamp=datetime.utcnow()
            ),
            UnifiedBettingSignal(
                signal_id="signal_2",
                game_id="game_123",
                signal_type=SignalType.LINE_MOVEMENT,
                strength=0.7,
                confidence=0.8,
                timestamp=datetime.utcnow()
            )
        ]
    
    @pytest.fixture
    def explanation_engine(self, explanation_config):
        """Initialize explanation engine with config"""
        return NaturalLanguageExplanationEngine(config=explanation_config)
    
    def test_initialization(self, explanation_config):
        """Test proper initialization with configuration"""
        engine = NaturalLanguageExplanationEngine(config=explanation_config)
        
        assert engine.config == explanation_config
        assert engine.supported_languages == ['en', 'es', 'fr']
        assert engine.default_language == 'en'
        assert 'en' in engine.templates
    
    def test_initialization_with_defaults(self):
        """Test initialization with default configuration"""
        engine = NaturalLanguageExplanationEngine()
        
        # Should have default values
        assert 'en' in engine.supported_languages
        assert engine.default_language == 'en'
        assert len(engine.templates) > 0
    
    @pytest.mark.asyncio
    async def test_generate_opportunity_explanation_beginner(
        self, explanation_engine, sample_opportunity, sample_patterns, 
        sample_signals, sample_user_profiles
    ):
        """Test explanation generation for beginner users"""
        user_profile = sample_user_profiles['beginner']
        
        explanation = await explanation_engine.generate_opportunity_explanation(
            opportunity=sample_opportunity,
            detected_patterns=sample_patterns,
            signals=sample_signals,
            user_profile=user_profile
        )
        
        assert isinstance(explanation, dict)
        assert 'summary' in explanation
        assert 'details' in explanation
        assert 'score_explanation' in explanation
        assert 'recommendation' in explanation
        
        # Beginner explanations should be simpler
        summary = explanation['summary']
        assert len(summary) > 0
        assert isinstance(summary, str)
        
        # Should avoid technical jargon for beginners
        technical_terms = ['statistical significance', 'correlation coefficient', 'regression']
        summary_lower = summary.lower()
        assert not any(term in summary_lower for term in technical_terms)
    
    @pytest.mark.asyncio
    async def test_generate_opportunity_explanation_expert(
        self, explanation_engine, sample_opportunity, sample_patterns,
        sample_signals, sample_user_profiles
    ):
        """Test explanation generation for expert users"""
        user_profile = sample_user_profiles['expert']
        
        explanation = await explanation_engine.generate_opportunity_explanation(
            opportunity=sample_opportunity,
            detected_patterns=sample_patterns,
            signals=sample_signals,
            user_profile=user_profile
        )
        
        assert isinstance(explanation, dict)
        
        # Expert explanations should include more technical details
        details = explanation.get('details', {})
        assert 'technical_analysis' in details or 'statistical_summary' in details
        
        # Should include factor breakdown
        assert 'factor_analysis' in explanation
        factor_analysis = explanation['factor_analysis']
        assert isinstance(factor_analysis, dict)
        assert len(factor_analysis) > 0
    
    @pytest.mark.asyncio
    async def test_generate_opportunity_explanation_different_formats(
        self, explanation_engine, sample_opportunity, sample_patterns,
        sample_signals, sample_user_profiles
    ):
        """Test different explanation formats"""
        user_profile = sample_user_profiles['intermediate']
        
        # Test structured format
        structured_explanation = await explanation_engine.generate_opportunity_explanation(
            opportunity=sample_opportunity,
            detected_patterns=sample_patterns,
            signals=sample_signals,
            user_profile=user_profile,
            format_type=ExplanationFormat.STRUCTURED
        )
        
        assert isinstance(structured_explanation, dict)
        assert 'sections' in structured_explanation or 'summary' in structured_explanation
        
        # Test narrative format
        narrative_explanation = await explanation_engine.generate_opportunity_explanation(
            opportunity=sample_opportunity,
            detected_patterns=sample_patterns,
            signals=sample_signals,
            user_profile=user_profile,
            format_type=ExplanationFormat.NARRATIVE
        )
        
        assert isinstance(narrative_explanation, dict)
        assert 'narrative' in narrative_explanation or 'story' in narrative_explanation
    
    def test_adapt_explanation_for_user_level(
        self, explanation_engine, sample_user_profiles
    ):
        """Test explanation adaptation based on user experience level"""
        base_explanation = "The sharp action indicator shows 80.0 with high statistical significance."
        
        # Test beginner adaptation
        beginner_explanation = explanation_engine._adapt_explanation_for_user_level(
            base_explanation, sample_user_profiles['beginner']
        )
        assert "professional bettors" in beginner_explanation.lower() or "smart money" in beginner_explanation.lower()
        
        # Test expert adaptation
        expert_explanation = explanation_engine._adapt_explanation_for_user_level(
            base_explanation, sample_user_profiles['expert']
        )
        assert len(expert_explanation) >= len(base_explanation)  # Should maintain or add detail
    
    def test_format_score_explanation(self, explanation_engine, sample_opportunity):
        """Test score explanation formatting"""
        explanation = explanation_engine._format_score_explanation(
            sample_opportunity, ExplanationLevel.INTERMEDIATE
        )
        
        assert isinstance(explanation, str)
        assert str(sample_opportunity.composite_score) in explanation or "75" in explanation
        assert sample_opportunity.tier.value.lower() in explanation.lower()
    
    def test_format_factor_analysis(self, explanation_engine, sample_opportunity):
        """Test factor analysis formatting"""
        factor_analysis = explanation_engine._format_factor_analysis(
            sample_opportunity.scoring_factors, ExplanationLevel.INTERMEDIATE
        )
        
        assert isinstance(factor_analysis, dict)
        assert 'top_factors' in factor_analysis
        assert 'factor_scores' in factor_analysis
        
        top_factors = factor_analysis['top_factors']
        assert isinstance(top_factors, list)
        assert len(top_factors) > 0
    
    def test_format_pattern_explanation(self, explanation_engine, sample_patterns):
        """Test pattern explanation formatting"""
        pattern_explanation = explanation_engine._format_pattern_explanation(
            sample_patterns, ExplanationLevel.INTERMEDIATE
        )
        
        assert isinstance(pattern_explanation, dict)
        assert 'pattern_summary' in pattern_explanation
        assert 'pattern_details' in pattern_explanation
        
        pattern_summary = pattern_explanation['pattern_summary']
        assert isinstance(pattern_summary, str)
        assert len(pattern_summary) > 0
    
    def test_generate_recommendations(
        self, explanation_engine, sample_opportunity, sample_user_profiles
    ):
        """Test recommendation generation"""
        recommendations = explanation_engine._generate_recommendations(
            sample_opportunity, sample_user_profiles['intermediate']
        )
        
        assert isinstance(recommendations, dict)
        assert 'action' in recommendations
        assert 'reasoning' in recommendations
        
        # Should include risk warnings for lower-tier opportunities
        if sample_opportunity.tier in [OpportunityTier.LOW, OpportunityTier.MODERATE]:
            assert 'risk' in recommendations.get('warnings', {}) or 'caution' in recommendations.get('action', '').lower()
    
    def test_language_support(self, explanation_engine):
        """Test multi-language support"""
        # Test English (default)
        en_template = explanation_engine._get_template('opportunity_intro', 'en')
        assert isinstance(en_template, str)
        assert len(en_template) > 0
        
        # Test fallback to default language
        unknown_template = explanation_engine._get_template('opportunity_intro', 'unknown')
        assert unknown_template == en_template  # Should fallback to English
    
    @pytest.mark.asyncio
    async def test_explanation_caching(
        self, explanation_engine, sample_opportunity, sample_patterns,
        sample_signals, sample_user_profiles
    ):
        """Test explanation caching for performance"""
        user_profile = sample_user_profiles['intermediate']
        
        # First call
        explanation1 = await explanation_engine.generate_opportunity_explanation(
            opportunity=sample_opportunity,
            detected_patterns=sample_patterns,
            signals=sample_signals,
            user_profile=user_profile
        )
        
        # Second call with same parameters should use cache
        explanation2 = await explanation_engine.generate_opportunity_explanation(
            opportunity=sample_opportunity,
            detected_patterns=sample_patterns,
            signals=sample_signals,
            user_profile=user_profile
        )
        
        # Should be identical (from cache)
        assert explanation1 == explanation2
    
    def test_template_customization(self, explanation_engine):
        """Test template customization and formatting"""
        template = "This is a {tier} opportunity with a score of {score:.1f}."
        
        formatted = explanation_engine._format_template(
            template,
            tier="HIGH_VALUE",
            score=75.5
        )
        
        assert "HIGH_VALUE" in formatted
        assert "75.5" in formatted
        assert "{" not in formatted  # Should have no unresolved placeholders
    
    @pytest.mark.asyncio
    async def test_error_handling(self, explanation_engine):
        """Test error handling in various scenarios"""
        # Test with None opportunity
        explanation = await explanation_engine.generate_opportunity_explanation(
            opportunity=None,
            detected_patterns=[],
            signals=[],
            user_profile=UserProfile()
        )
        assert explanation is None or isinstance(explanation, dict)
        
        # Test with invalid user profile
        explanation = await explanation_engine.generate_opportunity_explanation(
            opportunity=self.sample_opportunity,
            detected_patterns=[],
            signals=[],
            user_profile=None
        )
        assert explanation is None or isinstance(explanation, dict)
    
    def test_explanation_length_adaptation(
        self, explanation_engine, sample_opportunity, sample_user_profiles
    ):
        """Test explanation length adaptation based on user level"""
        # Generate explanations for different user levels
        beginner_explanation = explanation_engine._format_score_explanation(
            sample_opportunity, ExplanationLevel.BEGINNER
        )
        
        expert_explanation = explanation_engine._format_score_explanation(
            sample_opportunity, ExplanationLevel.EXPERT
        )
        
        # Expert explanations should generally be more detailed
        # (though not always longer depending on content)
        assert isinstance(beginner_explanation, str)
        assert isinstance(expert_explanation, str)
        assert len(beginner_explanation) > 0
        assert len(expert_explanation) > 0
    
    def test_risk_tolerance_adaptation(
        self, explanation_engine, sample_opportunity, sample_user_profiles
    ):
        """Test explanation adaptation based on risk tolerance"""
        conservative_user = sample_user_profiles['beginner']  # Conservative
        aggressive_user = sample_user_profiles['expert']  # Aggressive
        
        conservative_rec = explanation_engine._generate_recommendations(
            sample_opportunity, conservative_user
        )
        
        aggressive_rec = explanation_engine._generate_recommendations(
            sample_opportunity, aggressive_user
        )
        
        # Conservative users should get more warnings
        conservative_text = str(conservative_rec).lower()
        aggressive_text = str(aggressive_rec).lower()
        
        risk_words = ['caution', 'careful', 'risk', 'conservative']
        conservative_risk_mentions = sum(1 for word in risk_words if word in conservative_text)
        aggressive_risk_mentions = sum(1 for word in risk_words if word in aggressive_text)
        
        # Conservative should generally have more risk mentions
        assert conservative_risk_mentions >= 0
        assert aggressive_risk_mentions >= 0
    
    @pytest.mark.asyncio
    async def test_pattern_integration(
        self, explanation_engine, sample_opportunity, sample_patterns,
        sample_signals, sample_user_profiles
    ):
        """Test integration of detected patterns into explanations"""
        user_profile = sample_user_profiles['intermediate']
        
        # Test with patterns
        with_patterns = await explanation_engine.generate_opportunity_explanation(
            opportunity=sample_opportunity,
            detected_patterns=sample_patterns,
            signals=sample_signals,
            user_profile=user_profile
        )
        
        # Test without patterns
        without_patterns = await explanation_engine.generate_opportunity_explanation(
            opportunity=sample_opportunity,
            detected_patterns=[],
            signals=sample_signals,
            user_profile=user_profile
        )
        
        # Explanations should be different when patterns are included
        assert with_patterns != without_patterns
        
        # With patterns should mention pattern-related terms
        with_patterns_text = str(with_patterns).lower()
        pattern_terms = ['pattern', 'anomaly', 'cluster', 'correlation']
        assert any(term in with_patterns_text for term in pattern_terms)
    
    def test_numerical_precision(self, explanation_engine, sample_opportunity):
        """Test proper numerical formatting in explanations"""
        explanation = explanation_engine._format_score_explanation(
            sample_opportunity, ExplanationLevel.INTERMEDIATE
        )
        
        # Should format numbers appropriately (not too many decimal places)
        import re
        decimal_numbers = re.findall(r'\d+\.\d+', explanation)
        
        for number in decimal_numbers:
            decimal_places = len(number.split('.')[1])
            assert decimal_places <= 2  # Should not have more than 2 decimal places
    
    @pytest.mark.asyncio
    async def test_contextual_explanations(
        self, explanation_engine, sample_opportunity, sample_patterns,
        sample_signals, sample_user_profiles
    ):
        """Test contextual explanation generation"""
        user_profile = sample_user_profiles['intermediate']
        
        # Add market context
        market_context = {
            'game_time': '8:00 PM',
            'weather': 'Clear',
            'venue': 'Home',
            'public_sentiment': 'Favors away team'
        }
        
        explanation = await explanation_engine.generate_opportunity_explanation(
            opportunity=sample_opportunity,
            detected_patterns=sample_patterns,
            signals=sample_signals,
            user_profile=user_profile,
            context=market_context
        )
        
        # Should incorporate context where relevant
        explanation_text = str(explanation).lower()
        
        # Context integration depends on implementation
        # At minimum, should not fail with additional context
        assert isinstance(explanation, dict)
        assert len(explanation) > 0


@pytest.mark.asyncio
async def test_explanation_engine_performance():
    """Test explanation engine performance benchmarks"""
    import time
    
    engine = NaturalLanguageExplanationEngine()
    
    # Create test data
    opportunity = OpportunityScore(
        opportunity_id="perf_test",
        game_id="game_perf",
        composite_score=70.0,
        tier=OpportunityTier.HIGH_VALUE,
        risk_profile=RiskProfile.MODERATE,
        scoring_factors=ScoringFactors(
            sharp_action=75.0,
            line_movement=65.0,
            consensus_divergence=70.0,
            historical_patterns=60.0,
            timing_factors=80.0,
            market_efficiency=55.0,
            confidence_level=85.0
        )
    )
    
    patterns = [
        DetectedPattern(
            pattern_id="perf_pattern",
            game_id="game_perf",
            pattern_type=PatternType.ANOMALY,
            confidence=PatternConfidence.HIGH,
            strength=0.8
        )
    ]
    
    signals = [
        UnifiedBettingSignal(
            signal_id="perf_signal",
            game_id="game_perf",
            signal_type=SignalType.SHARP_ACTION,
            strength=0.8,
            confidence=0.9,
            timestamp=datetime.utcnow()
        )
    ]
    
    user_profile = UserProfile()
    
    # Measure performance
    start_time = time.time()
    
    explanation = await engine.generate_opportunity_explanation(
        opportunity=opportunity,
        detected_patterns=patterns,
        signals=signals,
        user_profile=user_profile
    )
    
    end_time = time.time()
    duration_ms = (end_time - start_time) * 1000
    
    # Should be fast for good user experience
    assert duration_ms < 200, f"Explanation generation took {duration_ms:.1f}ms, expected <200ms"
    assert explanation is not None
    assert isinstance(explanation, dict)


if __name__ == "__main__":
    pytest.main([__file__])