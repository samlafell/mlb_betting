"""
Natural Language Explanation Engine

Advanced NLP system that generates human-readable explanations for betting opportunities:
- Multi-factor explanation synthesis
- User-personalized communication
- Technical detail adaptation
- Risk communication
- Confidence explanation
- Market context integration

This engine translates complex ML and statistical analysis into clear,
actionable insights that users can understand and act upon.

Part of Issue #59: AI-Powered Betting Opportunity Discovery
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import re

from src.ml.opportunity_detection.opportunity_scoring_engine import OpportunityScore, OpportunityTier, RiskProfile, ScoringFactors
from src.ml.opportunity_detection.pattern_recognition import DetectedPattern, PatternType, PatternConfidence
from src.analysis.models.unified_models import UnifiedBettingSignal, SignalType, ConfidenceLevel
from src.core.logging import get_logger


class ExplanationStyle(str, Enum):
    """Different explanation styles for different user types"""
    BEGINNER = "beginner"        # Simple, educational explanations
    INTERMEDIATE = "intermediate"  # Balanced technical and simple
    ADVANCED = "advanced"        # Full technical detail
    PROFESSIONAL = "professional"  # Concise, data-focused
    MOBILE_BRIEF = "mobile_brief"  # Ultra-concise for mobile


class ExplanationFormat(str, Enum):
    """Output formats for explanations"""
    PARAGRAPH = "paragraph"      # Flowing text format
    BULLET_POINTS = "bullet_points"  # Structured list format
    STRUCTURED = "structured"    # JSON-like structured data
    NARRATIVE = "narrative"      # Story-like explanation


@dataclass
class ExplanationComponents:
    """Components of a comprehensive explanation"""
    # Core explanation
    headline: str = ""
    summary: str = ""
    detailed_reasoning: str = ""
    
    # Supporting elements
    key_factors: List[str] = field(default_factory=list)
    risk_assessment: str = ""
    confidence_explanation: str = ""
    market_context: str = ""
    
    # Actionable insights
    recommendation: str = ""
    stake_guidance: str = ""
    timing_advice: str = ""
    
    # Technical details (for advanced users)
    technical_analysis: str = ""
    statistical_basis: str = ""
    model_insights: str = ""
    
    # Warnings and disclaimers
    risk_warnings: List[str] = field(default_factory=list)
    limitations: List[str] = field(default_factory=list)


@dataclass
class UserProfile:
    """User profile for personalized explanations"""
    experience_level: ExplanationStyle = ExplanationStyle.INTERMEDIATE
    preferred_format: ExplanationFormat = ExplanationFormat.PARAGRAPH
    risk_tolerance: RiskProfile = RiskProfile.MODERATE
    technical_interest: bool = False
    brevity_preference: bool = False
    mobile_user: bool = False


class NaturalLanguageExplanationEngine:
    """
    Natural Language Explanation Engine
    
    Transforms complex betting analysis into clear, actionable explanations:
    
    1. Multi-Factor Analysis - Synthesizes multiple scoring factors into coherent narrative
    2. User Personalization - Adapts language and detail level to user experience
    3. Risk Communication - Clearly explains risks and uncertainties
    4. Confidence Explanation - Helps users understand confidence levels
    5. Market Context - Provides relevant market and timing context
    6. Actionable Insights - Translates analysis into clear recommendations
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the explanation engine"""
        self.config = config or {}
        self.logger = get_logger(__name__)
        
        # Language templates and patterns
        self._load_language_templates()
        
        # Explanation parameters
        self.max_paragraph_length = self.config.get('max_paragraph_length', 150)
        self.max_bullet_points = self.config.get('max_bullet_points', 5)
        self.technical_threshold = self.config.get('technical_threshold', 0.7)
        
        # Risk communication thresholds
        self.risk_thresholds = {
            'high_risk': 0.8,
            'moderate_risk': 0.5,
            'low_risk': 0.2
        }
        
        self.logger.info("NaturalLanguageExplanationEngine initialized")
    
    async def generate_opportunity_explanation(self,
                                             opportunity: OpportunityScore,
                                             detected_patterns: Optional[List[DetectedPattern]] = None,
                                             signals: Optional[List[UnifiedBettingSignal]] = None,
                                             user_profile: Optional[UserProfile] = None) -> Dict[str, Any]:
        """
        Generate comprehensive explanation for a betting opportunity
        
        Args:
            opportunity: Scored opportunity to explain
            detected_patterns: Optional ML-detected patterns
            signals: Optional underlying betting signals
            user_profile: User profile for personalization
            
        Returns:
            Dictionary with explanation components and formatted text
        """
        try:
            if not user_profile:
                user_profile = UserProfile()  # Use defaults
            
            self.logger.info(f"Generating explanation for opportunity {opportunity.opportunity_id}")
            
            # Build explanation components
            components = await self._build_explanation_components(
                opportunity, detected_patterns, signals, user_profile
            )
            
            # Format explanation based on user preferences
            formatted_explanation = await self._format_explanation(components, user_profile)
            
            # Generate metadata
            metadata = self._generate_explanation_metadata(opportunity, components, user_profile)
            
            result = {
                'explanation_id': f"exp_{opportunity.opportunity_id}_{int(datetime.utcnow().timestamp())}",
                'opportunity_id': opportunity.opportunity_id,
                'components': components,
                'formatted_text': formatted_explanation,
                'metadata': metadata,
                'generated_at': datetime.utcnow(),
                'user_profile': user_profile
            }
            
            self.logger.info(f"Generated explanation for {opportunity.opportunity_id} in {user_profile.experience_level} style")
            return result
            
        except Exception as e:
            self.logger.error(f"Error generating opportunity explanation: {e}", exc_info=True)
            return self._generate_fallback_explanation(opportunity, user_profile)
    
    async def generate_batch_explanations(self,
                                        opportunities: List[OpportunityScore],
                                        patterns_by_game: Optional[Dict[str, List[DetectedPattern]]] = None,
                                        signals_by_game: Optional[Dict[str, List[UnifiedBettingSignal]]] = None,
                                        user_profile: Optional[UserProfile] = None) -> List[Dict[str, Any]]:
        """
        Generate explanations for multiple opportunities in batch
        
        Args:
            opportunities: List of opportunities to explain
            patterns_by_game: Optional patterns grouped by game
            signals_by_game: Optional signals grouped by game
            user_profile: User profile for personalization
            
        Returns:
            List of explanation dictionaries
        """
        try:
            self.logger.info(f"Generating batch explanations for {len(opportunities)} opportunities")
            
            # Generate explanations in parallel
            tasks = []
            for opportunity in opportunities:
                game_patterns = patterns_by_game.get(opportunity.game_id, []) if patterns_by_game else None
                game_signals = signals_by_game.get(opportunity.game_id, []) if signals_by_game else None
                
                task = self.generate_opportunity_explanation(
                    opportunity, game_patterns, game_signals, user_profile
                )
                tasks.append(task)
            
            # Execute with reasonable concurrency
            batch_size = 5
            results = []
            
            for i in range(0, len(tasks), batch_size):
                batch_tasks = tasks[i:i + batch_size]
                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                
                for result in batch_results:
                    if isinstance(result, Exception):
                        self.logger.error(f"Batch explanation error: {result}")
                    else:
                        results.append(result)
            
            self.logger.info(f"Generated {len(results)} explanations successfully")
            return results
            
        except Exception as e:
            self.logger.error(f"Error generating batch explanations: {e}", exc_info=True)
            return []
    
    # Private methods for building explanations
    
    async def _build_explanation_components(self,
                                          opportunity: OpportunityScore,
                                          detected_patterns: Optional[List[DetectedPattern]],
                                          signals: Optional[List[UnifiedBettingSignal]],
                                          user_profile: UserProfile) -> ExplanationComponents:
        """Build all components of the explanation"""
        try:
            components = ExplanationComponents()
            
            # 1. Generate headline
            components.headline = self._generate_headline(opportunity, user_profile)
            
            # 2. Create summary
            components.summary = self._generate_summary(opportunity, user_profile)
            
            # 3. Explain key factors
            components.key_factors = self._explain_key_factors(
                opportunity.scoring_factors, opportunity.factor_weights, user_profile
            )
            
            # 4. Detailed reasoning
            components.detailed_reasoning = self._generate_detailed_reasoning(
                opportunity, detected_patterns, signals, user_profile
            )
            
            # 5. Risk assessment
            components.risk_assessment = self._generate_risk_assessment(opportunity, user_profile)
            
            # 6. Confidence explanation
            components.confidence_explanation = self._explain_confidence(opportunity, user_profile)
            
            # 7. Market context
            components.market_context = self._generate_market_context(
                opportunity, detected_patterns, user_profile
            )
            
            # 8. Recommendation and guidance
            components.recommendation = self._generate_recommendation(opportunity, user_profile)
            components.stake_guidance = self._generate_stake_guidance(opportunity, user_profile)
            components.timing_advice = self._generate_timing_advice(opportunity, user_profile)
            
            # 9. Technical details (for advanced users)
            if user_profile.technical_interest or user_profile.experience_level in [ExplanationStyle.ADVANCED, ExplanationStyle.PROFESSIONAL]:
                components.technical_analysis = self._generate_technical_analysis(
                    opportunity, detected_patterns, user_profile
                )
                components.statistical_basis = self._generate_statistical_basis(opportunity, user_profile)
                components.model_insights = self._generate_model_insights(opportunity, user_profile)
            
            # 10. Warnings and limitations
            components.risk_warnings = self._generate_risk_warnings(opportunity, user_profile)
            components.limitations = self._generate_limitations(opportunity, user_profile)
            
            return components
            
        except Exception as e:
            self.logger.error(f"Error building explanation components: {e}", exc_info=True)
            return ExplanationComponents()
    
    def _generate_headline(self, opportunity: OpportunityScore, user_profile: UserProfile) -> str:
        """Generate attention-grabbing headline"""
        try:
            tier_adjectives = {
                OpportunityTier.PREMIUM: "Premium",
                OpportunityTier.HIGH_VALUE: "High-Value",
                OpportunityTier.STANDARD: "Solid",
                OpportunityTier.LOW_GRADE: "Moderate"
            }
            
            confidence_descriptors = {
                ConfidenceLevel.VERY_HIGH: "High-Confidence",
                ConfidenceLevel.HIGH: "Strong",
                ConfidenceLevel.MEDIUM: "Promising",
                ConfidenceLevel.LOW: "Speculative",
                ConfidenceLevel.VERY_LOW: "Low-Confidence"
            }
            
            tier_adj = tier_adjectives.get(opportunity.tier, "")
            conf_desc = confidence_descriptors.get(opportunity.confidence_level, "")
            
            # Get game context if available
            game_context = ""
            if hasattr(opportunity, 'market_data') and opportunity.market_data:
                home_team = opportunity.market_data.get('home_team', '')
                away_team = opportunity.market_data.get('away_team', '')
                if home_team and away_team:
                    game_context = f" - {away_team} @ {home_team}"
            
            if user_profile.experience_level == ExplanationStyle.BEGINNER:
                return f"{tier_adj} Betting Opportunity{game_context}"
            elif user_profile.experience_level == ExplanationStyle.PROFESSIONAL:
                return f"{tier_adj} {conf_desc} Opportunity (Score: {opportunity.composite_score:.1f}){game_context}"
            else:
                return f"{tier_adj} {conf_desc} Betting Opportunity{game_context}"
                
        except Exception as e:
            self.logger.error(f"Error generating headline: {e}")
            return "Betting Opportunity Identified"
    
    def _generate_summary(self, opportunity: OpportunityScore, user_profile: UserProfile) -> str:
        """Generate concise summary of the opportunity"""
        try:
            # Base summary components
            score_desc = self._score_to_description(opportunity.composite_score)
            ev_desc = self._ev_to_description(opportunity.expected_value)
            
            if user_profile.experience_level == ExplanationStyle.BEGINNER:
                summary = (f"This {score_desc} betting opportunity has a composite score of "
                          f"{opportunity.composite_score:.1f} out of 100. {ev_desc}")
            
            elif user_profile.experience_level == ExplanationStyle.MOBILE_BRIEF:
                summary = (f"{score_desc.capitalize()} opportunity (Score: {opportunity.composite_score:.1f}, "
                          f"EV: {opportunity.expected_value:+.2%})")
            
            elif user_profile.experience_level == ExplanationStyle.PROFESSIONAL:
                summary = (f"Composite Score: {opportunity.composite_score:.1f} | "
                          f"Tier: {opportunity.tier.value} | "
                          f"EV: {opportunity.expected_value:+.2%} | "
                          f"Kelly: {opportunity.kelly_fraction:.3f}")
            
            else:  # INTERMEDIATE or ADVANCED
                summary = (f"This {score_desc} opportunity scores {opportunity.composite_score:.1f}/100 "
                          f"with an expected value of {opportunity.expected_value:+.2%}. "
                          f"The analysis suggests a {opportunity.kelly_fraction:.1%} Kelly fraction for optimal sizing.")
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error generating summary: {e}")
            return "Betting opportunity identified through multi-factor analysis."
    
    def _explain_key_factors(self,
                           scoring_factors: ScoringFactors,
                           factor_weights: Dict[str, float],
                           user_profile: UserProfile) -> List[str]:
        """Explain the key contributing factors"""
        try:
            factor_explanations = []
            
            # Get factor scores and sort by weighted contribution
            factors_list = [
                ('Strategy Performance', scoring_factors.strategy_performance, factor_weights.get('strategy_performance', 0)),
                ('ML Confidence', scoring_factors.ml_confidence, factor_weights.get('ml_confidence', 0)),
                ('Market Efficiency', scoring_factors.market_efficiency, factor_weights.get('market_efficiency', 0)),
                ('Data Quality', scoring_factors.data_quality, factor_weights.get('data_quality', 0)),
                ('Consensus Strength', scoring_factors.consensus_strength, factor_weights.get('consensus_strength', 0)),
                ('Timing Factor', scoring_factors.timing_factor, factor_weights.get('timing_factor', 0)),
                ('Value Potential', scoring_factors.value_potential, factor_weights.get('value_potential', 0)),
            ]
            
            # Sort by weighted contribution
            factors_list.sort(key=lambda x: x[1] * x[2], reverse=True)
            
            # Take top factors based on user preference
            max_factors = 3 if user_profile.brevity_preference else 5
            top_factors = factors_list[:max_factors]
            
            for factor_name, score, weight in top_factors:
                explanation = self._explain_individual_factor(factor_name, score, weight, user_profile)
                if explanation:
                    factor_explanations.append(explanation)
            
            return factor_explanations
            
        except Exception as e:
            self.logger.error(f"Error explaining key factors: {e}")
            return ["Multiple factors contribute to this opportunity assessment."]
    
    def _explain_individual_factor(self,
                                 factor_name: str,
                                 score: float,
                                 weight: float,
                                 user_profile: UserProfile) -> str:
        """Explain an individual scoring factor"""
        try:
            strength_desc = self._score_to_strength_description(score)
            
            factor_explanations = {
                'Strategy Performance': {
                    'beginner': f"{strength_desc} historical success rate for similar strategies",
                    'intermediate': f"{strength_desc} strategy performance based on historical data ({score:.1f}/100)",
                    'advanced': f"Strategy performance factor: {score:.1f}/100 (weight: {weight:.1%})",
                    'professional': f"Strategy: {score:.1f} (wt: {weight:.1%})"
                },
                'ML Confidence': {
                    'beginner': f"{strength_desc} machine learning prediction confidence",
                    'intermediate': f"{strength_desc} ML model confidence in the prediction ({score:.1f}/100)",
                    'advanced': f"ML confidence factor: {score:.1f}/100 based on model consensus (weight: {weight:.1%})",
                    'professional': f"ML: {score:.1f} (wt: {weight:.1%})"
                },
                'Market Efficiency': {
                    'beginner': f"{strength_desc} market conditions for finding value",
                    'intermediate': f"{strength_desc} market inefficiency signals ({score:.1f}/100)",
                    'advanced': f"Market efficiency assessment: {score:.1f}/100 indicating opportunity presence (weight: {weight:.1%})",
                    'professional': f"Market: {score:.1f} (wt: {weight:.1%})"
                },
                'Data Quality': {
                    'beginner': f"{strength_desc} data completeness and reliability",
                    'intermediate': f"{strength_desc} quality of underlying data ({score:.1f}/100)",
                    'advanced': f"Data quality score: {score:.1f}/100 reflecting completeness and freshness (weight: {weight:.1%})",
                    'professional': f"Data: {score:.1f} (wt: {weight:.1%})"
                },
                'Consensus Strength': {
                    'beginner': f"{strength_desc} agreement across different strategies",
                    'intermediate': f"{strength_desc} consensus among multiple analysis methods ({score:.1f}/100)",
                    'advanced': f"Cross-strategy consensus: {score:.1f}/100 indicating signal alignment (weight: {weight:.1%})",
                    'professional': f"Consensus: {score:.1f} (wt: {weight:.1%})"
                },
                'Timing Factor': {
                    'beginner': f"{strength_desc} timing for this type of bet",
                    'intermediate': f"{strength_desc} timing characteristics ({score:.1f}/100)",
                    'advanced': f"Timing factor: {score:.1f}/100 based on time-to-game analysis (weight: {weight:.1%})",
                    'professional': f"Timing: {score:.1f} (wt: {weight:.1%})"
                },
                'Value Potential': {
                    'beginner': f"{strength_desc} potential profit opportunity",
                    'intermediate': f"{strength_desc} expected value potential ({score:.1f}/100)",
                    'advanced': f"Value assessment: {score:.1f}/100 based on EV calculations (weight: {weight:.1%})",
                    'professional': f"Value: {score:.1f} (wt: {weight:.1%})"
                }
            }
            
            style_key = user_profile.experience_level.value
            if user_profile.mobile_user and style_key not in ['professional']:
                style_key = 'professional'  # Use concise format for mobile
            
            return factor_explanations.get(factor_name, {}).get(style_key, f"{factor_name}: {strength_desc}")
            
        except Exception as e:
            self.logger.error(f"Error explaining factor {factor_name}: {e}")
            return f"{factor_name}: {self._score_to_strength_description(score)}"
    
    def _generate_detailed_reasoning(self,
                                   opportunity: OpportunityScore,
                                   detected_patterns: Optional[List[DetectedPattern]],
                                   signals: Optional[List[UnifiedBettingSignal]],
                                   user_profile: UserProfile) -> str:
        """Generate detailed reasoning explanation"""
        try:
            reasoning_parts = []
            
            # Start with overall assessment
            if opportunity.composite_score >= 80:
                reasoning_parts.append("The analysis identifies this as an exceptional opportunity with multiple strong supporting factors.")
            elif opportunity.composite_score >= 65:
                reasoning_parts.append("This represents a high-quality betting opportunity with solid analytical support.")
            elif opportunity.composite_score >= 50:
                reasoning_parts.append("The analysis suggests a reasonable opportunity with moderate supporting evidence.")
            else:
                reasoning_parts.append("This is a lower-grade opportunity that requires careful consideration.")
            
            # Include pattern analysis if available
            if detected_patterns and len(detected_patterns) > 0:
                pattern_desc = self._describe_patterns(detected_patterns, user_profile)
                if pattern_desc:
                    reasoning_parts.append(pattern_desc)
            
            # Include signal analysis if available
            if signals and len(signals) > 0:
                signal_desc = self._describe_signals(signals, user_profile)
                if signal_desc:
                    reasoning_parts.append(signal_desc)
            
            # Add specific factor reasoning
            factors_reasoning = self._generate_factors_reasoning(opportunity, user_profile)
            if factors_reasoning:
                reasoning_parts.append(factors_reasoning)
            
            return " ".join(reasoning_parts)
            
        except Exception as e:
            self.logger.error(f"Error generating detailed reasoning: {e}")
            return "The opportunity is identified through comprehensive multi-factor analysis of market conditions and historical patterns."
    
    def _generate_risk_assessment(self, opportunity: OpportunityScore, user_profile: UserProfile) -> str:
        """Generate risk assessment explanation"""
        try:
            # Determine risk level based on multiple factors
            risk_indicators = []
            
            # Confidence-based risk
            if opportunity.confidence_level in [ConfidenceLevel.LOW, ConfidenceLevel.VERY_LOW]:
                risk_indicators.append("low prediction confidence")
            
            # EV-based risk
            if opportunity.expected_value < 0.01:
                risk_indicators.append("minimal expected value")
            
            # Kelly fraction risk
            if opportunity.kelly_fraction > 0.1:
                risk_indicators.append("high optimal bet size")
            elif opportunity.kelly_fraction < 0.01:
                risk_indicators.append("very small optimal bet size")
            
            # Tier-based assessment
            if opportunity.tier == OpportunityTier.LOW_GRADE:
                risk_level = "Higher"
                risk_desc = "This lower-tier opportunity carries increased uncertainty."
            elif opportunity.tier == OpportunityTier.PREMIUM:
                risk_level = "Lower" 
                risk_desc = "This premium opportunity shows strong analytical support, reducing risk."
            else:
                risk_level = "Moderate"
                risk_desc = "This opportunity carries typical betting risks."
            
            if user_profile.experience_level == ExplanationStyle.BEGINNER:
                return f"{risk_level} risk. {risk_desc} Remember that all sports betting involves risk of loss."
            elif user_profile.experience_level == ExplanationStyle.MOBILE_BRIEF:
                return f"{risk_level} risk ({opportunity.kelly_fraction:.1%} Kelly)"
            else:
                risk_details = f" Key considerations: {', '.join(risk_indicators)}." if risk_indicators else ""
                return f"Risk Assessment: {risk_level}. {risk_desc}{risk_details}"
                
        except Exception as e:
            self.logger.error(f"Error generating risk assessment: {e}")
            return "Standard betting risk applies - never bet more than you can afford to lose."
    
    def _explain_confidence(self, opportunity: OpportunityScore, user_profile: UserProfile) -> str:
        """Explain confidence level and what it means"""
        try:
            confidence_explanations = {
                ConfidenceLevel.VERY_HIGH: {
                    'description': 'Very High',
                    'meaning': 'multiple strong indicators align with high certainty',
                    'percentage': '90%+'
                },
                ConfidenceLevel.HIGH: {
                    'description': 'High', 
                    'meaning': 'strong supporting evidence with good consensus',
                    'percentage': '75-89%'
                },
                ConfidenceLevel.MEDIUM: {
                    'description': 'Medium',
                    'meaning': 'reasonable support but some uncertainty remains',
                    'percentage': '50-74%'
                },
                ConfidenceLevel.LOW: {
                    'description': 'Low',
                    'meaning': 'limited supporting evidence',
                    'percentage': '25-49%'
                },
                ConfidenceLevel.VERY_LOW: {
                    'description': 'Very Low',
                    'meaning': 'minimal supporting evidence',
                    'percentage': '<25%'
                }
            }
            
            conf_info = confidence_explanations.get(opportunity.confidence_level, {})
            
            if user_profile.experience_level == ExplanationStyle.BEGINNER:
                return (f"Confidence Level: {conf_info.get('description', 'Unknown')}. "
                       f"This means {conf_info.get('meaning', 'the analysis has mixed results')}.")
            elif user_profile.experience_level == ExplanationStyle.MOBILE_BRIEF:
                return f"{conf_info.get('description', 'Unknown')} confidence"
            else:
                return (f"Analysis Confidence: {conf_info.get('description', 'Unknown')} "
                       f"({conf_info.get('percentage', 'N/A')}) - "
                       f"{conf_info.get('meaning', 'mixed analytical results')}.")
                
        except Exception as e:
            self.logger.error(f"Error explaining confidence: {e}")
            return "Confidence level reflects the strength of analytical support for this opportunity."
    
    # Additional helper methods
    
    def _load_language_templates(self):
        """Load language templates and patterns"""
        # In a production system, these would be loaded from configuration files
        self.templates = {
            'risk_warnings': [
                "Sports betting involves risk of financial loss",
                "Past performance does not guarantee future results", 
                "Only bet what you can afford to lose",
                "Gambling can be addictive - seek help if needed"
            ],
            'limitations': [
                "Analysis based on historical data and current market conditions",
                "Market conditions can change rapidly",
                "No guarantee of accuracy or profitability",
                "External factors may affect outcomes"
            ]
        }
    
    def _score_to_description(self, score: float) -> str:
        """Convert numerical score to descriptive text"""
        if score >= 85:
            return "exceptional"
        elif score >= 70:
            return "strong"
        elif score >= 55:
            return "solid"
        elif score >= 40:
            return "modest"
        else:
            return "limited"
    
    def _score_to_strength_description(self, score: float) -> str:
        """Convert score to strength description"""
        if score >= 80:
            return "Very strong"
        elif score >= 65:
            return "Strong"
        elif score >= 50:
            return "Moderate"
        elif score >= 35:
            return "Weak"
        else:
            return "Very weak"
    
    def _ev_to_description(self, ev: float) -> str:
        """Convert expected value to descriptive text"""
        if ev >= 0.05:
            return "This suggests strong profit potential."
        elif ev >= 0.02:
            return "This indicates good profit potential."
        elif ev >= 0:
            return "This shows modest profit potential."
        else:
            return "This suggests limited profit potential."
    
    def _describe_patterns(self, patterns: List[DetectedPattern], user_profile: UserProfile) -> str:
        """Describe detected patterns in natural language"""
        try:
            if not patterns:
                return ""
            
            # Group patterns by type
            pattern_groups = {}
            for pattern in patterns:
                pattern_type = pattern.pattern_type
                if pattern_type not in pattern_groups:
                    pattern_groups[pattern_type] = []
                pattern_groups[pattern_type].append(pattern)
            
            descriptions = []
            
            for pattern_type, pattern_list in pattern_groups.items():
                if user_profile.experience_level == ExplanationStyle.BEGINNER:
                    desc = self._get_beginner_pattern_description(pattern_type, len(pattern_list))
                elif user_profile.experience_level == ExplanationStyle.PROFESSIONAL:
                    desc = f"{pattern_type.value}: {len(pattern_list)} detected"
                else:
                    desc = self._get_standard_pattern_description(pattern_type, pattern_list)
                
                descriptions.append(desc)
            
            if user_profile.brevity_preference:
                return f"Patterns detected: {', '.join(descriptions[:2])}."
            else:
                return f"Pattern analysis reveals: {', '.join(descriptions)}."
                
        except Exception as e:
            self.logger.error(f"Error describing patterns: {e}")
            return "Multiple market patterns support this opportunity."
    
    def _describe_signals(self, signals: List[UnifiedBettingSignal], user_profile: UserProfile) -> str:
        """Describe betting signals in natural language"""
        try:
            if not signals:
                return ""
            
            signal_count = len(signals)
            signal_types = list(set(s.signal_type.value for s in signals))
            avg_confidence = sum(s.confidence_score for s in signals) / signal_count
            
            # Check for consensus
            sides = [s.recommended_side for s in signals]
            consensus_side = max(set(sides), key=sides.count) if sides else "unknown"
            consensus_strength = sides.count(consensus_side) / len(sides) if sides else 0
            
            if user_profile.experience_level == ExplanationStyle.BEGINNER:
                if consensus_strength >= 0.8:
                    return f"Strong agreement across {signal_count} different analysis methods favoring {consensus_side}."
                else:
                    return f"Mixed signals from {signal_count} analysis methods require careful consideration."
            
            elif user_profile.experience_level == ExplanationStyle.PROFESSIONAL:
                return f"Signals: {signal_count} ({', '.join(signal_types[:3])}), consensus: {consensus_strength:.1%}"
            
            else:
                consensus_desc = "strong" if consensus_strength >= 0.8 else "moderate" if consensus_strength >= 0.6 else "weak"
                return (f"Analysis incorporates {signal_count} signals including {', '.join(signal_types[:3])}, "
                       f"showing {consensus_desc} consensus for {consensus_side} with average confidence of {avg_confidence:.1%}.")
                
        except Exception as e:
            self.logger.error(f"Error describing signals: {e}")
            return f"Analysis based on {len(signals)} different betting signals."
    
    def _get_beginner_pattern_description(self, pattern_type: PatternType, count: int) -> str:
        """Get beginner-friendly pattern descriptions"""
        descriptions = {
            PatternType.LINE_MOVEMENT_ANOMALY: f"unusual line movement patterns ({count})",
            PatternType.SHARP_MONEY_INFLUX: f"professional money indicators ({count})", 
            PatternType.PUBLIC_FADE_SETUP: f"contrarian betting opportunities ({count})",
            PatternType.STEAM_MOVE_PATTERN: f"rapid market movements ({count})",
            PatternType.REVERSE_LINE_MOVEMENT: f"line movement against public betting ({count})"
        }
        return descriptions.get(pattern_type, f"{pattern_type.value} patterns ({count})")
    
    def _get_standard_pattern_description(self, pattern_type: PatternType, pattern_list: List[DetectedPattern]) -> str:
        """Get standard pattern descriptions"""
        count = len(pattern_list)
        avg_confidence = sum(p.confidence_score for p in pattern_list) / count
        
        base_descriptions = {
            PatternType.LINE_MOVEMENT_ANOMALY: f"line movement anomalies",
            PatternType.SHARP_MONEY_INFLUX: f"sharp money patterns",
            PatternType.PUBLIC_FADE_SETUP: f"public fade setups",
            PatternType.STEAM_MOVE_PATTERN: f"steam move patterns",
            PatternType.REVERSE_LINE_MOVEMENT: f"reverse line movement"
        }
        
        base = base_descriptions.get(pattern_type, pattern_type.value)
        return f"{count} {base} (avg confidence: {avg_confidence:.1%})"
    
    def _generate_factors_reasoning(self, opportunity: OpportunityScore, user_profile: UserProfile) -> str:
        """Generate reasoning based on scoring factors"""
        try:
            factors = opportunity.scoring_factors
            
            # Identify strongest factors
            strong_factors = []
            if factors.strategy_performance >= 70:
                strong_factors.append("strong historical strategy performance")
            if factors.ml_confidence >= 70:
                strong_factors.append("high ML prediction confidence")
            if factors.consensus_strength >= 70:
                strong_factors.append("strong cross-method consensus")
            if factors.timing_factor >= 70:
                strong_factors.append("favorable timing characteristics")
            
            # Identify weak factors as risks
            weak_factors = []
            if factors.data_quality < 50:
                weak_factors.append("limited data quality")
            if factors.market_efficiency < 40:
                weak_factors.append("efficient market conditions")
            
            reasoning_parts = []
            
            if strong_factors:
                if user_profile.experience_level == ExplanationStyle.BEGINNER:
                    reasoning_parts.append(f"The opportunity is supported by {', '.join(strong_factors)}.")
                else:
                    reasoning_parts.append(f"Key strengths include {', '.join(strong_factors)}.")
            
            if weak_factors and user_profile.experience_level != ExplanationStyle.MOBILE_BRIEF:
                reasoning_parts.append(f"Consider {', '.join(weak_factors)} as limiting factors.")
            
            return " ".join(reasoning_parts)
            
        except Exception as e:
            self.logger.error(f"Error generating factors reasoning: {e}")
            return ""
    
    def _generate_recommendation(self, opportunity: OpportunityScore, user_profile: UserProfile) -> str:
        """Generate betting recommendation"""
        try:
            if opportunity.tier == OpportunityTier.PREMIUM:
                if user_profile.experience_level == ExplanationStyle.BEGINNER:
                    return "This is a high-quality opportunity worth strong consideration for experienced bettors."
                else:
                    return "Strong recommendation - consider standard position sizing."
            
            elif opportunity.tier == OpportunityTier.HIGH_VALUE:
                return "Good opportunity - consider moderate position sizing based on your bankroll management strategy."
            
            elif opportunity.tier == OpportunityTier.STANDARD:
                return "Reasonable opportunity - suitable for small to moderate position sizes."
            
            else:  # LOW_GRADE
                return "Proceed with caution - consider only small position sizes or skip this opportunity."
                
        except Exception as e:
            self.logger.error(f"Error generating recommendation: {e}")
            return "Assess based on your risk tolerance and bankroll management strategy."
    
    def _generate_stake_guidance(self, opportunity: OpportunityScore, user_profile: UserProfile) -> str:
        """Generate stake sizing guidance"""
        try:
            kelly = opportunity.kelly_fraction
            risk_profile_multiplier = {
                RiskProfile.CONSERVATIVE: 0.5,
                RiskProfile.MODERATE: 0.75,
                RiskProfile.AGGRESSIVE: 1.0
            }.get(opportunity.risk_profile, 0.75)
            
            suggested_kelly = kelly * risk_profile_multiplier
            
            if user_profile.experience_level == ExplanationStyle.BEGINNER:
                if suggested_kelly >= 0.05:
                    return "Consider a larger portion of your betting bankroll for this opportunity."
                elif suggested_kelly >= 0.02:
                    return "Suitable for a moderate portion of your betting bankroll."
                else:
                    return "Consider only a small portion of your betting bankroll."
            
            elif user_profile.experience_level == ExplanationStyle.PROFESSIONAL:
                return f"Suggested Kelly: {suggested_kelly:.2%} (risk-adjusted)"
            
            else:
                if suggested_kelly >= 0.05:
                    return f"Kelly Criterion suggests {suggested_kelly:.1%} of bankroll (higher confidence opportunity)."
                else:
                    return f"Kelly Criterion suggests {suggested_kelly:.1%} of bankroll (conservative sizing recommended)."
                    
        except Exception as e:
            self.logger.error(f"Error generating stake guidance: {e}")
            return "Follow your established bankroll management rules."
    
    def _generate_timing_advice(self, opportunity: OpportunityScore, user_profile: UserProfile) -> str:
        """Generate timing advice"""
        try:
            if hasattr(opportunity, 'time_to_game'):
                minutes = opportunity.time_to_game
            else:
                minutes = 240  # Default assumption
            
            if minutes <= 60:
                return "Close to game time - act quickly if interested, but be aware of limited reaction time."
            elif minutes <= 240:
                return "Good timing window - allows for additional research while maintaining value."
            elif minutes <= 1440:  # Same day
                return "Reasonable timing - monitor for any late developments that might affect the opportunity."
            else:
                return "Early opportunity - consider monitoring for line movement and additional information."
                
        except Exception as e:
            self.logger.error(f"Error generating timing advice: {e}")
            return "Consider timing in relation to your betting strategy."
    
    def _generate_technical_analysis(self, 
                                   opportunity: OpportunityScore,
                                   detected_patterns: Optional[List[DetectedPattern]],
                                   user_profile: UserProfile) -> str:
        """Generate technical analysis for advanced users"""
        try:
            technical_parts = []
            
            # Scoring breakdown
            factors = opportunity.scoring_factors
            weights = opportunity.factor_weights
            
            technical_parts.append(
                f"Scoring Breakdown: "
                f"Strategy({factors.strategy_performance:.1f}×{weights.get('strategy_performance', 0):.2f}) + "
                f"ML({factors.ml_confidence:.1f}×{weights.get('ml_confidence', 0):.2f}) + "
                f"Market({factors.market_efficiency:.1f}×{weights.get('market_efficiency', 0):.2f}) + "
                f"Consensus({factors.consensus_strength:.1f}×{weights.get('consensus_strength', 0):.2f})"
            )
            
            # Pattern analysis
            if detected_patterns:
                pattern_summary = f"Patterns: {len(detected_patterns)} detected, "
                high_conf_patterns = sum(1 for p in detected_patterns if p.confidence_score > 0.7)
                pattern_summary += f"{high_conf_patterns} high-confidence"
                technical_parts.append(pattern_summary)
            
            # Risk metrics
            technical_parts.append(
                f"Risk Metrics: EV={opportunity.expected_value:+.3f}, "
                f"Kelly={opportunity.kelly_fraction:.3f}, "
                f"Confidence={opportunity.confidence_level.value}"
            )
            
            return " | ".join(technical_parts)
            
        except Exception as e:
            self.logger.error(f"Error generating technical analysis: {e}")
            return "Technical details available upon request."
    
    def _generate_statistical_basis(self, opportunity: OpportunityScore, user_profile: UserProfile) -> str:
        """Generate statistical basis explanation"""
        # This would be expanded with actual statistical data in production
        return ("Statistical analysis based on multi-factor scoring algorithm with "
                f"{len(opportunity.factor_weights)} weighted factors and "
                f"confidence intervals derived from historical performance data.")
    
    def _generate_model_insights(self, opportunity: OpportunityScore, user_profile: UserProfile) -> str:
        """Generate ML model insights"""
        try:
            ml_data = opportunity.ml_predictions
            if not ml_data:
                return "No ML model data available for this opportunity."
            
            insights = []
            if 'home_ml_confidence' in ml_data:
                insights.append(f"ML Home Win Confidence: {ml_data['home_ml_confidence']:.1%}")
            if 'total_over_confidence' in ml_data:
                insights.append(f"ML Total Over Confidence: {ml_data['total_over_confidence']:.1%}")
            
            return " | ".join(insights) if insights else "ML model insights not available."
            
        except Exception as e:
            self.logger.error(f"Error generating model insights: {e}")
            return "ML model insights not available."
    
    def _generate_risk_warnings(self, opportunity: OpportunityScore, user_profile: UserProfile) -> List[str]:
        """Generate appropriate risk warnings"""
        warnings = []
        
        # Always include basic warning
        warnings.append("Sports betting involves risk of financial loss")
        
        # Specific warnings based on opportunity characteristics
        if opportunity.confidence_level in [ConfidenceLevel.LOW, ConfidenceLevel.VERY_LOW]:
            warnings.append("Low confidence analysis - higher uncertainty")
        
        if opportunity.expected_value < 0.01:
            warnings.append("Limited expected value - minimal profit potential")
        
        if opportunity.kelly_fraction > 0.1:
            warnings.append("High suggested bet size - use extreme caution")
        
        # Risk profile specific warnings
        if opportunity.risk_profile == RiskProfile.AGGRESSIVE:
            warnings.append("Aggressive risk profile - only for experienced bettors")
        
        return warnings[:3]  # Limit to 3 warnings to avoid overwhelming
    
    def _generate_limitations(self, opportunity: OpportunityScore, user_profile: UserProfile) -> List[str]:
        """Generate analysis limitations"""
        limitations = ["Analysis based on historical data and current market conditions"]
        
        if not opportunity.ml_predictions:
            limitations.append("No ML model predictions available")
        
        if hasattr(opportunity, 'data_quality_score') and opportunity.data_quality_score < 0.8:
            limitations.append("Limited data quality may affect accuracy")
        
        limitations.append("Market conditions can change rapidly")
        
        return limitations[:2]  # Keep it concise
    
    async def _format_explanation(self, components: ExplanationComponents, user_profile: UserProfile) -> str:
        """Format explanation based on user preferences"""
        try:
            if user_profile.preferred_format == ExplanationFormat.BULLET_POINTS:
                return self._format_as_bullets(components, user_profile)
            elif user_profile.preferred_format == ExplanationFormat.STRUCTURED:
                return self._format_as_structured(components, user_profile)
            elif user_profile.preferred_format == ExplanationFormat.NARRATIVE:
                return self._format_as_narrative(components, user_profile)
            else:  # PARAGRAPH
                return self._format_as_paragraphs(components, user_profile)
                
        except Exception as e:
            self.logger.error(f"Error formatting explanation: {e}")
            return self._format_as_paragraphs(components, user_profile)
    
    def _format_as_bullets(self, components: ExplanationComponents, user_profile: UserProfile) -> str:
        """Format as bullet points"""
        bullets = []
        
        bullets.append(f"• {components.headline}")
        bullets.append(f"• {components.summary}")
        
        if components.key_factors:
            bullets.append("• Key Factors:")
            for factor in components.key_factors[:3]:  # Limit for readability
                bullets.append(f"  - {factor}")
        
        bullets.append(f"• {components.confidence_explanation}")
        bullets.append(f"• {components.risk_assessment}")
        bullets.append(f"• {components.recommendation}")
        
        if user_profile.experience_level != ExplanationStyle.BEGINNER:
            bullets.append(f"• {components.stake_guidance}")
        
        return "\n".join(bullets)
    
    def _format_as_paragraphs(self, components: ExplanationComponents, user_profile: UserProfile) -> str:
        """Format as flowing paragraphs"""
        paragraphs = []
        
        # Opening paragraph
        paragraphs.append(f"{components.headline}. {components.summary}")
        
        # Analysis paragraph
        if components.detailed_reasoning:
            paragraphs.append(components.detailed_reasoning)
        
        # Key factors
        if components.key_factors:
            factors_text = "Key supporting factors include: " + "; ".join(components.key_factors[:4]) + "."
            paragraphs.append(factors_text)
        
        # Risk and confidence
        risk_conf_para = f"{components.confidence_explanation} {components.risk_assessment}"
        paragraphs.append(risk_conf_para)
        
        # Recommendations
        rec_para = f"{components.recommendation}"
        if user_profile.experience_level != ExplanationStyle.BEGINNER:
            rec_para += f" {components.stake_guidance}"
        paragraphs.append(rec_para)
        
        # Timing
        if components.timing_advice:
            paragraphs.append(components.timing_advice)
        
        return "\n\n".join(paragraphs)
    
    def _format_as_structured(self, components: ExplanationComponents, user_profile: UserProfile) -> str:
        """Format as structured data"""
        # This would return a JSON-like structure in production
        return f"""
OPPORTUNITY: {components.headline}
SUMMARY: {components.summary}
CONFIDENCE: {components.confidence_explanation}
RISK: {components.risk_assessment}
RECOMMENDATION: {components.recommendation}
STAKE: {components.stake_guidance}
TIMING: {components.timing_advice}
"""
    
    def _format_as_narrative(self, components: ExplanationComponents, user_profile: UserProfile) -> str:
        """Format as narrative story"""
        narrative_parts = [
            f"Here's what our analysis reveals about this betting opportunity: {components.summary}",
            components.detailed_reasoning,
            f"Looking at the confidence level, {components.confidence_explanation.lower()}",
            f"From a risk perspective, {components.risk_assessment.lower()}",
            f"Our recommendation: {components.recommendation.lower()}",
        ]
        
        return " ".join(filter(None, narrative_parts))
    
    def _generate_explanation_metadata(self, 
                                     opportunity: OpportunityScore,
                                     components: ExplanationComponents,
                                     user_profile: UserProfile) -> Dict[str, Any]:
        """Generate metadata about the explanation"""
        return {
            'word_count': len(components.summary.split()) + len(components.detailed_reasoning.split()),
            'complexity_level': user_profile.experience_level.value,
            'format': user_profile.preferred_format.value,
            'includes_technical': bool(components.technical_analysis),
            'risk_warnings_count': len(components.risk_warnings),
            'confidence_tier': opportunity.confidence_level.value,
            'opportunity_tier': opportunity.tier.value
        }
    
    def _generate_fallback_explanation(self, opportunity: OpportunityScore, user_profile: UserProfile) -> Dict[str, Any]:
        """Generate minimal fallback explanation in case of errors"""
        return {
            'explanation_id': f"fallback_{opportunity.opportunity_id}",
            'opportunity_id': opportunity.opportunity_id,
            'components': ExplanationComponents(
                headline=f"Betting Opportunity (Score: {opportunity.composite_score:.1f})",
                summary=f"This opportunity scores {opportunity.composite_score:.1f}/100 with {opportunity.tier.value} tier rating.",
                recommendation="Review based on your risk tolerance and betting strategy.",
                risk_assessment="Standard betting risks apply.",
                confidence_explanation=f"Analysis confidence: {opportunity.confidence_level.value}"
            ),
            'formatted_text': f"Betting opportunity identified with score of {opportunity.composite_score:.1f}/100. Review based on your strategy.",
            'metadata': {'is_fallback': True},
            'generated_at': datetime.utcnow(),
            'user_profile': user_profile
        }