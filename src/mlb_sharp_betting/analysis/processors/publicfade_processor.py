"""
Public Fade Processor

Identifies heavy public betting consensus as contrarian betting opportunities.
Tests the hypothesis that when there's way too much money on one side across 
multiple books, it's often a fade signal.

Converts public_money_fade_strategy_postgres.sql logic to Python processor.
"""

from typing import List, Dict, Any
from datetime import datetime
import json

from .base_strategy_processor import BaseStrategyProcessor
from ...models.betting_analysis import BettingSignal, SignalType, ProfitableStrategy


class PublicFadeProcessor(BaseStrategyProcessor):
    """
    Processor for detecting public fade signals
    
    Identifies games where heavy public money consensus across multiple books
    creates contrarian betting opportunities (fade the public).
    """
    
    def get_signal_type(self) -> SignalType:
        """Return the signal type this processor handles"""
        return SignalType.PUBLIC_FADE
    
    def get_strategy_category(self) -> str:
        """Return strategy category for proper routing"""
        return "CONTRARIAN_BETTING"
    
    def get_required_tables(self) -> List[str]:
        """Return database tables required for this strategy"""
        return ["splits.raw_mlb_betting_splits"]
    
    def get_strategy_description(self) -> str:
        """Return human-readable description of the strategy"""
        return "Identifies heavy public betting consensus across multiple books as contrarian fade opportunities"
    
    def validate_strategy_data(self, raw_data: List[Dict]) -> bool:
        """Validate we have multi-book public betting data"""
        if not raw_data:
            return False
            
        # Check we have stake and bet percentages from multiple sources
        sources = set(row.get('source') for row in raw_data if row.get('source'))
        has_percentages = any(
            row.get('home_or_over_stake_percentage') is not None and 
            row.get('home_or_over_bets_percentage') is not None 
            for row in raw_data
        )
        
        return len(sources) >= 1 and has_percentages
    
    async def process(self, minutes_ahead: int, 
                     profitable_strategies: List[ProfitableStrategy]) -> List[BettingSignal]:
        """Process public fade signals using profitable strategies"""
        start_time, end_time = self._create_time_window(minutes_ahead)
        
        # Get public betting data
        public_data = await self.repository.get_public_betting_data(start_time, end_time)
        
        if not public_data:
            self.logger.info("Insufficient public betting data for fade analysis")
            return []
        
        # Find heavy public consensus opportunities
        fade_opportunities = self._find_public_fade_opportunities(public_data)
        
        if not fade_opportunities:
            self.logger.info("No public fade opportunities found")
            return []
        
        # Convert to signals
        signals = []
        now_est = datetime.now(self.est)
        
        for fade_data in fade_opportunities:
            # Apply basic filters
            if not self._is_valid_fade_data(fade_data, now_est, minutes_ahead):
                continue
                
            # Check if public consensus is strong enough
            consensus_strength = fade_data.get('public_consensus_strength', 0)
            if consensus_strength < 75.0:  # Minimum 75% public consensus
                continue
            
            # Apply juice filter if needed
            if self._should_apply_juice_filter(fade_data):
                continue
            
            # Find matching profitable strategies
            matching_strategies = self._find_matching_strategies(profitable_strategies, fade_data)
            if not matching_strategies:
                continue
            
            matching_strategy = matching_strategies[0]  # Use best matching strategy
            
            # Calculate confidence
            confidence_data = self._calculate_confidence_for_public_fade(fade_data, matching_strategy)
            
            # Create the signal
            signal = self._create_betting_signal(fade_data, matching_strategy, confidence_data)
            signals.append(signal)
        
        self._log_processing_summary(len(signals), len(profitable_strategies), len(fade_opportunities))
        return signals
    
    def _find_public_fade_opportunities(self, public_data: List[Dict]) -> List[Dict]:
        """
        Find games with heavy public consensus across books
        Implements the core logic from public_money_fade_strategy_postgres.sql
        """
        fade_opportunities = []
        
        # Group data by game and split type for consensus analysis
        grouped_data = self._group_by_game_and_split_type(public_data)
        
        for game_key, book_data in grouped_data.items():
            if len(book_data) < 1:  # Need at least some data
                continue
            
            consensus_analysis = self._analyze_public_consensus(book_data)
            
            if consensus_analysis and self._is_significant_public_consensus(consensus_analysis):
                consensus_analysis['game_key'] = game_key
                fade_opportunities.append(consensus_analysis)
        
        return fade_opportunities
    
    def _group_by_game_and_split_type(self, data: List[Dict]) -> Dict[tuple, List[Dict]]:
        """Group public betting data by game and split type"""
        grouped = {}
        
        for record in data:
            key = (
                record.get('home_team'),
                record.get('away_team'),
                record.get('game_datetime'),
                record.get('split_type')
            )
            
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(record)
        
        return grouped
    
    def _analyze_public_consensus(self, book_data: List[Dict]) -> Dict[str, Any]:
        """
        Analyze public consensus across books for a specific game
        Returns consensus analysis or None if no significant consensus
        """
        if not book_data:
            return None
        
        # Extract money percentages
        money_percentages = []
        bet_percentages = []
        
        for record in book_data:
            money_pct = record.get('home_or_over_stake_percentage')
            bet_pct = record.get('home_or_over_bets_percentage')
            
            if money_pct is not None and bet_pct is not None:
                money_percentages.append(money_pct)
                bet_percentages.append(bet_pct)
        
        if not money_percentages:
            return None
        
        # Calculate consensus metrics
        avg_money_pct = sum(money_percentages) / len(money_percentages)
        avg_bet_pct = sum(bet_percentages) / len(bet_percentages)
        min_money_pct = min(money_percentages)
        max_money_pct = max(money_percentages)
        
        # Calculate standard deviation for consensus strength
        money_variance = self._calculate_variance(money_percentages)
        
        # Count books showing heavy consensus
        books_heavy_home = sum(1 for pct in money_percentages if pct >= 80)
        books_heavy_away = sum(1 for pct in money_percentages if pct <= 20)
        
        # Determine consensus type based on SQL logic
        consensus_type = None
        fade_recommendation = None
        fade_confidence = 'LOW'
        
        num_books = len(money_percentages)
        
        # Heavy public consensus (85%+ average with 2+ books)
        if avg_money_pct >= 85 and num_books >= 2:
            consensus_type = 'HEAVY_PUBLIC_HOME'
            fade_recommendation = book_data[0]['away_team']
            fade_confidence = 'HIGH'
        elif avg_money_pct <= 15 and num_books >= 2:
            consensus_type = 'HEAVY_PUBLIC_AWAY'
            fade_recommendation = book_data[0]['home_team']
            fade_confidence = 'HIGH'
        
        # Moderate public consensus (75%+ with 70%+ minimum across 3+ books)
        elif (avg_money_pct >= 75 and min_money_pct >= 70 and num_books >= 3):
            consensus_type = 'MODERATE_PUBLIC_HOME'
            fade_recommendation = book_data[0]['away_team']
            fade_confidence = 'MODERATE'
        elif (avg_money_pct <= 25 and max_money_pct <= 30 and num_books >= 3):
            consensus_type = 'MODERATE_PUBLIC_AWAY'
            fade_recommendation = book_data[0]['home_team']
            fade_confidence = 'MODERATE'
        
        if not consensus_type:
            return None
        
        # Calculate consensus strength
        if consensus_type.endswith('_HOME'):
            consensus_strength = avg_money_pct
        else:
            consensus_strength = 100 - avg_money_pct
        
        # Build consensus analysis
        consensus_analysis = {
            'home_team': book_data[0]['home_team'],
            'away_team': book_data[0]['away_team'],
            'game_datetime': book_data[0]['game_datetime'],
            'split_type': book_data[0]['split_type'],
            'source': 'PUBLIC_CONSENSUS',
            'book': 'FADE_OPPORTUNITY',
            
            # Consensus metrics
            'public_consensus_type': consensus_type,
            'fade_recommendation': fade_recommendation,
            'fade_confidence': fade_confidence,
            'public_consensus_strength': consensus_strength,
            
            # Statistical measures
            'num_books': num_books,
            'avg_money_pct': avg_money_pct,
            'avg_bet_pct': avg_bet_pct,
            'min_money_pct': min_money_pct,
            'max_money_pct': max_money_pct,
            'money_pct_variance': money_variance,
            
            # Book distribution
            'books_heavy_home': books_heavy_home,
            'books_heavy_away': books_heavy_away,
            
            # Timing
            'last_updated': max(r['last_updated'] for r in book_data),
            
            # Enhanced data for analysis
            'book_details': book_data,
            
            # For signal creation
            'differential': consensus_strength - 50,  # How far from 50/50
            'split_value': f"FADE_{consensus_type}",
        }
        
        return consensus_analysis
    
    def _calculate_variance(self, values: List[float]) -> float:
        """Calculate statistical variance of a list of values"""
        if len(values) < 2:
            return 0.0
        
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        return variance ** 0.5  # Return standard deviation
    
    def _is_significant_public_consensus(self, consensus_analysis: Dict[str, Any]) -> bool:
        """
        Determine if the public consensus is significant enough to fade
        Based on thresholds from SQL logic
        """
        # Must have adequate book coverage
        if consensus_analysis['num_books'] < 2:
            return False
        
        # Must have strong enough consensus
        consensus_strength = consensus_analysis['public_consensus_strength']
        if consensus_strength < 75.0:
            return False
        
        # Heavy consensus needs 2+ books, moderate needs 3+
        consensus_type = consensus_analysis['public_consensus_type']
        num_books = consensus_analysis['num_books']
        
        if consensus_type.startswith('HEAVY') and num_books < 2:
            return False
        if consensus_type.startswith('MODERATE') and num_books < 3:
            return False
        
        # Should have reasonable variance (not too spread out)
        if consensus_analysis['money_pct_variance'] > 15.0:
            return False
        
        return True
    
    def _is_valid_fade_data(self, fade_data: Dict[str, Any], now_est, minutes_ahead: int) -> bool:
        """Enhanced validation for public fade data"""
        # Basic validation
        required_fields = ['home_team', 'away_team', 'game_datetime', 'fade_recommendation', 'public_consensus_strength']
        if not all(field in fade_data for field in required_fields):
            return False
        
        # Time window validation
        game_time = self._normalize_game_time(fade_data['game_datetime'])
        time_diff = self._calculate_minutes_to_game(game_time, now_est)
        
        if time_diff < 0 or time_diff > minutes_ahead:
            return False
        
        # Must have a fade recommendation
        if not fade_data.get('fade_recommendation'):
            return False
        
        # Minimum consensus strength
        if fade_data.get('public_consensus_strength', 0) < 70.0:
            return False
        
        return True
    
    def _map_confidence_level_from_score(self, confidence_score: float) -> str:
        """Map confidence score to confidence level string"""
        if confidence_score >= 0.8:
            return 'VERY_HIGH'
        elif confidence_score >= 0.65:
            return 'HIGH'
        elif confidence_score >= 0.5:
            return 'MODERATE'
        elif confidence_score >= 0.35:
            return 'LOW'
        else:
            return 'VERY_LOW'
    
    def _calculate_confidence_for_public_fade(self, fade_data: Dict[str, Any], 
                                            strategy: ProfitableStrategy) -> Dict[str, Any]:
        """
        Calculate confidence score for public fade signals
        """
        # Convert strategy confidence from string to score
        confidence_mapping = {
            'HIGH': 0.8,
            'MODERATE': 0.6,
            'BASIC': 0.4,
            'LOW': 0.3,
            'INSUFFICIENT': 0.2
        }
        base_confidence = confidence_mapping.get(strategy.confidence, 0.4)
        
        # Adjust based on consensus strength
        consensus_strength = fade_data.get('public_consensus_strength', 50)
        strength_multiplier = min(1.5, 1.0 + (consensus_strength - 75.0) / 50.0)
        
        # Adjust based on number of books
        num_books = fade_data.get('num_books', 1)
        book_coverage_multiplier = min(1.3, 1.0 + (num_books - 2) / 5.0)
        
        # Adjust based on fade confidence level
        fade_confidence = fade_data.get('fade_confidence', 'LOW')
        confidence_level_multiplier = {
            'HIGH': 1.3,
            'MODERATE': 1.1,
            'LOW': 0.9
        }.get(fade_confidence, 1.0)
        
        # Adjust based on consensus variance (lower variance = stronger consensus)
        variance = fade_data.get('money_pct_variance', 10.0)
        variance_multiplier = max(0.8, 1.2 - (variance / 20.0))
        
        # Heavy consensus gets bonus
        consensus_type = fade_data.get('public_consensus_type', '')
        consensus_type_multiplier = 1.2 if consensus_type.startswith('HEAVY') else 1.0
        
        final_confidence = (base_confidence * strength_multiplier * book_coverage_multiplier * 
                          confidence_level_multiplier * variance_multiplier * consensus_type_multiplier)
        final_confidence = max(0.1, min(1.0, final_confidence))
        
        return {
            'confidence_score': final_confidence,  # Required field
            'confidence_level': self._map_confidence_level_from_score(final_confidence),
            'confidence_explanation': f"Public fade based on {consensus_strength:.1f}% consensus across {num_books} books",
            'recommendation_strength': fade_confidence,
            # Additional metadata
            'base_confidence': base_confidence,
            'strength_multiplier': strength_multiplier,
            'book_coverage_multiplier': book_coverage_multiplier,
            'confidence_level_multiplier': confidence_level_multiplier,
            'variance_multiplier': variance_multiplier,
            'consensus_type_multiplier': consensus_type_multiplier,
            'final_confidence': final_confidence,
            'consensus_strength': consensus_strength,
            'num_books': num_books,
            'fade_confidence': fade_confidence,
            'consensus_type': consensus_type,
            'variance': variance
        } 