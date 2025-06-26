"""
Book Conflict Processor

Detects games where different books show contradictory signals, indicating
potential line discrepancies and arbitrage opportunities.

Converts book_conflicts_strategy_postgres.sql logic to Python processor.
"""

from typing import List, Dict, Any
from datetime import datetime
import json

from .base_strategy_processor import BaseStrategyProcessor
from ...models.betting_analysis import BettingSignal, SignalType, ProfitableStrategy


class BookConflictProcessor(BaseStrategyProcessor):
    """
    Processor for detecting book conflict signals
    
    Identifies games where different books show contradictory sharp action signals,
    indicating potential line shopping opportunities or arbitrage situations.
    """
    
    def get_signal_type(self) -> SignalType:
        """Return the signal type this processor handles"""
        return SignalType.BOOK_CONFLICTS
    
    def get_strategy_category(self) -> str:
        """Return strategy category for proper routing"""
        return "BOOK_CONFLICTS"
    
    def get_required_tables(self) -> List[str]:
        """Return database tables required for this strategy"""
        return ["splits.raw_mlb_betting_splits"]
    
    def get_strategy_description(self) -> str:
        """Return human-readable description of the strategy"""
        return "Detects games where different books show contradictory signals, indicating line shopping opportunities or market inefficiencies"
    
    def validate_strategy_data(self, raw_data: List[Dict]) -> bool:
        """Validate we have multi-book data with conflicts"""
        if not raw_data:
            return False
            
        # Check we have data from multiple books
        books = set(row.get('book') for row in raw_data if row.get('book'))
        return len(books) >= 2
    
    async def process(self, minutes_ahead: int, 
                     profitable_strategies: List[ProfitableStrategy]) -> List[BettingSignal]:
        """Process book conflict signals using profitable strategies"""
        start_time, end_time = self._create_time_window(minutes_ahead)
        
        # Get multi-book data for conflict analysis
        multi_book_data = await self.repository.get_multi_book_data(start_time, end_time)
        
        if not multi_book_data:
            self.logger.info("Insufficient multi-book data for conflict analysis")
            return []
        
        # Find book conflicts
        book_conflicts = self._find_book_conflicts(multi_book_data)
        
        if not book_conflicts:
            self.logger.info("No book conflicts found")
            return []
        
        # Convert to signals
        signals = []
        now_est = datetime.now(self.est)
        
        for conflict_data in book_conflicts:
            # Apply basic filters
            if not self._is_valid_conflict_data(conflict_data, now_est, minutes_ahead):
                continue
                
            # Check if conflict strength is significant enough
            conflict_strength = conflict_data.get('weighted_sharp_variance', 0)
            if conflict_strength < 15.0:  # Minimum 15% conflict strength
                continue
            
            # Apply juice filter if needed
            if self._should_apply_juice_filter(conflict_data):
                continue
            
            # Find matching profitable strategies
            matching_strategies = self._find_matching_strategies(profitable_strategies, conflict_data)
            if not matching_strategies:
                continue
            
            matching_strategy = matching_strategies[0]  # Use best matching strategy
            
            # Calculate confidence
            confidence_data = self._calculate_confidence_for_book_conflict(conflict_data, matching_strategy)
            
            # Create the signal
            signal = self._create_betting_signal(conflict_data, matching_strategy, confidence_data)
            signals.append(signal)
        
        self._log_processing_summary(len(signals), len(profitable_strategies), len(book_conflicts))
        return signals
    
    def _find_book_conflicts(self, multi_book_data: List[Dict]) -> List[Dict]:
        """
        Find games where different books show conflicting sharp signals
        Implements the core logic from book_conflicts_strategy_postgres.sql
        """
        conflicts = []
        
        # Group data by game and split type for comparison across books
        grouped_data = self._group_by_game_and_split_type(multi_book_data)
        
        for game_key, book_data in grouped_data.items():
            if len(book_data) < 2:  # Need at least 2 books for conflict
                continue
            
            conflict_analysis = self._analyze_book_conflict(book_data)
            
            if conflict_analysis and self._is_significant_conflict(conflict_analysis):
                conflict_analysis['game_key'] = game_key
                conflicts.append(conflict_analysis)
        
        return conflicts
    
    def _group_by_game_and_split_type(self, data: List[Dict]) -> Dict[tuple, List[Dict]]:
        """Group multi-book data by game and split type"""
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
    
    def _analyze_book_conflict(self, book_data: List[Dict]) -> Dict[str, Any]:
        """
        Analyze conflict between books for a specific game
        Returns conflict analysis or None if no significant conflict
        """
        if len(book_data) < 2:
            return None
        
        # Calculate book credibility weights (from SQL logic)
        credibility_map = {
            'Pinnacle': 3.0, 'BookMaker': 2.5, 'Circa': 2.3,
            'BetMGM': 1.8, 'Caesars': 1.7, 'PointsBet': 1.6,
            'DraftKings': 1.5, 'FanDuel': 1.5, 'BetRivers': 1.2,
            'Barstool': 1.0
        }
        
        # Enhance each record with credibility and metrics
        enhanced_data = []
        for record in book_data:
            book = record.get('book', 'UNKNOWN')
            credibility = credibility_map.get(book, 1.0)
            differential = record.get('differential', 0)
            
            enhanced_record = record.copy()
            enhanced_record['book_credibility'] = credibility
            enhanced_record['weighted_sharp_signal'] = differential * credibility
            enhanced_record['raw_sharp_differential'] = differential
            
            # Classify sharp signal strength
            weighted_signal = enhanced_record['weighted_sharp_signal']
            if weighted_signal >= 20:
                enhanced_record['sharp_signal_class'] = 'PREMIUM_SHARP_HOME'
            elif weighted_signal >= 15:
                enhanced_record['sharp_signal_class'] = 'STRONG_SHARP_HOME'
            elif weighted_signal >= 10:
                enhanced_record['sharp_signal_class'] = 'MODERATE_SHARP_HOME'
            elif weighted_signal <= -20:
                enhanced_record['sharp_signal_class'] = 'PREMIUM_SHARP_AWAY'
            elif weighted_signal <= -15:
                enhanced_record['sharp_signal_class'] = 'STRONG_SHARP_AWAY'
            elif weighted_signal <= -10:
                enhanced_record['sharp_signal_class'] = 'MODERATE_SHARP_AWAY'
            else:
                enhanced_record['sharp_signal_class'] = 'NO_SHARP_SIGNAL'
            
            enhanced_data.append(enhanced_record)
        
        # Calculate conflict metrics
        weighted_signals = [r['weighted_sharp_signal'] for r in enhanced_data]
        raw_differentials = [r['raw_sharp_differential'] for r in enhanced_data]
        
        # Statistical variance measures
        weighted_variance = self._calculate_variance(weighted_signals)
        raw_variance = self._calculate_variance(raw_differentials)
        
        # Count unique signal directions
        signal_classes = set(r['sharp_signal_class'] for r in enhanced_data)
        unique_directions = len([s for s in signal_classes if 'SHARP' in s])
        
        # Determine if there's a real conflict (books pointing different directions)
        home_signals = len([r for r in enhanced_data if r['weighted_sharp_signal'] > 10])
        away_signals = len([r for r in enhanced_data if r['weighted_sharp_signal'] < -10])
        
        if home_signals == 0 or away_signals == 0:
            return None  # No conflict if all books agree
        
        # Build conflict analysis
        conflict_analysis = {
            'home_team': book_data[0]['home_team'],
            'away_team': book_data[0]['away_team'],
            'game_datetime': book_data[0]['game_datetime'],
            'split_type': book_data[0]['split_type'],
            'source': 'MULTI_BOOK',
            'book': 'CONFLICT',
            
            # Conflict metrics
            'num_books': len(book_data),
            'weighted_sharp_variance': weighted_variance,
            'raw_sharp_variance': raw_variance,
            'unique_signal_directions': unique_directions,
            'books_favor_home': home_signals,
            'books_favor_away': away_signals,
            
            # Average metrics
            'avg_credibility': sum(r['book_credibility'] for r in enhanced_data) / len(enhanced_data),
            'avg_weighted_signal': sum(weighted_signals) / len(weighted_signals),
            'avg_raw_differential': sum(raw_differentials) / len(raw_differentials),
            
            # Timing and quality
            'last_updated': max(r['last_updated'] for r in enhanced_data),
            'conflict_timing': self._classify_conflict_timing(book_data[0]),
            
            # Enhanced data for analysis
            'book_details': enhanced_data,
            
            # For signal creation
            'differential': weighted_variance,  # Use variance as differential measure
            'split_value': f"CONFLICT_{len(book_data)}_BOOKS",
        }
        
        return conflict_analysis
    
    def _calculate_variance(self, values: List[float]) -> float:
        """Calculate statistical variance of a list of values"""
        if len(values) < 2:
            return 0.0
        
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        return variance ** 0.5  # Return standard deviation
    
    def _is_significant_conflict(self, conflict_analysis: Dict[str, Any]) -> bool:
        """
        Determine if the conflict is significant enough to act on
        Based on enhanced thresholds from SQL logic
        """
        # Must have adequate book coverage
        if conflict_analysis['num_books'] < 2:
            return False
        
        # Must have real directional conflict
        if conflict_analysis['books_favor_home'] == 0 or conflict_analysis['books_favor_away'] == 0:
            return False
        
        # Must have sufficient variance (conflict strength)
        if conflict_analysis['weighted_sharp_variance'] < 12.0:  # Relaxed threshold
            return False
        
        # Should have decent average credibility
        if conflict_analysis['avg_credibility'] < 1.2:
            return False
        
        return True
    
    def _classify_conflict_timing(self, record: Dict[str, Any]) -> str:
        """Classify conflict timing based on time to game"""
        game_datetime = record['game_datetime']
        last_updated = record['last_updated']
        
        if isinstance(game_datetime, str):
            game_datetime = datetime.fromisoformat(game_datetime.replace('T', ' ').replace('Z', '+00:00'))
        if isinstance(last_updated, str):
            last_updated = datetime.fromisoformat(last_updated.replace('T', ' ').replace('Z', '+00:00'))
        
        hours_before = (game_datetime - last_updated).total_seconds() / 3600
        
        if hours_before <= 2:
            return 'CLOSING_CONFLICT'
        elif hours_before <= 6:
            return 'LATE_CONFLICT'
        elif hours_before <= 24:
            return 'EARLY_CONFLICT'
        else:
            return 'VERY_EARLY_CONFLICT'
    
    def _is_valid_conflict_data(self, conflict_data: Dict[str, Any], now_est, minutes_ahead: int) -> bool:
        """Enhanced validation for book conflict data"""
        # Basic validation
        required_fields = ['home_team', 'away_team', 'game_datetime', 'differential', 'num_books']
        if not all(field in conflict_data for field in required_fields):
            return False
        
        # Time window validation
        game_time = self._normalize_game_time(conflict_data['game_datetime'])
        time_diff = self._calculate_minutes_to_game(game_time, now_est)
        
        if time_diff < 0 or time_diff > minutes_ahead:
            return False
        
        # Book coverage validation
        if conflict_data['num_books'] < 2:
            return False
        
        # Minimum conflict strength
        if conflict_data.get('weighted_sharp_variance', 0) < 10.0:
            return False
        
        return True
    
    def _calculate_confidence_for_book_conflict(self, conflict_data: Dict[str, Any], 
                                              strategy: ProfitableStrategy) -> Dict[str, Any]:
        """
        Calculate confidence score for book conflict signals
        """
        base_confidence = strategy.confidence_score
        
        # Adjust based on conflict strength
        conflict_strength = conflict_data.get('weighted_sharp_variance', 0)
        strength_multiplier = min(1.5, 1.0 + (conflict_strength - 15.0) / 50.0)
        
        # Adjust based on book credibility
        avg_credibility = conflict_data.get('avg_credibility', 1.0)
        credibility_multiplier = min(1.3, avg_credibility / 2.0)
        
        # Adjust based on number of books
        num_books = conflict_data.get('num_books', 2)
        book_coverage_multiplier = min(1.2, 1.0 + (num_books - 2) / 10.0)
        
        # Timing penalty
        timing = conflict_data.get('conflict_timing', 'EARLY_CONFLICT')
        timing_multiplier = {
            'CLOSING_CONFLICT': 1.3,   # Premium timing
            'LATE_CONFLICT': 1.1,      # Good timing
            'EARLY_CONFLICT': 1.0,     # Normal timing
            'VERY_EARLY_CONFLICT': 0.9  # Slightly lower confidence
        }.get(timing, 1.0)
        
        final_confidence = base_confidence * strength_multiplier * credibility_multiplier * book_coverage_multiplier * timing_multiplier
        final_confidence = max(0.1, min(1.0, final_confidence))
        
        return {
            'base_confidence': base_confidence,
            'strength_multiplier': strength_multiplier,
            'credibility_multiplier': credibility_multiplier,
            'book_coverage_multiplier': book_coverage_multiplier,
            'timing_multiplier': timing_multiplier,
            'final_confidence': final_confidence,
            'conflict_strength': conflict_strength,
            'avg_credibility': avg_credibility,
            'num_books': num_books,
            'timing': timing
        } 