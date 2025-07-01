"""
Sharp Action Signal Processor

Professional betting pattern detection with book-specific strategy validation.
Detects sharp action where the money percentage differs significantly 
from bet percentage, indicating professional betting influence.

Enhanced to create separate strategies for:
- VSIN-DK: Sharp action signals from DraftKings via VSIN
- VSIN-Circa: Sharp action signals from Circa via VSIN  
- SBD-Unknown: Sharp action signals from SBD data

This processor implements the core sharp action detection algorithm,
migrated from the legacy sharp_action_detector_postgres.sql (15KB, 348 lines).
"""

from typing import List, Dict, Any, Tuple
from datetime import datetime

from ...models.betting_analysis import BettingSignal, SignalType, ProfitableStrategy
from .base_strategy_processor import BaseStrategyProcessor


class SharpActionProcessor(BaseStrategyProcessor):
    """
    Core sharp action detection processor with book-specific strategies.
    
    Implements professional betting pattern recognition by detecting significant
    discrepancies between money percentages and bet percentages, indicating
    sharp (professional) betting influence.
    
    Creates separate strategies for:
    - VSIN-DK: Sharp action signals from DraftKings via VSIN
    - VSIN-Circa: Sharp action signals from Circa via VSIN  
    - SBD-Unknown: Sharp action signals from SBD data
    
    This is the modern implementation of sharp_action_detector_postgres.sql.
    """
    
    def get_signal_type(self) -> SignalType:
        """Return the signal type this processor handles"""
        return SignalType.SHARP_ACTION
    
    def get_strategy_category(self) -> str:
        """Return strategy category for proper routing"""
        return "BOOK_SPECIFIC_SHARP_ACTION"
    
    def get_required_tables(self) -> List[str]:
        """Return database tables required for this strategy"""
        return ["splits.raw_mlb_betting_splits"]
    
    def get_strategy_description(self) -> str:
        """Return human-readable description of the strategy"""
        return "Book-specific sharp action detection with separate performance tracking per book"
    
    async def process(self, minutes_ahead: int, 
                     profitable_strategies: List[ProfitableStrategy]) -> List[BettingSignal]:
        """Process sharp action signals with book-specific strategy matching"""
        start_time, end_time = self._create_time_window(minutes_ahead)
        
        # Get raw signal data from repository
        raw_signals = await self.repository.get_sharp_signal_data(start_time, end_time)
        
        # 🚀 IMMEDIATE FIX: Short-circuit empty processing to eliminate redundancy
        if not raw_signals:
            self.logger.info(f"🔍 No sharp action data available, skipping processor (time window: {start_time} to {end_time})")
            return []
        
        # Get book-specific strategies
        book_strategies = self._get_book_specific_strategies(profitable_strategies)
        
        if not book_strategies:
            self.logger.warning("No profitable book-specific sharp action strategies found")
            return []
        
        signals = []
        now_est = datetime.now(self.est)
        
        # ✅ FIX: Add detailed debugging for signal filtering
        filter_stats = {
            'total_raw': len(raw_signals),
            'failed_validation': 0,
            'no_strategy_match': 0,
            'failed_juice_filter': 0,
            'passed_all_filters': 0
        }
        
        # 🚀 PERFORMANCE FIX: Log processor entry with strategy context
        strategy_names = list(book_strategies.keys())[:3]  # Show first 3 for brevity
        self.logger.info(f"🔥 Processing {len(raw_signals)} sharp action signals with {len(book_strategies)} strategies (e.g., {strategy_names}...)")
        
        for row in raw_signals:
            # Basic validation
            if not self._is_valid_signal_data(row, now_est, minutes_ahead):
                filter_stats['failed_validation'] += 1
                self.logger.debug(f"Signal filtered: basic validation failed for {row.get('away_team')} @ {row.get('home_team')} ({row.get('differential')}%)")
                continue
            
            # Create book-specific strategy identifier
            book_strategy_key = self._create_book_strategy_key(row)
            
            # Find matching book-specific strategy
            matching_strategy = await self._find_book_specific_strategy(
                book_strategy_key, row, book_strategies
            )
            
            if not matching_strategy:
                filter_stats['no_strategy_match'] += 1
                # 🚀 REDUCED LOGGING: Only log first few misses to avoid spam
                if filter_stats['no_strategy_match'] <= 3:
                    self.logger.info(f"🔍 No strategy match for key '{book_strategy_key}' | Available: {list(book_strategies.keys())[:5]} | Signal: {row.get('away_team')} @ {row.get('home_team')} ({row.get('differential')}%)")
                self._log_missing_strategy(book_strategy_key, row)
                continue
            
            # Apply juice filter
            if self._should_apply_juice_filter(row):
                filter_stats['failed_juice_filter'] += 1
                self.logger.debug(f"Signal filtered: juice filter failed for {row.get('away_team')} @ {row.get('home_team')}")
                continue
            
            filter_stats['passed_all_filters'] += 1
            
            # Calculate confidence score with book-specific adjustments
            confidence_data = self._calculate_book_specific_confidence(
                row, matching_strategy, book_strategy_key
            )
            
            # Create the signal with book-specific strategy name
            signal = self._create_book_specific_signal(row, matching_strategy, confidence_data, book_strategy_key)
            signals.append(signal)
        
        # 🚀 ENHANCED LOGGING: More informative filtering summary
        self.logger.info(f"🔍 Sharp Action Filtering Summary: {filter_stats}")
        if filter_stats['total_raw'] == 0:
            self.logger.info("✅ Processor completed - no raw data to process")
        elif filter_stats['passed_all_filters'] == 0:
            self.logger.warning(f"⚠️  All {filter_stats['total_raw']} signals filtered out")
        else:
            self.logger.info(f"✅ Generated {len(signals)} signals from {filter_stats['total_raw']} raw signals")
        
        self._log_book_specific_summary(signals, book_strategies, len(raw_signals))
        return signals
    
    def _get_book_specific_strategies(self, profitable_strategies: List[ProfitableStrategy]) -> Dict[str, ProfitableStrategy]:
        """
        Extract and organize book-specific strategies.
        
        Returns a dictionary mapping book strategy keys to strategies.
        """
        book_strategies = {}
        
        for strategy in profitable_strategies:
            # Check if this is a sharp action strategy
            if not self._is_sharp_action_strategy(strategy):
                continue
            
            # Extract book information from strategy name or create book-specific variants
            book_variants = self._extract_book_variants(strategy)
            
            for book_key, book_strategy in book_variants.items():
                book_strategies[book_key] = book_strategy
        
        self.logger.info(f"Found {len(book_strategies)} book-specific sharp action strategies")
        if book_strategies:
            self.logger.info(f"📊 Available book strategies: {list(book_strategies.keys())}")
        return book_strategies
    
    def _create_book_strategy_key(self, row: Dict[str, Any]) -> str:
        """
        Create a unique key for book-specific strategy matching.
        
        Format: "source-book-split_type"
        Examples:
        - "VSIN-draftkings-moneyline"
        - "VSIN-circa-spread" 
        - "SBD-unknown-total"
        """
        # Handle None values safely
        source = row.get('source', 'unknown')
        book = row.get('book', 'unknown')
        split_type = row.get('split_type', 'unknown')
        
        # Convert None to 'unknown' and then apply case transformations
        source = 'unknown' if source is None else str(source).upper()
        book = 'unknown' if book is None else str(book).lower()
        split_type = 'unknown' if split_type is None else str(split_type).lower()
        
        return f"{source}-{book}-{split_type}"
    
    async def _find_book_specific_strategy(self, book_strategy_key: str, row: Dict[str, Any], 
                                   book_strategies: Dict[str, ProfitableStrategy]) -> ProfitableStrategy:
        """
        Find the matching book-specific strategy for this signal.
        
        Tries exact match first, then falls back to more general matches.
        """
        abs_diff = abs(float(row['differential']))
        
        # Try exact book-specific match first
        if book_strategy_key in book_strategies:
            strategy = book_strategies[book_strategy_key]
            
            # Debug threshold calculation
            if strategy.win_rate >= 65:
                threshold = 10.0
            elif strategy.win_rate >= 60:
                threshold = 12.0
            elif strategy.win_rate >= 55:
                threshold = 15.0
            elif strategy.win_rate >= 50:
                threshold = 18.0
            else:
                threshold = 20.0
            
            threshold_pass = abs_diff >= threshold
            self.logger.info(f"🎯 Exact match found for '{book_strategy_key}': win_rate={strategy.win_rate}%, threshold={threshold}%, signal_diff={abs_diff}%, passes={threshold_pass}")
            
            # Extract source and split_type for dynamic thresholds
            source = row.get('source', 'unknown')
            split_type = row.get('split_type', 'unknown')
            source = 'unknown' if source is None else str(source).upper()
            split_type = 'unknown' if split_type is None else str(split_type).lower()
            
            threshold_result = await self._meets_strategy_threshold(strategy, abs_diff, source, split_type)
            if threshold_result:
                return strategy
            else:
                self.logger.warning(f"❌ Threshold failed for exact match '{book_strategy_key}': {abs_diff}% < threshold (win_rate={strategy.win_rate}%)")
        
        # Try source-level match (ignore book)
        source = row.get('source', 'unknown')
        split_type = row.get('split_type', 'unknown')
        
        # Handle None values safely
        source = 'unknown' if source is None else str(source).upper()
        split_type = 'unknown' if split_type is None else str(split_type).lower()
        source_key = f"{source}-any-{split_type}"
        
        if source_key in book_strategies:
            strategy = book_strategies[source_key]
            if await self._meets_strategy_threshold(strategy, abs_diff, source, split_type):
                return strategy
        
        # Try general sharp action match
        general_key = f"SHARP_ACTION-any-{split_type}"
        if general_key in book_strategies:
            strategy = book_strategies[general_key]
            if await self._meets_strategy_threshold(strategy, abs_diff, source, split_type):
                return strategy
        
        return None
    
    def _extract_book_variants(self, strategy: ProfitableStrategy) -> Dict[str, ProfitableStrategy]:
        """
        Extract or create book-specific variants from a general strategy.
        
        🚀 OPTIMIZED: Reduced redundant strategy creation by intelligent grouping
        """
        book_variants = {}
        
        # Check if strategy name indicates specific book
        strategy_name = strategy.strategy_name.lower()
        source_book = strategy.source_book.lower()
        
        # 🚀 OPTIMIZATION: Direct book-specific mapping (no expansion)
        if 'vsin' in source_book and 'draftkings' in source_book:
            # VSIN-DraftKings specific - use as-is
            key = f"VSIN-draftkings-{strategy.split_type}"
            book_variants[key] = strategy
            
        elif 'vsin' in source_book and 'circa' in source_book:
            # VSIN-Circa specific - use as-is
            key = f"VSIN-circa-{strategy.split_type}"
            book_variants[key] = strategy
            
        elif 'sbd' in source_book:
            # SBD specific - use as-is
            key = f"SBD-unknown-{strategy.split_type}"
            book_variants[key] = strategy
            
        # 🚀 SMART GROUPING: Only create variants for truly general strategies
        elif source_book in ['general', 'all', 'any', 'unknown'] or 'sharp_action' in strategy_name:
            # This is a general sharp action strategy - create consolidated variants
            # Only create for major book combinations to reduce redundancy
            
            primary_books = [
                ('VSIN', 'draftkings'),
                ('VSIN', 'circa'),
                ('SBD', 'unknown')
            ]
            
            for source, book in primary_books:
                variant = self._create_book_variant(strategy, source, book)
                key = f"{source}-{book}-{strategy.split_type}"
                book_variants[key] = variant
                
        else:
            # 🚀 FALLBACK: Treat as book-specific if not clearly general
            # This reduces over-expansion of strategies
            key = f"{source_book}-{strategy.split_type}"
            book_variants[key] = strategy
        
        # 🚀 PERFORMANCE LOG: Track variant creation
        if len(book_variants) > 3:
            self.logger.debug(f"⚠️  Created {len(book_variants)} variants for strategy '{strategy.strategy_name}' - consider consolidation")
        
        return book_variants
    
    def _create_book_variant(self, base_strategy: ProfitableStrategy, source: str, book: str) -> ProfitableStrategy:
        """Create a book-specific variant of a strategy."""
        return ProfitableStrategy(
            strategy_name=f"{base_strategy.strategy_name}_{source}_{book}",
            source_book=f"{source}-{book}",
            split_type=base_strategy.split_type,
            win_rate=base_strategy.win_rate,
            roi=base_strategy.roi,
            total_bets=base_strategy.total_bets,
            confidence=base_strategy.confidence,
            ci_lower=base_strategy.ci_lower,
            ci_upper=base_strategy.ci_upper
        )
    
    def _is_sharp_action_strategy(self, strategy: ProfitableStrategy) -> bool:
        """Check if strategy is related to sharp action detection."""
        strategy_name = strategy.strategy_name.lower()
        source_book = strategy.source_book.lower()
        
        # ✅ FIX: Recognize book-specific strategies as sharp action strategies
        # These represent sharp action detection for specific books/sources
        book_specific_patterns = [
            'vsin-circa', 'vsin-draftkings', 'sbd-unknown',
            'vsin-dk', 'vsin-draftking'
        ]
        
        # Check for explicit sharp action keywords
        sharp_keywords = [
            'sharp_action', 'sharp', 'money_differential', 'bet_money_diff', 'signal_combinations'
        ]
        
        # Strategy is sharp action if:
        # 1. Has explicit sharp keywords, OR
        # 2. Is a book-specific strategy (VSIN-circa-moneyline, SBD-unknown-spread, etc.)
        return (any(keyword in strategy_name for keyword in sharp_keywords) or
                any(pattern in source_book for pattern in book_specific_patterns) or
                any(pattern in strategy_name for pattern in book_specific_patterns))
    
    async def _meets_strategy_threshold(self, strategy: ProfitableStrategy, abs_diff: float, 
                                      source: str = "default", split_type: str = "default") -> bool:
        """Check if signal meets the strategy's threshold requirements using dynamic thresholds."""
        
        # 🎯 DYNAMIC THRESHOLDS: Use threshold manager if available
        if hasattr(self, 'threshold_manager') and self.threshold_manager:
            try:
                threshold_config = await self.threshold_manager.get_dynamic_threshold(
                    strategy_type='sharp_action',
                    source=source,
                    split_type=split_type
                )
                
                # Use appropriate threshold based on signal strength
                if abs_diff >= threshold_config.high_threshold:
                    threshold = threshold_config.minimum_threshold
                elif abs_diff >= threshold_config.moderate_threshold:
                    threshold = threshold_config.minimum_threshold
                else:
                    threshold = threshold_config.minimum_threshold
                
                self.logger.debug(f"🎯 Dynamic threshold for {source}/{split_type}: {threshold:.1f}% "
                                f"(phase: {threshold_config.phase.value}, sample_size: {threshold_config.sample_size})")
                
                return abs_diff >= threshold
                
            except Exception as e:
                self.logger.warning(f"Failed to get dynamic threshold, falling back to static: {e}")
        
        # 🔄 FALLBACK: Original static threshold logic (now much more aggressive)
        # ✅ FIX: Dramatically lowered thresholds to capture real-world signals
        if strategy.win_rate >= 65:
            threshold = 3.0   # Very aggressive for high performers (was 10.0)
        elif strategy.win_rate >= 60:
            threshold = 4.0   # Aggressive (was 12.0)
        elif strategy.win_rate >= 55:
            threshold = 5.0   # Moderate (was 15.0)
        elif strategy.win_rate >= 50:
            threshold = 6.0   # Conservative (was 18.0)
        else:
            threshold = 8.0   # Very conservative (was 20.0)
        
        self.logger.debug(f"📊 Static threshold fallback: {threshold:.1f}% for win_rate {strategy.win_rate:.1%}")
        return abs_diff >= threshold
    
    def _calculate_book_specific_confidence(self, row: Dict[str, Any], 
                                          matching_strategy: ProfitableStrategy,
                                          book_strategy_key: str) -> Dict[str, Any]:
        """Calculate confidence with book-specific adjustments."""
        base_confidence = self._calculate_confidence(
            row['differential'], row['source'], row['book'],
            row['split_type'], matching_strategy.strategy_name,
            row['last_updated'], self._normalize_game_time(row['game_datetime'])
        )
        
        # Apply book-specific confidence modifiers
        confidence_modifier = self._get_book_confidence_modifier(book_strategy_key)
        
        # Adjust confidence based on book reliability - use correct key
        adjusted_confidence = base_confidence['confidence_score'] * confidence_modifier
        adjusted_confidence = max(0.1, min(0.95, adjusted_confidence))  # Keep in valid range
        
        return {
            **base_confidence,
            'confidence_score': adjusted_confidence,  # Ensure we use the correct key
            'book_strategy_key': book_strategy_key,
            'book_modifier': confidence_modifier
        }
    
    def _get_book_confidence_modifier(self, book_strategy_key: str) -> float:
        """
        Get confidence modifier based on book reliability.
        
        Different books may have different reliability levels for sharp action.
        """
        if 'VSIN-draftkings' in book_strategy_key:
            return 1.1  # DraftKings data tends to be reliable
        elif 'VSIN-circa' in book_strategy_key:
            return 1.05  # Circa is also reliable but smaller market
        elif 'SBD' in book_strategy_key:
            return 0.95  # SBD data may be less reliable
        else:
            return 1.0  # Default modifier
    
    def _create_book_specific_signal(self, row: Dict[str, Any], matching_strategy: ProfitableStrategy,
                                   confidence_data: Dict[str, Any], book_strategy_key: str) -> BettingSignal:
        """Create betting signal with book-specific information."""
        signal = self._create_betting_signal(row, matching_strategy, confidence_data)
        
        # Add book-specific metadata
        signal.metadata = signal.metadata or {}
        signal.metadata.update({
            'book_strategy_key': book_strategy_key,
            'book_specific': True,
            'original_source': row.get('source'),
            'original_book': row.get('book')
        })
        
        # Update strategy name to include book information
        signal.strategy_name = f"{matching_strategy.strategy_name}_{book_strategy_key}"
        
        return signal
    
    def _log_missing_strategy(self, book_strategy_key: str, row: Dict[str, Any]):
        """Log when we can't find a matching strategy for a book."""
        self.logger.debug(
            f"No matching strategy found for {book_strategy_key}",
            extra={
                'differential': row.get('differential'),
                'game': f"{row.get('away_team')} @ {row.get('home_team')}"
            }
        )
    
    def _log_book_specific_summary(self, signals: List[BettingSignal], 
                                 book_strategies: Dict[str, ProfitableStrategy], 
                                 raw_signals_count: int):
        """Log summary of book-specific processing."""
        book_signal_counts = {}
        for signal in signals:
            book_key = signal.metadata.get('book_strategy_key', 'unknown') if signal.metadata else 'unknown'
            book_signal_counts[book_key] = book_signal_counts.get(book_key, 0) + 1
        
        self.logger.info(
            f"Book-specific sharp action processing complete: {len(signals)} signals from {raw_signals_count} raw signals",
            extra={
                'total_signals': len(signals),
                'raw_signals': raw_signals_count,
                'book_strategies': len(book_strategies),
                'signals_by_book': book_signal_counts
            }
        )
    
    def _is_valid_signal_data(self, row: Dict[str, Any], current_time: datetime, 
                             minutes_ahead: int) -> bool:
        """Validate signal data quality and timing"""
        try:
            # ✅ FIX: Add detailed validation debugging
            game_time = self._normalize_game_time(row['game_datetime'])
            time_diff_minutes = self._calculate_minutes_to_game(game_time, current_time)
            
            # Check time window - allow past games for testing/demo purposes
            # Original: if not (0 <= time_diff_minutes <= minutes_ahead):
            # Modified to allow games up to 24 hours in the past for testing
            if not (-1440 <= time_diff_minutes <= minutes_ahead):
                self.logger.warning(f"🕒 Time window check failed: {time_diff_minutes} minutes (need -1440-{minutes_ahead}) for {row.get('away_team')} @ {row.get('home_team')}")
                return False
            
            # Check data completeness
            required_fields = ['home_team', 'away_team', 'split_type', 'differential', 'source']
            missing_fields = [field for field in required_fields if row.get(field) is None]
            if missing_fields:
                self.logger.warning(f"📋 Missing required fields {missing_fields} for {row.get('away_team')} @ {row.get('home_team')}")
                return False
            
            # Check differential strength
            abs_diff = abs(float(row['differential']))
            if abs_diff < self.config.minimum_differential:
                self.logger.warning(f"📉 Differential too weak: {abs_diff} < {self.config.minimum_differential} for {row.get('away_team')} @ {row.get('home_team')}")
                return False
            
            self.logger.info(f"✅ Signal passed validation: {row.get('away_team')} @ {row.get('home_team')} ({abs_diff}% diff, {time_diff_minutes} min)")
            return True
            
        except (ValueError, TypeError, KeyError) as e:
            self.logger.warning(f"Invalid signal data exception: {e} for {row}")
            return False
    
    def _should_apply_juice_filter(self, row: Dict[str, Any]) -> bool:
        """Check if juice filter should be applied to this signal"""
        if row['split_type'] != 'moneyline':
            return False
        
        # Determine recommended side
        differential = row['differential']
        recommended_team = row['home_team'] if differential > 0 else row['away_team']
        
        return self._should_filter_juice(
            row['split_type'], row.get('split_value'),
            recommended_team, row['home_team'], row['away_team']
        ) 