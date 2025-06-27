"""
Underdog Moneyline Value Strategy Processor

Tests the hypothesis that public loves betting favorites, creating systematic value on underdogs.
Focuses on core underdog value detection where sharp money and public betting patterns 
create profitable opportunities.

Key Strategies:
- VALUE_AWAY_DOG: Away underdog when public heavily favors home favorite (≥65%)
- VALUE_HOME_DOG: Home underdog when public heavily favors away favorite (≤35%)
- MODERATE_VALUE: Underdog opportunities with lower public thresholds

Enhanced with sharp money confirmation and ROI-based confidence scoring.
"""

from typing import List, Dict, Any, Tuple
from datetime import datetime
import json

from ...models.betting_analysis import BettingSignal, SignalType, ProfitableStrategy
from .base_strategy_processor import BaseStrategyProcessor


class UnderdogValueProcessor(BaseStrategyProcessor):
    """
    Processor for underdog moneyline value strategy detection.
    
    Identifies value opportunities when public heavily favors favorites,
    creating systematic value on underdog sides.
    """
    
    def get_signal_type(self) -> SignalType:
        """Return the signal type this processor handles"""
        return SignalType.UNDERDOG_VALUE
    
    def get_strategy_category(self) -> str:
        """Return strategy category for proper routing"""
        return "VALUE_DETECTION"
    
    def get_required_tables(self) -> List[str]:
        """Return database tables required for this strategy"""
        return ["splits.raw_mlb_betting_splits"]
    
    def get_strategy_description(self) -> str:
        """Return human-readable description of the strategy"""
        return "Underdog moneyline value: Find value on underdogs when public heavily favors favorites"
    
    async def process(self, minutes_ahead: int, 
                     profitable_strategies: List[ProfitableStrategy]) -> List[BettingSignal]:
        """Process underdog value signals"""
        start_time, end_time = self._create_time_window(minutes_ahead)
        
        # Get underdog value strategies
        underdog_strategies = self._get_underdog_strategies(profitable_strategies)
        
        if not underdog_strategies:
            self.logger.warning("No profitable underdog value strategies found")
            return []
        
        # Get raw moneyline data for underdog analysis
        raw_data = await self.repository.get_underdog_value_data(start_time, end_time)
        
        if not raw_data:
            self.logger.info("No underdog value data found for analysis")
            return []
        
        signals = []
        now_est = datetime.now(self.est)
        
        for row in raw_data:
            # Basic validation
            if not self._is_valid_underdog_data(row, now_est, minutes_ahead):
                continue
            
            # Analyze underdog value opportunities
            underdog_analysis = self._analyze_underdog_value(row)
            
            if not underdog_analysis:
                continue
            
            # Find matching profitable strategy
            matching_strategy = self._find_underdog_strategy(
                underdog_analysis, underdog_strategies
            )
            
            if not matching_strategy:
                continue
            
            # Apply juice filter for underdog bets
            if self._should_apply_juice_filter(row, underdog_analysis):
                continue
            
            # Calculate confidence with underdog-specific adjustments
            confidence_data = self._calculate_underdog_confidence(
                row, underdog_analysis, matching_strategy
            )
            
            # Create the underdog value signal
            signal = self._create_underdog_signal(
                row, underdog_analysis, matching_strategy, confidence_data
            )
            signals.append(signal)
        
        self._log_underdog_summary(signals, underdog_strategies, len(raw_data))
        return signals
    
    def _get_underdog_strategies(self, profitable_strategies: List[ProfitableStrategy]) -> List[ProfitableStrategy]:
        """Extract underdog value strategies"""
        underdog_strategies = []
        
        for strategy in profitable_strategies:
            strategy_name = strategy.strategy_name.lower()
            if any(keyword in strategy_name for keyword in [
                'underdog', 'dog', 'value', 'underdog_ml', 'ml_value'
            ]):
                underdog_strategies.append(strategy)
        
        self.logger.info(f"Found {len(underdog_strategies)} underdog value strategies")
        return underdog_strategies
    
    def _analyze_underdog_value(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze underdog value opportunities in the betting data.
        
        Returns underdog analysis or None if no value opportunity found.
        """
        try:
            home_bet_pct = float(row.get('home_bet_pct', 0))
            home_stake_pct = float(row.get('home_stake_pct', 0))
            differential = home_stake_pct - home_bet_pct
            
            # Extract moneyline odds
            split_value = row.get('split_value', '{}')
            if isinstance(split_value, str):
                try:
                    odds_data = json.loads(split_value)
                    home_ml_odds = int(odds_data.get('home', 0))
                    away_ml_odds = int(odds_data.get('away', 0))
                except (json.JSONDecodeError, ValueError, TypeError):
                    return None
            else:
                return None
            
            # Determine favorites and underdogs
            if home_ml_odds < away_ml_odds:
                favorite_team = 'HOME_FAVORITE'
                underdog_team = 'AWAY_UNDERDOG'
                underdog_odds = away_ml_odds
            elif away_ml_odds < home_ml_odds:
                favorite_team = 'AWAY_FAVORITE'
                underdog_team = 'HOME_UNDERDOG'
                underdog_odds = home_ml_odds
            else:
                return None  # Pick 'em games
            
            # Only process underdog odds (positive)
            if underdog_odds <= 0:
                return None
            
            # Categorize underdog odds
            if 100 <= underdog_odds <= 200:
                odds_category = 'SMALL_DOG'
            elif underdog_odds > 200:
                odds_category = 'BIG_DOG'
            else:
                return None
            
            # Identify value signals
            value_signal = None
            
            if underdog_team == 'AWAY_UNDERDOG':
                # Away underdog value: public heavily on home favorite
                if home_bet_pct >= 65 and favorite_team == 'HOME_FAVORITE':
                    value_signal = 'VALUE_AWAY_DOG'
                elif home_bet_pct >= 60 and favorite_team == 'HOME_FAVORITE':
                    value_signal = 'MODERATE_VALUE_AWAY_DOG'
                    
            elif underdog_team == 'HOME_UNDERDOG':
                # Home underdog value: public heavily on away favorite
                if home_bet_pct <= 35 and favorite_team == 'AWAY_FAVORITE':
                    value_signal = 'VALUE_HOME_DOG'
                elif home_bet_pct <= 40 and favorite_team == 'AWAY_FAVORITE':
                    value_signal = 'MODERATE_VALUE_HOME_DOG'
            
            if not value_signal:
                return None
            
            # Sharp money confirmation
            sharp_confirmation = 'NO_SHARP_CONFIRMATION'
            if underdog_team == 'HOME_UNDERDOG' and differential >= 10:
                sharp_confirmation = 'SHARP_SUPPORTS_HOME_DOG'
            elif underdog_team == 'AWAY_UNDERDOG' and differential <= -10:
                sharp_confirmation = 'SHARP_SUPPORTS_AWAY_DOG'
            
            return {
                'value_signal': value_signal,
                'underdog_team': underdog_team,
                'underdog_side': row.get('away_team') if underdog_team == 'AWAY_UNDERDOG' else row.get('home_team'),
                'underdog_odds': underdog_odds,
                'odds_category': odds_category,
                'favorite_team': favorite_team,
                'home_bet_pct': home_bet_pct,
                'home_stake_pct': home_stake_pct,
                'differential': differential,
                'sharp_confirmation': sharp_confirmation,
                'home_ml_odds': home_ml_odds,
                'away_ml_odds': away_ml_odds
            }
            
        except (ValueError, TypeError, KeyError) as e:
            self.logger.warning(f"Error analyzing underdog value: {e}")
            return None
    
    def _find_underdog_strategy(self, underdog_analysis: Dict[str, Any], 
                              underdog_strategies: List[ProfitableStrategy]) -> ProfitableStrategy:
        """Find matching underdog value strategy"""
        value_signal = underdog_analysis['value_signal']
        underdog_odds = underdog_analysis['underdog_odds']
        sharp_confirmation = underdog_analysis['sharp_confirmation']
        
        # Look for specific underdog strategy matches
        for strategy in underdog_strategies:
            strategy_name = strategy.strategy_name.lower()
            
            # Match value signal type
            if 'value_away_dog' in value_signal.lower() and 'away' in strategy_name:
                if self._meets_underdog_threshold(strategy, underdog_odds):
                    return strategy
            elif 'value_home_dog' in value_signal.lower() and 'home' in strategy_name:
                if self._meets_underdog_threshold(strategy, underdog_odds):
                    return strategy
            elif 'moderate_value' in value_signal.lower() and 'moderate' in strategy_name:
                if self._meets_underdog_threshold(strategy, underdog_odds):
                    return strategy
            
            # General underdog matches
            elif 'underdog' in strategy_name or 'dog' in strategy_name:
                if self._meets_underdog_threshold(strategy, underdog_odds):
                    # Bonus for sharp confirmation
                    if sharp_confirmation != 'NO_SHARP_CONFIRMATION':
                        return strategy
                    elif strategy.win_rate >= 55:  # Only take general without sharp if good win rate
                        return strategy
        
        return None
    
    def _meets_underdog_threshold(self, strategy: ProfitableStrategy, underdog_odds: int) -> bool:
        """Check if underdog meets strategy thresholds"""
        # Underdog odds should be reasonable (not too high)
        if underdog_odds > 500:  # +500 is quite high
            return False
        
        # Dynamic thresholds based on strategy performance
        if strategy.win_rate >= 60:
            return underdog_odds >= 110  # Accept smaller dogs for high performers
        elif strategy.win_rate >= 55:
            return underdog_odds >= 120  # Moderate threshold
        elif strategy.win_rate >= 50:
            return underdog_odds >= 130  # Conservative threshold
        else:
            return underdog_odds >= 150  # Very conservative
    
    def _calculate_underdog_confidence(self, row: Dict[str, Any], 
                                     underdog_analysis: Dict[str, Any],
                                     matching_strategy: ProfitableStrategy) -> Dict[str, Any]:
        """Calculate confidence with underdog-specific adjustments"""
        base_confidence = self._calculate_confidence(
            underdog_analysis['differential'], 
            row.get('source', 'unknown'), 
            row.get('book', 'unknown'),
            'moneyline',
            matching_strategy.strategy_name,
            row.get('last_updated'), 
            self._normalize_game_time(row['game_datetime'])
        )
        
        # Apply underdog-specific modifiers
        underdog_modifier = self._get_underdog_confidence_modifier(underdog_analysis)
        
        # FIXED: Respect the confidence scorer's assessment - don't artificially inflate weak signals
        # The ConfidenceScorer already properly scored this based on differential strength
        base_score = base_confidence['confidence_score']
        
        # Only apply modest modifiers, and cap much lower for weak signals
        if base_score < 0.5:  # Weak signals (like 2% differential)
            # Don't boost weak signals significantly - they should stay weak
            adjusted_confidence = base_score * min(1.1, underdog_modifier)  # Max 10% boost
            adjusted_confidence = max(0.1, min(0.55, adjusted_confidence))  # Cap at 55% for weak signals
        elif base_score < 0.7:  # Moderate signals
            adjusted_confidence = base_score * min(1.2, underdog_modifier)  # Max 20% boost
            adjusted_confidence = max(0.1, min(0.80, adjusted_confidence))  # Cap at 80% for moderate signals
        else:  # Strong signals
            adjusted_confidence = base_score * underdog_modifier
            adjusted_confidence = max(0.1, min(0.92, adjusted_confidence))  # Cap at 92% max (not 95%)
        
        # CRITICAL: Never let a 2% differential signal get high confidence regardless of modifiers
        differential = abs(float(row.get('differential', 0)))
        if differential < 5.0:  # Very weak differentials
            adjusted_confidence = min(adjusted_confidence, 0.50)  # Hard cap at 50%
        elif differential < 10.0:  # Weak differentials  
            adjusted_confidence = min(adjusted_confidence, 0.65)  # Hard cap at 65%
        
        return {
            **base_confidence,
            'confidence_score': adjusted_confidence,
            'underdog_odds': underdog_analysis['underdog_odds'],
            'sharp_confirmation': underdog_analysis['sharp_confirmation'],
            'underdog_modifier': underdog_modifier
        }
    
    def _get_underdog_confidence_modifier(self, underdog_analysis: Dict[str, Any]) -> float:
        """Get confidence modifier based on underdog characteristics"""
        value_signal = underdog_analysis['value_signal']
        underdog_odds = underdog_analysis['underdog_odds']
        sharp_confirmation = underdog_analysis['sharp_confirmation']
        home_bet_pct = underdog_analysis['home_bet_pct']
        
        # Base modifier on value signal strength
        if 'VALUE_' in value_signal:
            signal_modifier = 1.2  # Strong value signals
        elif 'MODERATE_VALUE' in value_signal:
            signal_modifier = 1.05  # Moderate value signals
        else:
            signal_modifier = 1.0
        
        # Odds modifier (smaller dogs often more reliable)
        if 110 <= underdog_odds <= 160:
            odds_modifier = 1.15  # Sweet spot for small dogs
        elif 160 < underdog_odds <= 200:
            odds_modifier = 1.1   # Still good moderate dogs
        elif 200 < underdog_odds <= 300:
            odds_modifier = 1.05  # Bigger dogs need more confirmation
        else:
            odds_modifier = 0.95  # Very big dogs are riskier
        
        # Sharp confirmation bonus
        sharp_modifier = 1.2 if sharp_confirmation != 'NO_SHARP_CONFIRMATION' else 1.0
        
        # Public betting extreme bonus
        if 'AWAY_DOG' in value_signal and home_bet_pct >= 70:
            public_modifier = 1.1
        elif 'HOME_DOG' in value_signal and home_bet_pct <= 30:
            public_modifier = 1.1
        else:
            public_modifier = 1.0
        
        return signal_modifier * odds_modifier * sharp_modifier * public_modifier
    
    def _create_underdog_signal(self, row: Dict[str, Any], 
                              underdog_analysis: Dict[str, Any],
                              matching_strategy: ProfitableStrategy,
                              confidence_data: Dict[str, Any]) -> BettingSignal:
        """Create underdog value betting signal"""
        underdog_side = underdog_analysis['underdog_side']
        underdog_odds = underdog_analysis['underdog_odds']
        value_signal = underdog_analysis['value_signal']
        
        # Create recommendation
        recommendation = f"BET {underdog_side} ML (+{underdog_odds}) - {self._get_value_reason(underdog_analysis)}"
        
        signal = self._create_betting_signal(row, matching_strategy, confidence_data)
        
        # Update signal with underdog-specific information
        signal.recommendation = recommendation
        signal.metadata = signal.metadata or {}
        signal.metadata.update({
            'value_signal': value_signal,
            'underdog_team': underdog_analysis['underdog_team'],
            'underdog_side': underdog_side,
            'underdog_odds': underdog_odds,
            'odds_category': underdog_analysis['odds_category'],
            'favorite_team': underdog_analysis['favorite_team'],
            'home_bet_pct': underdog_analysis['home_bet_pct'],
            'sharp_confirmation': underdog_analysis['sharp_confirmation'],
            'bet_type': 'MONEYLINE',
            'expected_roi': self._calculate_expected_roi(underdog_odds, matching_strategy.win_rate)
        })
        
        return signal
    
    def _get_value_reason(self, underdog_analysis: Dict[str, Any]) -> str:
        """Get human-readable reason for the value bet"""
        value_signal = underdog_analysis['value_signal']
        home_bet_pct = underdog_analysis['home_bet_pct']
        sharp_confirmation = underdog_analysis['sharp_confirmation']
        
        if 'VALUE_AWAY_DOG' in value_signal:
            reason = f"Public heavily on home favorite ({home_bet_pct:.1f}%)"
        elif 'VALUE_HOME_DOG' in value_signal:
            reason = f"Public heavily on away favorite ({100-home_bet_pct:.1f}%)"
        elif 'MODERATE_VALUE_AWAY_DOG' in value_signal:
            reason = f"Moderate value, public on home ({home_bet_pct:.1f}%)"
        elif 'MODERATE_VALUE_HOME_DOG' in value_signal:
            reason = f"Moderate value, public on away ({100-home_bet_pct:.1f}%)"
        else:
            reason = "Underdog value opportunity"
        
        if sharp_confirmation != 'NO_SHARP_CONFIRMATION':
            reason += " + Sharp money confirmation"
        
        return reason
    
    def _calculate_expected_roi(self, underdog_odds: int, win_rate: float) -> float:
        """Calculate expected ROI for underdog bet"""
        if underdog_odds <= 0:
            return 0.0
        
        # Expected value calculation
        win_probability = win_rate / 100.0
        lose_probability = 1 - win_probability
        
        # For positive odds: profit = (odds/100) * bet for wins, -bet for losses
        expected_profit = (win_probability * (underdog_odds / 100.0)) - lose_probability
        
        return expected_profit * 100  # Convert to percentage
    
    def _is_valid_underdog_data(self, row: Dict[str, Any], current_time: datetime, 
                              minutes_ahead: int) -> bool:
        """Validate underdog data quality and timing"""
        try:
            # Check split type is moneyline
            if row.get('split_type') != 'moneyline':
                return False
            
            # Check time window
            game_time = self._normalize_game_time(row['game_datetime'])
            time_diff_minutes = self._calculate_minutes_to_game(game_time, current_time)
            
            if not (0 <= time_diff_minutes <= minutes_ahead):
                return False
            
            # Check data completeness
            required_fields = ['home_team', 'away_team', 'home_bet_pct', 'home_stake_pct', 'split_value']
            if not all(row.get(field) is not None for field in required_fields):
                return False
            
            # Check split_value format
            split_value = row.get('split_value', '')
            if not isinstance(split_value, str) or not split_value.startswith('{'):
                return False
            
            # Check percentage validity
            home_bet_pct = float(row.get('home_bet_pct', 0))
            home_stake_pct = float(row.get('home_stake_pct', 0))
            
            if not (0 <= home_bet_pct <= 100 and 0 <= home_stake_pct <= 100):
                return False
            
            return True
            
        except (ValueError, TypeError, KeyError) as e:
            self.logger.warning(f"Invalid underdog data: {e}")
            return False
    
    def _should_apply_juice_filter(self, row: Dict[str, Any], underdog_analysis: Dict[str, Any]) -> bool:
        """Check if juice filter should be applied to this underdog signal"""
        # Apply juice filter for underdog moneyline bets
        underdog_side = underdog_analysis['underdog_side']
        
        return self._should_filter_juice(
            'moneyline', row.get('split_value'),
            underdog_side, 
            row.get('home_team'), row.get('away_team')
        )
    
    def _log_underdog_summary(self, signals: List[BettingSignal], 
                            underdog_strategies: List[ProfitableStrategy], 
                            raw_data_count: int):
        """Log summary of underdog processing"""
        value_signal_counts = {}
        underdog_side_counts = {'HOME': 0, 'AWAY': 0}
        sharp_confirmation_counts = {'YES': 0, 'NO': 0}
        
        for signal in signals:
            if signal.metadata:
                value_signal = signal.metadata.get('value_signal', 'unknown')
                underdog_team = signal.metadata.get('underdog_team', 'unknown')
                sharp_confirmation = signal.metadata.get('sharp_confirmation', 'NO_SHARP_CONFIRMATION')
                
                value_signal_counts[value_signal] = value_signal_counts.get(value_signal, 0) + 1
                
                if 'HOME' in underdog_team:
                    underdog_side_counts['HOME'] += 1
                elif 'AWAY' in underdog_team:
                    underdog_side_counts['AWAY'] += 1
                
                if sharp_confirmation != 'NO_SHARP_CONFIRMATION':
                    sharp_confirmation_counts['YES'] += 1
                else:
                    sharp_confirmation_counts['NO'] += 1
        
        self.logger.info(
            f"Underdog value processing complete: {len(signals)} signals from {raw_data_count} raw records",
            extra={
                'total_signals': len(signals),
                'raw_data_count': raw_data_count,
                'underdog_strategies': len(underdog_strategies),
                'value_signals': value_signal_counts,
                'underdog_sides': underdog_side_counts,
                'sharp_confirmation': sharp_confirmation_counts
            }
        ) 