"""
Business Metrics Validator
Calculates and validates business-specific metrics for MLB betting models.

Issue #42: Automated Model Validation & Testing Pipeline
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


class BusinessMetricsValidator:
    """
    Validates business metrics for MLB betting models.
    
    Implements business validation requirements from Issue #42:
    - ROI calculation and validation
    - Sharpe ratio calculation
    - Maximum drawdown analysis
    - Win rate analysis
    """
    
    def __init__(self):
        # Standard betting parameters
        self.standard_bet_size = 100.0  # $100 standard bet
        self.vig_rate = 0.045  # 4.5% average vig
        self.risk_free_rate = 0.02  # 2% annual risk-free rate
    
    async def calculate_business_metrics(
        self, 
        predictions: np.ndarray, 
        actual_outcomes: np.ndarray,
        test_data: pd.DataFrame,
        bet_size: float = 100.0
    ) -> Dict[str, float]:
        """
        Calculate comprehensive business metrics for model predictions.
        
        Args:
            predictions: Model predictions (0/1 for binary, probabilities for regression)
            actual_outcomes: Actual game outcomes (0/1)
            test_data: Test dataset with additional context
            bet_size: Size of each bet in dollars
            
        Returns:
            Dictionary of business metrics
        """
        try:
            # Convert to binary predictions if needed
            binary_predictions = self._convert_to_binary_predictions(predictions)
            
            # Calculate betting outcomes
            betting_results = self._calculate_betting_results(
                binary_predictions, actual_outcomes, bet_size
            )
            
            # Calculate ROI metrics
            roi_metrics = self._calculate_roi_metrics(betting_results)
            
            # Calculate risk metrics
            risk_metrics = self._calculate_risk_metrics(betting_results)
            
            # Calculate performance metrics
            performance_metrics = self._calculate_performance_metrics(
                binary_predictions, actual_outcomes
            )
            
            # Combine all metrics
            business_metrics = {}
            business_metrics.update(roi_metrics)
            business_metrics.update(risk_metrics)
            business_metrics.update(performance_metrics)
            
            return business_metrics
            
        except Exception as e:
            logger.error(f"Business metrics calculation failed: {e}")
            return {
                "roi_percentage": 0.0,
                "sharpe_ratio": 0.0,
                "maximum_drawdown": 100.0,  # Worst case
                "win_rate": 0.0
            }
    
    def _convert_to_binary_predictions(self, predictions: np.ndarray) -> np.ndarray:
        """Convert model predictions to binary betting decisions."""
        if predictions.dtype == bool or np.all(np.isin(predictions, [0, 1])):
            return predictions.astype(int)
        else:
            # For probability predictions, use 0.5 threshold
            return (predictions > 0.5).astype(int)
    
    def _calculate_betting_results(
        self, 
        predictions: np.ndarray, 
        outcomes: np.ndarray, 
        bet_size: float
    ) -> Dict[str, Any]:
        """Calculate betting results for each prediction."""
        # Simulate betting odds (in practice, these would come from test_data)
        # Using typical MLB odds: favorites around -110, underdogs around +110
        simulated_odds = np.random.choice([-110, +110], size=len(predictions))
        
        bet_results = []
        cumulative_pnl = 0.0
        cumulative_pnl_history = []
        
        for i, (pred, outcome, odds) in enumerate(zip(predictions, outcomes, simulated_odds)):
            if pred == 1:  # Model predicts to bet
                if outcome == 1:  # Winning bet
                    if odds > 0:  # Positive odds (underdog)
                        profit = bet_size * (odds / 100)
                    else:  # Negative odds (favorite)
                        profit = bet_size * (100 / abs(odds))
                    pnl = profit
                else:  # Losing bet
                    pnl = -bet_size
                
                bet_results.append({
                    'bet_number': i,
                    'prediction': pred,
                    'outcome': outcome,
                    'odds': odds,
                    'bet_size': bet_size,
                    'pnl': pnl,
                    'correct': pred == outcome
                })
                
                cumulative_pnl += pnl
            else:
                # No bet placed
                bet_results.append({
                    'bet_number': i,
                    'prediction': pred,
                    'outcome': outcome,
                    'odds': odds,
                    'bet_size': 0.0,
                    'pnl': 0.0,
                    'correct': True  # No bet = no loss
                })
            
            cumulative_pnl_history.append(cumulative_pnl)
        
        return {
            'bet_results': bet_results,
            'cumulative_pnl': cumulative_pnl,
            'cumulative_pnl_history': cumulative_pnl_history,
            'total_bets_placed': sum(1 for r in bet_results if r['bet_size'] > 0),
            'winning_bets': sum(1 for r in bet_results if r['pnl'] > 0),
            'losing_bets': sum(1 for r in bet_results if r['pnl'] < 0)
        }
    
    def _calculate_roi_metrics(self, betting_results: Dict[str, Any]) -> Dict[str, float]:
        """Calculate return on investment metrics."""
        total_invested = sum(r['bet_size'] for r in betting_results['bet_results'])
        total_return = betting_results['cumulative_pnl']
        
        if total_invested == 0:
            roi_percentage = 0.0
        else:
            roi_percentage = (total_return / total_invested) * 100
        
        return {
            'roi_percentage': roi_percentage,
            'total_invested': total_invested,
            'total_return': total_return,
            'net_profit': total_return
        }
    
    def _calculate_risk_metrics(self, betting_results: Dict[str, Any]) -> Dict[str, float]:
        """Calculate risk-adjusted performance metrics."""
        pnl_history = betting_results['cumulative_pnl_history']
        
        if len(pnl_history) == 0:
            return {
                'sharpe_ratio': 0.0,
                'maximum_drawdown': 0.0,
                'volatility': 0.0
            }
        
        # Calculate returns
        returns = np.diff(pnl_history, prepend=0)
        
        # Calculate Sharpe ratio
        if len(returns) > 1 and np.std(returns) > 0:
            excess_returns = returns - (self.risk_free_rate / 252)  # Daily risk-free rate
            sharpe_ratio = np.mean(excess_returns) / np.std(returns) * np.sqrt(252)  # Annualized
        else:
            sharpe_ratio = 0.0
        
        # Calculate maximum drawdown
        running_max = np.maximum.accumulate(pnl_history)
        drawdowns = (pnl_history - running_max) / np.maximum(running_max, 1.0) * 100
        maximum_drawdown = abs(np.min(drawdowns)) if len(drawdowns) > 0 else 0.0
        
        # Calculate volatility
        volatility = np.std(returns) * np.sqrt(252) if len(returns) > 1 else 0.0
        
        return {
            'sharpe_ratio': sharpe_ratio,
            'maximum_drawdown': maximum_drawdown,
            'volatility': volatility
        }
    
    def _calculate_performance_metrics(
        self, 
        predictions: np.ndarray, 
        outcomes: np.ndarray
    ) -> Dict[str, float]:
        """Calculate betting performance metrics."""
        # Only consider games where bets were placed
        bet_mask = predictions == 1
        
        if np.sum(bet_mask) == 0:
            return {
                'win_rate': 0.0,
                'bet_frequency': 0.0,
                'accuracy_on_bets': 0.0,
                'total_bets_analyzed': 0
            }
        
        bet_predictions = predictions[bet_mask]
        bet_outcomes = outcomes[bet_mask]
        
        # Win rate (percentage of winning bets)
        correct_bets = np.sum(bet_predictions == bet_outcomes)
        win_rate = (correct_bets / len(bet_predictions)) * 100
        
        # Bet frequency (percentage of games bet on)
        bet_frequency = (np.sum(bet_mask) / len(predictions)) * 100
        
        # Accuracy on bets placed
        accuracy_on_bets = win_rate  # Same as win rate for binary classification
        
        return {
            'win_rate': win_rate,
            'bet_frequency': bet_frequency, 
            'accuracy_on_bets': accuracy_on_bets,
            'total_bets_analyzed': len(bet_predictions)
        }
    
    def calculate_kelly_criterion(
        self, 
        win_probability: float, 
        odds: float
    ) -> float:
        """
        Calculate optimal bet size using Kelly Criterion.
        
        Args:
            win_probability: Probability of winning (0-1)
            odds: Betting odds (American format)
            
        Returns:
            Optimal fraction of bankroll to bet
        """
        if odds > 0:  # Positive odds (underdog)
            decimal_odds = (odds / 100) + 1
        else:  # Negative odds (favorite)
            decimal_odds = (100 / abs(odds)) + 1
        
        p = win_probability  # Probability of winning
        q = 1 - p  # Probability of losing
        b = decimal_odds - 1  # Net odds received on the wager
        
        # Kelly formula: f = (bp - q) / b
        kelly_fraction = (b * p - q) / b
        
        # Ensure non-negative and reasonable bounds
        return max(0.0, min(kelly_fraction, 0.25))  # Cap at 25% of bankroll
    
    def simulate_betting_strategy(
        self, 
        predictions: np.ndarray, 
        outcomes: np.ndarray,
        initial_bankroll: float = 10000.0,
        use_kelly: bool = False
    ) -> Dict[str, Any]:
        """
        Simulate complete betting strategy performance.
        
        Args:
            predictions: Model predictions
            outcomes: Actual outcomes
            initial_bankroll: Starting bankroll in dollars
            use_kelly: Whether to use Kelly Criterion for bet sizing
            
        Returns:
            Complete simulation results
        """
        bankroll = initial_bankroll
        bankroll_history = [bankroll]
        bet_history = []
        
        for pred, outcome in zip(predictions, outcomes):
            if pred == 1:  # Model suggests betting
                # Simulate odds
                odds = np.random.choice([-110, +110])
                
                if use_kelly:
                    # Use Kelly Criterion (simplified)
                    win_prob = 0.55  # Estimated win probability
                    kelly_fraction = self.calculate_kelly_criterion(win_prob, odds)
                    bet_size = bankroll * kelly_fraction
                else:
                    # Fixed bet size (1% of bankroll)
                    bet_size = bankroll * 0.01
                
                bet_size = max(bet_size, 10.0)  # Minimum $10 bet
                bet_size = min(bet_size, bankroll * 0.05)  # Max 5% of bankroll
                
                # Place bet
                if outcome == 1:  # Win
                    if odds > 0:
                        profit = bet_size * (odds / 100)
                    else:
                        profit = bet_size * (100 / abs(odds))
                    bankroll += profit
                else:  # Loss
                    bankroll -= bet_size
                
                bet_history.append({
                    'bet_size': bet_size,
                    'odds': odds,
                    'outcome': outcome,
                    'profit': profit if outcome == 1 else -bet_size,
                    'bankroll_after': bankroll
                })
            
            bankroll_history.append(bankroll)
        
        final_roi = ((bankroll - initial_bankroll) / initial_bankroll) * 100
        
        return {
            'initial_bankroll': initial_bankroll,
            'final_bankroll': bankroll,
            'total_roi': final_roi,
            'bankroll_history': bankroll_history,
            'bet_history': bet_history,
            'total_bets': len(bet_history),
            'max_bankroll': max(bankroll_history),
            'min_bankroll': min(bankroll_history)
        }