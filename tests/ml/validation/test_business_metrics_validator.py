"""
Unit tests for BusinessMetricsValidator.
Tests Issue #42: Automated Model Validation & Testing Pipeline
"""

import pytest
import numpy as np
import pandas as pd
from src.ml.validation.business_metrics_validator import BusinessMetricsValidator


class TestBusinessMetricsValidator:
    """Test BusinessMetricsValidator functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.validator = BusinessMetricsValidator()
    
    def test_initialization(self):
        """Test business metrics validator initialization."""
        assert self.validator is not None
        assert self.validator.standard_bet_size == 100.0
        assert self.validator.vig_rate == 0.045
        assert self.validator.risk_free_rate == 0.02
    
    @pytest.mark.asyncio
    async def test_calculate_business_metrics_basic(self):
        """Test basic business metrics calculation."""
        # Create simple test data
        predictions = np.array([1, 0, 1, 1, 0])  # Binary predictions
        outcomes = np.array([1, 0, 0, 1, 0])     # Actual outcomes
        test_data = pd.DataFrame({
            'feature1': [1, 2, 3, 4, 5],
            'feature2': [2, 4, 6, 8, 10]
        })
        
        metrics = await self.validator.calculate_business_metrics(
            predictions, outcomes, test_data
        )
        
        # Check that all expected metrics are present
        expected_metrics = [
            'roi_percentage', 'total_invested', 'total_return', 'net_profit',
            'sharpe_ratio', 'maximum_drawdown', 'volatility',
            'win_rate', 'bet_frequency', 'accuracy_on_bets'
        ]
        
        for metric in expected_metrics:
            assert metric in metrics
            assert isinstance(metrics[metric], (int, float))
    
    def test_convert_to_binary_predictions(self):
        """Test conversion of predictions to binary format."""
        # Test with already binary predictions
        binary_preds = np.array([0, 1, 1, 0])
        result = self.validator._convert_to_binary_predictions(binary_preds)
        np.testing.assert_array_equal(result, binary_preds)
        
        # Test with probability predictions
        prob_preds = np.array([0.3, 0.7, 0.8, 0.2])
        result = self.validator._convert_to_binary_predictions(prob_preds)
        expected = np.array([0, 1, 1, 0])  # Using 0.5 threshold
        np.testing.assert_array_equal(result, expected)
    
    def test_calculate_betting_results(self):
        """Test betting results calculation."""
        predictions = np.array([1, 1, 0, 1, 0])  # 3 bets placed
        outcomes = np.array([1, 0, 1, 1, 0])     # 2 wins on placed bets
        bet_size = 100.0
        
        results = self.validator._calculate_betting_results(predictions, outcomes, bet_size)
        
        # Check structure
        assert 'bet_results' in results
        assert 'cumulative_pnl' in results
        assert 'total_bets_placed' in results
        assert 'winning_bets' in results
        assert 'losing_bets' in results
        
        # Check bet counting
        assert results['total_bets_placed'] == 3  # Only games where prediction = 1
        assert len(results['bet_results']) == 5   # All games tracked
        
        # Check that no-bet games have zero bet size
        no_bet_games = [r for r in results['bet_results'] if r['prediction'] == 0]
        for game in no_bet_games:
            assert game['bet_size'] == 0.0
            assert game['pnl'] == 0.0
    
    def test_calculate_roi_metrics(self):
        """Test ROI metrics calculation."""
        # Mock betting results
        betting_results = {
            'bet_results': [
                {'bet_size': 100.0, 'pnl': 90.0},   # Win
                {'bet_size': 100.0, 'pnl': -100.0}, # Loss
                {'bet_size': 100.0, 'pnl': 90.0},   # Win
                {'bet_size': 0.0, 'pnl': 0.0}       # No bet
            ],
            'cumulative_pnl': 80.0
        }
        
        roi_metrics = self.validator._calculate_roi_metrics(betting_results)
        
        assert roi_metrics['total_invested'] == 300.0  # 3 bets of 100 each
        assert roi_metrics['total_return'] == 80.0
        assert roi_metrics['roi_percentage'] == (80.0 / 300.0) * 100  # ~26.67%
        assert roi_metrics['net_profit'] == 80.0
    
    def test_calculate_risk_metrics(self):
        """Test risk metrics calculation."""
        # Mock PnL history with drawdown
        betting_results = {
            'cumulative_pnl_history': [0, 100, 50, 150, 100, 200]  # Has drawdowns
        }
        
        risk_metrics = self.validator._calculate_risk_metrics(betting_results)
        
        assert 'sharpe_ratio' in risk_metrics
        assert 'maximum_drawdown' in risk_metrics
        assert 'volatility' in risk_metrics
        
        # Maximum drawdown should be positive (percentage)
        assert risk_metrics['maximum_drawdown'] >= 0
        
        # Should have some drawdown in this sequence
        assert risk_metrics['maximum_drawdown'] > 0
    
    def test_calculate_performance_metrics(self):
        """Test performance metrics calculation."""
        # Test with some bets placed
        predictions = np.array([1, 1, 0, 1, 0])  # 3 bets placed
        outcomes = np.array([1, 0, 1, 1, 0])     # 2 correct bets placed
        
        perf_metrics = self.validator._calculate_performance_metrics(predictions, outcomes)
        
        assert perf_metrics['win_rate'] == (2/3) * 100  # 66.67% win rate
        assert perf_metrics['bet_frequency'] == (3/5) * 100  # 60% bet frequency
        assert perf_metrics['accuracy_on_bets'] == (2/3) * 100  # Same as win rate
        assert perf_metrics['total_bets_analyzed'] == 3
    
    def test_calculate_performance_metrics_no_bets(self):
        """Test performance metrics when no bets placed."""
        predictions = np.array([0, 0, 0, 0, 0])  # No bets placed
        outcomes = np.array([1, 0, 1, 1, 0])
        
        perf_metrics = self.validator._calculate_performance_metrics(predictions, outcomes)
        
        assert perf_metrics['win_rate'] == 0.0
        assert perf_metrics['bet_frequency'] == 0.0
        assert perf_metrics['accuracy_on_bets'] == 0.0
        assert perf_metrics['total_bets_analyzed'] == 0
    
    def test_kelly_criterion_positive_odds(self):
        """Test Kelly criterion with positive odds (underdog)."""
        # Test with 60% win probability and +150 odds
        kelly_fraction = self.validator.calculate_kelly_criterion(0.6, 150)
        
        assert 0 <= kelly_fraction <= 0.25  # Should be reasonable fraction
        assert isinstance(kelly_fraction, float)
    
    def test_kelly_criterion_negative_odds(self):
        """Test Kelly criterion with negative odds (favorite)."""
        # Test with 70% win probability and -110 odds
        kelly_fraction = self.validator.calculate_kelly_criterion(0.7, -110)
        
        assert 0 <= kelly_fraction <= 0.25  # Should be reasonable fraction
        assert isinstance(kelly_fraction, float)
    
    def test_kelly_criterion_edge_cases(self):
        """Test Kelly criterion edge cases."""
        # Test with very low win probability
        kelly_fraction = self.validator.calculate_kelly_criterion(0.1, 100)
        assert kelly_fraction == 0.0  # Should not bet with low probability
        
        # Test with very high win probability
        kelly_fraction = self.validator.calculate_kelly_criterion(0.9, -110)
        assert 0 < kelly_fraction <= 0.25  # Should bet but capped at 25%
    
    def test_simulate_betting_strategy(self):
        """Test complete betting strategy simulation."""
        predictions = np.array([1, 1, 0, 1, 0, 1])  # 4 bets placed
        outcomes = np.array([1, 0, 1, 1, 0, 1])     # Mixed results
        
        simulation = self.validator.simulate_betting_strategy(
            predictions, outcomes, initial_bankroll=10000.0
        )
        
        # Check structure
        expected_keys = [
            'initial_bankroll', 'final_bankroll', 'total_roi',
            'bankroll_history', 'bet_history', 'total_bets',
            'max_bankroll', 'min_bankroll'
        ]
        
        for key in expected_keys:
            assert key in simulation
        
        # Check logical constraints
        assert simulation['initial_bankroll'] == 10000.0
        assert simulation['final_bankroll'] > 0  # Should not go broke
        assert len(simulation['bankroll_history']) == len(predictions) + 1  # +1 for initial
        assert simulation['total_bets'] == 4  # Number of bets placed
        assert simulation['max_bankroll'] >= simulation['final_bankroll']
        assert simulation['min_bankroll'] <= simulation['final_bankroll']
    
    def test_simulate_betting_strategy_kelly(self):
        """Test betting strategy simulation with Kelly sizing."""
        predictions = np.array([1, 1, 1])
        outcomes = np.array([1, 1, 1])  # All wins
        
        simulation = self.validator.simulate_betting_strategy(
            predictions, outcomes, initial_bankroll=10000.0, use_kelly=True
        )
        
        # Should have positive results with all wins
        assert simulation['final_bankroll'] > simulation['initial_bankroll']
        assert simulation['total_roi'] > 0
        assert simulation['total_bets'] == 3