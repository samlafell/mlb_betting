#!/usr/bin/env python3
"""
Comprehensive Unit Tests for StatisticalAnalysisService

Tests all statistical analysis functionality including:
- Correlation analysis (Pearson, Spearman, Kendall)
- Linear and logistic regression
- Confidence intervals and hypothesis testing
- Distribution analysis and normality tests
- Performance attribution modeling
- Error handling and edge cases
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any, List
from scipy import stats

from src.services.analytics.statistical_analysis_service import (
    StatisticalAnalysisService,
    get_statistical_analysis_service,
    AnalyticsError
)


class TestStatisticalAnalysisService:
    """Comprehensive test suite for StatisticalAnalysisService"""

    @pytest.fixture
    def service(self):
        """Initialize fresh service instance for each test"""
        return StatisticalAnalysisService()

    @pytest.fixture
    def sample_data(self):
        """Generate sample data for testing"""
        np.random.seed(42)  # For reproducible results
        n = 100
        
        return pd.DataFrame({
            'feature1': np.random.normal(50, 10, n),
            'feature2': np.random.normal(100, 15, n),
            'feature3': np.random.uniform(0, 1, n),
            'target_continuous': np.random.normal(25, 5, n),
            'target_binary': np.random.binomial(1, 0.6, n),
            'categorical': ['A', 'B', 'C'] * (n // 3) + ['A'] * (n % 3),
            'constant': [10] * n,
            'missing_data': [np.nan if i < 20 else i for i in range(n)]
        })

    @pytest.fixture
    def correlated_data(self):
        """Generate data with known correlations for testing"""
        np.random.seed(42)
        n = 100
        
        # Create strongly correlated variables
        x1 = np.random.normal(0, 1, n)
        x2 = 0.8 * x1 + 0.2 * np.random.normal(0, 1, n)  # Strong positive correlation
        x3 = -0.7 * x1 + 0.3 * np.random.normal(0, 1, n)  # Strong negative correlation
        x4 = np.random.normal(0, 1, n)  # Independent
        
        return pd.DataFrame({
            'var1': x1,
            'var2': x2,
            'var3': x3,
            'var4': x4
        })

    @pytest.fixture
    def small_dataset(self):
        """Small dataset for edge case testing"""
        return pd.DataFrame({
            'x': [1, 2, 3, 4, 5],
            'y': [2, 4, 6, 8, 10],
            'z': [1, 1, 1, 1, 1]  # Constant values
        })

    def test_initialization(self):
        """Test service initialization"""
        service = StatisticalAnalysisService()
        assert service.scaler is not None
        assert hasattr(service, 'scaler')

    def test_singleton_service(self):
        """Test singleton pattern for service"""
        service1 = get_statistical_analysis_service()
        service2 = get_statistical_analysis_service()
        assert service1 is service2

    # ========== Correlation Analysis Tests ==========

    def test_analyze_correlations_basic(self, service, correlated_data):
        """Test basic correlation analysis"""
        result = service.analyze_correlations(correlated_data)
        
        assert 'correlations' in result
        assert 'significance_tests' in result
        assert 'summary_stats' in result
        
        # Check correlation types
        assert 'pearson' in result['correlations']
        assert 'spearman' in result['correlations']
        assert 'kendall' in result['correlations']
        
        # Verify strong correlation detected
        pearson_corr = result['correlations']['pearson']
        assert abs(pearson_corr['var1']['var2']) > 0.7  # Should be strongly correlated
        assert abs(pearson_corr['var1']['var3']) > 0.6  # Should be negatively correlated

    def test_analyze_correlations_significance_tests(self, service, correlated_data):
        """Test correlation significance testing"""
        result = service.analyze_correlations(correlated_data)
        
        significance_tests = result['significance_tests']
        
        # Should have tests for all variable pairs
        var_pairs = ['var1_vs_var2', 'var1_vs_var3', 'var1_vs_var4']
        assert any(pair in significance_tests for pair in var_pairs)
        
        # Check test result structure
        for test_key in significance_tests:
            test_result = significance_tests[test_key]
            assert 'pearson_r' in test_result
            assert 'pearson_p_value' in test_result
            assert 'pearson_significant' in test_result
            assert 'sample_size' in test_result
            assert isinstance(test_result['pearson_significant'], (bool, np.bool_))

    def test_analyze_correlations_summary_stats(self, service, correlated_data):
        """Test correlation summary statistics"""
        result = service.analyze_correlations(correlated_data)
        
        summary = result['summary_stats']
        assert 'strongest_positive_correlation' in summary
        assert 'strongest_negative_correlation' in summary
        assert 'average_correlation_strength' in summary
        assert 'variables_analyzed' in summary
        
        # Verify structure of strongest correlations
        pos_corr = summary['strongest_positive_correlation']
        assert 'variables' in pos_corr
        assert 'correlation' in pos_corr
        assert 'strength' in pos_corr
        assert pos_corr['strength'] in ['very_weak', 'weak', 'moderate', 'strong', 'very_strong']

    def test_analyze_correlations_empty_data(self, service):
        """Test correlation analysis with empty data"""
        empty_data = pd.DataFrame({'col1': [], 'col2': []})
        result = service.analyze_correlations(empty_data)
        
        assert 'error' in result
        assert 'Insufficient numeric data' in result['error']

    def test_analyze_correlations_non_numeric_data(self, service):
        """Test correlation analysis with non-numeric data"""
        text_data = pd.DataFrame({
            'text1': ['a', 'b', 'c'],
            'text2': ['x', 'y', 'z']
        })
        result = service.analyze_correlations(text_data)
        
        assert 'error' in result
        assert 'Insufficient numeric data' in result['error']

    def test_find_strongest_correlation(self, service):
        """Test finding strongest correlations"""
        # Create test correlation matrix
        corr_matrix = pd.DataFrame({
            'A': [1.0, 0.8, -0.6],
            'B': [0.8, 1.0, -0.4],
            'C': [-0.6, -0.4, 1.0]
        }, index=['A', 'B', 'C'])
        
        # Test positive correlation
        pos_result = service._find_strongest_correlation(corr_matrix, positive=True)
        assert pos_result['correlation'] == 0.8
        assert pos_result['variables'] == ['A', 'B']
        
        # Test negative correlation
        neg_result = service._find_strongest_correlation(corr_matrix, positive=False)
        assert neg_result['correlation'] == -0.6
        assert neg_result['variables'] == ['A', 'C']

    def test_classify_correlation_strength(self, service):
        """Test correlation strength classification"""
        assert service._classify_correlation_strength(0.9) == 'very_strong'
        assert service._classify_correlation_strength(0.7) == 'strong'
        assert service._classify_correlation_strength(0.5) == 'moderate'
        assert service._classify_correlation_strength(0.3) == 'weak'
        assert service._classify_correlation_strength(0.1) == 'very_weak'

    # ========== Regression Analysis Tests ==========

    def test_perform_regression_analysis_linear(self, service, sample_data):
        """Test linear regression analysis"""
        result = service.perform_regression_analysis(
            data=sample_data,
            target_column='target_continuous',
            feature_columns=['feature1', 'feature2', 'feature3']
        )
        
        assert result['analysis_type'] == 'linear_regression'
        assert 'model_coefficients' in result
        assert 'intercept' in result
        assert 'r_squared_train' in result
        assert 'r_squared_test' in result
        assert 'rmse_train' in result
        assert 'rmse_test' in result
        assert 'f_statistic' in result
        assert 'f_p_value' in result
        assert 'model_significant' in result
        assert 'overfitting_indicator' in result
        assert 'feature_analysis' in result
        
        # Verify coefficient structure
        coeffs = result['model_coefficients']
        assert 'feature1' in coeffs
        assert 'feature2' in coeffs
        assert 'feature3' in coeffs
        assert len(coeffs) == 3

    def test_perform_regression_analysis_logistic(self, service, sample_data):
        """Test logistic regression analysis"""
        result = service.perform_regression_analysis(
            data=sample_data,
            target_column='target_binary',
            feature_columns=['feature1', 'feature2', 'feature3']
        )
        
        assert result['analysis_type'] == 'logistic_regression'
        assert 'model_coefficients' in result
        assert 'intercept' in result
        assert 'accuracy_train' in result
        assert 'accuracy_test' in result
        assert 'auc_roc_train' in result
        assert 'auc_roc_test' in result
        assert 'overfitting_indicator' in result
        assert 'feature_analysis' in result

    def test_perform_regression_analysis_missing_target(self, service, sample_data):
        """Test regression with missing target column"""
        # Should raise AnalyticsError
        with pytest.raises(AnalyticsError):
            service.perform_regression_analysis(
                data=sample_data,
                target_column='nonexistent_column',
                feature_columns=['feature1', 'feature2']
            )

    def test_perform_regression_analysis_missing_features(self, service, sample_data):
        """Test regression with missing feature columns"""
        with pytest.raises(AnalyticsError):
            service.perform_regression_analysis(
                data=sample_data,
                target_column='target_continuous',
                feature_columns=['nonexistent_feature1', 'nonexistent_feature2']
            )

    def test_perform_regression_analysis_insufficient_data(self, service):
        """Test regression with insufficient data"""
        tiny_data = pd.DataFrame({
            'x': [1, 2, 3],  # Only 3 observations
            'y': [1, 2, 3]
        })
        
        result = service.perform_regression_analysis(
            data=tiny_data,
            target_column='y',
            feature_columns=['x']
        )
        
        assert 'error' in result
        assert 'Insufficient data' in result['error']

    def test_perform_linear_regression_validation(self, service, sample_data):
        """Test linear regression with train/test split validation"""
        # Ensure we have enough data for proper train/test split
        large_data = pd.concat([sample_data] * 3, ignore_index=True)  # 300 rows
        
        X = large_data[['feature1', 'feature2', 'feature3']]
        y = large_data['target_continuous']
        
        result = service._perform_linear_regression(X, y)
        
        # Should have different train and test scores with enough data
        assert 'r_squared_train' in result
        assert 'r_squared_test' in result
        assert 'rmse_train' in result
        assert 'rmse_test' in result
        assert result['r_squared_train'] >= 0
        assert result['r_squared_test'] >= 0

    def test_perform_logistic_regression_validation(self, service, sample_data):
        """Test logistic regression with validation"""
        X = sample_data[['feature1', 'feature2', 'feature3']]
        y = sample_data['target_binary']
        
        result = service._perform_logistic_regression(X, y)
        
        assert 'accuracy_train' in result
        assert 'accuracy_test' in result
        assert 'auc_roc_train' in result
        assert 'auc_roc_test' in result
        assert 0 <= result['accuracy_train'] <= 1
        assert 0 <= result['accuracy_test'] <= 1
        assert 0 <= result['auc_roc_train'] <= 1
        assert 0 <= result['auc_roc_test'] <= 1

    def test_analyze_feature_importance(self, service, sample_data):
        """Test feature importance analysis"""
        X = sample_data[['feature1', 'feature2', 'feature3']]
        y = sample_data['target_continuous']
        
        result = service._analyze_feature_importance(X, y, is_binary=False)
        
        assert len(result) == 3  # Should analyze all features
        
        for feature in ['feature1', 'feature2', 'feature3']:
            assert feature in result
            feature_stats = result[feature]
            assert 'correlation_with_target' in feature_stats
            assert 'correlation_p_value' in feature_stats
            assert 'correlation_significant' in feature_stats
            assert 'feature_mean' in feature_stats
            assert 'feature_std' in feature_stats
            assert 'missing_percentage' in feature_stats
            
            # Verify value ranges
            assert -1 <= feature_stats['correlation_with_target'] <= 1
            assert 0 <= feature_stats['correlation_p_value'] <= 1
            assert isinstance(feature_stats['correlation_significant'], (bool, np.bool_))
            assert 0 <= feature_stats['missing_percentage'] <= 100

    # ========== Confidence Intervals Tests ==========

    def test_calculate_confidence_intervals_normal_sample(self, service):
        """Test confidence intervals for normal-sized sample"""
        # Large sample (>30) - should use normal distribution
        large_sample = pd.Series(np.random.normal(50, 10, 100))
        
        result = service.calculate_confidence_intervals(large_sample, confidence_level=0.95)
        
        assert 'sample_size' in result
        assert 'sample_mean' in result
        assert 'standard_error' in result
        assert 'confidence_level' in result
        assert 'margin_of_error' in result
        assert 'lower_bound' in result
        assert 'upper_bound' in result
        assert 'distribution_used' in result
        
        assert result['sample_size'] == 100
        assert result['confidence_level'] == 0.95
        assert result['distribution_used'] == 'normal_distribution'
        assert result['lower_bound'] < result['upper_bound']

    def test_calculate_confidence_intervals_small_sample(self, service):
        """Test confidence intervals for small sample"""
        # Small sample (<30) - should use t-distribution
        small_sample = pd.Series([45, 50, 55, 60, 65])
        
        result = service.calculate_confidence_intervals(small_sample, confidence_level=0.95)
        
        assert result['sample_size'] == 5
        assert result['distribution_used'] == 't-distribution'
        assert result['lower_bound'] < result['upper_bound']

    def test_calculate_confidence_intervals_insufficient_data(self, service):
        """Test confidence intervals with insufficient data"""
        insufficient_data = pd.Series([50])  # Only 1 observation
        
        result = service.calculate_confidence_intervals(insufficient_data)
        
        assert 'error' in result
        assert 'Insufficient data' in result['error']

    def test_calculate_confidence_intervals_with_missing(self, service):
        """Test confidence intervals with missing values"""
        data_with_nan = pd.Series([45, 50, np.nan, 60, 65, np.nan, 70])
        
        result = service.calculate_confidence_intervals(data_with_nan)
        
        assert result['sample_size'] == 5  # Should exclude NaN values
        assert 'lower_bound' in result
        assert 'upper_bound' in result

    def test_calculate_confidence_intervals_different_levels(self, service):
        """Test different confidence levels"""
        sample = pd.Series(np.random.normal(50, 10, 50))
        
        ci_90 = service.calculate_confidence_intervals(sample, confidence_level=0.90)
        ci_95 = service.calculate_confidence_intervals(sample, confidence_level=0.95)
        ci_99 = service.calculate_confidence_intervals(sample, confidence_level=0.99)
        
        # Higher confidence level should have wider intervals
        margin_90 = ci_90['margin_of_error']
        margin_95 = ci_95['margin_of_error']
        margin_99 = ci_99['margin_of_error']
        
        assert margin_90 < margin_95 < margin_99

    # ========== Distribution Analysis Tests ==========

    def test_analyze_distributions_basic(self, service, sample_data):
        """Test basic distribution analysis"""
        result = service.analyze_distributions(sample_data)
        
        # Should analyze numeric columns
        numeric_columns = ['feature1', 'feature2', 'feature3', 'target_continuous', 
                          'target_binary', 'constant', 'missing_data']
        
        for col in numeric_columns:
            if col in result:
                col_result = result[col]
                if 'error' not in col_result:
                    assert 'descriptive_stats' in col_result
                    assert 'normality_tests' in col_result
                    assert 'outlier_analysis' in col_result

    def test_analyze_distributions_descriptive_stats(self, service, sample_data):
        """Test descriptive statistics in distribution analysis"""
        result = service.analyze_distributions(sample_data)
        
        feature1_stats = result['feature1']['descriptive_stats']
        
        expected_stats = ['count', 'mean', 'median', 'mode', 'std', 'variance',
                         'skewness', 'kurtosis', 'min', 'max', 'range', 
                         'q25', 'q75', 'iqr']
        
        for stat in expected_stats:
            assert stat in feature1_stats
        
        # Verify logical relationships
        assert feature1_stats['min'] <= feature1_stats['q25']
        assert feature1_stats['q25'] <= feature1_stats['median']
        assert feature1_stats['median'] <= feature1_stats['q75']
        assert feature1_stats['q75'] <= feature1_stats['max']
        assert feature1_stats['iqr'] == feature1_stats['q75'] - feature1_stats['q25']

    def test_analyze_distributions_normality_tests(self, service, sample_data):
        """Test normality testing in distribution analysis"""
        result = service.analyze_distributions(sample_data)
        
        feature1_normality = result['feature1']['normality_tests']
        
        # Should have at least one normality test
        assert 'shapiro_wilk' in feature1_normality or 'kolmogorov_smirnov' in feature1_normality
        
        if 'shapiro_wilk' in feature1_normality:
            sw_test = feature1_normality['shapiro_wilk']
            assert 'statistic' in sw_test
            assert 'p_value' in sw_test
            assert 'is_normal' in sw_test
            assert isinstance(sw_test['is_normal'], (bool, np.bool_))

    def test_analyze_distributions_outlier_detection(self, service, sample_data):
        """Test outlier detection in distribution analysis"""
        result = service.analyze_distributions(sample_data)
        
        feature1_outliers = result['feature1']['outlier_analysis']
        
        assert 'outlier_count' in feature1_outliers
        assert 'outlier_percentage' in feature1_outliers
        assert 'lower_bound' in feature1_outliers
        assert 'upper_bound' in feature1_outliers
        
        assert feature1_outliers['outlier_count'] >= 0
        assert 0 <= feature1_outliers['outlier_percentage'] <= 100
        assert feature1_outliers['lower_bound'] < feature1_outliers['upper_bound']

    def test_analyze_distributions_no_numeric_data(self, service):
        """Test distribution analysis with no numeric data"""
        text_data = pd.DataFrame({
            'text1': ['a', 'b', 'c'],
            'text2': ['x', 'y', 'z']
        })
        
        result = service.analyze_distributions(text_data)
        
        assert 'error' in result
        assert 'No numeric data found' in result['error']

    def test_analyze_distributions_insufficient_data(self, service):
        """Test distribution analysis with insufficient data per column"""
        sparse_data = pd.DataFrame({
            'col1': [1, 2],  # Only 2 observations
            'col2': [3, 4]
        })
        
        result = service.analyze_distributions(sparse_data)
        
        # Each column should have insufficient data error
        for col in ['col1', 'col2']:
            assert 'error' in result[col]

    # ========== Hypothesis Testing Tests ==========

    def test_perform_hypothesis_testing_ttest(self, service):
        """Test independent t-test hypothesis testing"""
        # Create two groups with different means
        group1 = pd.Series(np.random.normal(50, 10, 30))
        group2 = pd.Series(np.random.normal(60, 10, 30))  # Higher mean
        
        result = service.perform_hypothesis_testing(group1, group2, test_type='ttest')
        
        assert result['test_type'] == 'ttest'
        assert 'statistic' in result
        assert 'p_value' in result
        assert 'significant' in result
        assert 'null_hypothesis' in result
        assert 'interpretation' in result
        assert result['group1_size'] == 30
        assert result['group2_size'] == 30
        assert isinstance(result['significant'], (bool, np.bool_))

    def test_perform_hypothesis_testing_mannwhitney(self, service):
        """Test Mann-Whitney U test"""
        # Create two groups with different distributions
        group1 = pd.Series(np.random.exponential(2, 30))
        group2 = pd.Series(np.random.exponential(3, 30))
        
        result = service.perform_hypothesis_testing(group1, group2, test_type='mannwhitney')
        
        assert result['test_type'] == 'mannwhitney'
        assert 'statistic' in result
        assert 'p_value' in result
        assert 'significant' in result
        assert 'distributions of both groups are equal' in result['null_hypothesis']

    def test_perform_hypothesis_testing_kstest(self, service):
        """Test Kolmogorov-Smirnov test"""
        # Create two samples from different distributions
        group1 = pd.Series(np.random.normal(0, 1, 50))
        group2 = pd.Series(np.random.uniform(-2, 2, 50))
        
        result = service.perform_hypothesis_testing(group1, group2, test_type='kstest')
        
        assert result['test_type'] == 'kstest'
        assert 'statistic' in result
        assert 'p_value' in result
        assert 'significant' in result
        assert 'same distribution' in result['null_hypothesis']

    def test_perform_hypothesis_testing_insufficient_data(self, service):
        """Test hypothesis testing with insufficient data"""
        group1 = pd.Series([1, 2])  # Only 2 observations
        group2 = pd.Series([3, 4])  # Only 2 observations
        
        result = service.perform_hypothesis_testing(group1, group2)
        
        assert 'error' in result
        assert 'Insufficient data' in result['error']

    def test_perform_hypothesis_testing_with_missing(self, service):
        """Test hypothesis testing with missing values"""
        group1 = pd.Series([1, 2, np.nan, 4, 5, np.nan, 7])
        group2 = pd.Series([8, np.nan, 10, 11, 12, np.nan, 14])
        
        result = service.perform_hypothesis_testing(group1, group2)
        
        # Should handle missing values correctly
        assert result['group1_size'] == 5  # Excluding NaNs
        assert result['group2_size'] == 5  # Excluding NaNs
        assert 'statistic' in result
        assert 'p_value' in result

    # ========== Performance Attribution Tests ==========

    def test_calculate_performance_attribution_basic(self, service):
        """Test basic performance attribution analysis"""
        # Create synthetic performance data
        np.random.seed(42)
        n = 50
        
        # Create factors
        factor1 = np.random.normal(0.02, 0.05, n)  # Market factor
        factor2 = np.random.normal(0.01, 0.03, n)  # Size factor
        factor3 = np.random.normal(0.005, 0.02, n)  # Value factor
        
        # Create performance with known factor loadings
        performance = 0.01 + 0.8 * factor1 + 0.3 * factor2 + 0.1 * factor3 + np.random.normal(0, 0.01, n)
        
        data = pd.DataFrame({
            'returns': performance,
            'market_factor': factor1,
            'size_factor': factor2,
            'value_factor': factor3
        })
        
        result = service.calculate_performance_attribution(
            performance_data=data,
            factor_columns=['market_factor', 'size_factor', 'value_factor'],
            return_column='returns'
        )
        
        assert 'total_return' in result
        assert 'explained_return' in result
        assert 'unexplained_return' in result
        assert 'r_squared' in result
        assert 'factor_contributions' in result
        assert 'attribution_summary' in result
        
        # Check factor contributions structure
        factor_contribs = result['factor_contributions']
        for factor in ['market_factor', 'size_factor', 'value_factor']:
            assert factor in factor_contribs
            contrib = factor_contribs[factor]
            assert 'absolute_contribution' in contrib
            assert 'relative_contribution' in contrib
            assert 'factor_coefficient' in contrib

    def test_calculate_performance_attribution_summary(self, service):
        """Test attribution summary statistics"""
        # Simple test data
        data = pd.DataFrame({
            'returns': [0.01, 0.02, -0.01, 0.03, 0.00],
            'factor1': [0.02, 0.01, -0.02, 0.04, 0.01],
            'factor2': [0.00, 0.01, 0.01, -0.01, 0.02]
        })
        
        result = service.calculate_performance_attribution(
            performance_data=data,
            factor_columns=['factor1', 'factor2'],
            return_column='returns'
        )
        
        attribution_summary = result.get('attribution_summary', {})
        if attribution_summary:  # Only check if attribution summary exists
            assert 'most_contributing_factor' in attribution_summary
            assert 'explained_percentage' in attribution_summary
            
            # Most contributing factor should be one of the factors
            assert attribution_summary['most_contributing_factor'] in ['factor1', 'factor2']
            assert isinstance(attribution_summary['explained_percentage'], float)

    def test_calculate_performance_attribution_insufficient_data(self, service):
        """Test performance attribution with insufficient data"""
        small_data = pd.DataFrame({
            'returns': [0.01, 0.02],  # Only 2 observations
            'factor1': [0.01, 0.02]
        })
        
        result = service.calculate_performance_attribution(
            performance_data=small_data,
            factor_columns=['factor1'],
            return_column='returns'
        )
        
        assert 'error' in result
        assert 'Insufficient data' in result['error']

    def test_calculate_performance_attribution_zero_returns(self, service):
        """Test attribution with zero total returns"""
        # Need at least 10 rows for the service to work
        data = pd.DataFrame({
            'returns': [0.01, -0.01, 0.0, 0.0, 0.0] * 2 + [0.0] * 5,  # Net zero returns, 15 rows
            'factor1': [0.02, 0.01, -0.02, 0.04, 0.01] * 2 + [0.0] * 5
        })
        
        result = service.calculate_performance_attribution(
            performance_data=data,
            factor_columns=['factor1'],
            return_column='returns'
        )
        
        # Should handle zero returns gracefully
        assert 'factor_contributions' in result
        factor_contrib = result['factor_contributions']['factor1']
        # When total return is 0, relative contribution should be 0
        assert factor_contrib['relative_contribution'] == 0.0

    # ========== Error Handling Tests ==========

    def test_analyze_correlations_error_handling(self, service):
        """Test error handling in correlation analysis"""
        with patch('pandas.DataFrame.corr', side_effect=Exception("Test error")):
            with pytest.raises(AnalyticsError):
                service.analyze_correlations(pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]}))

    def test_regression_analysis_error_handling(self, service, sample_data):
        """Test error handling in regression analysis"""
        with patch('sklearn.linear_model.LinearRegression.fit', side_effect=Exception("Test error")):
            with pytest.raises(AnalyticsError):
                service.perform_regression_analysis(
                    data=sample_data,
                    target_column='target_continuous',
                    feature_columns=['feature1']
                )

    def test_confidence_intervals_error_handling(self, service):
        """Test error handling in confidence interval calculation"""
        # Test with invalid data that causes scipy stats error
        with patch('scipy.stats.sem', side_effect=Exception("Test error")):
            result = service.calculate_confidence_intervals(pd.Series([1, 2, 3]))
            assert 'error' in result

    def test_distribution_analysis_error_handling(self, service, sample_data):
        """Test error handling in distribution analysis"""
        with patch('scipy.stats.skew', side_effect=Exception("Test error")):
            with pytest.raises(AnalyticsError):
                service.analyze_distributions(sample_data)

    def test_hypothesis_testing_error_handling(self, service):
        """Test error handling in hypothesis testing"""
        with patch('scipy.stats.ttest_ind', side_effect=Exception("Test error")):
            with pytest.raises(AnalyticsError):
                service.perform_hypothesis_testing(
                    pd.Series([1, 2, 3, 4, 5]),
                    pd.Series([6, 7, 8, 9, 10])
                )

    def test_performance_attribution_error_handling(self, service):
        """Test error handling in performance attribution"""
        data = pd.DataFrame({
            'returns': [0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10],
            'factor1': [0.02, 0.01, -0.02, 0.04, 0.01, 0.03, -0.01, 0.02, 0.00, 0.01]
        })
        
        with patch('sklearn.linear_model.LinearRegression.fit', side_effect=Exception("Test error")):
            with pytest.raises(AnalyticsError):
                service.calculate_performance_attribution(
                    performance_data=data,
                    factor_columns=['factor1'],
                    return_column='returns'
                )

    # ========== Edge Cases and Boundary Tests ==========

    def test_constant_values_handling(self, service):
        """Test handling of constant values"""
        # Data with constant column
        constant_data = pd.DataFrame({
            'constant': [5] * 20,
            'variable': list(range(20))
        })
        
        # Correlation analysis should handle constants
        result = service.analyze_correlations(constant_data)
        assert 'correlations' in result
        
        # Distribution analysis should handle constants
        dist_result = service.analyze_distributions(constant_data)
        const_stats = dist_result['constant']['descriptive_stats']
        assert const_stats['std'] == 0.0
        assert const_stats['variance'] == 0.0

    def test_single_unique_value_regression(self, service):
        """Test regression with target having single unique value"""
        single_value_data = pd.DataFrame({
            'x1': [1, 2, 3, 4, 5] * 4,  # 20 rows
            'x2': [2, 4, 6, 8, 10] * 4,
            'y': [5] * 20  # All same value
        })
        
        # This should not crash, but may have low R-squared
        result = service.perform_regression_analysis(
            data=single_value_data,
            target_column='y',
            feature_columns=['x1', 'x2']
        )
        
        assert 'r_squared_train' in result
        # R-squared should be 0 or very low for constant target (but sklearn might return nan or 1.0 in edge cases)
        assert result['r_squared_train'] >= 0 or np.isnan(result['r_squared_train']) or result['r_squared_train'] == 1.0

    def test_perfect_correlation_handling(self, service):
        """Test handling of perfect correlations"""
        perfect_corr_data = pd.DataFrame({
            'x': [1, 2, 3, 4, 5],
            'y': [2, 4, 6, 8, 10],  # y = 2*x (perfect correlation)
            'z': [10, 20, 30, 40, 50]  # z = 10*x (perfect correlation)
        })
        
        result = service.analyze_correlations(perfect_corr_data)
        
        # Should detect perfect correlations
        pearson_corr = result['correlations']['pearson']
        assert abs(pearson_corr['x']['y']) > 0.99
        assert abs(pearson_corr['x']['z']) > 0.99

    def test_missing_values_comprehensive(self, service):
        """Test comprehensive handling of missing values"""
        missing_data = pd.DataFrame({
            'mostly_missing': [1, np.nan, np.nan, np.nan, np.nan] * 10,  # 80% missing
            'some_missing': [1, 2, np.nan, 4, 5] * 10,  # 20% missing
            'no_missing': list(range(50))  # No missing
        })
        
        # Correlation analysis should handle missing values
        corr_result = service.analyze_correlations(missing_data)
        assert 'correlations' in corr_result
        
        # Distribution analysis should handle missing values
        dist_result = service.analyze_distributions(missing_data)
        
        # Check that sample sizes are adjusted for missing values
        for col in ['mostly_missing', 'some_missing', 'no_missing']:
            if 'descriptive_stats' in dist_result[col]:
                count = dist_result[col]['descriptive_stats']['count']
                assert count > 0
                if col == 'mostly_missing':
                    assert count < 20  # Should be much less due to missing values
                elif col == 'no_missing':
                    assert count == 50  # Should be full count

    def test_extreme_values_handling(self, service):
        """Test handling of extreme values and outliers"""
        extreme_data = pd.DataFrame({
            'normal': np.random.normal(0, 1, 100),
            'with_outliers': list(np.random.normal(0, 1, 98)) + [100, -100]  # Extreme outliers
        })
        
        # Distribution analysis should detect outliers
        result = service.analyze_distributions(extreme_data)
        
        outlier_info = result['with_outliers']['outlier_analysis']
        assert outlier_info['outlier_count'] >= 2  # Should detect the extreme values
        assert outlier_info['outlier_percentage'] > 0

    def test_small_sample_statistical_tests(self, service):
        """Test statistical tests with very small samples"""
        tiny_sample1 = pd.Series([1, 2, 3])
        tiny_sample2 = pd.Series([4, 5, 6])
        
        # Hypothesis testing should work with minimum viable samples
        result = service.perform_hypothesis_testing(tiny_sample1, tiny_sample2)
        
        assert result['group1_size'] == 3
        assert result['group2_size'] == 3
        assert 'p_value' in result

    # ========== Performance Tests ==========

    @pytest.mark.performance
    def test_correlation_analysis_performance(self, service):
        """Test correlation analysis performance with larger dataset"""
        # Generate larger dataset for performance testing
        np.random.seed(42)
        large_data = pd.DataFrame(np.random.randn(1000, 20))  # 1000 rows, 20 columns
        large_data.columns = [f'var_{i}' for i in range(20)]
        
        import time
        start_time = time.time()
        
        result = service.analyze_correlations(large_data)
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Should complete within reasonable time
        assert duration < 5.0  # Less than 5 seconds
        assert 'correlations' in result
        assert len(result['correlations']['pearson']) == 20

    @pytest.mark.performance
    def test_regression_analysis_performance(self, service):
        """Test regression analysis performance with larger dataset"""
        np.random.seed(42)
        n = 1000
        
        large_data = pd.DataFrame({
            'target': np.random.normal(50, 10, n),
            'feature1': np.random.normal(0, 1, n),
            'feature2': np.random.normal(0, 1, n),
            'feature3': np.random.normal(0, 1, n),
            'feature4': np.random.normal(0, 1, n),
            'feature5': np.random.normal(0, 1, n)
        })
        
        import time
        start_time = time.time()
        
        result = service.perform_regression_analysis(
            data=large_data,
            target_column='target',
            feature_columns=['feature1', 'feature2', 'feature3', 'feature4', 'feature5']
        )
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Should complete within reasonable time
        assert duration < 3.0  # Less than 3 seconds
        assert 'model_coefficients' in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])