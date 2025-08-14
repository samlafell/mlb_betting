#!/usr/bin/env python3
"""
Statistical Analysis Service

Advanced statistical analysis engine for MLB betting analytics including:
- Regression analysis (linear, logistic, polynomial)  
- Correlation analysis (Pearson, Spearman, Kendall)
- Confidence intervals and hypothesis testing
- Distribution analysis and normality tests
- Time series analysis and forecasting
- Performance attribution modeling
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from scipy import stats
from scipy.optimize import curve_fit
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_squared_error, classification_report
import warnings
warnings.filterwarnings('ignore')

from ...core.enhanced_logging import get_contextual_logger, LogComponent
from ...core.exceptions import AnalyticsError

logger = get_contextual_logger(__name__, LogComponent.SERVICES)


class StatisticalAnalysisService:
    """
    Comprehensive statistical analysis service for betting analytics.
    
    Provides advanced statistical methods for analyzing betting patterns,
    line movements, and strategy performance.
    """
    
    def __init__(self):
        self.scaler = StandardScaler()
        
    def analyze_correlations(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Perform comprehensive correlation analysis.
        
        Args:
            data: DataFrame with numeric columns for analysis
            
        Returns:
            Dictionary containing correlation results
        """
        try:
            # Select only numeric columns
            numeric_data = data.select_dtypes(include=[np.number])
            
            if numeric_data.empty or len(numeric_data.columns) < 2:
                return {
                    'error': 'Insufficient numeric data for correlation analysis',
                    'correlations': {},
                    'significance_tests': {}
                }
            
            results = {
                'correlations': {},
                'significance_tests': {},
                'correlation_matrix': {},
                'summary_stats': {}
            }
            
            # Pearson correlation
            pearson_corr = numeric_data.corr(method='pearson')
            results['correlations']['pearson'] = pearson_corr.to_dict()
            
            # Spearman correlation (rank-based, less sensitive to outliers)
            spearman_corr = numeric_data.corr(method='spearman')
            results['correlations']['spearman'] = spearman_corr.to_dict()
            
            # Kendall correlation (rank-based, smaller sample sizes)
            kendall_corr = numeric_data.corr(method='kendall')
            results['correlations']['kendall'] = kendall_corr.to_dict()
            
            # Significance tests for key correlations
            significance_results = {}
            columns = list(numeric_data.columns)
            
            for i, col1 in enumerate(columns):
                for j, col2 in enumerate(columns[i+1:], i+1):
                    series1 = numeric_data[col1].dropna()
                    series2 = numeric_data[col2].dropna()
                    
                    # Align series (remove NaN pairs)
                    aligned_data = pd.concat([series1, series2], axis=1).dropna()
                    if len(aligned_data) < 3:
                        continue
                        
                    s1_aligned = aligned_data.iloc[:, 0]
                    s2_aligned = aligned_data.iloc[:, 1]
                    
                    # Pearson correlation test
                    try:
                        pearson_r, pearson_p = stats.pearsonr(s1_aligned, s2_aligned)
                        significance_results[f"{col1}_vs_{col2}"] = {
                            'pearson_r': float(pearson_r),
                            'pearson_p_value': float(pearson_p),
                            'pearson_significant': pearson_p < 0.05,
                            'sample_size': len(s1_aligned)
                        }
                    except Exception as e:
                        logger.warning(f"Could not compute Pearson correlation for {col1} vs {col2}: {e}")
            
            results['significance_tests'] = significance_results
            
            # Summary statistics
            results['summary_stats'] = {
                'strongest_positive_correlation': self._find_strongest_correlation(pearson_corr, positive=True),
                'strongest_negative_correlation': self._find_strongest_correlation(pearson_corr, positive=False),
                'average_correlation_strength': float(np.abs(pearson_corr.values[np.triu_indices_from(pearson_corr.values, k=1)]).mean()),
                'variables_analyzed': len(numeric_data.columns)
            }
            
            return results
            
        except Exception as e:
            logger.error(f"Error in correlation analysis: {e}")
            raise AnalyticsError(f"Correlation analysis failed: {str(e)}")
    
    def _find_strongest_correlation(self, corr_matrix: pd.DataFrame, positive: bool = True) -> Dict[str, Any]:
        """Find the strongest positive or negative correlation."""
        # Get upper triangle (excluding diagonal)
        mask = np.triu(np.ones_like(corr_matrix, dtype=bool), k=1)
        upper_triangle = corr_matrix.where(mask)
        
        if positive:
            max_corr = upper_triangle.max().max()
            max_idx = upper_triangle.stack().idxmax()
        else:
            min_corr = upper_triangle.min().min()
            max_idx = upper_triangle.stack().idxmin()
            max_corr = min_corr
        
        return {
            'variables': list(max_idx),
            'correlation': float(max_corr),
            'strength': self._classify_correlation_strength(abs(max_corr))
        }
    
    def _classify_correlation_strength(self, abs_corr: float) -> str:
        """Classify correlation strength based on absolute value."""
        if abs_corr >= 0.8:
            return 'very_strong'
        elif abs_corr >= 0.6:
            return 'strong'
        elif abs_corr >= 0.4:
            return 'moderate'
        elif abs_corr >= 0.2:
            return 'weak'
        else:
            return 'very_weak'
    
    def perform_regression_analysis(self, data: pd.DataFrame, target_column: str, feature_columns: List[str]) -> Dict[str, Any]:
        """
        Perform comprehensive regression analysis.
        
        Args:
            data: DataFrame containing the data
            target_column: Name of the target variable
            feature_columns: List of feature column names
            
        Returns:
            Dictionary containing regression results
        """
        try:
            # Prepare data
            if target_column not in data.columns:
                raise ValueError(f"Target column '{target_column}' not found in data")
            
            missing_features = [col for col in feature_columns if col not in data.columns]
            if missing_features:
                raise ValueError(f"Feature columns not found: {missing_features}")
            
            # Create clean dataset
            analysis_data = data[[target_column] + feature_columns].dropna()
            
            if len(analysis_data) < 10:
                return {'error': 'Insufficient data for regression analysis (minimum 10 observations required)'}
            
            X = analysis_data[feature_columns]
            y = analysis_data[target_column]
            
            # Determine if this is a classification or regression problem
            is_binary = len(y.unique()) == 2 and set(y.unique()).issubset({0, 1})
            
            results = {
                'analysis_type': 'logistic_regression' if is_binary else 'linear_regression',
                'sample_size': len(analysis_data),
                'features': feature_columns,
                'target': target_column
            }
            
            if is_binary:
                results.update(self._perform_logistic_regression(X, y))
            else:
                results.update(self._perform_linear_regression(X, y))
            
            # Add feature importance analysis
            results['feature_analysis'] = self._analyze_feature_importance(X, y, is_binary)
            
            return results
            
        except Exception as e:
            logger.error(f"Error in regression analysis: {e}")
            raise AnalyticsError(f"Regression analysis failed: {str(e)}")
    
    def _perform_linear_regression(self, X: pd.DataFrame, y: pd.Series) -> Dict[str, Any]:
        """Perform linear regression analysis."""
        # Split data for validation
        if len(X) > 20:
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        else:
            X_train, X_test, y_train, y_test = X, X, y, y
        
        # Fit model
        model = LinearRegression()
        model.fit(X_train, y_train)
        
        # Predictions
        y_pred_train = model.predict(X_train)
        y_pred_test = model.predict(X_test)
        
        # Calculate metrics
        r2_train = r2_score(y_train, y_pred_train)
        r2_test = r2_score(y_test, y_pred_test)
        rmse_train = np.sqrt(mean_squared_error(y_train, y_pred_train))
        rmse_test = np.sqrt(mean_squared_error(y_test, y_pred_test))
        
        # Statistical significance
        n = len(X_train)
        p = len(X_train.columns)
        
        # F-statistic
        mse_residual = mean_squared_error(y_train, y_pred_train)
        mse_total = np.var(y_train, ddof=1)
        f_statistic = ((mse_total - mse_residual) * (n - p - 1)) / (mse_residual * p) if mse_residual > 0 else 0
        f_p_value = 1 - stats.f.cdf(f_statistic, p, n - p - 1) if f_statistic > 0 else 1
        
        return {
            'model_coefficients': dict(zip(X.columns, model.coef_)),
            'intercept': float(model.intercept_),
            'r_squared_train': float(r2_train),
            'r_squared_test': float(r2_test),
            'rmse_train': float(rmse_train),
            'rmse_test': float(rmse_test),
            'f_statistic': float(f_statistic),
            'f_p_value': float(f_p_value),
            'model_significant': f_p_value < 0.05,
            'overfitting_indicator': abs(r2_train - r2_test) > 0.1
        }
    
    def _perform_logistic_regression(self, X: pd.DataFrame, y: pd.Series) -> Dict[str, Any]:
        """Perform logistic regression analysis."""
        # Split data for validation
        if len(X) > 20:
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
        else:
            X_train, X_test, y_train, y_test = X, X, y, y
        
        # Fit model
        model = LogisticRegression(random_state=42, max_iter=1000)
        model.fit(X_train, y_train)
        
        # Predictions
        y_pred_train = model.predict(X_train)
        y_pred_test = model.predict(X_test)
        y_pred_proba_train = model.predict_proba(X_train)[:, 1]
        y_pred_proba_test = model.predict_proba(X_test)[:, 1]
        
        # Calculate metrics
        train_accuracy = model.score(X_train, y_train)
        test_accuracy = model.score(X_test, y_test)
        
        # AUC-ROC
        try:
            from sklearn.metrics import roc_auc_score
            auc_train = roc_auc_score(y_train, y_pred_proba_train)
            auc_test = roc_auc_score(y_test, y_pred_proba_test)
        except:
            auc_train = auc_test = 0.5
        
        return {
            'model_coefficients': dict(zip(X.columns, model.coef_[0])),
            'intercept': float(model.intercept_[0]),
            'accuracy_train': float(train_accuracy),
            'accuracy_test': float(test_accuracy),
            'auc_roc_train': float(auc_train),
            'auc_roc_test': float(auc_test),
            'overfitting_indicator': abs(train_accuracy - test_accuracy) > 0.1
        }
    
    def _analyze_feature_importance(self, X: pd.DataFrame, y: pd.Series, is_binary: bool) -> Dict[str, Any]:
        """Analyze feature importance and relationships."""
        feature_analysis = {}
        
        for feature in X.columns:
            # Correlation with target
            corr_coef, corr_p = stats.pearsonr(X[feature], y)
            
            # Basic statistics
            feature_stats = {
                'correlation_with_target': float(corr_coef),
                'correlation_p_value': float(corr_p),
                'correlation_significant': corr_p < 0.05,
                'feature_mean': float(X[feature].mean()),
                'feature_std': float(X[feature].std()),
                'missing_percentage': float(X[feature].isnull().mean() * 100)
            }
            
            feature_analysis[feature] = feature_stats
        
        return feature_analysis
    
    def calculate_confidence_intervals(self, data: pd.Series, confidence_level: float = 0.95) -> Dict[str, float]:
        """
        Calculate confidence intervals for a data series.
        
        Args:
            data: Pandas series of numeric data
            confidence_level: Confidence level (default 0.95 for 95% CI)
            
        Returns:
            Dictionary with confidence interval results
        """
        try:
            clean_data = data.dropna()
            n = len(clean_data)
            
            if n < 2:
                return {'error': 'Insufficient data for confidence interval calculation'}
            
            mean = clean_data.mean()
            std_err = stats.sem(clean_data)  # Standard error of mean
            
            # t-distribution for small samples, normal for large samples
            if n < 30:
                # t-distribution
                degrees_freedom = n - 1
                t_critical = stats.t.ppf((1 + confidence_level) / 2, degrees_freedom)
                margin_error = t_critical * std_err
            else:
                # Normal distribution
                z_critical = stats.norm.ppf((1 + confidence_level) / 2)
                margin_error = z_critical * std_err
            
            return {
                'sample_size': int(n),
                'sample_mean': float(mean),
                'standard_error': float(std_err),
                'confidence_level': confidence_level,
                'margin_of_error': float(margin_error),
                'lower_bound': float(mean - margin_error),
                'upper_bound': float(mean + margin_error),
                'distribution_used': 't-distribution' if n < 30 else 'normal_distribution'
            }
            
        except Exception as e:
            logger.error(f"Error calculating confidence intervals: {e}")
            return {'error': str(e)}
    
    def analyze_distributions(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze the distribution of numeric variables.
        
        Args:
            data: DataFrame to analyze
            
        Returns:
            Dictionary with distribution analysis results
        """
        try:
            numeric_data = data.select_dtypes(include=[np.number])
            
            if numeric_data.empty:
                return {'error': 'No numeric data found for distribution analysis'}
            
            results = {}
            
            for column in numeric_data.columns:
                series = numeric_data[column].dropna()
                
                if len(series) < 3:
                    results[column] = {'error': 'Insufficient data for analysis'}
                    continue
                
                # Descriptive statistics
                column_results = {
                    'descriptive_stats': {
                        'count': int(len(series)),
                        'mean': float(series.mean()),
                        'median': float(series.median()),
                        'mode': float(series.mode().iloc[0]) if not series.mode().empty else None,
                        'std': float(series.std()),
                        'variance': float(series.var()),
                        'skewness': float(stats.skew(series)),
                        'kurtosis': float(stats.kurtosis(series)),
                        'min': float(series.min()),
                        'max': float(series.max()),
                        'range': float(series.max() - series.min()),
                        'q25': float(series.quantile(0.25)),
                        'q75': float(series.quantile(0.75)),
                        'iqr': float(series.quantile(0.75) - series.quantile(0.25))
                    }
                }
                
                # Normality tests
                try:
                    # Shapiro-Wilk test (good for small samples)
                    if len(series) <= 5000:
                        shapiro_stat, shapiro_p = stats.shapiro(series)
                        column_results['normality_tests'] = {
                            'shapiro_wilk': {
                                'statistic': float(shapiro_stat),
                                'p_value': float(shapiro_p),
                                'is_normal': shapiro_p > 0.05
                            }
                        }
                    
                    # Kolmogorov-Smirnov test against normal distribution
                    ks_stat, ks_p = stats.kstest(stats.zscore(series), 'norm')
                    if 'normality_tests' not in column_results:
                        column_results['normality_tests'] = {}
                    column_results['normality_tests']['kolmogorov_smirnov'] = {
                        'statistic': float(ks_stat),
                        'p_value': float(ks_p),
                        'is_normal': ks_p > 0.05
                    }
                    
                except Exception as e:
                    column_results['normality_tests'] = {'error': str(e)}
                
                # Outlier detection using IQR method
                Q1 = series.quantile(0.25)
                Q3 = series.quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                outliers = series[(series < lower_bound) | (series > upper_bound)]
                
                column_results['outlier_analysis'] = {
                    'outlier_count': int(len(outliers)),
                    'outlier_percentage': float(len(outliers) / len(series) * 100),
                    'lower_bound': float(lower_bound),
                    'upper_bound': float(upper_bound)
                }
                
                results[column] = column_results
            
            return results
            
        except Exception as e:
            logger.error(f"Error in distribution analysis: {e}")
            raise AnalyticsError(f"Distribution analysis failed: {str(e)}")
    
    def perform_hypothesis_testing(self, data1: pd.Series, data2: pd.Series, test_type: str = 'ttest') -> Dict[str, Any]:
        """
        Perform hypothesis testing between two groups.
        
        Args:
            data1: First group data
            data2: Second group data
            test_type: Type of test ('ttest', 'mannwhitney', 'kstest')
            
        Returns:
            Dictionary with test results
        """
        try:
            clean_data1 = data1.dropna()
            clean_data2 = data2.dropna()
            
            if len(clean_data1) < 3 or len(clean_data2) < 3:
                return {'error': 'Insufficient data for hypothesis testing (minimum 3 observations per group)'}
            
            results = {
                'test_type': test_type,
                'group1_size': len(clean_data1),
                'group2_size': len(clean_data2),
                'group1_mean': float(clean_data1.mean()),
                'group2_mean': float(clean_data2.mean()),
                'group1_std': float(clean_data1.std()),
                'group2_std': float(clean_data2.std())
            }
            
            if test_type == 'ttest':
                # Independent t-test
                statistic, p_value = stats.ttest_ind(clean_data1, clean_data2)
                results.update({
                    'statistic': float(statistic),
                    'p_value': float(p_value),
                    'significant': p_value < 0.05,
                    'null_hypothesis': 'The means of both groups are equal',
                    'interpretation': 'Groups have significantly different means' if p_value < 0.05 else 'No significant difference between group means'
                })
                
            elif test_type == 'mannwhitney':
                # Mann-Whitney U test (non-parametric)
                statistic, p_value = stats.mannwhitneyu(clean_data1, clean_data2, alternative='two-sided')
                results.update({
                    'statistic': float(statistic),
                    'p_value': float(p_value),
                    'significant': p_value < 0.05,
                    'null_hypothesis': 'The distributions of both groups are equal',
                    'interpretation': 'Groups have significantly different distributions' if p_value < 0.05 else 'No significant difference between group distributions'
                })
                
            elif test_type == 'kstest':
                # Kolmogorov-Smirnov test
                statistic, p_value = stats.ks_2samp(clean_data1, clean_data2)
                results.update({
                    'statistic': float(statistic),
                    'p_value': float(p_value),
                    'significant': p_value < 0.05,
                    'null_hypothesis': 'Both samples come from the same distribution',
                    'interpretation': 'Samples come from different distributions' if p_value < 0.05 else 'Samples likely come from the same distribution'
                })
            
            return results
            
        except Exception as e:
            logger.error(f"Error in hypothesis testing: {e}")
            raise AnalyticsError(f"Hypothesis testing failed: {str(e)}")
    
    def calculate_performance_attribution(self, performance_data: pd.DataFrame, factor_columns: List[str], return_column: str) -> Dict[str, Any]:
        """
        Calculate performance attribution using factor analysis.
        
        Args:
            performance_data: DataFrame with performance and factor data
            factor_columns: List of factor column names
            return_column: Column name containing returns/performance
            
        Returns:
            Dictionary with attribution results
        """
        try:
            # Clean data
            analysis_data = performance_data[[return_column] + factor_columns].dropna()
            
            if len(analysis_data) < 10:
                return {'error': 'Insufficient data for performance attribution analysis'}
            
            X = analysis_data[factor_columns]
            y = analysis_data[return_column]
            
            # Perform regression
            model = LinearRegression()
            model.fit(X, y)
            
            # Calculate attribution
            factor_contributions = {}
            total_return = y.sum()
            
            for i, factor in enumerate(factor_columns):
                factor_return = (X.iloc[:, i] * model.coef_[i]).sum()
                factor_contributions[factor] = {
                    'absolute_contribution': float(factor_return),
                    'relative_contribution': float(factor_return / total_return * 100) if total_return != 0 else 0.0,
                    'factor_coefficient': float(model.coef_[i])
                }
            
            # Unexplained component (alpha)
            residuals = y - model.predict(X)
            unexplained_return = residuals.sum()
            
            results = {
                'total_return': float(total_return),
                'explained_return': float(total_return - unexplained_return),
                'unexplained_return': float(unexplained_return),
                'r_squared': float(r2_score(y, model.predict(X))),
                'factor_contributions': factor_contributions,
                'attribution_summary': {
                    'most_contributing_factor': max(factor_contributions.keys(), key=lambda x: abs(factor_contributions[x]['absolute_contribution'])),
                    'explained_percentage': float((total_return - unexplained_return) / total_return * 100) if total_return != 0 else 0.0
                }
            }
            
            return results
            
        except Exception as e:
            logger.error(f"Error in performance attribution: {e}")
            raise AnalyticsError(f"Performance attribution analysis failed: {str(e)}")


# Singleton instance
_statistical_analysis_service = None

def get_statistical_analysis_service() -> StatisticalAnalysisService:
    """Get or create the statistical analysis service instance."""
    global _statistical_analysis_service
    if _statistical_analysis_service is None:
        _statistical_analysis_service = StatisticalAnalysisService()
    return _statistical_analysis_service