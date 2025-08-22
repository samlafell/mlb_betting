"""
Unit tests for ModelQualityGates.
Tests Issue #42: Automated Model Validation & Testing Pipeline
"""

import pytest
from src.ml.validation.quality_gates import (
    ModelQualityGates, 
    ValidationStatus, 
    ValidationResult,
    QualityThreshold
)


class TestModelQualityGates:
    """Test ModelQualityGates functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.gates = ModelQualityGates()
    
    def test_initialization(self):
        """Test quality gates initialization."""
        assert self.gates is not None
        
        # Check that all threshold categories exist
        assert len(self.gates.accuracy_thresholds) > 0
        assert len(self.gates.business_metric_thresholds) > 0
        assert len(self.gates.stability_thresholds) > 0
        assert len(self.gates.performance_thresholds) > 0
    
    def test_get_all_thresholds(self):
        """Test getting all thresholds."""
        all_thresholds = self.gates.get_all_thresholds()
        
        assert len(all_thresholds) > 10  # Should have many thresholds
        assert "moneyline_accuracy" in all_thresholds
        assert "roi_percentage" in all_thresholds
        assert "maximum_drawdown" in all_thresholds
    
    def test_get_critical_thresholds(self):
        """Test getting only critical thresholds."""
        critical_thresholds = self.gates.get_critical_thresholds()
        all_thresholds = self.gates.get_all_thresholds()
        
        # Critical thresholds should be subset of all thresholds
        assert len(critical_thresholds) <= len(all_thresholds)
        
        # All critical thresholds should be marked as critical
        for threshold in critical_thresholds.values():
            assert threshold.critical is True
    
    def test_validate_metric_pass(self):
        """Test metric validation that should pass."""
        # Test moneyline accuracy above threshold
        result = self.gates.validate_metric("moneyline_accuracy", 0.65)
        
        assert result.status == ValidationStatus.PASS
        assert result.threshold_name == "moneyline_accuracy"
        assert result.actual_value == 0.65
        assert result.expected_min == 0.6  # From quality gates
        assert "meets requirements" in result.message
    
    def test_validate_metric_fail(self):
        """Test metric validation that should fail."""
        # Test ROI below threshold (critical metric)
        result = self.gates.validate_metric("roi_percentage", 1.0)  # Below 3.0 threshold
        
        assert result.status == ValidationStatus.FAIL
        assert result.threshold_name == "roi_percentage"
        assert result.actual_value == 1.0
        assert result.expected_min == 3.0
        assert result.critical is True
        assert "below minimum" in result.message
    
    def test_validate_metric_warning(self):
        """Test metric validation that should warn (non-critical)."""
        # Test Sharpe ratio below threshold (non-critical metric)
        result = self.gates.validate_metric("sharpe_ratio", 0.3)  # Below 0.5 threshold
        
        assert result.status == ValidationStatus.WARNING
        assert result.threshold_name == "sharpe_ratio"
        assert result.actual_value == 0.3
        assert result.expected_min == 0.5
        assert result.critical is False
        assert "below minimum" in result.message
    
    def test_validate_metric_maximum_threshold(self):
        """Test validation against maximum thresholds."""
        # Test maximum drawdown above threshold
        result = self.gates.validate_metric("maximum_drawdown", 30.0)  # Above 25.0 max
        
        assert result.status == ValidationStatus.FAIL
        assert result.actual_value == 30.0
        assert result.expected_max == 25.0
        assert "above maximum" in result.message
    
    def test_validate_metric_unknown(self):
        """Test validation of unknown metric."""
        result = self.gates.validate_metric("unknown_metric", 0.5)
        
        assert result.status == ValidationStatus.SKIP
        assert result.threshold_name == "unknown_metric"
        assert "No threshold defined" in result.message
        assert result.critical is False
    
    def test_validate_model_metrics(self):
        """Test validating multiple metrics."""
        metrics = {
            "moneyline_accuracy": 0.65,  # Should pass
            "roi_percentage": 1.0,  # Should fail (critical)
            "sharpe_ratio": 0.3,  # Should warn (non-critical)
            "unknown_metric": 0.5  # Should skip
        }
        
        results = self.gates.validate_model_metrics(metrics)
        
        assert len(results) == 4
        assert results["moneyline_accuracy"].status == ValidationStatus.PASS
        assert results["roi_percentage"].status == ValidationStatus.FAIL
        assert results["sharpe_ratio"].status == ValidationStatus.WARNING
        assert results["unknown_metric"].status == ValidationStatus.SKIP
    
    def test_get_overall_status_pass(self):
        """Test overall status when all critical metrics pass."""
        results = {
            "metric1": ValidationResult(
                threshold_name="metric1",
                status=ValidationStatus.PASS,
                actual_value=0.8,
                expected_min=0.6,
                critical=True
            ),
            "metric2": ValidationResult(
                threshold_name="metric2", 
                status=ValidationStatus.WARNING,
                actual_value=0.4,
                expected_min=0.5,
                critical=False
            )
        }
        
        status = self.gates.get_overall_status(results)
        assert status == ValidationStatus.WARNING  # Warning due to non-critical failure
    
    def test_get_overall_status_fail(self):
        """Test overall status when critical metrics fail."""
        results = {
            "metric1": ValidationResult(
                threshold_name="metric1",
                status=ValidationStatus.FAIL,
                actual_value=0.4,
                expected_min=0.6,
                critical=True
            ),
            "metric2": ValidationResult(
                threshold_name="metric2",
                status=ValidationStatus.PASS,
                actual_value=0.8,
                expected_min=0.5,
                critical=False
            )
        }
        
        status = self.gates.get_overall_status(results)
        assert status == ValidationStatus.FAIL
    
    def test_get_overall_status_all_pass(self):
        """Test overall status when everything passes."""
        results = {
            "metric1": ValidationResult(
                threshold_name="metric1",
                status=ValidationStatus.PASS,
                actual_value=0.8,
                expected_min=0.6,
                critical=True
            ),
            "metric2": ValidationResult(
                threshold_name="metric2",
                status=ValidationStatus.PASS,
                actual_value=0.8,
                expected_min=0.5,
                critical=False
            )
        }
        
        status = self.gates.get_overall_status(results)
        assert status == ValidationStatus.PASS
    
    def test_generate_validation_summary(self):
        """Test validation summary generation."""
        results = {
            "pass_metric": ValidationResult(
                threshold_name="pass_metric",
                status=ValidationStatus.PASS,
                actual_value=0.8,
                expected_min=0.6,
                message="Pass message"
            ),
            "fail_metric": ValidationResult(
                threshold_name="fail_metric",
                status=ValidationStatus.FAIL,
                actual_value=0.4,
                expected_min=0.6,
                message="Fail message"
            ),
            "warning_metric": ValidationResult(
                threshold_name="warning_metric",
                status=ValidationStatus.WARNING,
                actual_value=0.5,
                expected_min=0.6,
                message="Warning message"
            )
        }
        
        summary = self.gates.generate_validation_summary(results)
        
        assert "Model Validation Summary" in summary
        assert "Passed: 1" in summary
        assert "Failed: 1" in summary  
        assert "Warnings: 1" in summary
        assert "Fail message" in summary
        assert "Warning message" in summary
        assert "DEPLOYMENT BLOCKED" in summary  # Due to failure
    
    def test_business_metric_thresholds(self):
        """Test specific business metric thresholds from Issue #42."""
        # Test Issue #42 requirements
        assert self.gates.business_metric_thresholds["roi_percentage"].minimum_value == 3.0
        assert self.gates.business_metric_thresholds["maximum_drawdown"].maximum_value == 25.0
        assert self.gates.business_metric_thresholds["win_rate"].minimum_value == 52.0
        assert self.gates.business_metric_thresholds["sharpe_ratio"].minimum_value == 0.5
    
    def test_accuracy_thresholds(self):
        """Test accuracy thresholds from Issue #42."""
        # Test Issue #42 requirements  
        assert self.gates.accuracy_thresholds["moneyline_accuracy"].minimum_value == 0.60
        assert self.gates.accuracy_thresholds["spread_accuracy"].minimum_value == 0.58
        assert self.gates.accuracy_thresholds["total_accuracy"].minimum_value == 0.56