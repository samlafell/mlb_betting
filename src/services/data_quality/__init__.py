#!/usr/bin/env python3
"""
Data Quality Validation Package

Comprehensive data quality validation gates for the RAW→STAGING→CURATED pipeline.
Ensures only high-quality data flows through to prevent bad betting decisions.

Key Components:
- ValidationService: Core validation engine with configurable rules
- Quality metrics and scoring system
- Real-time alerting and monitoring integration
- Performance-optimized for production workloads

Usage:
    from src.services.data_quality import DataQualityValidationService
    
    service = DataQualityValidationService(config)
    reports = await service.validate_full_pipeline()
    metrics = await service.get_quality_metrics()
    gates = await service.check_quality_gates()
"""

from .validation_service import (
    DataQualityValidationService,
    ValidationStatus,
    QualityDimension,
    PipelineStage,
    QualityRule,
    ValidationResult,
    QualityReport,
    QualityMetrics
)

__all__ = [
    "DataQualityValidationService",
    "ValidationStatus", 
    "QualityDimension",
    "PipelineStage",
    "QualityRule",
    "ValidationResult",
    "QualityReport",
    "QualityMetrics"
]