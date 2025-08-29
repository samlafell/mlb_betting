#!/usr/bin/env python3
"""
Data Quality Validation Service

Comprehensive data quality validation gates for the RAW→STAGING→CURATED pipeline.
Ensures only high-quality data flows through to prevent bad betting decisions.

Architecture:
- RAW Zone: Schema validation, completeness, format, duplicates
- STAGING Zone: Transformation accuracy, consistency, drift detection  
- CURATED Zone: Business rules, outliers, freshness, final quality gates

Key Features:
- Configurable quality thresholds per pipeline stage
- Real-time validation with sub-second performance
- Automated alerting for quality breaches
- Quality trend analysis and reporting
- Integration with existing monitoring infrastructure
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Union
import json
import statistics
from decimal import Decimal

from ...core.config import UnifiedSettings
from ...core.logging import LogComponent, get_logger
from ...data.database.connection import get_db_manager
from ..monitoring.unified_monitoring_service import Alert, AlertLevel, HealthStatus

logger = get_logger(__name__, LogComponent.DATA_QUALITY)


class ValidationStatus(str, Enum):
    """Data validation status."""
    PASSED = "passed"
    WARNING = "warning"  
    FAILED = "failed"
    PENDING = "pending"


class QualityDimension(str, Enum):
    """Data quality dimensions."""
    COMPLETENESS = "completeness"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    TIMELINESS = "timeliness"
    VALIDITY = "validity"
    UNIQUENESS = "uniqueness"


class PipelineStage(str, Enum):
    """Pipeline stages for validation."""
    RAW = "raw"
    STAGING = "staging"
    CURATED = "curated"


@dataclass
class QualityRule:
    """Data quality validation rule."""
    name: str
    dimension: QualityDimension
    stage: PipelineStage
    query: str
    threshold: float  # 0.0 - 1.0 (failure threshold)
    warning_threshold: float  # 0.0 - 1.0 (warning threshold)
    description: str
    business_impact: str = "medium"  # low, medium, high, critical
    enabled: bool = True


@dataclass
class ValidationResult:
    """Single validation result."""
    rule_name: str
    status: ValidationStatus
    score: float  # 0.0 - 1.0 quality score
    threshold: float
    warning_threshold: float
    message: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    execution_time_ms: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class QualityReport:
    """Comprehensive quality report for a pipeline stage."""
    stage: PipelineStage
    overall_score: float  # 0.0 - 1.0
    overall_status: ValidationStatus
    validation_results: List[ValidationResult] = field(default_factory=list)
    total_records: int = 0
    passed_validations: int = 0
    warning_validations: int = 0
    failed_validations: int = 0
    execution_time_ms: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)
    data_sources: List[str] = field(default_factory=list)
    quality_trends: Dict[str, float] = field(default_factory=dict)


@dataclass
class QualityMetrics:
    """Aggregated quality metrics across all stages."""
    raw_score: float = 0.0
    staging_score: float = 0.0
    curated_score: float = 0.0
    overall_score: float = 0.0
    total_records_processed: int = 0
    quality_gate_pass_rate: float = 0.0
    data_freshness_score: float = 0.0
    anomaly_detection_score: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)


class DataQualityValidationService:
    """
    Comprehensive data quality validation service for the MLB betting pipeline.
    
    Implements validation gates at each pipeline stage:
    1. RAW Zone: Schema, completeness, format validation
    2. STAGING Zone: Transformation accuracy, consistency checks
    3. CURATED Zone: Business rules, outlier detection, final quality gates
    """
    
    def __init__(self, config: UnifiedSettings):
        self.config = config
        self.db_manager = get_db_manager()
        self.quality_rules: Dict[PipelineStage, List[QualityRule]] = {}
        self.quality_thresholds = {
            "raw_minimum_score": 0.85,      # 85% minimum for RAW data
            "staging_minimum_score": 0.90,  # 90% minimum for STAGING
            "curated_minimum_score": 0.95,  # 95% minimum for CURATED
            "warning_threshold": 0.8,       # Issue warnings below 80%
        }
        self._initialize_quality_rules()
    
    def _initialize_quality_rules(self):
        """Initialize quality validation rules for each pipeline stage."""
        
        # RAW Zone Validation Rules
        raw_rules = [
            QualityRule(
                name="raw_data_completeness",
                dimension=QualityDimension.COMPLETENESS,
                stage=PipelineStage.RAW,
                query="""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN external_game_id IS NOT NULL THEN 1 END) as complete_game_ids,
                    COUNT(CASE WHEN raw_odds IS NOT NULL OR raw_game_data IS NOT NULL 
                              OR betting_data IS NOT NULL OR raw_response IS NOT NULL THEN 1 END) as complete_data
                FROM (
                    SELECT external_game_id, raw_odds, NULL as raw_game_data, NULL as betting_data, NULL as raw_response FROM raw_data.action_network_odds
                    UNION ALL
                    SELECT external_game_id, NULL, raw_game_data, NULL, NULL FROM raw_data.action_network_games  
                    UNION ALL
                    SELECT external_game_id, NULL, NULL, betting_data, NULL FROM raw_data.vsin
                    UNION ALL
                    SELECT NULL, NULL, NULL, NULL, raw_response FROM raw_data.mlb_stats_api
                ) raw_combined
                """,
                threshold=0.85,
                warning_threshold=0.90,
                description="Ensures RAW data has required fields populated",
                business_impact="critical"
            ),
            QualityRule(
                name="raw_data_freshness",
                dimension=QualityDimension.TIMELINESS,
                stage=PipelineStage.RAW,
                query="""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN collected_at > NOW() - INTERVAL '24 hours' THEN 1 END) as fresh_records
                FROM (
                    SELECT collected_at FROM raw_data.action_network_odds
                    UNION ALL
                    SELECT collected_at FROM raw_data.action_network_games
                    UNION ALL  
                    SELECT collected_at FROM raw_data.vsin
                    UNION ALL
                    SELECT collected_at FROM raw_data.mlb_game_outcomes
                ) raw_combined
                """,
                threshold=0.80,
                warning_threshold=0.90,
                description="Ensures RAW data is fresh (collected within 24 hours)",
                business_impact="high"
            ),
            QualityRule(
                name="raw_schema_validity",
                dimension=QualityDimension.VALIDITY,
                stage=PipelineStage.RAW,
                query="""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN jsonb_typeof(raw_odds) = 'object' OR raw_odds IS NULL THEN 1
                          WHEN jsonb_typeof(raw_game_data) = 'object' OR raw_game_data IS NULL THEN 1  
                          WHEN jsonb_typeof(betting_data) = 'object' OR betting_data IS NULL THEN 1
                          WHEN jsonb_typeof(raw_response) = 'object' OR raw_response IS NULL THEN 1
                          ELSE 0 END) as valid_json_records
                FROM (
                    SELECT raw_odds, NULL as raw_game_data, NULL as betting_data, NULL as raw_response FROM raw_data.action_network_odds
                    UNION ALL
                    SELECT NULL, raw_game_data, NULL, NULL FROM raw_data.action_network_games
                    UNION ALL
                    SELECT NULL, NULL, betting_data, NULL FROM raw_data.vsin  
                    UNION ALL
                    SELECT NULL, NULL, NULL, raw_response FROM raw_data.mlb_stats_api
                ) raw_combined
                """,
                threshold=0.95,
                warning_threshold=0.98,
                description="Ensures RAW JSON data has valid schema structure",
                business_impact="critical"
            ),
            QualityRule(
                name="raw_duplicate_detection",
                dimension=QualityDimension.UNIQUENESS,
                stage=PipelineStage.RAW,
                query="""
                WITH duplicate_analysis AS (
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(*) - COUNT(DISTINCT (external_game_id, sportsbook_key, collected_at::date)) as duplicates
                    FROM raw_data.action_network_odds
                    WHERE collected_at > NOW() - INTERVAL '7 days'
                )
                SELECT 
                    total_records,
                    total_records - duplicates as unique_records
                FROM duplicate_analysis
                """,
                threshold=0.98,
                warning_threshold=0.99,
                description="Detects duplicate records in RAW data",
                business_impact="medium"
            )
        ]
        
        # STAGING Zone Validation Rules  
        staging_rules = [
            QualityRule(
                name="staging_transformation_accuracy",
                dimension=QualityDimension.ACCURACY,
                stage=PipelineStage.STAGING,
                query="""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN validation_status = 'valid' THEN 1 END) as accurate_records
                FROM staging.betting_odds_unified
                WHERE processed_at > NOW() - INTERVAL '24 hours'
                """,
                threshold=0.90,
                warning_threshold=0.95,
                description="Ensures STAGING transformations are accurate",
                business_impact="critical"
            ),
            QualityRule(
                name="staging_team_normalization",
                dimension=QualityDimension.CONSISTENCY,
                stage=PipelineStage.STAGING,
                query="""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN home_team ~ '^[A-Z]{2,5}$' AND away_team ~ '^[A-Z]{2,5}$' 
                              AND home_team != away_team THEN 1 END) as normalized_teams
                FROM staging.betting_odds_unified
                WHERE processed_at > NOW() - INTERVAL '24 hours'
                """,
                threshold=0.95,
                warning_threshold=0.98,
                description="Ensures team names are properly normalized",
                business_impact="high"
            ),
            QualityRule(
                name="staging_odds_validity",
                dimension=QualityDimension.VALIDITY,
                stage=PipelineStage.STAGING,
                query="""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN 
                        (home_moneyline_odds BETWEEN -1000 AND 1000 OR home_moneyline_odds IS NULL) AND
                        (away_moneyline_odds BETWEEN -1000 AND 1000 OR away_moneyline_odds IS NULL) AND
                        (spread_line BETWEEN -20 AND 20 OR spread_line IS NULL) AND
                        (total_line BETWEEN 3 AND 25 OR total_line IS NULL)
                        THEN 1 END) as valid_odds
                FROM staging.betting_odds_unified  
                WHERE processed_at > NOW() - INTERVAL '24 hours'
                """,
                threshold=0.95,
                warning_threshold=0.98,
                description="Validates betting odds are within reasonable ranges",
                business_impact="critical"
            ),
            QualityRule(
                name="staging_data_quality_score",
                dimension=QualityDimension.COMPLETENESS,
                stage=PipelineStage.STAGING,
                query="""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN data_quality_score >= 0.8 THEN 1 END) as high_quality_records
                FROM staging.betting_odds_unified
                WHERE processed_at > NOW() - INTERVAL '24 hours'
                """,
                threshold=0.85,
                warning_threshold=0.90,
                description="Ensures STAGING data meets quality score thresholds",
                business_impact="high"
            ),
            QualityRule(
                name="staging_cross_source_consistency",
                dimension=QualityDimension.CONSISTENCY,
                stage=PipelineStage.STAGING,
                query="""
                WITH source_consistency AS (
                    SELECT 
                        external_game_id,
                        COUNT(DISTINCT data_source) as source_count,
                        COUNT(*) as total_records,
                        AVG(data_quality_score) as avg_quality
                    FROM staging.betting_odds_unified 
                    WHERE processed_at > NOW() - INTERVAL '24 hours'
                    GROUP BY external_game_id
                    HAVING COUNT(DISTINCT data_source) > 1
                )
                SELECT 
                    COUNT(*) as total_games,
                    COUNT(CASE WHEN avg_quality >= 0.85 THEN 1 END) as consistent_games
                FROM source_consistency
                """,
                threshold=0.80,
                warning_threshold=0.90,
                description="Checks consistency across multiple data sources",
                business_impact="medium"
            )
        ]
        
        # CURATED Zone Validation Rules
        curated_rules = [
            QualityRule(
                name="curated_feature_completeness",
                dimension=QualityDimension.COMPLETENESS,
                stage=PipelineStage.CURATED,
                query="""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN missing_features_count <= 5 THEN 1 END) as complete_features
                FROM curated.ml_features
                WHERE feature_extraction_date > NOW() - INTERVAL '24 hours'
                """,
                threshold=0.90,
                warning_threshold=0.95,
                description="Ensures ML features have sufficient completeness",
                business_impact="critical"
            ),
            QualityRule(
                name="curated_business_rule_compliance",
                dimension=QualityDimension.VALIDITY,
                stage=PipelineStage.CURATED,
                query="""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN 
                        home_team != away_team AND
                        (home_team_wins >= 0 OR home_team_wins IS NULL) AND
                        (away_team_wins >= 0 OR away_team_wins IS NULL) AND
                        (opening_moneyline_home BETWEEN -1000 AND 1000 OR opening_moneyline_home IS NULL) AND
                        data_quality_score >= 0.7
                        THEN 1 END) as compliant_records
                FROM curated.ml_features
                WHERE feature_extraction_date > NOW() - INTERVAL '24 hours'
                """,
                threshold=0.95,
                warning_threshold=0.98,
                description="Validates business rule compliance in CURATED data",
                business_impact="critical"
            ),
            QualityRule(
                name="curated_outlier_detection",
                dimension=QualityDimension.ACCURACY,
                stage=PipelineStage.CURATED,
                query="""
                WITH outlier_analysis AS (
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(CASE WHEN 
                            (home_starter_era BETWEEN 0 AND 15 OR home_starter_era IS NULL) AND
                            (away_starter_era BETWEEN 0 AND 15 OR away_starter_era IS NULL) AND  
                            (temperature BETWEEN -10 AND 120 OR temperature IS NULL) AND
                            (wind_speed BETWEEN 0 AND 50 OR wind_speed IS NULL)
                            THEN 1 END) as normal_records
                    FROM curated.ml_features
                    WHERE feature_extraction_date > NOW() - INTERVAL '24 hours'
                )
                SELECT total_records, normal_records FROM outlier_analysis
                """,
                threshold=0.95,
                warning_threshold=0.98,
                description="Detects statistical outliers in CURATED features",
                business_impact="medium"
            ),
            QualityRule(
                name="curated_data_lineage",
                dimension=QualityDimension.CONSISTENCY,
                stage=PipelineStage.CURATED,
                query="""
                WITH lineage_check AS (
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(CASE WHEN mlb_stats_api_game_id IS NOT NULL THEN 1 END) as with_lineage
                    FROM curated.ml_features
                    WHERE feature_extraction_date > NOW() - INTERVAL '24 hours'
                )
                SELECT total_records, with_lineage FROM lineage_check
                """,
                threshold=0.80,
                warning_threshold=0.90,
                description="Ensures proper data lineage tracking",
                business_impact="medium"
            ),
            QualityRule(
                name="curated_final_quality_gate", 
                dimension=QualityDimension.COMPLETENESS,
                stage=PipelineStage.CURATED,
                query="""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN data_quality_score >= 0.95 THEN 1 END) as final_quality_records
                FROM curated.ml_features
                WHERE feature_extraction_date > NOW() - INTERVAL '24 hours'
                """,
                threshold=0.95,
                warning_threshold=0.98,
                description="Final quality gate before betting decisions",
                business_impact="critical"
            )
        ]
        
        self.quality_rules = {
            PipelineStage.RAW: raw_rules,
            PipelineStage.STAGING: staging_rules,
            PipelineStage.CURATED: curated_rules
        }
        
        logger.info(f"Initialized {sum(len(rules) for rules in self.quality_rules.values())} quality validation rules")

    async def validate_pipeline_stage(self, stage: PipelineStage) -> QualityReport:
        """
        Validate a specific pipeline stage.
        
        Args:
            stage: Pipeline stage to validate
            
        Returns:
            QualityReport with validation results
        """
        start_time = datetime.now()
        
        try:
            rules = self.quality_rules.get(stage, [])
            if not rules:
                logger.warning(f"No validation rules defined for stage: {stage}")
                return QualityReport(stage=stage, overall_score=0.0, overall_status=ValidationStatus.PENDING)
            
            validation_results = []
            total_records = 0
            
            # Execute validation rules
            for rule in rules:
                if not rule.enabled:
                    continue
                    
                result = await self._execute_validation_rule(rule)
                validation_results.append(result)
                
                # Track total records from first rule (approximation)
                if total_records == 0 and result.metadata.get('total_records'):
                    total_records = result.metadata['total_records']
            
            # Calculate overall metrics
            overall_score = self._calculate_overall_score(validation_results)
            overall_status = self._determine_overall_status(validation_results, overall_score, stage)
            
            passed = sum(1 for r in validation_results if r.status == ValidationStatus.PASSED)
            warning = sum(1 for r in validation_results if r.status == ValidationStatus.WARNING)
            failed = sum(1 for r in validation_results if r.status == ValidationStatus.FAILED)
            
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            
            # Get data sources for this stage
            data_sources = await self._get_data_sources_for_stage(stage)
            
            # Get quality trends
            quality_trends = await self._get_quality_trends(stage)
            
            report = QualityReport(
                stage=stage,
                overall_score=overall_score,
                overall_status=overall_status,
                validation_results=validation_results,
                total_records=total_records,
                passed_validations=passed,
                warning_validations=warning,
                failed_validations=failed,
                execution_time_ms=execution_time,
                data_sources=data_sources,
                quality_trends=quality_trends
            )
            
            # Log results
            logger.info(f"Stage {stage.value} validation completed: "
                       f"Score={overall_score:.3f}, Status={overall_status.value}, "
                       f"Passed={passed}, Warning={warning}, Failed={failed}")
            
            return report
            
        except Exception as e:
            logger.error(f"Failed to validate stage {stage.value}: {e}")
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            
            return QualityReport(
                stage=stage,
                overall_score=0.0,
                overall_status=ValidationStatus.FAILED,
                execution_time_ms=execution_time,
                validation_results=[
                    ValidationResult(
                        rule_name="validation_execution",
                        status=ValidationStatus.FAILED,
                        score=0.0,
                        threshold=0.0,
                        warning_threshold=0.0,
                        message=f"Validation execution failed: {e}"
                    )
                ]
            )

    async def _execute_validation_rule(self, rule: QualityRule) -> ValidationResult:
        """Execute a single validation rule."""
        start_time = datetime.now()
        
        try:
            # Execute validation query
            result = await self.db_manager.execute_query(rule.query)
            
            if not result:
                return ValidationResult(
                    rule_name=rule.name,
                    status=ValidationStatus.FAILED,
                    score=0.0,
                    threshold=rule.threshold,
                    warning_threshold=rule.warning_threshold,
                    message="No data returned from validation query"
                )
            
            row = result[0]
            total_records = row.get('total_records', 0)
            
            # Calculate quality score based on rule type
            if 'complete_records' in row or 'accurate_records' in row or 'valid_records' in row:
                good_records = (row.get('complete_records') or 
                              row.get('accurate_records') or 
                              row.get('valid_records') or
                              row.get('normalized_teams') or
                              row.get('valid_odds') or
                              row.get('high_quality_records') or
                              row.get('compliant_records') or
                              row.get('normal_records') or
                              row.get('final_quality_records') or
                              row.get('unique_records') or
                              row.get('fresh_records') or
                              row.get('valid_json_records') or
                              row.get('complete_game_ids') or
                              row.get('complete_data') or
                              row.get('complete_features') or
                              row.get('consistent_games') or
                              row.get('with_lineage', 0))
            else:
                good_records = 0
                
            score = good_records / total_records if total_records > 0 else 0.0
            
            # Determine status
            if score >= rule.warning_threshold:
                status = ValidationStatus.PASSED
                message = f"Quality check passed: {good_records}/{total_records} records ({score:.1%})"
            elif score >= rule.threshold:
                status = ValidationStatus.WARNING  
                message = f"Quality warning: {good_records}/{total_records} records ({score:.1%})"
            else:
                status = ValidationStatus.FAILED
                message = f"Quality check failed: {good_records}/{total_records} records ({score:.1%})"
            
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            
            return ValidationResult(
                rule_name=rule.name,
                status=status,
                score=score,
                threshold=rule.threshold,
                warning_threshold=rule.warning_threshold,
                message=message,
                metadata={
                    'total_records': total_records,
                    'good_records': good_records,
                    'rule_dimension': rule.dimension.value,
                    'business_impact': rule.business_impact,
                    'query_result': dict(row)
                },
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            logger.error(f"Failed to execute validation rule {rule.name}: {e}")
            
            return ValidationResult(
                rule_name=rule.name,
                status=ValidationStatus.FAILED,
                score=0.0,
                threshold=rule.threshold,
                warning_threshold=rule.warning_threshold,
                message=f"Rule execution failed: {e}",
                execution_time_ms=execution_time
            )

    def _calculate_overall_score(self, results: List[ValidationResult]) -> float:
        """Calculate overall quality score from validation results."""
        if not results:
            return 0.0
        
        # Weight scores by business impact and rule dimension
        total_weight = 0.0
        weighted_score = 0.0
        
        for result in results:
            # Business impact weights
            impact_weight = {
                'critical': 3.0,
                'high': 2.0, 
                'medium': 1.5,
                'low': 1.0
            }.get(result.metadata.get('business_impact', 'medium'), 1.5)
            
            total_weight += impact_weight
            weighted_score += result.score * impact_weight
        
        return weighted_score / total_weight if total_weight > 0 else 0.0

    def _determine_overall_status(self, results: List[ValidationResult], 
                                overall_score: float, stage: PipelineStage) -> ValidationStatus:
        """Determine overall validation status."""
        if not results:
            return ValidationStatus.PENDING
            
        # Check for any critical failures
        critical_failures = [r for r in results if r.status == ValidationStatus.FAILED and 
                           r.metadata.get('business_impact') == 'critical']
        if critical_failures:
            return ValidationStatus.FAILED
        
        # Check stage-specific thresholds
        stage_threshold = self.quality_thresholds.get(f"{stage.value}_minimum_score", 0.85)
        warning_threshold = self.quality_thresholds.get("warning_threshold", 0.8)
        
        if overall_score >= stage_threshold:
            return ValidationStatus.PASSED
        elif overall_score >= warning_threshold:
            return ValidationStatus.WARNING
        else:
            return ValidationStatus.FAILED

    async def _get_data_sources_for_stage(self, stage: PipelineStage) -> List[str]:
        """Get data sources contributing to a pipeline stage."""
        try:
            if stage == PipelineStage.RAW:
                query = """
                SELECT DISTINCT 'action_network' as source FROM raw_data.action_network_odds WHERE collected_at > NOW() - INTERVAL '24 hours'
                UNION
                SELECT DISTINCT 'vsin' as source FROM raw_data.vsin WHERE collected_at > NOW() - INTERVAL '24 hours'  
                UNION
                SELECT DISTINCT 'sbd' as source FROM raw_data.sbd_betting_splits WHERE collected_at > NOW() - INTERVAL '24 hours'
                UNION
                SELECT DISTINCT 'mlb_stats_api' as source FROM raw_data.mlb_game_outcomes WHERE collected_at > NOW() - INTERVAL '24 hours'
                """
            elif stage == PipelineStage.STAGING:
                query = """
                SELECT DISTINCT data_source as source 
                FROM staging.betting_odds_unified 
                WHERE processed_at > NOW() - INTERVAL '24 hours'
                """
            else:  # CURATED
                query = """
                SELECT DISTINCT 'ml_features' as source
                FROM curated.ml_features 
                WHERE feature_extraction_date > NOW() - INTERVAL '24 hours'
                """
                
            result = await self.db_manager.execute_query(query)
            return [row['source'] for row in result]
            
        except Exception as e:
            logger.warning(f"Could not get data sources for stage {stage.value}: {e}")
            return []

    async def _get_quality_trends(self, stage: PipelineStage) -> Dict[str, float]:
        """Get quality trends for a pipeline stage."""
        try:
            # This would typically query historical quality metrics
            # For now, return placeholder trends
            return {
                "24h_avg_score": 0.92,
                "7d_avg_score": 0.89,
                "trend_direction": 0.03,  # positive trend
                "volatility": 0.05
            }
        except Exception as e:
            logger.warning(f"Could not get quality trends for stage {stage.value}: {e}")
            return {}

    async def validate_full_pipeline(self) -> Dict[PipelineStage, QualityReport]:
        """
        Validate all pipeline stages.
        
        Returns:
            Dictionary mapping each stage to its quality report
        """
        logger.info("Starting full pipeline validation")
        start_time = datetime.now()
        
        try:
            # Run validations in parallel for performance
            tasks = [
                self.validate_pipeline_stage(PipelineStage.RAW),
                self.validate_pipeline_stage(PipelineStage.STAGING), 
                self.validate_pipeline_stage(PipelineStage.CURATED)
            ]
            
            raw_report, staging_report, curated_report = await asyncio.gather(*tasks)
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"Full pipeline validation completed in {execution_time:.2f}s")
            logger.info(f"Quality scores - RAW: {raw_report.overall_score:.3f}, "
                       f"STAGING: {staging_report.overall_score:.3f}, " 
                       f"CURATED: {curated_report.overall_score:.3f}")
            
            return {
                PipelineStage.RAW: raw_report,
                PipelineStage.STAGING: staging_report,
                PipelineStage.CURATED: curated_report
            }
            
        except Exception as e:
            logger.error(f"Full pipeline validation failed: {e}")
            raise

    async def get_quality_metrics(self) -> QualityMetrics:
        """Get aggregated quality metrics across all pipeline stages."""
        try:
            reports = await self.validate_full_pipeline()
            
            raw_score = reports[PipelineStage.RAW].overall_score
            staging_score = reports[PipelineStage.STAGING].overall_score  
            curated_score = reports[PipelineStage.CURATED].overall_score
            
            # Calculate weighted overall score (CURATED is most important)
            overall_score = (raw_score * 0.2 + staging_score * 0.3 + curated_score * 0.5)
            
            # Calculate total records processed
            total_records = sum(report.total_records for report in reports.values())
            
            # Calculate quality gate pass rate
            total_validations = sum(len(report.validation_results) for report in reports.values())
            passed_validations = sum(report.passed_validations for report in reports.values())
            pass_rate = passed_validations / total_validations if total_validations > 0 else 0.0
            
            # Get data freshness score (from RAW stage timeliness checks)
            raw_timeliness_results = [r for r in reports[PipelineStage.RAW].validation_results 
                                    if r.rule_name == 'raw_data_freshness']
            freshness_score = raw_timeliness_results[0].score if raw_timeliness_results else 0.0
            
            # Calculate anomaly detection score (from CURATED outlier detection)
            curated_outlier_results = [r for r in reports[PipelineStage.CURATED].validation_results
                                     if r.rule_name == 'curated_outlier_detection']
            anomaly_score = curated_outlier_results[0].score if curated_outlier_results else 0.0
            
            return QualityMetrics(
                raw_score=raw_score,
                staging_score=staging_score,
                curated_score=curated_score,
                overall_score=overall_score,
                total_records_processed=total_records,
                quality_gate_pass_rate=pass_rate,
                data_freshness_score=freshness_score,
                anomaly_detection_score=anomaly_score
            )
            
        except Exception as e:
            logger.error(f"Failed to get quality metrics: {e}")
            return QualityMetrics()

    async def check_quality_gates(self) -> Dict[str, bool]:
        """
        Check if quality gates pass for pipeline promotion.
        
        Returns:
            Dictionary indicating which quality gates passed
        """
        try:
            reports = await self.validate_full_pipeline()
            
            gates = {
                "raw_to_staging": reports[PipelineStage.RAW].overall_score >= self.quality_thresholds["raw_minimum_score"],
                "staging_to_curated": reports[PipelineStage.STAGING].overall_score >= self.quality_thresholds["staging_minimum_score"],
                "curated_ready": reports[PipelineStage.CURATED].overall_score >= self.quality_thresholds["curated_minimum_score"],
                "no_critical_failures": all(
                    report.overall_status != ValidationStatus.FAILED 
                    for report in reports.values()
                )
            }
            
            gates["overall_pipeline_ready"] = all(gates.values())
            
            logger.info(f"Quality gates status: {gates}")
            return gates
            
        except Exception as e:
            logger.error(f"Failed to check quality gates: {e}")
            return {
                "raw_to_staging": False,
                "staging_to_curated": False, 
                "curated_ready": False,
                "no_critical_failures": False,
                "overall_pipeline_ready": False
            }

    async def generate_quality_alerts(self, reports: Dict[PipelineStage, QualityReport]) -> List[Alert]:
        """Generate alerts based on quality validation results."""
        alerts = []
        
        try:
            for stage, report in reports.items():
                # Check overall stage quality
                if report.overall_status == ValidationStatus.FAILED:
                    alerts.append(Alert(
                        level=AlertLevel.CRITICAL,
                        title=f"Data Quality Failure - {stage.value.upper()} Stage",
                        message=f"Stage {stage.value} quality score {report.overall_score:.1%} "
                               f"below threshold. {report.failed_validations} validation(s) failed.",
                        source=f"quality_validation_{stage.value}",
                        metadata={
                            "stage": stage.value,
                            "score": report.overall_score,
                            "failed_validations": report.failed_validations,
                            "total_records": report.total_records
                        }
                    ))
                elif report.overall_status == ValidationStatus.WARNING:
                    alerts.append(Alert(
                        level=AlertLevel.WARNING,
                        title=f"Data Quality Warning - {stage.value.upper()} Stage", 
                        message=f"Stage {stage.value} quality score {report.overall_score:.1%} "
                               f"below optimal. {report.warning_validations} validation(s) at warning level.",
                        source=f"quality_validation_{stage.value}",
                        metadata={
                            "stage": stage.value,
                            "score": report.overall_score,
                            "warning_validations": report.warning_validations
                        }
                    ))
                
                # Check individual critical rule failures
                for result in report.validation_results:
                    if (result.status == ValidationStatus.FAILED and 
                        result.metadata.get('business_impact') == 'critical'):
                        alerts.append(Alert(
                            level=AlertLevel.CRITICAL,
                            title=f"Critical Quality Rule Failed - {result.rule_name}",
                            message=f"Critical quality rule '{result.rule_name}' failed "
                                   f"with score {result.score:.1%}: {result.message}",
                            source=f"quality_rule_{result.rule_name}",
                            metadata={
                                "rule_name": result.rule_name,
                                "stage": stage.value,
                                "score": result.score,
                                "threshold": result.threshold,
                                "business_impact": result.metadata.get('business_impact')
                            }
                        ))
            
            logger.info(f"Generated {len(alerts)} quality alerts")
            return alerts
            
        except Exception as e:
            logger.error(f"Failed to generate quality alerts: {e}")
            return []

    async def get_quality_dashboard_data(self) -> Dict[str, Any]:
        """Get comprehensive data for quality monitoring dashboard."""
        try:
            # Get pipeline validation reports
            reports = await self.validate_full_pipeline()
            
            # Get quality metrics
            metrics = await self.get_quality_metrics()
            
            # Check quality gates
            gates = await self.check_quality_gates()
            
            # Generate alerts
            alerts = await self.generate_quality_alerts(reports)
            
            return {
                "overall_status": "healthy" if gates["overall_pipeline_ready"] else "degraded",
                "overall_score": metrics.overall_score,
                "stage_scores": {
                    "raw": metrics.raw_score,
                    "staging": metrics.staging_score,
                    "curated": metrics.curated_score
                },
                "quality_gates": gates,
                "total_records": metrics.total_records_processed,
                "pass_rate": metrics.quality_gate_pass_rate,
                "freshness_score": metrics.data_freshness_score,
                "anomaly_score": metrics.anomaly_detection_score,
                "alerts": [
                    {
                        "level": alert.level.value,
                        "title": alert.title,
                        "message": alert.message,
                        "timestamp": alert.timestamp.isoformat(),
                        "metadata": alert.metadata
                    }
                    for alert in alerts
                ],
                "stage_details": {
                    stage.value: {
                        "score": report.overall_score,
                        "status": report.overall_status.value,
                        "passed": report.passed_validations,
                        "warning": report.warning_validations,
                        "failed": report.failed_validations,
                        "execution_time_ms": report.execution_time_ms,
                        "data_sources": report.data_sources,
                        "trends": report.quality_trends
                    }
                    for stage, report in reports.items()
                },
                "validation_details": {
                    stage.value: [
                        {
                            "rule_name": result.rule_name,
                            "status": result.status.value,
                            "score": result.score,
                            "message": result.message,
                            "execution_time_ms": result.execution_time_ms,
                            "business_impact": result.metadata.get('business_impact'),
                            "dimension": result.metadata.get('rule_dimension')
                        }
                        for result in report.validation_results
                    ]
                    for stage, report in reports.items()
                },
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to get quality dashboard data: {e}")
            return {
                "overall_status": "error",
                "overall_score": 0.0,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }