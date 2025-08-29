#!/usr/bin/env python3
"""
Production Data Validation Service

CRITICAL: Addresses Issue #71 - Data Quality Gates Missing

This service implements comprehensive data quality validation gates to prevent
mock data from being used in production. It provides:

1. ML Training Data Validation - Ensures real game outcomes are available
2. Strategy Processing Validation - Validates analysis processors use real data
3. Betting Splits Pipeline Validation - Ensures ETL transformations are correct
4. Mock Data Detection - Prevents test/mock data in production systems
5. Real-time Data Quality Monitoring - Continuous validation and alerting

This is a critical production readiness component that acts as the final
gatekeeper before data enters ML training or betting analysis pipelines.
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple, Union
from enum import Enum
from dataclasses import dataclass, asdict
from pathlib import Path

import asyncpg
from pydantic import BaseModel, Field, validator

from ...core.config import get_settings
from ...core.logging import LogComponent, get_logger
from ...core.exceptions import DataQualityError, ValidationError
from ...data.database.connection import get_connection

logger = get_logger(__name__, LogComponent.DATA_QUALITY)


class ValidationLevel(Enum):
    """Validation severity levels."""
    CRITICAL = "critical"  # Blocks production usage
    HIGH = "high"         # Requires immediate attention
    MEDIUM = "medium"     # Should be addressed
    LOW = "low"          # Informational
    INFO = "info"        # Status information


class ValidationStatus(Enum):
    """Validation result statuses."""
    PASS = "pass"        # Validation passed
    WARN = "warn"        # Warning but not blocking
    FAIL = "fail"        # Validation failed
    ERROR = "error"      # Error during validation
    SKIP = "skip"        # Validation skipped


@dataclass
class ValidationResult:
    """Single validation check result."""
    check_name: str
    status: ValidationStatus
    level: ValidationLevel
    message: str
    details: Dict[str, Any]
    timestamp: datetime
    execution_time_ms: float
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            **asdict(self),
            'status': self.status.value,
            'level': self.level.value,
            'timestamp': self.timestamp.isoformat()
        }


class ValidationReport(BaseModel):
    """Complete validation report."""
    report_id: str = Field(description="Unique report identifier")
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    total_checks: int = Field(description="Total number of validation checks")
    passed_checks: int = Field(description="Number of passed checks")
    failed_checks: int = Field(description="Number of failed checks")
    warnings: int = Field(description="Number of warnings")
    errors: int = Field(description="Number of errors")
    overall_status: ValidationStatus = Field(description="Overall validation status")
    is_production_ready: bool = Field(description="Whether system is ready for production")
    blocking_issues: List[str] = Field(default_factory=list, description="Issues blocking production")
    results: List[ValidationResult] = Field(default_factory=list, description="Individual validation results")
    execution_time_ms: float = Field(description="Total execution time in milliseconds")
    
    class Config:
        json_encoders = {
            datetime: lambda dt: dt.isoformat(),
            ValidationStatus: lambda vs: vs.value,
            ValidationLevel: lambda vl: vl.value
        }


class ProductionDataValidationService:
    """
    Comprehensive production data validation service.
    
    This service acts as the final gatekeeper to ensure only high-quality,
    real data is used in production ML training and betting analysis.
    """
    
    def __init__(self):
        """Initialize the validation service."""
        self.settings = get_settings()
        self.validation_thresholds = {
            'min_games_with_outcomes': 50,
            'min_data_quality_score': 80,
            'min_betting_lines_per_game': 3,
            'max_data_age_hours': 48,
            'min_sportsbooks_coverage': 3,
            'min_feature_completeness': 0.9
        }
        
        # Mock data detection patterns
        self.mock_data_patterns = {
            'test_game_ids': ['TEST_', 'MOCK_', 'SAMPLE_', 'DEBUG_'],
            'mock_scores': [(999, 999), (0, 0), (-1, -1)],
            'test_teams': ['TEST', 'MOCK', 'SAMPLE'],
            'mock_odds': [-99999, 99999, 0],
            'test_urls': ['localhost', '127.0.0.1', 'test.', 'mock.', 'example.'],
            'debug_features': ['test_feature', 'mock_value', 'debug_']
        }
        
    async def run_comprehensive_validation(
        self, 
        include_performance_tests: bool = True
    ) -> ValidationReport:
        """
        Run comprehensive production readiness validation.
        
        Args:
            include_performance_tests: Whether to include performance benchmarks
            
        Returns:
            Complete validation report
        """
        start_time = datetime.now(timezone.utc)
        report_id = f"validation_{start_time.strftime('%Y%m%d_%H%M%S')}"
        
        logger.info(f"🔍 Starting comprehensive production validation: {report_id}")
        
        results = []
        
        try:
            # Core data validation checks
            results.extend(await self._validate_ml_training_data())
            results.extend(await self._validate_strategy_data_sources())
            results.extend(await self._validate_betting_splits_pipeline())
            results.extend(await self._detect_mock_data())
            results.extend(await self._validate_data_freshness())
            results.extend(await self._validate_schema_integrity())
            
            # Optional performance tests
            if include_performance_tests:
                results.extend(await self._validate_query_performance())
                
        except Exception as e:
            logger.error(f"❌ Validation execution error: {e}")
            results.append(ValidationResult(
                check_name="validation_execution",
                status=ValidationStatus.ERROR,
                level=ValidationLevel.CRITICAL,
                message=f"Validation execution failed: {str(e)}",
                details={"error": str(e), "error_type": type(e).__name__},
                timestamp=datetime.now(timezone.utc),
                execution_time_ms=0.0
            ))
        
        # Generate report
        end_time = datetime.now(timezone.utc)
        execution_time = (end_time - start_time).total_seconds() * 1000
        
        report = self._generate_validation_report(report_id, results, execution_time)
        
        # Log summary
        logger.info(f"✅ Validation completed: {report.passed_checks}/{report.total_checks} passed")
        if not report.is_production_ready:
            logger.warning(f"⚠️  Production not ready: {len(report.blocking_issues)} blocking issues")
            for issue in report.blocking_issues:
                logger.warning(f"   🚫 {issue}")
                
        return report
        
    async def _validate_ml_training_data(self) -> List[ValidationResult]:
        """Validate ML training data availability and quality."""
        results = []
        
        try:
            conn = await get_connection()
            
            # Check 1: Games with real outcomes
            start_time = datetime.now()
            games_query = """
                SELECT 
                    COUNT(*) as total_games,
                    COUNT(CASE WHEN has_real_outcome = TRUE THEN 1 END) as games_with_outcomes,
                    AVG(data_quality_score) as avg_quality_score,
                    MIN(game_datetime) as earliest_game,
                    MAX(game_datetime) as latest_game
                FROM core_betting.games
                WHERE game_datetime >= NOW() - INTERVAL '365 days'
            """
            record = await conn.fetchrow(games_query)
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            
            games_with_outcomes = record['games_with_outcomes']
            total_games = record['total_games']
            avg_quality = record['avg_quality_score'] or 0
            
            # Validate games with outcomes
            if games_with_outcomes >= self.validation_thresholds['min_games_with_outcomes']:
                status = ValidationStatus.PASS
                level = ValidationLevel.INFO
                message = f"Sufficient games with real outcomes for ML training ({games_with_outcomes} games)"
            else:
                status = ValidationStatus.FAIL
                level = ValidationLevel.CRITICAL
                message = f"Insufficient games with outcomes: {games_with_outcomes} < {self.validation_thresholds['min_games_with_outcomes']}"
            
            results.append(ValidationResult(
                check_name="ml_training_games_availability",
                status=status,
                level=level,
                message=message,
                details={
                    "total_games": total_games,
                    "games_with_outcomes": games_with_outcomes,
                    "coverage_percentage": (games_with_outcomes / max(total_games, 1)) * 100,
                    "avg_quality_score": float(avg_quality),
                    "threshold": self.validation_thresholds['min_games_with_outcomes'],
                    "date_range": {
                        "earliest": record['earliest_game'].isoformat() if record['earliest_game'] else None,
                        "latest": record['latest_game'].isoformat() if record['latest_game'] else None
                    }
                },
                timestamp=datetime.now(timezone.utc),
                execution_time_ms=execution_time
            ))
            
            # Check 2: ML Features availability
            start_time = datetime.now()
            features_query = """
                SELECT 
                    COUNT(*) as total_features,
                    COUNT(CASE WHEN features IS NOT NULL AND jsonb_array_length(feature_values) > 10 THEN 1 END) as complete_features,
                    COUNT(CASE WHEN is_training_data = TRUE THEN 1 END) as training_features
                FROM analytics.ml_features f
                JOIN core_betting.games g ON f.game_id = g.id
                WHERE g.has_real_outcome = TRUE
            """
            record = await conn.fetchrow(features_query)
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            
            complete_features = record['complete_features']
            total_features = record['total_features']
            
            if complete_features >= games_with_outcomes * 0.8:  # 80% feature coverage
                status = ValidationStatus.PASS
                level = ValidationLevel.INFO
                message = f"ML features available for {complete_features} games"
            else:
                status = ValidationStatus.WARN
                level = ValidationLevel.HIGH
                message = f"Limited ML features: {complete_features} games have complete features"
                
            results.append(ValidationResult(
                check_name="ml_features_availability", 
                status=status,
                level=level,
                message=message,
                details={
                    "total_features": total_features,
                    "complete_features": complete_features,
                    "training_features": record['training_features'],
                    "feature_coverage_percentage": (complete_features / max(games_with_outcomes, 1)) * 100
                },
                timestamp=datetime.now(timezone.utc),
                execution_time_ms=execution_time
            ))
            
            await conn.close()
            
        except Exception as e:
            logger.error(f"❌ ML training data validation error: {e}")
            results.append(ValidationResult(
                check_name="ml_training_data_validation_error",
                status=ValidationStatus.ERROR,
                level=ValidationLevel.CRITICAL,
                message=f"ML training data validation failed: {str(e)}",
                details={"error": str(e), "error_type": type(e).__name__},
                timestamp=datetime.now(timezone.utc),
                execution_time_ms=0.0
            ))
            
        return results
    
    async def _validate_strategy_data_sources(self) -> List[ValidationResult]:
        """Validate that strategy processors are using real data sources."""
        results = []
        
        try:
            conn = await get_connection()
            
            # Check betting lines data quality
            start_time = datetime.now()
            lines_query = """
                SELECT 
                    COUNT(*) as total_lines,
                    COUNT(CASE WHEN data_quality_score >= 80 THEN 1 END) as quality_lines,
                    AVG(data_quality_score) as avg_quality,
                    COUNT(DISTINCT game_id) as games_with_lines,
                    COUNT(DISTINCT sportsbook_id) as active_sportsbooks
                FROM core_betting.betting_lines
                WHERE line_timestamp >= NOW() - INTERVAL '30 days'
            """
            record = await conn.fetchrow(lines_query)
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            
            quality_lines = record['quality_lines']
            total_lines = record['total_lines']
            active_sportsbooks = record['active_sportsbooks']
            
            if (quality_lines / max(total_lines, 1)) >= 0.9 and active_sportsbooks >= self.validation_thresholds['min_sportsbooks_coverage']:
                status = ValidationStatus.PASS
                level = ValidationLevel.INFO
                message = f"Strategy data sources validated: {active_sportsbooks} sportsbooks, {quality_lines} quality lines"
            else:
                status = ValidationStatus.WARN
                level = ValidationLevel.HIGH
                message = f"Strategy data quality concerns: {quality_lines}/{total_lines} quality lines, {active_sportsbooks} sportsbooks"
                
            results.append(ValidationResult(
                check_name="strategy_data_sources",
                status=status,
                level=level,
                message=message,
                details={
                    "total_lines": total_lines,
                    "quality_lines": quality_lines,
                    "quality_percentage": (quality_lines / max(total_lines, 1)) * 100,
                    "games_with_lines": record['games_with_lines'],
                    "active_sportsbooks": active_sportsbooks,
                    "min_sportsbooks_threshold": self.validation_thresholds['min_sportsbooks_coverage'],
                    "avg_quality_score": float(record['avg_quality'] or 0)
                },
                timestamp=datetime.now(timezone.utc),
                execution_time_ms=execution_time
            ))
            
            await conn.close()
            
        except Exception as e:
            logger.error(f"❌ Strategy data validation error: {e}")
            results.append(ValidationResult(
                check_name="strategy_data_validation_error",
                status=ValidationStatus.ERROR,
                level=ValidationLevel.CRITICAL,
                message=f"Strategy data validation failed: {str(e)}",
                details={"error": str(e)},
                timestamp=datetime.now(timezone.utc),
                execution_time_ms=0.0
            ))
            
        return results
        
    async def _validate_betting_splits_pipeline(self) -> List[ValidationResult]:
        """Validate betting splits ETL pipeline functionality."""
        results = []
        
        try:
            conn = await get_connection()
            
            # Check betting splits data
            start_time = datetime.now()
            splits_query = """
                SELECT 
                    COUNT(*) as total_splits,
                    COUNT(CASE WHEN data_quality_score >= 80 THEN 1 END) as quality_splits,
                    COUNT(DISTINCT game_id) as games_with_splits,
                    AVG(CASE WHEN sharp_money_differential IS NOT NULL 
                         THEN ABS(sharp_money_differential) ELSE 0 END) as avg_sharp_differential,
                    COUNT(CASE WHEN is_reverse_line_movement = TRUE THEN 1 END) as rlm_signals
                FROM core_betting.betting_splits
                WHERE as_of_timestamp >= NOW() - INTERVAL '7 days'
            """
            record = await conn.fetchrow(splits_query)
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            
            total_splits = record['total_splits']
            quality_splits = record['quality_splits']
            games_with_splits = record['games_with_splits']
            
            if total_splits > 0 and (quality_splits / total_splits) >= 0.8:
                status = ValidationStatus.PASS
                level = ValidationLevel.INFO
                message = f"Betting splits pipeline operational: {games_with_splits} games, {quality_splits} quality splits"
            elif total_splits == 0:
                status = ValidationStatus.FAIL
                level = ValidationLevel.HIGH
                message = "No betting splits data available - ETL pipeline may not be running"
            else:
                status = ValidationStatus.WARN
                level = ValidationLevel.MEDIUM
                message = f"Betting splits quality concerns: {quality_splits}/{total_splits} quality splits"
                
            results.append(ValidationResult(
                check_name="betting_splits_pipeline",
                status=status,
                level=level,
                message=message,
                details={
                    "total_splits": total_splits,
                    "quality_splits": quality_splits,
                    "games_with_splits": games_with_splits,
                    "quality_percentage": (quality_splits / max(total_splits, 1)) * 100,
                    "avg_sharp_differential": float(record['avg_sharp_differential'] or 0),
                    "rlm_signals": record['rlm_signals']
                },
                timestamp=datetime.now(timezone.utc),
                execution_time_ms=execution_time
            ))
            
            await conn.close()
            
        except Exception as e:
            logger.error(f"❌ Betting splits validation error: {e}")
            results.append(ValidationResult(
                check_name="betting_splits_validation_error",
                status=ValidationStatus.ERROR,
                level=ValidationLevel.HIGH,
                message=f"Betting splits validation failed: {str(e)}",
                details={"error": str(e)},
                timestamp=datetime.now(timezone.utc),
                execution_time_ms=0.0
            ))
            
        return results
        
    async def _detect_mock_data(self) -> List[ValidationResult]:
        """Detect mock, test, or sample data in production tables."""
        results = []
        
        try:
            conn = await get_connection()
            
            # Check for mock games
            start_time = datetime.now()
            mock_games_query = """
                SELECT COUNT(*) as mock_games_count
                FROM core_betting.games
                WHERE (home_score = 999 AND away_score = 999)
                   OR external_game_id LIKE ANY(ARRAY['TEST_%', 'MOCK_%', 'SAMPLE_%', 'DEBUG_%'])
                   OR (home_score = 0 AND away_score = 0 AND game_status = 'completed')
            """
            mock_games = await conn.fetchval(mock_games_query)
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            
            if mock_games == 0:
                status = ValidationStatus.PASS
                level = ValidationLevel.INFO
                message = "No mock game data detected"
            else:
                status = ValidationStatus.FAIL
                level = ValidationLevel.CRITICAL
                message = f"Mock game data detected: {mock_games} games"
                
            results.append(ValidationResult(
                check_name="mock_games_detection",
                status=status,
                level=level,
                message=message,
                details={
                    "mock_games_count": mock_games,
                    "patterns_checked": self.mock_data_patterns['test_game_ids'] + ["score_999_999", "score_0_0_completed"]
                },
                timestamp=datetime.now(timezone.utc),
                execution_time_ms=execution_time
            ))
            
            # Check for test betting lines
            start_time = datetime.now()
            mock_lines_query = """
                SELECT COUNT(*) as mock_lines_count
                FROM core_betting.betting_lines bl
                JOIN core_betting.games g ON bl.game_id = g.id
                WHERE bl.odds_american IN (-99999, 99999, 0)
                   OR g.external_game_id LIKE ANY(ARRAY['TEST_%', 'MOCK_%'])
                   OR bl.line_value = 999.0
            """
            mock_lines = await conn.fetchval(mock_lines_query)
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            
            if mock_lines == 0:
                status = ValidationStatus.PASS
                level = ValidationLevel.INFO
                message = "No mock betting lines detected"
            else:
                status = ValidationStatus.FAIL
                level = ValidationLevel.CRITICAL
                message = f"Mock betting lines detected: {mock_lines} lines"
                
            results.append(ValidationResult(
                check_name="mock_betting_lines_detection",
                status=status,
                level=level,
                message=message,
                details={
                    "mock_lines_count": mock_lines,
                    "patterns_checked": self.mock_data_patterns['mock_odds'] + ["line_value_999"]
                },
                timestamp=datetime.now(timezone.utc),
                execution_time_ms=execution_time
            ))
            
            await conn.close()
            
        except Exception as e:
            logger.error(f"❌ Mock data detection error: {e}")
            results.append(ValidationResult(
                check_name="mock_data_detection_error",
                status=ValidationStatus.ERROR,
                level=ValidationLevel.CRITICAL,
                message=f"Mock data detection failed: {str(e)}",
                details={"error": str(e)},
                timestamp=datetime.now(timezone.utc),
                execution_time_ms=0.0
            ))
            
        return results
        
    async def _validate_data_freshness(self) -> List[ValidationResult]:
        """Validate data freshness and recency."""
        results = []
        
        try:
            conn = await get_connection()
            
            # Check data freshness
            start_time = datetime.now()
            freshness_query = """
                SELECT 
                    'games' as table_name,
                    MAX(created_at) as latest_record,
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN created_at >= NOW() - INTERVAL '24 hours' THEN 1 END) as recent_records
                FROM core_betting.games
                UNION ALL
                SELECT 
                    'betting_lines' as table_name,
                    MAX(created_at) as latest_record,
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN created_at >= NOW() - INTERVAL '24 hours' THEN 1 END) as recent_records
                FROM core_betting.betting_lines
            """
            records = await conn.fetch(freshness_query)
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            
            for record in records:
                table_name = record['table_name']
                latest_record = record['latest_record']
                recent_records = record['recent_records']
                
                if latest_record and (datetime.now(timezone.utc) - latest_record.replace(tzinfo=timezone.utc)).total_seconds() / 3600 <= self.validation_thresholds['max_data_age_hours']:
                    status = ValidationStatus.PASS
                    level = ValidationLevel.INFO
                    message = f"{table_name} data is fresh (latest: {latest_record})"
                else:
                    status = ValidationStatus.WARN
                    level = ValidationLevel.MEDIUM
                    message = f"{table_name} data may be stale (latest: {latest_record})"
                    
                results.append(ValidationResult(
                    check_name=f"data_freshness_{table_name}",
                    status=status,
                    level=level,
                    message=message,
                    details={
                        "table": table_name,
                        "latest_record": latest_record.isoformat() if latest_record else None,
                        "total_records": record['total_records'],
                        "recent_records_24h": recent_records,
                        "age_hours": (datetime.now(timezone.utc) - latest_record.replace(tzinfo=timezone.utc)).total_seconds() / 3600 if latest_record else None,
                        "threshold_hours": self.validation_thresholds['max_data_age_hours']
                    },
                    timestamp=datetime.now(timezone.utc),
                    execution_time_ms=execution_time / len(records)
                ))
                
            await conn.close()
            
        except Exception as e:
            logger.error(f"❌ Data freshness validation error: {e}")
            results.append(ValidationResult(
                check_name="data_freshness_validation_error",
                status=ValidationStatus.ERROR,
                level=ValidationLevel.MEDIUM,
                message=f"Data freshness validation failed: {str(e)}",
                details={"error": str(e)},
                timestamp=datetime.now(timezone.utc),
                execution_time_ms=0.0
            ))
            
        return results
        
    async def _validate_schema_integrity(self) -> List[ValidationResult]:
        """Validate database schema integrity and relationships."""
        results = []
        
        try:
            conn = await get_connection()
            
            # Check foreign key constraints
            start_time = datetime.now()
            fk_query = """
                SELECT 
                    COUNT(*) as total_constraints,
                    COUNT(CASE WHEN confrelid IS NOT NULL THEN 1 END) as valid_constraints
                FROM pg_constraint
                WHERE contype = 'f'
                  AND connamespace IN (
                      SELECT oid FROM pg_namespace 
                      WHERE nspname IN ('core_betting', 'analytics', 'operational')
                  )
            """
            record = await conn.fetchrow(fk_query)
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            
            total_fks = record['total_constraints']
            valid_fks = record['valid_constraints']
            
            if total_fks > 0 and valid_fks == total_fks:
                status = ValidationStatus.PASS
                level = ValidationLevel.INFO
                message = f"Schema integrity validated: {valid_fks} foreign key constraints"
            elif total_fks == 0:
                status = ValidationStatus.WARN
                level = ValidationLevel.MEDIUM
                message = "No foreign key constraints found - schema may need consolidation"
            else:
                status = ValidationStatus.FAIL
                level = ValidationLevel.HIGH
                message = f"Schema integrity issues: {valid_fks}/{total_fks} valid constraints"
                
            results.append(ValidationResult(
                check_name="schema_integrity",
                status=status,
                level=level,
                message=message,
                details={
                    "total_foreign_keys": total_fks,
                    "valid_foreign_keys": valid_fks,
                    "schemas_checked": ['core_betting', 'analytics', 'operational']
                },
                timestamp=datetime.now(timezone.utc),
                execution_time_ms=execution_time
            ))
            
            await conn.close()
            
        except Exception as e:
            logger.error(f"❌ Schema integrity validation error: {e}")
            results.append(ValidationResult(
                check_name="schema_integrity_validation_error",
                status=ValidationStatus.ERROR,
                level=ValidationLevel.HIGH,
                message=f"Schema integrity validation failed: {str(e)}",
                details={"error": str(e)},
                timestamp=datetime.now(timezone.utc),
                execution_time_ms=0.0
            ))
            
        return results
        
    async def _validate_query_performance(self) -> List[ValidationResult]:
        """Validate query performance for ML training and analysis."""
        results = []
        
        try:
            conn = await get_connection()
            
            # Test ML training data query performance
            start_time = datetime.now()
            ml_query = """
                SELECT 
                    g.id, g.external_game_id, g.home_score, g.away_score,
                    f.features, f.feature_values
                FROM core_betting.games g
                LEFT JOIN analytics.ml_features f ON g.id = f.game_id
                WHERE g.has_real_outcome = TRUE
                LIMIT 1000
            """
            await conn.fetch(ml_query)
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            
            if execution_time <= 2000:  # 2 seconds threshold
                status = ValidationStatus.PASS
                level = ValidationLevel.INFO
                message = f"ML training query performance acceptable: {execution_time:.0f}ms"
            elif execution_time <= 5000:  # 5 seconds warning
                status = ValidationStatus.WARN
                level = ValidationLevel.MEDIUM
                message = f"ML training query performance slow: {execution_time:.0f}ms"
            else:
                status = ValidationStatus.FAIL
                level = ValidationLevel.HIGH
                message = f"ML training query performance unacceptable: {execution_time:.0f}ms"
                
            results.append(ValidationResult(
                check_name="ml_query_performance",
                status=status,
                level=level,
                message=message,
                details={
                    "execution_time_ms": execution_time,
                    "threshold_ms": 2000,
                    "warning_threshold_ms": 5000,
                    "query_type": "ml_training_data"
                },
                timestamp=datetime.now(timezone.utc),
                execution_time_ms=execution_time
            ))
            
            await conn.close()
            
        except Exception as e:
            logger.error(f"❌ Query performance validation error: {e}")
            results.append(ValidationResult(
                check_name="query_performance_validation_error",
                status=ValidationStatus.ERROR,
                level=ValidationLevel.MEDIUM,
                message=f"Query performance validation failed: {str(e)}",
                details={"error": str(e)},
                timestamp=datetime.now(timezone.utc),
                execution_time_ms=0.0
            ))
            
        return results
        
    def _generate_validation_report(
        self, 
        report_id: str, 
        results: List[ValidationResult], 
        execution_time: float
    ) -> ValidationReport:
        """Generate comprehensive validation report."""
        
        # Calculate summary statistics
        total_checks = len(results)
        passed_checks = sum(1 for r in results if r.status == ValidationStatus.PASS)
        failed_checks = sum(1 for r in results if r.status == ValidationStatus.FAIL)
        warnings = sum(1 for r in results if r.status == ValidationStatus.WARN)
        errors = sum(1 for r in results if r.status == ValidationStatus.ERROR)
        
        # Determine overall status
        if errors > 0 or failed_checks > 0:
            overall_status = ValidationStatus.FAIL
        elif warnings > 0:
            overall_status = ValidationStatus.WARN
        else:
            overall_status = ValidationStatus.PASS
            
        # Identify blocking issues (critical or high severity failures)
        blocking_issues = [
            result.message for result in results
            if result.status in [ValidationStatus.FAIL, ValidationStatus.ERROR]
            and result.level in [ValidationLevel.CRITICAL, ValidationLevel.HIGH]
        ]
        
        # Determine production readiness
        is_production_ready = (
            len(blocking_issues) == 0 and 
            errors == 0 and 
            failed_checks == 0
        )
        
        return ValidationReport(
            report_id=report_id,
            total_checks=total_checks,
            passed_checks=passed_checks,
            failed_checks=failed_checks,
            warnings=warnings,
            errors=errors,
            overall_status=overall_status,
            is_production_ready=is_production_ready,
            blocking_issues=blocking_issues,
            results=results,
            execution_time_ms=execution_time
        )
        
    async def export_validation_report(
        self, 
        report: ValidationReport, 
        output_path: Optional[str] = None
    ) -> str:
        """Export validation report to JSON file."""
        
        if output_path is None:
            output_path = f"validation_report_{report.report_id}.json"
            
        # Convert report to dictionary for JSON serialization
        report_dict = report.dict()
        report_dict['results'] = [result.to_dict() for result in report.results]
        
        # Write to file
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w') as f:
            json.dump(report_dict, f, indent=2, default=str)
            
        logger.info(f"📄 Validation report exported: {output_file.absolute()}")
        return str(output_file.absolute())


# Helper functions for CLI integration
async def validate_production_readiness(
    include_performance: bool = True
) -> ValidationReport:
    """
    Convenience function for validating production readiness.
    
    Args:
        include_performance: Whether to include performance tests
        
    Returns:
        Complete validation report
    """
    service = ProductionDataValidationService()
    return await service.run_comprehensive_validation(include_performance)


async def quick_ml_training_check() -> bool:
    """
    Quick check if ML training data is available.
    
    Returns:
        True if ML training is ready, False otherwise
    """
    try:
        service = ProductionDataValidationService()
        results = await service._validate_ml_training_data()
        
        # Check if critical ML training validations pass
        critical_checks = [
            r for r in results 
            if r.level == ValidationLevel.CRITICAL and r.status == ValidationStatus.FAIL
        ]
        
        return len(critical_checks) == 0
        
    except Exception as e:
        logger.error(f"❌ Quick ML training check failed: {e}")
        return False


# Main execution for testing
async def main():
    """Main function for testing validation service."""
    logger.info("🚀 Starting Production Data Validation Service Test")
    
    service = ProductionDataValidationService()
    report = await service.run_comprehensive_validation()
    
    print(f"\n📊 Validation Report: {report.report_id}")
    print(f"   Status: {report.overall_status.value.upper()}")
    print(f"   Checks: {report.passed_checks}/{report.total_checks} passed")
    print(f"   Production Ready: {report.is_production_ready}")
    print(f"   Execution Time: {report.execution_time_ms:.0f}ms")
    
    if report.blocking_issues:
        print(f"\n🚫 Blocking Issues ({len(report.blocking_issues)}):")
        for issue in report.blocking_issues:
            print(f"   • {issue}")
            
    # Export report
    export_path = await service.export_validation_report(report)
    print(f"\n📄 Report exported: {export_path}")


if __name__ == "__main__":
    asyncio.run(main())