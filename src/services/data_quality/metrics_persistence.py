#!/usr/bin/env python3
"""
Data Quality Metrics Persistence Service

Performance-optimized service for persisting data quality validation results
and metrics to the database for historical tracking and trend analysis.
"""

import asyncio
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any
from contextlib import asynccontextmanager

from ...core.config import UnifiedSettings
from ...core.logging import LogComponent, get_logger
from ...data.database.connection import get_db_manager
from .validation_service import (
    PipelineStage,
    QualityReport,
    ValidationResult,
    QualityMetrics,
    ValidationStatus
)

logger = get_logger(__name__, LogComponent.DATA_QUALITY)


class DataQualityMetricsPersistence:
    """
    Performance-optimized persistence service for data quality metrics.
    
    Features:
    - Bulk insert operations for high throughput
    - Connection pooling and transaction management
    - Async operations for non-blocking persistence
    - Configurable batch sizes and retention policies
    - Time-series data optimization
    """
    
    def __init__(self, config: UnifiedSettings):
        self.config = config
        self.db_manager = get_db_manager()
        
        # Performance configuration
        self.batch_size = 100  # Records per batch insert
        self.connection_pool_size = 5
        self.retention_days = 90  # Default retention period
        
        # Performance optimization flags
        self.enable_batch_insert = True
        self.enable_async_writes = True
        self.enable_compression = True
        
    async def persist_validation_run(
        self, 
        reports: Dict[PipelineStage, QualityReport],
        metrics: QualityMetrics,
        triggered_by: str = "manual",
        notes: Optional[str] = None
    ) -> str:
        """
        Persist a complete validation run with all results.
        
        Args:
            reports: Validation reports by pipeline stage
            metrics: Overall quality metrics
            triggered_by: What triggered this validation
            notes: Optional notes about the validation
            
        Returns:
            UUID of the created validation run
        """
        run_id = str(uuid.uuid4())
        start_time = datetime.now()
        
        try:
            async with self._get_connection() as conn:
                async with conn.transaction():
                    # Insert main validation run record
                    validation_run_id = await self._insert_validation_run(
                        conn, run_id, reports, metrics, triggered_by, notes
                    )
                    
                    # Insert rule results in batches for performance
                    await self._insert_rule_results_batch(conn, validation_run_id, reports)
                    
                    # Insert time series metrics
                    await self._insert_timeseries_metrics(conn, reports, metrics)
                    
            execution_time = (datetime.now() - start_time).total_seconds()
            logger.info(f"Persisted validation run {run_id} in {execution_time:.3f}s")
            
            return run_id
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            logger.error(f"Failed to persist validation run after {execution_time:.3f}s: {e}")
            raise
    
    async def _insert_validation_run(
        self,
        conn,
        run_id: str,
        reports: Dict[PipelineStage, QualityReport],
        metrics: QualityMetrics,
        triggered_by: str,
        notes: Optional[str]
    ) -> int:
        """Insert the main validation run record."""
        
        # Determine stage for this run
        if len(reports) > 1:
            stage = "all"
            overall_score = metrics.overall_score
        else:
            stage = list(reports.keys())[0].value
            overall_score = list(reports.values())[0].overall_score
        
        # Calculate overall status
        all_statuses = [report.overall_status for report in reports.values()]
        if any(status == ValidationStatus.FAILED for status in all_statuses):
            overall_status = "failed"
        elif any(status == ValidationStatus.WARNING for status in all_statuses):
            overall_status = "warning"
        else:
            overall_status = "passed"
        
        # Aggregate validation counts
        total_validations = sum(len(report.validation_results) for report in reports.values())
        passed_validations = sum(report.passed_validations for report in reports.values())
        warning_validations = sum(report.warning_validations for report in reports.values())
        failed_validations = sum(report.failed_validations for report in reports.values())
        total_records = sum(report.total_records for report in reports.values())
        data_sources_count = len(set().union(*(report.data_sources for report in reports.values())))
        
        # Get execution time
        execution_duration_ms = sum(report.execution_time_ms for report in reports.values())
        execution_end_time = datetime.now()
        execution_start_time = execution_end_time - timedelta(milliseconds=execution_duration_ms)
        
        query = """
        INSERT INTO monitoring.data_quality_validation_runs (
            run_id, stage, execution_start_time, execution_end_time, execution_duration_ms,
            overall_score, overall_status, raw_score, staging_score, curated_score,
            total_validations, passed_validations, warning_validations, failed_validations,
            total_records_validated, data_sources_count, data_freshness_score,
            anomaly_detection_score, quality_gate_pass_rate, triggered_by, notes
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
        RETURNING id
        """
        
        from datetime import timedelta
        
        result = await conn.fetchrow(
            query,
            run_id,
            stage,
            execution_start_time,
            execution_end_time,
            int(execution_duration_ms),
            float(overall_score),
            overall_status,
            float(metrics.raw_score) if stage == "all" else None,
            float(metrics.staging_score) if stage == "all" else None,
            float(metrics.curated_score) if stage == "all" else None,
            total_validations,
            passed_validations,
            warning_validations,
            failed_validations,
            total_records,
            data_sources_count,
            float(metrics.data_freshness_score),
            float(metrics.anomaly_detection_score),
            float(metrics.quality_gate_pass_rate),
            triggered_by,
            notes
        )
        
        return result['id']
    
    async def _insert_rule_results_batch(
        self,
        conn,
        validation_run_id: int,
        reports: Dict[PipelineStage, QualityReport]
    ):
        """Insert rule results using batch operations for performance."""
        
        # Collect all rule results
        all_results = []
        for stage, report in reports.items():
            for result in report.validation_results:
                all_results.append((stage, result))
        
        if not all_results:
            return
        
        # Process in batches
        for i in range(0, len(all_results), self.batch_size):
            batch = all_results[i:i + self.batch_size]
            await self._insert_rule_results_single_batch(conn, validation_run_id, batch)
    
    async def _insert_rule_results_single_batch(
        self,
        conn,
        validation_run_id: int,
        batch: List[tuple]
    ):
        """Insert a single batch of rule results."""
        
        query = """
        INSERT INTO monitoring.data_quality_rule_results (
            validation_run_id, rule_name, rule_stage, rule_dimension,
            execution_start_time, execution_end_time, execution_duration_ms,
            score, status, threshold, warning_threshold, total_records,
            valid_records, business_impact, message, metadata
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
        """
        
        rows = []
        for stage, result in batch:
            execution_end_time = datetime.now()
            execution_start_time = execution_end_time - timedelta(milliseconds=result.execution_time_ms)
            
            from datetime import timedelta
            import json
            
            total_records = result.metadata.get('total_records', 0)
            valid_records = result.metadata.get('good_records', 0)
            business_impact = result.metadata.get('business_impact', 'medium')
            rule_dimension = result.metadata.get('rule_dimension', 'unknown')
            
            rows.append((
                validation_run_id,
                result.rule_name,
                stage.value,
                rule_dimension,
                execution_start_time,
                execution_end_time,
                int(result.execution_time_ms),
                float(result.score),
                result.status.value,
                float(result.threshold),
                float(result.warning_threshold),
                total_records,
                valid_records,
                business_impact,
                result.message,
                json.dumps(result.metadata) if result.metadata else None
            ))
        
        await conn.executemany(query, rows)
    
    async def _insert_timeseries_metrics(
        self,
        conn,
        reports: Dict[PipelineStage, QualityReport],
        metrics: QualityMetrics
    ):
        """Insert time series metrics for trending analysis."""
        
        query = """
        INSERT INTO monitoring.data_quality_metrics_timeseries (
            measurement_time, stage, dimension, metric_name, value, 
            threshold, records_count, tags
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """
        
        measurement_time = datetime.now()
        rows = []
        
        # Overall metrics
        rows.extend([
            (measurement_time, "overall", "overall", "pipeline_score", float(metrics.overall_score), 0.90, metrics.total_records_processed, {"source": "pipeline"}),
            (measurement_time, "overall", "timeliness", "freshness_score", float(metrics.data_freshness_score), 0.90, 0, {"source": "pipeline"}),
            (measurement_time, "overall", "accuracy", "anomaly_score", float(metrics.anomaly_detection_score), 0.95, 0, {"source": "pipeline"}),
            (measurement_time, "overall", "overall", "gate_pass_rate", float(metrics.quality_gate_pass_rate), 0.95, 0, {"source": "pipeline"})
        ])
        
        # Stage-specific metrics
        for stage, report in reports.items():
            stage_name = stage.value
            
            # Overall stage score
            rows.append((
                measurement_time, stage_name, "overall", "stage_score", 
                float(report.overall_score), self._get_stage_threshold(stage), 
                report.total_records, {"source": "validation"}
            ))
            
            # Rule dimension scores
            dimension_scores = {}
            for result in report.validation_results:
                dimension = result.metadata.get('rule_dimension', 'unknown')
                if dimension not in dimension_scores:
                    dimension_scores[dimension] = []
                dimension_scores[dimension].append(result.score)
            
            for dimension, scores in dimension_scores.items():
                avg_score = sum(scores) / len(scores) if scores else 0.0
                rows.append((
                    measurement_time, stage_name, dimension, "dimension_score",
                    float(avg_score), 0.90, 0, {"rule_count": len(scores)}
                ))
        
        # Insert in batches
        import json
        formatted_rows = []
        for row in rows:
            formatted_row = list(row)
            formatted_row[7] = json.dumps(formatted_row[7])  # Convert tags to JSON
            formatted_rows.append(tuple(formatted_row))
        
        for i in range(0, len(formatted_rows), self.batch_size):
            batch = formatted_rows[i:i + self.batch_size]
            await conn.executemany(query, batch)
    
    def _get_stage_threshold(self, stage: PipelineStage) -> float:
        """Get quality threshold for a pipeline stage."""
        thresholds = {
            PipelineStage.RAW: 0.85,
            PipelineStage.STAGING: 0.90,
            PipelineStage.CURATED: 0.95
        }
        return thresholds.get(stage, 0.90)
    
    async def persist_quality_alert(
        self,
        alert_level: str,
        title: str,
        message: str,
        source: str,
        stage: Optional[str] = None,
        rule_name: Optional[str] = None,
        affected_records: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
        validation_run_id: Optional[int] = None
    ) -> str:
        """
        Persist a quality alert to the database.
        
        Returns:
            UUID of the created alert
        """
        alert_id = str(uuid.uuid4())
        
        try:
            query = """
            INSERT INTO monitoring.data_quality_alerts (
                alert_id, alert_level, title, message, source, stage,
                rule_name, affected_records, metadata, validation_run_id
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """
            
            import json
            
            await self.db_manager.execute_query(
                query,
                alert_id,
                alert_level,
                title,
                message,
                source,
                stage,
                rule_name,
                affected_records,
                json.dumps(metadata) if metadata else None,
                validation_run_id
            )
            
            logger.debug(f"Persisted quality alert {alert_id}")
            return alert_id
            
        except Exception as e:
            logger.error(f"Failed to persist quality alert: {e}")
            raise
    
    async def get_quality_trends(
        self,
        stage: Optional[str] = None,
        hours_back: int = 24,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get quality trends data for dashboard."""
        
        try:
            where_clause = ""
            params = [hours_back, limit]
            
            if stage:
                where_clause = "AND stage = $3"
                params.append(stage)
            
            query = f"""
            SELECT 
                stage,
                overall_score,
                overall_status,
                execution_start_time,
                total_records_validated,
                data_freshness_score,
                quality_gate_pass_rate,
                execution_duration_ms
            FROM monitoring.data_quality_validation_runs
            WHERE execution_start_time > NOW() - INTERVAL '%s hours'
                {where_clause}
            ORDER BY execution_start_time DESC
            LIMIT $2
            """
            
            result = await self.db_manager.execute_query(query.replace('%s', '$1'), *params)
            return [dict(row) for row in result]
            
        except Exception as e:
            logger.error(f"Failed to get quality trends: {e}")
            return []
    
    async def get_active_alerts(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get active quality alerts."""
        
        try:
            query = """
            SELECT 
                alert_id,
                alert_level,
                title,
                message,
                source,
                stage,
                rule_name,
                created_at,
                affected_records,
                metadata
            FROM monitoring.data_quality_alerts
            WHERE resolved_at IS NULL
            ORDER BY 
                CASE alert_level 
                    WHEN 'critical' THEN 1 
                    WHEN 'error' THEN 2 
                    WHEN 'warning' THEN 3 
                    ELSE 4 
                END,
                created_at DESC
            LIMIT $1
            """
            
            result = await self.db_manager.execute_query(query, limit)
            
            alerts = []
            for row in result:
                alert = dict(row)
                if alert['metadata']:
                    import json
                    alert['metadata'] = json.loads(alert['metadata'])
                alerts.append(alert)
            
            return alerts
            
        except Exception as e:
            logger.error(f"Failed to get active alerts: {e}")
            return []
    
    async def resolve_alert(
        self,
        alert_id: str,
        resolved_by: str = "system",
        resolution_notes: Optional[str] = None
    ) -> bool:
        """Mark an alert as resolved."""
        
        try:
            query = """
            UPDATE monitoring.data_quality_alerts
            SET resolved_at = NOW(), resolved_by = $2, resolution_notes = $3
            WHERE alert_id = $1 AND resolved_at IS NULL
            """
            
            result = await self.db_manager.execute_query(
                query, alert_id, resolved_by, resolution_notes
            )
            
            # Check if any rows were updated
            return bool(result)
            
        except Exception as e:
            logger.error(f"Failed to resolve alert {alert_id}: {e}")
            return False
    
    async def cleanup_old_data(self, retention_days: Optional[int] = None) -> Dict[str, int]:
        """Clean up old quality data based on retention policy."""
        
        retention = retention_days or self.retention_days
        
        try:
            query = "SELECT * FROM monitoring.cleanup_old_quality_data($1)"
            result = await self.db_manager.execute_query(query, retention)
            
            if result:
                row = result[0]
                cleanup_stats = {
                    "deleted_runs": row['deleted_runs'],
                    "deleted_rule_results": row['deleted_rule_results'],
                    "deleted_alerts": row['deleted_alerts'],
                    "deleted_metrics": row['deleted_metrics']
                }
                
                total_deleted = sum(cleanup_stats.values())
                logger.info(f"Cleaned up {total_deleted} old quality records "
                           f"(retention: {retention} days)")
                
                return cleanup_stats
            
            return {}
            
        except Exception as e:
            logger.error(f"Failed to cleanup old quality data: {e}")
            return {}
    
    async def get_dashboard_metrics(self, hours_back: int = 24) -> Dict[str, Any]:
        """Get comprehensive dashboard metrics."""
        
        try:
            query = "SELECT * FROM monitoring.get_quality_dashboard_metrics($1)"
            result = await self.db_manager.execute_query(query, hours_back)
            
            metrics = {}
            for row in result:
                stage = row['stage']
                metrics[stage] = {
                    'current_score': float(row['current_score'] or 0),
                    'avg_score_24h': float(row['avg_score_24h'] or 0),
                    'trend_direction': row['trend_direction'],
                    'active_alerts': row['active_alerts'],
                    'last_validation': row['last_validation'],
                    'sla_compliance': float(row['sla_compliance'] or 0)
                }
            
            return metrics
            
        except Exception as e:
            logger.error(f"Failed to get dashboard metrics: {e}")
            return {}
    
    @asynccontextmanager
    async def _get_connection(self):
        """Get a database connection with proper resource management."""
        # Use the existing database manager's connection
        # This is a simplified version - in production you'd use a proper connection pool
        try:
            # For now, we'll use the database manager directly
            # In a full implementation, you'd get a connection from a pool
            yield self.db_manager
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise
    
    async def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics for the persistence service."""
        
        try:
            # Get recent validation run performance
            query = """
            SELECT 
                COUNT(*) as total_runs,
                AVG(execution_duration_ms) as avg_duration_ms,
                MAX(execution_duration_ms) as max_duration_ms,
                MIN(execution_duration_ms) as min_duration_ms,
                AVG(total_records_validated) as avg_records_per_run
            FROM monitoring.data_quality_validation_runs
            WHERE execution_start_time > NOW() - INTERVAL '24 hours'
            """
            
            result = await self.db_manager.execute_query(query)
            stats = dict(result[0]) if result else {}
            
            # Add persistence configuration
            stats.update({
                'batch_size': self.batch_size,
                'retention_days': self.retention_days,
                'enable_batch_insert': self.enable_batch_insert,
                'enable_async_writes': self.enable_async_writes
            })
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get performance stats: {e}")
            return {}