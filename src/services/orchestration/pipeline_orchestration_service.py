#!/usr/bin/env python3
"""
Pipeline Orchestration Service

Migrated and enhanced pipeline orchestration functionality from the legacy module.
Provides intelligent orchestration of data collection, analysis, backtesting,
and detection pipelines with comprehensive state management and optimization.

Legacy Source: src/mlb_sharp_betting/services/pipeline_orchestrator.py
Enhanced Features:
- Unified architecture integration
- Enhanced state analysis and decision making
- Improved pipeline coordination and dependency management
- Better error handling and recovery
- Comprehensive monitoring and metrics

Part of Phase 5D: Critical Business Logic Migration
"""

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from ...core.config import get_settings
from ...core.exceptions import OrchestrationError, PipelineError
from ...core.logging import get_logger
from ...data.database.connection import get_connection
from ...services.data.enhanced_data_service import EnhancedDataService

logger = get_logger(__name__)


class PipelineStage(str, Enum):
    """Pipeline stage enumeration."""

    DATA_COLLECTION = "data_collection"
    DATA_VALIDATION = "data_validation"
    ANALYSIS = "analysis"
    BACKTESTING = "backtesting"
    DETECTION = "detection"
    NOTIFICATION = "notification"


class PipelineStatus(str, Enum):
    """Pipeline execution status."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL_SUCCESS = "partial_success"
    CANCELLED = "cancelled"


class SystemHealth(str, Enum):
    """System health status."""

    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


@dataclass
class PipelineStageResult:
    """Result of a pipeline stage execution."""

    stage: PipelineStage
    status: PipelineStatus
    start_time: datetime
    end_time: datetime | None = None
    execution_time_seconds: float = 0.0
    records_processed: int = 0
    success_count: int = 0
    error_count: int = 0
    errors: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def mark_completed(self, status: PipelineStatus):
        """Mark stage as completed."""
        self.end_time = datetime.now(timezone.utc)
        self.execution_time_seconds = (self.end_time - self.start_time).total_seconds()
        self.status = status


@dataclass
class PipelineExecutionResult:
    """Result of a complete pipeline execution."""

    pipeline_id: str
    pipeline_type: str
    start_time: datetime
    end_time: datetime | None = None
    total_execution_time: float = 0.0
    overall_status: PipelineStatus = PipelineStatus.PENDING
    stages: dict[PipelineStage, PipelineStageResult] = field(default_factory=dict)
    system_state: dict[str, Any] = field(default_factory=dict)
    recommendations: list[str] = field(default_factory=list)

    def mark_completed(self, status: PipelineStatus):
        """Mark pipeline as completed."""
        self.end_time = datetime.now(timezone.utc)
        self.total_execution_time = (self.end_time - self.start_time).total_seconds()
        self.overall_status = status

    def get_stage_result(self, stage: PipelineStage) -> PipelineStageResult | None:
        """Get result for a specific stage."""
        return self.stages.get(stage)

    def is_stage_successful(self, stage: PipelineStage) -> bool:
        """Check if a stage completed successfully."""
        result = self.get_stage_result(stage)
        return result is not None and result.status == PipelineStatus.SUCCESS


@dataclass
class SystemStateAnalysis:
    """Analysis of current system state."""

    data_age_hours: float | None = None
    backtesting_age_hours: float | None = None
    analysis_age_hours: float | None = None

    needs_data_collection: bool = False
    needs_backtesting: bool = False
    needs_analysis: bool = False

    data_quality_score: float = 0.0
    system_health: SystemHealth = SystemHealth.UNKNOWN

    total_records: int = 0
    unique_games: int = 0
    active_strategies: int = 0

    data_quality_issues: list[str] = field(default_factory=list)
    recommendations: list[str] = field(default_factory=list)

    last_analysis_time: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )


@dataclass
class OrchestrationConfig:
    """Configuration for pipeline orchestration."""

    # Data freshness thresholds (hours)
    data_freshness_threshold: float = 6.0
    backtesting_freshness_threshold: float = 24.0
    analysis_freshness_threshold: float = 2.0

    # Quality thresholds
    min_data_quality_score: float = 0.7
    min_records_threshold: int = 10
    min_games_threshold: int = 5

    # Pipeline settings
    enable_parallel_execution: bool = True
    max_concurrent_stages: int = 3
    stage_timeout_seconds: int = 300

    # Auto-execution settings
    enable_auto_data_collection: bool = True
    enable_auto_backtesting: bool = True
    enable_auto_analysis: bool = True

    # Retry settings
    max_retries: int = 2
    retry_delay_seconds: int = 30


@dataclass
class OrchestrationMetrics:
    """Metrics for pipeline orchestration."""

    total_pipelines: int = 0
    successful_pipelines: int = 0
    failed_pipelines: int = 0
    partial_success_pipelines: int = 0

    total_stages: int = 0
    successful_stages: int = 0
    failed_stages: int = 0

    total_execution_time: float = 0.0
    average_execution_time: float = 0.0

    data_collections: int = 0
    backtesting_runs: int = 0
    analysis_runs: int = 0

    system_health_checks: int = 0
    auto_executions: int = 0

    def increment(self, metric: str, value: int = 1):
        """Increment a metric counter."""
        if hasattr(self, metric):
            current_value = getattr(self, metric)
            setattr(self, metric, current_value + value)

    def update(self, metric: str, value: Any):
        """Update a metric value."""
        if hasattr(self, metric):
            setattr(self, metric, value)

    def calculate_success_rate(self) -> float:
        """Calculate pipeline success rate."""
        if self.total_pipelines == 0:
            return 0.0
        return (self.successful_pipelines / self.total_pipelines) * 100.0

    def to_dict(self) -> dict[str, Any]:
        """Convert metrics to dictionary."""
        metrics_dict = {
            field.name: getattr(self, field.name)
            for field in self.__dataclass_fields__.values()
        }
        metrics_dict["success_rate_percentage"] = self.calculate_success_rate()
        return metrics_dict


class PipelineOrchestrationService:
    """
    Pipeline Orchestration Service

    Provides intelligent orchestration of data collection, analysis, backtesting,
    and detection pipelines with comprehensive state management and optimization.

    Features:
    - Intelligent system state analysis and decision making
    - Multi-stage pipeline coordination with dependency management
    - Parallel and sequential execution modes
    - Comprehensive error handling and recovery
    - Real-time monitoring and metrics
    - Auto-execution based on system state
    - Integration with unified architecture services
    """

    def __init__(self, config: OrchestrationConfig | None = None):
        """Initialize the pipeline orchestration service."""
        self.config = config or OrchestrationConfig()
        self.settings = get_settings()
        self.logger = logger.bind(service="PipelineOrchestrationService")

        # State management
        self.metrics = OrchestrationMetrics()
        self.active_pipelines: dict[str, PipelineExecutionResult] = {}
        self.completed_pipelines: list[PipelineExecutionResult] = []
        self.pipeline_history_limit = 50

        # Services
        self.data_service = EnhancedDataService()

        # Concurrency control
        self.execution_semaphore = asyncio.Semaphore(self.config.max_concurrent_stages)

        self.logger.info(
            "PipelineOrchestrationService initialized",
            parallel_execution=self.config.enable_parallel_execution,
            max_concurrent_stages=self.config.max_concurrent_stages,
            data_threshold=self.config.data_freshness_threshold,
        )

    async def analyze_system_state(self) -> SystemStateAnalysis:
        """
        Analyze current system state to determine what needs to be executed.

        Returns:
            Comprehensive system state analysis
        """
        self.logger.info("Starting system state analysis")

        analysis = SystemStateAnalysis()

        try:
            # Analyze data freshness and quality
            await self._analyze_data_state(analysis)

            # Analyze backtesting state
            await self._analyze_backtesting_state(analysis)

            # Analyze analysis/detection state
            await self._analyze_analysis_state(analysis)

            # Calculate overall system health
            analysis.system_health = self._calculate_system_health(analysis)

            # Generate recommendations
            analysis.recommendations = self._generate_recommendations(analysis)

            self.metrics.increment("system_health_checks")

            self.logger.info(
                "System state analysis completed",
                system_health=analysis.system_health.value,
                needs_data=analysis.needs_data_collection,
                needs_backtesting=analysis.needs_backtesting,
                data_quality=analysis.data_quality_score,
            )

            return analysis

        except Exception as e:
            self.logger.error("System state analysis failed", error=str(e))
            analysis.system_health = SystemHealth.CRITICAL
            analysis.recommendations.append(f"System analysis failed: {str(e)}")
            return analysis

    async def execute_smart_pipeline(
        self,
        pipeline_type: str = "full",
        force_execution: bool = False,
        detection_minutes: int = 60,
    ) -> PipelineExecutionResult:
        """
        Execute an intelligent pipeline based on system state analysis.

        Args:
            pipeline_type: Type of pipeline to execute ("full", "data_only", "analysis_only")
            force_execution: Force execution regardless of system state
            detection_minutes: Minutes ahead for detection window

        Returns:
            Pipeline execution result
        """
        pipeline_id = str(uuid.uuid4())

        result = PipelineExecutionResult(
            pipeline_id=pipeline_id,
            pipeline_type=pipeline_type,
            start_time=datetime.now(timezone.utc),
        )

        self.active_pipelines[pipeline_id] = result

        try:
            self.logger.info(
                "Starting smart pipeline execution",
                pipeline_id=pipeline_id,
                pipeline_type=pipeline_type,
                force_execution=force_execution,
            )

            # Analyze system state first
            if not force_execution:
                system_analysis = await self.analyze_system_state()
                result.system_state = {
                    "data_age_hours": system_analysis.data_age_hours,
                    "needs_data_collection": system_analysis.needs_data_collection,
                    "needs_backtesting": system_analysis.needs_backtesting,
                    "system_health": system_analysis.system_health.value,
                    "data_quality_score": system_analysis.data_quality_score,
                }
            else:
                # Force execution - assume all stages needed
                result.system_state = {
                    "force_execution": True,
                    "needs_data_collection": True,
                    "needs_backtesting": True,
                    "needs_analysis": True,
                }

            # Determine stages to execute
            stages_to_execute = self._determine_stages_to_execute(
                pipeline_type, result.system_state, force_execution
            )

            if not stages_to_execute:
                result.mark_completed(PipelineStatus.SUCCESS)
                result.recommendations.append("No stages needed - system is up to date")
                self.logger.info(
                    "Pipeline completed - no stages needed", pipeline_id=pipeline_id
                )
                return result

            # Execute stages
            if self.config.enable_parallel_execution and len(stages_to_execute) > 1:
                await self._execute_stages_parallel(result, stages_to_execute)
            else:
                await self._execute_stages_sequential(result, stages_to_execute)

            # Determine overall status
            successful_stages = sum(
                1
                for stage_result in result.stages.values()
                if stage_result.status == PipelineStatus.SUCCESS
            )
            total_stages = len(result.stages)

            if successful_stages == total_stages:
                result.mark_completed(PipelineStatus.SUCCESS)
                self.metrics.increment("successful_pipelines")
            elif successful_stages > 0:
                result.mark_completed(PipelineStatus.PARTIAL_SUCCESS)
                self.metrics.increment("partial_success_pipelines")
            else:
                result.mark_completed(PipelineStatus.FAILED)
                self.metrics.increment("failed_pipelines")

            self.metrics.increment("total_pipelines")
            self.metrics.total_execution_time += result.total_execution_time
            if self.metrics.total_pipelines > 0:
                self.metrics.average_execution_time = (
                    self.metrics.total_execution_time / self.metrics.total_pipelines
                )

            self.logger.info(
                "Smart pipeline execution completed",
                pipeline_id=pipeline_id,
                status=result.overall_status.value,
                execution_time=result.total_execution_time,
                stages_executed=len(result.stages),
            )

            return result

        except Exception as e:
            result.mark_completed(PipelineStatus.FAILED)
            self.metrics.increment("failed_pipelines")

            self.logger.error(
                "Smart pipeline execution failed", pipeline_id=pipeline_id, error=str(e)
            )

            raise OrchestrationError(f"Pipeline {pipeline_id} failed: {str(e)}") from e

        finally:
            # Move to completed pipelines
            if pipeline_id in self.active_pipelines:
                completed_pipeline = self.active_pipelines.pop(pipeline_id)
                self.completed_pipelines.append(completed_pipeline)

                # Maintain history limit
                if len(self.completed_pipelines) > self.pipeline_history_limit:
                    self.completed_pipelines = self.completed_pipelines[
                        -self.pipeline_history_limit :
                    ]

    async def get_pipeline_recommendations(self) -> dict[str, Any]:
        """
        Get intelligent recommendations for pipeline execution.

        Returns:
            Dictionary with recommendations and system insights
        """
        try:
            analysis = await self.analyze_system_state()

            recommendations = {
                "system_health": analysis.system_health.value,
                "immediate_actions": [],
                "scheduled_actions": [],
                "optimization_suggestions": [],
                "data_quality_insights": {
                    "score": analysis.data_quality_score,
                    "issues": analysis.data_quality_issues,
                    "total_records": analysis.total_records,
                    "unique_games": analysis.unique_games,
                },
            }

            # Immediate actions
            if analysis.needs_data_collection:
                recommendations["immediate_actions"].append(
                    {
                        "action": "data_collection",
                        "priority": "high",
                        "reason": f"Data is {analysis.data_age_hours:.1f} hours old",
                    }
                )

            if analysis.system_health == SystemHealth.CRITICAL:
                recommendations["immediate_actions"].append(
                    {
                        "action": "system_health_check",
                        "priority": "critical",
                        "reason": "System health is critical",
                    }
                )

            # Scheduled actions
            if analysis.needs_backtesting:
                recommendations["scheduled_actions"].append(
                    {
                        "action": "backtesting",
                        "schedule": "daily",
                        "reason": f"Backtesting is {analysis.backtesting_age_hours:.1f} hours old",
                    }
                )

            # Optimization suggestions
            if analysis.data_quality_score < self.config.min_data_quality_score:
                recommendations["optimization_suggestions"].append(
                    {
                        "suggestion": "improve_data_quality",
                        "current_score": analysis.data_quality_score,
                        "target_score": self.config.min_data_quality_score,
                        "actions": [
                            "validate_data_sources",
                            "enhance_collection_logic",
                        ],
                    }
                )

            return recommendations

        except Exception as e:
            self.logger.error(
                "Failed to generate pipeline recommendations", error=str(e)
            )
            return {
                "error": str(e),
                "system_health": "unknown",
                "immediate_actions": [
                    {"action": "investigate_error", "priority": "high"}
                ],
            }

    # Private helper methods

    async def _analyze_data_state(self, analysis: SystemStateAnalysis):
        """Analyze data freshness and quality."""
        try:
            async with get_connection() as conn:
                async with conn.cursor() as cursor:
                    # Check data freshness
                    await cursor.execute("""
                        SELECT 
                            MAX(created_at) as latest_update,
                            COUNT(*) as total_records,
                            COUNT(DISTINCT game_id) as unique_games
                        FROM curated.games_complete
                        WHERE created_at >= NOW() - INTERVAL '7 days'
                    """)

                    row = await cursor.fetchone()
                    if row:
                        analysis.total_records = row[1] or 0
                        analysis.unique_games = row[2] or 0

                        if row[0]:  # latest_update
                            age = datetime.now(timezone.utc) - row[0]
                            analysis.data_age_hours = age.total_seconds() / 3600
                            analysis.needs_data_collection = (
                                analysis.data_age_hours
                                > self.config.data_freshness_threshold
                            )
                        else:
                            analysis.needs_data_collection = True
                            analysis.data_quality_issues.append("No recent data found")

                    # Calculate data quality score
                    quality_factors = []

                    if analysis.total_records >= self.config.min_records_threshold:
                        quality_factors.append(0.4)

                    if analysis.unique_games >= self.config.min_games_threshold:
                        quality_factors.append(0.3)

                    if (
                        analysis.data_age_hours
                        and analysis.data_age_hours
                        < self.config.data_freshness_threshold
                    ):
                        quality_factors.append(0.3)

                    analysis.data_quality_score = sum(quality_factors)

        except Exception as e:
            self.logger.error("Failed to analyze data state", error=str(e))
            analysis.data_quality_issues.append(f"Data analysis failed: {str(e)}")

    async def _analyze_backtesting_state(self, analysis: SystemStateAnalysis):
        """Analyze backtesting freshness."""
        try:
            # This would check backtesting results freshness
            # For now, we'll use a placeholder
            analysis.backtesting_age_hours = 25.0  # Placeholder
            analysis.needs_backtesting = (
                analysis.backtesting_age_hours
                > self.config.backtesting_freshness_threshold
            )

        except Exception as e:
            self.logger.error("Failed to analyze backtesting state", error=str(e))

    async def _analyze_analysis_state(self, analysis: SystemStateAnalysis):
        """Analyze analysis/detection state."""
        try:
            # This would check analysis results freshness
            # For now, we'll use a placeholder
            analysis.analysis_age_hours = 3.0  # Placeholder
            analysis.needs_analysis = (
                analysis.analysis_age_hours > self.config.analysis_freshness_threshold
            )

        except Exception as e:
            self.logger.error("Failed to analyze analysis state", error=str(e))

    def _calculate_system_health(self, analysis: SystemStateAnalysis) -> SystemHealth:
        """Calculate overall system health."""
        if analysis.data_quality_score >= 0.8 and not analysis.data_quality_issues:
            return SystemHealth.HEALTHY
        elif analysis.data_quality_score >= 0.5:
            return SystemHealth.WARNING
        else:
            return SystemHealth.CRITICAL

    def _generate_recommendations(self, analysis: SystemStateAnalysis) -> list[str]:
        """Generate recommendations based on analysis."""
        recommendations = []

        if analysis.needs_data_collection:
            recommendations.append("Execute data collection pipeline")

        if analysis.needs_backtesting:
            recommendations.append("Execute backtesting pipeline")

        if analysis.needs_analysis:
            recommendations.append("Execute analysis pipeline")

        if analysis.data_quality_score < self.config.min_data_quality_score:
            recommendations.append("Investigate data quality issues")

        if analysis.system_health == SystemHealth.CRITICAL:
            recommendations.append("Immediate system health investigation required")

        return recommendations

    def _determine_stages_to_execute(
        self, pipeline_type: str, system_state: dict[str, Any], force_execution: bool
    ) -> list[PipelineStage]:
        """Determine which stages need to be executed."""
        stages = []

        if pipeline_type == "data_only":
            if force_execution or system_state.get("needs_data_collection", False):
                stages.append(PipelineStage.DATA_COLLECTION)
        elif pipeline_type == "analysis_only":
            if force_execution or system_state.get("needs_analysis", False):
                stages.append(PipelineStage.ANALYSIS)
        else:  # full pipeline
            if force_execution or system_state.get("needs_data_collection", False):
                stages.append(PipelineStage.DATA_COLLECTION)

            if force_execution or system_state.get("needs_backtesting", False):
                stages.append(PipelineStage.BACKTESTING)

            if force_execution or system_state.get("needs_analysis", False):
                stages.append(PipelineStage.ANALYSIS)

        return stages

    async def _execute_stages_sequential(
        self, result: PipelineExecutionResult, stages: list[PipelineStage]
    ):
        """Execute stages sequentially."""
        for stage in stages:
            stage_result = await self._execute_single_stage(stage, result)
            result.stages[stage] = stage_result

            # Stop on critical failures
            if stage_result.status == PipelineStatus.FAILED and stage in [
                PipelineStage.DATA_COLLECTION
            ]:
                self.logger.error(
                    "Critical stage failed, stopping pipeline",
                    stage=stage.value,
                    pipeline_id=result.pipeline_id,
                )
                break

    async def _execute_stages_parallel(
        self, result: PipelineExecutionResult, stages: list[PipelineStage]
    ):
        """Execute stages in parallel where possible."""
        # For now, implement as sequential since stages have dependencies
        # In a full implementation, this would handle parallel execution of independent stages
        await self._execute_stages_sequential(result, stages)

    async def _execute_single_stage(
        self, stage: PipelineStage, result: PipelineExecutionResult
    ) -> PipelineStageResult:
        """Execute a single pipeline stage."""
        stage_result = PipelineStageResult(
            stage=stage,
            status=PipelineStatus.RUNNING,
            start_time=datetime.now(timezone.utc),
        )

        try:
            async with self.execution_semaphore:
                if stage == PipelineStage.DATA_COLLECTION:
                    await self._execute_data_collection_stage(stage_result)
                elif stage == PipelineStage.BACKTESTING:
                    await self._execute_backtesting_stage(stage_result)
                elif stage == PipelineStage.ANALYSIS:
                    await self._execute_analysis_stage(stage_result)
                else:
                    raise PipelineError(f"Unknown stage: {stage}")

            stage_result.mark_completed(PipelineStatus.SUCCESS)
            self.metrics.increment("successful_stages")

        except Exception as e:
            stage_result.errors.append(str(e))
            stage_result.mark_completed(PipelineStatus.FAILED)
            self.metrics.increment("failed_stages")

            self.logger.error(
                "Stage execution failed",
                stage=stage.value,
                pipeline_id=result.pipeline_id,
                error=str(e),
            )

        self.metrics.increment("total_stages")
        return stage_result

    async def _execute_data_collection_stage(self, stage_result: PipelineStageResult):
        """Execute data collection stage."""
        self.logger.info("Executing data collection stage")

        try:
            # Use the enhanced data service
            collection_result = await self.data_service.collect_and_store_all("mlb")

            stage_result.records_processed = collection_result.get(
                "total_records_collected", 0
            )
            stage_result.success_count = collection_result.get(
                "total_records_stored", 0
            )
            stage_result.metadata["collection_result"] = collection_result

            self.metrics.increment("data_collections")

            if not collection_result.get("success", False):
                raise PipelineError("Data collection failed")

        except Exception as e:
            raise PipelineError(f"Data collection stage failed: {str(e)}") from e

    async def _execute_backtesting_stage(self, stage_result: PipelineStageResult):
        """Execute backtesting stage."""
        self.logger.info("Executing backtesting stage")

        try:
            # This would integrate with the backtesting service
            # For now, we'll use a placeholder
            await asyncio.sleep(1)  # Simulate backtesting

            stage_result.records_processed = 100  # Placeholder
            stage_result.success_count = 95  # Placeholder
            stage_result.metadata["backtesting_result"] = {"success": True}

            self.metrics.increment("backtesting_runs")

        except Exception as e:
            raise PipelineError(f"Backtesting stage failed: {str(e)}") from e

    async def _execute_analysis_stage(self, stage_result: PipelineStageResult):
        """Execute analysis stage."""
        self.logger.info("Executing analysis stage")

        try:
            # This would integrate with the analysis service
            # For now, we'll use a placeholder
            await asyncio.sleep(0.5)  # Simulate analysis

            stage_result.records_processed = 50  # Placeholder
            stage_result.success_count = 48  # Placeholder
            stage_result.metadata["analysis_result"] = {"success": True}

            self.metrics.increment("analysis_runs")

        except Exception as e:
            raise PipelineError(f"Analysis stage failed: {str(e)}") from e

    # Public query methods

    def get_active_pipelines(self) -> list[PipelineExecutionResult]:
        """Get all currently active pipelines."""
        return list(self.active_pipelines.values())

    def get_recent_pipelines(self, limit: int = 10) -> list[PipelineExecutionResult]:
        """Get recent completed pipelines."""
        return self.completed_pipelines[-limit:] if self.completed_pipelines else []

    def get_pipeline_status(self, pipeline_id: str) -> PipelineExecutionResult | None:
        """Get status of a specific pipeline."""
        # Check active pipelines first
        if pipeline_id in self.active_pipelines:
            return self.active_pipelines[pipeline_id]

        # Check completed pipelines
        for pipeline in self.completed_pipelines:
            if pipeline.pipeline_id == pipeline_id:
                return pipeline

        return None

    def get_metrics(self) -> dict[str, Any]:
        """Get comprehensive orchestration metrics."""
        return self.metrics.to_dict()

    async def cleanup(self):
        """Cleanup service resources."""
        self.logger.info("Cleaning up pipeline orchestration service")
        await self.data_service.cleanup()
        self.logger.info("Pipeline orchestration service cleanup completed")


# Service instance for easy importing
pipeline_orchestration_service = PipelineOrchestrationService()


# Convenience functions
async def analyze_system_state() -> SystemStateAnalysis:
    """Convenience function to analyze system state."""
    return await pipeline_orchestration_service.analyze_system_state()


async def execute_smart_pipeline(
    pipeline_type: str = "full", force_execution: bool = False
) -> PipelineExecutionResult:
    """Convenience function to execute smart pipeline."""
    return await pipeline_orchestration_service.execute_smart_pipeline(
        pipeline_type, force_execution
    )


async def get_pipeline_recommendations() -> dict[str, Any]:
    """Convenience function to get pipeline recommendations."""
    return await pipeline_orchestration_service.get_pipeline_recommendations()


if __name__ == "__main__":
    # Example usage
    async def main():
        try:
            # Test system analysis
            analysis = await analyze_system_state()
            print(f"System health: {analysis.system_health.value}")
            print(f"Needs data collection: {analysis.needs_data_collection}")

            # Test pipeline execution
            result = await execute_smart_pipeline("data_only")
            print(f"Pipeline completed: {result.overall_status.value}")

            # Test recommendations
            recommendations = await get_pipeline_recommendations()
            print(
                f"Immediate actions: {len(recommendations.get('immediate_actions', []))}"
            )

        except Exception as e:
            print(f"Error: {e}")

    asyncio.run(main())
