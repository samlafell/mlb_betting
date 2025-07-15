#!/usr/bin/env python3
"""
Pre-Game Workflow Service

Migrated and enhanced pre-game workflow automation from the legacy module.
Implements automated three-stage workflows that trigger before MLB games
with comprehensive error handling, retry logic, and notification systems.

Legacy Source: src/mlb_sharp_betting/services/pre_game_workflow.py
Enhanced Features:
- Unified architecture integration
- Enhanced async workflow management
- Improved error handling and retry logic
- Better notification and alerting
- Comprehensive monitoring and metrics

Part of Phase 5D: Critical Business Logic Migration
"""

import asyncio
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from ...core.config import get_settings
from ...core.exceptions import ValidationError, WorkflowError
from ...core.logging import get_logger
from ...services.game.game_manager_service import GameManagerService

logger = get_logger(__name__)


class WorkflowStage(str, Enum):
    """Workflow stage identifiers."""

    DATA_COLLECTION = "data_collection"
    BETTING_ANALYSIS = "betting_analysis"
    NOTIFICATION = "notification"
    VALIDATION = "validation"


class StageStatus(str, Enum):
    """Stage execution status."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    RETRYING = "retrying"
    SKIPPED = "skipped"
    TIMEOUT = "timeout"


class WorkflowStatus(str, Enum):
    """Overall workflow status."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL_SUCCESS = "partial_success"
    CANCELLED = "cancelled"


class NotificationType(str, Enum):
    """Notification type enumeration."""

    SUCCESS = "success"
    FAILURE = "failure"
    WARNING = "warning"
    INFO = "info"


@dataclass
class StageResult:
    """Result of a workflow stage execution."""

    stage: WorkflowStage
    status: StageStatus
    start_time: datetime
    end_time: datetime | None = None
    execution_time_seconds: float = 0.0
    stdout: str = ""
    stderr: str = ""
    return_code: int | None = None
    error_message: str = ""
    retry_count: int = 0
    output_files: list[Path] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def mark_completed(self, status: StageStatus, error_message: str = ""):
        """Mark stage as completed with status."""
        self.end_time = datetime.now(timezone.utc)
        self.execution_time_seconds = (self.end_time - self.start_time).total_seconds()
        self.status = status
        if error_message:
            self.error_message = error_message


@dataclass
class WorkflowResult:
    """Complete workflow execution result."""

    workflow_id: str
    game_id: str
    game_description: str
    start_time: datetime
    end_time: datetime | None = None
    total_execution_time: float = 0.0
    stages: dict[WorkflowStage, StageResult] = field(default_factory=dict)
    overall_status: WorkflowStatus = WorkflowStatus.PENDING
    notifications_sent: int = 0
    context: dict[str, Any] = field(default_factory=dict)

    def mark_completed(self, status: WorkflowStatus):
        """Mark workflow as completed with status."""
        self.end_time = datetime.now(timezone.utc)
        self.total_execution_time = (self.end_time - self.start_time).total_seconds()
        self.overall_status = status

    def get_stage_result(self, stage: WorkflowStage) -> StageResult | None:
        """Get result for a specific stage."""
        return self.stages.get(stage)

    def is_stage_successful(self, stage: WorkflowStage) -> bool:
        """Check if a stage completed successfully."""
        result = self.get_stage_result(stage)
        return result is not None and result.status == StageStatus.SUCCESS


@dataclass
class WorkflowConfig:
    """Configuration for workflow execution."""

    # Retry settings
    max_retries: int = 3
    retry_delay_base: float = 2.0
    retry_exponential_backoff: bool = True

    # Timeout settings
    stage_timeout_seconds: int = 90
    workflow_timeout_seconds: int = 300

    # Execution settings
    max_concurrent_workflows: int = 1
    enable_notifications: bool = True
    enable_file_attachments: bool = True

    # Stage enablement
    enable_data_collection: bool = True
    enable_betting_analysis: bool = True
    enable_validation: bool = True

    # Paths and commands
    project_root: Path | None = None
    data_collection_command: list[str] = field(
        default_factory=lambda: [
            "uv",
            "run",
            "python",
            "-m",
            "src.interfaces.cli.main",
            "collect",
            "all",
        ]
    )
    analysis_command: list[str] = field(
        default_factory=lambda: [
            "uv",
            "run",
            "python",
            "-m",
            "src.interfaces.cli.main",
            "analyze",
            "opportunities",
        ]
    )


@dataclass
class WorkflowMetrics:
    """Metrics for workflow execution tracking."""

    total_workflows: int = 0
    successful_workflows: int = 0
    failed_workflows: int = 0
    partial_success_workflows: int = 0
    cancelled_workflows: int = 0

    total_stages: int = 0
    successful_stages: int = 0
    failed_stages: int = 0
    retried_stages: int = 0

    total_execution_time: float = 0.0
    average_execution_time: float = 0.0

    notifications_sent: int = 0
    notification_failures: int = 0

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
        """Calculate overall workflow success rate."""
        if self.total_workflows == 0:
            return 0.0
        return (self.successful_workflows / self.total_workflows) * 100.0

    def to_dict(self) -> dict[str, Any]:
        """Convert metrics to dictionary."""
        metrics_dict = {
            field.name: getattr(self, field.name)
            for field in self.__dataclass_fields__.values()
        }
        metrics_dict["success_rate_percentage"] = self.calculate_success_rate()
        return metrics_dict


class PreGameWorkflowService:
    """
    Pre-Game Workflow Service

    Provides automated workflow execution for pre-game data collection,
    analysis, and notification with comprehensive error handling and monitoring.

    Features:
    - Three-stage automated workflows (data collection, analysis, notification)
    - Comprehensive retry logic with exponential backoff
    - Concurrency control and semaphore-based execution
    - Real-time monitoring and metrics
    - Flexible notification systems
    - Timeout handling and error recovery
    - Integration with unified architecture
    """

    def __init__(self, config: WorkflowConfig | None = None):
        """Initialize the pre-game workflow service."""
        self.config = config or WorkflowConfig()
        self.settings = get_settings()
        self.logger = logger.bind(service="PreGameWorkflowService")

        # Set project root if not provided
        if not self.config.project_root:
            self.config.project_root = Path(__file__).parent.parent.parent.parent

        # State management
        self.metrics = WorkflowMetrics()
        self.active_workflows: dict[str, WorkflowResult] = {}
        self.completed_workflows: list[WorkflowResult] = []
        self.workflow_history_limit = 100

        # Concurrency control
        self.workflow_semaphore = asyncio.Semaphore(
            self.config.max_concurrent_workflows
        )

        # Services
        self.game_manager = GameManagerService()

        self.logger.info(
            "PreGameWorkflowService initialized",
            max_concurrent_workflows=self.config.max_concurrent_workflows,
            max_retries=self.config.max_retries,
            notifications_enabled=self.config.enable_notifications,
        )

    async def execute_pre_game_workflow(
        self,
        game_id: str,
        context: dict[str, Any] | None = None,
        minutes_before: int = 5,
    ) -> WorkflowResult:
        """
        Execute a complete pre-game workflow for a specific game.

        Args:
            game_id: Game identifier
            context: Additional context for workflow execution
            minutes_before: Minutes before game start (for logging/context)

        Returns:
            WorkflowResult with execution details
        """
        workflow_id = str(uuid.uuid4())

        # Get game information
        game_data = await self.game_manager.get_game_by_id(game_id)
        if not game_data:
            raise WorkflowError(f"Game not found: {game_id}")

        game_description = (
            f"{game_data.get('away_team', 'TBD')} @ {game_data.get('home_team', 'TBD')}"
        )

        # Initialize workflow result
        workflow_result = WorkflowResult(
            workflow_id=workflow_id,
            game_id=game_id,
            game_description=game_description,
            start_time=datetime.now(timezone.utc),
            context=context or {},
        )

        # Add to active workflows
        self.active_workflows[workflow_id] = workflow_result

        try:
            self.logger.info(
                "Starting pre-game workflow",
                workflow_id=workflow_id,
                game_id=game_id,
                game_description=game_description,
                minutes_before=minutes_before,
            )

            # Execute workflow with concurrency control
            result = await self._execute_workflow_with_semaphore(workflow_result)

            # Update metrics
            self.metrics.increment("total_workflows")
            if result.overall_status == WorkflowStatus.SUCCESS:
                self.metrics.increment("successful_workflows")
            elif result.overall_status == WorkflowStatus.FAILED:
                self.metrics.increment("failed_workflows")
            elif result.overall_status == WorkflowStatus.PARTIAL_SUCCESS:
                self.metrics.increment("partial_success_workflows")

            self.metrics.total_execution_time += result.total_execution_time
            if self.metrics.total_workflows > 0:
                self.metrics.average_execution_time = (
                    self.metrics.total_execution_time / self.metrics.total_workflows
                )

            self.logger.info(
                "Pre-game workflow completed",
                workflow_id=workflow_id,
                status=result.overall_status.value,
                execution_time=result.total_execution_time,
            )

            return result

        except Exception as e:
            workflow_result.mark_completed(WorkflowStatus.FAILED)
            self.metrics.increment("failed_workflows")

            self.logger.error(
                "Pre-game workflow failed",
                workflow_id=workflow_id,
                game_id=game_id,
                error=str(e),
            )

            raise WorkflowError(f"Workflow {workflow_id} failed: {str(e)}") from e

        finally:
            # Move to completed workflows
            if workflow_id in self.active_workflows:
                completed_workflow = self.active_workflows.pop(workflow_id)
                self.completed_workflows.append(completed_workflow)

                # Maintain history limit
                if len(self.completed_workflows) > self.workflow_history_limit:
                    self.completed_workflows = self.completed_workflows[
                        -self.workflow_history_limit :
                    ]

    async def _execute_workflow_with_semaphore(
        self, workflow_result: WorkflowResult
    ) -> WorkflowResult:
        """Execute workflow with semaphore-based concurrency control."""
        async with self.workflow_semaphore:
            return await self._execute_workflow_stages(workflow_result)

    async def _execute_workflow_stages(
        self, workflow_result: WorkflowResult
    ) -> WorkflowResult:
        """Execute all workflow stages in sequence."""
        workflow_result.overall_status = WorkflowStatus.RUNNING

        # Define stage execution order and handlers
        stages = [
            (WorkflowStage.DATA_COLLECTION, self._execute_data_collection_stage),
            (WorkflowStage.BETTING_ANALYSIS, self._execute_betting_analysis_stage),
            (WorkflowStage.VALIDATION, self._execute_validation_stage),
            (WorkflowStage.NOTIFICATION, self._execute_notification_stage),
        ]

        successful_stages = 0
        total_stages = len(stages)

        for stage, handler in stages:
            # Check if stage is enabled
            if not self._is_stage_enabled(stage):
                self.logger.debug("Skipping disabled stage", stage=stage.value)
                continue

            try:
                # Execute stage with retry logic
                stage_result = await self._execute_stage_with_retry(
                    stage, handler, workflow_result
                )

                workflow_result.stages[stage] = stage_result

                if stage_result.status == StageStatus.SUCCESS:
                    successful_stages += 1
                    self.metrics.increment("successful_stages")
                else:
                    self.metrics.increment("failed_stages")

                self.metrics.increment("total_stages")

                # For critical stages, fail the entire workflow if they fail
                if stage_result.status == StageStatus.FAILED and stage in [
                    WorkflowStage.DATA_COLLECTION
                ]:
                    self.logger.error(
                        "Critical stage failed, stopping workflow",
                        stage=stage.value,
                        workflow_id=workflow_result.workflow_id,
                    )
                    break

            except Exception as e:
                self.logger.error(
                    "Stage execution error",
                    stage=stage.value,
                    workflow_id=workflow_result.workflow_id,
                    error=str(e),
                )

                # Create failed stage result
                stage_result = StageResult(
                    stage=stage,
                    status=StageStatus.FAILED,
                    start_time=datetime.now(timezone.utc),
                    error_message=str(e),
                )
                stage_result.mark_completed(StageStatus.FAILED, str(e))
                workflow_result.stages[stage] = stage_result

                self.metrics.increment("failed_stages")
                self.metrics.increment("total_stages")

        # Determine overall workflow status
        if successful_stages == total_stages:
            workflow_result.mark_completed(WorkflowStatus.SUCCESS)
        elif successful_stages > 0:
            workflow_result.mark_completed(WorkflowStatus.PARTIAL_SUCCESS)
        else:
            workflow_result.mark_completed(WorkflowStatus.FAILED)

        return workflow_result

    async def _execute_stage_with_retry(
        self, stage: WorkflowStage, handler: Callable, workflow_result: WorkflowResult
    ) -> StageResult:
        """Execute a stage with retry logic."""
        stage_result = StageResult(
            stage=stage,
            status=StageStatus.PENDING,
            start_time=datetime.now(timezone.utc),
        )

        for attempt in range(self.config.max_retries + 1):
            try:
                stage_result.status = StageStatus.RUNNING
                stage_result.retry_count = attempt

                if attempt > 0:
                    stage_result.status = StageStatus.RETRYING
                    self.metrics.increment("retried_stages")

                    # Calculate retry delay
                    if self.config.retry_exponential_backoff:
                        delay = self.config.retry_delay_base * (2 ** (attempt - 1))
                    else:
                        delay = self.config.retry_delay_base

                    self.logger.info(
                        "Retrying stage",
                        stage=stage.value,
                        attempt=attempt,
                        delay=delay,
                    )

                    await asyncio.sleep(delay)

                # Execute stage handler with timeout
                await asyncio.wait_for(
                    handler(stage_result, workflow_result),
                    timeout=self.config.stage_timeout_seconds,
                )

                # If we get here, stage succeeded
                stage_result.mark_completed(StageStatus.SUCCESS)

                self.logger.info(
                    "Stage completed successfully",
                    stage=stage.value,
                    attempt=attempt,
                    execution_time=stage_result.execution_time_seconds,
                )

                return stage_result

            except asyncio.TimeoutError:
                error_msg = (
                    f"Stage timeout after {self.config.stage_timeout_seconds} seconds"
                )
                stage_result.error_message = error_msg

                if attempt == self.config.max_retries:
                    stage_result.mark_completed(StageStatus.TIMEOUT, error_msg)
                    self.logger.error(
                        "Stage timed out after all retries",
                        stage=stage.value,
                        attempts=attempt + 1,
                    )
                    return stage_result

            except Exception as e:
                error_msg = str(e)
                stage_result.error_message = error_msg

                if attempt == self.config.max_retries:
                    stage_result.mark_completed(StageStatus.FAILED, error_msg)
                    self.logger.error(
                        "Stage failed after all retries",
                        stage=stage.value,
                        attempts=attempt + 1,
                        error=error_msg,
                    )
                    return stage_result

                self.logger.warning(
                    "Stage attempt failed",
                    stage=stage.value,
                    attempt=attempt,
                    error=error_msg,
                )

        # Should not reach here, but handle gracefully
        stage_result.mark_completed(StageStatus.FAILED, "Unexpected retry loop exit")
        return stage_result

    # Stage handlers

    async def _execute_data_collection_stage(
        self, stage_result: StageResult, workflow_result: WorkflowResult
    ):
        """Execute data collection stage."""
        self.logger.info(
            "Executing data collection stage", workflow_id=workflow_result.workflow_id
        )

        try:
            # Run data collection command
            result = await self._run_subprocess(
                self.config.data_collection_command, self.config.project_root
            )

            stage_result.stdout = result["stdout"]
            stage_result.stderr = result["stderr"]
            stage_result.return_code = result["return_code"]

            if result["return_code"] != 0:
                raise WorkflowError(
                    f"Data collection failed with code {result['return_code']}"
                )

            # Store metadata
            stage_result.metadata["command"] = " ".join(
                self.config.data_collection_command
            )
            stage_result.metadata["output_lines"] = len(result["stdout"].split("\n"))

        except Exception as e:
            raise WorkflowError(f"Data collection stage failed: {str(e)}") from e

    async def _execute_betting_analysis_stage(
        self, stage_result: StageResult, workflow_result: WorkflowResult
    ):
        """Execute betting analysis stage."""
        self.logger.info(
            "Executing betting analysis stage", workflow_id=workflow_result.workflow_id
        )

        try:
            # Run analysis command
            result = await self._run_subprocess(
                self.config.analysis_command, self.config.project_root
            )

            stage_result.stdout = result["stdout"]
            stage_result.stderr = result["stderr"]
            stage_result.return_code = result["return_code"]

            if result["return_code"] != 0:
                raise WorkflowError(
                    f"Analysis failed with code {result['return_code']}"
                )

            # Store metadata
            stage_result.metadata["command"] = " ".join(self.config.analysis_command)
            stage_result.metadata["analysis_output"] = result["stdout"]

        except Exception as e:
            raise WorkflowError(f"Betting analysis stage failed: {str(e)}") from e

    async def _execute_validation_stage(
        self, stage_result: StageResult, workflow_result: WorkflowResult
    ):
        """Execute validation stage."""
        self.logger.info(
            "Executing validation stage", workflow_id=workflow_result.workflow_id
        )

        try:
            # Validate previous stages
            validation_errors = []

            # Check data collection results
            data_stage = workflow_result.get_stage_result(WorkflowStage.DATA_COLLECTION)
            if data_stage and data_stage.status != StageStatus.SUCCESS:
                validation_errors.append(
                    "Data collection stage did not complete successfully"
                )

            # Check analysis results
            analysis_stage = workflow_result.get_stage_result(
                WorkflowStage.BETTING_ANALYSIS
            )
            if analysis_stage and analysis_stage.status != StageStatus.SUCCESS:
                validation_errors.append(
                    "Betting analysis stage did not complete successfully"
                )

            if validation_errors:
                stage_result.metadata["validation_errors"] = validation_errors
                raise ValidationError(
                    f"Validation failed: {'; '.join(validation_errors)}"
                )

            stage_result.metadata["validation_passed"] = True

        except Exception as e:
            raise WorkflowError(f"Validation stage failed: {str(e)}") from e

    async def _execute_notification_stage(
        self, stage_result: StageResult, workflow_result: WorkflowResult
    ):
        """Execute notification stage."""
        if not self.config.enable_notifications:
            stage_result.metadata["notifications_disabled"] = True
            return

        self.logger.info(
            "Executing notification stage", workflow_id=workflow_result.workflow_id
        )

        try:
            # Determine notification type based on workflow status
            if workflow_result.is_stage_successful(
                WorkflowStage.DATA_COLLECTION
            ) and workflow_result.is_stage_successful(WorkflowStage.BETTING_ANALYSIS):
                notification_type = NotificationType.SUCCESS
            else:
                notification_type = NotificationType.FAILURE

            # Send notification (placeholder implementation)
            await self._send_notification(workflow_result, notification_type)

            workflow_result.notifications_sent += 1
            self.metrics.increment("notifications_sent")

            stage_result.metadata["notification_type"] = notification_type.value
            stage_result.metadata["notification_sent"] = True

        except Exception as e:
            self.metrics.increment("notification_failures")
            raise WorkflowError(f"Notification stage failed: {str(e)}") from e

    # Helper methods

    def _is_stage_enabled(self, stage: WorkflowStage) -> bool:
        """Check if a stage is enabled in configuration."""
        stage_config_map = {
            WorkflowStage.DATA_COLLECTION: self.config.enable_data_collection,
            WorkflowStage.BETTING_ANALYSIS: self.config.enable_betting_analysis,
            WorkflowStage.VALIDATION: self.config.enable_validation,
            WorkflowStage.NOTIFICATION: self.config.enable_notifications,
        }
        return stage_config_map.get(stage, True)

    async def _run_subprocess(self, cmd: list[str], cwd: Path) -> dict[str, Any]:
        """Run a subprocess command with proper error handling."""
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=cwd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            stdout, stderr = await process.communicate()

            return {
                "return_code": process.returncode,
                "stdout": stdout.decode("utf-8") if stdout else "",
                "stderr": stderr.decode("utf-8") if stderr else "",
            }

        except Exception as e:
            return {"return_code": -1, "stdout": "", "stderr": str(e)}

    async def _send_notification(
        self, workflow_result: WorkflowResult, notification_type: NotificationType
    ):
        """Send workflow notification."""
        # This is a placeholder implementation
        # In a real implementation, this would integrate with email/SMS/Slack services

        message = f"Workflow {workflow_result.workflow_id} for game {workflow_result.game_description} "
        if notification_type == NotificationType.SUCCESS:
            message += "completed successfully"
        else:
            message += "failed or completed with errors"

        self.logger.info(
            "Notification sent",
            workflow_id=workflow_result.workflow_id,
            type=notification_type.value,
            message=message,
        )

    # Public query methods

    def get_workflow_status(self, workflow_id: str) -> WorkflowResult | None:
        """Get status of a specific workflow."""
        # Check active workflows first
        if workflow_id in self.active_workflows:
            return self.active_workflows[workflow_id]

        # Check completed workflows
        for workflow in self.completed_workflows:
            if workflow.workflow_id == workflow_id:
                return workflow

        return None

    def get_active_workflows(self) -> list[WorkflowResult]:
        """Get all currently active workflows."""
        return list(self.active_workflows.values())

    def get_recent_workflows(self, limit: int = 10) -> list[WorkflowResult]:
        """Get recent completed workflows."""
        return self.completed_workflows[-limit:] if self.completed_workflows else []

    def get_metrics(self) -> dict[str, Any]:
        """Get comprehensive workflow metrics."""
        return self.metrics.to_dict()

    async def cancel_workflow(self, workflow_id: str) -> bool:
        """Cancel an active workflow."""
        if workflow_id in self.active_workflows:
            workflow = self.active_workflows[workflow_id]
            workflow.mark_completed(WorkflowStatus.CANCELLED)
            self.metrics.increment("cancelled_workflows")

            self.logger.info("Workflow cancelled", workflow_id=workflow_id)
            return True

        return False


# Service instance for easy importing
pre_game_workflow_service = PreGameWorkflowService()


# Convenience functions
async def execute_workflow_for_game(
    game_id: str, context: dict[str, Any] | None = None, minutes_before: int = 5
) -> WorkflowResult:
    """Convenience function to execute workflow for a game."""
    return await pre_game_workflow_service.execute_pre_game_workflow(
        game_id, context, minutes_before
    )


def get_workflow_metrics() -> dict[str, Any]:
    """Convenience function to get workflow metrics."""
    return pre_game_workflow_service.get_metrics()


if __name__ == "__main__":
    # Example usage
    async def main():
        try:
            # Test workflow execution
            result = await execute_workflow_for_game("test_game_123")
            print(f"Workflow completed: {result.overall_status.value}")
            print(f"Execution time: {result.total_execution_time:.2f}s")

            # Test metrics
            metrics = get_workflow_metrics()
            print(f"Success rate: {metrics['success_rate_percentage']:.1f}%")

        except Exception as e:
            print(f"Error: {e}")

    asyncio.run(main())
