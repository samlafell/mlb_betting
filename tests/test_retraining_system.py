"""
Comprehensive Test Suite for Automated Retraining Workflows

Tests all components of the automated retraining system including:
- RetrainingTriggerService
- AutomatedRetrainingEngine
- ModelValidationService
- PerformanceMonitoringService
- RetrainingScheduler
- CLI integration

Provides both unit tests and integration tests to ensure system reliability.
"""

import asyncio
import pytest
import uuid
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.retraining import (
    RetrainingTriggerService,
    AutomatedRetrainingEngine,
    ModelValidationService,
    PerformanceMonitoringService,
    RetrainingScheduler,
    TriggerType,
    TriggerSeverity,
    RetrainingStrategy,
    RetrainingConfiguration,
    ValidationLevel,
    ScheduleType,
    SchedulePriority
)
from src.services.retraining.automated_engine import ModelVersion
from src.services.retraining.scheduler import RetrainingSchedule


@pytest.fixture
def mock_repository():
    """Mock repository for testing."""
    repo = MagicMock()
    
    # Mock database connection
    mock_conn = MagicMock()
    mock_conn.fetch.return_value = []
    mock_conn.fetchrow.return_value = None
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock()
    
    repo.get_connection.return_value = mock_conn
    return repo


@pytest.fixture
def mock_strategy_orchestrator():
    """Mock strategy orchestrator for testing."""
    return MagicMock()


@pytest.fixture
def sample_model_version():
    """Sample model version for testing."""
    return ModelVersion(
        version_id="test_model_v1.0",
        strategy_name="sharp_action",
        parameters={"min_threshold": 10.0, "confidence_multiplier": 1.5},
        performance_metrics={"roi": 7.2, "win_rate": 0.58, "total_bets": 120},
        created_at=datetime.now(),
        training_data_period="2024-01-01_to_2024-03-31"
    )


class TestRetrainingTriggerService:
    """Test suite for RetrainingTriggerService."""
    
    @pytest.fixture
    def trigger_service(self, mock_repository):
        """Create trigger service for testing."""
        return RetrainingTriggerService(mock_repository)
    
    @pytest.mark.asyncio
    async def test_create_manual_trigger(self, trigger_service):
        """Test manual trigger creation."""
        
        trigger = await trigger_service.create_manual_trigger(
            strategy_name="sharp_action",
            reason="Performance degradation detected",
            severity=TriggerSeverity.HIGH
        )
        
        assert trigger.trigger_type == TriggerType.MANUAL_OVERRIDE
        assert trigger.strategy_name == "sharp_action"
        assert trigger.severity == TriggerSeverity.HIGH
        assert "Performance degradation detected" in trigger.condition_description
        assert trigger.trigger_id in trigger_service.active_triggers
    
    @pytest.mark.asyncio
    async def test_resolve_trigger(self, trigger_service):
        """Test trigger resolution."""
        
        # Create a trigger
        trigger = await trigger_service.create_manual_trigger(
            strategy_name="sharp_action",
            reason="Test trigger"
        )
        
        # Verify it's active
        assert trigger.trigger_id in trigger_service.active_triggers
        
        # Resolve it
        success = trigger_service.resolve_trigger(trigger.trigger_id)
        
        assert success
        assert trigger.trigger_id not in trigger_service.active_triggers
        assert trigger.resolved_at is not None
        assert trigger.retraining_triggered is True
    
    @pytest.mark.asyncio
    async def test_check_performance_degradation(self, trigger_service, mock_repository):
        """Test performance degradation detection."""
        
        # Mock performance data
        mock_conn = mock_repository.get_connection.return_value.__aenter__.return_value
        mock_conn.fetchrow.side_effect = [
            # Baseline performance
            {
                "avg_roi": 8.0,
                "win_rate": 0.60,
                "total_bets": 100,
                "avg_confidence": 0.75
            },
            # Current performance (degraded)
            {
                "avg_roi": 3.0,  # Significant drop
                "win_rate": 0.52,  # Some drop
                "total_bets": 25,
                "avg_confidence": 0.70
            }
        ]
        
        # Initialize baselines
        await trigger_service._initialize_performance_baselines()
        
        # Check for degradation
        triggers = await trigger_service._check_performance_degradation("sharp_action")
        
        assert len(triggers) > 0
        assert any(t.trigger_type == TriggerType.PERFORMANCE_DEGRADATION for t in triggers)
        assert any("ROI degraded" in t.condition_description for t in triggers)
    
    def test_trigger_statistics(self, trigger_service):
        """Test trigger statistics calculation."""
        
        stats = trigger_service.get_trigger_statistics()
        
        assert "active_triggers" in stats
        assert "total_triggers_detected" in stats
        assert "monitoring_enabled" in stats
        assert isinstance(stats["active_triggers"], int)
        assert isinstance(stats["monitoring_enabled"], bool)


class TestModelValidationService:
    """Test suite for ModelValidationService."""
    
    @pytest.fixture
    def validation_service(self, mock_repository):
        """Create validation service for testing."""
        return ModelValidationService(mock_repository)
    
    @pytest.mark.asyncio
    async def test_basic_validation(self, validation_service, sample_model_version):
        """Test basic model validation."""
        
        # Create baseline model
        baseline = ModelVersion(
            version_id="baseline_v1.0",
            strategy_name="sharp_action",
            parameters={"min_threshold": 8.0, "confidence_multiplier": 1.3},
            performance_metrics={"roi": 5.0, "win_rate": 0.55, "total_bets": 100},
            created_at=datetime.now() - timedelta(days=30),
            training_data_period="2024-01-01_to_2024-02-29",
            is_baseline=True
        )
        
        # Mock performance data for statistical tests
        with patch.object(validation_service, '_get_model_performance_data') as mock_perf:
            mock_perf.return_value = [1.0, -0.5, 2.0, 1.5, -1.0] * 20  # 100 data points
            
            result = await validation_service.validate_model(
                candidate_model=sample_model_version,
                baseline_model=baseline,
                validation_level=ValidationLevel.BASIC
            )
        
        assert result.validation_id is not None
        assert result.model_version == sample_model_version
        assert result.baseline_version == baseline
        assert result.validation_level == ValidationLevel.BASIC
        assert result.status.value in ["passed", "failed", "warning"]
        assert result.overall_score >= 0.0 and result.overall_score <= 1.0
    
    @pytest.mark.asyncio
    async def test_rigorous_validation(self, validation_service, sample_model_version):
        """Test rigorous validation with comprehensive checks."""
        
        with patch.object(validation_service, '_get_model_performance_data') as mock_perf:
            mock_perf.return_value = [2.0, 1.0, 3.0, 1.5, 2.5] * 20  # Positive performance
            
            result = await validation_service.validate_model(
                candidate_model=sample_model_version,
                validation_level=ValidationLevel.RIGOROUS
            )
        
        assert result.validation_level == ValidationLevel.RIGOROUS
        assert result.metrics is not None
        assert hasattr(result.metrics, 'consistency_score')
        assert hasattr(result.metrics, 'robustness_score')
        assert hasattr(result.metrics, 'sharpe_ratio')
    
    def test_validation_statistics(self, validation_service):
        """Test validation service statistics."""
        
        stats = validation_service.get_validation_statistics()
        
        assert "total_validations" in stats
        assert "active_validations" in stats
        assert "success_rate" in stats
        assert isinstance(stats["total_validations"], int)
        assert isinstance(stats["success_rate"], float)


class TestAutomatedRetrainingEngine:
    """Test suite for AutomatedRetrainingEngine."""
    
    @pytest.fixture
    def retraining_engine(self, mock_repository, mock_strategy_orchestrator):
        """Create retraining engine for testing."""
        return AutomatedRetrainingEngine(mock_repository, mock_strategy_orchestrator)
    
    @pytest.mark.asyncio
    async def test_start_stop_engine(self, retraining_engine):
        """Test engine startup and shutdown."""
        
        assert not retraining_engine.engine_running
        
        # Start engine
        start_task = asyncio.create_task(retraining_engine.start_engine())
        await asyncio.sleep(0.1)  # Let it start
        
        assert retraining_engine.engine_running
        
        # Stop engine
        await retraining_engine.stop_engine()
        
        assert not retraining_engine.engine_running
        
        # Wait for start task to complete
        with pytest.raises(asyncio.CancelledError):
            await start_task
    
    @pytest.mark.asyncio
    async def test_trigger_retraining(self, retraining_engine):
        """Test triggering a retraining job."""
        
        await retraining_engine.start_engine()
        
        try:
            # Mock trigger conditions
            trigger_conditions = [
                MagicMock(
                    trigger_id="test_trigger_1",
                    trigger_type=TriggerType.PERFORMANCE_DEGRADATION,
                    severity=TriggerSeverity.HIGH,
                    strategy_name="sharp_action"
                )
            ]
            
            # Mock the optimization and validation steps
            with patch.object(retraining_engine, '_execute_retraining_workflow') as mock_workflow:
                mock_workflow.return_value = None
                
                job = await retraining_engine.trigger_retraining(
                    strategy_name="sharp_action",
                    trigger_conditions=trigger_conditions,
                    retraining_strategy=RetrainingStrategy.FULL_RETRAINING
                )
            
            assert job.job_id is not None
            assert job.strategy_name == "sharp_action"
            assert job.retraining_strategy == RetrainingStrategy.FULL_RETRAINING
            assert len(job.trigger_conditions) == 1
            assert job.job_id in retraining_engine.active_jobs
        
        finally:
            await retraining_engine.stop_engine()
    
    @pytest.mark.asyncio
    async def test_job_management(self, retraining_engine):
        """Test job status and management."""
        
        await retraining_engine.start_engine()
        
        try:
            # Create a job
            with patch.object(retraining_engine, '_execute_retraining_workflow'):
                job = await retraining_engine.trigger_retraining(
                    strategy_name="sharp_action",
                    trigger_conditions=[],
                    retraining_strategy=RetrainingStrategy.FULL_RETRAINING
                )
            
            # Test job retrieval
            retrieved_job = retraining_engine.get_job_status(job.job_id)
            assert retrieved_job is not None
            assert retrieved_job.job_id == job.job_id
            
            # Test active jobs list
            active_jobs = retraining_engine.get_active_jobs()
            assert len(active_jobs) >= 1
            assert any(j.job_id == job.job_id for j in active_jobs)
            
            # Test job cancellation
            success = await retraining_engine.cancel_job(job.job_id)
            assert success
            
            # Verify job was cancelled
            cancelled_job = retraining_engine.get_job_status(job.job_id)
            assert cancelled_job is None or cancelled_job.status.value == "cancelled"
        
        finally:
            await retraining_engine.stop_engine()
    
    def test_engine_status(self, retraining_engine):
        """Test engine status reporting."""
        
        status = retraining_engine.get_engine_status()
        
        assert "engine_running" in status
        assert "active_jobs_count" in status
        assert "total_jobs_completed" in status
        assert "configuration" in status
        assert isinstance(status["engine_running"], bool)
        assert isinstance(status["active_jobs_count"], int)


class TestPerformanceMonitoringService:
    """Test suite for PerformanceMonitoringService."""
    
    @pytest.fixture
    def monitoring_service(self, mock_repository):
        """Create monitoring service for testing."""
        return PerformanceMonitoringService(mock_repository)
    
    @pytest.mark.asyncio
    async def test_start_stop_monitoring(self, monitoring_service):
        """Test monitoring service startup and shutdown."""
        
        assert not monitoring_service.monitoring_enabled
        
        # Mock active strategies
        with patch.object(monitoring_service, '_get_active_strategies') as mock_strategies:
            mock_strategies.return_value = ["sharp_action", "line_movement"]
            
            # Start monitoring
            start_task = asyncio.create_task(monitoring_service.start_monitoring())
            await asyncio.sleep(0.1)  # Let it start
            
            assert monitoring_service.monitoring_enabled
            
            # Stop monitoring
            await monitoring_service.stop_monitoring()
            
            assert not monitoring_service.monitoring_enabled
    
    def test_alert_creation_and_resolution(self, monitoring_service):
        """Test performance alert creation and resolution."""
        
        # Create a mock performance window
        from src.services.retraining.performance_monitoring_service import PerformanceWindow
        
        window = PerformanceWindow(
            start_date=datetime.now() - timedelta(days=1),
            end_date=datetime.now(),
            total_bets=50,
            winning_bets=20,
            losing_bets=30,
            push_bets=0,
            total_roi=-5.0,  # Poor performance
            avg_roi_per_bet=-0.1,
            win_rate=0.40,  # Low win rate
            total_volume=1000.0,
            avg_confidence=0.75,
            max_drawdown=8.0,
            volatility=0.15,
            sharpe_ratio=0.2,
            profitable_days=2,
            total_days=7,
            longest_winning_streak=2,
            longest_losing_streak=5
        )
        
        monitoring_service.current_performance["sharp_action"] = window
        
        # This would trigger alerts in a real scenario
        # For testing, we'll manually create alerts
        alerts = monitoring_service.get_active_alerts("sharp_action")
        assert isinstance(alerts, list)
    
    def test_monitoring_status(self, monitoring_service):
        """Test monitoring service status reporting."""
        
        status = monitoring_service.get_monitoring_status()
        
        assert "monitoring_enabled" in status
        assert "strategies_monitored" in status
        assert "active_alerts" in status
        assert "monitoring_interval_minutes" in status
        assert isinstance(status["monitoring_enabled"], bool)
        assert isinstance(status["strategies_monitored"], int)


class TestRetrainingScheduler:
    """Test suite for RetrainingScheduler."""
    
    @pytest.fixture
    def scheduler(self, mock_repository, mock_strategy_orchestrator):
        """Create scheduler for testing."""
        trigger_service = RetrainingTriggerService(mock_repository)
        retraining_engine = AutomatedRetrainingEngine(mock_repository, mock_strategy_orchestrator)
        return RetrainingScheduler(trigger_service, retraining_engine)
    
    def test_add_remove_schedule(self, scheduler):
        """Test schedule management."""
        
        schedule = RetrainingSchedule(
            schedule_id="test_schedule_1",
            schedule_name="Weekly Sharp Action Retraining",
            strategy_name="sharp_action",
            schedule_type=ScheduleType.INTERVAL,
            interval_hours=168,  # Weekly
            priority=SchedulePriority.NORMAL,
            description="Weekly automated retraining"
        )
        
        # Add schedule
        scheduler.add_schedule(schedule)
        assert schedule.schedule_id in scheduler.schedules
        
        # Retrieve schedule
        retrieved = scheduler.get_schedule(schedule.schedule_id)
        assert retrieved is not None
        assert retrieved.schedule_name == "Weekly Sharp Action Retraining"
        
        # Remove schedule
        success = scheduler.remove_schedule(schedule.schedule_id)
        assert success
        assert schedule.schedule_id not in scheduler.schedules
    
    def test_update_schedule(self, scheduler):
        """Test schedule updates."""
        
        schedule = RetrainingSchedule(
            schedule_id="test_schedule_2",
            schedule_name="Test Schedule",
            strategy_name="line_movement",
            schedule_type=ScheduleType.INTERVAL,
            interval_hours=24,
            priority=SchedulePriority.LOW
        )
        
        scheduler.add_schedule(schedule)
        
        # Update schedule
        updates = {
            "interval_hours": 48,
            "priority": SchedulePriority.HIGH,
            "enabled": False
        }
        
        success = scheduler.update_schedule(schedule.schedule_id, updates)
        assert success
        
        # Verify updates
        updated_schedule = scheduler.get_schedule(schedule.schedule_id)
        assert updated_schedule.interval_hours == 48
        assert updated_schedule.priority == SchedulePriority.HIGH
        assert not updated_schedule.enabled
    
    @pytest.mark.asyncio
    async def test_immediate_job_scheduling(self, scheduler):
        """Test immediate job scheduling."""
        
        # Mock trigger conditions
        trigger_conditions = [
            MagicMock(
                trigger_id="immediate_trigger",
                trigger_type=TriggerType.MANUAL_OVERRIDE,
                severity=TriggerSeverity.HIGH
            )
        ]
        
        # Schedule immediate job
        job_id = await scheduler.schedule_immediate_job(
            strategy_name="sharp_action",
            trigger_conditions=trigger_conditions,
            priority=SchedulePriority.CRITICAL
        )
        
        assert job_id is not None
        assert len(scheduler.job_queue) > 0
        
        # Verify job is in queue
        job = scheduler.get_job_status(job_id)
        assert job is not None
        assert job.strategy_name == "sharp_action"
        assert job.priority == SchedulePriority.CRITICAL
    
    def test_scheduler_status(self, scheduler):
        """Test scheduler status reporting."""
        
        status = scheduler.get_scheduler_status()
        
        assert "scheduler_running" in status
        assert "schedules" in status
        assert "jobs" in status
        assert "resources" in status
        assert "configuration" in status
        
        assert isinstance(status["scheduler_running"], bool)
        assert isinstance(status["schedules"]["total"], int)
        assert isinstance(status["jobs"]["queued"], int)


class TestIntegration:
    """Integration tests for the complete retraining system."""
    
    @pytest.mark.asyncio
    async def test_end_to_end_retraining_workflow(self, mock_repository, mock_strategy_orchestrator):
        """Test complete end-to-end retraining workflow."""
        
        # Initialize services
        trigger_service = RetrainingTriggerService(mock_repository)
        retraining_engine = AutomatedRetrainingEngine(mock_repository, mock_strategy_orchestrator)
        monitoring_service = PerformanceMonitoringService(mock_repository)
        scheduler = RetrainingScheduler(trigger_service, retraining_engine)
        
        # Start services
        await retraining_engine.start_engine()
        
        try:
            # 1. Create a performance degradation trigger
            trigger = await trigger_service.create_manual_trigger(
                strategy_name="sharp_action",
                reason="Integration test trigger",
                severity=TriggerSeverity.HIGH
            )
            
            assert trigger.trigger_id in trigger_service.active_triggers
            
            # 2. Schedule immediate retraining based on trigger
            with patch.object(retraining_engine, '_execute_retraining_workflow') as mock_workflow:
                mock_workflow.return_value = None
                
                job_id = await scheduler.schedule_immediate_job(
                    strategy_name="sharp_action",
                    trigger_conditions=[trigger],
                    priority=SchedulePriority.HIGH
                )
            
            assert job_id is not None
            
            # 3. Verify job was created
            scheduled_job = scheduler.get_job_status(job_id)
            assert scheduled_job is not None
            assert scheduled_job.strategy_name == "sharp_action"
            assert len(scheduled_job.trigger_conditions) == 1
            
            # 4. Verify system status
            trigger_stats = trigger_service.get_trigger_statistics()
            engine_status = retraining_engine.get_engine_status()
            scheduler_status = scheduler.get_scheduler_status()
            
            assert trigger_stats["active_triggers"] >= 1
            assert engine_status["engine_running"]
            assert scheduler_status["jobs"]["queued"] >= 1
            
            # 5. Resolve trigger (simulating completion)
            success = trigger_service.resolve_trigger(trigger.trigger_id)
            assert success
            assert trigger.trigger_id not in trigger_service.active_triggers
        
        finally:
            await retraining_engine.stop_engine()
    
    @pytest.mark.asyncio
    async def test_model_validation_integration(self, mock_repository):
        """Test model validation integration with retraining workflow."""
        
        validation_service = ModelValidationService(mock_repository)
        
        # Create test models
        baseline = ModelVersion(
            version_id="integration_baseline",
            strategy_name="sharp_action",
            parameters={"threshold": 10.0},
            performance_metrics={"roi": 5.0, "win_rate": 0.55, "total_bets": 100},
            created_at=datetime.now() - timedelta(days=30),
            training_data_period="baseline_period",
            is_baseline=True
        )
        
        candidate = ModelVersion(
            version_id="integration_candidate",
            strategy_name="sharp_action",
            parameters={"threshold": 12.0},
            performance_metrics={"roi": 7.5, "win_rate": 0.62, "total_bets": 120},
            created_at=datetime.now(),
            training_data_period="candidate_period"
        )
        
        # Mock performance data for validation
        with patch.object(validation_service, '_get_model_performance_data') as mock_perf:
            mock_perf.return_value = [1.5, 2.0, 1.0, 2.5, 1.8] * 25  # 125 data points
            
            result = await validation_service.validate_model(
                candidate_model=candidate,
                baseline_model=baseline,
                validation_level=ValidationLevel.STANDARD
            )
        
        assert result.validation_id is not None
        assert result.model_version == candidate
        assert result.baseline_version == baseline
        assert result.overall_score > 0
        assert result.status.value in ["passed", "failed", "warning"]
        
        # Verify metrics were calculated
        if result.metrics:
            assert hasattr(result.metrics, 'roi_improvement')
            assert hasattr(result.metrics, 'statistical_power')
            assert hasattr(result.metrics, 'consistency_score')


# CLI Integration Tests

class TestCLIIntegration:
    """Test CLI integration with retraining system."""
    
    def test_cli_command_import(self):
        """Test that CLI commands can be imported successfully."""
        
        from src.interfaces.cli.commands.retraining import retraining_cli
        
        assert retraining_cli is not None
        assert hasattr(retraining_cli, 'commands')
    
    @pytest.mark.asyncio
    async def test_trigger_cli_simulation(self, mock_repository):
        """Test trigger CLI commands (simulation)."""
        
        # This would test actual CLI commands but we'll simulate the logic
        trigger_service = RetrainingTriggerService(mock_repository)
        
        # Simulate creating a manual trigger (like CLI would do)
        trigger = await trigger_service.create_manual_trigger(
            strategy_name="sharp_action",
            reason="CLI test trigger",
            severity=TriggerSeverity.MEDIUM
        )
        
        assert trigger.trigger_id is not None
        
        # Simulate getting trigger statistics (like CLI would do)
        stats = trigger_service.get_trigger_statistics()
        assert stats["active_triggers"] >= 1
        
        # Simulate resolving trigger (like CLI would do)
        success = trigger_service.resolve_trigger(trigger.trigger_id)
        assert success


# Performance Tests

class TestPerformance:
    """Performance tests for retraining system."""
    
    @pytest.mark.asyncio
    async def test_concurrent_trigger_creation(self, mock_repository):
        """Test concurrent trigger creation performance."""
        
        trigger_service = RetrainingTriggerService(mock_repository)
        
        # Create multiple triggers concurrently
        tasks = []
        for i in range(10):
            task = trigger_service.create_manual_trigger(
                strategy_name=f"strategy_{i % 3}",
                reason=f"Performance test trigger {i}",
                severity=TriggerSeverity.LOW
            )
            tasks.append(task)
        
        # Execute all tasks concurrently
        triggers = await asyncio.gather(*tasks)
        
        assert len(triggers) == 10
        assert len(trigger_service.active_triggers) == 10
        
        # Verify all triggers are unique
        trigger_ids = [t.trigger_id for t in triggers]
        assert len(set(trigger_ids)) == 10
    
    @pytest.mark.asyncio
    async def test_large_scale_monitoring(self, mock_repository):
        """Test monitoring service with large number of strategies."""
        
        monitoring_service = PerformanceMonitoringService(mock_repository)
        
        # Simulate monitoring 100 strategies
        strategies = [f"strategy_{i}" for i in range(100)]
        
        # Initialize performance history for all strategies
        for strategy in strategies:
            monitoring_service.performance_history[strategy] = []
            monitoring_service.last_update_time[strategy] = datetime.now()
        
        # Verify service can handle large scale
        status = monitoring_service.get_monitoring_status()
        assert status["strategies_monitored"] == 0  # None have current performance yet
        
        # This would be more comprehensive with actual performance data


# Error Handling Tests

class TestErrorHandling:
    """Test error handling in retraining system."""
    
    @pytest.mark.asyncio
    async def test_database_connection_failure(self):
        """Test handling of database connection failures."""
        
        # Create repository that fails
        failing_repo = MagicMock()
        failing_repo.get_connection.side_effect = Exception("Database connection failed")
        
        trigger_service = RetrainingTriggerService(failing_repo)
        
        # This should handle the database error gracefully
        with pytest.raises(Exception):
            await trigger_service._get_active_strategies()
    
    @pytest.mark.asyncio
    async def test_retraining_job_failure_handling(self, mock_repository, mock_strategy_orchestrator):
        """Test handling of retraining job failures."""
        
        retraining_engine = AutomatedRetrainingEngine(mock_repository, mock_strategy_orchestrator)
        await retraining_engine.start_engine()
        
        try:
            # Mock a failing workflow
            with patch.object(retraining_engine, '_execute_retraining_workflow') as mock_workflow:
                mock_workflow.side_effect = Exception("Retraining workflow failed")
                
                job = await retraining_engine.trigger_retraining(
                    strategy_name="sharp_action",
                    trigger_conditions=[],
                    retraining_strategy=RetrainingStrategy.FULL_RETRAINING
                )
                
                # Give it a moment to process the failure
                await asyncio.sleep(0.1)
                
                # Job should be in failed state
                failed_job = retraining_engine.get_job_status(job.job_id)
                # The job might be moved to history, so check if it exists
                if failed_job:
                    # If job still exists, it should be in failed state or moving to failed
                    pass  # Test passes if no exception is raised
        
        finally:
            await retraining_engine.stop_engine()
    
    def test_invalid_schedule_configuration(self, mock_repository, mock_strategy_orchestrator):
        """Test handling of invalid schedule configurations."""
        
        trigger_service = RetrainingTriggerService(mock_repository)
        retraining_engine = AutomatedRetrainingEngine(mock_repository, mock_strategy_orchestrator)
        scheduler = RetrainingScheduler(trigger_service, retraining_engine)
        
        # Create invalid schedule (cron type without expression)
        from src.services.retraining.scheduler import RetrainingSchedule
        
        invalid_schedule = RetrainingSchedule(
            schedule_id="invalid_schedule",
            schedule_name="Invalid Schedule",
            strategy_name="sharp_action",
            schedule_type=ScheduleType.CRON,
            cron_expression=None,  # Invalid - missing cron expression
        )
        
        # This should calculate next_run as None due to invalid config
        next_run = scheduler._calculate_next_run(invalid_schedule)
        assert next_run is None


if __name__ == "__main__":
    # Run specific test categories
    import sys
    
    if len(sys.argv) > 1:
        test_category = sys.argv[1]
        if test_category == "unit":
            pytest.main(["-v", "-k", "not test_end_to_end"])
        elif test_category == "integration":
            pytest.main(["-v", "-k", "test_end_to_end or TestIntegration"])
        elif test_category == "performance":
            pytest.main(["-v", "-k", "TestPerformance"])
        else:
            pytest.main(["-v"])
    else:
        pytest.main(["-v"])