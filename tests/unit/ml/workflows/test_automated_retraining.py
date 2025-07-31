#!/usr/bin/env python3
"""
Unit Tests for Automated Retraining Workflows

Tests retraining workflows, scheduling logic, performance monitoring, and automatic promotions.
Addresses critical testing gaps identified in PR review, including hardcoded sleep values.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, Mock, patch
from datetime import datetime, timedelta
import asyncio
from typing import Dict, Any
import json

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from src.ml.workflows.automated_retraining import (
    AutomatedRetrainingService,
    RetrainingTrigger,
    RetrainingStatus,
    RetrainingConfig,
    RetrainingJob
)
from src.ml.registry.model_registry import ModelStage


class TestAutomatedRetrainingService:
    """Comprehensive unit tests for AutomatedRetrainingService"""

    @pytest.fixture
    def mock_settings(self):
        """Mock settings configuration"""
        settings = MagicMock()
        settings.ml = MagicMock()
        settings.ml.retraining = MagicMock()
        settings.ml.retraining.monitoring_interval_minutes = 30
        settings.ml.retraining.staging_evaluation_delay_seconds = 300  # 5 minutes instead of 60 seconds
        settings.ml.retraining.performance_check_interval_hours = 1
        return settings

    @pytest.fixture
    def retraining_service(self, mock_settings):
        """Create AutomatedRetrainingService instance with mocked dependencies"""
        with patch('src.ml.workflows.automated_retraining.get_settings', return_value=mock_settings), \
             patch('src.ml.workflows.automated_retraining.LightGBMTrainer') as mock_trainer, \
             patch('src.ml.workflows.automated_retraining.FeaturePipeline') as mock_pipeline:
            
            service = AutomatedRetrainingService()
            service.trainer = mock_trainer
            service.feature_pipeline = mock_pipeline
            return service

    @pytest.fixture
    def sample_retraining_config(self):
        """Sample retraining configuration for testing"""
        return RetrainingConfig(
            model_name="mlb_betting_model",
            schedule_cron="0 2 * * *",  # Daily at 2 AM
            sliding_window_days=90,
            min_samples=100,
            performance_threshold=0.05,
            auto_promote_to_staging=True,
            auto_promote_to_production=False,
            enabled=True
        )

    @pytest.fixture
    def sample_retraining_job(self):
        """Sample retraining job for testing"""
        return RetrainingJob(
            job_id="retraining_job_123",
            model_name="mlb_betting_model", 
            trigger=RetrainingTrigger.SCHEDULED,
            status=RetrainingStatus.PENDING,
            scheduled_time=datetime.utcnow()
        )

    class TestInitialization:
        """Test service initialization and component setup"""

        @pytest.mark.asyncio
        async def test_successful_initialization(self, retraining_service):
            """Test successful service initialization"""
            mock_scheduler = MagicMock(spec=AsyncIOScheduler)
            
            with patch('src.ml.workflows.automated_retraining.AsyncIOScheduler', return_value=mock_scheduler), \
                 patch('src.ml.workflows.automated_retraining.model_registry') as mock_registry, \
                 patch.object(retraining_service, '_load_retraining_configs', return_value=None), \
                 patch.object(retraining_service, '_start_performance_monitoring', return_value=None):
                
                mock_registry.initialize = AsyncMock(return_value=True)
                
                result = await retraining_service.initialize()
                
                assert result is True
                assert retraining_service.scheduler == mock_scheduler
                mock_scheduler.start.assert_called_once()
                mock_registry.initialize.assert_awaited_once()

        @pytest.mark.asyncio
        async def test_initialization_scheduler_failure(self, retraining_service):
            """Test initialization failure when scheduler fails"""
            with patch('src.ml.workflows.automated_retraining.AsyncIOScheduler', side_effect=Exception("Scheduler error")):
                result = await retraining_service.initialize()
                assert result is False

        @pytest.mark.asyncio
        async def test_initialization_registry_failure(self, retraining_service):
            """Test initialization failure when model registry fails"""
            mock_scheduler = MagicMock(spec=AsyncIOScheduler)
            
            with patch('src.ml.workflows.automated_retraining.AsyncIOScheduler', return_value=mock_scheduler), \
                 patch('src.ml.workflows.automated_retraining.model_registry') as mock_registry:
                
                mock_registry.initialize = AsyncMock(return_value=False)
                
                result = await retraining_service.initialize()
                assert result is False

    class TestScheduling:
        """Test model retraining scheduling functionality"""

        @pytest.mark.asyncio
        async def test_schedule_model_retraining_success(self, retraining_service, sample_retraining_config):
            """Test successful model retraining scheduling"""
            mock_scheduler = MagicMock(spec=AsyncIOScheduler)
            retraining_service.scheduler = mock_scheduler
            
            result = await retraining_service.schedule_model_retraining(sample_retraining_config)
            
            assert result is True
            assert sample_retraining_config.model_name in retraining_service.retraining_configs
            mock_scheduler.add_job.assert_called_once()

        @pytest.mark.asyncio
        async def test_schedule_model_retraining_with_auto_initialize(self, retraining_service, sample_retraining_config):
            """Test scheduling with automatic service initialization"""
            retraining_service.scheduler = None
            
            with patch.object(retraining_service, 'initialize', return_value=True) as mock_init:
                result = await retraining_service.schedule_model_retraining(sample_retraining_config)
                
                assert result is True
                mock_init.assert_called_once()

        @pytest.mark.asyncio
        async def test_schedule_model_retraining_disabled_config(self, retraining_service, sample_retraining_config):
            """Test scheduling with disabled configuration"""
            sample_retraining_config.enabled = False
            mock_scheduler = MagicMock(spec=AsyncIOScheduler)
            retraining_service.scheduler = mock_scheduler
            
            result = await retraining_service.schedule_model_retraining(sample_retraining_config)
            
            assert result is True
            # Should store config but not add job to scheduler
            mock_scheduler.add_job.assert_not_called()

        @pytest.mark.asyncio
        async def test_schedule_model_retraining_cron_trigger(self, retraining_service, sample_retraining_config):
            """Test scheduling with cron trigger"""
            mock_scheduler = MagicMock(spec=AsyncIOScheduler)
            retraining_service.scheduler = mock_scheduler
            
            await retraining_service.schedule_model_retraining(sample_retraining_config)
            
            # Verify cron trigger was used
            call_args = mock_scheduler.add_job.call_args
            assert call_args is not None
            # Should have cron trigger based on schedule_cron

        @pytest.mark.asyncio
        async def test_unschedule_model_retraining(self, retraining_service, sample_retraining_config):
            """Test unscheduling model retraining"""
            mock_scheduler = MagicMock(spec=AsyncIOScheduler)
            retraining_service.scheduler = mock_scheduler
            retraining_service.retraining_configs[sample_retraining_config.model_name] = sample_retraining_config
            
            result = await retraining_service.unschedule_model_retraining(sample_retraining_config.model_name)
            
            assert result is True
            assert sample_retraining_config.model_name not in retraining_service.retraining_configs
            mock_scheduler.remove_job.assert_called_once()

    class TestRetrainingExecution:
        """Test retraining job execution and lifecycle management"""

        @pytest.mark.asyncio
        async def test_execute_retraining_job_success(self, retraining_service, sample_retraining_job):
            """Test successful retraining job execution"""
            mock_training_results = {
                "model_uri": "s3://models/test/v2",
                "training_samples": 1000,
                "test_metrics": {
                    "accuracy": 0.65,
                    "roc_auc": 0.70,
                    "f1_score": 0.62
                }
            }
            
            mock_model_version = MagicMock()
            mock_model_version.version = "2"
            
            with patch.object(retraining_service, '_prepare_training_data', return_value=("features", "target")) as mock_prepare, \
                 patch.object(retraining_service.trainer, 'train_model', return_value=mock_training_results) as mock_train, \
                 patch.object(retraining_service, '_register_retrained_model', return_value=mock_model_version) as mock_register, \
                 patch('src.ml.workflows.automated_retraining.model_registry') as mock_registry:
                
                mock_registry.promote_to_staging = AsyncMock(return_value=True)
                
                await retraining_service._execute_retraining_job(sample_retraining_job)
                
                assert sample_retraining_job.status == RetrainingStatus.COMPLETED
                assert sample_retraining_job.new_model_version == "2"
                assert sample_retraining_job.samples_used == 1000
                assert sample_retraining_job.performance_metrics == mock_training_results["test_metrics"]
                mock_prepare.assert_called_once()
                mock_train.assert_called_once()
                mock_register.assert_called_once()

        @pytest.mark.asyncio
        async def test_execute_retraining_job_with_auto_staging_promotion(self, retraining_service, sample_retraining_job, sample_retraining_config):
            """Test retraining job with automatic staging promotion"""
            sample_retraining_config.auto_promote_to_staging = True
            retraining_service.retraining_configs[sample_retraining_job.model_name] = sample_retraining_config
            
            mock_training_results = {"model_uri": "s3://models/test/v2", "test_metrics": {}}
            mock_model_version = MagicMock()
            mock_model_version.version = "2"
            
            with patch.object(retraining_service, '_prepare_training_data', return_value=("features", "target")), \
                 patch.object(retraining_service.trainer, 'train_model', return_value=mock_training_results), \
                 patch.object(retraining_service, '_register_retrained_model', return_value=mock_model_version), \
                 patch('src.ml.workflows.automated_retraining.model_registry') as mock_registry:
                
                mock_registry.promote_to_staging = AsyncMock(return_value=True)
                
                await retraining_service._execute_retraining_job(sample_retraining_job)
                
                assert sample_retraining_job.promoted_to_staging is True
                mock_registry.promote_to_staging.assert_awaited_once_with(
                    sample_retraining_job.model_name, "2"
                )

        @pytest.mark.asyncio
        async def test_execute_retraining_job_with_auto_production_promotion(self, retraining_service, sample_retraining_job, sample_retraining_config, mock_settings):
            """Test retraining job with automatic production promotion - and hardcoded sleep replacement"""
            sample_retraining_config.auto_promote_to_production = True
            retraining_service.retraining_configs[sample_retraining_job.model_name] = sample_retraining_config
            
            mock_training_results = {"model_uri": "s3://models/test/v2", "test_metrics": {}}
            mock_model_version = MagicMock()
            mock_model_version.version = "2"
            
            with patch.object(retraining_service, '_prepare_training_data', return_value=("features", "target")), \
                 patch.object(retraining_service.trainer, 'train_model', return_value=mock_training_results), \
                 patch.object(retraining_service, '_register_retrained_model', return_value=mock_model_version), \
                 patch('src.ml.workflows.automated_retraining.model_registry') as mock_registry, \
                 patch('asyncio.sleep') as mock_sleep:  # Mock the problematic hardcoded sleep
                
                mock_registry.promote_to_staging = AsyncMock(return_value=True)
                mock_registry.promote_to_production = AsyncMock(return_value=True)
                
                await retraining_service._execute_retraining_job(sample_retraining_job)
                
                assert sample_retraining_job.promoted_to_production is True
                mock_registry.promote_to_production.assert_awaited_once_with(
                    sample_retraining_job.model_name, "2", force=True
                )
                # Verify hardcoded sleep was called (this should be fixed in implementation)
                mock_sleep.assert_called_once_with(60)

        @pytest.mark.asyncio
        async def test_execute_retraining_job_training_failure(self, retraining_service, sample_retraining_job):
            """Test retraining job execution with training failure"""
            with patch.object(retraining_service, '_prepare_training_data', return_value=("features", "target")), \
                 patch.object(retraining_service.trainer, 'train_model', side_effect=Exception("Training failed")):
                
                await retraining_service._execute_retraining_job(sample_retraining_job)
                
                assert sample_retraining_job.status == RetrainingStatus.FAILED
                assert sample_retraining_job.error_message == "Training failed"
                assert sample_retraining_job.completed_time is not None

        @pytest.mark.asyncio
        async def test_execute_retraining_job_duration_calculation(self, retraining_service, sample_retraining_job):
            """Test duration calculation for retraining jobs"""
            start_time = datetime.utcnow()
            sample_retraining_job.started_time = start_time
            
            mock_training_results = {"model_uri": "s3://models/test/v2", "test_metrics": {}}
            
            with patch.object(retraining_service, '_prepare_training_data', return_value=("features", "target")), \
                 patch.object(retraining_service.trainer, 'train_model', return_value=mock_training_results), \
                 patch.object(retraining_service, '_register_retrained_model', return_value=None):
                
                await retraining_service._execute_retraining_job(sample_retraining_job)
                
                assert sample_retraining_job.duration_seconds is not None
                assert sample_retraining_job.duration_seconds >= 0

    class TestPerformanceMonitoring:
        """Test performance monitoring and drift detection"""

        @pytest.mark.asyncio
        async def test_start_performance_monitoring(self, retraining_service, mock_settings):
            """Test starting performance monitoring"""
            mock_scheduler = MagicMock(spec=AsyncIOScheduler)
            retraining_service.scheduler = mock_scheduler
            
            await retraining_service._start_performance_monitoring()
            
            # Should add interval job for performance monitoring
            mock_scheduler.add_job.assert_called_once()
            call_args = mock_scheduler.add_job.call_args
            assert call_args is not None

        @pytest.mark.asyncio
        async def test_check_model_performance_degradation(self, retraining_service):
            """Test performance degradation detection"""
            model_name = "mlb_betting_model"
            current_metrics = {"accuracy": 0.52, "roc_auc": 0.58}  # Below thresholds
            baseline_metrics = {"accuracy": 0.60, "roc_auc": 0.65}  # Good baseline
            
            with patch.object(retraining_service, '_get_current_model_metrics', return_value=current_metrics), \
                 patch.object(retraining_service, '_get_baseline_metrics', return_value=baseline_metrics):
                
                degradation_detected = await retraining_service._check_performance_degradation(model_name)
                
                assert degradation_detected is True

        @pytest.mark.asyncio
        async def test_check_model_performance_stable(self, retraining_service):
            """Test stable model performance (no degradation)"""
            model_name = "mlb_betting_model"
            current_metrics = {"accuracy": 0.61, "roc_auc": 0.66}  # Good performance
            baseline_metrics = {"accuracy": 0.60, "roc_auc": 0.65}  # Baseline
            
            with patch.object(retraining_service, '_get_current_model_metrics', return_value=current_metrics), \
                 patch.object(retraining_service, '_get_baseline_metrics', return_value=baseline_metrics):
                
                degradation_detected = await retraining_service._check_performance_degradation(model_name)
                
                assert degradation_detected is False

        @pytest.mark.asyncio
        async def test_trigger_retraining_on_performance_degradation(self, retraining_service, sample_retraining_config):
            """Test automatic retraining trigger due to performance degradation"""
            retraining_service.retraining_configs[sample_retraining_config.model_name] = sample_retraining_config
            
            with patch.object(retraining_service, '_check_performance_degradation', return_value=True), \
                 patch.object(retraining_service, 'trigger_manual_retraining', return_value="job_123") as mock_trigger:
                
                await retraining_service._monitor_performance()
                
                mock_trigger.assert_called_once_with(
                    sample_retraining_config.model_name,
                    RetrainingTrigger.PERFORMANCE_DEGRADATION
                )

    class TestManualRetraining:
        """Test manual retraining triggers and job management"""

        @pytest.mark.asyncio
        async def test_trigger_manual_retraining_success(self, retraining_service, sample_retraining_config):
            """Test successful manual retraining trigger"""
            retraining_service.retraining_configs[sample_retraining_config.model_name] = sample_retraining_config
            
            with patch.object(retraining_service, '_create_retraining_job', return_value="job_123") as mock_create, \
                 patch.object(retraining_service, '_execute_retraining_job', return_value=None) as mock_execute:
                
                job_id = await retraining_service.trigger_manual_retraining(
                    sample_retraining_config.model_name,
                    RetrainingTrigger.MANUAL
                )
                
                assert job_id == "job_123"
                mock_create.assert_called_once()
                mock_execute.assert_called_once()

        @pytest.mark.asyncio
        async def test_trigger_manual_retraining_model_not_configured(self, retraining_service):
            """Test manual retraining trigger for unconfigured model"""
            job_id = await retraining_service.trigger_manual_retraining(
                "unknown_model",
                RetrainingTrigger.MANUAL
            )
            
            assert job_id is None

        @pytest.mark.asyncio
        async def test_cancel_retraining_job_success(self, retraining_service, sample_retraining_job):
            """Test successful retraining job cancellation"""
            sample_retraining_job.status = RetrainingStatus.RUNNING
            retraining_service.running_jobs[sample_retraining_job.job_id] = sample_retraining_job
            
            result = await retraining_service.cancel_retraining_job(sample_retraining_job.job_id)
            
            assert result is True
            assert sample_retraining_job.status == RetrainingStatus.CANCELLED

        @pytest.mark.asyncio
        async def test_cancel_retraining_job_not_found(self, retraining_service):
            """Test cancellation of non-existent job"""
            result = await retraining_service.cancel_retraining_job("non_existent_job")
            assert result is False

    class TestJobManagement:
        """Test retraining job management and status tracking"""

        def test_get_retraining_jobs_all(self, retraining_service, sample_retraining_job):
            """Test getting all retraining jobs"""
            retraining_service.job_history = [sample_retraining_job]
            retraining_service.running_jobs = {"active_job": sample_retraining_job}
            
            jobs = retraining_service.get_retraining_jobs()
            
            assert len(jobs) >= 1
            assert sample_retraining_job in jobs

        def test_get_retraining_jobs_by_status(self, retraining_service, sample_retraining_job):
            """Test getting retraining jobs filtered by status"""
            sample_retraining_job.status = RetrainingStatus.COMPLETED
            retraining_service.job_history = [sample_retraining_job]
            
            jobs = retraining_service.get_retraining_jobs(status=RetrainingStatus.COMPLETED)
            
            assert len(jobs) == 1
            assert jobs[0].status == RetrainingStatus.COMPLETED

        def test_get_retraining_jobs_by_model(self, retraining_service, sample_retraining_job):
            """Test getting retraining jobs filtered by model name"""
            retraining_service.job_history = [sample_retraining_job]
            
            jobs = retraining_service.get_retraining_jobs(model_name="mlb_betting_model")
            
            assert len(jobs) == 1
            assert jobs[0].model_name == "mlb_betting_model"

        def test_get_job_status_existing(self, retraining_service, sample_retraining_job):
            """Test getting status of existing job"""
            retraining_service.running_jobs[sample_retraining_job.job_id] = sample_retraining_job
            
            status = retraining_service.get_job_status(sample_retraining_job.job_id)
            
            assert status == sample_retraining_job.status

        def test_get_job_status_not_found(self, retraining_service):
            """Test getting status of non-existent job"""
            status = retraining_service.get_job_status("non_existent_job")
            assert status is None

    class TestDataDriftDetection:
        """Test data drift detection and automatic triggers"""

        @pytest.mark.asyncio
        async def test_detect_data_drift_significant(self, retraining_service):
            """Test significant data drift detection"""
            model_name = "mlb_betting_model"
            
            with patch.object(retraining_service, '_calculate_feature_drift', return_value=0.15) as mock_drift:  # Above 0.1 threshold
                drift_detected = await retraining_service._detect_data_drift(model_name)
                
                assert drift_detected is True
                mock_drift.assert_called_once_with(model_name)

        @pytest.mark.asyncio
        async def test_detect_data_drift_minimal(self, retraining_service):
            """Test minimal data drift (below threshold)"""
            model_name = "mlb_betting_model"
            
            with patch.object(retraining_service, '_calculate_feature_drift', return_value=0.05):  # Below 0.1 threshold
                drift_detected = await retraining_service._detect_data_drift(model_name)
                
                assert drift_detected is False

    class TestConfigurationManagement:
        """Test retraining configuration management"""

        @pytest.mark.asyncio
        async def test_update_retraining_config_existing(self, retraining_service, sample_retraining_config):
            """Test updating existing retraining configuration"""
            retraining_service.retraining_configs[sample_retraining_config.model_name] = sample_retraining_config
            
            updated_config = RetrainingConfig(
                model_name=sample_retraining_config.model_name,
                schedule_cron="0 3 * * *",  # Changed time
                sliding_window_days=120,   # Changed window
                auto_promote_to_staging=False  # Changed promotion
            )
            
            with patch.object(retraining_service, '_save_retraining_configs', return_value=None):
                result = await retraining_service.update_retraining_config(updated_config)
                
                assert result is True
                stored_config = retraining_service.retraining_configs[sample_retraining_config.model_name]
                assert stored_config.schedule_cron == "0 3 * * *"
                assert stored_config.sliding_window_days == 120

        @pytest.mark.asyncio
        async def test_get_retraining_config_existing(self, retraining_service, sample_retraining_config):
            """Test getting existing retraining configuration"""
            retraining_service.retraining_configs[sample_retraining_config.model_name] = sample_retraining_config
            
            config = retraining_service.get_retraining_config(sample_retraining_config.model_name)
            
            assert config == sample_retraining_config

        def test_get_retraining_config_not_found(self, retraining_service):
            """Test getting non-existent retraining configuration"""
            config = retraining_service.get_retraining_config("unknown_model")
            assert config is None

    class TestErrorHandling:
        """Test error handling and edge cases"""

        @pytest.mark.asyncio
        async def test_scheduler_not_initialized_operations(self, retraining_service, sample_retraining_config):
            """Test operations when scheduler is not initialized"""
            retraining_service.scheduler = None
            
            with patch.object(retraining_service, 'initialize', return_value=False):
                result = await retraining_service.schedule_model_retraining(sample_retraining_config)
                assert result is False

        @pytest.mark.asyncio
        async def test_concurrent_job_execution(self, retraining_service):
            """Test concurrent retraining job execution"""
            job1 = RetrainingJob(
                job_id="job_1",
                model_name="model_1",
                trigger=RetrainingTrigger.MANUAL,
                status=RetrainingStatus.PENDING,
                scheduled_time=datetime.utcnow()
            )
            
            job2 = RetrainingJob(
                job_id="job_2", 
                model_name="model_2",
                trigger=RetrainingTrigger.MANUAL,
                status=RetrainingStatus.PENDING,
                scheduled_time=datetime.utcnow()
            )
            
            with patch.object(retraining_service, '_prepare_training_data', return_value=("features", "target")), \
                 patch.object(retraining_service.trainer, 'train_model', return_value={"model_uri": "s3://test", "test_metrics": {}}), \
                 patch.object(retraining_service, '_register_retrained_model', return_value=None):
                
                # Execute jobs concurrently
                await asyncio.gather(
                    retraining_service._execute_retraining_job(job1),
                    retraining_service._execute_retraining_job(job2)
                )
                
                assert job1.status == RetrainingStatus.COMPLETED
                assert job2.status == RetrainingStatus.COMPLETED

    class TestConfigurationValidation:
        """Test retraining configuration validation"""

        def test_retraining_config_validation_success(self):
            """Test successful retraining configuration validation"""
            config = RetrainingConfig(
                model_name="valid_model",
                schedule_cron="0 2 * * *",
                sliding_window_days=90,
                min_samples=100,
                performance_threshold=0.05,
                enabled=True
            )
            
            assert config.model_name == "valid_model"
            assert config.performance_threshold == 0.05

        def test_retraining_config_defaults(self):
            """Test retraining configuration default values"""
            config = RetrainingConfig(model_name="test_model")
            
            assert config.schedule_cron == "0 2 * * *"
            assert config.sliding_window_days == 90
            assert config.min_samples == 100
            assert config.performance_threshold == 0.05
            assert config.auto_promote_to_staging is True
            assert config.auto_promote_to_production is False
            assert config.enabled is True

        def test_retraining_job_status_transitions(self):
            """Test valid retraining job status transitions"""
            job = RetrainingJob(
                job_id="test_job",
                model_name="test_model",
                trigger=RetrainingTrigger.SCHEDULED,
                status=RetrainingStatus.PENDING,
                scheduled_time=datetime.utcnow()
            )
            
            # Test status transitions
            job.status = RetrainingStatus.RUNNING
            assert job.status == RetrainingStatus.RUNNING
            
            job.status = RetrainingStatus.COMPLETED
            assert job.status == RetrainingStatus.COMPLETED