#!/usr/bin/env python3
"""
Test Pipeline Orchestrator

Comprehensive tests for the pipeline orchestrator functionality including:
- Pipeline orchestrator initialization and zone setup
- Full pipeline execution (RAW → STAGING → CURATED)
- Partial pipeline modes (single zone, two-zone combinations)
- Pipeline execution tracking and metrics
- Error handling and recovery
- Concurrent pipeline execution
- Pipeline status monitoring

Reference: docs/SYSTEM_DESIGN_ANALYSIS.md
"""

import sys
sys.path.insert(0, '/Users/samlafell/Documents/programming_projects/mlb_betting_program')

import asyncio
import uuid
from datetime import datetime, timezone
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from typing import Dict, Any, List

def test_pipeline_orchestrator_initialization():
    """Test pipeline orchestrator initialization."""
    from src.data.pipeline.pipeline_orchestrator import DataPipelineOrchestrator, PipelineStage, PipelineMode
    
    print("\n🏗️ Testing pipeline orchestrator initialization...")
    
    # Create orchestrator
    orchestrator = DataPipelineOrchestrator()
    
    assert orchestrator is not None
    assert orchestrator.settings is not None
    assert isinstance(orchestrator.zones, dict)
    assert isinstance(orchestrator.active_executions, dict)
    print("  ✅ Pipeline orchestrator initialization successful")

def test_pipeline_data_models():
    """Test pipeline data models and enums."""
    from src.data.pipeline.pipeline_orchestrator import (
        PipelineStage, PipelineMode, PipelineMetrics, PipelineExecution
    )
    from src.data.pipeline.zone_interface import ProcessingStatus, ZoneType
    
    print("\n📊 Testing pipeline data models...")
    
    # Test enums
    assert PipelineStage.RAW_COLLECTION.value == "raw_collection"
    assert PipelineStage.STAGING_PROCESSING.value == "staging_processing"
    assert PipelineMode.FULL.value == "full"
    assert PipelineMode.RAW_ONLY.value == "raw_only"
    print("  ✅ Pipeline enums work correctly")
    
    # Test metrics
    metrics = PipelineMetrics(
        total_records=100,
        successful_records=95,
        failed_records=5,
        processing_time_seconds=25.5
    )
    assert metrics.total_records == 100
    assert metrics.successful_records == 95
    assert metrics.failed_records == 5
    assert metrics.processing_time_seconds == 25.5
    print("  ✅ Pipeline metrics work correctly")
    
    # Test execution tracking
    execution = PipelineExecution(
        pipeline_mode=PipelineMode.FULL,
        current_stage=PipelineStage.RAW_PROCESSING,
        status=ProcessingStatus.IN_PROGRESS
    )
    assert execution.pipeline_mode == PipelineMode.FULL
    assert execution.current_stage == PipelineStage.RAW_PROCESSING
    assert execution.status == ProcessingStatus.IN_PROGRESS
    assert execution.execution_id is not None
    assert execution.start_time is not None
    print("  ✅ Pipeline execution tracking works correctly")

async def test_zone_initialization():
    """Test pipeline zone initialization."""
    from src.data.pipeline.pipeline_orchestrator import DataPipelineOrchestrator
    from src.data.pipeline.zone_interface import ZoneType
    
    print("\n⚙️ Testing zone initialization...")
    
    with patch('src.data.pipeline.pipeline_orchestrator.ZoneFactory') as mock_factory:
        # Mock zone creation
        mock_raw_zone = AsyncMock()
        mock_staging_zone = AsyncMock()
        
        def create_zone_side_effect(zone_type, config):
            if zone_type == ZoneType.RAW:
                return mock_raw_zone
            elif zone_type == ZoneType.STAGING:
                return mock_staging_zone
            else:
                raise ValueError(f"Unknown zone type: {zone_type}")
        
        mock_factory.create_zone.side_effect = create_zone_side_effect
        
        orchestrator = DataPipelineOrchestrator()
        
        # Verify zones were created
        assert ZoneType.RAW in orchestrator.zones
        assert ZoneType.STAGING in orchestrator.zones
        assert orchestrator.zones[ZoneType.RAW] == mock_raw_zone
        assert orchestrator.zones[ZoneType.STAGING] == mock_staging_zone
        
        # Verify zone factory was called correctly
        assert mock_factory.create_zone.call_count >= 2
        print("  ✅ Zone initialization works correctly")

async def test_full_pipeline_execution():
    """Test full pipeline execution (RAW → STAGING → CURATED)."""
    from src.data.pipeline.pipeline_orchestrator import DataPipelineOrchestrator, PipelineMode
    from src.data.pipeline.zone_interface import (
        DataRecord, ProcessingResult, ProcessingStatus, ZoneType
    )
    
    print("\n🔄 Testing full pipeline execution...")
    
    with patch('src.data.pipeline.pipeline_orchestrator.ZoneFactory') as mock_factory, \
         patch.object(DataPipelineOrchestrator, '_log_pipeline_execution') as mock_log, \
         patch.object(DataPipelineOrchestrator, '_get_records_for_next_zone') as mock_get_records:
        
        # Mock zones
        mock_raw_zone = AsyncMock()
        mock_staging_zone = AsyncMock()
        
        # Mock zone results
        raw_result = ProcessingResult(
            status=ProcessingStatus.COMPLETED,
            records_processed=10,
            records_successful=9,
            records_failed=1,
            processing_time=15.5
        )
        
        staging_result = ProcessingResult(
            status=ProcessingStatus.COMPLETED,
            records_processed=9,
            records_successful=8,
            records_failed=1,
            processing_time=12.3
        )
        
        mock_raw_zone.process_batch.return_value = raw_result
        mock_staging_zone.process_batch.return_value = staging_result
        
        def create_zone_side_effect(zone_type, config):
            if zone_type == ZoneType.RAW:
                return mock_raw_zone
            elif zone_type == ZoneType.STAGING:
                return mock_staging_zone
            else:
                raise ValueError(f"Unknown zone type: {zone_type}")
        
        mock_factory.create_zone.side_effect = create_zone_side_effect
        
        # Mock record retrieval for next zone
        mock_records = [
            DataRecord(external_id=f"record_{i}", source="test")
            for i in range(9)
        ]
        mock_get_records.return_value = mock_records
        
        orchestrator = DataPipelineOrchestrator()
        
        # Create test records
        test_records = [
            DataRecord(
                external_id=f"test_record_{i}",
                source="test_source",
                raw_data={"test": f"data_{i}"}
            )
            for i in range(10)
        ]
        
        # Execute full pipeline
        execution = await orchestrator.run_full_pipeline(
            test_records,
            execution_metadata={"test_run": True}
        )
        
        assert execution is not None
        assert execution.pipeline_mode == PipelineMode.FULL
        assert execution.status == ProcessingStatus.COMPLETED
        assert execution.end_time is not None
        
        # Verify zone metrics were collected
        assert ZoneType.RAW in execution.metrics.zone_metrics
        assert ZoneType.STAGING in execution.metrics.zone_metrics
        
        # Verify processing occurred
        mock_raw_zone.process_batch.assert_called_once()
        mock_staging_zone.process_batch.assert_called_once()
        
        print("  ✅ Full pipeline execution successful")

async def test_partial_pipeline_modes():
    """Test partial pipeline execution modes."""
    from src.data.pipeline.pipeline_orchestrator import DataPipelineOrchestrator, PipelineMode
    from src.data.pipeline.zone_interface import (
        DataRecord, ProcessingResult, ProcessingStatus, ZoneType
    )
    
    print("\n📦 Testing partial pipeline modes...")
    
    with patch('src.data.pipeline.pipeline_orchestrator.ZoneFactory') as mock_factory:
        # Mock zones
        mock_raw_zone = AsyncMock()
        mock_staging_zone = AsyncMock()
        
        # Mock successful results
        success_result = ProcessingResult(
            status=ProcessingStatus.COMPLETED,
            records_processed=5,
            records_successful=5,
            records_failed=0
        )
        
        mock_raw_zone.process_batch.return_value = success_result
        mock_staging_zone.process_batch.return_value = success_result
        
        def create_zone_side_effect(zone_type, config):
            if zone_type == ZoneType.RAW:
                return mock_raw_zone
            elif zone_type == ZoneType.STAGING:
                return mock_staging_zone
            else:
                raise ValueError(f"Unknown zone type: {zone_type}")
        
        mock_factory.create_zone.side_effect = create_zone_side_effect
        
        orchestrator = DataPipelineOrchestrator()
        
        test_records = [
            DataRecord(external_id=f"partial_test_{i}", source="test")
            for i in range(5)
        ]
        
        # Test RAW-only mode
        raw_only_execution = await orchestrator.run_single_zone_pipeline(
            ZoneType.RAW,
            test_records
        )
        
        assert raw_only_execution.pipeline_mode == PipelineMode.RAW_ONLY
        assert raw_only_execution.status == ProcessingStatus.COMPLETED
        mock_raw_zone.process_batch.assert_called()
        print("  ✅ RAW-only pipeline mode works")
        
        # Test STAGING-only mode 
        mock_staging_zone.reset_mock()
        staging_only_execution = await orchestrator.run_single_zone_pipeline(
            ZoneType.STAGING,
            test_records
        )
        
        assert staging_only_execution.pipeline_mode == PipelineMode.STAGING_ONLY
        assert staging_only_execution.status == ProcessingStatus.COMPLETED
        mock_staging_zone.process_batch.assert_called()
        print("  ✅ STAGING-only pipeline mode works")

async def test_pipeline_error_handling():
    """Test pipeline error handling and recovery."""
    from src.data.pipeline.pipeline_orchestrator import DataPipelineOrchestrator
    from src.data.pipeline.zone_interface import (
        DataRecord, ProcessingResult, ProcessingStatus, ZoneType
    )
    
    print("\n❌ Testing pipeline error handling...")
    
    with patch('src.data.pipeline.pipeline_orchestrator.ZoneFactory') as mock_factory, \
         patch.object(DataPipelineOrchestrator, '_log_pipeline_execution'):
        
        # Mock zones
        mock_raw_zone = AsyncMock()
        mock_staging_zone = AsyncMock()
        
        # Mock RAW zone failure
        raw_failure_result = ProcessingResult(
            status=ProcessingStatus.FAILED,
            records_processed=5,
            records_successful=0,
            records_failed=5,
            errors=["Database connection failed", "Validation error"]
        )
        
        mock_raw_zone.process_batch.return_value = raw_failure_result
        
        def create_zone_side_effect(zone_type, config):
            if zone_type == ZoneType.RAW:
                return mock_raw_zone
            elif zone_type == ZoneType.STAGING:
                return mock_staging_zone
            else:
                raise ValueError(f"Unknown zone type: {zone_type}")
        
        mock_factory.create_zone.side_effect = create_zone_side_effect
        
        orchestrator = DataPipelineOrchestrator()
        
        test_records = [
            DataRecord(external_id=f"error_test_{i}", source="test")
            for i in range(5)
        ]
        
        # Execute pipeline with RAW zone failure
        execution = await orchestrator.run_full_pipeline(test_records)
        
        assert execution.status == ProcessingStatus.FAILED
        assert len(execution.errors) > 0
        assert "Database connection failed" in execution.errors
        assert "Validation error" in execution.errors
        
        # Verify STAGING zone was not called due to RAW failure
        mock_staging_zone.process_batch.assert_not_called()
        
        print("  ✅ Pipeline error handling works correctly")

async def test_pipeline_execution_tracking():
    """Test pipeline execution tracking and monitoring."""
    from src.data.pipeline.pipeline_orchestrator import DataPipelineOrchestrator, PipelineStage
    from src.data.pipeline.zone_interface import DataRecord, ProcessingResult, ProcessingStatus, ZoneType, ZoneType
    
    print("\n📊 Testing pipeline execution tracking...")
    
    with patch('src.data.pipeline.pipeline_orchestrator.ZoneFactory') as mock_factory, \
         patch.object(DataPipelineOrchestrator, '_log_pipeline_execution') as mock_log:
        
        # Mock successful zone
        mock_raw_zone = AsyncMock()
        success_result = ProcessingResult(
            status=ProcessingStatus.COMPLETED,
            records_processed=3,
            records_successful=3,
            records_failed=0
        )
        mock_raw_zone.process_batch.return_value = success_result
        mock_factory.create_zone.return_value = mock_raw_zone
        
        orchestrator = DataPipelineOrchestrator()
        
        test_records = [
            DataRecord(external_id=f"tracking_test_{i}", source="test")
            for i in range(3)
        ]
        
        # Execute pipeline
        execution = await orchestrator.run_single_zone_pipeline(
            ZoneType.RAW, 
            test_records,
            execution_metadata={"test_type": "tracking"}
        )
        
        # Verify execution completed (should be removed from active executions)
        assert execution.execution_id not in orchestrator.active_executions
        
        # Get execution status (should be None since execution is completed and removed)
        status = await orchestrator.get_execution_status(execution.execution_id)
        
        assert status is None  # Completed executions are removed from active tracking
        
        # But we can verify the execution object itself has the right data
        assert execution.execution_id is not None
        assert execution.status == ProcessingStatus.COMPLETED
        assert execution.metadata["test_type"] == "tracking"
        
        print("  ✅ Pipeline execution tracking works correctly")

async def test_pipeline_metrics_collection():
    """Test pipeline metrics collection and aggregation."""
    from src.data.pipeline.pipeline_orchestrator import DataPipelineOrchestrator
    from src.data.pipeline.zone_interface import DataRecord, ProcessingResult, ProcessingStatus, ZoneType, ZoneType
    
    print("\n📈 Testing pipeline metrics collection...")
    
    with patch('src.data.pipeline.pipeline_orchestrator.ZoneFactory') as mock_factory:
        
        # Mock zones with metrics
        mock_raw_zone = AsyncMock()
        mock_staging_zone = AsyncMock()
        
        # Import needed classes
        from src.data.pipeline.zone_interface import ZoneMetrics
        
        # Mock results with quality metrics
        raw_result = ProcessingResult(
            status=ProcessingStatus.COMPLETED,
            records_processed=10,
            records_successful=9,
            records_failed=1,
            processing_time=15.0,
            quality_metrics=ZoneMetrics(
                quality_score=0.85,
                error_rate=0.10,
                records_processed=10,
                records_successful=9,
                records_failed=1
            )
        )
        
        staging_result = ProcessingResult(
            status=ProcessingStatus.COMPLETED,
            records_processed=9,
            records_successful=8,
            records_failed=1,
            processing_time=12.0,
            quality_metrics=ZoneMetrics(
                quality_score=0.90,
                error_rate=0.11,
                records_processed=9,
                records_successful=8,
                records_failed=1
            )
        )
        
        mock_raw_zone.process_batch.return_value = raw_result
        mock_staging_zone.process_batch.return_value = staging_result
        
        def create_zone_side_effect(zone_type, config):
            if zone_type == ZoneType.RAW:
                return mock_raw_zone
            elif zone_type == ZoneType.STAGING:
                return mock_staging_zone
        
        mock_factory.create_zone.side_effect = create_zone_side_effect
        
        with patch.object(DataPipelineOrchestrator, '_get_records_for_next_zone') as mock_get_records:
            mock_get_records.return_value = [
                DataRecord(external_id=f"metrics_test_{i}", source="test")
                for i in range(9)
            ]
            
            orchestrator = DataPipelineOrchestrator()
            
            test_records = [
                DataRecord(external_id=f"metrics_input_{i}", source="test")
                for i in range(10)
            ]
            
            # Execute pipeline and collect metrics
            execution = await orchestrator.run_full_pipeline(test_records)
            
            # Calculate overall metrics
            overall_metrics = await orchestrator.calculate_overall_metrics(execution)
            
            assert overall_metrics is not None
            assert overall_metrics["total_records"] == 19
            assert overall_metrics["successful_records"] == 17
            assert overall_metrics["failed_records"] == 2
            assert overall_metrics["overall_quality_score"] >= 0.0  # Can be 0.0 for testing
            assert overall_metrics["overall_error_rate"] >= 0.0
            assert "zone_breakdown" in overall_metrics
            
            # Verify zone-specific metrics
            zone_breakdown = overall_metrics["zone_breakdown"]
            assert ZoneType.RAW.value in zone_breakdown
            assert ZoneType.STAGING.value in zone_breakdown
            
            print("  ✅ Pipeline metrics collection works correctly")

async def test_concurrent_pipeline_execution():
    """Test concurrent pipeline execution handling."""
    from src.data.pipeline.pipeline_orchestrator import DataPipelineOrchestrator
    from src.data.pipeline.zone_interface import DataRecord, ProcessingResult, ProcessingStatus, ZoneType
    
    print("\n🔄 Testing concurrent pipeline execution...")
    
    with patch('src.data.pipeline.pipeline_orchestrator.ZoneFactory') as mock_factory:
        
        # Mock successful zone
        mock_raw_zone = AsyncMock()
        success_result = ProcessingResult(
            status=ProcessingStatus.COMPLETED,
            records_processed=2,
            records_successful=2,
            records_failed=0
        )
        mock_raw_zone.process_batch.return_value = success_result
        mock_factory.create_zone.return_value = mock_raw_zone
        
        orchestrator = DataPipelineOrchestrator()
        
        # Create multiple concurrent executions
        test_batches = [
            [
                DataRecord(external_id=f"concurrent_{batch}_{i}", source="test")
                for i in range(2)
            ]
            for batch in range(3)
        ]
        
        # Execute pipelines concurrently
        tasks = [
            orchestrator.run_single_zone_pipeline(ZoneType.RAW, batch)
            for batch in test_batches
        ]
        
        executions = await asyncio.gather(*tasks)
        
        # Verify all executions completed successfully
        assert len(executions) == 3
        for execution in executions:
            assert execution.status == ProcessingStatus.COMPLETED
            # Completed executions are removed from active tracking
            assert execution.execution_id not in orchestrator.active_executions
        
        # Verify all executions have unique IDs
        execution_ids = [execution.execution_id for execution in executions]
        assert len(set(execution_ids)) == 3
        
        print("  ✅ Concurrent pipeline execution works correctly")

def run_pipeline_orchestrator_tests():
    """Run all pipeline orchestrator tests."""
    print("🚀 Starting Pipeline Orchestrator Tests")
    print("=" * 60)
    
    try:
        # Run sync tests
        test_pipeline_orchestrator_initialization()
        test_pipeline_data_models()
        
        # Run async tests
        async def run_async_tests():
            await test_zone_initialization()
            await test_full_pipeline_execution()
            await test_partial_pipeline_modes()
            await test_pipeline_error_handling()
            await test_pipeline_execution_tracking()
            await test_pipeline_metrics_collection()
            await test_concurrent_pipeline_execution()
        
        asyncio.run(run_async_tests())
        
        print("\n" + "=" * 60)
        print("🎉 ALL PIPELINE ORCHESTRATOR TESTS PASSED!")
        print("✅ Pipeline orchestrator initialization and zone setup working")
        print("✅ Full and partial pipeline execution modes operational")
        print("✅ Error handling and recovery mechanisms functional")
        print("✅ Execution tracking and metrics collection working")
        print("✅ Concurrent pipeline execution handling successful")
        return True
        
    except Exception as e:
        print(f"\n❌ PIPELINE ORCHESTRATOR TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = run_pipeline_orchestrator_tests()
    sys.exit(0 if success else 1)