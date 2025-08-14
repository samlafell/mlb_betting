"""
Data Pipeline Orchestrator

Coordinates the three-tier data pipeline: RAW → STAGING → CURATED
Manages data flow, monitoring, and error handling across zones.

Reference: docs/SYSTEM_DESIGN_ANALYSIS.md
"""

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from ...core.config import get_settings
from ...core.logging import LogComponent, get_logger
from ...data.database.connection import get_connection
from .zone_interface import (
    DataRecord,
    ProcessingResult,
    ProcessingStatus,
    ZoneFactory,
    ZoneType,
    create_zone_config,
)

# Import zone processors to trigger registration
from .unified_staging_processor import UnifiedStagingProcessor  # Ensure unified processor is registered
from .raw_zone_consolidated import RawZoneConsolidatedProcessor  # Ensure consolidated raw processor is registered

logger = get_logger(__name__, LogComponent.CORE)


class PipelineStage(str, Enum):
    """Pipeline execution stages."""

    RAW_COLLECTION = "raw_collection"
    RAW_PROCESSING = "raw_processing"
    STAGING_PROCESSING = "staging_processing"
    CURATED_PROCESSING = "curated_processing"
    VALIDATION = "validation"
    COMPLETION = "completion"


class PipelineMode(str, Enum):
    """Pipeline execution modes."""

    FULL = "full"  # Process through all zones
    RAW_ONLY = "raw_only"  # Process only RAW zone
    STAGING_ONLY = "staging_only"  # Process only STAGING zone
    CURATED_ONLY = "curated_only"  # Process only CURATED zone
    RAW_TO_STAGING = "raw_to_staging"  # Process RAW and STAGING only
    STAGING_TO_CURATED = "staging_to_curated"  # Process STAGING and CURATED only


@dataclass
class PipelineMetrics:
    """Pipeline execution metrics."""

    total_records: int = 0
    successful_records: int = 0
    failed_records: int = 0
    processing_time_seconds: float = 0.0
    zone_metrics: dict[ZoneType, dict[str, Any]] = field(default_factory=dict)
    quality_scores: dict[ZoneType, float] = field(default_factory=dict)
    error_rates: dict[ZoneType, float] = field(default_factory=dict)


class PipelineExecution(BaseModel):
    """Pipeline execution tracking."""

    execution_id: UUID = Field(default_factory=uuid.uuid4)
    pipeline_mode: PipelineMode = PipelineMode.FULL
    current_stage: PipelineStage = PipelineStage.RAW_COLLECTION
    status: ProcessingStatus = ProcessingStatus.PENDING
    start_time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    end_time: datetime | None = None
    metrics: PipelineMetrics = Field(default_factory=PipelineMetrics)
    errors: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class DataPipelineOrchestrator:
    """
    Multi-zone pipeline coordination orchestrator.

    Manages the complete data flow from RAW → STAGING → CURATED zones
    with comprehensive monitoring, error handling, and quality control.
    """

    def __init__(self):
        self.settings = get_settings()
        self.zones = {}
        self._initialize_zones()
        self.active_executions: dict[UUID, PipelineExecution] = {}

    def _initialize_zones(self):
        """Initialize all pipeline zones."""
        try:
            # Initialize RAW zone
            if self.settings.pipeline.zones.raw_enabled:
                raw_config = create_zone_config(
                    ZoneType.RAW,
                    self.settings.schemas.raw,
                    batch_size=1000,
                    validation_enabled=self.settings.pipeline.validation_enabled,
                    auto_promotion=self.settings.pipeline.auto_promotion,
                )
                self.zones[ZoneType.RAW] = ZoneFactory.create_zone(
                    ZoneType.RAW, raw_config
                )

            # Initialize STAGING zone
            if self.settings.pipeline.zones.staging_enabled:
                staging_config = create_zone_config(
                    ZoneType.STAGING,
                    self.settings.schemas.staging,
                    batch_size=500,
                    quality_threshold=self.settings.pipeline.quality_threshold,
                    validation_enabled=self.settings.pipeline.validation_enabled,
                    auto_promotion=self.settings.pipeline.auto_promotion,
                )
                self.zones[ZoneType.STAGING] = ZoneFactory.create_zone(
                    ZoneType.STAGING, staging_config
                )

            # Initialize CURATED zone
            if self.settings.pipeline.zones.curated_enabled:
                curated_config = create_zone_config(
                    ZoneType.CURATED,
                    self.settings.schemas.curated,
                    batch_size=200,
                    quality_threshold=self.settings.pipeline.quality_threshold,
                    validation_enabled=self.settings.pipeline.validation_enabled,
                )
                self.zones[ZoneType.CURATED] = ZoneFactory.create_zone(
                    ZoneType.CURATED, curated_config
                )

            logger.info(f"Initialized pipeline zones: {list(self.zones.keys())}")

        except Exception as e:
            logger.error(f"Error initializing pipeline zones: {e}")
            raise

    async def run_full_pipeline(
        self,
        records: list[DataRecord],
        execution_metadata: dict[str, Any] | None = None,
    ) -> PipelineExecution:
        """
        Run the complete pipeline: RAW → STAGING → CURATED.

        Args:
            records: Initial data records to process
            execution_metadata: Additional execution metadata

        Returns:
            PipelineExecution with complete results
        """
        execution = PipelineExecution(
            pipeline_mode=PipelineMode.FULL, metadata=execution_metadata or {}
        )

        try:
            self.active_executions[execution.execution_id] = execution

            logger.info(f"Starting full pipeline execution {execution.execution_id}")

            # Log pipeline start
            await self._log_pipeline_execution(execution, PipelineStage.RAW_COLLECTION)

            # Stage 1: RAW processing
            execution.current_stage = PipelineStage.RAW_PROCESSING
            raw_result = await self._process_zone(ZoneType.RAW, records)
            execution.metrics.zone_metrics[ZoneType.RAW] = raw_result.model_dump()

            if raw_result.status == ProcessingStatus.FAILED:
                execution.status = ProcessingStatus.FAILED
                execution.errors.extend(raw_result.errors)
                return await self._complete_execution(execution)

            # Stage 2: STAGING processing (if enabled and RAW successful)
            if ZoneType.STAGING in self.zones and raw_result.records_successful > 0:
                execution.current_stage = PipelineStage.STAGING_PROCESSING

                # Get processed records from RAW zone for STAGING
                raw_records = await self._get_records_for_next_zone(
                    ZoneType.RAW, raw_result.records_successful
                )

                staging_result = await self._process_zone(ZoneType.STAGING, raw_records)
                execution.metrics.zone_metrics[ZoneType.STAGING] = (
                    staging_result.model_dump()
                )

                # Stage 3: CURATED processing (if enabled and STAGING successful)
                if (
                    ZoneType.CURATED in self.zones
                    and staging_result.records_successful > 0
                ):
                    execution.current_stage = PipelineStage.CURATED_PROCESSING

                    # Get processed records from STAGING zone for CURATED
                    staging_records = await self._get_records_for_next_zone(
                        ZoneType.STAGING, staging_result.records_successful
                    )

                    curated_result = await self._process_zone(
                        ZoneType.CURATED, staging_records
                    )
                    execution.metrics.zone_metrics[ZoneType.CURATED] = (
                        curated_result.model_dump()
                    )

            # Update overall metrics (preserve existing zone metrics)
            await self._update_pipeline_metrics(execution)

            # Determine overall status
            execution.status = self._determine_execution_status(execution)

            logger.info(
                f"Completed full pipeline execution {execution.execution_id}: "
                f"Status: {execution.status}, "
                f"Success: {execution.metrics.successful_records}/{execution.metrics.total_records}"
            )

            return await self._complete_execution(execution)

        except Exception as e:
            logger.error(f"Pipeline execution {execution.execution_id} failed: {e}")
            execution.status = ProcessingStatus.FAILED
            execution.errors.append(str(e))
            return await self._complete_execution(execution)

    async def run_single_zone_pipeline(
        self,
        zone_type: ZoneType,
        records: list[DataRecord],
        execution_metadata: dict[str, Any] | None = None,
    ) -> PipelineExecution:
        """
        Run pipeline for a specific zone only.

        Args:
            zone_type: Zone to process
            records: Records to process
            execution_metadata: Additional execution metadata

        Returns:
            PipelineExecution with zone results
        """
        # Determine pipeline mode based on zone
        mode_mapping = {
            ZoneType.RAW: PipelineMode.RAW_ONLY,
            ZoneType.STAGING: PipelineMode.STAGING_ONLY,
            ZoneType.CURATED: PipelineMode.CURATED_ONLY,
        }

        execution = PipelineExecution(
            pipeline_mode=mode_mapping.get(zone_type, PipelineMode.FULL),
            current_stage=PipelineStage.RAW_PROCESSING,  # Will be updated
            metadata=execution_metadata or {},
        )

        try:
            self.active_executions[execution.execution_id] = execution

            logger.info(
                f"Starting {zone_type} pipeline execution {execution.execution_id}"
            )

            # Process the specific zone
            result = await self._process_zone(zone_type, records)
            execution.metrics.zone_metrics[zone_type] = result.model_dump()

            # Update metrics and status
            await self._update_pipeline_metrics(execution)
            execution.status = self._determine_execution_status(execution)

            logger.info(
                f"Completed {zone_type} pipeline execution {execution.execution_id}: "
                f"Status: {execution.status}"
            )

            return await self._complete_execution(execution)

        except Exception as e:
            logger.error(
                f"Zone pipeline execution {execution.execution_id} failed: {e}"
            )
            execution.status = ProcessingStatus.FAILED
            execution.errors.append(str(e))
            return await self._complete_execution(execution)

    async def _process_zone(
        self, zone_type: ZoneType, records: list[DataRecord]
    ) -> ProcessingResult:
        """Process records through a specific zone."""
        try:
            if zone_type not in self.zones:
                raise ValueError(f"Zone {zone_type} not initialized or not enabled")

            zone = self.zones[zone_type]
            logger.info(f"Processing {len(records)} records through {zone_type} zone")

            # Enhanced processing for STAGING zone with multi-bet type support
            if zone_type == ZoneType.STAGING and hasattr(zone, 'process_record_multi_bet_types'):
                logger.info("Using enhanced multi-bet type processing for STAGING zone")
                all_staging_records = []
                
                # Process each raw record into multiple staging records (one per bet type)
                for raw_record in records:
                    staging_records = await zone.process_record_multi_bet_types(raw_record)
                    if staging_records:
                        all_staging_records.extend(staging_records)
                        logger.debug(f"Generated {len(staging_records)} staging records from raw record {getattr(raw_record, 'external_id', None) or 'unknown'}")
                
                logger.info(f"Generated {len(all_staging_records)} total staging records from {len(records)} raw records")
                
                # Store all staging records to appropriate tables
                if all_staging_records:
                    await zone.store_records(all_staging_records)
                    logger.info(f"Successfully stored {len(all_staging_records)} staging records to database")
                
                # Create processing result
                result = ProcessingResult(
                    status=ProcessingStatus.COMPLETED,
                    records_processed=len(records),
                    records_successful=len(all_staging_records),
                    records_failed=len(records) - len([r for r in records if any(
                        getattr(sr, 'id', None) == getattr(r, 'id', None) 
                        for sr in all_staging_records
                    )]),
                    processing_time=0.0,  # Would be calculated in real implementation
                    metadata={
                        "multi_bet_processing": True,
                        "staging_records_generated": len(all_staging_records),
                        "bet_types_processed": ["moneyline", "spread", "total"]
                    }
                )
            else:
                # Standard processing for other zones
                result = await zone.process_batch(records)

            logger.info(
                f"{zone_type} zone processing completed: "
                f"{result.records_successful} successful, {result.records_failed} failed"
            )

            return result

        except Exception as e:
            logger.error(f"Error processing {zone_type} zone: {e}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                records_processed=len(records),
                errors=[str(e)],
            )

    async def _get_records_for_next_zone(
        self, current_zone: ZoneType, limit: int
    ) -> list[DataRecord]:
        """Get processed records from current zone for next zone processing."""
        try:
            # Query processed records from current zone for next zone processing
            # Uses wider time windows and fallback logic to ensure data flow

            if current_zone == ZoneType.RAW:
                # Query source-specific raw_data tables for recently processed records
                # Use Action Network as primary source for pipeline flow
                # Try 24-hour window first, then fallback to most recent records
                queries = [
                    """
                    SELECT * FROM raw_data.action_network_odds 
                    WHERE processed_at > NOW() - INTERVAL '24 hours'
                    ORDER BY processed_at DESC
                    LIMIT $1
                    """,
                    """
                    SELECT * FROM raw_data.action_network_odds 
                    WHERE processed_at IS NOT NULL
                    ORDER BY processed_at DESC
                    LIMIT $1
                    """
                ]
            elif current_zone == ZoneType.STAGING:
                # Query historical staging table for recently processed records
                # Try 24-hour window first, then fallback to most recent records
                queries = [
                    """
                    SELECT * FROM staging.action_network_odds_historical
                    WHERE data_processing_time > NOW() - INTERVAL '24 hours'
                    ORDER BY data_processing_time DESC
                    LIMIT $1
                    """,
                    """
                    SELECT * FROM staging.action_network_odds_historical
                    WHERE data_processing_time IS NOT NULL
                    ORDER BY data_processing_time DESC
                    LIMIT $1
                    """
                ]
            else:
                return []

            # Execute queries with fallback logic
            rows = []
            async with get_connection() as connection:
                for i, query in enumerate(queries):
                    try:
                        rows = await connection.fetch(query, limit)
                        if rows:
                            if i == 0:
                                logger.info(f"Found {len(rows)} records in 24-hour window from {current_zone} zone")
                            else:
                                logger.info(f"Using fallback query: found {len(rows)} most recent records from {current_zone} zone")
                            break
                    except Exception as e:
                        logger.warning(f"Query {i+1} failed for {current_zone} zone: {e}")
                        continue

            # Convert rows to DataRecord objects with enhanced field handling
            records = []
            
            # Determine source from the table being queried since tables don't have source column
            inferred_source = "unknown"
            if current_zone == ZoneType.RAW:
                inferred_source = "action_network"  # We know we're querying action_network_odds
            elif current_zone == ZoneType.STAGING:
                inferred_source = "action_network"  # We know we're querying action_network_odds_historical
            
            for row in rows:
                # Handle both raw_data and raw_odds fields for Action Network compatibility
                raw_data_field = row.get("raw_data") or row.get("raw_odds")
                
                # Parse JSON string to dictionary for multi-bet type processing
                if raw_data_field and isinstance(raw_data_field, str):
                    try:
                        import json
                        raw_data_field = json.loads(raw_data_field)
                    except (json.JSONDecodeError, TypeError) as e:
                        logger.warning(f"Failed to parse raw_data JSON for record {row.get('id')}: {e}")
                        raw_data_field = None
                
                # Fix field mapping for unified staging processor compatibility
                # Map database field names to DataRecord field names
                external_id = row.get("external_id") or row.get("external_game_id")  # Support both field names
                
                # Store sportsbook_key in raw_data for processor access
                if raw_data_field and isinstance(raw_data_field, dict):
                    sportsbook_key = row.get("sportsbook_key")
                    if sportsbook_key:
                        raw_data_field['_sportsbook_key'] = sportsbook_key
                
                # Use inferred source since database tables don't have source column
                source = row.get("source", inferred_source)
                if not source or source == "unknown":
                    source = inferred_source
                    logger.debug(f"Using inferred source '{source}' for record {external_id}")
                
                record = DataRecord(
                    id=row.get("id"),
                    external_id=external_id,
                    source=source,
                    raw_data=raw_data_field,
                    processed_at=row.get("processed_at"),
                    created_at=row.get("created_at"),
                )
                
                records.append(record)

            logger.info(f"Retrieved {len(records)} records from {current_zone} zone for next zone processing")
            return records

        except Exception as e:
            logger.error(f"Error getting records from {current_zone} zone: {e}")
            return []

    async def _update_pipeline_metrics(self, execution: PipelineExecution) -> None:
        """Update comprehensive pipeline metrics from zone results."""

        try:
            total_processed = 0
            total_successful = 0
            total_failed = 0
            total_time = 0.0

            # Aggregate metrics from all zones
            for zone_type, zone_result in execution.metrics.zone_metrics.items():
                if isinstance(zone_result, dict):
                    processed = zone_result.get("records_processed", 0)
                    successful = zone_result.get("records_successful", 0)
                    failed = zone_result.get("records_failed", 0)
                    processing_time = zone_result.get("processing_time", 0.0)

                    total_processed += processed
                    total_successful += successful
                    total_failed += failed
                    total_time += processing_time

                    # Calculate quality score for zone
                    if processed > 0:
                        quality_score = successful / processed
                        error_rate = failed / processed
                    else:
                        quality_score = 0.0
                        error_rate = 0.0

                    execution.metrics.quality_scores[zone_type] = quality_score
                    execution.metrics.error_rates[zone_type] = error_rate

            execution.metrics.total_records = total_processed
            execution.metrics.successful_records = total_successful
            execution.metrics.failed_records = total_failed
            execution.metrics.processing_time_seconds = total_time

        except Exception as e:
            logger.error(f"Error updating pipeline metrics: {e}")

    def _determine_execution_status(
        self, execution: PipelineExecution
    ) -> ProcessingStatus:
        """Determine overall execution status from zone results."""
        try:
            zone_statuses = []

            for zone_result in execution.metrics.zone_metrics.values():
                if isinstance(zone_result, dict):
                    status = zone_result.get("status")
                    if status:
                        # Handle both string values and ProcessingStatus enums
                        if isinstance(status, ProcessingStatus):
                            zone_statuses.append(status)
                        elif isinstance(status, str):
                            zone_statuses.append(ProcessingStatus(status))

            if not zone_statuses:
                return ProcessingStatus.FAILED

            # If any zone failed completely, execution failed
            if ProcessingStatus.FAILED in zone_statuses:
                return ProcessingStatus.FAILED

            # If all zones completed, execution completed
            if all(status == ProcessingStatus.COMPLETED for status in zone_statuses):
                return ProcessingStatus.COMPLETED

            # Otherwise, in progress or partial completion
            return ProcessingStatus.IN_PROGRESS

        except Exception as e:
            logger.error(f"Error determining execution status: {e}")
            return ProcessingStatus.FAILED

    async def _complete_execution(
        self, execution: PipelineExecution
    ) -> PipelineExecution:
        """Complete pipeline execution with cleanup."""
        try:
            execution.end_time = datetime.now(timezone.utc)
            execution.current_stage = PipelineStage.COMPLETION

            # Log pipeline completion
            await self._log_pipeline_execution(execution, PipelineStage.COMPLETION)

            # Remove from active executions
            if execution.execution_id in self.active_executions:
                del self.active_executions[execution.execution_id]

            return execution

        except Exception as e:
            logger.error(f"Error completing execution: {e}")
            return execution

    async def _log_pipeline_execution(
        self, execution: PipelineExecution, stage: PipelineStage
    ) -> None:
        """Log pipeline execution to database."""
        try:
            # Use the correct schema for Docker PostgreSQL instance
            query = """
            INSERT INTO public.pipeline_execution_log 
            (execution_id, pipeline_name, zone, status, records_processed, 
             records_successful, records_failed, processing_time_ms, metadata, 
             started_at, completed_at, pipeline_stage, start_time, end_time)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            """

            # Map pipeline mode to zone for the schema
            zone_mapping = {
                PipelineMode.RAW_ONLY: "raw",
                PipelineMode.STAGING_ONLY: "staging", 
                PipelineMode.CURATED_ONLY: "curated",
                PipelineMode.FULL: "staging",  # Use staging as primary for full pipeline
                PipelineMode.RAW_TO_STAGING: "staging",
                PipelineMode.STAGING_TO_CURATED: "curated"
            }
            
            # Map ProcessingStatus to database-compatible status values
            status_mapping = {
                ProcessingStatus.PENDING: "running",  # Map pending to running for constraint
                ProcessingStatus.IN_PROGRESS: "running",
                ProcessingStatus.COMPLETED: "completed",
                ProcessingStatus.FAILED: "failed",
                ProcessingStatus.SKIPPED: "skipped"
            }
            
            zone_value = zone_mapping.get(execution.pipeline_mode, "staging")
            pipeline_name = f"{execution.pipeline_mode.value}_pipeline"
            status_value = status_mapping.get(execution.status, "running")
            processing_time_ms = int(execution.metrics.processing_time_seconds * 1000) if execution.metrics.processing_time_seconds else None

            async with get_connection() as connection:
                await connection.execute(
                    query,
                    execution.execution_id,              # $1 execution_id
                    pipeline_name,                       # $2 pipeline_name
                    zone_value,                          # $3 zone  
                    status_value,                        # $4 status (mapped to constraint values)
                    execution.metrics.total_records,     # $5 records_processed
                    execution.metrics.successful_records, # $6 records_successful
                    execution.metrics.failed_records,    # $7 records_failed
                    processing_time_ms,                  # $8 processing_time_ms
                    json.dumps(execution.metadata) if execution.metadata else "{}",  # $9 metadata
                    execution.start_time,                # $10 started_at
                    execution.end_time,                  # $11 completed_at
                    stage.value,                         # $12 pipeline_stage
                    execution.start_time,                # $13 start_time
                    execution.end_time,                  # $14 end_time
                )

        except Exception as e:
            logger.error(f"Error logging pipeline execution: {e}")

    async def get_execution_status(self, execution_id: UUID) -> dict[str, Any] | None:
        """Get status of a pipeline execution."""
        execution = self.active_executions.get(execution_id)
        if not execution:
            return None

        return {
            "execution_id": str(execution.execution_id),
            "pipeline_mode": execution.pipeline_mode.value,
            "current_stage": execution.current_stage.value,
            "status": execution.status.value,
            "start_time": execution.start_time.isoformat(),
            "end_time": execution.end_time.isoformat() if execution.end_time else None,
            "metadata": execution.metadata,
            "metrics": {
                "total_records": execution.metrics.total_records,
                "successful_records": execution.metrics.successful_records,
                "failed_records": execution.metrics.failed_records,
                "processing_time_seconds": execution.metrics.processing_time_seconds,
            },
        }

    async def list_active_executions(self) -> list[PipelineExecution]:
        """List all active pipeline executions."""
        return list(self.active_executions.values())

    async def calculate_overall_metrics(
        self, execution: PipelineExecution
    ) -> dict[str, Any]:
        """Calculate overall pipeline metrics for an execution."""
        try:
            overall_metrics = {
                "total_records": execution.metrics.total_records,
                "successful_records": execution.metrics.successful_records,
                "failed_records": execution.metrics.failed_records,
                "processing_time_seconds": execution.metrics.processing_time_seconds,
                "overall_quality_score": 0.0,
                "overall_error_rate": 0.0,
                "zone_breakdown": {},
            }

            # Calculate overall scores
            if execution.metrics.total_records > 0:
                overall_metrics["overall_error_rate"] = (
                    execution.metrics.failed_records / execution.metrics.total_records
                )

            quality_scores = []
            for zone_type, zone_result in execution.metrics.zone_metrics.items():
                if isinstance(zone_result, dict):
                    quality_metrics = zone_result.get("quality_metrics")
                    if quality_metrics and hasattr(quality_metrics, "quality_score"):
                        quality_scores.append(quality_metrics.quality_score)

                    # Add zone breakdown
                    overall_metrics["zone_breakdown"][zone_type.value] = {
                        "records_processed": zone_result.get("records_processed", 0),
                        "records_successful": zone_result.get("records_successful", 0),
                        "records_failed": zone_result.get("records_failed", 0),
                        "processing_time": zone_result.get("processing_time", 0.0),
                        "quality_score": getattr(quality_metrics, "quality_score", 0.0)
                        if quality_metrics
                        else 0.0,
                    }

            if quality_scores:
                overall_metrics["overall_quality_score"] = sum(quality_scores) / len(
                    quality_scores
                )

            return overall_metrics

        except Exception as e:
            logger.error(f"Error calculating overall metrics: {e}")
            return {
                "total_records": 0,
                "successful_records": 0,
                "failed_records": 0,
                "processing_time_seconds": 0.0,
                "overall_quality_score": 0.0,
                "overall_error_rate": 0.0,
                "zone_breakdown": {},
            }

    async def get_zone_health(self) -> dict[ZoneType, dict[str, Any]]:
        """Get health status of all zones."""
        health_status = {}

        for zone_type, zone in self.zones.items():
            try:
                health = await zone.health_check()
                health_status[zone_type] = health
            except Exception as e:
                health_status[zone_type] = {"status": "error", "error": str(e)}

        return health_status

    async def cleanup(self) -> None:
        """Cleanup orchestrator resources."""
        try:
            # Cleanup all zones
            for zone in self.zones.values():
                if hasattr(zone, "cleanup"):
                    await zone.cleanup()

            # Clear active executions
            self.active_executions.clear()

            logger.info("Pipeline orchestrator cleanup completed")

        except Exception as e:
            logger.error(f"Error during orchestrator cleanup: {e}")


# Convenience function to create orchestrator
async def create_pipeline_orchestrator() -> DataPipelineOrchestrator:
    """Create and initialize a new pipeline orchestrator."""
    orchestrator = DataPipelineOrchestrator()
    return orchestrator
