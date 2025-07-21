"""
Base Zone Processor

Abstract base class for zone-specific data processors.
Provides common functionality for all zone processors.

Reference: docs/SYSTEM_DESIGN_ANALYSIS.md
"""

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from ...core.config import get_settings
from ...core.logging import get_logger, LogComponent
from ...data.database.connection import get_connection
from .zone_interface import (
    DataRecord, 
    DataZone, 
    ProcessingResult, 
    ProcessingStatus,
    ZoneConfig,
    ZoneMetrics
)

logger = get_logger(__name__, LogComponent.CORE)


class BaseZoneProcessor(DataZone):
    """
    Base implementation for all zone processors.
    
    Provides common functionality:
    - Database connection management
    - Batch processing orchestration
    - Error handling and retry logic
    - Metrics collection
    - Validation framework
    """

    def __init__(self, config: ZoneConfig):
        super().__init__(config)
        self.settings = get_settings()
        self._connection = None
        
    async def get_connection(self):
        """Get database connection."""
        if not self._connection:
            self._connection = await get_connection()
        return self._connection

    async def process_batch(
        self, 
        records: List[DataRecord],
        **kwargs
    ) -> ProcessingResult:
        """
        Process a batch of records with error handling and metrics.
        
        Args:
            records: List of data records to process
            **kwargs: Additional processing parameters
            
        Returns:
            ProcessingResult with processing status and metrics
        """
        start_time = time.time()
        result = ProcessingResult(
            status=ProcessingStatus.IN_PROGRESS,
            records_processed=len(records)
        )
        
        try:
            logger.info(
                f"Starting {self.zone_type} batch processing: {len(records)} records"
            )
            
            # Validate batch size
            if len(records) > self.config.batch_size:
                logger.warning(
                    f"Batch size {len(records)} exceeds configured limit {self.config.batch_size}"
                )
            
            # Process records
            successful_records = []
            failed_records = []
            errors = []
            
            for record in records:
                try:
                    # Validate record if enabled
                    if self.config.validation_enabled:
                        is_valid = await self.validate_record(record)
                        if not is_valid:
                            failed_records.append(record)
                            continue
                    
                    # Calculate quality score
                    quality_score = await self.get_quality_score(record)
                    record.quality_score = quality_score
                    
                    # Check quality threshold
                    if quality_score < self.config.quality_threshold:
                        logger.warning(
                            f"Record quality {quality_score} below threshold {self.config.quality_threshold}"
                        )
                        failed_records.append(record)
                        continue
                    
                    # Process individual record
                    processed_record = await self.process_record(record, **kwargs)
                    if processed_record:
                        successful_records.append(processed_record)
                    else:
                        failed_records.append(record)
                        
                except Exception as e:
                    logger.error(f"Error processing record {record.external_id}: {e}")
                    failed_records.append(record)
                    errors.append(str(e))
            
            # Store processed records
            if successful_records:
                await self.store_records(successful_records)
            
            # Update result
            result.records_successful = len(successful_records)
            result.records_failed = len(failed_records)
            result.errors = errors
            result.status = (
                ProcessingStatus.COMPLETED if not failed_records 
                else ProcessingStatus.FAILED if not successful_records
                else ProcessingStatus.COMPLETED  # Partial success
            )
            
            # Auto-promotion if enabled
            if (self.config.auto_promotion and successful_records and 
                result.status == ProcessingStatus.COMPLETED):
                await self.promote_to_next_zone(successful_records)
            
            logger.info(
                f"{self.zone_type} batch processing completed: "
                f"{len(successful_records)} successful, {len(failed_records)} failed"
            )
            
        except Exception as e:
            logger.error(f"Batch processing failed: {e}")
            result.status = ProcessingStatus.FAILED
            result.errors = [str(e)]
            
        finally:
            result.processing_time = time.time() - start_time
            result.quality_metrics = ZoneMetrics(
                records_processed=result.records_processed,
                records_successful=result.records_successful,
                records_failed=result.records_failed,
                processing_time_seconds=result.processing_time,
                quality_score=self._calculate_batch_quality_score(records),
                error_rate=result.records_failed / max(result.records_processed, 1)
            )
            
            # Update zone metrics
            self.update_metrics(result)
        
        return result

    @abstractmethod
    async def process_record(
        self, 
        record: DataRecord, 
        **kwargs
    ) -> Optional[DataRecord]:
        """
        Process a single record. Must be implemented by subclasses.
        
        Args:
            record: Data record to process
            **kwargs: Additional processing parameters
            
        Returns:
            Processed record or None if processing failed
        """
        pass

    @abstractmethod
    async def store_records(self, records: List[DataRecord]) -> None:
        """
        Store processed records to database. Must be implemented by subclasses.
        
        Args:
            records: List of processed records to store
        """
        pass

    async def validate_record(self, record: DataRecord) -> bool:
        """
        Default record validation. Can be overridden by subclasses.
        
        Args:
            record: Data record to validate
            
        Returns:
            True if record passes validation
        """
        try:
            # Basic validation
            if not record.source:
                record.validation_errors = record.validation_errors or []
                record.validation_errors.append("Missing source")
                return False
            
            if not record.external_id and not record.raw_data:
                record.validation_errors = record.validation_errors or []
                record.validation_errors.append("Missing external_id and raw_data")
                return False
            
            # Zone-specific validation
            return await self.validate_record_custom(record)
            
        except Exception as e:
            logger.error(f"Validation error for record {record.external_id}: {e}")
            record.validation_errors = record.validation_errors or []
            record.validation_errors.append(f"Validation exception: {e}")
            return False

    async def validate_record_custom(self, record: DataRecord) -> bool:
        """
        Custom record validation. Override in subclasses.
        
        Args:
            record: Data record to validate
            
        Returns:
            True if record passes custom validation
        """
        return True

    async def get_quality_score(self, record: DataRecord) -> float:
        """
        Calculate quality score for a record.
        
        Args:
            record: Data record to score
            
        Returns:
            Quality score between 0.0 and 1.0
        """
        try:
            score = 0.0
            components = 0
            
            # Completeness score
            completeness = await self._calculate_completeness_score(record)
            score += completeness
            components += 1
            
            # Accuracy score (can be overridden)
            accuracy = await self._calculate_accuracy_score(record)
            score += accuracy
            components += 1
            
            # Consistency score (can be overridden)
            consistency = await self._calculate_consistency_score(record)
            score += consistency
            components += 1
            
            return score / max(components, 1)
            
        except Exception as e:
            logger.error(f"Error calculating quality score: {e}")
            return 0.0

    async def _calculate_completeness_score(self, record: DataRecord) -> float:
        """Calculate completeness score based on required fields."""
        required_fields = ['source', 'external_id']
        optional_fields = ['raw_data']
        
        score = 0.0
        total_weight = 0.0
        
        # Required fields (weight: 0.8)
        required_weight = 0.8 / len(required_fields)
        for field in required_fields:
            if getattr(record, field, None):
                score += required_weight
            total_weight += required_weight
        
        # Optional fields (weight: 0.2)
        optional_weight = 0.2 / len(optional_fields)
        for field in optional_fields:
            if getattr(record, field, None):
                score += optional_weight
            total_weight += optional_weight
        
        return score / max(total_weight, 1)

    async def _calculate_accuracy_score(self, record: DataRecord) -> float:
        """Calculate accuracy score. Override in subclasses for specific logic."""
        return 1.0  # Default: assume accurate

    async def _calculate_consistency_score(self, record: DataRecord) -> float:
        """Calculate consistency score. Override in subclasses for specific logic."""
        return 1.0  # Default: assume consistent

    def _calculate_batch_quality_score(self, records: List[DataRecord]) -> float:
        """Calculate average quality score for a batch of records."""
        if not records:
            return 0.0
        
        total_score = sum(
            record.quality_score or 0.0 for record in records
        )
        return total_score / len(records)

    async def promote_to_next_zone(
        self, 
        records: List[DataRecord]
    ) -> ProcessingResult:
        """
        Default promotion logic. Override in subclasses for specific behavior.
        
        Args:
            records: Records to promote
            
        Returns:
            ProcessingResult with promotion status
        """
        # Default implementation logs promotion
        logger.info(
            f"Promoting {len(records)} records from {self.zone_type} to next zone"
        )
        
        return ProcessingResult(
            status=ProcessingStatus.COMPLETED,
            records_processed=len(records),
            records_successful=len(records)
        )

    async def retry_failed_records(
        self, 
        failed_records: List[DataRecord],
        max_retries: Optional[int] = None
    ) -> ProcessingResult:
        """
        Retry processing failed records with exponential backoff.
        
        Args:
            failed_records: List of records that failed processing
            max_retries: Maximum number of retry attempts
            
        Returns:
            ProcessingResult with retry status
        """
        max_retries = max_retries or self.config.retry_attempts
        
        for attempt in range(max_retries):
            if not failed_records:
                break
                
            logger.info(f"Retry attempt {attempt + 1}/{max_retries} for {len(failed_records)} records")
            
            # Exponential backoff
            if attempt > 0:
                wait_time = 2 ** attempt
                logger.info(f"Waiting {wait_time} seconds before retry")
                await asyncio.sleep(wait_time)
            
            # Retry processing
            result = await self.process_batch(failed_records)
            
            # Update failed records list
            if result.status == ProcessingStatus.COMPLETED:
                failed_records = []
            else:
                # Keep only records that failed again
                logger.warning(f"Retry {attempt + 1} failed for some records")
        
        return ProcessingResult(
            status=ProcessingStatus.COMPLETED if not failed_records else ProcessingStatus.FAILED,
            records_processed=len(failed_records),
            records_successful=0 if failed_records else len(failed_records),
            records_failed=len(failed_records)
        )

    async def cleanup(self) -> None:
        """Cleanup resources."""
        if self._connection:
            await self._connection.close()
            self._connection = None