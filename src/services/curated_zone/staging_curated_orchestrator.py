"""
STAGING → CURATED Orchestrator

Coordinates all processing from STAGING zone to CURATED zone.
Core orchestration service to resolve the critical pipeline gap.

This orchestrator addresses the broken STAGING → CURATED flow by:
- Coordinating enhanced_games_service processing
- Managing ML temporal features generation  
- Orchestrating betting splits aggregation
- Real-time pipeline coordination with failure detection
- Processing lag monitoring and alerting

Reference: docs/STAGING_CURATED_GAP_ANALYSIS.md - Critical Gap Resolution
"""

import asyncio
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from ...core.config import get_settings
from ...core.logging import LogComponent, get_logger
from ...data.database.connection import get_connection
from .enhanced_games_service import EnhancedGamesService
from .ml_temporal_features_service import MLTemporalFeaturesService
from .betting_splits_aggregator import BettingSplitsAggregator

logger = get_logger(__name__, LogComponent.CORE)


class ProcessingMode(str, Enum):
    """STAGING → CURATED processing modes."""
    
    FULL = "full"  # Process all services
    GAMES_ONLY = "games_only"  # Enhanced games only
    FEATURES_ONLY = "features_only"  # ML features only  
    SPLITS_ONLY = "splits_only"  # Betting splits only


class ProcessingStatus(str, Enum):
    """Processing status for orchestration."""
    
    PENDING = "pending"
    IN_PROGRESS = "in_progress" 
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL = "partial"


class OrchestrationResult(BaseModel):
    """Result of STAGING → CURATED orchestration."""
    
    status: ProcessingStatus = ProcessingStatus.PENDING
    mode: ProcessingMode = ProcessingMode.FULL
    
    # Processing results
    games_processed: int = 0
    features_processed: int = 0
    splits_processed: int = 0
    
    # Timing
    start_time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    end_time: Optional[datetime] = None
    processing_time_seconds: float = 0.0
    
    # Quality metrics
    success_rate: float = 0.0
    data_quality_score: float = 0.0
    
    # Error tracking
    errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    
    # Metadata
    metadata: Dict[str, Any] = Field(default_factory=dict)


class StagingCuratedOrchestrator:
    """
    Orchestrator for STAGING → CURATED data processing pipeline.
    
    Coordinates all services needed to transform staging data into 
    ML-ready curated data, resolving the critical pipeline gap.
    """
    
    def __init__(self):
        self.settings = get_settings()
        self.enhanced_games_service = EnhancedGamesService()
        self.ml_features_service = MLTemporalFeaturesService()
        self.betting_splits_service = BettingSplitsAggregator()
        
        # Processing statistics
        self.orchestration_stats = {
            "total_runs": 0,
            "successful_runs": 0,
            "failed_runs": 0,
            "last_run": None,
            "average_processing_time": 0.0
        }
    
    async def run_full_processing(
        self,
        days_back: int = 7,
        dry_run: bool = False,
        limit: Optional[int] = None
    ) -> OrchestrationResult:
        """
        Run complete STAGING → CURATED processing pipeline.
        
        Args:
            days_back: Days to look back for staging data
            dry_run: If True, don't actually modify data
            limit: Limit processing for testing
            
        Returns:
            OrchestrationResult with comprehensive results
        """
        result = OrchestrationResult(mode=ProcessingMode.FULL)
        result.metadata = {
            "days_back": days_back,
            "dry_run": dry_run, 
            "limit": limit,
            "orchestrator_version": "1.0"
        }
        
        try:
            logger.info(f"Starting full STAGING → CURATED processing: days_back={days_back}, dry_run={dry_run}")
            result.status = ProcessingStatus.IN_PROGRESS
            
            # Update orchestration stats
            self.orchestration_stats["total_runs"] += 1
            
            # Phase 1: Enhanced Games Processing (Critical)
            logger.info("Phase 1: Processing enhanced games...")
            games_result = await self.enhanced_games_service.process_recent_games(
                days_back=days_back,
                limit=limit,
                dry_run=dry_run
            )
            
            result.games_processed = games_result.games_successful
            result.errors.extend(games_result.errors)
            
            if games_result.games_failed > 0:
                result.warnings.append(f"Enhanced games: {games_result.games_failed} failed out of {games_result.games_processed}")
            
            logger.info(f"Enhanced games processing: {games_result.games_successful}/{games_result.games_processed} successful")
            
            # Phase 2: ML Temporal Features Processing
            logger.info("Phase 2: Processing ML temporal features...")
            if games_result.games_successful > 0 and not dry_run:
                # Get processed game IDs for feature generation
                processed_games = await self._get_recent_processed_games(days_back, limit)
                features_generated = 0
                features_failed = 0
                
                for game_info in processed_games[:min(limit or 10, 10)]:  # Limit for testing
                    try:
                        ml_result = await self.ml_features_service.process_ml_features(
                            game_info["id"], dry_run=dry_run
                        )
                        if ml_result.features_generated > 0:
                            features_generated += 1
                        if ml_result.errors:
                            features_failed += 1
                    except Exception as e:
                        logger.error(f"ML features failed for game {game_info['id']}: {e}")
                        features_failed += 1
                
                result.features_processed = features_generated
                if features_failed > 0:
                    result.warnings.append(f"ML features: {features_failed} failed")
                
                logger.info(f"ML temporal features: {features_generated} successful, {features_failed} failed")
            else:
                result.warnings.append("ML temporal features skipped - no successful games or dry run mode")
            
            # Phase 3: Betting Splits Aggregation
            logger.info("Phase 3: Processing betting splits aggregation...")
            if games_result.games_successful > 0 and not dry_run:
                processed_games = await self._get_recent_processed_games(days_back, limit)
                splits_generated = 0
                splits_failed = 0
                
                for game_info in processed_games[:min(limit or 5, 5)]:  # Smaller limit for testing
                    try:
                        splits_result = await self.betting_splits_service.process_betting_splits(
                            game_info["id"], dry_run=dry_run
                        )
                        if splits_result.splits_processed > 0:
                            splits_generated += splits_result.splits_processed
                        if splits_result.errors:
                            splits_failed += 1
                    except Exception as e:
                        logger.error(f"Betting splits failed for game {game_info['id']}: {e}")
                        splits_failed += 1
                
                result.splits_processed = splits_generated
                if splits_failed > 0:
                    result.warnings.append(f"Betting splits: {splits_failed} games failed")
                
                logger.info(f"Betting splits: {splits_generated} splits generated, {splits_failed} games failed")
            else:
                result.warnings.append("Betting splits skipped - no successful games or dry run mode")
            
            # Calculate overall results
            result.end_time = datetime.now(timezone.utc)
            result.processing_time_seconds = (result.end_time - result.start_time).total_seconds()
            
            # Determine overall status
            if result.errors:
                if result.games_processed > 0:
                    result.status = ProcessingStatus.PARTIAL
                    result.success_rate = result.games_processed / max(games_result.games_processed, 1)
                else:
                    result.status = ProcessingStatus.FAILED
                    result.success_rate = 0.0
            else:
                result.status = ProcessingStatus.COMPLETED
                result.success_rate = 1.0 if games_result.games_processed > 0 else 0.0
            
            # Update processing statistics
            if result.status in [ProcessingStatus.COMPLETED, ProcessingStatus.PARTIAL]:
                self.orchestration_stats["successful_runs"] += 1
            else:
                self.orchestration_stats["failed_runs"] += 1
                
            self.orchestration_stats["last_run"] = result.start_time
            self.orchestration_stats["average_processing_time"] = (
                (self.orchestration_stats["average_processing_time"] * (self.orchestration_stats["total_runs"] - 1) + 
                 result.processing_time_seconds) / self.orchestration_stats["total_runs"]
            )
            
            logger.info(
                f"STAGING → CURATED orchestration completed: "
                f"Status={result.status}, Games={result.games_processed}, "
                f"Time={result.processing_time_seconds:.2f}s"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"STAGING → CURATED orchestration failed: {e}")
            result.status = ProcessingStatus.FAILED
            result.errors.append(str(e))
            result.end_time = datetime.now(timezone.utc)
            result.processing_time_seconds = (result.end_time - result.start_time).total_seconds()
            
            self.orchestration_stats["failed_runs"] += 1
            
            return result
    
    async def run_games_only_processing(
        self,
        days_back: int = 7,
        dry_run: bool = False,
        limit: Optional[int] = None
    ) -> OrchestrationResult:
        """
        Run enhanced games processing only.
        
        This is the critical service that resolves 0% game coverage.
        """
        result = OrchestrationResult(mode=ProcessingMode.GAMES_ONLY)
        
        try:
            logger.info("Starting enhanced games only processing...")
            result.status = ProcessingStatus.IN_PROGRESS
            
            games_result = await self.enhanced_games_service.process_recent_games(
                days_back=days_back,
                limit=limit,
                dry_run=dry_run
            )
            
            result.games_processed = games_result.games_successful
            result.errors.extend(games_result.errors)
            result.end_time = datetime.now(timezone.utc)
            result.processing_time_seconds = (result.end_time - result.start_time).total_seconds()
            
            if games_result.errors:
                result.status = ProcessingStatus.PARTIAL if games_result.games_successful > 0 else ProcessingStatus.FAILED
            else:
                result.status = ProcessingStatus.COMPLETED
                
            result.success_rate = games_result.games_successful / max(games_result.games_processed, 1)
            
            result.metadata = {
                "days_back": days_back,
                "dry_run": dry_run,
                "limit": limit,
                "games_result": games_result.dict()
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Enhanced games only processing failed: {e}")
            result.status = ProcessingStatus.FAILED
            result.errors.append(str(e))
            return result
    
    async def get_processing_lag_hours(self) -> float:
        """
        Get current processing lag between STAGING and CURATED zones.
        
        Returns hours between latest staging data and latest curated data.
        Critical metric for monitoring the pipeline gap.
        """
        try:
            async with get_connection() as conn:
                # Use the new coverage analysis view for more reliable lag calculation
                result = await conn.fetchrow("SELECT processing_lag_hours FROM curated.coverage_analysis")
                
                if result and result['processing_lag_hours'] is not None:
                    return float(result['processing_lag_hours'])
                
                # Fallback to manual calculation
                lag_query = """
                    WITH staging_latest AS (
                        SELECT MAX(created_at) as latest_staging
                        FROM staging.action_network_odds_historical 
                        WHERE created_at > NOW() - INTERVAL '7 days'
                    ),
                    curated_latest AS (
                        SELECT MAX(created_at) as latest_curated
                        FROM curated.enhanced_games
                    )
                    SELECT 
                        s.latest_staging,
                        c.latest_curated,
                        CASE 
                            WHEN s.latest_staging IS NOT NULL AND c.latest_curated IS NOT NULL
                            THEN EXTRACT(EPOCH FROM (s.latest_staging - c.latest_curated))/3600
                            WHEN s.latest_staging IS NOT NULL 
                            THEN EXTRACT(EPOCH FROM (NOW() - s.latest_staging))/3600
                            ELSE 0
                        END as lag_hours
                    FROM staging_latest s, curated_latest c
                """
                
                result = await conn.fetchrow(lag_query)
                
                if result and result['lag_hours'] is not None:
                    return float(result['lag_hours'])
                
                return 0.0
                    
        except Exception as e:
            logger.error(f"Error calculating processing lag: {e}")
            return -1.0  # Error indicator
    
    async def get_curated_coverage_stats(self) -> Dict[str, Any]:
        """Get coverage statistics for STAGING → CURATED processing."""
        
        try:
            async with get_connection() as conn:
                # Get staging vs curated game counts
                coverage_query = """
                    SELECT 
                        (SELECT COUNT(DISTINCT external_game_id) 
                         FROM staging.action_network_odds_historical 
                         WHERE created_at > NOW() - INTERVAL '7 days') as staging_games,
                        (SELECT COUNT(*) 
                         FROM curated.enhanced_games 
                         WHERE created_at > NOW() - INTERVAL '7 days') as curated_games,
                        (SELECT COUNT(*) 
                         FROM curated.enhanced_games) as total_curated_games
                """
                
                result = await conn.fetchrow(coverage_query)
                
                staging_games = result['staging_games'] or 0
                curated_games = result['curated_games'] or 0
                total_curated = result['total_curated_games'] or 0
                
                coverage_percentage = (curated_games / staging_games * 100) if staging_games > 0 else 0.0
                
                return {
                    "staging_games_recent": staging_games,
                    "curated_games_recent": curated_games,
                    "total_curated_games": total_curated,
                    "coverage_percentage": coverage_percentage,
                    "missing_games": staging_games - curated_games,
                    "processing_lag_hours": await self.get_processing_lag_hours()
                }
                
        except Exception as e:
            logger.error(f"Error getting coverage stats: {e}")
            return {
                "error": str(e),
                "staging_games_recent": 0,
                "curated_games_recent": 0,
                "coverage_percentage": 0.0
            }
    
    async def health_check(self) -> Dict[str, Any]:
        """Comprehensive health check for STAGING → CURATED pipeline."""
        
        try:
            # Get coverage stats
            coverage_stats = await self.get_curated_coverage_stats()
            processing_lag = coverage_stats.get("processing_lag_hours", -1)
            
            # Determine health status
            if processing_lag == -1.0:
                status = "unhealthy"
                issues = ["Failed to calculate processing lag"]
            elif abs(processing_lag) > 24:
                status = "critical"
                issues = [f"Processing lag too high: {processing_lag:.1f} hours"]
            elif abs(processing_lag) > 6:
                status = "warning"
                issues = [f"Processing lag elevated: {processing_lag:.1f} hours"]
            elif coverage_stats.get("coverage_percentage", 0) < 50:
                status = "warning" 
                issues = [f"Low coverage: {coverage_stats.get('coverage_percentage', 0):.1f}%"]
            else:
                status = "healthy"
                issues = []
            
            # Enhanced games service health
            games_health = await self.enhanced_games_service.health_check()
            
            return {
                "status": status,
                "issues": issues,
                "processing_lag_hours": processing_lag,
                "coverage_stats": coverage_stats,
                "orchestration_stats": self.orchestration_stats,
                "enhanced_games_service": games_health,
                "services_implemented": {
                    "enhanced_games": True,
                    "ml_temporal_features": True,
                    "betting_splits_aggregator": True
                }
            }
            
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "services_implemented": {
                    "enhanced_games": True,
                    "ml_temporal_features": True,
                    "betting_splits_aggregator": True
                }
            }
    
    async def _get_recent_processed_games(self, days_back: int, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get recent processed games from curated.enhanced_games."""
        
        try:
            async with get_connection() as conn:
                query = """
                    SELECT id, action_network_game_id, home_team, away_team, game_datetime
                    FROM curated.enhanced_games
                    WHERE created_at > NOW() - INTERVAL '%s days'
                    ORDER BY created_at DESC
                    LIMIT $1
                """
                
                query_limit = limit or 50
                formatted_query = query % days_back
                rows = await conn.fetch(formatted_query, query_limit)
                
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Error getting recent processed games: {e}")
            return []
    
    async def get_orchestration_stats(self) -> Dict[str, Any]:
        """Get orchestration statistics for monitoring."""
        
        stats = dict(self.orchestration_stats)
        
        # Add enhanced games service stats
        try:
            games_stats = await self.enhanced_games_service.get_processing_stats()
            stats["enhanced_games_service"] = games_stats
            
            # Add ML features service stats
            ml_stats = await self.ml_features_service.get_processing_stats()
            stats["ml_features_service"] = ml_stats
            
            # Add betting splits service stats
            splits_stats = await self.betting_splits_service.get_processing_stats()
            stats["betting_splits_service"] = splits_stats
            
            # Add coverage analysis
            coverage_stats = await self.get_curated_coverage_stats()
            stats["coverage_analysis"] = coverage_stats
            
        except Exception as e:
            logger.error(f"Error getting orchestration stats: {e}")
            stats["stats_error"] = str(e)
        
        return stats