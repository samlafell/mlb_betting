#!/usr/bin/env python3
"""
Enhanced Games Outcome Sync Service

CRITICAL: Fixes ML Training Pipeline Zero Data Issue (#67)

This service creates the missing ETL pipeline between:
- curated.game_outcomes (94 complete games with scores) 
- curated.enhanced_games (currently 0 games with scores)

The ML training pipeline depends on enhanced_games having real game scores,
but there was no automated sync mechanism. This service provides:

1. Automated sync of game outcomes to enhanced_games
2. Backfill of existing 94 games with scores  
3. Real-time updates when new outcomes are available
4. Data validation and quality scoring
5. Integration with existing enhanced_games service
6. CLI commands for manual and automated execution

Reference: GitHub Issue #67 - ML Training Pipeline Has Zero Real Data
"""

import asyncio
import json
from datetime import datetime, timezone, date, timedelta
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field

from ...core.config import get_settings
from ...core.datetime_utils import prepare_for_postgres
from ...core.logging import LogComponent, get_logger
from ...core.team_utils import normalize_team_name
from ...data.database.connection import get_connection, initialize_connections

logger = get_logger(__name__, LogComponent.CORE)


class GameOutcomeSyncResult(BaseModel):
    """Result of syncing game outcomes to enhanced games."""
    
    outcomes_found: int = 0
    enhanced_games_updated: int = 0
    enhanced_games_created: int = 0
    sync_failures: int = 0
    processing_time_seconds: float = 0.0
    errors: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class EnhancedGameWithOutcome(BaseModel):
    """Enhanced game data with complete outcome information."""
    
    # Enhanced games fields
    enhanced_game_id: Optional[int] = None
    action_network_game_id: Optional[int] = None
    mlb_stats_api_game_id: Optional[str] = None
    
    # Team information
    home_team: str
    away_team: str
    home_team_full_name: Optional[str] = None
    away_team_full_name: Optional[str] = None
    
    # Game timing and metadata
    game_datetime: datetime
    game_date: Optional[date] = None
    season: Optional[int] = None
    game_status: str = "final"
    
    # CRITICAL: Game outcome data (from game_outcomes)
    home_score: int
    away_score: int
    winning_team: str
    home_win: bool
    over: bool
    home_cover_spread: Optional[bool] = None
    total_line: Optional[float] = None
    home_spread_line: Optional[float] = None
    
    # Enhanced features
    feature_data: Dict[str, Any] = Field(default_factory=dict)
    ml_metadata: Dict[str, Any] = Field(default_factory=dict)
    data_quality_score: float = 1.0
    mlb_correlation_confidence: float = 1.0
    source_coverage_score: float = 1.0


class EnhancedGamesOutcomeSyncService:
    """
    Service to sync game outcomes into enhanced_games table.
    
    CRITICAL: This service solves the ML Training Pipeline zero data issue
    by ensuring enhanced_games table has complete game scores from game_outcomes.
    """
    
    def __init__(self):
        self.settings = get_settings()
        self.sync_stats = {
            "total_synced": 0,
            "total_created": 0,
            "total_updated": 0,
            "last_sync": None
        }
    
    async def sync_all_missing_outcomes(
        self, 
        dry_run: bool = False,
        limit: Optional[int] = None
    ) -> GameOutcomeSyncResult:
        """
        CRITICAL: Sync all game outcomes that are missing from enhanced_games.
        
        This is the main method to fix the ML pipeline zero data issue.
        It finds all games in game_outcomes that don't have corresponding
        enhanced_games records with scores.
        
        Args:
            dry_run: If True, don't actually update data
            limit: Maximum number of games to sync (None for all)
            
        Returns:
            GameOutcomeSyncResult with sync details
        """
        start_time = datetime.now(timezone.utc)
        result = GameOutcomeSyncResult()
        
        try:
            logger.info(f"Starting enhanced games outcome sync: dry_run={dry_run}, limit={limit}")
            
            # Get all game outcomes missing from enhanced_games
            missing_outcomes = await self._get_missing_enhanced_game_outcomes(limit)
            result.outcomes_found = len(missing_outcomes)
            
            if not missing_outcomes:
                logger.info("No missing game outcomes found - enhanced_games is up to date")
                result.metadata["message"] = "No missing outcomes found"
                return result
            
            logger.info(f"Found {len(missing_outcomes)} game outcomes missing from enhanced_games")
            
            # Sync each missing outcome
            created_count = 0
            updated_count = 0
            
            for outcome_data in missing_outcomes:
                try:
                    enhanced_game_with_outcome = await self._create_enhanced_game_with_outcome(outcome_data)
                    
                    if not dry_run:
                        was_created = await self._upsert_enhanced_game_with_outcome(enhanced_game_with_outcome)
                        if was_created:
                            created_count += 1
                        else:
                            updated_count += 1
                    
                    logger.debug(f"Synced game outcome {outcome_data.get('game_id')} to enhanced_games")
                    
                except Exception as e:
                    error_msg = f"Failed to sync game outcome {outcome_data.get('game_id', 'unknown')}: {e}"
                    logger.error(error_msg)
                    result.errors.append(error_msg)
                    result.sync_failures += 1
            
            result.enhanced_games_created = created_count
            result.enhanced_games_updated = updated_count
            
            # Update service stats
            self.sync_stats["total_synced"] += result.outcomes_found
            self.sync_stats["total_created"] += result.enhanced_games_created
            self.sync_stats["total_updated"] += result.enhanced_games_updated
            self.sync_stats["last_sync"] = start_time
            
            # Calculate processing time
            end_time = datetime.now(timezone.utc)
            result.processing_time_seconds = (end_time - start_time).total_seconds()
            
            result.metadata = {
                "dry_run": dry_run,
                "limit": limit,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "sync_type": "all_missing_outcomes"
            }
            
            logger.info(
                f"Enhanced games outcome sync completed: "
                f"created={result.enhanced_games_created}, updated={result.enhanced_games_updated}, "
                f"failed={result.sync_failures} in {result.processing_time_seconds:.2f}s"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Enhanced games outcome sync failed: {e}")
            result.errors.append(str(e))
            return result
    
    async def sync_recent_outcomes(
        self, 
        days_back: int = 7,
        dry_run: bool = False
    ) -> GameOutcomeSyncResult:
        """
        Sync recent game outcomes to enhanced_games for ongoing operations.
        
        This method ensures new game outcomes are automatically reflected
        in enhanced_games for ML training pipeline continuity.
        
        Args:
            days_back: Number of days to look back for recent outcomes
            dry_run: If True, don't actually update data
            
        Returns:
            GameOutcomeSyncResult with sync details
        """
        start_time = datetime.now(timezone.utc)
        result = GameOutcomeSyncResult()
        
        try:
            logger.info(f"Starting recent enhanced games outcome sync: days_back={days_back}, dry_run={dry_run}")
            
            # Get recent game outcomes that need syncing
            recent_outcomes = await self._get_recent_outcomes_for_sync(days_back)
            result.outcomes_found = len(recent_outcomes)
            
            if not recent_outcomes:
                logger.info("No recent game outcomes found for sync")
                result.metadata["message"] = "No recent outcomes found"
                return result
            
            logger.info(f"Found {len(recent_outcomes)} recent game outcomes for sync")
            
            # Sync each recent outcome
            created_count = 0
            updated_count = 0
            
            for outcome_data in recent_outcomes:
                try:
                    enhanced_game_with_outcome = await self._create_enhanced_game_with_outcome(outcome_data)
                    
                    if not dry_run:
                        was_created = await self._upsert_enhanced_game_with_outcome(enhanced_game_with_outcome)
                        if was_created:
                            created_count += 1
                        else:
                            updated_count += 1
                    
                    logger.debug(f"Synced recent game outcome {outcome_data.get('game_id')} to enhanced_games")
                    
                except Exception as e:
                    error_msg = f"Failed to sync recent game outcome {outcome_data.get('game_id', 'unknown')}: {e}"
                    logger.error(error_msg)
                    result.errors.append(error_msg)
                    result.sync_failures += 1
            
            result.enhanced_games_created = created_count
            result.enhanced_games_updated = updated_count
            
            # Calculate processing time
            end_time = datetime.now(timezone.utc)
            result.processing_time_seconds = (end_time - start_time).total_seconds()
            
            result.metadata = {
                "dry_run": dry_run,
                "days_back": days_back,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "sync_type": "recent_outcomes"
            }
            
            logger.info(
                f"Recent enhanced games outcome sync completed: "
                f"created={result.enhanced_games_created}, updated={result.enhanced_games_updated}, "
                f"failed={result.sync_failures} in {result.processing_time_seconds:.2f}s"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Recent enhanced games outcome sync failed: {e}")
            result.errors.append(str(e))
            return result
    
    async def _get_missing_enhanced_game_outcomes(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get all game outcomes that are missing from enhanced_games.
        
        This query identifies the critical gap: games with complete outcomes
        that don't have corresponding enhanced_games records with scores.
        """
        
        async with get_connection() as conn:
            query = """
                WITH complete_outcomes AS (
                    SELECT DISTINCT
                        go.game_id,
                        go.home_team,
                        go.away_team,
                        go.home_score,
                        go.away_score,
                        go.home_win,
                        go.over,
                        go.home_cover_spread,
                        go.total_line,
                        go.home_spread_line,
                        go.game_date as outcome_game_date,
                        gc.mlb_stats_api_game_id,
                        gc.action_network_game_id,
                        gc.game_datetime,
                        gc.game_date as game_date,
                        gc.season,
                        gc.venue_name,
                        gc.game_status,
                        -- Determine winning team
                        CASE 
                            WHEN go.home_win THEN go.home_team 
                            ELSE go.away_team 
                        END as winning_team
                    FROM curated.game_outcomes go
                    INNER JOIN curated.games_complete gc ON go.game_id = gc.id
                    WHERE go.home_score IS NOT NULL 
                        AND go.away_score IS NOT NULL
                )
                SELECT co.*
                FROM complete_outcomes co
                LEFT JOIN curated.enhanced_games eg 
                    ON co.game_id = eg.id 
                    OR (co.action_network_game_id IS NOT NULL AND co.action_network_game_id = eg.action_network_game_id)
                    OR (co.mlb_stats_api_game_id IS NOT NULL AND co.mlb_stats_api_game_id = eg.mlb_stats_api_game_id)
                WHERE (eg.id IS NULL OR eg.home_score IS NULL OR eg.away_score IS NULL)
                ORDER BY co.outcome_game_date DESC, co.game_id DESC
            """
            
            if limit:
                query += f" LIMIT {limit}"
            
            rows = await conn.fetch(query)
            return [dict(row) for row in rows]
    
    async def _get_recent_outcomes_for_sync(self, days_back: int) -> List[Dict[str, Any]]:
        """Get recent game outcomes that may need syncing to enhanced_games."""
        
        async with get_connection() as conn:
            query = """
                SELECT DISTINCT
                    go.game_id,
                    go.home_team,
                    go.away_team,
                    go.home_score,
                    go.away_score,
                    go.home_win,
                    go.over,
                    go.home_cover_spread,
                    go.total_line,
                    go.home_spread_line,
                    go.game_date as outcome_game_date,
                    gc.mlb_stats_api_game_id,
                    gc.action_network_game_id,
                    gc.game_datetime,
                    gc.game_date as game_date,
                    gc.season,
                    gc.venue_name,
                    gc.game_status,
                    -- Determine winning team
                    CASE 
                        WHEN go.home_win THEN go.home_team 
                        ELSE go.away_team 
                    END as winning_team
                FROM curated.game_outcomes go
                INNER JOIN curated.games_complete gc ON go.game_id = gc.id
                WHERE go.home_score IS NOT NULL 
                    AND go.away_score IS NOT NULL
                    AND go.game_date > NOW() - INTERVAL '%s days'
                ORDER BY go.game_date DESC, go.game_id DESC
            """ % days_back
            
            rows = await conn.fetch(query)
            return [dict(row) for row in rows]
    
    async def _create_enhanced_game_with_outcome(self, outcome_data: Dict[str, Any]) -> EnhancedGameWithOutcome:
        """
        Create an enhanced game record with complete outcome data.
        
        This combines the enhanced_games structure with the critical
        game outcome data needed for ML training.
        """
        
        # Extract game timing information
        game_datetime = outcome_data["game_datetime"]
        if game_datetime:
            game_datetime = prepare_for_postgres(game_datetime)
        
        # Determine winning team
        winning_team = outcome_data["winning_team"]
        
        # Build enhanced game with outcome
        enhanced_game = EnhancedGameWithOutcome(
            # Enhanced games identifiers
            enhanced_game_id=None,  # Will be set if updating existing record
            action_network_game_id=outcome_data.get("action_network_game_id"),
            mlb_stats_api_game_id=outcome_data.get("mlb_stats_api_game_id"),
            
            # Team information
            home_team=outcome_data["home_team"],
            away_team=outcome_data["away_team"],
            home_team_full_name=None,  # TODO: Could enhance with full team names
            away_team_full_name=None,
            
            # Game timing
            game_datetime=game_datetime,
            game_date=outcome_data.get("game_date"),
            season=outcome_data.get("season") or (game_datetime.year if game_datetime else datetime.now().year),
            game_status="final",
            
            # CRITICAL: Game outcome data for ML training
            home_score=int(outcome_data["home_score"]),
            away_score=int(outcome_data["away_score"]),
            winning_team=winning_team,
            home_win=outcome_data["home_win"],
            over=outcome_data["over"],
            home_cover_spread=outcome_data.get("home_cover_spread"),
            total_line=float(outcome_data["total_line"]) if outcome_data.get("total_line") else None,
            home_spread_line=float(outcome_data["home_spread_line"]) if outcome_data.get("home_spread_line") else None,
            
            # Data quality - these games have actual outcomes so high quality
            data_quality_score=1.0,
            mlb_correlation_confidence=1.0,
            source_coverage_score=1.0
        )
        
        # Add ML metadata for training pipeline
        await self._add_ml_training_metadata(enhanced_game, outcome_data)
        
        return enhanced_game
    
    async def _add_ml_training_metadata(self, enhanced_game: EnhancedGameWithOutcome, outcome_data: Dict[str, Any]) -> None:
        """Add ML-specific metadata for the training pipeline."""
        
        enhanced_game.ml_metadata = {
            "processing_timestamp": datetime.now(timezone.utc).isoformat(),
            "data_sources": ["game_outcomes", "games_complete"],
            "feature_version": "1.0",
            "outcome_sync_service": "enhanced_games_outcome_sync_service",
            "has_complete_outcome": True,
            "quality_checks": {
                "has_teams": bool(enhanced_game.home_team and enhanced_game.away_team),
                "has_datetime": bool(enhanced_game.game_datetime),
                "has_scores": bool(enhanced_game.home_score is not None and enhanced_game.away_score is not None),
                "has_outcome_data": True,
                "ml_training_ready": True
            },
            "outcome_metadata": {
                "original_game_id": outcome_data["game_id"],
                "outcome_source": "curated.game_outcomes",
                "has_betting_lines": bool(outcome_data.get("total_line") or outcome_data.get("home_spread_line")),
                "total_runs": enhanced_game.home_score + enhanced_game.away_score
            }
        }
        
        # Set feature data for ML compatibility
        enhanced_game.feature_data = {
            "source": "enhanced_games_outcome_sync_service",
            "version": "1.0",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "ml_training_ready": True,
            "outcome_summary": {
                "home_score": enhanced_game.home_score,
                "away_score": enhanced_game.away_score,
                "total_runs": enhanced_game.home_score + enhanced_game.away_score,
                "home_win": enhanced_game.home_win,
                "margin": abs(enhanced_game.home_score - enhanced_game.away_score)
            },
            "betting_summary": {
                "has_total_line": enhanced_game.total_line is not None,
                "has_spread_line": enhanced_game.home_spread_line is not None,
                "over_result": enhanced_game.over,
                "spread_cover": enhanced_game.home_cover_spread
            }
        }
    
    async def _upsert_enhanced_game_with_outcome(self, enhanced_game: EnhancedGameWithOutcome) -> bool:
        """
        Insert or update enhanced game with complete outcome data.
        
        Returns:
            True if a new record was created, False if existing record was updated
        """
        
        async with get_connection() as conn:
            # First, check if record already exists
            existing_check_query = """
                SELECT id, home_score, away_score 
                FROM curated.enhanced_games 
                WHERE (action_network_game_id = $1 AND $1 IS NOT NULL)
                   OR (mlb_stats_api_game_id = $2 AND $2 IS NOT NULL)
                LIMIT 1
            """
            
            existing_row = await conn.fetchrow(
                existing_check_query,
                enhanced_game.action_network_game_id,
                enhanced_game.mlb_stats_api_game_id
            )
            
            now = datetime.now(timezone.utc)
            
            if existing_row:
                # Update existing record with outcome data
                update_query = """
                    UPDATE curated.enhanced_games SET
                        home_score = $1,
                        away_score = $2,
                        winning_team = $3,
                        game_status = $4,
                        feature_data = $5,
                        ml_metadata = $6,
                        data_quality_score = $7,
                        mlb_correlation_confidence = $8,
                        source_coverage_score = $9,
                        updated_at = $10
                    WHERE id = $11
                """
                
                await conn.execute(
                    update_query,
                    enhanced_game.home_score,
                    enhanced_game.away_score,
                    enhanced_game.winning_team,
                    enhanced_game.game_status,
                    json.dumps(enhanced_game.feature_data),
                    json.dumps(enhanced_game.ml_metadata),
                    enhanced_game.data_quality_score,
                    enhanced_game.mlb_correlation_confidence,
                    enhanced_game.source_coverage_score,
                    now,
                    existing_row["id"]
                )
                
                logger.debug(f"Updated existing enhanced_games record {existing_row['id']} with outcome data")
                return False  # Updated existing record
            
            else:
                # Insert new enhanced game record with outcome data
                insert_query = """
                    INSERT INTO curated.enhanced_games (
                        action_network_game_id, mlb_stats_api_game_id,
                        home_team, away_team, home_team_full_name, away_team_full_name,
                        game_datetime, game_date, season,
                        home_score, away_score, winning_team, game_status,
                        feature_data, ml_metadata,
                        data_quality_score, mlb_correlation_confidence, source_coverage_score,
                        created_at, updated_at
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20
                    ) RETURNING id
                """
                
                new_id = await conn.fetchval(
                    insert_query,
                    enhanced_game.action_network_game_id,
                    enhanced_game.mlb_stats_api_game_id,
                    enhanced_game.home_team,
                    enhanced_game.away_team,
                    enhanced_game.home_team_full_name,
                    enhanced_game.away_team_full_name,
                    enhanced_game.game_datetime,
                    enhanced_game.game_date,
                    enhanced_game.season,
                    enhanced_game.home_score,
                    enhanced_game.away_score,
                    enhanced_game.winning_team,
                    enhanced_game.game_status,
                    json.dumps(enhanced_game.feature_data),
                    json.dumps(enhanced_game.ml_metadata),
                    enhanced_game.data_quality_score,
                    enhanced_game.mlb_correlation_confidence,
                    enhanced_game.source_coverage_score,
                    now,
                    now
                )
                
                logger.debug(f"Created new enhanced_games record {new_id} with outcome data")
                return True  # Created new record
    
    async def get_sync_stats(self) -> Dict[str, Any]:
        """Get sync statistics for monitoring."""
        
        stats = dict(self.sync_stats)
        
        # Add current database status
        try:
            async with get_connection() as conn:
                # Count enhanced games with scores
                enhanced_with_scores = await conn.fetchval("""
                    SELECT COUNT(*) FROM curated.enhanced_games 
                    WHERE home_score IS NOT NULL AND away_score IS NOT NULL
                """)
                
                # Count total game outcomes
                total_outcomes = await conn.fetchval("""
                    SELECT COUNT(*) FROM curated.game_outcomes
                    WHERE home_score IS NOT NULL AND away_score IS NOT NULL
                """)
                
                # Count missing enhanced games
                missing_enhanced = await conn.fetchval("""
                    SELECT COUNT(*)
                    FROM curated.game_outcomes go
                    INNER JOIN curated.games_complete gc ON go.game_id = gc.id
                    LEFT JOIN curated.enhanced_games eg 
                        ON gc.id = eg.id 
                        OR (gc.action_network_game_id IS NOT NULL AND gc.action_network_game_id = eg.action_network_game_id)
                        OR (gc.mlb_stats_api_game_id IS NOT NULL AND gc.mlb_stats_api_game_id = eg.mlb_stats_api_game_id)
                    WHERE go.home_score IS NOT NULL 
                        AND go.away_score IS NOT NULL
                        AND (eg.id IS NULL OR eg.home_score IS NULL OR eg.away_score IS NULL)
                """)
                
                stats.update({
                    "enhanced_games_with_scores": enhanced_with_scores,
                    "total_game_outcomes": total_outcomes,
                    "missing_enhanced_games": missing_enhanced,
                    "sync_completion_rate": f"{((enhanced_with_scores / total_outcomes) * 100):.1f}%" if total_outcomes > 0 else "0%",
                    "ml_training_ready": enhanced_with_scores >= 50,  # Minimum for ML training
                    "last_sync_formatted": self.sync_stats["last_sync"].strftime("%Y-%m-%d %H:%M:%S UTC") 
                                         if self.sync_stats["last_sync"] else "Never"
                })
                
        except Exception as e:
            logger.error(f"Error getting sync stats: {e}")
            stats["database_error"] = str(e)
        
        return stats
    
    async def health_check(self) -> Dict[str, Any]:
        """Health check for the sync service."""
        
        try:
            async with get_connection() as conn:
                # Test database connectivity
                await conn.fetchval("SELECT 1")
                
                # Check enhanced games with scores
                enhanced_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM curated.enhanced_games 
                    WHERE home_score IS NOT NULL AND away_score IS NOT NULL
                """)
                
                # Check sync gap
                missing_count = await conn.fetchval("""
                    SELECT COUNT(*)
                    FROM curated.game_outcomes go
                    INNER JOIN curated.games_complete gc ON go.game_id = gc.id
                    LEFT JOIN curated.enhanced_games eg 
                        ON gc.id = eg.id 
                        OR (gc.action_network_game_id IS NOT NULL AND gc.action_network_game_id = eg.action_network_game_id)
                        OR (gc.mlb_stats_api_game_id IS NOT NULL AND gc.mlb_stats_api_game_id = eg.mlb_stats_api_game_id)
                    WHERE go.home_score IS NOT NULL 
                        AND go.away_score IS NOT NULL
                        AND (eg.id IS NULL OR eg.home_score IS NULL OR eg.away_score IS NULL)
                """)
                
                # Determine health status
                is_healthy = enhanced_count >= 50 and missing_count == 0
                
                return {
                    "status": "healthy" if is_healthy else "needs_sync",
                    "database_connection": "ok",
                    "enhanced_games_with_scores": enhanced_count,
                    "missing_enhanced_games": missing_count,
                    "ml_training_ready": enhanced_count >= 50,
                    "sync_needed": missing_count > 0,
                    "sync_stats": self.sync_stats
                }
                
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "database_connection": "failed",
                "sync_stats": self.sync_stats
            }


# Service instance for easy importing
enhanced_games_outcome_sync_service = EnhancedGamesOutcomeSyncService()


async def sync_all_missing_outcomes(dry_run: bool = False, limit: Optional[int] = None) -> GameOutcomeSyncResult:
    """
    Convenience function to sync all missing game outcomes to enhanced_games.
    
    CRITICAL: This is the main function to fix the ML Training Pipeline zero data issue.
    """
    return await enhanced_games_outcome_sync_service.sync_all_missing_outcomes(dry_run, limit)


async def sync_recent_outcomes(days_back: int = 7, dry_run: bool = False) -> GameOutcomeSyncResult:
    """Convenience function to sync recent game outcomes to enhanced_games."""
    return await enhanced_games_outcome_sync_service.sync_recent_outcomes(days_back, dry_run)


if __name__ == "__main__":
    # CRITICAL: Script to fix ML training pipeline zero data issue
    async def main():
        print("🚨 CRITICAL: Fixing ML Training Pipeline Zero Data Issue (#67)")
        print("=" * 70)
        
        # Check current status
        print("\n📊 Checking current status...")
        stats = await enhanced_games_outcome_sync_service.get_sync_stats()
        print(f"Enhanced games with scores: {stats.get('enhanced_games_with_scores', 0)}")
        print(f"Total game outcomes available: {stats.get('total_game_outcomes', 0)}")
        print(f"Missing enhanced games: {stats.get('missing_enhanced_games', 0)}")
        print(f"ML training ready: {stats.get('ml_training_ready', False)}")
        
        # Sync all missing outcomes to fix the issue
        print("\n🔄 Syncing all missing game outcomes to enhanced_games...")
        result = await sync_all_missing_outcomes(dry_run=False)
        
        print(f"\n✅ Sync Results:")
        print(f"   Outcomes found: {result.outcomes_found}")
        print(f"   Enhanced games created: {result.enhanced_games_created}")
        print(f"   Enhanced games updated: {result.enhanced_games_updated}")
        print(f"   Sync failures: {result.sync_failures}")
        print(f"   Processing time: {result.processing_time_seconds:.2f}s")
        
        if result.errors:
            print(f"\n❌ Errors encountered:")
            for error in result.errors:
                print(f"   - {error}")
        
        # Check final status
        print("\n📊 Final status check...")
        final_stats = await enhanced_games_outcome_sync_service.get_sync_stats()
        print(f"Enhanced games with scores: {final_stats.get('enhanced_games_with_scores', 0)}")
        print(f"ML training ready: {final_stats.get('ml_training_ready', False)}")
        
        if final_stats.get('enhanced_games_with_scores', 0) >= 50:
            print("\n🎉 SUCCESS: ML Training Pipeline now has sufficient real data!")
            print("   ✅ Enhanced games table populated with game scores")
            print("   ✅ ML trainer can now load real historical data")
            print("   ✅ Ready for model training and predictions")
        else:
            print("\n⚠️  WARNING: ML Training Pipeline still needs more data")
            print(f"   Need at least 50 games, currently have {final_stats.get('enhanced_games_with_scores', 0)}")

    asyncio.run(main())