#!/usr/bin/env python3
"""
Optimized Game Resolution Service

High-performance singleton service for MLB Stats API game ID resolution with intelligent caching.
Eliminates redundant API calls by implementing batch processing and multi-level caching.
"""

import asyncio
from datetime import datetime, date
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass
from collections import defaultdict
import structlog

from ..data.collection.base import DataSource
from ..services.mlb_stats_api_game_resolution_service import MLBStatsAPIGameResolutionService, GameMatchResult

logger = structlog.get_logger(__name__)


@dataclass
class GameResolutionRequest:
    """Request for game ID resolution."""
    external_game_id: str
    home_team: Optional[str] = None
    away_team: Optional[str] = None
    game_date: Optional[date] = None
    source: DataSource = DataSource.ACTION_NETWORK


class OptimizedGameResolutionService:
    """
    Singleton service for efficient MLB Stats API game ID resolution.
    
    Key Features:
    - Single service instance shared across all processors
    - Batch processing to resolve multiple games in one operation
    - Multi-level caching (memory + database + session)
    - Intelligent deduplication to prevent redundant API calls
    """
    
    _instance: Optional['OptimizedGameResolutionService'] = None
    _lock = asyncio.Lock()
    
    def __init__(self):
        self.logger = logger.bind(component="OptimizedGameResolutionService")
        self._underlying_service: Optional[MLBStatsAPIGameResolutionService] = None
        self._initialized = False
        
        # Multi-level cache
        self._memory_cache: Dict[str, str] = {}  # external_game_id -> mlb_stats_api_game_id
        self._session_cache: Dict[str, str] = {}  # temporary cache for current session
        self._pending_resolutions: Dict[str, asyncio.Future] = {}  # avoid duplicate concurrent requests
        
        # Statistics
        self.stats = {
            "cache_hits": 0,
            "cache_misses": 0,
            "api_calls_avoided": 0,
            "batch_operations": 0,
            "games_resolved": 0
        }
    
    @classmethod
    async def get_instance(cls) -> 'OptimizedGameResolutionService':
        """Get the singleton instance, creating it if necessary."""
        if cls._instance is None:
            async with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
                    await cls._instance.initialize()
        return cls._instance
    
    async def initialize(self):
        """Initialize the underlying MLB Stats API service (once)."""
        if self._initialized:
            return
        
        self.logger.info("Initializing optimized game resolution service")
        
        try:
            self._underlying_service = MLBStatsAPIGameResolutionService()
            await self._underlying_service.initialize()
            
            # Pre-load any existing game mappings from database
            await self._load_database_cache()
            
            self._initialized = True
            self.logger.info("Optimized game resolution service initialized successfully",
                           cache_size=len(self._memory_cache))
            
        except Exception as e:
            self.logger.error("Failed to initialize optimized game resolution service", error=str(e))
            raise
    
    async def _load_database_cache(self):
        """Load existing game ID mappings from database into memory cache."""
        try:
            # This would query the database for existing mappings
            # For now, we'll implement a simple version
            self.logger.debug("Loading database cache for game ID mappings")
            # TODO: Implement database cache loading
            pass
        except Exception as e:
            self.logger.warning("Failed to load database cache", error=str(e))
    
    async def resolve_game_id(self, external_game_id: str, home_team: str = None, 
                            away_team: str = None, game_date: date = None,
                            source: DataSource = DataSource.ACTION_NETWORK) -> Optional[str]:
        """
        Resolve a single game ID with caching.
        
        This method is optimized for individual calls and includes deduplication
        for concurrent requests for the same game.
        """
        # Check memory cache first
        if external_game_id in self._memory_cache:
            self.stats["cache_hits"] += 1
            self.logger.debug("Cache hit for game ID", external_game_id=external_game_id,
                            mlb_game_id=self._memory_cache[external_game_id])
            return self._memory_cache[external_game_id]
        
        # Check if this game is already being resolved
        if external_game_id in self._pending_resolutions:
            self.logger.debug("Waiting for concurrent resolution", external_game_id=external_game_id)
            try:
                return await self._pending_resolutions[external_game_id]
            except Exception:
                # If the pending resolution failed, we'll try again
                pass
        
        # Create a future for this resolution to prevent duplicates
        future = asyncio.Future()
        self._pending_resolutions[external_game_id] = future
        
        try:
            # Perform the actual resolution
            result = await self._resolve_single_game(external_game_id, home_team, away_team, game_date, source)
            
            # Cache the result
            if result:
                self._memory_cache[external_game_id] = result
                self._session_cache[external_game_id] = result
                self.stats["games_resolved"] += 1
            
            self.stats["cache_misses"] += 1
            
            # Complete the future
            future.set_result(result)
            return result
            
        except Exception as e:
            future.set_exception(e)
            raise
        finally:
            # Clean up the pending resolution
            self._pending_resolutions.pop(external_game_id, None)
    
    async def batch_resolve_games(self, requests: List[GameResolutionRequest]) -> Dict[str, Optional[str]]:
        """
        Efficiently resolve multiple games in a single batch operation.
        
        This is the primary optimization - group requests by unique games and resolve once.
        """
        if not requests:
            return {}
        
        self.stats["batch_operations"] += 1
        self.logger.info("Starting batch game resolution", total_requests=len(requests))
        
        # Phase 1: Group by unique external game IDs
        unique_games = {}
        all_game_ids = []
        
        for request in requests:
            all_game_ids.append(request.external_game_id)
            if request.external_game_id not in unique_games:
                unique_games[request.external_game_id] = request
        
        self.logger.info("Batch deduplication completed", 
                        total_requests=len(requests),
                        unique_games=len(unique_games),
                        duplicates_avoided=len(requests) - len(unique_games))
        
        # Phase 2: Check cache for all unique games
        results = {}
        uncached_requests = []
        
        for external_game_id, request in unique_games.items():
            if external_game_id in self._memory_cache:
                results[external_game_id] = self._memory_cache[external_game_id]
                self.stats["cache_hits"] += 1
            else:
                uncached_requests.append(request)
                self.stats["cache_misses"] += 1
        
        self.logger.info("Cache lookup completed",
                        cache_hits=len(results),
                        cache_misses=len(uncached_requests))
        
        # Phase 3: Resolve uncached games
        if uncached_requests:
            self.logger.info("Resolving uncached games", count=len(uncached_requests))
            
            for request in uncached_requests:
                try:
                    mlb_game_id = await self._resolve_single_game(
                        request.external_game_id,
                        request.home_team,
                        request.away_team,
                        request.game_date,
                        request.source
                    )
                    
                    results[request.external_game_id] = mlb_game_id
                    
                    if mlb_game_id:
                        self._memory_cache[request.external_game_id] = mlb_game_id
                        self._session_cache[request.external_game_id] = mlb_game_id
                        self.stats["games_resolved"] += 1
                        
                except Exception as e:
                    self.logger.error("Failed to resolve game", 
                                    external_game_id=request.external_game_id, 
                                    error=str(e))
                    results[request.external_game_id] = None
        
        # Phase 4: Calculate API calls saved
        api_calls_saved = len(all_game_ids) - len(unique_games) - len(uncached_requests)
        self.stats["api_calls_avoided"] += api_calls_saved
        
        self.logger.info("Batch resolution completed",
                        total_requests=len(requests),
                        unique_games=len(unique_games),
                        api_calls_made=len(uncached_requests),
                        api_calls_saved=api_calls_saved,
                        success_rate=f"{len([r for r in results.values() if r]) / len(results) * 100:.1f}%")
        
        return results
    
    async def _resolve_single_game(self, external_game_id: str, home_team: str = None,
                                 away_team: str = None, game_date: date = None,
                                 source: DataSource = DataSource.ACTION_NETWORK) -> Optional[str]:
        """Resolve a single game using the underlying service."""
        if not self._underlying_service:
            raise RuntimeError("Service not initialized")
        
        try:
            result = await self._underlying_service.resolve_game_id(
                external_id=external_game_id,
                source=source,
                home_team=home_team,
                away_team=away_team,
                game_date=game_date
            )
            
            if result and result.mlb_game_id and result.confidence.value != 'NONE':
                self.logger.debug("Successfully resolved game",
                                external_game_id=external_game_id,
                                mlb_game_id=result.mlb_game_id,
                                confidence=result.confidence.value)
                return result.mlb_game_id
            else:
                self.logger.debug("No resolution found",
                                external_game_id=external_game_id,
                                method=result.match_method if result else "service_error")
                return None
                
        except Exception as e:
            self.logger.error("Game resolution error",
                            external_game_id=external_game_id,
                            error=str(e))
            return None
    
    async def cleanup(self):
        """Cleanup resources."""
        if self._underlying_service:
            await self._underlying_service.cleanup()
        
        self.logger.info("Optimized game resolution service cleanup completed",
                        final_stats=self.get_stats())
    
    def get_stats(self) -> Dict[str, Any]:
        """Get performance statistics."""
        total_requests = self.stats["cache_hits"] + self.stats["cache_misses"]
        cache_hit_rate = (self.stats["cache_hits"] / max(total_requests, 1)) * 100
        
        return {
            **self.stats,
            "total_requests": total_requests,
            "cache_hit_rate": f"{cache_hit_rate:.1f}%",
            "memory_cache_size": len(self._memory_cache),
            "session_cache_size": len(self._session_cache)
        }
    
    def clear_session_cache(self):
        """Clear the session cache (keeps memory cache)."""
        self._session_cache.clear()
        self.logger.debug("Session cache cleared")
    
    @classmethod
    async def reset_instance(cls):
        """Reset the singleton instance (for testing)."""
        async with cls._lock:
            if cls._instance:
                await cls._instance.cleanup()
            cls._instance = None


# Convenience functions for easy integration
async def resolve_game_id_optimized(external_game_id: str, home_team: str = None, 
                                  away_team: str = None, game_date: date = None,
                                  source: DataSource = DataSource.ACTION_NETWORK) -> Optional[str]:
    """Convenience function for optimized single game resolution."""
    service = await OptimizedGameResolutionService.get_instance()
    return await service.resolve_game_id(external_game_id, home_team, away_team, game_date, source)


async def batch_resolve_games_optimized(requests: List[GameResolutionRequest]) -> Dict[str, Optional[str]]:
    """Convenience function for optimized batch game resolution."""
    service = await OptimizedGameResolutionService.get_instance()
    return await service.batch_resolve_games(requests)