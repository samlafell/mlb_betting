"""
Database Coordinator Service for Multi-Process DuckDB Access

This service provides process-level coordination to handle DuckDB's single-writer limitation.

MIGRATION NOTE: This coordinator now supports both the legacy file-locking approach
and the new optimized connection pooling approach. Set use_optimized=True to use
the new high-performance architecture.
"""

import fcntl
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, List, Optional, Union
import structlog

from ..db.connection import get_db_manager, DatabaseManager
from ..core.exceptions import DatabaseError

logger = structlog.get_logger(__name__)


class DatabaseCoordinator:
    """
    Database coordinator that supports both legacy file-locking and optimized approaches.
    
    MIGRATION: Set use_optimized=True to enable the new high-performance architecture
    with connection pooling, batched writes, and lock-free reads.
    """
    
    def __init__(self, lock_timeout: float = 30.0, use_optimized: bool = False):
        self.lock_timeout = lock_timeout
        self.use_optimized = use_optimized
        self.lock_file_path = Path("data/raw/duckdb_coordinator.lock")
        self.db_manager = None
        
        # Initialize optimized adapter if requested
        if self.use_optimized:
            self._init_optimized_adapter()
        else:
            # Ensure lock directory exists for legacy mode
            self.lock_file_path.parent.mkdir(parents=True, exist_ok=True)
            logger.info("DatabaseCoordinator using legacy file-locking mode")
    
    def _init_optimized_adapter(self):
        """Initialize the optimized database adapter"""
        try:
            from .database_service_adapter import get_database_service_adapter
            self._optimized_adapter = get_database_service_adapter()
            logger.info("DatabaseCoordinator using optimized connection pooling mode")
        except ImportError as e:
            logger.error("Failed to import optimized adapter, falling back to legacy mode", error=str(e))
            self.use_optimized = False
            self.lock_file_path.parent.mkdir(parents=True, exist_ok=True)
    
    @contextmanager
    def _get_exclusive_lock(self, timeout: float = None):
        """Get exclusive file lock for database access"""
        timeout = timeout or self.lock_timeout
        lock_file = None
        
        try:
            lock_file = open(self.lock_file_path, 'w')
            start_time = time.time()
            
            while time.time() - start_time < timeout:
                try:
                    # Try non-blocking exclusive lock
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    logger.debug("Acquired database lock")
                    yield lock_file
                    return
                except IOError:
                    # Lock held by another process, wait briefly
                    time.sleep(0.05)
                    continue
            
            raise DatabaseError(f"Failed to acquire database lock within {timeout}s")
            
        except Exception as e:
            if "Failed to acquire" not in str(e):
                logger.error("Error acquiring database lock", error=str(e))
            raise
        finally:
            if lock_file:
                try:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                    lock_file.close()
                    logger.debug("Released database lock")
                except:
                    pass
    
    def _get_db_manager(self) -> DatabaseManager:
        """Get database manager, creating if needed within lock context"""
        # Always create a fresh database manager within lock context
        # to avoid cross-process connection issues
        return get_db_manager()
    
    def execute_read(
        self, 
        query: str, 
        parameters: Optional[tuple] = None,
        timeout: float = 30.0
    ) -> Optional[List[tuple]]:
        """Execute a read operation using optimized or legacy approach"""
        if self.use_optimized:
            # Use optimized lock-free read
            try:
                return self._optimized_adapter.execute_read(query, parameters, timeout)
            except Exception as e:
                logger.error("Optimized read failed, falling back to legacy", error=str(e))
                # Fall through to legacy approach
        
        # Legacy file-locking approach
        try:
            # Reads can use shorter timeout since they're less critical
            with self._get_exclusive_lock(timeout=min(timeout, 10.0)):
                db_manager = self._get_db_manager()
                result = db_manager.execute_query(query, parameters, fetch=True)
                logger.debug("Read operation completed", query_length=len(query))
                return result
                
        except Exception as e:
            logger.error("Read operation failed", error=str(e), query=query[:100])
            # For reads, fallback to direct connection with retry
            return self._fallback_read(query, parameters)
    
    def execute_write(
        self, 
        query: str, 
        parameters: Optional[tuple] = None,
        timeout: float = 60.0
    ) -> Any:
        """Execute a write operation using optimized or legacy approach"""
        if self.use_optimized:
            # Use optimized batched write
            try:
                return self._optimized_adapter.execute_write(query, parameters, timeout)
            except Exception as e:
                logger.error("Optimized write failed, falling back to legacy", error=str(e))
                # Fall through to legacy approach
        
        # Legacy file-locking approach
        try:
            with self._get_exclusive_lock(timeout=timeout):
                db_manager = self._get_db_manager()
                result = db_manager.execute_query(query, parameters, fetch=False)
                logger.debug("Write operation completed", query_length=len(query))
                return result
                
        except Exception as e:
            logger.error("Write operation failed", error=str(e), query=query[:100])
            raise DatabaseError(f"Coordinated write failed: {e}")
    
    def execute_bulk_insert(
        self,
        query: str,
        parameters_list: List[tuple],
        timeout: float = 300.0
    ) -> str:
        """Execute a bulk insert operation using optimized or legacy approach"""
        if self.use_optimized:
            # Use optimized batch insert
            try:
                return self._optimized_adapter.execute_bulk_insert(query, parameters_list, timeout)
            except Exception as e:
                logger.error("Optimized bulk insert failed, falling back to legacy", error=str(e))
                # Fall through to legacy approach
        
        # Legacy file-locking approach
        try:
            with self._get_exclusive_lock(timeout=timeout):
                db_manager = self._get_db_manager()
                db_manager.execute_many(query, parameters_list)
                logger.info("Bulk insert completed", rows=len(parameters_list))
                return f"Bulk insert completed: {len(parameters_list)} rows"
                
        except Exception as e:
            logger.error("Bulk insert failed", error=str(e), rows=len(parameters_list))
            raise DatabaseError(f"Coordinated bulk insert failed: {e}")
    
    def _fallback_read(self, query: str, parameters: Optional[tuple] = None, max_retries: int = 3) -> Optional[List[tuple]]:
        """Fallback read with exponential backoff for high availability"""
        for attempt in range(max_retries):
            try:
                # Short delay with exponential backoff
                if attempt > 0:
                    delay = 0.1 * (2 ** attempt)
                    time.sleep(delay)
                
                # Try direct database connection
                temp_db = DatabaseManager()
                result = temp_db.execute_query(query, parameters, fetch=True)
                logger.warning("Fallback read succeeded", attempt=attempt + 1)
                return result
                
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error("All fallback read attempts failed", error=str(e))
                    raise DatabaseError(f"Read operation failed after {max_retries} attempts: {e}")
                continue
        
        return None
    
    def start(self):
        """Start coordinator"""
        if self.use_optimized:
            # Optimized adapter starts automatically on first use
            logger.info("Optimized database coordinator ready")
        else:
            logger.info("File-based database coordinator ready")
        
    def stop(self):
        """Stop coordinator and cleanup"""
        if self.use_optimized and hasattr(self, '_optimized_adapter'):
            try:
                self._optimized_adapter.stop()
                logger.info("Optimized coordinator stopped")
            except Exception as e:
                logger.warning("Error stopping optimized adapter", error=str(e))
        
        if self.lock_file_path.exists():
            try:
                self.lock_file_path.unlink()
                logger.info("Coordinator cleanup completed")
            except:
                pass
    
    def is_healthy(self) -> bool:
        """Check if coordinator is healthy"""
        if self.use_optimized and hasattr(self, '_optimized_adapter'):
            return self._optimized_adapter.is_healthy()
        return True  # File-based approach is always healthy
    
    def get_queue_size(self) -> int:
        """Get queue size"""
        if self.use_optimized and hasattr(self, '_optimized_adapter'):
            return self._optimized_adapter.get_queue_size()
        return 0  # File-based approach has no queue
    
    def get_performance_stats(self) -> dict:
        """Get performance statistics"""
        if self.use_optimized and hasattr(self, '_optimized_adapter'):
            return self._optimized_adapter.get_performance_stats()
        return {
            "mode": "legacy_file_locking",
            "read_pool_size": 1,
            "write_queue_size": 0,
            "status": "active"
        }


# Global coordinator instance
_coordinator: Optional[DatabaseCoordinator] = None


def get_database_coordinator() -> DatabaseCoordinator:
    """Get the global database coordinator instance"""
    global _coordinator
    
    if _coordinator is None:
        _coordinator = DatabaseCoordinator()
        logger.info("File-based database coordinator initialized")
    
    return _coordinator


def shutdown_coordinator():
    """Shutdown the global coordinator"""
    global _coordinator
    
    if _coordinator:
        _coordinator.stop()
        _coordinator = None


# Context manager for easy use
@contextmanager
def coordinated_database_access():
    """Context manager for coordinated database access"""
    coordinator = get_database_coordinator()
    yield coordinator


# Convenience functions that match existing DatabaseManager interface
def execute_coordinated_query(
    query: str, 
    parameters: Optional[tuple] = None,
    fetch: bool = True
) -> Optional[List[tuple]]:
    """Execute query through coordinator"""
    coordinator = get_database_coordinator()
    
    if fetch:
        return coordinator.execute_read(query, parameters)
    else:
        return coordinator.execute_write(query, parameters)


def execute_coordinated_many(
    query: str,
    parameters_list: List[tuple]
) -> str:
    """Execute bulk insert through coordinator"""
    coordinator = get_database_coordinator()
    return coordinator.execute_bulk_insert(query, parameters_list) 