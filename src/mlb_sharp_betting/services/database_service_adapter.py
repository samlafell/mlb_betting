"""
Database Service Adapter

This module provides a drop-in replacement for the current DatabaseCoordinator
that uses the optimized DuckDB connection manager under the hood.

Maintains backward compatibility while leveraging the new architecture:
- Single writer + read pool strategy
- Batched write operations
- Lock-free read operations
"""

import asyncio
import threading
from typing import Any, List, Optional, Dict
from contextlib import contextmanager
import structlog

from ..db.optimized_connection import (
    OptimizedDatabaseManager,
    ConnectionConfig,
    OperationPriority,
    get_optimized_db_manager
)
from ..core.exceptions import DatabaseError

logger = structlog.get_logger(__name__)


class DatabaseServiceAdapter:
    """
    Adapter that provides the same interface as the current DatabaseCoordinator
    but uses the optimized connection manager internally.
    
    This enables a seamless transition without changing existing code.
    """
    
    def __init__(self, config: Optional[ConnectionConfig] = None):
        """
        Initialize the adapter with optional configuration.
        
        Args:
            config: Optional connection configuration. If None, uses defaults
                   optimized for the betting pipeline.
        """
        # Use optimized config for betting pipeline if none provided
        if config is None:
            config = ConnectionConfig(
                read_pool_size=8,           # Parallel reads for analysis
                write_batch_size=500,       # Batch size for efficiency
                write_batch_timeout=2.0,    # Quick batching for real-time
                enable_wal_mode=True        # Concurrent access
            )
        
        self.config = config
        self._optimized_manager: Optional[OptimizedDatabaseManager] = None
        self._event_loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_thread: Optional[threading.Thread] = None
        self._initialization_lock = threading.Lock()
        self._initialized = False

    def _ensure_event_loop(self) -> None:
        """Ensure there's an event loop running for async operations"""
        if self._event_loop is not None and not self._event_loop.is_closed():
            return
            
        with self._initialization_lock:
            if self._event_loop is not None and not self._event_loop.is_closed():
                return
                
            # Create new event loop in dedicated thread
            self._event_loop = asyncio.new_event_loop()
            
            def run_loop():
                asyncio.set_event_loop(self._event_loop)
                self._event_loop.run_forever()
            
            self._loop_thread = threading.Thread(target=run_loop, daemon=True)
            self._loop_thread.start()
            
            logger.info("Event loop started for database service adapter")

    def _get_optimized_manager(self) -> OptimizedDatabaseManager:
        """Get or create the optimized database manager"""
        if self._optimized_manager is None:
            self._ensure_event_loop()
            
            # Initialize the manager asynchronously
            future = asyncio.run_coroutine_threadsafe(
                get_optimized_db_manager(self.config),
                self._event_loop
            )
            self._optimized_manager = future.result(timeout=30.0)
            self._initialized = True
            
        return self._optimized_manager

    def execute_read(
        self, 
        query: str, 
        parameters: Optional[tuple] = None,
        timeout: float = 30.0
    ) -> Optional[List[tuple]]:
        """
        Execute a read operation (compatible with old interface).
        
        Args:
            query: SQL query to execute
            parameters: Optional query parameters
            timeout: Operation timeout (for compatibility, not enforced)
            
        Returns:
            Query results as list of tuples
        """
        try:
            manager = self._get_optimized_manager()
            
            # Execute async operation in the background event loop
            future = asyncio.run_coroutine_threadsafe(
                manager.execute_read(query, parameters),
                self._event_loop
            )
            
            result = future.result(timeout=timeout)
            logger.debug("Read operation completed via adapter", query_length=len(query))
            return result
            
        except Exception as e:
            logger.error("Adapter read operation failed", error=str(e), query=query[:100])
            raise DatabaseError(f"Read operation failed: {e}")

    def execute_write(
        self, 
        query: str, 
        parameters: Optional[tuple] = None,
        timeout: float = 60.0
    ) -> Any:
        """
        Execute a write operation (compatible with old interface).
        
        Args:
            query: SQL query to execute
            parameters: Optional query parameters  
            timeout: Operation timeout
            
        Returns:
            Write operation result
        """
        try:
            manager = self._get_optimized_manager()
            
            # Determine priority based on query type
            priority = OperationPriority.NORMAL
            query_lower = query.lower().strip()
            
            if any(keyword in query_lower for keyword in ['create table', 'drop table', 'alter table']):
                priority = OperationPriority.HIGH
            elif 'pre_game' in query_lower or 'critical' in query_lower:
                priority = OperationPriority.CRITICAL
            
            # For single writes, use immediate execution to avoid batching delays
            # This provides better performance for small operations
            future = asyncio.run_coroutine_threadsafe(
                manager.execute_immediate_write(query, parameters),
                self._event_loop
            )
            
            result = future.result(timeout=timeout)
            logger.debug("Write operation completed via adapter", query_length=len(query))
            return result
            
        except Exception as e:
            logger.error("Adapter write operation failed", error=str(e), query=query[:100])
            raise DatabaseError(f"Write operation failed: {e}")

    def execute_bulk_insert(
        self,
        query: str,
        parameters_list: List[tuple],
        timeout: float = 300.0
    ) -> str:
        """
        Execute a bulk insert operation (compatible with old interface).
        
        Args:
            query: INSERT query template
            parameters_list: List of parameter tuples
            timeout: Operation timeout
            
        Returns:
            Success message
        """
        try:
            manager = self._get_optimized_manager()
            
            # Execute bulk operation via the write queue
            future = asyncio.run_coroutine_threadsafe(
                self._execute_bulk_via_queue(manager, query, parameters_list),
                self._event_loop
            )
            
            result = future.result(timeout=timeout)
            logger.info("Bulk operation completed via adapter", rows=len(parameters_list))
            return result
            
        except Exception as e:
            logger.error("Adapter bulk operation failed", error=str(e), rows=len(parameters_list))
            raise DatabaseError(f"Bulk operation failed: {e}")

    async def _execute_bulk_via_queue(
        self,
        manager: OptimizedDatabaseManager,
        query: str,
        parameters_list: List[tuple]
    ) -> str:
        """Execute bulk operation via the write queue"""
        # Create a single write operation with all parameters
        from ..db.optimized_connection import WriteOperation
        
        future = asyncio.Future()
        operation = WriteOperation(
            query=query,
            parameters=parameters_list,
            priority=OperationPriority.NORMAL,
            future=future,
            timestamp=asyncio.get_event_loop().time(),
            is_batch=True
        )
        
        # Add to write queue
        async with manager._write_queue_lock:
            if len(manager._write_queue) >= manager.config.max_queue_size:
                raise DatabaseError("Write queue is full")
            manager._write_queue.append(operation)
        
        await future
        return f"Bulk operation completed: {len(parameters_list)} rows"

    def execute_immediate_write(
        self,
        query: str,
        parameters: Optional[tuple] = None,
        timeout: float = 60.0
    ) -> Any:
        """
        Execute an immediate write operation bypassing the batch queue.
        
        Use this for critical operations that can't wait for batching.
        
        Args:
            query: SQL query to execute
            parameters: Optional query parameters
            timeout: Operation timeout
            
        Returns:
            Write operation result
        """
        try:
            manager = self._get_optimized_manager()
            
            future = asyncio.run_coroutine_threadsafe(
                manager.execute_immediate_write(query, parameters),
                self._event_loop
            )
            
            result = future.result(timeout=timeout)
            logger.debug("Immediate write completed via adapter", query_length=len(query))
            return result
            
        except Exception as e:
            logger.error("Adapter immediate write failed", error=str(e), query=query[:100])
            raise DatabaseError(f"Immediate write failed: {e}")

    def start(self) -> None:
        """Start the service (compatible with old interface)"""
        try:
            self._get_optimized_manager()
            logger.info("Database service adapter started with optimized manager")
        except Exception as e:
            logger.error("Failed to start database service adapter", error=str(e))
            raise

    def stop(self) -> None:
        """Stop the service and cleanup"""
        if self._optimized_manager:
            try:
                future = asyncio.run_coroutine_threadsafe(
                    self._optimized_manager.cleanup(),
                    self._event_loop
                )
                future.result(timeout=30.0)
            except Exception as e:
                logger.warning("Error during optimized manager cleanup", error=str(e))
        
        if self._event_loop and not self._event_loop.is_closed():
            self._event_loop.call_soon_threadsafe(self._event_loop.stop)
            
        if self._loop_thread and self._loop_thread.is_alive():
            self._loop_thread.join(timeout=5.0)
        
        logger.info("Database service adapter stopped")

    def is_healthy(self) -> bool:
        """Check if the service is healthy (compatible with old interface)"""
        if not self._initialized or self._optimized_manager is None:
            return False
            
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._optimized_manager.health_check(),
                self._event_loop
            )
            return future.result(timeout=5.0)
        except Exception as e:
            logger.warning("Health check failed", error=str(e))
            return False

    def get_queue_size(self) -> int:
        """Get current write queue size"""
        if not self._initialized or self._optimized_manager is None:
            return 0
            
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._optimized_manager.get_stats(),
                self._event_loop
            )
            stats = future.result(timeout=5.0)
            return stats.get('write_queue_size', 0)
        except Exception:
            return 0

    def get_performance_stats(self) -> Dict[str, Any]:
        """Get detailed performance statistics"""
        if not self._initialized or self._optimized_manager is None:
            return {
                "status": "not_initialized",
                "read_pool_size": 0,
                "write_queue_size": 0
            }
            
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._optimized_manager.get_stats(),
                self._event_loop
            )
            stats = future.result(timeout=5.0)
            stats["status"] = "active"
            return stats
        except Exception as e:
            logger.warning("Failed to get performance stats", error=str(e))
            return {
                "status": "error",
                "error": str(e),
                "read_pool_size": 0,
                "write_queue_size": 0
            }


# Global adapter instance
_adapter: Optional[DatabaseServiceAdapter] = None


def get_database_service_adapter(config: Optional[ConnectionConfig] = None) -> DatabaseServiceAdapter:
    """Get the global database service adapter instance"""
    global _adapter
    
    if _adapter is None:
        _adapter = DatabaseServiceAdapter(config)
        _adapter.start()
        logger.info("Database service adapter initialized")
    
    return _adapter


def shutdown_adapter() -> None:
    """Shutdown the global adapter"""
    global _adapter
    
    if _adapter:
        _adapter.stop()
        _adapter = None


# Context manager for easy use (compatible with old interface)
@contextmanager
def optimized_database_access():
    """Context manager for optimized database access"""
    adapter = get_database_service_adapter()
    yield adapter


# Convenience functions that match existing DatabaseCoordinator interface
def execute_optimized_read(
    query: str, 
    parameters: Optional[tuple] = None,
    timeout: float = 30.0
) -> Optional[List[tuple]]:
    """Execute read query through optimized adapter"""
    adapter = get_database_service_adapter()
    return adapter.execute_read(query, parameters, timeout)


def execute_optimized_write(
    query: str, 
    parameters: Optional[tuple] = None,
    timeout: float = 60.0
) -> Any:
    """Execute write query through optimized adapter"""
    adapter = get_database_service_adapter()
    return adapter.execute_write(query, parameters, timeout)


def execute_optimized_bulk_insert(
    query: str,
    parameters_list: List[tuple],
    timeout: float = 300.0
) -> str:
    """Execute bulk insert through optimized adapter"""
    adapter = get_database_service_adapter()
    return adapter.execute_bulk_insert(query, parameters_list, timeout) 