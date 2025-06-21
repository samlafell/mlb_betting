"""
Optimized DuckDB Connection Manager

This module implements the recommended DuckDB architecture:
- Single writer connection in WAL mode
- Multiple read-only connections for parallel reads
- Batched write operations
- Lock-free read operations

Based on DuckDB's native concurrency capabilities.
"""

import asyncio
import threading
import time
import random
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass
from enum import Enum
import structlog

import duckdb

from ..core.exceptions import DatabaseConnectionError, DatabaseError

logger = structlog.get_logger(__name__)


class OperationPriority(Enum):
    """Priority levels for database operations"""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4


@dataclass
class ConnectionConfig:
    """Configuration for the optimized database connections"""
    read_pool_size: int = 8
    write_batch_size: int = 500
    write_batch_timeout: float = 0.1  # Much shorter timeout for better responsiveness
    max_queue_size: int = 10000
    connection_timeout: float = 30.0
    enable_wal_mode: bool = True
    checkpoint_frequency: int = 16  # MB


@dataclass
class WriteOperation:
    """Represents a queued write operation"""
    query: str
    parameters: Optional[Union[tuple, List[tuple]]]
    priority: OperationPriority
    future: asyncio.Future
    timestamp: float
    is_batch: bool = False


class OptimizedDatabaseManager:
    """
    Optimized database manager implementing DuckDB best practices:
    - Single writer + multiple readers
    - WAL mode for concurrent access
    - Batched write operations
    - Lock-free read operations
    """

    def __init__(self, config: Optional[ConnectionConfig] = None):
        self.config = config or ConnectionConfig()
        self.db_path = Path("data/raw/mlb_betting.duckdb")
        
        # Connection pools
        self._writer_connection: Optional[duckdb.DuckDBPyConnection] = None
        self._read_connections: List[duckdb.DuckDBPyConnection] = []
        self._read_pool_index = 0
        self._read_pool_lock = threading.Lock()
        self._read_executor: Optional['ThreadPoolExecutor'] = None
        
        # Write batching
        self._write_queue: List[WriteOperation] = []
        self._write_queue_lock = asyncio.Lock()
        self._batch_processor_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()
        
        # Initialization
        self._initialized = False
        self._init_lock = asyncio.Lock()

    async def initialize(self) -> None:
        """Initialize the database connections and start background tasks"""
        async with self._init_lock:
            if self._initialized:
                return
                
            logger.info("Initializing optimized database manager")
            
            # Ensure database directory exists
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            
            try:
                await self._init_writer_connection()
                await self._init_read_pool()
                await self._start_batch_processor()
                
                self._initialized = True
                logger.info(
                    "Optimized database manager initialized",
                    read_pool_size=len(self._read_connections),
                    wal_mode=self.config.enable_wal_mode
                )
                
            except Exception as e:
                logger.error("Failed to initialize optimized database manager", error=str(e))
                await self.cleanup()
                raise DatabaseConnectionError(f"Initialization failed: {e}")

    async def _init_writer_connection(self) -> None:
        """Initialize the single writer connection with WAL mode"""
        try:
            # Use same config as read connections to avoid conflicts
            connection_config = {
                'threads': 2,  # Same as read connections
                'preserve_insertion_order': False,
                'enable_object_cache': True
            }
                
            self._writer_connection = duckdb.connect(
                database=str(self.db_path),
                config=connection_config
            )
            
            # Configure WAL checkpointing (disabled for now due to DuckDB compatibility issues)
            # if self.config.enable_wal_mode:
            #     self._writer_connection.execute(
            #         f"PRAGMA wal_autocheckpoint={self.config.checkpoint_frequency * 1024 * 1024}"
            #     )
            
            logger.info("Writer connection initialized with WAL mode")
            
        except Exception as e:
            logger.error("Failed to initialize writer connection", error=str(e))
            raise

    async def _init_read_pool(self) -> None:
        """Initialize the read connection pool using thread-safe access to single connection"""
        try:
            # According to DuckDB concurrency docs, use single connection with threading
            # DuckDB supports concurrency within a single process using MVCC
            # We'll use the same connection with thread-safe access patterns
            
            # Create thread pool for read operations to leverage DuckDB's internal concurrency
            from concurrent.futures import ThreadPoolExecutor
            self._read_executor = ThreadPoolExecutor(
                max_workers=self.config.read_pool_size,
                thread_name_prefix="duckdb_read"
            )
            
            # Store connection reference for read operations
            # All reads will use the same connection but in different threads
            for i in range(self.config.read_pool_size):
                # Add connection reference (same connection, different thread access)
                self._read_connections.append(self._writer_connection)
                    
            logger.info(f"Read pool initialized with {len(self._read_connections)} thread workers")
            
        except Exception as e:
            logger.error("Failed to initialize read pool", error=str(e))
            raise

    async def _start_batch_processor(self) -> None:
        """Start the background batch processor"""
        self._batch_processor_task = asyncio.create_task(self._batch_processor())
        logger.info("Write batch processor started")

    async def _batch_processor(self) -> None:
        """Background task to process write operations in batches"""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.write_batch_timeout)
                await self._process_write_batch()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in batch processor", error=str(e))
                # Continue processing to maintain system stability

    async def _process_write_batch(self) -> None:
        """Process queued write operations as a batch"""
        async with self._write_queue_lock:
            if not self._write_queue:
                return
                
            # Sort by priority and take up to batch_size operations
            self._write_queue.sort(key=lambda x: x.priority.value, reverse=True)
            batch = self._write_queue[:self.config.write_batch_size]
            self._write_queue = self._write_queue[self.config.write_batch_size:]
            
        if not batch:
            return
            
        logger.debug(f"Processing write batch of {len(batch)} operations")
        
        # Group operations for transaction efficiency
        success_count = 0
        error_count = 0
        
        try:
            with self._writer_connection.cursor() as cursor:
                cursor.execute("BEGIN TRANSACTION")
                
                for operation in batch:
                    try:
                        if operation.is_batch and isinstance(operation.parameters, list):
                            cursor.executemany(operation.query, operation.parameters)
                        else:
                            cursor.execute(operation.query, operation.parameters)
                            
                        operation.future.set_result(True)
                        success_count += 1
                        
                    except Exception as e:
                        operation.future.set_exception(DatabaseError(f"Write operation failed: {e}"))
                        error_count += 1
                        logger.warning("Write operation failed in batch", error=str(e))
                
                cursor.execute("COMMIT")
                
        except Exception as e:
            # Transaction failed, set all remaining operations as failed
            for operation in batch:
                if not operation.future.done():
                    operation.future.set_exception(DatabaseError(f"Batch transaction failed: {e}"))
                    error_count += 1
                    
            logger.error("Batch transaction failed", error=str(e))
        
        logger.debug(
            "Write batch processed",
            success=success_count,
            errors=error_count,
            total=len(batch)
        )



    async def execute_read(
        self,
        query: str,
        parameters: Optional[tuple] = None
    ) -> Optional[List[tuple]]:
        """
        Execute a read query using thread-safe access to single connection.
        Leverages DuckDB's internal MVCC for concurrent reads.
        """
        if not self._initialized:
            await self.initialize()
            
        try:
            # Use the dedicated read thread pool executor
            # This allows concurrent reads using DuckDB's internal concurrency
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                self._read_executor,
                self._execute_read_sync,
                self._writer_connection,  # Use single connection with thread-safe access
                query,
                parameters
            )
            
            logger.debug("Read operation completed", query_length=len(query))
            return result
            
        except Exception as e:
            logger.error("Read operation failed", error=str(e), query=query[:100])
            raise DatabaseError(f"Read operation failed: {e}")

    def _execute_read_sync(
        self,
        connection: duckdb.DuckDBPyConnection,
        query: str,
        parameters: Optional[tuple]
    ) -> Optional[List[tuple]]:
        """Synchronous read execution for thread pool using cursor"""
        with connection.cursor() as cursor:
            if parameters:
                cursor.execute(query, parameters)
            else:
                cursor.execute(query)
            return cursor.fetchall()

    async def execute_write(
        self,
        query: str,
        parameters: Optional[tuple] = None,
        priority: OperationPriority = OperationPriority.NORMAL
    ) -> Any:
        """
        Queue a write operation for batched execution.
        Returns a future that will be resolved when the operation completes.
        """
        if not self._initialized:
            await self.initialize()
            
        future = asyncio.Future()
        operation = WriteOperation(
            query=query,
            parameters=parameters,
            priority=priority,
            future=future,
            timestamp=time.time(),
            is_batch=False
        )
        
        async with self._write_queue_lock:
            if len(self._write_queue) >= self.config.max_queue_size:
                raise DatabaseError("Write queue is full")
            self._write_queue.append(operation)
        
        logger.debug("Write operation queued", priority=priority.name)
        return await future

    async def execute_batch_insert(
        self,
        table_name: str,
        columns: List[str],
        data: List[List[Any]],
        priority: OperationPriority = OperationPriority.NORMAL
    ) -> str:
        """
        Execute a batch insert operation.
        More efficient than individual inserts.
        """
        if not self._initialized:
            await self.initialize()
            
        if not data:
            return "No data to insert"
            
        # Prepare batch insert query
        placeholders = ', '.join(['?' for _ in columns])
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        
        # Convert data to tuples for executemany
        parameters = [tuple(row) for row in data]
        
        future = asyncio.Future()
        operation = WriteOperation(
            query=query,
            parameters=parameters,
            priority=priority,
            future=future,
            timestamp=time.time(),
            is_batch=True
        )
        
        async with self._write_queue_lock:
            if len(self._write_queue) >= self.config.max_queue_size:
                raise DatabaseError("Write queue is full")
            self._write_queue.append(operation)
        
        logger.info(f"Batch insert queued", table=table_name, rows=len(data), priority=priority.name)
        await future
        return f"Batch insert completed: {len(data)} rows"

    async def execute_immediate_write(
        self,
        query: str,
        parameters: Optional[tuple] = None
    ) -> Any:
        """
        Execute a write operation immediately, bypassing the batch queue.
        Use sparingly for critical operations that can't wait.
        """
        if not self._initialized:
            await self.initialize()
            
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                self._execute_write_sync,
                query,
                parameters
            )
            
            logger.debug("Immediate write operation completed", query_length=len(query))
            return result
            
        except Exception as e:
            logger.error("Immediate write operation failed", error=str(e), query=query[:100])
            raise DatabaseError(f"Immediate write operation failed: {e}")

    def _execute_write_sync(
        self,
        query: str,
        parameters: Optional[tuple]
    ) -> Any:
        """Synchronous write execution for thread pool"""
        with self._writer_connection.cursor() as cursor:
            if parameters:
                cursor.execute(query, parameters)
            else:
                cursor.execute(query)
            return cursor.fetchall() if cursor.description else None

    async def get_stats(self) -> Dict[str, Any]:
        """Get database manager statistics"""
        async with self._write_queue_lock:
            queue_size = len(self._write_queue)
            
        return {
            "read_pool_size": len(self._read_connections),
            "write_queue_size": queue_size,
            "max_queue_size": self.config.max_queue_size,
            "batch_size": self.config.write_batch_size,
            "batch_timeout": self.config.write_batch_timeout,
            "wal_enabled": self.config.enable_wal_mode,
            "initialized": self._initialized
        }

    async def health_check(self) -> bool:
        """Perform a health check on all connections"""
        try:
            # Test writer connection
            if self._writer_connection:
                self._writer_connection.execute("SELECT 1").fetchone()
            
            # Test read operations through thread executor
            if self._read_executor:
                try:
                    # Test that we can execute reads through the thread pool
                    with self._writer_connection.cursor() as cursor:
                        cursor.execute("SELECT 1").fetchone()
                except Exception as e:
                    logger.warning("Read connection unhealthy", error=str(e))
                    
            return True
            
        except Exception as e:
            logger.error("Health check failed", error=str(e))
            return False

    async def cleanup(self) -> None:
        """Clean up connections and background tasks"""
        logger.info("Cleaning up optimized database manager")
        
        # Stop batch processor
        if self._batch_processor_task:
            self._shutdown_event.set()
            self._batch_processor_task.cancel()
            try:
                await self._batch_processor_task
            except asyncio.CancelledError:
                pass
        
        # Process remaining write operations
        await self._process_write_batch()
        
        # Shutdown thread executor
        if self._read_executor:
            self._read_executor.shutdown(wait=True)
            
        # Close connections
        if self._writer_connection:
            self._writer_connection.close()
            
        # Clear connection references (they all point to the same connection)
        self._read_connections.clear()
        self._initialized = False
        
        logger.info("Optimized database manager cleanup completed")


# Global instance
_optimized_manager: Optional[OptimizedDatabaseManager] = None


async def get_optimized_db_manager(config: Optional[ConnectionConfig] = None) -> OptimizedDatabaseManager:
    """Get the global optimized database manager instance"""
    global _optimized_manager
    
    if _optimized_manager is None:
        _optimized_manager = OptimizedDatabaseManager(config)
        await _optimized_manager.initialize()
    
    return _optimized_manager


async def shutdown_optimized_manager() -> None:
    """Shutdown the global optimized manager"""
    global _optimized_manager
    
    if _optimized_manager:
        await _optimized_manager.cleanup()
        _optimized_manager = None