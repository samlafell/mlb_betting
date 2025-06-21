"""
Database connection management for the MLB Sharp Betting system.

This module provides database connection management with thread-safe cursor access.
DuckDB uses a single connection per database with multiple cursors for concurrent access.
"""

import logging
import threading
import time
import random
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator, List, Optional

import duckdb
import structlog

# from ..core.config import get_settings  # Not needed with hardcoded path
from ..core.exceptions import DatabaseConnectionError, DatabaseError

logger = structlog.get_logger(__name__)


def retry_on_conflict(max_retries=3, base_delay=0.1):
    """Decorator to retry operations on DuckDB transaction conflicts"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if "Transaction conflict" in str(e) or "cannot update a table that has been altered" in str(e):
                        if attempt < max_retries - 1:
                            # Exponential backoff with jitter
                            delay = base_delay * (2 ** attempt) + random.uniform(0, 0.1)
                            logger.warning(f"Transaction conflict, retrying in {delay:.2f}s (attempt {attempt + 1}/{max_retries})")
                            time.sleep(delay)
                            continue
                    raise
            return None
        return wrapper
    return decorator


class DatabaseManager:
    """
    Database connection manager for DuckDB with singleton pattern.
    
    DuckDB is an embedded database that uses a single connection per database file.
    For thread safety, we create separate cursors for each thread rather than 
    connection pooling.
    """

    _instance: Optional["DatabaseManager"] = None
    _lock = threading.Lock()

    def __new__(cls) -> "DatabaseManager":
        """Singleton pattern implementation."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        """Initialize database manager."""
        if hasattr(self, "_initialized"):
            logger.info("Database manager already initialized, skipping")
            return
            
        logger.info("Initializing new database manager instance")
        self._connection: Optional[duckdb.DuckDBPyConnection] = None
        self._connection_lock = threading.RLock()
        self._initialized = True
        
        # Initialize the single connection
        self._init_connection()
        
        logger.info("Database manager initialized")

    def _init_connection(self) -> None:
        """Initialize the DuckDB connection."""
        try:
            logger.info("Initializing DuckDB connection")
            
            # Hardcoded database path to bypass configuration issues
            db_path = Path("data/raw/mlb_betting.duckdb")
            logger.info("Database path being used", path=str(db_path))
            
            # Ensure database directory exists
            db_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Create the single connection with same config as optimized manager
            # This ensures compatibility when both legacy and optimized modes access same DB
            try:
                self._connection = duckdb.connect(
                    database=str(db_path),
                    config={
                        'threads': 2,  # Same as optimized read cursors
                        'preserve_insertion_order': False,
                        'enable_object_cache': True  # Match optimized configuration
                    }
                )
                logger.info(
                    "DuckDB connection established",
                    database_path=str(db_path)
                )
            except duckdb.duckdb.IOException as e:
                if "Conflicting lock" in str(e):
                    logger.error(
                        "Database is locked by another process. "
                        "Check for stuck processes and kill them if necessary.",
                        error=str(e)
                    )
                    # Provide helpful information about finding the process
                    import os
                    current_pid = os.getpid()
                    logger.error(
                        "Current process PID: %d. "
                        "Use 'ps aux | grep python | grep mlb' to find conflicting processes.",
                        current_pid
                    )
                raise DatabaseConnectionError(
                    f"Database is locked by another process. "
                    f"Kill any stuck processes and try again: {e}"
                )
                
        except Exception as e:
            logger.error("Failed to initialize DuckDB connection", error=str(e))
            raise DatabaseConnectionError(f"Failed to initialize connection: {e}")

    @contextmanager
    @retry_on_conflict(max_retries=3, base_delay=0.1)
    def get_cursor(self) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """
        Context manager for database cursors with retry logic.
        
        In DuckDB, cursors are created from the main connection and are the
        recommended way to handle concurrent access from multiple threads.
        
        Yields:
            A database cursor
            
        Raises:
            DatabaseConnectionError: If unable to get cursor
        """
        if self._connection is None:
            raise DatabaseConnectionError("Database connection not initialized")
            
        cursor = None
        try:
            # Create a cursor from the main connection
            # DuckDB cursors are lightweight and thread-safe
            with self._connection_lock:
                cursor = self._connection.cursor()
            yield cursor
        except Exception as e:
            logger.error("Database cursor operation failed", error=str(e))
            if isinstance(e, (DatabaseConnectionError, DatabaseError)):
                raise
            raise DatabaseError(f"Database cursor operation failed: {e}")
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception as e:
                    logger.warning("Error closing cursor", error=str(e))

    @contextmanager
    @retry_on_conflict(max_retries=3, base_delay=0.1)
    def get_connection(self) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """
        Context manager for the main database connection with retry logic.
        
        Use this sparingly - prefer get_cursor() for most operations.
        This is provided for compatibility but should be used with caution
        in multi-threaded environments.
        
        Yields:
            The main database connection
            
        Raises:
            DatabaseConnectionError: If connection not available
        """
        if self._connection is None:
            raise DatabaseConnectionError("Database connection not initialized")
            
        try:
            with self._connection_lock:
                yield self._connection
        except Exception as e:
            logger.error("Database connection operation failed", error=str(e))
            if isinstance(e, (DatabaseConnectionError, DatabaseError)):
                raise
            raise DatabaseError(f"Database connection operation failed: {e}")

    @retry_on_conflict(max_retries=3, base_delay=0.1)
    def execute_query(
        self, 
        query: str, 
        parameters: Optional[tuple] = None,
        fetch: bool = True
    ) -> Optional[List[tuple]]:
        """
        Execute a query with optional parameters and retry logic.
        
        Args:
            query: SQL query to execute
            parameters: Optional parameters for the query
            fetch: Whether to fetch results
            
        Returns:
            Query results if fetch=True, None otherwise
            
        Raises:
            DatabaseError: If query execution fails
        """
        try:
            with self.get_cursor() as cursor:
                if parameters:
                    cursor.execute(query, parameters)
                else:
                    cursor.execute(query)
                
                if fetch:
                    return cursor.fetchall()
                return None
        except Exception as e:
            logger.error("Query execution failed", query=query, error=str(e))
            raise DatabaseError(f"Query execution failed: {e}")

    @retry_on_conflict(max_retries=3, base_delay=0.1)
    def execute_many(
        self,
        query: str,
        parameters_list: List[tuple]
    ) -> None:
        """
        Execute a query multiple times with different parameters and retry logic.
        
        Args:
            query: SQL query to execute
            parameters_list: List of parameter tuples
            
        Raises:
            DatabaseError: If query execution fails
        """
        try:
            with self.get_cursor() as cursor:
                cursor.executemany(query, parameters_list)
            logger.debug("Batch query executed successfully", 
                        batch_size=len(parameters_list))
        except Exception as e:
            logger.error("Batch query execution failed", 
                        query=query, batch_size=len(parameters_list), error=str(e))
            raise DatabaseError(f"Batch query execution failed: {e}")

    @retry_on_conflict(max_retries=3, base_delay=0.1)
    def execute_script(self, script: str) -> None:
        """
        Execute a SQL script with retry logic.
        
        Args:
            script: SQL script to execute
            
        Raises:
            DatabaseError: If script execution fails
        """
        try:
            with self.get_cursor() as cursor:
                cursor.executescript(script)
            logger.info("SQL script executed successfully")
        except Exception as e:
            logger.error("Script execution failed", error=str(e))
            raise DatabaseError(f"Script execution failed: {e}")

    def table_exists(self, table_name: str, schema: Optional[str] = None) -> bool:
        """
        Check if a table exists.
        
        Args:
            table_name: Name of the table
            schema: Optional schema name
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            if schema:
                query = """
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = ? AND table_name = ?
                """
                result = self.execute_query(query, (schema, table_name))
            else:
                query = """
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = ?
                """
                result = self.execute_query(query, (table_name,))
            
            return result[0][0] > 0 if result else False
        except Exception as e:
            logger.warning("Error checking table existence", 
                         table_name=table_name, error=str(e))
            return False

    def get_table_info(self, table_name: str, schema: Optional[str] = None) -> List[dict]:
        """
        Get information about a table's columns.
        
        Args:
            table_name: Name of the table
            schema: Optional schema name
            
        Returns:
            List of column information dictionaries
        """
        try:
            if schema:
                query = f"DESCRIBE {schema}.{table_name}"
            else:
                query = f"DESCRIBE {table_name}"
            
            result = self.execute_query(query)
            if result:
                # Convert to list of dicts for easier handling
                columns = []
                for row in result:
                    columns.append({
                        "column_name": row[0],
                        "column_type": row[1],
                        "null": row[2],
                        "key": row[3] if len(row) > 3 else None,
                        "default": row[4] if len(row) > 4 else None,
                        "extra": row[5] if len(row) > 5 else None,
                    })
                return columns
            return []
        except Exception as e:
            logger.error("Error getting table info", 
                        table_name=table_name, error=str(e))
            raise DatabaseError(f"Failed to get table info: {e}")

    @contextmanager
    def transaction(self) -> Generator[None, None, None]:
        """
        Context manager for database transactions.
        
        DuckDB handles transactions automatically within connections.
        This provides a simple interface for batched operations.
        """
        try:
            # DuckDB auto-manages transactions within a connection
            # We'll use the connection context to ensure consistency
            with self.get_cursor() as cursor:
                # Start explicit transaction
                cursor.execute("BEGIN")
                logger.debug("Transaction started")
                
                try:
                    yield
                    # Commit on success
                    cursor.execute("COMMIT")
                    logger.debug("Transaction committed")
                except Exception:
                    # Rollback on error
                    try:
                        cursor.execute("ROLLBACK")
                        logger.debug("Transaction rolled back")
                    except Exception as rollback_error:
                        logger.warning("Failed to rollback transaction", error=str(rollback_error))
                    raise
                    
        except Exception as e:
            logger.error("Transaction failed", error=str(e))
            raise

    def vacuum(self) -> None:
        """Vacuum the database to reclaim space."""
        try:
            self.execute_query("VACUUM", fetch=False)
            logger.info("Database vacuumed successfully")
        except Exception as e:
            logger.error("Failed to vacuum database", error=str(e))
            raise DatabaseError(f"Failed to vacuum database: {e}")

    def analyze(self, table_name: Optional[str] = None) -> None:
        """
        Analyze table statistics for query optimization.
        
        Args:
            table_name: Optional specific table to analyze
        """
        try:
            if table_name:
                query = f"ANALYZE {table_name}"
            else:
                query = "ANALYZE"
            
            self.execute_query(query, fetch=False)
            logger.info("Database analysis completed", table=table_name)
        except Exception as e:
            logger.error("Failed to analyze database", 
                        table=table_name, error=str(e))
            raise DatabaseError(f"Failed to analyze database: {e}")

    def close(self) -> None:
        """Close the database connection."""
        with self._connection_lock:
            if self._connection is not None:
                try:
                    self._connection.close()
                    self._connection = None
                    logger.info("Database connection closed")
                except Exception as e:
                    logger.warning("Error closing database connection", error=str(e))

    def __del__(self) -> None:
        """Cleanup on deletion."""
        self.close()
    
    @classmethod
    def reset_singleton(cls) -> None:
        """Reset the singleton instance. Use with caution."""
        with cls._lock:
            if cls._instance is not None:
                try:
                    cls._instance.close()
                except Exception as e:
                    logger.warning("Error closing connection during reset", error=str(e))
            cls._instance = None
            logger.info("Database manager singleton reset")


# Module-level functions for convenience
def get_db_manager() -> DatabaseManager:
    """
    Get the singleton database manager instance.
    
    Returns:
        The database manager instance
    """
    return DatabaseManager()


def get_db_connection() -> Any:
    """
    Get a database connection context manager.
    
    Returns:
        Database connection context manager
    """
    return get_db_manager().get_connection()


def get_db_cursor() -> Any:
    """
    Get a database cursor context manager.
    
    This is the recommended way to access the database in multi-threaded
    environments with DuckDB.
    
    Returns:
        Database cursor context manager
    """
    return get_db_manager().get_cursor()


def execute_query(
    query: str, 
    parameters: Optional[tuple] = None,
    fetch: bool = True
) -> Optional[List[tuple]]:
    """
    Execute a query using the default database manager.
    
    Args:
        query: SQL query to execute
        parameters: Optional parameters for the query
        fetch: Whether to fetch results
        
    Returns:
        Query results if fetch=True, None otherwise
    """
    return get_db_manager().execute_query(query, parameters, fetch) 