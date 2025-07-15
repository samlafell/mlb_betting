"""
Database connection management for the MLB Sharp Betting system.

This module provides PostgreSQL database connection management with thread-safe cursor access.
Uses connection pooling for optimal performance in multi-threaded environments.

Consolidated from multiple database managers for improved maintainability.
"""

import random
import threading
import time
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, Optional
from urllib.parse import quote_plus

import psycopg2
import psycopg2.pool
import structlog
from psycopg2.extras import DictCursor
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import QueuePool

from ..core.exceptions import DatabaseConnectionError, DatabaseError

logger = structlog.get_logger(__name__)


def retry_on_conflict(max_retries=3, base_delay=0.1):
    """Decorator to retry operations on PostgreSQL transaction conflicts"""

    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    error_msg = str(e).lower()
                    if any(
                        conflict in error_msg
                        for conflict in [
                            "could not serialize",
                            "deadlock",
                            "connection closed",
                            "connection already closed",
                            "server closed the connection",
                        ]
                    ):
                        if attempt < max_retries - 1:
                            # Exponential backoff with jitter
                            delay = base_delay * (2**attempt) + random.uniform(0, 0.1)
                            logger.warning(
                                f"Database conflict, retrying in {delay:.2f}s (attempt {attempt + 1}/{max_retries})"
                            )
                            time.sleep(delay)
                            continue
                    raise
            return None

        return wrapper

    return decorator


class DatabaseManager:
    """
    PostgreSQL database connection manager with connection pooling.

    Consolidated implementation that combines the best features from multiple
    database managers for improved maintainability and functionality.

    Uses psycopg2 connection pooling for thread-safe database access
    with automatic connection management and retry logic.
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

        logger.info("Initializing new PostgreSQL database manager instance")
        self._pool: psycopg2.pool.ThreadedConnectionPool | None = None
        self._pool_lock = threading.RLock()

        # SQLAlchemy components
        self.engine = None
        self.SessionLocal = None

        self._initialized = True

        # Initialize the connection pool and SQLAlchemy
        self._init_connection_pool()
        self._init_sqlalchemy()

        logger.info("PostgreSQL database manager initialized")

    def is_initialized(self) -> bool:
        """Check if the database manager is properly initialized."""
        return (
            hasattr(self, "_initialized")
            and self._initialized
            and self._pool is not None
            and self.engine is not None
        )

    def initialize(self) -> None:
        """
        Public method to initialize the database manager.

        This is a no-op if already initialized since initialization
        happens in __init__, but provides a consistent interface.
        """
        if not self.is_initialized():
            logger.warning(
                "Database manager not properly initialized, reinitializing..."
            )
            self._init_connection_pool()
            self._init_sqlalchemy()
        else:
            logger.debug("Database manager already initialized")

    def _init_connection_pool(self) -> None:
        """Initialize the PostgreSQL connection pool."""
        try:
            logger.info("Initializing PostgreSQL connection pool")

            # Import settings here to avoid circular imports
            from ..core.config import get_settings

            settings = get_settings()

            # Create connection pool
            try:
                self._pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=settings.postgres.min_connections,
                    maxconn=settings.postgres.max_connections,
                    host=settings.postgres.host,
                    port=settings.postgres.port,
                    database=settings.postgres.database,
                    user=settings.postgres.user,
                    password=settings.postgres.password,
                    cursor_factory=DictCursor,
                    # Connection pool settings
                    options="-c search_path=splits,main,public",
                )
                logger.info(
                    "PostgreSQL connection pool established",
                    host=settings.postgres.host,
                    database=settings.postgres.database,
                    min_connections=settings.postgres.min_connections,
                    max_connections=settings.postgres.max_connections,
                )
            except psycopg2.OperationalError as e:
                logger.error(
                    "Failed to connect to PostgreSQL database. "
                    "Check connection settings and ensure database is running.",
                    error=str(e),
                )
                raise DatabaseConnectionError(
                    f"PostgreSQL connection failed. "
                    f"Check database settings and connectivity: {e}"
                )

        except Exception as e:
            logger.error(
                "Failed to initialize PostgreSQL connection pool", error=str(e)
            )
            raise DatabaseConnectionError(f"Failed to initialize connection pool: {e}")

    def _init_sqlalchemy(self) -> None:
        """Initialize SQLAlchemy engine and session factory."""
        try:
            # Import settings here to avoid circular imports
            from ..core.config import get_settings

            settings = get_settings()

            # Build SQLAlchemy URL
            password_part = (
                f":{quote_plus(settings.postgres.password)}"
                if settings.postgres.password
                else ""
            )
            url = f"postgresql://{settings.postgres.user}{password_part}@{settings.postgres.host}:{settings.postgres.port}/{settings.postgres.database}"

            # Create engine with connection pooling
            self.engine = create_engine(
                url,
                poolclass=QueuePool,
                pool_size=10,
                max_overflow=20,
                pool_timeout=30,
                pool_recycle=3600,  # Recycle connections every hour
                echo=False,  # Set to True for SQL debugging
            )

            # Create session factory
            self.SessionLocal = sessionmaker(
                autocommit=False, autoflush=False, bind=self.engine
            )

            # Test connection
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                result.fetchone()

            logger.info("SQLAlchemy engine initialized successfully")

        except Exception as e:
            logger.error("Failed to initialize SQLAlchemy", error=str(e))
            raise DatabaseConnectionError(f"Failed to initialize SQLAlchemy: {e}")

    def _convert_parameters(
        self, query: str, parameters: tuple | None
    ) -> tuple[str, tuple | None]:
        """
        Convert legacy ? parameters to psycopg2 %s format for better PostgreSQL compatibility.

        Args:
            query: SQL query string
            parameters: Query parameters

        Returns:
            Tuple of (converted_query, parameters)
        """
        if parameters and "?" in query:
            # Simply replace all ? with %s for psycopg2
            pg_query = query.replace("?", "%s")
            return pg_query, parameters
        return query, parameters

    @contextmanager
    @retry_on_conflict(max_retries=3, base_delay=0.1)
    def get_cursor(self) -> Generator[psycopg2.extras.DictCursor, None, None]:
        """
        Context manager for database cursors with retry logic.

        Gets a connection from the pool and provides a cursor.
        Automatically handles connection return to pool.

        Yields:
            A PostgreSQL dictionary cursor

        Raises:
            DatabaseConnectionError: If unable to get cursor
        """
        if self._pool is None:
            raise DatabaseConnectionError("Database connection pool not initialized")

        connection = None
        cursor = None
        try:
            # Get connection from pool
            with self._pool_lock:
                connection = self._pool.getconn()

            if connection is None:
                raise DatabaseConnectionError("Unable to get connection from pool")

            # Create cursor
            cursor = connection.cursor(cursor_factory=DictCursor)
            yield cursor

            # Commit transaction on success
            connection.commit()

        except Exception as e:
            # Rollback on error
            if connection:
                try:
                    connection.rollback()
                except Exception as rollback_error:
                    logger.warning(
                        "Error rolling back transaction", error=str(rollback_error)
                    )

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

            if connection is not None:
                try:
                    with self._pool_lock:
                        self._pool.putconn(connection)
                except Exception as e:
                    logger.warning("Error returning connection to pool", error=str(e))

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """
        Context manager for SQLAlchemy sessions.

        Use this for ORM operations and complex queries.

        Yields:
            A SQLAlchemy session

        Raises:
            DatabaseConnectionError: If unable to create session
        """
        if self.SessionLocal is None:
            raise DatabaseConnectionError("SQLAlchemy session factory not initialized")

        session = self.SessionLocal()
        try:
            logger.debug("Created SQLAlchemy session")
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error("Session error, rolled back", error=str(e))
            if isinstance(e, (DatabaseConnectionError, DatabaseError)):
                raise
            raise DatabaseError(f"SQLAlchemy session error: {e}")
        finally:
            session.close()
            logger.debug("Closed SQLAlchemy session")

    @contextmanager
    @retry_on_conflict(max_retries=3, base_delay=0.1)
    def get_connection(self) -> Generator[psycopg2.extensions.connection, None, None]:
        """
        Context manager for database connections with retry logic.

        Use this when you need direct connection access.
        Prefer get_cursor() for most operations.

        Yields:
            A PostgreSQL database connection

        Raises:
            DatabaseConnectionError: If connection not available
        """
        if self._pool is None:
            raise DatabaseConnectionError("Database connection pool not initialized")

        connection = None
        try:
            # Get connection from pool
            with self._pool_lock:
                connection = self._pool.getconn()

            if connection is None:
                raise DatabaseConnectionError("Unable to get connection from pool")

            yield connection

            # Commit on success
            connection.commit()

        except Exception as e:
            # Rollback on error
            if connection:
                try:
                    connection.rollback()
                except Exception as rollback_error:
                    logger.warning(
                        "Error rolling back connection", error=str(rollback_error)
                    )

            logger.error("Database connection operation failed", error=str(e))
            if isinstance(e, (DatabaseConnectionError, DatabaseError)):
                raise
            raise DatabaseError(f"Database connection operation failed: {e}")
        finally:
            if connection is not None:
                try:
                    with self._pool_lock:
                        self._pool.putconn(connection)
                except Exception as e:
                    logger.warning("Error returning connection to pool", error=str(e))

    @retry_on_conflict(max_retries=3, base_delay=0.1)
    def execute_query(
        self,
        query: str,
        parameters: tuple | None = None,
        fetch: bool = True,
        dict_cursor: bool = True,
    ) -> list[Any] | None:
        """
        Execute a query with optional parameters and retry logic.

        Args:
            query: SQL query to execute
            parameters: Optional parameters for the query
            fetch: Whether to fetch results
            dict_cursor: Whether to return dict-like results

        Returns:
            Query results if fetch=True, None otherwise

        Raises:
            DatabaseError: If query execution fails
        """
        try:
            # Convert parameters for PostgreSQL compatibility
            pg_query, pg_parameters = self._convert_parameters(query, parameters)

            # Import SQL operations logger
            from ..core.logging import log_sql_operation

            # Log SQL execution start
            start_operation_data = {
                "operation_type": "SINGLE_QUERY",
                "query_hash": hash(pg_query),
                "query_length": len(pg_query),
                "parameter_count": len(pg_parameters) if pg_parameters else 0,
                "fetch_mode": fetch,
                "query_preview": pg_query[:100].replace("\n", " ").replace("\t", " "),
                "parameters_preview": str(pg_parameters)[:200]
                if pg_parameters
                else "None",
                "success": True,  # Will be updated if error occurs
            }

            with self.get_cursor() as cursor:
                start_time = time.time()

                if pg_parameters:
                    cursor.execute(pg_query, pg_parameters)
                else:
                    cursor.execute(pg_query)

                execution_time = time.time() - start_time

                if fetch:
                    results = cursor.fetchall()

                    # Log successful completion
                    completion_data = start_operation_data.copy()
                    completion_data.update(
                        {
                            "execution_time_ms": round(execution_time * 1000, 2),
                            "rows_returned": len(results) if results else 0,
                            "rows_affected": 0,
                            "success": True,
                        }
                    )
                    log_sql_operation(completion_data)

                    logger.debug(
                        "SQL_EXECUTION_COMPLETE",
                        operation_type="SINGLE_QUERY",
                        query_hash=hash(pg_query),
                        execution_time_ms=round(execution_time * 1000, 2),
                        rows_returned=len(results) if results else 0,
                        success=True,
                    )
                    return results
                else:
                    rows_affected = cursor.rowcount

                    # Log successful completion
                    completion_data = start_operation_data.copy()
                    completion_data.update(
                        {
                            "execution_time_ms": round(execution_time * 1000, 2),
                            "rows_returned": 0,
                            "rows_affected": rows_affected,
                            "success": True,
                        }
                    )
                    log_sql_operation(completion_data)

                    logger.debug(
                        "SQL_EXECUTION_COMPLETE",
                        operation_type="SINGLE_QUERY",
                        query_hash=hash(pg_query),
                        execution_time_ms=round(execution_time * 1000, 2),
                        rows_affected=rows_affected,
                        success=True,
                    )
                    return rows_affected

        except Exception as e:
            # Log error
            error_data = start_operation_data.copy()
            error_data.update(
                {
                    "success": False,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "execution_time_ms": 0,
                    "rows_returned": 0,
                    "rows_affected": 0,
                }
            )
            log_sql_operation(error_data)

            logger.error(
                "SQL_EXECUTION_ERROR",
                operation_type="SINGLE_QUERY",
                query_hash=hash(query),
                error_type=type(e).__name__,
                error_message=str(e),
                query_preview=query[:100].replace("\n", " ").replace("\t", " "),
                parameters_preview=str(parameters)[:200] if parameters else "None",
            )
            raise DatabaseError(f"Query execution failed: {e}")

    @retry_on_conflict(max_retries=3, base_delay=0.1)
    def execute_many(self, query: str, parameters_list: list[tuple]) -> int:
        """
        Execute a query multiple times with different parameters and retry logic.

        Args:
            query: SQL query to execute
            parameters_list: List of parameter tuples

        Returns:
            Number of rows affected

        Raises:
            DatabaseError: If query execution fails
        """
        try:
            # Convert parameters for PostgreSQL compatibility
            pg_query, _ = self._convert_parameters(
                query, parameters_list[0] if parameters_list else None
            )

            # Structured logging for batch SQL execution
            logger.debug(
                "SQL_EXECUTION_START",
                operation_type="BATCH_QUERY",
                query_hash=hash(pg_query),
                query_length=len(pg_query),
                batch_size=len(parameters_list),
                query_preview=pg_query[:100].replace("\n", " ").replace("\t", " "),
                first_params_preview=str(parameters_list[0])[:200]
                if parameters_list
                else "None",
            )

            with self.get_cursor() as cursor:
                start_time = time.time()
                cursor.executemany(pg_query, parameters_list)
                execution_time = time.time() - start_time

                rows_affected = cursor.rowcount
                logger.debug(
                    "SQL_EXECUTION_COMPLETE",
                    operation_type="BATCH_QUERY",
                    query_hash=hash(pg_query),
                    execution_time_ms=round(execution_time * 1000, 2),
                    batch_size=len(parameters_list),
                    rows_affected=rows_affected,
                    rows_per_second=round(len(parameters_list) / execution_time, 1)
                    if execution_time > 0
                    else 0,
                    success=True,
                )
                return rows_affected

        except Exception as e:
            logger.error(
                "SQL_EXECUTION_ERROR",
                operation_type="BATCH_QUERY",
                query_hash=hash(query),
                error_type=type(e).__name__,
                error_message=str(e),
                batch_size=len(parameters_list),
                query_preview=query[:100].replace("\n", " ").replace("\t", " "),
            )
            raise DatabaseError(f"Batch query execution failed: {e}")

    @retry_on_conflict(max_retries=3, base_delay=0.1)
    def execute_transaction(self, operations: list[tuple]) -> list[Any]:
        """
        Execute multiple operations in a single transaction.

        Args:
            operations: List of (query, parameters) tuples

        Returns:
            List of results for each operation

        Raises:
            DatabaseError: If transaction fails
        """
        results = []
        try:
            # Structured logging for transaction start
            logger.debug(
                "SQL_EXECUTION_START",
                operation_type="TRANSACTION",
                transaction_id=id(operations),
                operations_count=len(operations),
                first_operation_preview=operations[0][0][:100]
                .replace("\n", " ")
                .replace("\t", " ")
                if operations
                else "None",
            )

            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=DictCursor) as cursor:
                    transaction_start_time = time.time()

                    for i, (query, parameters) in enumerate(operations):
                        # Convert parameters for PostgreSQL compatibility
                        pg_query, pg_parameters = self._convert_parameters(
                            query, parameters
                        )

                        # Log each operation within the transaction
                        logger.debug(
                            "SQL_TRANSACTION_OPERATION",
                            transaction_id=id(operations),
                            operation_index=i,
                            operation_type="TRANSACTION_STEP",
                            query_hash=hash(pg_query),
                            query_preview=pg_query[:100]
                            .replace("\n", " ")
                            .replace("\t", " "),
                            parameters_preview=str(pg_parameters)[:200]
                            if pg_parameters
                            else "None",
                        )

                        if pg_parameters:
                            cursor.execute(pg_query, pg_parameters)
                        else:
                            cursor.execute(pg_query)

                        # Fetch results if it's a SELECT query
                        if query.strip().upper().startswith("SELECT"):
                            result = cursor.fetchall()
                            results.append(result)
                            logger.debug(
                                "SQL_TRANSACTION_OPERATION_RESULT",
                                transaction_id=id(operations),
                                operation_index=i,
                                result_type="ROWS_RETURNED",
                                rows_count=len(result),
                            )
                        else:
                            results.append(cursor.rowcount)
                            logger.debug(
                                "SQL_TRANSACTION_OPERATION_RESULT",
                                transaction_id=id(operations),
                                operation_index=i,
                                result_type="ROWS_AFFECTED",
                                rows_count=cursor.rowcount,
                            )

                    transaction_time = time.time() - transaction_start_time

                    # Commit handled by connection context manager
                    logger.debug(
                        "SQL_EXECUTION_COMPLETE",
                        operation_type="TRANSACTION",
                        transaction_id=id(operations),
                        operations_count=len(operations),
                        execution_time_ms=round(transaction_time * 1000, 2),
                        success=True,
                    )
                    return results

        except Exception as e:
            logger.error(
                "SQL_EXECUTION_ERROR",
                operation_type="TRANSACTION",
                transaction_id=id(operations),
                operations_count=len(operations),
                error_type=type(e).__name__,
                error_message=str(e),
            )
            raise DatabaseError(f"Transaction failed: {e}")

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
                cursor.execute(script)
            logger.info("SQL script executed successfully")
        except Exception as e:
            logger.error("Script execution failed", error=str(e))
            raise DatabaseError(f"Script execution failed: {e}")

    def test_connection(self) -> bool:
        """
        Test database connectivity.

        Returns:
            True if connection is successful, False otherwise
        """
        try:
            result = self.execute_query("SELECT 1 as test_value")
            return result is not None and len(result) > 0
        except Exception as e:
            logger.error("Connection test failed", error=str(e))
            return False

    def get_pool_status(self) -> dict[str, Any]:
        """
        Get connection pool status information.

        Returns:
            Dictionary with pool status information
        """
        try:
            if self._pool and hasattr(self._pool, "_pool"):
                pool = self._pool._pool
                used = getattr(self._pool, "_used", [])
                return {
                    "total_connections": len(pool) + len(used),
                    "available_connections": len(pool),
                    "used_connections": len(used),
                    "min_connections": getattr(self._pool, "minconn", "unknown"),
                    "max_connections": getattr(self._pool, "maxconn", "unknown"),
                }
            return {"status": "pool_info_unavailable"}
        except Exception as e:
            logger.warning("Error getting pool status", error=str(e))
            return {"status": "error", "error": str(e)}

    def table_exists(self, table_name: str, schema: str | None = None) -> bool:
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
                WHERE table_schema = %s AND table_name = %s
                """
                result = self.execute_query(query, (schema, table_name))
            else:
                query = """
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = %s
                """
                result = self.execute_query(query, (table_name,))

            return result[0][0] > 0 if result else False
        except Exception as e:
            logger.warning(
                "Error checking table existence", table_name=table_name, error=str(e)
            )
            return False

    def get_table_info(self, table_name: str, schema: str | None = None) -> list[dict]:
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
                query = """
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """
                result = self.execute_query(query, (schema, table_name))
            else:
                query = """
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns 
                WHERE table_name = %s
                ORDER BY ordinal_position
                """
                result = self.execute_query(query, (table_name,))

            if result:
                columns = []
                for row in result:
                    columns.append(
                        {
                            "column_name": row[0],
                            "data_type": row[1],
                            "is_nullable": row[2],
                            "column_default": row[3],
                        }
                    )
                return columns
            return []
        except Exception as e:
            logger.error(
                "Error getting table info", table_name=table_name, error=str(e)
            )
            raise DatabaseError(f"Failed to get table info: {e}")

    @contextmanager
    def transaction(self) -> Generator[None, None, None]:
        """
        Context manager for database transactions.

        Provides explicit transaction control with automatic
        commit/rollback handling.
        """
        try:
            with self.get_connection() as connection:
                # PostgreSQL automatically starts a transaction
                logger.debug("Transaction started")

                try:
                    yield
                    # Commit handled automatically by connection context manager
                    logger.debug("Transaction committed")
                except Exception:
                    # Rollback handled automatically by connection context manager
                    logger.debug("Transaction rolled back")
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

    def analyze(self, table_name: str | None = None) -> None:
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
            logger.error("Failed to analyze database", table=table_name, error=str(e))
            raise DatabaseError(f"Failed to analyze database: {e}")

    def close(self) -> None:
        """Close the database connection pool."""
        with self._pool_lock:
            if self._pool is not None:
                try:
                    self._pool.closeall()
                    self._pool = None
                    logger.info("Database connection pool closed")
                except Exception as e:
                    logger.warning(
                        "Error closing database connection pool", error=str(e)
                    )

            if hasattr(self, "engine") and self.engine:
                try:
                    self.engine.dispose()
                    logger.info("SQLAlchemy engine disposed")
                except Exception as e:
                    logger.warning("Error disposing SQLAlchemy engine", error=str(e))

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
                    logger.warning(
                        "Error closing connection during reset", error=str(e)
                    )
            cls._instance = None
            logger.info("Database manager singleton reset")


# Compatibility aliases for legacy code
class PostgreSQLManager(DatabaseManager):
    """Compatibility alias for legacy code that imports PostgreSQLManager."""

    pass


class PostgreSQLDatabaseManager(DatabaseManager):
    """Compatibility alias for legacy code that imports PostgreSQLDatabaseManager."""

    pass


# Module-level functions for convenience
def get_db_manager() -> DatabaseManager:
    """
    Get the singleton database manager instance.

    Returns:
        The PostgreSQL database manager instance
    """
    try:
        return DatabaseManager()
    except Exception as e:
        logger.error("Failed to initialize PostgreSQL database manager", error=str(e))
        raise DatabaseConnectionError(f"Database initialization failed: {e}")


def get_postgres_manager() -> DatabaseManager:
    """Compatibility function for legacy code."""
    return get_db_manager()


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

    This is the recommended way to access the database.

    Returns:
        Database cursor context manager
    """
    return get_db_manager().get_cursor()


def execute_query(
    query: str, parameters: tuple | None = None, fetch: bool = True
) -> list[tuple] | None:
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
