"""
DEPRECATED: Database Coordinator Service

⚠️  DEPRECATION NOTICE: This service has been consolidated into DataService.
Please update imports to use:
    from ..services.data_service import get_data_service

This wrapper is provided for backward compatibility during the transition.
It will be removed in a future version.

MIGRATION GUIDE:
- Replace `get_database_coordinator()` with `get_data_service()`
- Use `service.execute_read()` instead of `coordinator.execute_read()`
- Use `service.execute_write()` instead of `coordinator.execute_write()`
- The DataService provides all coordinator functionality plus:
  - Data collection (from DataCollector)
  - Data persistence (from DataPersistenceService)
  - Data deduplication (from DataDeduplicationService)
  - Unified interface for all data operations
"""

import time
from contextlib import contextmanager
from typing import Any

import structlog

from ..core.exceptions import DatabaseError
from ..db.connection import get_db_manager

logger = structlog.get_logger(__name__)


class DatabaseCoordinator:
    """
    Unified database coordinator using the consolidated PostgreSQL connection manager.

    This replaces both the legacy file-locking coordinator and the separate PostgreSQL
    coordinator with a single, simplified implementation that leverages PostgreSQL's
    native MVCC and connection pooling capabilities.
    """

    def __init__(self):
        """Initialize the database coordinator."""
        self.db_manager = get_db_manager()
        self._stats = {
            "read_operations": 0,
            "write_operations": 0,
            "bulk_operations": 0,
            "transaction_operations": 0,
            "errors": 0,
            "start_time": time.time(),
        }
        logger.info(
            "Database Coordinator initialized with PostgreSQL connection manager"
        )

    def execute_read(
        self,
        query: str,
        parameters: tuple | dict | None = None,
        timeout: float = 30.0,
    ) -> list[Any] | None:
        """
        Execute a read operation using the consolidated connection manager.

        Args:
            query: SQL query to execute
            parameters: Query parameters (tuple or dict)
            timeout: Query timeout in seconds (for compatibility - not enforced)

        Returns:
            Query results as list of rows
        """
        try:
            # Structured logging for coordinator read operation
            logger.debug(
                "COORDINATOR_OPERATION_START",
                operation_type="READ",
                coordinator_type="DATABASE_COORDINATOR",
                query_hash=hash(query),
                timeout_seconds=timeout,
                parameter_count=len(parameters) if parameters else 0,
                query_preview=query[:100].replace("\n", " ").replace("\t", " "),
                parameters_preview=str(parameters)[:200] if parameters else "None",
            )

            start_time = time.time()
            result = self.db_manager.execute_query(query, parameters, fetch=True)
            execution_time = time.time() - start_time

            self._stats["read_operations"] += 1
            logger.debug(
                "COORDINATOR_OPERATION_COMPLETE",
                operation_type="READ",
                coordinator_type="DATABASE_COORDINATOR",
                query_hash=hash(query),
                execution_time_ms=round(execution_time * 1000, 2),
                rows_returned=len(result) if result else 0,
                success=True,
            )

            return result

        except Exception as e:
            self._stats["errors"] += 1
            logger.error(
                "COORDINATOR_OPERATION_ERROR",
                operation_type="READ",
                coordinator_type="DATABASE_COORDINATOR",
                query_hash=hash(query),
                error_type=type(e).__name__,
                error_message=str(e),
                query_preview=query[:100].replace("\n", " ").replace("\t", " "),
                parameters_preview=str(parameters)[:200] if parameters else "None",
            )
            raise DatabaseError(f"Database read failed: {e}")

    def execute_write(
        self,
        query: str,
        parameters: tuple | dict | None = None,
        timeout: float = 60.0,
    ) -> Any:
        """
        Execute a write operation using the consolidated connection manager.

        Args:
            query: SQL query to execute
            parameters: Query parameters (tuple or dict)
            timeout: Query timeout in seconds (for compatibility - not enforced)

        Returns:
            Query result (rowcount for regular writes, row data for RETURNING clauses)
        """
        try:
            # Check if this is a RETURNING query (needs fetch=True to get row data)
            has_returning = "RETURNING" in query.upper()

            # Structured logging for coordinator write operation
            logger.debug(
                "COORDINATOR_OPERATION_START",
                operation_type="WRITE",
                coordinator_type="DATABASE_COORDINATOR",
                query_hash=hash(query),
                timeout_seconds=timeout,
                has_returning=has_returning,
                parameter_count=len(parameters) if parameters else 0,
                query_preview=query[:100].replace("\n", " ").replace("\t", " "),
                parameters_preview=str(parameters)[:200] if parameters else "None",
            )

            start_time = time.time()

            if has_returning:
                result = self.db_manager.execute_query(query, parameters, fetch=True)
            else:
                result = self.db_manager.execute_query(query, parameters, fetch=False)

            execution_time = time.time() - start_time

            self._stats["write_operations"] += 1
            logger.debug(
                "COORDINATOR_OPERATION_COMPLETE",
                operation_type="WRITE",
                coordinator_type="DATABASE_COORDINATOR",
                query_hash=hash(query),
                execution_time_ms=round(execution_time * 1000, 2),
                has_returning=has_returning,
                rows_returned=len(result) if has_returning and result else None,
                rows_affected=result if not has_returning else None,
                success=True,
            )

            return result

        except Exception as e:
            self._stats["errors"] += 1
            logger.error(
                "COORDINATOR_OPERATION_ERROR",
                operation_type="WRITE",
                coordinator_type="DATABASE_COORDINATOR",
                query_hash=hash(query),
                error_type=type(e).__name__,
                error_message=str(e),
                query_preview=query[:100].replace("\n", " ").replace("\t", " "),
                parameters_preview=str(parameters)[:200] if parameters else "None",
            )
            raise DatabaseError(f"Database write failed: {e}")

    def execute_bulk_insert(
        self,
        query: str,
        parameters_list: list[tuple | dict],
        timeout: float = 300.0,
    ) -> str:
        """
        Execute a bulk insert operation using the consolidated connection manager.

        Args:
            query: SQL insert query
            parameters_list: List of parameter sets for bulk insert
            timeout: Query timeout in seconds (for compatibility - not enforced)

        Returns:
            Success message with row count
        """
        try:
            start_time = time.time()
            self.db_manager.execute_many(query, parameters_list)
            execution_time = time.time() - start_time

            self._stats["bulk_operations"] += 1
            logger.info(
                "Bulk insert completed",
                rows=len(parameters_list),
                execution_time=f"{execution_time:.3f}s",
                rows_per_second=f"{len(parameters_list) / execution_time:.1f}",
            )

            return f"Bulk insert completed: {len(parameters_list)} rows in {execution_time:.3f}s"

        except Exception as e:
            self._stats["errors"] += 1
            logger.error("Bulk insert failed", error=str(e), rows=len(parameters_list))
            raise DatabaseError(f"Database bulk insert failed: {e}")

    def execute_transaction(self, operations: list[dict[str, Any]]) -> list[Any]:
        """
        Execute multiple operations in a single transaction using the consolidated connection manager.

        Args:
            operations: List of operation dictionaries with 'query', 'parameters', and 'fetch'

        Returns:
            List of results for each operation
        """
        try:
            start_time = time.time()

            # Convert operations to the format expected by the consolidated manager
            transaction_ops = []
            for op in operations:
                query = op["query"]
                parameters = op.get("parameters")
                # Convert to tuple format expected by execute_transaction
                transaction_ops.append((query, parameters))

            # Use consolidated manager's transaction support
            results = self.db_manager.execute_transaction(transaction_ops)

            execution_time = time.time() - start_time
            self._stats["transaction_operations"] += 1
            logger.info(
                "Transaction completed",
                operations=len(operations),
                execution_time=f"{execution_time:.3f}s",
            )

            return results

        except Exception as e:
            self._stats["errors"] += 1
            logger.error("Transaction failed", error=str(e), operations=len(operations))
            raise DatabaseError(f"Database transaction failed: {e}")

    def test_connection(self) -> bool:
        """Test the database connection."""
        try:
            result = self.execute_read("SELECT 1 as test")
            return result is not None and len(result) > 0
        except Exception as e:
            logger.error("Connection test failed", error=str(e))
            return False

    def is_healthy(self) -> bool:
        """Check if the coordinator is healthy."""
        try:
            return self.db_manager.test_connection()
        except Exception:
            return False

    def get_performance_stats(self) -> dict[str, Any]:
        """Get performance statistics including timing validation metrics."""
        uptime = time.time() - self._stats["start_time"]
        total_ops = (
            self._stats["read_operations"]
            + self._stats["write_operations"]
            + self._stats["bulk_operations"]
            + self._stats["transaction_operations"]
        )

        # Get additional stats from the connection manager
        pool_stats = self.db_manager.get_pool_status()

        # Get timing validation metrics
        timing_metrics = self._get_timing_validation_metrics()

        base_stats = {
            "mode": "postgresql_unified",
            "uptime_seconds": uptime,
            "total_operations": total_ops,
            "read_operations": self._stats["read_operations"],
            "write_operations": self._stats["write_operations"],
            "bulk_operations": self._stats["bulk_operations"],
            "transaction_operations": self._stats["transaction_operations"],
            "errors": self._stats["errors"],
            "operations_per_second": total_ops / uptime if uptime > 0 else 0,
            "error_rate": self._stats["errors"] / total_ops if total_ops > 0 else 0,
            "health": "healthy" if self.is_healthy() else "unhealthy",
            "connection_pool": pool_stats,
        }

        # Add timing validation metrics if available
        if timing_metrics:
            base_stats["timing_validation"] = timing_metrics

        return base_stats

    def _get_timing_validation_metrics(self) -> dict[str, Any] | None:
        """Get timing validation metrics from the database."""
        try:
            # Query current timing validation status
            query = """
            SELECT 
                COUNT(*) as total_splits,
                COUNT(*) FILTER (WHERE is_within_grace_period(game_datetime)) as valid_splits,
                COUNT(*) FILTER (WHERE NOT is_within_grace_period(game_datetime)) as expired_splits,
                ROUND(
                    COUNT(*) FILTER (WHERE NOT is_within_grace_period(game_datetime))::NUMERIC / 
                    NULLIF(COUNT(*)::NUMERIC, 0) * 100, 2
                ) as rejection_rate_percent
            FROM splits.raw_mlb_betting_splits 
            WHERE last_updated >= CURRENT_DATE - INTERVAL '24 hours'
            """

            result = self.execute_read(query)
            if result and len(result) > 0:
                row = result[0]
                return {
                    "total_splits_24h": row["total_splits"]
                    if isinstance(row, dict)
                    else row[0],
                    "valid_splits_24h": row["valid_splits"]
                    if isinstance(row, dict)
                    else row[1],
                    "expired_splits_24h": row["expired_splits"]
                    if isinstance(row, dict)
                    else row[2],
                    "rejection_rate_percent_24h": float(
                        row["rejection_rate_percent"] or 0
                    )
                    if isinstance(row, dict)
                    else float(row[3] or 0),
                    "last_checked": time.time(),
                }

        except Exception as e:
            logger.debug("Could not fetch timing validation metrics", error=str(e))

        return None

    def get_queue_size(self) -> int:
        """Get queue size (compatibility method - always returns 0 for PostgreSQL)."""
        return 0

    def start(self):
        """Start the coordinator (compatibility method - no-op for unified coordinator)."""
        logger.debug(
            "Database coordinator start called (no-op for unified coordinator)"
        )

    def stop(self):
        """Stop the coordinator (compatibility method - no-op for unified coordinator)."""
        logger.debug("Database coordinator stop called (no-op for unified coordinator)")

    def get_timing_validation_status(self) -> dict[str, Any]:
        """
        Get detailed timing validation status for monitoring.

        Returns:
            Dictionary with current timing validation metrics
        """
        try:
            query = """
            SELECT 
                DATE(game_datetime) as game_date,
                COUNT(*) as total_splits,
                COUNT(*) FILTER (WHERE is_within_grace_period(game_datetime)) as valid_splits,
                COUNT(*) FILTER (WHERE NOT is_within_grace_period(game_datetime)) as expired_splits,
                ROUND(
                    COUNT(*) FILTER (WHERE NOT is_within_grace_period(game_datetime))::NUMERIC / 
                    NULLIF(COUNT(*)::NUMERIC, 0) * 100, 2
                ) as rejection_rate_percent
            FROM splits.raw_mlb_betting_splits 
            WHERE last_updated >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY DATE(game_datetime)
            ORDER BY game_date DESC
            LIMIT 7
            """

            results = self.execute_read(query)

            return {
                "daily_metrics": results or [],
                "query_time": time.time(),
                "status": "healthy" if results else "no_data",
            }

        except Exception as e:
            logger.error("Failed to get timing validation status", error=str(e))
            return {
                "daily_metrics": [],
                "query_time": time.time(),
                "status": "error",
                "error": str(e),
            }

    def check_expired_splits(self, hours_back: int = 24) -> dict[str, Any]:
        """
        Check for splits that were stored for games that had already started.

        Args:
            hours_back: Number of hours to look back

        Returns:
            Dictionary with expired splits information
        """
        try:
            query = """
            SELECT 
                game_id,
                home_team,
                away_team,
                game_datetime,
                last_updated,
                EXTRACT(EPOCH FROM (last_updated - game_datetime))/60 as minutes_after_start
            FROM splits.raw_mlb_betting_splits 
            WHERE last_updated >= CURRENT_TIMESTAMP - INTERVAL '%s hours'
              AND NOT is_within_grace_period(game_datetime)
            ORDER BY minutes_after_start DESC
            LIMIT 20
            """

            results = self.execute_read(query, (hours_back,))

            return {
                "expired_splits": results or [],
                "count": len(results) if results else 0,
                "hours_checked": hours_back,
                "query_time": time.time(),
            }

        except Exception as e:
            logger.error("Failed to check expired splits", error=str(e))
            return {
                "expired_splits": [],
                "count": 0,
                "hours_checked": hours_back,
                "query_time": time.time(),
                "error": str(e),
            }

    def get_current_games_status(self) -> dict[str, Any]:
        """
        Get status of current/upcoming games for timing validation.

        Returns:
            Dictionary with current games timing status
        """
        try:
            query = """
            SELECT 
                game_id,
                home_team,
                away_team,
                game_datetime,
                status,
                get_grace_period_status(game_datetime) as timing_status,
                EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - game_datetime))/60 as minutes_since_start
            FROM splits.games
            WHERE game_datetime >= CURRENT_DATE - INTERVAL '6 hours'
              AND game_datetime <= CURRENT_DATE + INTERVAL '18 hours'
            ORDER BY game_datetime
            """

            results = self.execute_read(query)

            # Categorize games by timing status
            status_counts = {}
            for row in results or []:
                timing_status = (
                    row["timing_status"] if isinstance(row, dict) else row[5]
                )
                status_counts[timing_status] = status_counts.get(timing_status, 0) + 1

            return {
                "games": results or [],
                "total_games": len(results) if results else 0,
                "status_breakdown": status_counts,
                "query_time": time.time(),
            }

        except Exception as e:
            logger.error("Failed to get current games status", error=str(e))
            return {
                "games": [],
                "total_games": 0,
                "status_breakdown": {},
                "query_time": time.time(),
                "error": str(e),
            }

    def close(self):
        """Close the coordinator and all connections."""
        try:
            if hasattr(self.db_manager, "close"):
                self.db_manager.close()
            logger.info("Database Coordinator closed")
        except Exception as e:
            logger.error("Error closing coordinator", error=str(e))


# Global coordinator instance
_coordinator_instance: DatabaseCoordinator | None = None


def get_database_coordinator() -> DatabaseCoordinator:
    """
    DEPRECATED: Get the global database coordinator instance.

    ⚠️  DEPRECATION WARNING: This function is deprecated.
    Please use get_data_service() from data_service.py instead.

    This wrapper provides backward compatibility during transition.
    """
    import warnings

    warnings.warn(
        "get_database_coordinator() is deprecated. Use get_data_service() instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    global _coordinator_instance

    if _coordinator_instance is None:
        _coordinator_instance = DatabaseCoordinator()
        logger.info("Created new unified database coordinator instance (DEPRECATED)")

    return _coordinator_instance


# Backward compatibility aliases
def get_postgres_database_coordinator() -> DatabaseCoordinator:
    """
    Backward compatibility alias for PostgreSQL coordinator.
    Now returns the unified coordinator.
    """
    logger.debug("Using backward compatibility alias - returning unified coordinator")
    return get_database_coordinator()


def shutdown_coordinator():
    """Shutdown the global coordinator instance."""
    global _coordinator_instance

    if _coordinator_instance:
        _coordinator_instance.close()
        _coordinator_instance = None
        logger.info("Database coordinator shutdown")


# Alias for backward compatibility
shutdown_postgres_coordinator = shutdown_coordinator


@contextmanager
def coordinated_database_access():
    """
    Context manager for database access with automatic cleanup.

    Usage:
        with coordinated_database_access() as coordinator:
            result = coordinator.execute_read("SELECT * FROM games")
    """
    coordinator = get_database_coordinator()
    try:
        yield coordinator
    except Exception as e:
        logger.error("Database access context error", error=str(e))
        raise


# Backward compatibility alias
postgres_database_access = coordinated_database_access


# Convenience functions for direct usage
def execute_coordinated_query(
    query: str, parameters: tuple | dict | None = None, fetch: bool = True
) -> list[Any] | None:
    """
    Execute a query using the unified coordinator.

    Args:
        query: SQL query to execute
        parameters: Query parameters
        fetch: Whether to fetch results (True for SELECT, False for INSERT/UPDATE/DELETE)

    Returns:
        Query results if fetch=True, None otherwise
    """
    coordinator = get_database_coordinator()

    if fetch:
        return coordinator.execute_read(query, parameters)
    else:
        coordinator.execute_write(query, parameters)
        return None


def execute_coordinated_many(query: str, parameters_list: list[tuple | dict]) -> str:
    """
    Execute a bulk operation using the unified coordinator.

    Args:
        query: SQL query to execute
        parameters_list: List of parameter sets for bulk operation

    Returns:
        Success message with row count
    """
    coordinator = get_database_coordinator()
    return coordinator.execute_bulk_insert(query, parameters_list)


# Backward compatibility aliases
execute_postgres_query = execute_coordinated_query
execute_postgres_many = execute_coordinated_many


# Legacy compatibility wrapper for the old PostgreSQL wrapper
class PostgreSQLCompatibilityWrapper:
    """Compatibility wrapper for existing code that used the separate PostgreSQL coordinator."""

    def __init__(self, coordinator=None):
        self.coordinator = coordinator or get_database_coordinator()

    def execute_read(self, query: str, parameters=None, timeout: float = 30.0):
        return self.coordinator.execute_read(query, parameters, timeout)

    def execute_write(self, query: str, parameters=None, timeout: float = 60.0):
        return self.coordinator.execute_write(query, parameters, timeout)

    def execute_bulk_insert(self, query: str, parameters_list, timeout: float = 300.0):
        return self.coordinator.execute_bulk_insert(query, parameters_list, timeout)

    def is_healthy(self):
        return self.coordinator.is_healthy()

    def get_performance_stats(self):
        return self.coordinator.get_performance_stats()

    def start(self):
        return self.coordinator.start()

    def stop(self):
        return self.coordinator.stop()

    def get_queue_size(self):
        return self.coordinator.get_queue_size()
