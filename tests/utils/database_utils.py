"""
Secure database utilities for testing.

Provides secure database operations, connection pooling, and credential sanitization.
"""

import asyncio
import logging
import re
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, AsyncGenerator
from unittest.mock import Mock

import asyncpg

from src.core.config import get_settings
from src.data.database.connection_pool import create_db_pool


logger = logging.getLogger(__name__)


def sanitize_connection_string(connection_string: str) -> str:
    """
    Sanitize database connection string by masking sensitive information.
    
    Args:
        connection_string: Database connection string that may contain credentials
        
    Returns:
        Sanitized connection string with masked credentials
    """
    # Pattern to match password in connection strings
    password_pattern = r'(password=)[^&;\s]*'
    sanitized = re.sub(password_pattern, r'\1****', connection_string, flags=re.IGNORECASE)
    
    # Pattern to match password in URLs
    url_password_pattern = r'(://[^:]*:)[^@]*(@)'
    sanitized = re.sub(url_password_pattern, r'\1****\2', sanitized)
    
    return sanitized


def sanitize_db_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Sanitize database configuration by masking sensitive fields.
    
    Args:
        config: Database configuration dictionary
        
    Returns:
        Sanitized configuration with masked sensitive fields
    """
    sanitized = config.copy()
    sensitive_fields = ['password', 'passwd', 'pass', 'secret', 'token', 'key']
    
    for field in sensitive_fields:
        if field in sanitized:
            sanitized[field] = '****'
            
    # Handle nested configurations
    for key, value in sanitized.items():
        if isinstance(value, dict):
            sanitized[key] = sanitize_db_config(value)
            
    return sanitized


class TestDatabaseManager:
    """
    Manages database connections and operations for testing with security best practices.
    """
    
    def __init__(self):
        self._pool: Optional[asyncpg.Pool] = None
        self._config = get_settings()
        self._test_data_tracking: Dict[str, List[str]] = {}
        
    async def initialize(self) -> None:
        """Initialize database connection pool."""
        if self._pool is None:
            try:
                self._pool = await create_db_pool()
                logger.info("Test database pool initialized successfully")
            except Exception as e:
                sanitized_config = sanitize_db_config(self._config.database.model_dump())
                logger.error(f"Failed to initialize test database pool: {e}")
                logger.debug(f"Database config (sanitized): {sanitized_config}")
                raise
    
    async def cleanup(self) -> None:
        """Clean up database connections."""
        if self._pool:
            await self._pool.close()
            self._pool = None
            logger.info("Test database pool closed successfully")
    
    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[asyncpg.Connection, None]:
        """
        Get a database connection with proper resource management.
        
        Yields:
            Database connection that is automatically returned to pool
        """
        if not self._pool:
            await self.initialize()
            
        async with self._pool.acquire() as conn:
            try:
                yield conn
            except Exception as e:
                # Log error without exposing sensitive data
                logger.error(f"Database operation failed: {type(e).__name__}: {str(e)}")
                raise
    
    async def execute_safe_query(
        self, 
        query: str, 
        params: Optional[List[Any]] = None,
        connection: Optional[asyncpg.Connection] = None
    ) -> Any:
        """
        Execute a parameterized query safely.
        
        Args:
            query: SQL query with parameter placeholders
            params: Query parameters (prevents SQL injection)
            connection: Existing connection to use (optional)
            
        Returns:
            Query result
            
        Raises:
            ValueError: If query appears to use string interpolation
        """
        # Basic check for potential SQL injection patterns
        if any(pattern in query.lower() for pattern in ['%s', '.format', 'f"', "f'"]):
            raise ValueError(
                "Query appears to use string interpolation. Use parameterized queries only."
            )
        
        if connection:
            return await connection.execute(query, *(params or []))
        else:
            async with self.get_connection() as conn:
                return await conn.execute(query, *(params or []))
    
    async def fetch_safe_query(
        self,
        query: str,
        params: Optional[List[Any]] = None,
        connection: Optional[asyncpg.Connection] = None
    ) -> List[asyncpg.Record]:
        """
        Fetch results from a parameterized query safely.
        
        Args:
            query: SQL query with parameter placeholders
            params: Query parameters (prevents SQL injection)
            connection: Existing connection to use (optional)
            
        Returns:
            Query results
        """
        # Basic check for potential SQL injection patterns
        if any(pattern in query.lower() for pattern in ['%s', '.format', 'f"', "f'"]):
            raise ValueError(
                "Query appears to use string interpolation. Use parameterized queries only."
            )
        
        if connection:
            return await connection.fetch(query, *(params or []))
        else:
            async with self.get_connection() as conn:
                return await conn.fetch(query, *(params or []))
    
    def track_test_data(self, test_name: str, external_ids: List[str]) -> None:
        """
        Track test data for cleanup.
        
        Args:
            test_name: Name of the test creating the data
            external_ids: List of external IDs to track for cleanup
        """
        if test_name not in self._test_data_tracking:
            self._test_data_tracking[test_name] = []
        self._test_data_tracking[test_name].extend(external_ids)
    
    async def cleanup_test_data(self, test_name: str) -> None:
        """
        Clean up tracked test data for a specific test.
        
        Args:
            test_name: Name of the test to clean up data for
        """
        if test_name not in self._test_data_tracking:
            return
            
        external_ids = list(set(self._test_data_tracking[test_name]))  # Remove duplicates
        if not external_ids:
            return
            
        async with self.get_connection() as conn:
            # Clean up in reverse dependency order using parameterized queries
            cleanup_queries = [
                "DELETE FROM staging.action_network_odds_historical WHERE external_game_id = ANY($1)",
                "DELETE FROM staging.action_network_games WHERE external_game_id = ANY($1)",
                "DELETE FROM raw_data.action_network_odds WHERE external_game_id = ANY($1)",
                "DELETE FROM raw_data.action_network_history WHERE external_game_id = ANY($1)",
            ]
            
            for query in cleanup_queries:
                try:
                    await conn.execute(query, external_ids)
                except Exception as e:
                    logger.warning(f"Failed to clean up data with query {query[:50]}...: {e}")
        
        # Clear tracking for this test
        del self._test_data_tracking[test_name]
        logger.info(f"Cleaned up test data for {test_name}: {len(external_ids)} records")
    
    async def cleanup_all_test_data(self) -> None:
        """Clean up all tracked test data."""
        for test_name in list(self._test_data_tracking.keys()):
            await self.cleanup_test_data(test_name)


class MockDatabaseManager:
    """
    Mock database manager for unit tests that don't require real database access.
    """
    
    def __init__(self):
        self.connection_mock = Mock()
        self.pool_mock = Mock()
        self._data_store: Dict[str, List[Dict[str, Any]]] = {}
    
    async def initialize(self) -> None:
        """Mock initialization."""
        pass
    
    async def cleanup(self) -> None:
        """Mock cleanup."""
        pass
    
    @asynccontextmanager
    async def get_connection(self):
        """Yield mock connection."""
        yield self.connection_mock
    
    def set_query_result(self, query_pattern: str, result: Any) -> None:
        """
        Set mock result for queries matching a pattern.
        
        Args:
            query_pattern: Pattern to match in SQL queries
            result: Result to return for matching queries
        """
        self.connection_mock.execute.return_value = result
        self.connection_mock.fetch.return_value = result
        self.connection_mock.fetchrow.return_value = result
        self.connection_mock.fetchval.return_value = result


# Global test database manager instance
_test_db_manager: Optional[TestDatabaseManager] = None


def get_test_db_manager() -> TestDatabaseManager:
    """
    Get the global test database manager instance.
    
    Returns:
        TestDatabaseManager instance
    """
    global _test_db_manager
    if _test_db_manager is None:
        _test_db_manager = TestDatabaseManager()
    return _test_db_manager


async def cleanup_test_environment():
    """Clean up the test environment."""
    global _test_db_manager
    if _test_db_manager:
        await _test_db_manager.cleanup_all_test_data()
        await _test_db_manager.cleanup()
        _test_db_manager = None