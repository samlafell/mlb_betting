"""
Unified Database Connection Management

Consolidates database connection patterns from all legacy modules.
Provides connection pooling, async support, transaction management,
and comprehensive error handling for PostgreSQL 17.
"""

import asyncio
import contextlib
from typing import Any, AsyncContextManager, Dict, List, Optional, Union
from urllib.parse import urlparse

import asyncpg
import psycopg2
import psycopg2.pool
from psycopg2.extras import RealDictCursor

from ...core.config import UnifiedSettings
from ...core.exceptions import DatabaseError
from ...core.logging import LogComponent, get_logger

logger = get_logger(__name__, LogComponent.DATABASE)


class DatabaseConnection:
    """
    Unified database connection with both sync and async support.
    
    Provides connection management, transaction handling, and query execution
    with comprehensive error handling and logging.
    """
    
    def __init__(
        self,
        connection_string: str,
        *,
        pool_size: int = 10,
        max_overflow: int = 20,
        pool_timeout: int = 30,
        pool_recycle: int = 3600,
        enable_async: bool = True,
        enable_sync: bool = True,
    ):
        """
        Initialize database connection.
        
        Args:
            connection_string: PostgreSQL connection string
            pool_size: Base connection pool size
            max_overflow: Maximum overflow connections
            pool_timeout: Pool timeout in seconds
            pool_recycle: Pool recycle time in seconds
            enable_async: Enable async connection support
            enable_sync: Enable sync connection support
        """
        self.connection_string = connection_string
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.pool_timeout = pool_timeout
        self.pool_recycle = pool_recycle
        self.enable_async = enable_async
        self.enable_sync = enable_sync
        
        # Parse connection string
        self._parse_connection_string()
        
        # Connection pools
        self._async_pool: Optional[asyncpg.Pool] = None
        self._sync_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None
        
        # Connection state
        self._is_connected = False
        self._connection_lock = asyncio.Lock()
    
    def _parse_connection_string(self) -> None:
        """Parse connection string and extract components."""
        try:
            parsed = urlparse(self.connection_string)
            self.host = parsed.hostname or "localhost"
            self.port = parsed.port or 5432
            self.database = parsed.path.lstrip("/") if parsed.path else "postgres"
            self.username = parsed.username or "postgres"
            self.password = parsed.password or ""
            
            logger.info(
                "Parsed database connection string",
                operation="connection_parsing",
                extra={
                    "host": self.host,
                    "port": self.port,
                    "database": self.database,
                    "username": self.username,
                }
            )
        except Exception as e:
            raise DatabaseError(
                f"Failed to parse connection string: {str(e)}",
                operation="connection_parsing",
                cause=e
            )
    
    async def connect(self) -> None:
        """Establish database connections."""
        async with self._connection_lock:
            if self._is_connected:
                return
            
            start_time = logger.log_operation_start("database_connect")
            
            try:
                # Create async pool
                if self.enable_async:
                    self._async_pool = await asyncpg.create_pool(
                        host=self.host,
                        port=self.port,
                        user=self.username,
                        password=self.password,
                        database=self.database,
                        min_size=self.pool_size // 2,
                        max_size=self.pool_size + self.max_overflow,
                        command_timeout=self.pool_timeout,
                        server_settings={
                            'application_name': 'unified_betting_system',
                            'timezone': 'America/New_York',  # EST timezone
                        }
                    )
                    
                    logger.info(
                        "Created async connection pool",
                        operation="async_pool_creation",
                        extra={
                            "min_size": self.pool_size // 2,
                            "max_size": self.pool_size + self.max_overflow,
                        }
                    )
                
                # Create sync pool
                if self.enable_sync:
                    self._sync_pool = psycopg2.pool.ThreadedConnectionPool(
                        minconn=self.pool_size // 2,
                        maxconn=self.pool_size + self.max_overflow,
                        host=self.host,
                        port=self.port,
                        user=self.username,
                        password=self.password,
                        database=self.database,
                        cursor_factory=RealDictCursor,
                        options="-c timezone=America/New_York"  # EST timezone
                    )
                    
                    logger.info(
                        "Created sync connection pool",
                        operation="sync_pool_creation",
                        extra={
                            "min_connections": self.pool_size // 2,
                            "max_connections": self.pool_size + self.max_overflow,
                        }
                    )
                
                self._is_connected = True
                logger.log_operation_end("database_connect", start_time, success=True)
                
            except Exception as e:
                logger.log_operation_end("database_connect", start_time, success=False, error=e)
                raise DatabaseError(
                    f"Failed to establish database connection: {str(e)}",
                    operation="database_connect",
                    cause=e,
                    details={
                        "host": self.host,
                        "port": self.port,
                        "database": self.database,
                    }
                )
    
    async def disconnect(self) -> None:
        """Close database connections."""
        async with self._connection_lock:
            if not self._is_connected:
                return
            
            start_time = logger.log_operation_start("database_disconnect")
            
            try:
                # Close async pool
                if self._async_pool:
                    await self._async_pool.close()
                    self._async_pool = None
                    logger.info("Closed async connection pool", operation="async_pool_close")
                
                # Close sync pool
                if self._sync_pool:
                    self._sync_pool.closeall()
                    self._sync_pool = None
                    logger.info("Closed sync connection pool", operation="sync_pool_close")
                
                self._is_connected = False
                logger.log_operation_end("database_disconnect", start_time, success=True)
                
            except Exception as e:
                logger.log_operation_end("database_disconnect", start_time, success=False, error=e)
                raise DatabaseError(
                    f"Failed to close database connection: {str(e)}",
                    operation="database_disconnect",
                    cause=e
                )
    
    @contextlib.asynccontextmanager
    async def get_async_connection(self) -> AsyncContextManager[asyncpg.Connection]:
        """Get async database connection from pool."""
        if not self._is_connected or not self._async_pool:
            await self.connect()
        
        if not self._async_pool:
            raise DatabaseError(
                "Async connection pool not available",
                operation="get_async_connection"
            )
        
        async with self._async_pool.acquire() as connection:
            try:
                yield connection
            except Exception as e:
                # Log connection error
                logger.error(
                    "Error with async database connection",
                    operation="async_connection_error",
                    error=e
                )
                raise DatabaseError(
                    f"Database connection error: {str(e)}",
                    operation="async_connection_error",
                    cause=e
                )
    
    @contextlib.contextmanager
    def get_sync_connection(self):
        """Get sync database connection from pool."""
        if not self._is_connected or not self._sync_pool:
            # Need to connect synchronously
            import asyncio
            asyncio.run(self.connect())
        
        if not self._sync_pool:
            raise DatabaseError(
                "Sync connection pool not available",
                operation="get_sync_connection"
            )
        
        connection = None
        try:
            connection = self._sync_pool.getconn()
            yield connection
        except Exception as e:
            logger.error(
                "Error with sync database connection",
                operation="sync_connection_error",
                error=e
            )
            raise DatabaseError(
                f"Database connection error: {str(e)}",
                operation="sync_connection_error",
                cause=e
            )
        finally:
            if connection:
                self._sync_pool.putconn(connection)
    
    async def execute_async(
        self,
        query: str,
        *args,
        fetch: str = "none",
        **kwargs
    ) -> Any:
        """
        Execute async query.
        
        Args:
            query: SQL query
            *args: Query parameters
            fetch: Fetch mode (none, one, all)
            **kwargs: Additional parameters
            
        Returns:
            Query result based on fetch mode
        """
        start_time = logger.log_operation_start("async_query_execution")
        
        try:
            async with self.get_async_connection() as conn:
                logger.log_database_query(
                    query,
                    params=list(args) if args else None,
                    **kwargs
                )
                
                if fetch == "all":
                    result = await conn.fetch(query, *args)
                elif fetch == "one":
                    result = await conn.fetchrow(query, *args)
                else:
                    result = await conn.execute(query, *args)
                
                logger.log_operation_end("async_query_execution", start_time, success=True)
                return result
                
        except Exception as e:
            logger.log_operation_end("async_query_execution", start_time, success=False, error=e)
            raise DatabaseError(
                f"Failed to execute async query: {str(e)}",
                operation="async_query_execution",
                cause=e,
                details={
                    "query": query[:200] + "..." if len(query) > 200 else query,
                    "fetch_mode": fetch,
                }
            )
    
    def execute_sync(
        self,
        query: str,
        params: Optional[Union[List, Dict]] = None,
        *,
        fetch: str = "none",
        **kwargs
    ) -> Any:
        """
        Execute sync query.
        
        Args:
            query: SQL query
            params: Query parameters
            fetch: Fetch mode (none, one, all)
            **kwargs: Additional parameters
            
        Returns:
            Query result based on fetch mode
        """
        start_time = logger.log_operation_start("sync_query_execution")
        
        try:
            with self.get_sync_connection() as conn:
                with conn.cursor() as cursor:
                    logger.log_database_query(
                        query,
                        params=params,
                        **kwargs
                    )
                    
                    cursor.execute(query, params)
                    
                    if fetch == "all":
                        result = cursor.fetchall()
                    elif fetch == "one":
                        result = cursor.fetchone()
                    else:
                        result = cursor.rowcount
                    
                    conn.commit()
                    
                    logger.log_operation_end("sync_query_execution", start_time, success=True)
                    return result
                    
        except Exception as e:
            logger.log_operation_end("sync_query_execution", start_time, success=False, error=e)
            raise DatabaseError(
                f"Failed to execute sync query: {str(e)}",
                operation="sync_query_execution",
                cause=e,
                details={
                    "query": query[:200] + "..." if len(query) > 200 else query,
                    "fetch_mode": fetch,
                }
            )
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform database health check."""
        start_time = logger.log_operation_start("database_health_check")
        
        try:
            health_info = {
                "connected": self._is_connected,
                "async_pool_available": self._async_pool is not None,
                "sync_pool_available": self._sync_pool is not None,
            }
            
            if self._async_pool:
                health_info.update({
                    "async_pool_size": self._async_pool.get_size(),
                    "async_pool_idle": self._async_pool.get_idle_size(),
                })
            
            if self._sync_pool:
                health_info.update({
                    "sync_pool_min": self._sync_pool.minconn,
                    "sync_pool_max": self._sync_pool.maxconn,
                })
            
            # Test connection
            if self._is_connected:
                try:
                    result = await self.execute_async("SELECT 1", fetch="one")
                    health_info["connection_test"] = "passed"
                except Exception as e:
                    health_info["connection_test"] = "failed"
                    health_info["connection_error"] = str(e)
            
            logger.log_operation_end("database_health_check", start_time, success=True)
            return health_info
            
        except Exception as e:
            logger.log_operation_end("database_health_check", start_time, success=False, error=e)
            raise DatabaseError(
                f"Database health check failed: {str(e)}",
                operation="database_health_check",
                cause=e
            )


class ConnectionPool:
    """
    Connection pool manager for multiple database connections.
    
    Manages multiple database connections with different configurations
    and provides unified access patterns.
    """
    
    def __init__(self):
        """Initialize connection pool manager."""
        self._connections: Dict[str, DatabaseConnection] = {}
        self._default_connection: Optional[str] = None
        
        logger.info("Initialized connection pool manager", operation="pool_manager_init")
    
    def add_connection(
        self,
        name: str,
        connection_string: str,
        *,
        is_default: bool = False,
        **kwargs
    ) -> None:
        """
        Add database connection to pool.
        
        Args:
            name: Connection name
            connection_string: PostgreSQL connection string
            is_default: Whether this is the default connection
            **kwargs: Additional connection parameters
        """
        if name in self._connections:
            logger.warning(
                f"Connection '{name}' already exists, replacing",
                operation="add_connection",
                extra={"connection_name": name}
            )
        
        self._connections[name] = DatabaseConnection(connection_string, **kwargs)
        
        if is_default or not self._default_connection:
            self._default_connection = name
        
        logger.info(
            f"Added database connection: {name}",
            operation="add_connection",
            extra={
                "connection_name": name,
                "is_default": is_default,
            }
        )
    
    def get_connection(self, name: Optional[str] = None) -> DatabaseConnection:
        """
        Get database connection by name.
        
        Args:
            name: Connection name (uses default if None)
            
        Returns:
            DatabaseConnection instance
        """
        connection_name = name or self._default_connection
        
        if not connection_name:
            raise DatabaseError(
                "No database connection specified and no default connection set",
                operation="get_connection"
            )
        
        if connection_name not in self._connections:
            raise DatabaseError(
                f"Database connection '{connection_name}' not found",
                operation="get_connection",
                details={"connection_name": connection_name}
            )
        
        return self._connections[connection_name]
    
    async def connect_all(self) -> None:
        """Connect all database connections."""
        start_time = logger.log_operation_start("connect_all_connections")
        
        try:
            for name, connection in self._connections.items():
                await connection.connect()
                logger.info(
                    f"Connected to database: {name}",
                    operation="connect_all_connections",
                    extra={"connection_name": name}
                )
            
            logger.log_operation_end("connect_all_connections", start_time, success=True)
            
        except Exception as e:
            logger.log_operation_end("connect_all_connections", start_time, success=False, error=e)
            raise DatabaseError(
                f"Failed to connect all database connections: {str(e)}",
                operation="connect_all_connections",
                cause=e
            )
    
    async def disconnect_all(self) -> None:
        """Disconnect all database connections."""
        start_time = logger.log_operation_start("disconnect_all_connections")
        
        try:
            for name, connection in self._connections.items():
                await connection.disconnect()
                logger.info(
                    f"Disconnected from database: {name}",
                    operation="disconnect_all_connections",
                    extra={"connection_name": name}
                )
            
            logger.log_operation_end("disconnect_all_connections", start_time, success=True)
            
        except Exception as e:
            logger.log_operation_end("disconnect_all_connections", start_time, success=False, error=e)
            raise DatabaseError(
                f"Failed to disconnect all database connections: {str(e)}",
                operation="disconnect_all_connections",
                cause=e
            )
    
    async def health_check_all(self) -> Dict[str, Any]:
        """Perform health check on all connections."""
        start_time = logger.log_operation_start("health_check_all_connections")
        
        try:
            health_results = {}
            
            for name, connection in self._connections.items():
                try:
                    health_results[name] = await connection.health_check()
                except Exception as e:
                    health_results[name] = {
                        "error": str(e),
                        "healthy": False,
                    }
            
            logger.log_operation_end("health_check_all_connections", start_time, success=True)
            return health_results
            
        except Exception as e:
            logger.log_operation_end("health_check_all_connections", start_time, success=False, error=e)
            raise DatabaseError(
                f"Failed to health check all database connections: {str(e)}",
                operation="health_check_all_connections",
                cause=e
            )


# Global connection pool instance
_connection_pool = ConnectionPool()


def get_connection_pool() -> ConnectionPool:
    """Get the global connection pool instance."""
    return _connection_pool


def get_connection(name: Optional[str] = None) -> DatabaseConnection:
    """Get database connection from global pool."""
    return _connection_pool.get_connection(name)


async def close_all_connections() -> None:
    """Close all database connections."""
    await _connection_pool.disconnect_all()


def initialize_connections(settings: UnifiedSettings) -> None:
    """
    Initialize database connections from settings.
    
    Args:
        settings: Unified settings instance
    """
    logger.info("Initializing database connections", operation="initialize_connections")
    
    # Add main database connection
    _connection_pool.add_connection(
        name="main",
        connection_string=settings.database.connection_string,
        is_default=True,
        pool_size=settings.database.pool_size,
        max_overflow=settings.database.max_overflow,
        pool_timeout=settings.database.pool_timeout,
        pool_recycle=settings.database.pool_recycle,
    )
    
    # Add read replica if configured
    if settings.database.read_replica_connection_string:
        _connection_pool.add_connection(
            name="read_replica",
            connection_string=settings.database.read_replica_connection_string,
            pool_size=settings.database.pool_size // 2,
            max_overflow=settings.database.max_overflow // 2,
            pool_timeout=settings.database.pool_timeout,
            pool_recycle=settings.database.pool_recycle,
        )
    
    logger.info("Database connections initialized", operation="initialize_connections") 