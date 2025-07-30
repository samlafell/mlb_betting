"""
Database Connection Pool Management
Async connection pool for ML pipeline database operations
"""

import asyncio
import logging
from typing import Optional, Dict, Any, AsyncContextManager
from contextlib import asynccontextmanager

import asyncpg
from asyncpg import Pool, Connection
from pydantic import BaseModel

from ...core.config import get_settings

logger = logging.getLogger(__name__)


class DatabaseConfig(BaseModel):
    """Database configuration model"""
    host: str
    port: int
    database: str
    user: str
    password: str
    min_size: int = 5
    max_size: int = 20
    max_queries: int = 50000
    max_inactive_connection_lifetime: float = 300.0
    timeout: float = 60.0


class DatabaseConnectionPool:
    """Async database connection pool manager"""
    
    def __init__(self, config: Optional[DatabaseConfig] = None):
        self.config = config or self._get_default_config()
        self.pool: Optional[Pool] = None
        self._lock = asyncio.Lock()
    
    def _get_default_config(self) -> DatabaseConfig:
        """Get default database config from settings"""
        settings = get_settings()
        return DatabaseConfig(
            host=settings.database.host,
            port=settings.database.port,
            database=settings.database.database,
            user=settings.database.user,
            password=settings.database.password
        )
    
    async def initialize(self) -> None:
        """Initialize the connection pool"""
        async with self._lock:
            if self.pool is not None:
                logger.warning("Connection pool already initialized")
                return
            
            try:
                logger.info("Initializing database connection pool...")
                
                self.pool = await asyncpg.create_pool(
                    host=self.config.host,
                    port=self.config.port,
                    database=self.config.database,
                    user=self.config.user,
                    password=self.config.password,
                    min_size=self.config.min_size,
                    max_size=self.config.max_size,
                    max_queries=self.config.max_queries,
                    max_inactive_connection_lifetime=self.config.max_inactive_connection_lifetime,
                    timeout=self.config.timeout,
                    command_timeout=30.0,
                    server_settings={
                        'application_name': 'mlb_ml_pipeline',
                        'timezone': 'UTC'
                    }
                )
                
                # Test the connection
                async with self.pool.acquire() as conn:
                    result = await conn.fetchval("SELECT 1")
                    if result != 1:
                        raise Exception("Database connection test failed")
                
                logger.info(f"✅ Database connection pool initialized with {self.config.min_size}-{self.config.max_size} connections")
                
            except Exception as e:
                logger.error(f"❌ Failed to initialize database connection pool: {e}")
                raise
    
    async def close(self) -> None:
        """Close the connection pool"""
        async with self._lock:
            if self.pool is not None:
                logger.info("Closing database connection pool...")
                await self.pool.close()
                self.pool = None
                logger.info("✅ Database connection pool closed")
    
    @asynccontextmanager
    async def get_connection(self) -> AsyncContextManager[Connection]:
        """Get a connection from the pool with automatic cleanup"""
        if not self.pool:
            raise RuntimeError("Connection pool not initialized. Call initialize() first.")
        
        connection = None
        try:
            connection = await self.pool.acquire()
            logger.debug("Acquired database connection from pool")
            yield connection
        except asyncpg.PostgresError as e:
            logger.error(f"Database error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected database error: {e}")
            raise
        finally:
            if connection:
                await self.pool.release(connection)
                logger.debug("Released database connection back to pool")
    
    @asynccontextmanager
    async def get_transaction(self) -> AsyncContextManager[Connection]:
        """Get a connection with automatic transaction management"""
        async with self.get_connection() as conn:
            async with conn.transaction():
                try:
                    yield conn
                except Exception:
                    # Transaction will be automatically rolled back
                    logger.debug("Transaction rolled back due to exception")
                    raise
                else:
                    # Transaction will be automatically committed
                    logger.debug("Transaction committed successfully")
    
    async def execute(self, query: str, *args) -> str:
        """Execute a command and return status"""
        async with self.get_connection() as conn:
            return await conn.execute(query, *args)
    
    async def fetch(self, query: str, *args) -> list:
        """Fetch all rows"""
        async with self.get_connection() as conn:
            return await conn.fetch(query, *args)
    
    async def fetchrow(self, query: str, *args) -> Optional[asyncpg.Record]:
        """Fetch one row"""
        async with self.get_connection() as conn:
            return await conn.fetchrow(query, *args)
    
    async def fetchval(self, query: str, *args) -> Any:
        """Fetch a single value"""
        async with self.get_connection() as conn:
            return await conn.fetchval(query, *args)
    
    async def executemany(self, query: str, args_list: list) -> None:
        """Execute many commands with different parameters"""
        async with self.get_transaction() as conn:
            await conn.executemany(query, args_list)
    
    async def get_pool_stats(self) -> Dict[str, Any]:
        """Get connection pool statistics"""
        if not self.pool:
            return {"status": "not_initialized"}
        
        return {
            "status": "active",
            "size": self.pool.get_size(),
            "max_size": self.config.max_size,
            "min_size": self.config.min_size,
            "idle_connections": self.pool.get_idle_size(),
            "max_queries": self.config.max_queries,
            "timeout": self.config.timeout
        }
    
    async def health_check(self) -> Dict[str, Any]:
        """Health check for the connection pool"""
        try:
            if not self.pool:
                return {"status": "unhealthy", "error": "Pool not initialized"}
            
            # Test connection with timeout
            async with asyncio.wait_for(self.get_connection(), timeout=5.0) as conn:
                result = await conn.fetchval("SELECT 1")
                
            if result == 1:
                stats = await self.get_pool_stats()
                return {
                    "status": "healthy",
                    "pool_stats": stats,
                    "test_query": "passed"
                }
            else:
                return {"status": "unhealthy", "error": "Test query failed"}
                
        except asyncio.TimeoutError:
            return {"status": "unhealthy", "error": "Connection timeout"}
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}


# Global connection pool instance
_connection_pool: Optional[DatabaseConnectionPool] = None


async def get_connection_pool() -> DatabaseConnectionPool:
    """Get the global connection pool instance"""
    global _connection_pool
    
    if _connection_pool is None:
        _connection_pool = DatabaseConnectionPool()
        await _connection_pool.initialize()
    
    return _connection_pool


async def close_connection_pool() -> None:
    """Close the global connection pool"""
    global _connection_pool
    
    if _connection_pool:
        await _connection_pool.close()
        _connection_pool = None


# Convenience functions for common operations
async def execute_query(query: str, *args) -> str:
    """Execute a query using the global pool"""
    pool = await get_connection_pool()
    return await pool.execute(query, *args)


async def fetch_all(query: str, *args) -> list:
    """Fetch all rows using the global pool"""
    pool = await get_connection_pool()
    return await pool.fetch(query, *args)


async def fetch_one(query: str, *args) -> Optional[asyncpg.Record]:
    """Fetch one row using the global pool"""
    pool = await get_connection_pool()
    return await pool.fetchrow(query, *args)


async def fetch_value(query: str, *args) -> Any:
    """Fetch a single value using the global pool"""
    pool = await get_connection_pool()
    return await pool.fetchval(query, *args)


@asynccontextmanager
async def get_db_connection() -> AsyncContextManager[Connection]:
    """Get a database connection with automatic cleanup"""
    pool = await get_connection_pool()
    async with pool.get_connection() as conn:
        yield conn


@asynccontextmanager
async def get_db_transaction() -> AsyncContextManager[Connection]:
    """Get a database transaction with automatic commit/rollback"""
    pool = await get_connection_pool()
    async with pool.get_transaction() as conn:
        yield conn