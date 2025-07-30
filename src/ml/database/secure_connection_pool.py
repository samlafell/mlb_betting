"""
Secure Connection Pool Management
Production-grade database and Redis connection management with security features
"""

import os
import ssl
import asyncio
import logging
from typing import Optional, Dict, Any, Union
from contextlib import asynccontextmanager
from dataclasses import dataclass

import asyncpg
import redis.asyncio as redis
from redis.asyncio.connection import ConnectionPool as RedisConnectionPool
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.pool import NullPool, QueuePool
from pydantic import BaseModel

logger = logging.getLogger(__name__)


@dataclass
class SecurityConfig:
    """Security configuration for connections"""

    require_ssl: bool = True
    ssl_ca_path: Optional[str] = None
    ssl_cert_path: Optional[str] = None
    ssl_key_path: Optional[str] = None
    verify_ssl: bool = True
    min_tls_version: str = "1.2"


class DatabaseSecurityConfig(BaseModel):
    """Database security configuration"""

    host: str = os.getenv("DB_HOST", "localhost")
    port: int = int(os.getenv("DB_PORT", "5432"))
    database: str = os.getenv("DB_NAME", "mlb_betting")
    username: str = os.getenv("DB_USER", "ml_user")
    password: str = os.getenv("DB_PASSWORD", "")

    # SSL Configuration
    ssl_mode: str = os.getenv("DB_SSL_MODE", "require")
    ssl_cert: Optional[str] = os.getenv("DB_SSL_CERT")
    ssl_key: Optional[str] = os.getenv("DB_SSL_KEY")
    ssl_rootcert: Optional[str] = os.getenv("DB_SSL_ROOTCERT")

    # Connection Pool Configuration
    pool_size: int = int(os.getenv("DB_POOL_SIZE", "10"))
    max_overflow: int = int(os.getenv("DB_MAX_OVERFLOW", "20"))
    pool_timeout: int = int(os.getenv("DB_POOL_TIMEOUT", "30"))
    pool_recycle: int = int(os.getenv("DB_POOL_RECYCLE", "3600"))  # 1 hour

    # Security Settings
    statement_timeout: int = int(
        os.getenv("DB_STATEMENT_TIMEOUT", "30000")
    )  # 30 seconds
    idle_in_transaction_session_timeout: int = int(
        os.getenv("DB_IDLE_TIMEOUT", "300000")
    )  # 5 minutes

    def get_ssl_context(self) -> Optional[ssl.SSLContext]:
        """Create SSL context for database connections"""
        if self.ssl_mode == "disable":
            return None

        context = ssl.create_default_context()

        if self.ssl_mode == "require":
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        elif self.ssl_mode == "verify-ca":
            context.check_hostname = False
            context.verify_mode = ssl.CERT_REQUIRED
            if self.ssl_rootcert:
                context.load_verify_locations(self.ssl_rootcert)
        elif self.ssl_mode == "verify-full":
            context.check_hostname = True
            context.verify_mode = ssl.CERT_REQUIRED
            if self.ssl_rootcert:
                context.load_verify_locations(self.ssl_rootcert)

        # Load client certificates if provided
        if self.ssl_cert and self.ssl_key:
            context.load_cert_chain(self.ssl_cert, self.ssl_key)

        return context

    def get_database_url(self) -> str:
        """Generate secure database connection URL"""
        ssl_params = []

        if self.ssl_mode != "disable":
            ssl_params.append(f"ssl={self.ssl_mode}")

            if self.ssl_cert:
                ssl_params.append(f"sslcert={self.ssl_cert}")
            if self.ssl_key:
                ssl_params.append(f"sslkey={self.ssl_key}")
            if self.ssl_rootcert:
                ssl_params.append(f"sslrootcert={self.ssl_rootcert}")

        ssl_query = "&".join(ssl_params)
        query_separator = "?" if ssl_query else ""

        return (
            f"postgresql+asyncpg://{self.username}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}"
            f"{query_separator}{ssl_query}"
        )


class RedisSecurityConfig(BaseModel):
    """Redis security configuration"""

    host: str = os.getenv("REDIS_HOST", "localhost")
    port: int = int(os.getenv("REDIS_PORT", "6379"))
    database: int = int(os.getenv("REDIS_DB", "0"))
    password: Optional[str] = os.getenv("REDIS_PASSWORD")
    username: Optional[str] = os.getenv("REDIS_USERNAME", "default")

    # TLS Configuration
    ssl_enabled: bool = os.getenv("REDIS_SSL_ENABLED", "false").lower() == "true"
    ssl_cert_reqs: str = os.getenv("REDIS_SSL_CERT_REQS", "required")
    ssl_ca_certs: Optional[str] = os.getenv("REDIS_SSL_CA_CERTS")
    ssl_certfile: Optional[str] = os.getenv("REDIS_SSL_CERTFILE")
    ssl_keyfile: Optional[str] = os.getenv("REDIS_SSL_KEYFILE")

    # Connection Pool Configuration
    max_connections: int = int(os.getenv("REDIS_MAX_CONNECTIONS", "50"))
    retry_on_timeout: bool = True
    socket_timeout: int = int(os.getenv("REDIS_SOCKET_TIMEOUT", "5"))
    socket_connect_timeout: int = int(os.getenv("REDIS_CONNECT_TIMEOUT", "5"))

    # Security Settings
    socket_keepalive: bool = True
    socket_keepalive_options: Dict[int, int] = {
        1: 1,  # TCP_KEEPIDLE
        2: 3,  # TCP_KEEPINTVL
        3: 5,  # TCP_KEEPCNT
    }

    def get_ssl_context(self) -> Optional[ssl.SSLContext]:
        """Create SSL context for Redis connections"""
        if not self.ssl_enabled:
            return None

        context = ssl.create_default_context()

        # Configure certificate verification
        if self.ssl_cert_reqs == "none":
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        elif self.ssl_cert_reqs == "optional":
            context.check_hostname = False
            context.verify_mode = ssl.CERT_OPTIONAL
        else:  # required
            context.check_hostname = True
            context.verify_mode = ssl.CERT_REQUIRED

        # Load CA certificates
        if self.ssl_ca_certs:
            context.load_verify_locations(self.ssl_ca_certs)

        # Load client certificates
        if self.ssl_certfile and self.ssl_keyfile:
            context.load_cert_chain(self.ssl_certfile, self.ssl_keyfile)

        return context


class SecureConnectionManager:
    """Secure connection management for database and Redis"""

    def __init__(self):
        self.db_config = DatabaseSecurityConfig()
        self.redis_config = RedisSecurityConfig()
        self.db_engine = None
        self.session_factory = None
        self.redis_pool = None
        self._redis_client = None

    async def initialize_database(self) -> None:
        """Initialize secure database connection pool"""
        try:
            # Create SSL context
            ssl_context = self.db_config.get_ssl_context()

            # Configure connection arguments
            connect_args = {
                "server_settings": {
                    "application_name": "mlb_ml_api",
                    "statement_timeout": str(self.db_config.statement_timeout),
                    "idle_in_transaction_session_timeout": str(
                        self.db_config.idle_in_transaction_session_timeout
                    ),
                }
            }

            if ssl_context:
                connect_args["ssl"] = ssl_context

            # Create async engine with security settings
            self.db_engine = create_async_engine(
                self.db_config.get_database_url(),
                poolclass=QueuePool,
                pool_size=self.db_config.pool_size,
                max_overflow=self.db_config.max_overflow,
                pool_timeout=self.db_config.pool_timeout,
                pool_recycle=self.db_config.pool_recycle,
                pool_pre_ping=True,  # Validate connections
                connect_args=connect_args,
                echo=os.getenv("DB_ECHO", "false").lower() == "true",
            )

            # Create session factory
            self.session_factory = async_sessionmaker(
                self.db_engine, class_=AsyncSession, expire_on_commit=False
            )

            logger.info("✅ Database connection pool initialized successfully")

        except Exception as e:
            logger.error(f"❌ Failed to initialize database connection pool: {e}")
            raise

    async def initialize_redis(self) -> None:
        """Initialize secure Redis connection pool"""
        try:
            # Configure connection parameters
            connection_kwargs = {
                "host": self.redis_config.host,
                "port": self.redis_config.port,
                "db": self.redis_config.database,
                "username": self.redis_config.username,
                "password": self.redis_config.password,
                "socket_timeout": self.redis_config.socket_timeout,
                "socket_connect_timeout": self.redis_config.socket_connect_timeout,
                "socket_keepalive": self.redis_config.socket_keepalive,
                "socket_keepalive_options": self.redis_config.socket_keepalive_options,
                "retry_on_timeout": self.redis_config.retry_on_timeout,
                "decode_responses": True,
                "protocol": 3,  # RESP3 protocol
            }

            # Add SSL configuration if enabled
            if self.redis_config.ssl_enabled:
                ssl_context = self.redis_config.get_ssl_context()
                connection_kwargs.update(
                    {
                        "ssl": True,
                        "ssl_context": ssl_context,
                    }
                )

            # Create secure connection pool
            self.redis_pool = RedisConnectionPool(
                max_connections=self.redis_config.max_connections, **connection_kwargs
            )

            # Create Redis client
            self._redis_client = redis.Redis(connection_pool=self.redis_pool)

            # Test connection
            await self._redis_client.ping()

            logger.info("✅ Redis connection pool initialized successfully")

        except Exception as e:
            logger.error(f"❌ Failed to initialize Redis connection pool: {e}")
            raise

    @asynccontextmanager
    async def get_database_session(self):
        """Get secure database session with automatic cleanup"""
        if not self.session_factory:
            raise RuntimeError("Database not initialized")

        async with self.session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

    async def get_redis_client(self) -> redis.Redis:
        """Get Redis client with connection pool"""
        if not self._redis_client:
            raise RuntimeError("Redis not initialized")
        return self._redis_client

    async def health_check(self) -> Dict[str, bool]:
        """Check health of all connections"""
        health_status = {"database": False, "redis": False}

        # Check database
        try:
            if self.db_engine:
                async with self.get_database_session() as session:
                    result = await session.execute("SELECT 1")
                    health_status["database"] = bool(result.scalar())
        except Exception as e:
            logger.warning(f"Database health check failed: {e}")

        # Check Redis
        try:
            if self._redis_client:
                pong = await self._redis_client.ping()
                health_status["redis"] = pong
        except Exception as e:
            logger.warning(f"Redis health check failed: {e}")

        return health_status

    async def close_connections(self) -> None:
        """Gracefully close all connections"""
        try:
            # Close Redis connections
            if self._redis_client:
                await self._redis_client.aclose()
                logger.info("✅ Redis connections closed")

            # Close database connections
            if self.db_engine:
                await self.db_engine.dispose()
                logger.info("✅ Database connections closed")

        except Exception as e:
            logger.error(f"❌ Error closing connections: {e}")


class ResourceMonitor:
    """Monitor connection pool and resource usage"""

    def __init__(self, connection_manager: SecureConnectionManager):
        self.connection_manager = connection_manager

    async def get_pool_stats(self) -> Dict[str, Any]:
        """Get connection pool statistics"""
        stats = {}

        # Database pool stats
        if self.connection_manager.db_engine:
            pool = self.connection_manager.db_engine.pool
            stats["database"] = {
                "size": pool.size(),
                "checked_in": pool.checkedin(),
                "checked_out": pool.checkedout(),
                "overflow": pool.overflow(),
                "invalid": pool.invalid(),
            }

        # Redis pool stats
        if self.connection_manager.redis_pool:
            stats["redis"] = {
                "max_connections": self.connection_manager.redis_pool.max_connections,
                "created_connections": len(
                    self.connection_manager.redis_pool._created_connections
                ),
                "available_connections": len(
                    self.connection_manager.redis_pool._available_connections
                ),
                "in_use_connections": len(
                    self.connection_manager.redis_pool._in_use_connections
                ),
            }

        return stats

    async def monitor_resources(self) -> Dict[str, Any]:
        """Monitor resource usage and performance"""
        health = await self.connection_manager.health_check()
        pool_stats = await self.get_pool_stats()

        return {
            "timestamp": asyncio.get_event_loop().time(),
            "health": health,
            "pool_stats": pool_stats,
            "alerts": self._check_alerts(pool_stats),
        }

    def _check_alerts(self, pool_stats: Dict[str, Any]) -> list:
        """Check for resource alerts"""
        alerts = []

        # Database alerts
        if "database" in pool_stats:
            db_stats = pool_stats["database"]

            # High connection usage
            if db_stats["checked_out"] > (db_stats["size"] * 0.8):
                alerts.append(
                    {
                        "severity": "warning",
                        "component": "database",
                        "message": "High database connection usage",
                        "usage": f"{db_stats['checked_out']}/{db_stats['size']}",
                    }
                )

            # Connection overflow
            if db_stats["overflow"] > 0:
                alerts.append(
                    {
                        "severity": "warning",
                        "component": "database",
                        "message": "Database connection overflow detected",
                        "overflow": db_stats["overflow"],
                    }
                )

        # Redis alerts
        if "redis" in pool_stats:
            redis_stats = pool_stats["redis"]

            # High connection usage
            usage_ratio = (
                redis_stats["in_use_connections"] / redis_stats["max_connections"]
            )
            if usage_ratio > 0.8:
                alerts.append(
                    {
                        "severity": "warning",
                        "component": "redis",
                        "message": "High Redis connection usage",
                        "usage": f"{redis_stats['in_use_connections']}/{redis_stats['max_connections']}",
                    }
                )

        return alerts


# Global connection manager instance
connection_manager = SecureConnectionManager()
resource_monitor = ResourceMonitor(connection_manager)


async def get_secure_db_session():
    """Dependency for getting secure database session"""
    async with connection_manager.get_database_session() as session:
        yield session


async def get_secure_redis_client():
    """Dependency for getting secure Redis client"""
    return await connection_manager.get_redis_client()


# Export key components
__all__ = [
    "SecureConnectionManager",
    "DatabaseSecurityConfig",
    "RedisSecurityConfig",
    "ResourceMonitor",
    "connection_manager",
    "resource_monitor",
    "get_secure_db_session",
    "get_secure_redis_client",
]
