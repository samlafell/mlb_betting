"""
Schema manager for test-specific database isolation.

Provides dynamic schema creation, isolation, and cleanup for tests.
"""

import asyncio
import hashlib
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set
from uuid import uuid4

import asyncpg

from tests.utils.logging_utils import create_test_logger
from tests.utils.database_utils import sanitize_db_config
from src.core.config import get_settings


@dataclass
class SchemaConfig:
    """Configuration for test schema management."""
    
    prefix: str = "test_schema"
    auto_cleanup: bool = True
    max_age_hours: int = 24
    include_base_tables: bool = True
    copy_data: bool = False
    isolation_level: str = "full"  # full, partial, minimal


@dataclass 
class SchemaInfo:
    """Information about a test schema."""
    
    name: str
    created_at: datetime
    test_identifier: str
    config: SchemaConfig
    tables_created: List[str]
    is_isolated: bool


class SchemaManager:
    """Manages test-specific database schemas for isolation."""
    
    def __init__(self, config: SchemaConfig = None):
        self.config = config or SchemaConfig()
        self.logger = create_test_logger("schema_manager")
        self._connection_pool: Optional[asyncpg.Pool] = None
        self._managed_schemas: Dict[str, SchemaInfo] = {}
        self._base_schemas = ["raw_data", "staging", "analytics", "public"]
        self._cleanup_running = False
    
    async def initialize(self, connection_pool: asyncpg.Pool = None):
        """Initialize schema manager with database connection."""
        if connection_pool:
            self._connection_pool = connection_pool
        else:
            # Create our own connection pool if not provided
            settings = get_settings()
            db_config = settings.database
            
            sanitized_config = sanitize_db_config(db_config.model_dump())
            self.logger.log_dict("debug", "Database configuration", sanitized_config)
            
            self._connection_pool = await asyncpg.create_pool(
                host=db_config.host,
                port=db_config.port,
                database=db_config.database,
                user=db_config.username,
                password=db_config.password,
                min_size=2,
                max_size=5
            )
        
        self.logger.info("✅ Schema manager initialized")
    
    def generate_schema_name(self, test_identifier: str) -> str:
        """Generate unique schema name for test."""
        # Create deterministic but unique schema name
        timestamp = int(time.time())
        test_hash = hashlib.md5(test_identifier.encode()).hexdigest()[:8]
        return f"{self.config.prefix}_{test_hash}_{timestamp}"
    
    async def create_test_schema(self, test_identifier: str, config: SchemaConfig = None) -> str:
        """Create isolated test schema."""
        if not self._connection_pool:
            raise RuntimeError("Schema manager not initialized")
        
        schema_config = config or self.config
        schema_name = self.generate_schema_name(test_identifier)
        
        async with self._connection_pool.acquire() as conn:
            try:
                # Create the schema
                await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                self.logger.info(f"📝 Created test schema: {schema_name}")
                
                tables_created = []
                
                if schema_config.include_base_tables:
                    tables_created = await self._create_base_tables(conn, schema_name, schema_config)
                
                # Record schema info
                schema_info = SchemaInfo(
                    name=schema_name,
                    created_at=datetime.utcnow(),
                    test_identifier=test_identifier,
                    config=schema_config,
                    tables_created=tables_created,
                    is_isolated=True
                )
                
                self._managed_schemas[schema_name] = schema_info
                
                self.logger.info(f"✅ Test schema ready: {schema_name} with {len(tables_created)} tables")
                return schema_name
                
            except Exception as e:
                self.logger.error(f"❌ Failed to create test schema {schema_name}: {e}")
                # Clean up on failure
                try:
                    await conn.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
                except:
                    pass
                raise
    
    async def _create_base_tables(self, conn: asyncpg.Connection, schema_name: str, config: SchemaConfig) -> List[str]:
        """Create base tables in test schema."""
        tables_created = []
        
        try:
            if config.isolation_level == "full":
                # Copy full table structures from all base schemas
                for base_schema in self._base_schemas:
                    schema_tables = await self._get_schema_tables(conn, base_schema)
                    for table_name in schema_tables:
                        await self._copy_table_structure(conn, base_schema, table_name, schema_name)
                        tables_created.append(f"{base_schema}.{table_name}")
                        
                        if config.copy_data:
                            await self._copy_table_data(conn, base_schema, table_name, schema_name)
            
            elif config.isolation_level == "partial":
                # Only copy critical tables for testing
                critical_tables = {
                    "raw_data": ["action_network_odds", "action_network_history"],
                    "staging": ["action_network_games", "action_network_odds_historical"],
                    "analytics": ["strategy_results"]
                }
                
                for base_schema, table_list in critical_tables.items():
                    for table_name in table_list:
                        if await self._table_exists(conn, base_schema, table_name):
                            await self._copy_table_structure(conn, base_schema, table_name, schema_name)
                            tables_created.append(f"{base_schema}.{table_name}")
            
            elif config.isolation_level == "minimal":
                # Create minimal test-specific tables
                await self._create_minimal_test_tables(conn, schema_name)
                tables_created = ["test_data", "test_results"]
            
            self.logger.info(f"📊 Created {len(tables_created)} tables in {schema_name}")
            return tables_created
            
        except Exception as e:
            self.logger.error(f"❌ Failed to create base tables in {schema_name}: {e}")
            raise
    
    async def _get_schema_tables(self, conn: asyncpg.Connection, schema_name: str) -> List[str]:
        """Get list of tables in schema."""
        try:
            rows = await conn.fetch("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = $1 
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """, schema_name)
            return [row['table_name'] for row in rows]
        except:
            return []
    
    async def _table_exists(self, conn: asyncpg.Connection, schema_name: str, table_name: str) -> bool:
        """Check if table exists in schema."""
        try:
            count = await conn.fetchval("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = $1 AND table_name = $2
            """, schema_name, table_name)
            return count > 0
        except:
            return False
    
    async def _copy_table_structure(self, conn: asyncpg.Connection, source_schema: str, 
                                  table_name: str, target_schema: str):
        """Copy table structure to test schema."""
        try:
            # Get table creation DDL
            create_table_sql = f"""
                CREATE TABLE {target_schema}.{table_name} 
                (LIKE {source_schema}.{table_name} INCLUDING ALL)
            """
            await conn.execute(create_table_sql)
            
            # Copy indexes (optional)
            try:
                await self._copy_table_indexes(conn, source_schema, table_name, target_schema)
            except Exception as e:
                self.logger.warning(f"⚠️ Could not copy indexes for {table_name}: {e}")
                
        except Exception as e:
            self.logger.warning(f"⚠️ Could not copy table {source_schema}.{table_name}: {e}")
    
    async def _copy_table_indexes(self, conn: asyncpg.Connection, source_schema: str, 
                                table_name: str, target_schema: str):
        """Copy table indexes to test schema."""
        # Get indexes for the table
        indexes = await conn.fetch("""
            SELECT indexname, indexdef 
            FROM pg_indexes 
            WHERE schemaname = $1 AND tablename = $2
            AND indexname NOT LIKE '%_pkey'
        """, source_schema, table_name)
        
        for index in indexes:
            try:
                # Modify index definition for target schema
                index_def = index['indexdef']
                new_index_name = f"{target_schema}_{index['indexname']}"
                modified_def = index_def.replace(
                    f"{source_schema}.{table_name}", 
                    f"{target_schema}.{table_name}"
                ).replace(index['indexname'], new_index_name)
                
                await conn.execute(modified_def)
            except Exception as e:
                self.logger.warning(f"⚠️ Could not copy index {index['indexname']}: {e}")
    
    async def _copy_table_data(self, conn: asyncpg.Connection, source_schema: str, 
                             table_name: str, target_schema: str):
        """Copy table data to test schema."""
        try:
            copy_sql = f"""
                INSERT INTO {target_schema}.{table_name} 
                SELECT * FROM {source_schema}.{table_name}
                LIMIT 1000
            """
            rows_copied = await conn.execute(copy_sql)
            self.logger.info(f"📋 Copied data to {target_schema}.{table_name}: {rows_copied}")
        except Exception as e:
            self.logger.warning(f"⚠️ Could not copy data for {table_name}: {e}")
    
    async def _create_minimal_test_tables(self, conn: asyncpg.Connection, schema_name: str):
        """Create minimal tables for basic testing."""
        test_tables = [
            f"""
            CREATE TABLE {schema_name}.test_data (
                id SERIAL PRIMARY KEY,
                test_identifier VARCHAR(255),
                data JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            f"""
            CREATE TABLE {schema_name}.test_results (
                id SERIAL PRIMARY KEY,
                test_name VARCHAR(255),
                status VARCHAR(50),
                metrics JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        ]
        
        for table_sql in test_tables:
            await conn.execute(table_sql)
    
    async def get_schema_connection_string(self, schema_name: str) -> str:
        """Get connection string with schema search path."""
        if schema_name not in self._managed_schemas:
            raise ValueError(f"Schema {schema_name} not managed by this instance")
        
        settings = get_settings()
        db_config = settings.database
        
        return (
            f"postgresql://{db_config.username}:{db_config.password}@"
            f"{db_config.host}:{db_config.port}/{db_config.database}"
            f"?options=-c search_path={schema_name},public"
        )
    
    async def execute_in_schema(self, schema_name: str, query: str, *params) -> Any:
        """Execute query in specific test schema."""
        if not self._connection_pool:
            raise RuntimeError("Schema manager not initialized")
        
        async with self._connection_pool.acquire() as conn:
            # Set search path to test schema
            await conn.execute(f"SET search_path TO {schema_name}, public")
            
            try:
                if params:
                    result = await conn.fetchval(query, *params)
                else:
                    result = await conn.execute(query)
                return result
            finally:
                # Reset search path
                await conn.execute("SET search_path TO public")
    
    async def get_schema_info(self, schema_name: str) -> Optional[SchemaInfo]:
        """Get information about a managed schema."""
        return self._managed_schemas.get(schema_name)
    
    async def list_managed_schemas(self) -> List[SchemaInfo]:
        """List all managed schemas."""
        return list(self._managed_schemas.values())
    
    async def cleanup_schema(self, schema_name: str):
        """Clean up specific test schema."""
        if not self._connection_pool:
            return
        
        try:
            async with self._connection_pool.acquire() as conn:
                await conn.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
            
            if schema_name in self._managed_schemas:
                del self._managed_schemas[schema_name]
            
            self.logger.info(f"🧹 Cleaned up test schema: {schema_name}")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to cleanup schema {schema_name}: {e}")
    
    async def cleanup_expired_schemas(self):
        """Clean up expired test schemas."""
        if self._cleanup_running:
            return
        
        self._cleanup_running = True
        
        try:
            if not self._connection_pool:
                return
            
            cutoff_time = datetime.utcnow() - timedelta(hours=self.config.max_age_hours)
            expired_schemas = []
            
            for schema_name, schema_info in self._managed_schemas.items():
                if schema_info.created_at < cutoff_time:
                    expired_schemas.append(schema_name)
            
            # Also check for orphaned schemas in database
            async with self._connection_pool.acquire() as conn:
                db_schemas = await conn.fetch("""
                    SELECT schema_name 
                    FROM information_schema.schemata 
                    WHERE schema_name LIKE $1
                """, f"{self.config.prefix}_%")
                
                for row in db_schemas:
                    schema_name = row['schema_name']
                    if schema_name not in self._managed_schemas:
                        # Orphaned schema - check if it's old enough to clean
                        try:
                            # Extract timestamp from schema name
                            parts = schema_name.split('_')
                            if len(parts) >= 3:
                                timestamp = int(parts[-1])
                                schema_time = datetime.fromtimestamp(timestamp)
                                if schema_time < cutoff_time:
                                    expired_schemas.append(schema_name)
                        except:
                            # If we can't parse timestamp, consider it expired
                            expired_schemas.append(schema_name)
            
            # Clean up expired schemas
            for schema_name in expired_schemas:
                await self.cleanup_schema(schema_name)
            
            if expired_schemas:
                self.logger.info(f"🧹 Cleaned up {len(expired_schemas)} expired test schemas")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to cleanup expired schemas: {e}")
        finally:
            self._cleanup_running = False
    
    async def cleanup_all_managed_schemas(self):
        """Clean up all managed schemas."""
        schema_names = list(self._managed_schemas.keys())
        for schema_name in schema_names:
            await self.cleanup_schema(schema_name)
        
        self.logger.info(f"🧹 Cleaned up all {len(schema_names)} managed schemas")
    
    async def get_schema_statistics(self, schema_name: str) -> Dict[str, Any]:
        """Get statistics about a test schema."""
        if not self._connection_pool:
            return {}
        
        try:
            async with self._connection_pool.acquire() as conn:
                # Get table statistics
                stats = await conn.fetch("""
                    SELECT 
                        t.table_name,
                        COALESCE(s.n_tup_ins, 0) as inserts,
                        COALESCE(s.n_tup_upd, 0) as updates,
                        COALESCE(s.n_tup_del, 0) as deletes,
                        COALESCE(s.n_live_tup, 0) as live_rows
                    FROM information_schema.tables t
                    LEFT JOIN pg_stat_user_tables s ON s.relname = t.table_name
                    WHERE t.table_schema = $1
                    ORDER BY t.table_name
                """, schema_name)
                
                return {
                    "schema_name": schema_name,
                    "table_count": len(stats),
                    "tables": [dict(row) for row in stats],
                    "total_rows": sum(row['live_rows'] for row in stats),
                    "total_operations": sum(
                        row['inserts'] + row['updates'] + row['deletes'] 
                        for row in stats
                    )
                }
                
        except Exception as e:
            self.logger.error(f"❌ Failed to get schema statistics for {schema_name}: {e}")
            return {}
    
    async def validate_schema_isolation(self, schema_name: str) -> Dict[str, bool]:
        """Validate that schema is properly isolated."""
        if not self._connection_pool:
            return {"isolated": False, "error": "No connection pool"}
        
        try:
            async with self._connection_pool.acquire() as conn:
                # Check if schema exists
                schema_exists = await conn.fetchval("""
                    SELECT COUNT(*) FROM information_schema.schemata 
                    WHERE schema_name = $1
                """, schema_name)
                
                if not schema_exists:
                    return {"isolated": False, "error": "Schema does not exist"}
                
                # Check if schema has its own tables
                table_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_schema = $1
                """, schema_name)
                
                # Check if schema is in search path (should not be for isolation)
                search_path = await conn.fetchval("SHOW search_path")
                in_search_path = schema_name in search_path
                
                return {
                    "isolated": True,
                    "schema_exists": bool(schema_exists),
                    "has_tables": table_count > 0,
                    "table_count": table_count,
                    "in_search_path": in_search_path,
                    "properly_isolated": bool(schema_exists) and table_count > 0 and not in_search_path
                }
                
        except Exception as e:
            self.logger.error(f"❌ Failed to validate schema isolation for {schema_name}: {e}")
            return {"isolated": False, "error": str(e)}
    
    async def cleanup(self):
        """Clean up schema manager."""
        if self.config.auto_cleanup:
            await self.cleanup_all_managed_schemas()
        
        if self._connection_pool:
            await self._connection_pool.close()
        
        self.logger.info("✅ Schema manager cleanup completed")


# Global schema manager instance
_global_schema_manager: Optional[SchemaManager] = None


async def get_schema_manager(config: SchemaConfig = None) -> SchemaManager:
    """Get global schema manager instance."""
    global _global_schema_manager
    
    if _global_schema_manager is None:
        _global_schema_manager = SchemaManager(config)
        await _global_schema_manager.initialize()
    
    return _global_schema_manager


async def create_test_schema(test_identifier: str, config: SchemaConfig = None) -> str:
    """Convenience function to create test schema."""
    manager = await get_schema_manager()
    return await manager.create_test_schema(test_identifier, config)


async def cleanup_test_schema(schema_name: str):
    """Convenience function to cleanup test schema."""
    manager = await get_schema_manager()
    await manager.cleanup_schema(schema_name)


# Context manager for test schema isolation
class TestSchemaContext:
    """Context manager for isolated test schema."""
    
    def __init__(self, test_identifier: str, config: SchemaConfig = None):
        self.test_identifier = test_identifier
        self.config = config
        self.schema_name: Optional[str] = None
        self.manager: Optional[SchemaManager] = None
    
    async def __aenter__(self) -> str:
        """Create and enter test schema context."""
        self.manager = await get_schema_manager(self.config)
        self.schema_name = await self.manager.create_test_schema(
            self.test_identifier, self.config
        )
        return self.schema_name
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Clean up test schema context."""
        if self.manager and self.schema_name:
            await self.manager.cleanup_schema(self.schema_name)


# Pytest fixtures for schema isolation
def pytest_schema_fixture(config: SchemaConfig = None):
    """Create pytest fixture for schema isolation."""
    import pytest
    
    @pytest.fixture
    async def isolated_schema(request):
        """Provide isolated test schema."""
        test_id = f"{request.node.name}_{uuid4().hex[:8]}"
        
        async with TestSchemaContext(test_id, config) as schema_name:
            yield schema_name
    
    return isolated_schema