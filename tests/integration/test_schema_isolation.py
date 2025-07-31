"""
Integration tests for schema isolation in pipeline testing.

Tests schema manager integration with existing test infrastructure.
"""

import pytest
import asyncio
from datetime import datetime
from uuid import uuid4

from tests.utils.schema_manager import (
    SchemaManager, SchemaConfig, TestSchemaContext, 
    get_schema_manager, pytest_schema_fixture
)
from tests.utils.database_utils import get_test_db_manager
from tests.utils.logging_utils import create_test_logger, setup_secure_test_logging
from tests.utils.retry_utils import RetryManager, RetryConfig


@pytest.mark.integration
@pytest.mark.asyncio
class TestSchemaIsolationIntegration:
    """Test schema isolation with real database operations."""
    
    @pytest.fixture(autouse=True)
    async def setup(self):
        """Setup integration test environment."""
        setup_secure_test_logging()
        self.logger = create_test_logger("schema_isolation_integration")
        
        # Setup database manager
        try:
            self.db_manager = get_test_db_manager()
            await self.db_manager.initialize()
            self.has_database = True
            
            # Setup schema manager with integration-friendly config
            self.schema_config = SchemaConfig(
                prefix="integration_test_schema",
                isolation_level="partial",  # Good balance for integration tests
                include_base_tables=True,
                copy_data=False,  # Don't copy production data
                auto_cleanup=True
            )
            
            self.schema_manager = SchemaManager(self.schema_config)
            await self.schema_manager.initialize(self.db_manager._pool)
            
            self.created_schemas = []
            
        except Exception as e:
            self.logger.warning(f"⚠️ Database not available for integration tests: {e}")
            self.has_database = False
            pytest.skip("Database required for schema integration tests")
        
        yield
        
        # Cleanup
        if self.has_database:
            for schema_name in self.created_schemas:
                try:
                    await self.schema_manager.cleanup_schema(schema_name)
                except:
                    pass
            
            await self.schema_manager.cleanup()
            await self.db_manager.cleanup()
    
    async def test_isolated_schema_data_operations(self):
        """Test data operations in isolated schema."""
        test_id = "data_operations_test"
        
        # Create isolated schema
        schema_name = await self.schema_manager.create_test_schema(test_id)
        self.created_schemas.append(schema_name)
        
        # Verify schema isolation
        validation = await self.schema_manager.validate_schema_isolation(schema_name)
        assert validation["properly_isolated"] is True
        
        self.logger.info(f"✅ Created isolated schema: {schema_name}")
        
        # Test data operations in isolated schema
        async with self.db_manager._pool.acquire() as conn:
            # Set search path to isolated schema
            await conn.execute(f"SET search_path TO {schema_name}, public")
            
            try:
                # Test if we can create test data
                test_table = f"{schema_name}.test_data"
                
                # Insert test data
                test_record_id = str(uuid4())
                insert_sql = f"""
                    INSERT INTO {test_table} (test_identifier, data) 
                    VALUES ($1, $2) 
                    RETURNING id
                """
                
                record_id = await conn.fetchval(
                    insert_sql, 
                    test_id, 
                    '{"test": true, "isolation": "verified"}'
                )
                
                assert record_id is not None
                self.logger.info(f"✅ Inserted test data in isolated schema: record_id={record_id}")
                
                # Verify data exists in isolated schema
                count = await conn.fetchval(f"SELECT COUNT(*) FROM {test_table}")
                assert count == 1
                
                # Retrieve and verify data
                retrieved = await conn.fetchrow(
                    f"SELECT test_identifier, data FROM {test_table} WHERE id = $1",
                    record_id
                )
                
                assert retrieved['test_identifier'] == test_id
                assert '"isolation": "verified"' in retrieved['data']
                
                self.logger.info("✅ Data operations in isolated schema successful")
                
            finally:
                # Reset search path
                await conn.execute("SET search_path TO public")
    
    async def test_schema_isolation_prevents_data_leakage(self):
        """Test that isolated schemas prevent data leakage between tests."""
        test_id_1 = "isolation_test_1"
        test_id_2 = "isolation_test_2"
        
        # Create two isolated schemas
        schema_1 = await self.schema_manager.create_test_schema(test_id_1)
        schema_2 = await self.schema_manager.create_test_schema(test_id_2)
        
        self.created_schemas.extend([schema_1, schema_2])
        
        async with self.db_manager._pool.acquire() as conn:
            # Insert data in schema 1
            await conn.execute(f"SET search_path TO {schema_1}, public")
            
            schema_1_record = await conn.fetchval(
                f"INSERT INTO {schema_1}.test_data (test_identifier, data) VALUES ($1, $2) RETURNING id",
                test_id_1, '{"schema": "1", "secret": "schema1_data"}'
            )
            
            # Insert data in schema 2
            await conn.execute(f"SET search_path TO {schema_2}, public")
            
            schema_2_record = await conn.fetchval(
                f"INSERT INTO {schema_2}.test_data (test_identifier, data) VALUES ($1, $2) RETURNING id",
                test_id_2, '{"schema": "2", "secret": "schema2_data"}'
            )
            
            # Verify isolation - schema 1 should not see schema 2 data
            await conn.execute(f"SET search_path TO {schema_1}, public")
            schema_1_count = await conn.fetchval(f"SELECT COUNT(*) FROM {schema_1}.test_data")
            assert schema_1_count == 1  # Only its own data
            
            schema_1_data = await conn.fetchval(
                f"SELECT data FROM {schema_1}.test_data WHERE test_identifier = $1",
                test_id_1
            )
            assert "schema1_data" in schema_1_data
            assert "schema2_data" not in schema_1_data
            
            # Verify isolation - schema 2 should not see schema 1 data
            await conn.execute(f"SET search_path TO {schema_2}, public")
            schema_2_count = await conn.fetchval(f"SELECT COUNT(*) FROM {schema_2}.test_data")
            assert schema_2_count == 1  # Only its own data
            
            schema_2_data = await conn.fetchval(
                f"SELECT data FROM {schema_2}.test_data WHERE test_identifier = $1",
                test_id_2
            )
            assert "schema2_data" in schema_2_data
            assert "schema1_data" not in schema_2_data
            
            # Reset search path
            await conn.execute("SET search_path TO public")
        
        self.logger.info("✅ Schema isolation prevents data leakage between tests")
    
    async def test_schema_with_retry_resilience(self):
        """Test schema operations with retry logic for resilience."""
        retry_config = RetryConfig.for_database_operations()
        retry_manager = RetryManager(retry_config)
        
        test_id = "retry_resilience_test"
        
        # Schema creation with retry
        async def create_schema_with_retry():
            return await self.schema_manager.create_test_schema(test_id)
        
        schema_name = await retry_manager.execute_async(create_schema_with_retry)
        self.created_schemas.append(schema_name)
        
        # Data operations with retry
        async def insert_test_data():
            async with self.db_manager._pool.acquire() as conn:
                await conn.execute(f"SET search_path TO {schema_name}, public")
                
                try:
                    return await conn.fetchval(
                        f"INSERT INTO {schema_name}.test_data (test_identifier, data) VALUES ($1, $2) RETURNING id",
                        test_id, '{"retry": "tested", "resilient": true}'
                    )
                finally:
                    await conn.execute("SET search_path TO public")
        
        record_id = await retry_manager.execute_async(insert_test_data)
        assert record_id is not None
        
        # Cleanup with retry
        async def cleanup_schema_with_retry():
            await self.schema_manager.cleanup_schema(schema_name)
            self.created_schemas.remove(schema_name)
        
        await retry_manager.execute_async(cleanup_schema_with_retry)
        
        # Verify cleanup
        validation = await self.schema_manager.validate_schema_isolation(schema_name)
        assert validation.get("schema_exists") is False
        
        self.logger.info("✅ Schema operations with retry resilience successful")
    
    async def test_concurrent_schema_operations(self):
        """Test concurrent schema operations for race condition safety."""
        test_count = 5
        test_tasks = []
        
        async def create_and_use_schema(test_id: str):
            """Create schema, use it, and return results."""
            try:
                schema_name = await self.schema_manager.create_test_schema(f"concurrent_{test_id}")
                self.created_schemas.append(schema_name)
                
                # Perform some operations
                async with self.db_manager._pool.acquire() as conn:
                    await conn.execute(f"SET search_path TO {schema_name}, public")
                    
                    try:
                        record_id = await conn.fetchval(
                            f"INSERT INTO {schema_name}.test_data (test_identifier, data) VALUES ($1, $2) RETURNING id",
                            f"concurrent_{test_id}", f'{{"concurrent": true, "id": "{test_id}"}}'
                        )
                        
                        return {
                            "schema_name": schema_name,
                            "record_id": record_id,
                            "success": True
                        }
                    finally:
                        await conn.execute("SET search_path TO public")
                        
            except Exception as e:
                self.logger.error(f"❌ Concurrent schema operation failed for {test_id}: {e}")
                return {"schema_name": None, "record_id": None, "success": False, "error": str(e)}
        
        # Launch concurrent operations
        for i in range(test_count):
            task = asyncio.create_task(create_and_use_schema(str(i)))
            test_tasks.append(task)
        
        # Wait for all operations to complete
        results = await asyncio.gather(*test_tasks, return_exceptions=True)
        
        # Verify results
        successful_results = [r for r in results if isinstance(r, dict) and r.get("success")]
        assert len(successful_results) == test_count, f"Expected {test_count} successful operations, got {len(successful_results)}"
        
        # Verify schema uniqueness
        schema_names = [r["schema_name"] for r in successful_results]
        assert len(set(schema_names)) == test_count, "All schema names should be unique"
        
        self.logger.info(f"✅ Concurrent schema operations successful: {test_count} schemas created")
    
    async def test_schema_statistics_and_monitoring(self):
        """Test schema statistics collection and monitoring."""
        test_id = "statistics_monitoring_test"
        
        schema_name = await self.schema_manager.create_test_schema(test_id)
        self.created_schemas.append(schema_name)
        
        # Perform some operations to generate statistics
        async with self.db_manager._pool.acquire() as conn:
            await conn.execute(f"SET search_path TO {schema_name}, public")
            
            try:
                # Insert multiple records
                for i in range(5):
                    await conn.execute(
                        f"INSERT INTO {schema_name}.test_data (test_identifier, data) VALUES ($1, $2)",
                        f"{test_id}_{i}", f'{{"iteration": {i}, "stats": "test"}}'
                    )
                
                # Insert test results
                await conn.execute(
                    f"INSERT INTO {schema_name}.test_results (test_name, status, metrics) VALUES ($1, $2, $3)",
                    test_id, "completed", '{"records": 5, "success": true}'
                )
                
            finally:
                await conn.execute("SET search_path TO public")
        
        # Collect statistics
        stats = await self.schema_manager.get_schema_statistics(schema_name)
        
        assert "schema_name" in stats
        assert stats["schema_name"] == schema_name
        assert "table_count" in stats
        assert stats["table_count"] >= 2  # At least test_data and test_results
        assert "total_rows" in stats
        assert stats["total_rows"] >= 6  # 5 test_data + 1 test_results
        
        # Verify table-specific statistics
        assert "tables" in stats
        table_stats = {table["table_name"]: table for table in stats["tables"]}
        
        if "test_data" in table_stats:
            assert table_stats["test_data"]["live_rows"] == 5
        
        if "test_results" in table_stats:
            assert table_stats["test_results"]["live_rows"] == 1
        
        self.logger.info(f"✅ Schema statistics collected: {stats['total_rows']} total rows in {stats['table_count']} tables")


@pytest.mark.integration
@pytest.mark.asyncio 
class TestSchemaContextIntegration:
    """Test schema context manager integration."""
    
    @pytest.fixture(autouse=True)
    async def setup(self):
        """Setup context integration test environment."""
        setup_secure_test_logging()
        self.logger = create_test_logger("schema_context_integration")
        
        try:
            self.db_manager = get_test_db_manager()
            await self.db_manager.initialize()
            self.has_database = True
        except Exception as e:
            self.logger.warning(f"⚠️ Database not available: {e}")
            self.has_database = False
            pytest.skip("Database required for context integration tests")
        
        yield
        
        if self.has_database:
            await self.db_manager.cleanup()
    
    async def test_context_manager_integration(self):
        """Test context manager with real database operations."""
        test_id = "context_integration_test"
        config = SchemaConfig(
            prefix="context_integration_schema",
            isolation_level="minimal",
            auto_cleanup=True
        )
        
        created_schema_name = None
        
        # Use context manager
        async with TestSchemaContext(test_id, config) as schema_name:
            created_schema_name = schema_name
            self.logger.info(f"📝 In context with schema: {schema_name}")
            
            # Perform operations in context
            manager = await get_schema_manager()
            validation = await manager.validate_schema_isolation(schema_name)
            assert validation["properly_isolated"] is True
            
            # Test data operations
            async with self.db_manager._pool.acquire() as conn:
                await conn.execute(f"SET search_path TO {schema_name}, public")
                
                try:
                    record_id = await conn.fetchval(
                        f"INSERT INTO {schema_name}.test_data (test_identifier, data) VALUES ($1, $2) RETURNING id",
                        test_id, '{"context": "manager", "test": "integration"}'
                    )
                    
                    assert record_id is not None
                    self.logger.info(f"✅ Context operations successful: record_id={record_id}")
                    
                finally:
                    await conn.execute("SET search_path TO public")
        
        # Verify cleanup after context
        self.logger.info(f"🧹 Verifying cleanup of schema: {created_schema_name}")
        
        manager = await get_schema_manager()
        validation = await manager.validate_schema_isolation(created_schema_name)
        assert validation.get("schema_exists") is False
        
        self.logger.info("✅ Context manager integration with cleanup successful")
    
    async def test_context_manager_error_handling(self):
        """Test context manager error handling and cleanup."""
        test_id = "context_error_handling_test"
        config = SchemaConfig(
            prefix="context_error_schema",
            isolation_level="minimal"
        )
        
        created_schema_name = None
        
        try:
            async with TestSchemaContext(test_id, config) as schema_name:
                created_schema_name = schema_name
                self.logger.info(f"📝 Schema created: {schema_name}")
                
                # Verify schema exists
                manager = await get_schema_manager()
                validation = await manager.validate_schema_isolation(schema_name)
                assert validation["schema_exists"] is True
                
                # Simulate an error in the context
                raise ValueError("Simulated error in schema context")
                
        except ValueError as e:
            assert str(e) == "Simulated error in schema context"
            self.logger.info("✅ Expected error caught in context")
        
        # Verify cleanup happened despite error
        if created_schema_name:
            manager = await get_schema_manager()
            validation = await manager.validate_schema_isolation(created_schema_name)
            assert validation.get("schema_exists") is False
            
        self.logger.info("✅ Context manager error handling and cleanup successful")


# Utility functions for integration testing
async def verify_schema_isolation_complete(schema_manager: SchemaManager, schema_name: str) -> Dict[str, Any]:
    """Comprehensive verification of schema isolation."""
    validation = await schema_manager.validate_schema_isolation(schema_name)
    stats = await schema_manager.get_schema_statistics(schema_name)
    
    return {
        "validation": validation,
        "statistics": stats,
        "fully_isolated": (
            validation.get("properly_isolated", False) and
            stats.get("table_count", 0) > 0
        )
    }


if __name__ == "__main__":
    # Run integration test
    async def quick_integration_test():
        logger = create_test_logger("quick_integration_test")
        logger.info("🧪 Running quick schema isolation integration test...")
        
        try:
            # Test basic schema manager functionality
            config = SchemaConfig(prefix="quick_test_schema", isolation_level="minimal")
            manager = SchemaManager(config)
            
            # Test name generation
            schema_name = manager.generate_schema_name("quick_test")
            assert schema_name.startswith("quick_test_schema_")
            logger.info(f"✅ Schema name generation: {schema_name}")
            
            # Test configuration
            assert config.prefix == "quick_test_schema"
            assert config.isolation_level == "minimal"
            logger.info("✅ Schema configuration validated")
            
            logger.info("✅ Quick integration test completed (database operations skipped)")
            
        except Exception as e:
            logger.error(f"❌ Quick integration test failed: {e}")
    
    asyncio.run(quick_integration_test())