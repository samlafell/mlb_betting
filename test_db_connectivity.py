#!/usr/bin/env python3
"""
Quick test of database connectivity
"""
from src.core.config import get_settings
from src.data.database.connection import initialize_connections, get_connection
import asyncio

async def test_basic_connectivity():
    try:
        config = get_settings()
        print('✅ Config loaded')
        
        # Initialize connections
        initialize_connections(config)
        print('✅ Connections initialized')
        
        # Test connection
        async with get_connection() as conn:
            result = await conn.fetchval('SELECT 1')
            print(f'✅ Database connection successful: {result}')
            
            # Check schemas
            schemas = await conn.fetch('SELECT schema_name FROM information_schema.schemata ORDER BY schema_name')
            schema_names = [row["schema_name"] for row in schemas]
            print(f'📊 Available schemas: {schema_names}')
            
            # Check pipeline zones specifically
            pipeline_schemas = ['raw_data', 'staging', 'curated']
            for schema in pipeline_schemas:
                if schema in schema_names:
                    print(f'✅ {schema} schema exists')
                else:
                    print(f'❌ {schema} schema missing')
                    
    except Exception as e:
        print(f'❌ Connection error: {e}')
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_basic_connectivity())