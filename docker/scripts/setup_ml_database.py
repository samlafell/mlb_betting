#!/usr/bin/env python3
"""
Setup ML Database Tables for Docker Compose Environment
Ensures ML database schema is properly initialized
"""

import asyncio
import asyncpg
import os
import sys
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent / 'src'))

from core.config import DatabaseSettings


async def setup_ml_database():
    """Setup ML database tables if they don't exist"""
    
    # Get database configuration
    try:
        db_config = DatabaseSettings()
        
        # Database connection parameters
        dsn = f"postgresql://{db_config.user}:{db_config.password}@{db_config.host}:{db_config.port}/{db_config.database}"
        
        print(f"Connecting to database: {db_config.host}:{db_config.port}/{db_config.database}")
        
        # Connect to database
        conn = await asyncpg.connect(dsn)
        
        # Check if curated schema exists
        schema_exists = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'curated')"
        )
        
        if not schema_exists:
            print("Creating curated schema...")
            await conn.execute("CREATE SCHEMA IF NOT EXISTS curated")
        
        # List of ML migrations to run
        ml_migrations = [
            '011_create_ml_curated_zone.sql',
            '012_create_ml_features_part2.sql', 
            '013_create_ml_prediction_tables.sql',
            '014_create_ml_views_and_indexes.sql'
        ]
        
        # Execute each migration
        migrations_dir = Path(__file__).parent.parent.parent / 'sql' / 'migrations'
        
        for migration_file in ml_migrations:
            migration_path = migrations_dir / migration_file
            
            if migration_path.exists():
                print(f"Running migration: {migration_file}")
                
                with open(migration_path, 'r') as f:
                    sql_content = f.read()
                
                try:
                    # Execute the migration SQL
                    await conn.execute(sql_content)
                    print(f"✅ Successfully applied {migration_file}")
                    
                except Exception as e:
                    print(f"⚠️  Warning applying {migration_file}: {e}")
                    # Continue with other migrations
                    continue
            else:
                print(f"❌ Migration file not found: {migration_path}")
        
        # Verify key tables exist
        tables_to_check = [
            'curated.enhanced_games',
            'curated.ml_predictions', 
            'curated.ml_model_performance',
            'curated.ml_experiments'
        ]
        
        print("\nVerifying ML tables...")
        for table in tables_to_check:
            schema, table_name = table.split('.')
            table_exists = await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = $1 AND table_name = $2
                )
                """,
                schema, table_name
            )
            
            if table_exists:
                print(f"✅ {table} exists")
            else:
                print(f"❌ {table} missing")
        
        # Close connection
        await conn.close()
        
        print("\n🎉 ML database setup completed!")
        
    except Exception as e:
        print(f"❌ Error setting up ML database: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(setup_ml_database())