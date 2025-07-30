#!/usr/bin/env python3
"""
Simple ML Database Setup Script
Sets up ML database tables without complex config system
"""

import asyncio
import asyncpg
import os
from pathlib import Path


async def setup_ml_database():
    """Setup ML database tables using environment variables"""

    try:
        # Get database configuration from environment variables
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")
        database = os.getenv("POSTGRES_DB", "mlb_betting")
        user = os.getenv("POSTGRES_USER", "postgres")
        password = os.getenv("POSTGRES_PASSWORD", "")

        # Database connection parameters
        dsn = f"postgresql://{user}:{password}@{host}:{port}/{database}"

        print(f"Connecting to database: {host}:{port}/{database}")

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
            "011_create_ml_curated_zone.sql",
            "012_create_ml_features_part2.sql",
            "013_create_ml_prediction_tables.sql",
            "014_create_ml_views_and_indexes.sql",
        ]

        # Execute each migration
        migrations_dir = Path(__file__).parent.parent.parent / "sql" / "migrations"

        for migration_file in ml_migrations:
            migration_path = migrations_dir / migration_file

            if migration_path.exists():
                print(f"Running migration: {migration_file}")

                with open(migration_path, "r") as f:
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
            "curated.enhanced_games",
            "curated.ml_predictions",
            "curated.ml_model_performance",
            "curated.ml_experiments",
        ]

        print("\nVerifying ML tables...")
        for table in tables_to_check:
            schema, table_name = table.split(".")
            table_exists = await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = $1 AND table_name = $2
                )
                """,
                schema,
                table_name,
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
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(setup_ml_database())
