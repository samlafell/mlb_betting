#!/usr/bin/env python3
"""
Simple database connection test for migration utilities.
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.core.config import get_settings
from src.data.database import get_connection
from src.data.database.connection import initialize_connections


async def test_connection():
    """Test basic database connectivity."""
    settings = get_settings()

    print("🔍 Testing database connection...")
    print(f"Database: {settings.database.database}")
    print(f"Host: {settings.database.host}:{settings.database.port}")

    # Initialize connections first
    initialize_connections(settings)

    try:
        connection_manager = get_connection()
        async with connection_manager.get_async_connection() as conn:
            result = await conn.fetchrow("SELECT version() as version")
            print("✅ Connection successful!")
            print(f"PostgreSQL version: {result['version']}")

            # Test basic data access
            count_result = await conn.fetchrow("""
                SELECT COUNT(*) as count FROM curated.games_complete
            """)
            print(f"📊 Games in database: {count_result['count']:,}")

    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

    return True


if __name__ == "__main__":
    success = asyncio.run(test_connection())
    sys.exit(0 if success else 1)
