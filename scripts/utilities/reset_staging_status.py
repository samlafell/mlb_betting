#!/usr/bin/env python3
"""
Reset staging status from 'parsed' to 'new' so collection orchestrator will process them.
"""

import asyncio
import sys
from datetime import date
sys.path.append('.')

from sportsbookreview.services.data_storage_service import DataStorageService

async def reset_staging_status():
    """Reset staging status to allow processing."""
    storage = DataStorageService()
    await storage.initialize_connection()
    
    target_date = date(2025, 7, 9)
    
    try:
        print(f"🔄 Resetting staging status for {target_date}...")
        
        async with storage.pool.acquire() as conn:
            # Reset staging records from 'parsed' to 'new'
            updated = await conn.execute('''
                UPDATE sbr_parsed_games 
                SET status = 'new'
                WHERE DATE(parsed_at) = $1
                AND status = 'parsed'
            ''', target_date)
            
            print(f"✅ Updated {updated} staging records from 'parsed' to 'new'")
            
            # Check current status
            status_counts = await conn.fetch('''
                SELECT status, COUNT(*) as count
                FROM sbr_parsed_games 
                WHERE DATE(parsed_at) = $1
                GROUP BY status
                ORDER BY status
            ''', target_date)
            
            print(f"\n📊 Current staging status for {target_date}:")
            for record in status_counts:
                print(f"  {record['status']}: {record['count']} records")
        
    finally:
        await storage.close_connection()

if __name__ == "__main__":
    asyncio.run(reset_staging_status()) 