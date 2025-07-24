#!/usr/bin/env python3
"""Debug ON CONFLICT behavior"""

import asyncio
import asyncpg
from src.core.config import get_settings

async def check_on_conflict_behavior():
    settings = get_settings()
    conn = await asyncpg.connect(settings.database.connection_string)
    
    print('🔍 CHECKING ON CONFLICT BEHAVIOR')
    
    # Check recent game updates 
    game_updates = await conn.fetch("""
        SELECT external_game_id, 
               COUNT(*) as record_count,
               MIN(collected_at) as first_collected,
               MAX(collected_at) as last_collected,
               MAX(id) as latest_id
        FROM raw_data.action_network_history
        WHERE external_game_id IN (SELECT DISTINCT external_game_id 
                                  FROM raw_data.action_network_history 
                                  WHERE collected_at >= NOW() - INTERVAL '2 hours')
        GROUP BY external_game_id
        ORDER BY last_collected DESC
    """)
    
    for record in game_updates:
        print(f'Game {record["external_game_id"]}:')
        print(f'  Records: {record["record_count"]}')
        print(f'  First:   {record["first_collected"]}')  
        print(f'  Last:    {record["last_collected"]}')
        print(f'  Latest ID: {record["latest_id"]}')
        
        if record['record_count'] > 1:
            print('  ⚠️  Multiple records!')
        elif record['first_collected'] != record['last_collected']:
            print('  ✅ Record was updated')
        else:
            print('  ➡️  No updates detected')
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(check_on_conflict_behavior())