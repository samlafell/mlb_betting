#!/usr/bin/env python3
import asyncio
import sys
from datetime import date
sys.path.insert(0, 'src')
from mlb_sharp_betting.services.game_updater import GameUpdater
from mlb_sharp_betting.db.connection import get_db_manager

async def process_june_17_games():
    """Process games from June 17th to catch the missing STL @ CWS game."""
    print('🔧 PROCESSING JUNE 17TH GAMES...')
    
    db_manager = get_db_manager()
    game_updater = GameUpdater(db_manager)
    
    # Process June 17th specifically
    june_17 = date(2025, 6, 17)
    await game_updater.update_game_outcomes_for_date(june_17)
    
    print('✅ June 17th processing complete')
    
    # Check if the missing game was processed
    with db_manager.get_cursor() as cursor:
        cursor.execute("SELECT * FROM mlb_betting.main.game_outcomes WHERE game_id = '777467'")
        outcome = cursor.fetchone()
        
        if outcome:
            print(f'✅ Game 777467 outcome now exists: {outcome}')
        else:
            print('❌ Game 777467 still missing outcome')

if __name__ == "__main__":
    asyncio.run(process_june_17_games()) 