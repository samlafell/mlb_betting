import asyncio
from src.mlb_sharp_betting.db import DatabaseManager, get_db_manager
from src.mlb_sharp_betting.db.repositories import get_betting_split_repository

async def check_splits():
    # Use the singleton pattern and set database path
    db = get_db_manager()
    db.database_path = 'data/raw/mlb_betting.duckdb'
    await db.initialize()
    
    repo = get_betting_split_repository(db)
    splits = repo.find_all(limit=10)
    
    print(f'Found {len(splits)} betting splits in database')
    print()
    
    for i, split in enumerate(splits[:5]):
        print(f'{i+1}. Game ID: {split.game_id}')
        print(f'   Teams: {split.away_team} @ {split.home_team}')
        print(f'   Split Type: {split.split_type}')
        print(f'   Line Value: {split.split_value}')
        print(f'   Source: {split.source}')
        print(f'   Book: {split.book}')
        print()
    
    # Check for recent games on June 16
    recent_splits = repo.find_all()
    june_16_splits = [s for s in recent_splits if '777484' in s.game_id or '777483' in s.game_id or '777486' in s.game_id or '777487' in s.game_id or '777488' in s.game_id]
    
    print(f'June 16 games splits: {len(june_16_splits)}')
    for split in june_16_splits:
        print(f'   {split.game_id}: {split.split_type}={split.split_value} ({split.source}/{split.book})')
    
    db.close()

if __name__ == "__main__":
    asyncio.run(check_splits()) 