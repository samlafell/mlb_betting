import duckdb

conn = duckdb.connect('data/raw/mlb_betting.duckdb')

# Check if we have any betting splits
result = conn.execute('SELECT COUNT(*) FROM betting_splits').fetchone()
print(f'Total betting splits: {result[0]}')

if result[0] > 0:
    splits = conn.execute('SELECT game_id, home_team, away_team, split_type, split_value, source, book FROM betting_splits LIMIT 10').fetchall()
    print('\nSample splits:')
    for split in splits:
        print(f'  {split[0]}: {split[1]} vs {split[2]}, {split[3]}={split[4]} ({split[5]}/{split[6]})')
    
    # Check for June 16 games specifically
    june_splits = conn.execute("""
        SELECT game_id, home_team, away_team, split_type, split_value, source, book 
        FROM splits.raw_mlb_betting_splits 
        WHERE game_id IN ('777483', '777484', '777486', '777487', '777488')
        ORDER BY game_id, split_type
    """).fetchall()
    
    print(f'\nJune 16 games splits: {len(june_splits)}')
    for split in june_splits:
        print(f'  {split[0]}: {split[1]} vs {split[2]}, {split[3]}={split[4]} ({split[5]}/{split[6]})')

conn.close() 