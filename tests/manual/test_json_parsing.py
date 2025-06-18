import duckdb

def test_json_parsing():
    """Test JSON parsing approach for split_value column"""
    
    conn = duckdb.connect('data/raw/mlb_betting.duckdb')
    
    # Test query to parse JSON split_value data
    test_query = """
    WITH parsed_values AS (
        SELECT 
            game_id,
            split_type,
            split_value,
            book,
            home_or_over_bets_percentage,
            home_or_over_stake_percentage,
            -- Parse JSON for moneyline, direct cast for others
            CASE 
                WHEN split_type = 'moneyline' AND split_value LIKE '{%}' THEN
                    TRY_CAST(json_extract_string(split_value, '$.home') AS FLOAT)
                WHEN split_type IN ('spread', 'total') THEN
                    TRY_CAST(split_value AS FLOAT)
                ELSE NULL
            END as home_line,
            CASE 
                WHEN split_type = 'moneyline' AND split_value LIKE '{%}' THEN
                    TRY_CAST(json_extract_string(split_value, '$.away') AS FLOAT)
                WHEN split_type IN ('spread', 'total') THEN
                    TRY_CAST(split_value AS FLOAT)  -- Same value for both
                ELSE NULL
            END as away_line
        FROM mlb_betting.splits.raw_mlb_betting_splits
        WHERE split_value IS NOT NULL
        LIMIT 20
    )
    SELECT 
        split_type,
        split_value,
        home_line,
        away_line,
        home_or_over_bets_percentage,
        home_or_over_stake_percentage
    FROM parsed_values
    ORDER BY split_type, game_id
    """
    
    try:
        result = conn.execute(test_query).fetchall()
        print("JSON Parsing Test Results:")
        print("=" * 80)
        print(f"{'Type':<10} {'Original Value':<25} {'Home Line':<12} {'Away Line':<12} {'Bet%':<8} {'Stake%'}")
        print("-" * 80)
        
        for row in result:
            split_type, split_value, home_line, away_line, bet_pct, stake_pct = row
            print(f"{split_type:<10} {str(split_value):<25} {home_line:<12} {away_line:<12} {bet_pct:<8} {stake_pct}")
        
        print("\n" + "=" * 80)
        print("Test completed successfully!")
        return True
        
    except Exception as e:
        print(f"Error in JSON parsing test: {e}")
        return False

def test_line_movement():
    """Test line movement calculation with proper JSON handling"""
    
    conn = duckdb.connect('data/raw/mlb_betting.duckdb')
    
    movement_query = """
    WITH parsed_values AS (
        SELECT 
            game_id,
            split_type,
            book,
            last_updated,
            CASE 
                WHEN split_type = 'moneyline' AND split_value LIKE '{%}' THEN
                    TRY_CAST(json_extract_string(split_value, '$.home') AS FLOAT)
                WHEN split_type IN ('spread', 'total') THEN
                    TRY_CAST(split_value AS FLOAT)
                ELSE NULL
            END as line_value,
            home_or_over_stake_percentage - home_or_over_bets_percentage as sharp_differential
        FROM mlb_betting.splits.raw_mlb_betting_splits
        WHERE split_value IS NOT NULL 
            AND home_or_over_stake_percentage IS NOT NULL 
            AND home_or_over_bets_percentage IS NOT NULL
    ),
    line_movement AS (
        SELECT 
            game_id,
            split_type,
            book,
            FIRST_VALUE(line_value) OVER (
                PARTITION BY game_id, split_type, book 
                ORDER BY last_updated ASC
            ) as opening_line,
            LAST_VALUE(line_value) OVER (
                PARTITION BY game_id, split_type, book 
                ORDER BY last_updated ASC
                RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as closing_line,
            AVG(sharp_differential) as avg_sharp_differential,
            COUNT(*) as line_changes
        FROM parsed_values
        WHERE line_value IS NOT NULL
        GROUP BY game_id, split_type, book
        HAVING COUNT(*) > 1  -- Only games with line movement
    )
    SELECT 
        split_type,
        COUNT(*) as games_with_movement,
        AVG(ABS(closing_line - opening_line)) as avg_line_movement,
        AVG(avg_sharp_differential) as avg_sharp_differential,
        AVG(line_changes) as avg_line_changes
    FROM line_movement
    WHERE opening_line IS NOT NULL AND closing_line IS NOT NULL
    GROUP BY split_type
    ORDER BY split_type
    """
    
    try:
        result = conn.execute(movement_query).fetchall()
        print("\nLine Movement Analysis:")
        print("=" * 80)
        print(f"{'Type':<12} {'Games':<8} {'Avg Movement':<15} {'Sharp Diff':<12} {'Line Changes'}")
        print("-" * 80)
        
        for row in result:
            split_type, games, movement, sharp_diff, changes = row
            print(f"{split_type:<12} {games:<8} {movement:<15.2f} {sharp_diff:<12.2f} {changes:<.1f}")
        
        print("\n" + "=" * 80)
        return True
        
    except Exception as e:
        print(f"Error in line movement test: {e}")
        return False

if __name__ == "__main__":
    print("Testing JSON parsing for MLB betting data...")
    
    # Test JSON parsing
    if test_json_parsing():
        print("\n✅ JSON parsing test passed!")
    else:
        print("\n❌ JSON parsing test failed!")
    
    # Test line movement calculation
    if test_line_movement():
        print("\n✅ Line movement test passed!")
    else:
        print("\n❌ Line movement test failed!")
    
    print("\nTesting complete!") 