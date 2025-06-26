-- Executive Summary Report
-- Neutral baseline measuring overall market efficiency (no directional strategy)
-- This measures how often favorites vs underdogs perform relative to their implied odds

WITH strategy_summary AS (
    SELECT 
        'executive-summary' as source_book_type,
        'market-efficiency' as split_type,
        'baseline_performance' as strategy_variant,
        
        -- Count total games with valid data
        COUNT(DISTINCT rmbs.game_id) as total_bets,
        
        -- Measure market efficiency: how often favorites actually win
        -- This is NOT a betting strategy but a market baseline
        COUNT(DISTINCT CASE WHEN go.home_win = true THEN rmbs.game_id END) as home_wins,
        COUNT(DISTINCT CASE WHEN go.home_win = false THEN rmbs.game_id END) as away_wins,
        
        -- Market efficiency metrics (how often favorites win regardless of home/away)
        ROUND((AVG(CASE WHEN go.home_win = true THEN 1.0 ELSE 0.0 END) * 100)::NUMERIC, 1) as home_win_rate,
        
        -- Theoretical ROI if always betting home at standard -110 odds
        -- This shows house edge / market efficiency, not a recommended strategy
        ROUND((
            (COUNT(DISTINCT CASE WHEN go.home_win = true THEN rmbs.game_id END) * 100.0 - 
             COUNT(DISTINCT CASE WHEN go.home_win = false THEN rmbs.game_id END) * 110.0) / 
            (COUNT(DISTINCT rmbs.game_id) * 110.0) * 100
        )::NUMERIC, 1) as theoretical_home_bet_roi
        
    FROM splits.raw_mlb_betting_splits rmbs
    JOIN public.game_outcomes go ON rmbs.game_id = go.game_id
    WHERE rmbs.last_updated < rmbs.game_datetime
      AND rmbs.home_or_over_stake_percentage IS NOT NULL
      AND rmbs.home_or_over_bets_percentage IS NOT NULL
      AND go.home_score IS NOT NULL
      AND go.away_score IS NOT NULL
      AND go.home_win IS NOT NULL  -- Ensure we have valid win/loss data
)

SELECT 
    source_book_type,
    split_type,
    strategy_variant,
    total_bets,
    home_wins,
    away_wins,
    home_win_rate,
    theoretical_home_bet_roi as roi_per_100_unit  -- Keep column name for compatibility
FROM strategy_summary
WHERE total_bets >= 10; 