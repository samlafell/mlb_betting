-- Book Conflicts Strategy Analysis
-- Tests scenarios where different books show contradictory signals
-- Example: Circa has 30% money + 80% bets vs DraftKings has 80% money + 30% bets
-- This identifies situations where books disagree on sharp vs public action

WITH latest_splits AS (
    SELECT 
        home_team, away_team, game_datetime, split_type, split_value,
        home_or_over_stake_percentage as money_pct,
        home_or_over_bets_percentage as bet_pct,
        source, COALESCE(book, 'UNKNOWN') as book, last_updated,
        ROW_NUMBER() OVER (
            PARTITION BY home_team, away_team, game_datetime, split_type, source, COALESCE(book, 'UNKNOWN')
            ORDER BY last_updated DESC
        ) as rn
    FROM splits.raw_mlb_betting_splits
    WHERE home_or_over_stake_percentage IS NOT NULL 
      AND home_or_over_bets_percentage IS NOT NULL
      AND game_datetime IS NOT NULL
),

-- Calculate sharp signals for each book
book_signals AS (
    SELECT 
        home_team, away_team, game_datetime, split_type,
        source, book, money_pct, bet_pct,
        
        -- Sharp signal classification
        CASE 
            WHEN money_pct - bet_pct >= 20 THEN 'STRONG_SHARP_HOME'  -- Much more money than bets on home
            WHEN money_pct - bet_pct >= 10 THEN 'MODERATE_SHARP_HOME'
            WHEN bet_pct - money_pct >= 20 THEN 'STRONG_SHARP_AWAY'  -- Much more bets than money (sharp on away)
            WHEN bet_pct - money_pct >= 10 THEN 'MODERATE_SHARP_AWAY'
            ELSE 'NO_SHARP_SIGNAL'
        END as sharp_signal,
        
        -- Money vs bet differential 
        money_pct - bet_pct as sharp_differential,
        
        -- Overall directional bias
        CASE 
            WHEN money_pct >= 60 THEN 'MONEY_FAVORS_HOME'
            WHEN money_pct <= 40 THEN 'MONEY_FAVORS_AWAY'
            ELSE 'MONEY_BALANCED'
        END as money_direction,
        
        CASE 
            WHEN bet_pct >= 60 THEN 'BETS_FAVOR_HOME'
            WHEN bet_pct <= 40 THEN 'BETS_FAVOR_AWAY'
            ELSE 'BETS_BALANCED'
        END as bet_direction
    FROM latest_splits 
    WHERE rn = 1
),

-- Find games with multiple books and conflicting signals
book_conflicts AS (
    SELECT 
        home_team, away_team, game_datetime, split_type,
        COUNT(DISTINCT source || '-' || book) as num_books,
        
        -- Collect all sharp signals
        STRING_AGG(DISTINCT sharp_signal, '|') as all_sharp_signals,
        STRING_AGG(DISTINCT money_direction, '|') as all_money_directions,
        STRING_AGG(DISTINCT bet_direction, '|') as all_bet_directions,
        
        -- Statistical measures of disagreement
        STDDEV(money_pct) as money_pct_variance,
        STDDEV(bet_pct) as bet_pct_variance,
        STDDEV(sharp_differential) as sharp_diff_variance,
        
        -- Average values
        AVG(money_pct) as avg_money_pct,
        AVG(bet_pct) as avg_bet_pct,
        AVG(sharp_differential) as avg_sharp_diff,
        
        -- Conflict detection
        COUNT(DISTINCT sharp_signal) as unique_sharp_signals,
        COUNT(DISTINCT money_direction) as unique_money_directions,
        COUNT(DISTINCT bet_direction) as unique_bet_directions,
        
        -- Specific conflict types
        CASE 
            WHEN COUNT(DISTINCT sharp_signal) >= 3 AND STDDEV(sharp_differential) >= 15 THEN 'HIGH_SHARP_CONFLICT'
            WHEN COUNT(DISTINCT sharp_signal) >= 2 AND STDDEV(sharp_differential) >= 10 THEN 'MODERATE_SHARP_CONFLICT'
            WHEN COUNT(DISTINCT money_direction) >= 2 AND COUNT(DISTINCT bet_direction) >= 2 THEN 'DIRECTIONAL_CONFLICT'
            WHEN STDDEV(money_pct) >= 20 OR STDDEV(bet_pct) >= 20 THEN 'HIGH_VARIANCE_CONFLICT'
            ELSE NULL
        END as conflict_type,
        
        -- Determine recommended action based on conflict pattern
        CASE 
            -- When books strongly disagree on sharp signals, follow the sharpest signal
            WHEN COUNT(DISTINCT sharp_signal) >= 3 AND MAX(ABS(sharp_differential)) >= 20 THEN
                CASE WHEN AVG(sharp_differential) > 0 THEN 'FOLLOW_SHARP_HOME' ELSE 'FOLLOW_SHARP_AWAY' END
            -- When directional conflict exists, fade the public consensus
            WHEN COUNT(DISTINCT money_direction) >= 2 AND AVG(money_pct) > 65 THEN 'FADE_PUBLIC_HOME'
            WHEN COUNT(DISTINCT money_direction) >= 2 AND AVG(money_pct) < 35 THEN 'FADE_PUBLIC_AWAY'
            -- When high variance, follow the book with strongest signal
            WHEN STDDEV(sharp_differential) >= 15 THEN
                CASE WHEN AVG(sharp_differential) > 5 THEN 'FOLLOW_CONSENSUS_HOME' ELSE 'FOLLOW_CONSENSUS_AWAY' END
            ELSE NULL
        END as recommended_action
        
    FROM book_signals
    GROUP BY home_team, away_team, game_datetime, split_type
    HAVING COUNT(DISTINCT source || '-' || book) >= 2  -- Need multiple books
      AND (COUNT(DISTINCT sharp_signal) >= 2 OR STDDEV(sharp_differential) >= 10)  -- Need actual conflicts
),

-- Add game outcomes for backtesting
conflicts_with_outcomes AS (
    SELECT 
        bc.*,
        go.home_score, go.away_score, go.home_win,
        
        -- Determine if recommended action was successful
        CASE 
            WHEN bc.split_type = 'moneyline' AND bc.recommended_action = 'FOLLOW_SHARP_HOME' THEN 
                CASE WHEN go.home_win = true THEN 1 ELSE 0 END
            WHEN bc.split_type = 'moneyline' AND bc.recommended_action = 'FOLLOW_SHARP_AWAY' THEN 
                CASE WHEN go.home_win = false THEN 1 ELSE 0 END
            WHEN bc.split_type = 'moneyline' AND bc.recommended_action = 'FADE_PUBLIC_HOME' THEN 
                CASE WHEN go.home_win = false THEN 1 ELSE 0 END
            WHEN bc.split_type = 'moneyline' AND bc.recommended_action = 'FADE_PUBLIC_AWAY' THEN 
                CASE WHEN go.home_win = true THEN 1 ELSE 0 END
            WHEN bc.split_type = 'moneyline' AND bc.recommended_action = 'FOLLOW_CONSENSUS_HOME' THEN 
                CASE WHEN go.home_win = true THEN 1 ELSE 0 END
            WHEN bc.split_type = 'moneyline' AND bc.recommended_action = 'FOLLOW_CONSENSUS_AWAY' THEN 
                CASE WHEN go.home_win = false THEN 1 ELSE 0 END
            ELSE NULL
        END as action_successful,
        
        -- Also test opposite strategy (contrarian to our recommendation)
        CASE 
            WHEN bc.split_type = 'moneyline' AND bc.recommended_action = 'FOLLOW_SHARP_HOME' THEN 
                CASE WHEN go.home_win = false THEN 1 ELSE 0 END
            WHEN bc.split_type = 'moneyline' AND bc.recommended_action = 'FOLLOW_SHARP_AWAY' THEN 
                CASE WHEN go.home_win = true THEN 1 ELSE 0 END
            WHEN bc.split_type = 'moneyline' AND bc.recommended_action = 'FADE_PUBLIC_HOME' THEN 
                CASE WHEN go.home_win = true THEN 1 ELSE 0 END
            WHEN bc.split_type = 'moneyline' AND bc.recommended_action = 'FADE_PUBLIC_AWAY' THEN 
                CASE WHEN go.home_win = false THEN 1 ELSE 0 END
            WHEN bc.split_type = 'moneyline' AND bc.recommended_action = 'FOLLOW_CONSENSUS_HOME' THEN 
                CASE WHEN go.home_win = false THEN 1 ELSE 0 END
            WHEN bc.split_type = 'moneyline' AND bc.recommended_action = 'FOLLOW_CONSENSUS_AWAY' THEN 
                CASE WHEN go.home_win = true THEN 1 ELSE 0 END
            ELSE NULL
        END as contrarian_successful
        
    FROM book_conflicts bc
    LEFT JOIN game_outcomes go ON bc.home_team = go.home_team 
        AND bc.away_team = go.away_team 
        AND DATE(bc.game_datetime) = DATE(go.game_date)
    WHERE go.game_date IS NOT NULL  -- Only include games with outcomes
      AND go.home_score IS NOT NULL
      AND go.away_score IS NOT NULL
      AND DATE(go.game_date) >= CURRENT_DATE - INTERVAL '7 days'  -- Recent games
)

-- Final analysis: Book Conflicts Strategy Performance
SELECT 
    'BOOK_CONFLICTS' as strategy_name,
    split_type,
    conflict_type as strategy_variant,
    recommended_action as action_type,
    'book-conflicts' as source_book_type,
    COUNT(*) as total_bets,
    SUM(action_successful) as wins,
    ROUND(AVG(action_successful) * 100, 1) as win_rate,
    ROUND(((SUM(action_successful) * 100.0) - ((COUNT(*) - SUM(action_successful)) * 110.0)) / (COUNT(*) * 110.0) * 100, 1) as roi_per_100_unit,
    
    -- Compare to contrarian approach
    SUM(contrarian_successful) as contrarian_wins,
    ROUND(AVG(contrarian_successful) * 100, 1) as contrarian_win_rate_pct,
    ROUND(((SUM(contrarian_successful) * 100.0) - ((COUNT(*) - SUM(contrarian_successful)) * 110.0)) / (COUNT(*) * 110.0) * 100, 1) as contrarian_roi_per_100,
    
    -- Conflict quality metrics
    ROUND(AVG(num_books), 1) as avg_books_per_conflict,
    ROUND(AVG(unique_sharp_signals), 1) as avg_unique_signals,
    ROUND(AVG(sharp_diff_variance), 1) as avg_signal_variance,
    ROUND(AVG(money_pct_variance), 1) as avg_money_variance,
    ROUND(AVG(bet_pct_variance), 1) as avg_bet_variance,
    
    -- Performance edge calculation
    ROUND(AVG(action_successful) - AVG(contrarian_successful), 3) as strategy_edge,
    
    COUNT(DISTINCT home_team || '-' || away_team || '-' || DATE(game_datetime)) as unique_games,
    
    -- Recent examples for validation
    STRING_AGG(
        home_team || ' vs ' || away_team || ' (Action: ' || recommended_action || 
        ', Conflict: ' || conflict_type || ', Books: ' || num_books || 
        ', Result: ' || CASE WHEN action_successful = 1 THEN 'WIN' ELSE 'LOSS' END || ')', 
        '; '
    ) as recent_examples

FROM conflicts_with_outcomes
WHERE action_successful IS NOT NULL
GROUP BY split_type, conflict_type, recommended_action
HAVING COUNT(*) >= 3  -- Minimum sample size

UNION ALL

-- Summary across all conflict types
SELECT 
    'BOOK_CONFLICTS_SUMMARY' as strategy_name,
    'ALL' as split_type,
    'ALL_CONFLICTS' as strategy_variant,
    'ALL_ACTIONS' as action_type,
    'book-conflicts-summary' as source_book_type,
    COUNT(*) as total_bets,
    SUM(action_successful) as wins,
    ROUND(AVG(action_successful) * 100, 1) as win_rate,
    ROUND(((SUM(action_successful) * 100.0) - ((COUNT(*) - SUM(action_successful)) * 110.0)) / (COUNT(*) * 110.0) * 100, 1) as roi_per_100_unit,
    
    SUM(contrarian_successful) as contrarian_wins,
    ROUND(AVG(contrarian_successful) * 100, 1) as contrarian_win_rate_pct,
    ROUND(((SUM(contrarian_successful) * 100.0) - ((COUNT(*) - SUM(contrarian_successful)) * 110.0)) / (COUNT(*) * 110.0) * 100, 1) as contrarian_roi_per_100,
    
    ROUND(AVG(num_books), 1) as avg_books_per_conflict,
    ROUND(AVG(unique_sharp_signals), 1) as avg_unique_signals,
    ROUND(AVG(sharp_diff_variance), 1) as avg_signal_variance,
    ROUND(AVG(money_pct_variance), 1) as avg_money_variance,
    ROUND(AVG(bet_pct_variance), 1) as avg_bet_variance,
    
    ROUND(AVG(action_successful) - AVG(contrarian_successful), 3) as strategy_edge,
    
    COUNT(DISTINCT home_team || '-' || away_team || '-' || DATE(game_datetime)) as unique_games,
    
    'Summary of all book conflict opportunities' as recent_examples

FROM conflicts_with_outcomes
WHERE action_successful IS NOT NULL

ORDER BY roi_per_100_unit DESC; 