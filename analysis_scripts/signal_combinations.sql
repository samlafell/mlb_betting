-- =================================================================
-- QUERY 4: COMBINED SIGNAL STRATEGY
-- Logic: Look for combinations of signals that work together
-- =================================================================

-- Signal Combinations Strategy Analysis
-- Tests combinations of different betting signals for improved accuracy
-- Uses multiple data points per game to identify patterns

WITH game_signals AS (
    SELECT 
        rmbs.game_id,
        rmbs.source,
        COALESCE(rmbs.book, 'UNKNOWN') as book,
        rmbs.split_type,
        rmbs.home_team,
        rmbs.away_team,
        rmbs.game_datetime,
        rmbs.home_or_over_stake_percentage as stake_pct,
        rmbs.home_or_over_bets_percentage as bet_pct,
        rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage as differential,
        
        -- Calculate signal strength categories
        CASE 
            WHEN ABS(rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage) >= 20 THEN 'STRONG'
            WHEN ABS(rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage) >= 10 THEN 'MODERATE'
            WHEN ABS(rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage) >= 5 THEN 'WEAK'
            ELSE 'MINIMAL'
        END as signal_strength,
        
        -- Determine signal direction
        CASE 
            WHEN rmbs.home_or_over_stake_percentage > rmbs.home_or_over_bets_percentage THEN 'SHARP_HOME_OVER'
            ELSE 'SHARP_AWAY_UNDER'
        END as signal_direction,
        
        go.home_win,
        go.home_cover_spread,
        go.over,
        
        ROW_NUMBER() OVER (PARTITION BY rmbs.game_id, rmbs.source, rmbs.book, rmbs.split_type 
                          ORDER BY rmbs.last_updated DESC) as latest_rank
        
    FROM mlb_betting.splits.raw_mlb_betting_splits rmbs
    JOIN mlb_betting.main.game_outcomes go ON rmbs.game_id = go.game_id
    WHERE rmbs.last_updated < rmbs.game_datetime
      AND rmbs.split_value IS NOT NULL
      AND rmbs.home_or_over_stake_percentage IS NOT NULL
      AND rmbs.home_or_over_bets_percentage IS NOT NULL
      AND go.home_score IS NOT NULL
      AND go.away_score IS NOT NULL
),

-- Aggregate signals by game to find combinations
game_aggregated AS (
    SELECT 
        game_id,
        source,
        book,
        home_team,
        away_team,
        
        -- Count signals by type and strength
        COUNT(CASE WHEN split_type = 'moneyline' AND signal_strength IN ('STRONG', 'MODERATE') THEN 1 END) as ml_signals,
        COUNT(CASE WHEN split_type = 'spread' AND signal_strength IN ('STRONG', 'MODERATE') THEN 1 END) as spread_signals,
        COUNT(CASE WHEN split_type = 'total' AND signal_strength IN ('STRONG', 'MODERATE') THEN 1 END) as total_signals,
        
        -- Get consensus direction across bet types
        MODE() WITHIN GROUP (ORDER BY signal_direction) as consensus_direction,
        
        -- Get the strongest signal type
        MODE() WITHIN GROUP (ORDER BY signal_strength) as strongest_signal,
        
        -- Check for conflicting signals
        COUNT(DISTINCT signal_direction) as direction_conflicts,
        
        -- Get outcomes
        MAX(home_win) as home_win,
        MAX(home_cover_spread) as home_cover_spread,
        MAX(over) as over
        
    FROM game_signals
    WHERE latest_rank = 1  -- Use only the latest signal for each bet type
    GROUP BY game_id, source, book, home_team, away_team
    HAVING COUNT(*) >= 2  -- At least 2 bet types available
),

-- Define combination strategies
combination_strategies AS (
    SELECT 
        source || '-' || book as source_book_type,
        'combined_signals' as split_type,
        game_id,
        
        -- Strategy 1: Multiple strong signals in same direction
        CASE WHEN ml_signals >= 1 AND spread_signals >= 1 AND direction_conflicts = 1 
             THEN 'MULTI_SIGNAL_CONSENSUS' 
             ELSE NULL END as strategy_1,
             
        -- Strategy 2: Strong moneyline + moderate spread
        CASE WHEN ml_signals >= 1 AND spread_signals >= 1 AND strongest_signal IN ('STRONG', 'MODERATE')
             THEN 'ML_SPREAD_COMBO'
             ELSE NULL END as strategy_2,
             
        -- Strategy 3: All three bet types align
        CASE WHEN ml_signals >= 1 AND spread_signals >= 1 AND total_signals >= 1 AND direction_conflicts = 1
             THEN 'TRIPLE_CONSENSUS'
             ELSE NULL END as strategy_3,
             
        -- Strategy 4: Fade conflicting signals (contrarian)
        CASE WHEN direction_conflicts >= 2 AND (ml_signals + spread_signals + total_signals) >= 3
             THEN 'FADE_CONFLICTS'
             ELSE NULL END as strategy_4,
        
        consensus_direction,
        home_win,
        home_cover_spread,
        over
        
    FROM game_aggregated
),

-- Calculate strategy performance
strategy_performance AS (
    SELECT 
        source_book_type,
        split_type,
        
        -- Strategy 1: Multi-signal consensus
        COUNT(CASE WHEN strategy_1 IS NOT NULL THEN 1 END) as multi_consensus_bets,
        SUM(CASE 
            WHEN strategy_1 IS NOT NULL THEN
                CASE WHEN consensus_direction = 'SHARP_HOME_OVER' AND home_win = true THEN 1
                     WHEN consensus_direction = 'SHARP_AWAY_UNDER' AND home_win = false THEN 1
                     ELSE 0 END
            ELSE 0
        END) as multi_consensus_wins,
        
        -- Strategy 2: ML + Spread combo
        COUNT(CASE WHEN strategy_2 IS NOT NULL THEN 1 END) as ml_spread_bets,
        SUM(CASE 
            WHEN strategy_2 IS NOT NULL THEN
                CASE WHEN consensus_direction = 'SHARP_HOME_OVER' AND home_win = true THEN 1
                     WHEN consensus_direction = 'SHARP_AWAY_UNDER' AND home_win = false THEN 1
                     ELSE 0 END
            ELSE 0
        END) as ml_spread_wins,
        
        -- Strategy 3: Triple consensus
        COUNT(CASE WHEN strategy_3 IS NOT NULL THEN 1 END) as triple_consensus_bets,
        SUM(CASE 
            WHEN strategy_3 IS NOT NULL THEN
                CASE WHEN consensus_direction = 'SHARP_HOME_OVER' AND home_win = true THEN 1
                     WHEN consensus_direction = 'SHARP_AWAY_UNDER' AND home_win = false THEN 1
                     ELSE 0 END
            ELSE 0
        END) as triple_consensus_wins,
        
        -- Strategy 4: Fade conflicts
        COUNT(CASE WHEN strategy_4 IS NOT NULL THEN 1 END) as fade_conflicts_bets,
        SUM(CASE 
            WHEN strategy_4 IS NOT NULL THEN
                CASE WHEN consensus_direction = 'SHARP_HOME_OVER' AND home_win = false THEN 1  -- Fade
                     WHEN consensus_direction = 'SHARP_AWAY_UNDER' AND home_win = true THEN 1   -- Fade
                     ELSE 0 END
            ELSE 0
        END) as fade_conflicts_wins
        
    FROM combination_strategies
    GROUP BY source_book_type, split_type
)

-- Final results formatted for backtesting service
SELECT 
    source_book_type,
    split_type,
    'multi_signal_consensus' as strategy_variant,
    multi_consensus_bets as total_bets,
    multi_consensus_wins as wins,
    CASE WHEN multi_consensus_bets > 0 
         THEN ROUND(100.0 * multi_consensus_wins / multi_consensus_bets, 1) 
         ELSE 0 END as win_rate,
    CASE WHEN multi_consensus_bets > 0 
         THEN ROUND(((multi_consensus_wins * 100) - ((multi_consensus_bets - multi_consensus_wins) * 110)) / (multi_consensus_bets * 110) * 100, 1)
         ELSE 0 END as roi_per_100_unit
FROM strategy_performance
WHERE multi_consensus_bets >= 3

UNION ALL

SELECT 
    source_book_type,
    split_type,
    'ml_spread_combo' as strategy_variant,
    ml_spread_bets as total_bets,
    ml_spread_wins as wins,
    CASE WHEN ml_spread_bets > 0 
         THEN ROUND(100.0 * ml_spread_wins / ml_spread_bets, 1) 
         ELSE 0 END as win_rate,
    CASE WHEN ml_spread_bets > 0 
         THEN ROUND(((ml_spread_wins * 100) - ((ml_spread_bets - ml_spread_wins) * 110)) / (ml_spread_bets * 110) * 100, 1)
         ELSE 0 END as roi_per_100_unit
FROM strategy_performance
WHERE ml_spread_bets >= 3

UNION ALL

SELECT 
    source_book_type,
    split_type,
    'triple_consensus' as strategy_variant,
    triple_consensus_bets as total_bets,
    triple_consensus_wins as wins,
    CASE WHEN triple_consensus_bets > 0 
         THEN ROUND(100.0 * triple_consensus_wins / triple_consensus_bets, 1) 
         ELSE 0 END as win_rate,
    CASE WHEN triple_consensus_bets > 0 
         THEN ROUND(((triple_consensus_wins * 100) - ((triple_consensus_bets - triple_consensus_wins) * 110)) / (triple_consensus_bets * 110) * 100, 1)
         ELSE 0 END as roi_per_100_unit
FROM strategy_performance
WHERE triple_consensus_bets >= 3

UNION ALL

SELECT 
    source_book_type,
    split_type,
    'fade_conflicts' as strategy_variant,
    fade_conflicts_bets as total_bets,
    fade_conflicts_wins as wins,
    CASE WHEN fade_conflicts_bets > 0 
         THEN ROUND(100.0 * fade_conflicts_wins / fade_conflicts_bets, 1) 
         ELSE 0 END as win_rate,
    CASE WHEN fade_conflicts_bets > 0 
         THEN ROUND(((fade_conflicts_wins * 100) - ((fade_conflicts_bets - fade_conflicts_wins) * 110)) / (fade_conflicts_bets * 110) * 100, 1)
         ELSE 0 END as roi_per_100_unit
FROM strategy_performance
WHERE fade_conflicts_bets >= 3

ORDER BY roi_per_100_unit DESC;