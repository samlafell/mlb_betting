-- Consensus Moneyline Strategy Analysis
-- Tests scenarios where both public bets AND sharp money align on moneyline bets
-- Simplified version that works with available game outcome data

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
      AND split_type = 'moneyline'  -- Only moneyline bets
),

consensus_heavy_signals AS (
    SELECT 
        home_team, away_team, game_datetime, split_type, split_value,
        money_pct, bet_pct, source, book, last_updated,
        CASE 
            WHEN money_pct >= 90 AND bet_pct >= 90 THEN 'CONSENSUS_HEAVY_HOME'
            WHEN money_pct <= 10 AND bet_pct <= 10 THEN 'CONSENSUS_HEAVY_AWAY'
            ELSE NULL
        END as consensus_type,
        CASE 
            WHEN money_pct >= 90 AND bet_pct >= 90 THEN home_team
            WHEN money_pct <= 10 AND bet_pct <= 10 THEN away_team
            ELSE NULL
        END as recommended_side,
        (money_pct + bet_pct) / 2 as consensus_strength,
        ABS(money_pct - bet_pct) as consensus_alignment,
        money_pct - bet_pct as sharp_public_diff  -- Added to match mixed_consensus
    FROM latest_splits 
    WHERE rn = 1
      AND ((money_pct >= 90 AND bet_pct >= 90) OR (money_pct <= 10 AND bet_pct <= 10))
),

mixed_consensus_signals AS (
    SELECT 
        home_team, away_team, game_datetime, split_type, split_value,
        money_pct, bet_pct, source, book, last_updated,
        CASE 
            WHEN money_pct >= 80 AND bet_pct >= 60 THEN 'MIXED_CONSENSUS_HOME'
            WHEN money_pct <= 20 AND bet_pct <= 40 THEN 'MIXED_CONSENSUS_AWAY'
            ELSE NULL
        END as consensus_type,
        CASE 
            WHEN money_pct >= 80 AND bet_pct >= 60 THEN home_team
            WHEN money_pct <= 20 AND bet_pct <= 40 THEN away_team
            ELSE NULL
        END as recommended_side,
        (money_pct + bet_pct) / 2 as consensus_strength,
        ABS(money_pct - bet_pct) as consensus_alignment,
        money_pct - bet_pct as sharp_public_diff
    FROM latest_splits 
    WHERE rn = 1
      AND ((money_pct >= 80 AND bet_pct >= 60) OR (money_pct <= 20 AND bet_pct <= 40))
),

-- Combine all consensus signals
all_consensus AS (
    SELECT *, 'CONSENSUS_HEAVY' as strategy_type FROM consensus_heavy_signals
    UNION ALL
    SELECT *, 'MIXED_CONSENSUS' as strategy_type FROM mixed_consensus_signals
),

-- Join with game outcomes for backtesting
consensus_with_outcomes AS (
    SELECT 
        c.*,
        g.home_score, g.away_score,
        -- Determine if consensus recommendation was correct (follow strategy)
        CASE 
            WHEN c.recommended_side = c.home_team THEN 
                CASE WHEN g.home_score > g.away_score THEN 1 ELSE 0 END
            WHEN c.recommended_side = c.away_team THEN 
                CASE WHEN g.away_score > g.home_score THEN 1 ELSE 0 END
            ELSE 0
        END as consensus_follow_correct,
        -- Test fade strategy (opposite of consensus)
        CASE 
            WHEN c.recommended_side = c.home_team THEN 
                CASE WHEN g.away_score > g.home_score THEN 1 ELSE 0 END
            WHEN c.recommended_side = c.away_team THEN 
                CASE WHEN g.home_score > g.away_score THEN 1 ELSE 0 END
            ELSE 0
        END as consensus_fade_correct
    FROM all_consensus c
    LEFT JOIN games g ON c.home_team = g.home_team 
        AND c.away_team = g.away_team 
        AND DATE(c.game_datetime) = DATE(g.game_datetime)
    WHERE g.game_datetime IS NOT NULL  -- Only include games with outcomes
      AND g.home_score IS NOT NULL    -- Only completed games
      AND g.away_score IS NOT NULL
      AND DATE(g.game_datetime) >= CURRENT_DATE - INTERVAL '30 days'  -- Recent games only
)

-- Final analysis: Follow vs Fade consensus
SELECT 
    strategy_type || '_FOLLOW' as strategy_name,
    source || '-' || book as source_book_type,
    COUNT(*) as total_bets,
    SUM(consensus_follow_correct) as wins,
    ROUND(AVG(consensus_follow_correct) * 100, 1) as win_rate_pct,
    ROUND((SUM(consensus_follow_correct) * 100.0 - (COUNT(*) - SUM(consensus_follow_correct)) * 110.0) / COUNT(*), 1) as roi_per_100,
    ROUND(AVG(consensus_strength), 1) as avg_consensus_strength,
    ROUND(AVG(consensus_alignment), 1) as avg_consensus_alignment,
    COUNT(DISTINCT home_team || '-' || away_team || '-' || DATE(game_datetime)) as unique_games,
    -- Show recent example bets
    STRING_AGG(
        home_team || ' vs ' || away_team || ' (Rec: ' || recommended_side || ', Result: ' || 
        CASE WHEN consensus_follow_correct = 1 THEN 'WIN' ELSE 'LOSS' END || ')', 
        '; '
    ) as recent_examples
FROM consensus_with_outcomes
WHERE consensus_follow_correct IS NOT NULL
GROUP BY strategy_type, source, book
HAVING COUNT(*) >= 3  -- Lower threshold since this is moneyline only

UNION ALL

SELECT 
    strategy_type || '_FADE' as strategy_name,
    source || '-' || book as source_book_type,
    COUNT(*) as total_bets,
    SUM(consensus_fade_correct) as wins,
    ROUND(AVG(consensus_fade_correct) * 100, 1) as win_rate_pct,
    ROUND((SUM(consensus_fade_correct) * 100.0 - (COUNT(*) - SUM(consensus_fade_correct)) * 110.0) / COUNT(*), 1) as roi_per_100,
    ROUND(AVG(consensus_strength), 1) as avg_consensus_strength,
    ROUND(AVG(consensus_alignment), 1) as avg_consensus_alignment,
    COUNT(DISTINCT home_team || '-' || away_team || '-' || DATE(game_datetime)) as unique_games,
    -- Show recent example bets  
    STRING_AGG(
        home_team || ' vs ' || away_team || ' (Fade: ' || 
        CASE WHEN recommended_side = home_team THEN away_team ELSE home_team END || 
        ', Result: ' || CASE WHEN consensus_fade_correct = 1 THEN 'WIN' ELSE 'LOSS' END || ')', 
        '; '
    ) as recent_examples
FROM consensus_with_outcomes
WHERE consensus_fade_correct IS NOT NULL
GROUP BY strategy_type, source, book
HAVING COUNT(*) >= 3  -- Lower threshold since this is moneyline only

ORDER BY strategy_name, roi_per_100 DESC; 