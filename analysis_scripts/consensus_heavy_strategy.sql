-- Consensus Heavy Strategy Analysis
-- Tests scenarios where both public bets AND sharp money heavily favor one side
-- This could be either a "follow the consensus" or "fade the consensus" strategy

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

consensus_signals AS (
    SELECT 
        home_team, away_team, game_datetime, split_type, split_value,
        money_pct, bet_pct, source, book, last_updated,
        CASE 
            WHEN money_pct >= 90 AND bet_pct >= 90 THEN 'CONSENSUS_HEAVY_HOME_OVER'
            WHEN money_pct <= 10 AND bet_pct <= 10 THEN 'CONSENSUS_HEAVY_AWAY_UNDER'
            ELSE NULL
        END as consensus_type,
        CASE 
            WHEN money_pct >= 90 AND bet_pct >= 90 THEN home_team
            WHEN money_pct <= 10 AND bet_pct <= 10 AND split_type IN ('moneyline', 'spread') THEN away_team
            WHEN money_pct <= 10 AND bet_pct <= 10 AND split_type = 'total' THEN 'UNDER'
            WHEN money_pct >= 90 AND bet_pct >= 90 AND split_type = 'total' THEN 'OVER'
            ELSE NULL
        END as recommended_side,
        -- Consensus strength (how aligned are bet% and money%)
        ABS(money_pct - bet_pct) as consensus_alignment,
        (money_pct + bet_pct) / 2 as consensus_strength
    FROM latest_splits 
    WHERE rn = 1
      AND ((money_pct >= 90 AND bet_pct >= 90) OR (money_pct <= 10 AND bet_pct <= 10))
),

-- Join with game outcomes for backtesting
consensus_with_outcomes AS (
    SELECT 
        c.*,
        g.home_score, g.away_score,
        g.total_runs, g.home_spread, g.game_date,
        -- Determine if consensus recommendation was correct
        CASE 
            WHEN c.split_type = 'moneyline' AND c.recommended_side = c.home_team THEN 
                CASE WHEN g.home_score > g.away_score THEN 1 ELSE 0 END
            WHEN c.split_type = 'moneyline' AND c.recommended_side = c.away_team THEN 
                CASE WHEN g.away_score > g.home_score THEN 1 ELSE 0 END
            WHEN c.split_type = 'spread' AND c.recommended_side = c.home_team THEN 
                CASE WHEN (g.home_score + g.home_spread) > g.away_score THEN 1 ELSE 0 END
            WHEN c.split_type = 'spread' AND c.recommended_side = c.away_team THEN 
                CASE WHEN g.away_score > (g.home_score + g.home_spread) THEN 1 ELSE 0 END
            WHEN c.split_type = 'total' AND c.recommended_side = 'OVER' THEN 
                CASE WHEN g.total_runs > c.split_value THEN 1 ELSE 0 END
            WHEN c.split_type = 'total' AND c.recommended_side = 'UNDER' THEN 
                CASE WHEN g.total_runs < c.split_value THEN 1 ELSE 0 END
            ELSE 0
        END as consensus_follow_correct,
        -- Test fade strategy (opposite of consensus)
        CASE 
            WHEN c.split_type = 'moneyline' AND c.recommended_side = c.home_team THEN 
                CASE WHEN g.away_score > g.home_score THEN 1 ELSE 0 END
            WHEN c.split_type = 'moneyline' AND c.recommended_side = c.away_team THEN 
                CASE WHEN g.home_score > g.away_score THEN 1 ELSE 0 END
            WHEN c.split_type = 'spread' AND c.recommended_side = c.home_team THEN 
                CASE WHEN g.away_score > (g.home_score + g.home_spread) THEN 1 ELSE 0 END
            WHEN c.split_type = 'spread' AND c.recommended_side = c.away_team THEN 
                CASE WHEN (g.home_score + g.home_spread) > g.away_score THEN 1 ELSE 0 END
            WHEN c.split_type = 'total' AND c.recommended_side = 'OVER' THEN 
                CASE WHEN g.total_runs < c.split_value THEN 1 ELSE 0 END
            WHEN c.split_type = 'total' AND c.recommended_side = 'UNDER' THEN 
                CASE WHEN g.total_runs > c.split_value THEN 1 ELSE 0 END
            ELSE 0
        END as consensus_fade_correct
    FROM consensus_signals c
    LEFT JOIN games g ON c.home_team = g.home_team 
        AND c.away_team = g.away_team 
        AND DATE(c.game_datetime) = DATE(g.game_datetime)
    WHERE g.game_datetime IS NOT NULL  -- Only include games with outcomes
      AND DATE(g.game_datetime) >= CURRENT_DATE - INTERVAL '30 days'  -- Recent games only
)

-- Final analysis: Follow vs Fade consensus
SELECT 
    'CONSENSUS_HEAVY_FOLLOW' as strategy_name,
    split_type,
    source || '-' || book as source_book_type,
    COUNT(*) as total_bets,
    SUM(consensus_follow_correct) as wins,
    ROUND(AVG(consensus_follow_correct) * 100, 1) as win_rate_pct,
    ROUND(((SUM(consensus_follow_correct) * 100.0) - ((COUNT(*) - SUM(consensus_follow_correct)) * 110.0)) / (COUNT(*) * 110.0) * 100, 1) as roi_per_100,
    ROUND(AVG(consensus_strength), 1) as avg_consensus_strength,
    ROUND(AVG(consensus_alignment), 1) as avg_consensus_alignment,
    COUNT(DISTINCT home_team || '-' || away_team || '-' || DATE(g.game_datetime)) as unique_games
FROM consensus_with_outcomes
WHERE consensus_follow_correct IS NOT NULL
GROUP BY split_type, source, book
HAVING COUNT(*) >= 5

UNION ALL

SELECT 
    'CONSENSUS_HEAVY_FADE' as strategy_name,
    split_type,
    source || '-' || book as source_book_type,
    COUNT(*) as total_bets,
    SUM(consensus_fade_correct) as wins,
    ROUND(AVG(consensus_fade_correct) * 100, 1) as win_rate_pct,
    ROUND(((SUM(consensus_fade_correct) * 100.0) - ((COUNT(*) - SUM(consensus_fade_correct)) * 110.0)) / (COUNT(*) * 110.0) * 100, 1) as roi_per_100,
    ROUND(AVG(consensus_strength), 1) as avg_consensus_strength,
    ROUND(AVG(consensus_alignment), 1) as avg_consensus_alignment,
    COUNT(DISTINCT home_team || '-' || away_team || '-' || DATE(g.game_datetime)) as unique_games
FROM consensus_with_outcomes
WHERE consensus_fade_correct IS NOT NULL
GROUP BY split_type, source, book
HAVING COUNT(*) >= 5

ORDER BY strategy_name, roi_per_100 DESC; 