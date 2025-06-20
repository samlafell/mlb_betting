-- Mixed Consensus Strategy Analysis
-- Tests scenarios where public bets (60%+) and sharp money (80%+) moderately favor one side
-- Less extreme than consensus heavy, but still represents alignment

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

mixed_consensus_signals AS (
    SELECT 
        home_team, away_team, game_datetime, split_type, split_value,
        money_pct, bet_pct, source, book, last_updated,
        CASE 
            WHEN money_pct >= 80 AND bet_pct >= 60 THEN 'MIXED_CONSENSUS_HOME_OVER'
            WHEN money_pct <= 20 AND bet_pct <= 40 THEN 'MIXED_CONSENSUS_AWAY_UNDER'
            ELSE NULL
        END as consensus_type,
        CASE 
            WHEN money_pct >= 80 AND bet_pct >= 60 THEN home_team
            WHEN money_pct <= 20 AND bet_pct <= 40 AND split_type IN ('moneyline', 'spread') THEN away_team
            WHEN money_pct <= 20 AND bet_pct <= 40 AND split_type = 'total' THEN 'UNDER'
            WHEN money_pct >= 80 AND bet_pct >= 60 AND split_type = 'total' THEN 'OVER'
            ELSE NULL
        END as recommended_side,
        -- Consensus metrics
        ABS(money_pct - bet_pct) as bet_money_gap,
        (money_pct + bet_pct) / 2 as consensus_average,
        money_pct - bet_pct as sharp_public_diff  -- Positive = sharps more confident than public
    FROM latest_splits 
    WHERE rn = 1
      AND ((money_pct >= 80 AND bet_pct >= 60) OR (money_pct <= 20 AND bet_pct <= 40))
),

-- Join with game outcomes for backtesting
mixed_consensus_with_outcomes AS (
    SELECT 
        mc.*,
        g.home_score, g.away_score,
        g.total_runs, g.home_spread, g.game_date,
        -- Determine if consensus recommendation was correct
        CASE 
            WHEN mc.split_type = 'moneyline' AND mc.recommended_side = mc.home_team THEN 
                CASE WHEN g.home_score > g.away_score THEN 1 ELSE 0 END
            WHEN mc.split_type = 'moneyline' AND mc.recommended_side = mc.away_team THEN 
                CASE WHEN g.away_score > g.home_score THEN 1 ELSE 0 END
            WHEN mc.split_type = 'spread' AND mc.recommended_side = mc.home_team THEN 
                CASE WHEN (g.home_score + g.home_spread) > g.away_score THEN 1 ELSE 0 END
            WHEN mc.split_type = 'spread' AND mc.recommended_side = mc.away_team THEN 
                CASE WHEN g.away_score > (g.home_score + g.home_spread) THEN 1 ELSE 0 END
            WHEN mc.split_type = 'total' AND mc.recommended_side = 'OVER' THEN 
                CASE WHEN g.total_runs > mc.split_value THEN 1 ELSE 0 END
            WHEN mc.split_type = 'total' AND mc.recommended_side = 'UNDER' THEN 
                CASE WHEN g.total_runs < mc.split_value THEN 1 ELSE 0 END
            ELSE 0
        END as mixed_consensus_follow_correct,
        -- Test fade strategy (opposite of consensus)
        CASE 
            WHEN mc.split_type = 'moneyline' AND mc.recommended_side = mc.home_team THEN 
                CASE WHEN g.away_score > g.home_score THEN 1 ELSE 0 END
            WHEN mc.split_type = 'moneyline' AND mc.recommended_side = mc.away_team THEN 
                CASE WHEN g.home_score > g.away_score THEN 1 ELSE 0 END
            WHEN mc.split_type = 'spread' AND mc.recommended_side = mc.home_team THEN 
                CASE WHEN g.away_score > (g.home_score + g.home_spread) THEN 1 ELSE 0 END
            WHEN mc.split_type = 'spread' AND mc.recommended_side = mc.away_team THEN 
                CASE WHEN (g.home_score + g.home_spread) > g.away_score THEN 1 ELSE 0 END
            WHEN mc.split_type = 'total' AND mc.recommended_side = 'OVER' THEN 
                CASE WHEN g.total_runs < mc.split_value THEN 1 ELSE 0 END
            WHEN mc.split_type = 'total' AND mc.recommended_side = 'UNDER' THEN 
                CASE WHEN g.total_runs > mc.split_value THEN 1 ELSE 0 END
            ELSE 0
        END as mixed_consensus_fade_correct
    FROM mixed_consensus_signals mc
    LEFT JOIN games g ON mc.home_team = g.home_team 
        AND mc.away_team = g.away_team 
        AND DATE(mc.game_datetime) = DATE(g.game_datetime)
    WHERE g.game_datetime IS NOT NULL  -- Only include games with outcomes
      AND DATE(g.game_datetime) >= CURRENT_DATE - INTERVAL '30 days'  -- Recent games only
)

-- Final analysis with enhanced metrics
SELECT 
    'MIXED_CONSENSUS_FOLLOW' as strategy_name,
    split_type,
    source || '-' || book as source_book_type,
    COUNT(*) as total_bets,
    SUM(mixed_consensus_follow_correct) as wins,
    ROUND(AVG(mixed_consensus_follow_correct) * 100, 1) as win_rate_pct,
    ROUND(((SUM(mixed_consensus_follow_correct) * 100.0) - ((COUNT(*) - SUM(mixed_consensus_follow_correct)) * 110.0)) / (COUNT(*) * 110.0) * 100, 1) as roi_per_100,
    ROUND(AVG(consensus_average), 1) as avg_consensus_pct,
    ROUND(AVG(bet_money_gap), 1) as avg_bet_money_gap,
    ROUND(AVG(sharp_public_diff), 1) as avg_sharp_public_diff,
    COUNT(DISTINCT home_team || '-' || away_team || '-' || DATE(g.game_datetime)) as unique_games
FROM mixed_consensus_with_outcomes
WHERE mixed_consensus_follow_correct IS NOT NULL
GROUP BY split_type, source, book
HAVING COUNT(*) >= 5

UNION ALL

SELECT 
    'MIXED_CONSENSUS_FADE' as strategy_name,
    split_type,
    source || '-' || book as source_book_type,
    COUNT(*) as total_bets,
    SUM(mixed_consensus_fade_correct) as wins,
    ROUND(AVG(mixed_consensus_fade_correct) * 100, 1) as win_rate_pct,
    ROUND(((SUM(mixed_consensus_fade_correct) * 100.0) - ((COUNT(*) - SUM(mixed_consensus_fade_correct)) * 110.0)) / (COUNT(*) * 110.0) * 100, 1) as roi_per_100,
    ROUND(AVG(consensus_average), 1) as avg_consensus_pct,
    ROUND(AVG(bet_money_gap), 1) as avg_bet_money_gap,
    ROUND(AVG(sharp_public_diff), 1) as avg_sharp_public_diff,
    COUNT(DISTINCT home_team || '-' || away_team || '-' || DATE(g.game_datetime)) as unique_games
FROM mixed_consensus_with_outcomes
WHERE mixed_consensus_fade_correct IS NOT NULL
GROUP BY split_type, source, book
HAVING COUNT(*) >= 5

ORDER BY strategy_name, roi_per_100 DESC; 