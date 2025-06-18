-- Current Consensus Signals Analysis
-- Shows what consensus signals we have today (without requiring game outcomes)

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
      AND DATE(game_datetime) >= CURRENT_DATE  -- Today's games only
)

-- Show all consensus patterns
SELECT 
    'CONSENSUS_HEAVY' as signal_type,
    CASE 
        WHEN money_pct >= 90 AND bet_pct >= 90 THEN 'FOLLOW'
        WHEN money_pct <= 10 AND bet_pct <= 10 THEN 'FOLLOW'
    END as strategy,
    split_type,
    home_team || ' @ ' || away_team as matchup,
    ROUND(money_pct, 1) as money_percentage,
    ROUND(bet_pct, 1) as bet_percentage,
    ROUND((money_pct + bet_pct) / 2, 1) as consensus_strength,
    ROUND(ABS(money_pct - bet_pct), 1) as alignment_gap,
    CASE 
        WHEN money_pct >= 90 AND bet_pct >= 90 THEN 
            CASE WHEN split_type = 'moneyline' THEN home_team
                 WHEN split_type = 'spread' THEN home_team || ' spread'
                 WHEN split_type = 'total' THEN 'OVER'
            END
        WHEN money_pct <= 10 AND bet_pct <= 10 THEN 
            CASE WHEN split_type = 'moneyline' THEN away_team
                 WHEN split_type = 'spread' THEN away_team || ' spread'
                 WHEN split_type = 'total' THEN 'UNDER'
            END
    END as recommendation,
    source || '-' || book as data_source,
    game_datetime
FROM latest_splits 
WHERE rn = 1
  AND ((money_pct >= 90 AND bet_pct >= 90) OR (money_pct <= 10 AND bet_pct <= 10))

UNION ALL

SELECT 
    'MIXED_CONSENSUS' as signal_type,
    CASE 
        WHEN money_pct >= 80 AND bet_pct >= 60 THEN 'FOLLOW' 
        WHEN money_pct <= 20 AND bet_pct <= 40 THEN 'FOLLOW'
    END as strategy,
    split_type,
    home_team || ' @ ' || away_team as matchup,
    ROUND(money_pct, 1) as money_percentage,
    ROUND(bet_pct, 1) as bet_percentage,
    ROUND((money_pct + bet_pct) / 2, 1) as consensus_strength,
    ROUND(ABS(money_pct - bet_pct), 1) as alignment_gap,
    CASE 
        WHEN money_pct >= 80 AND bet_pct >= 60 THEN 
            CASE WHEN split_type = 'moneyline' THEN home_team
                 WHEN split_type = 'spread' THEN home_team || ' spread'
                 WHEN split_type = 'total' THEN 'OVER'
            END
        WHEN money_pct <= 20 AND bet_pct <= 40 THEN 
            CASE WHEN split_type = 'moneyline' THEN away_team
                 WHEN split_type = 'spread' THEN away_team || ' spread'
                 WHEN split_type = 'total' THEN 'UNDER'
            END
    END as recommendation,
    source || '-' || book as data_source,
    game_datetime
FROM latest_splits 
WHERE rn = 1
  AND ((money_pct >= 80 AND bet_pct >= 60) OR (money_pct <= 20 AND bet_pct <= 40))

ORDER BY consensus_strength DESC, game_datetime ASC; 