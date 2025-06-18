-- =================================================================
-- QUERY 4: COMBINED SIGNAL STRATEGY
-- Logic: Look for combinations of signals that work together
-- =================================================================

WITH combined_signals AS (
    SELECT 
        rmbs.game_id,
        rmbs.source,
        rmbs.book,
        rmbs.split_type,
        rmbs.home_team,
        rmbs.away_team,
        rmbs.home_or_over_stake_percentage as stake_pct,
        rmbs.home_or_over_bets_percentage as bet_pct,
        rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage as differential,
        
        -- Calculate multiple signal types
        STDDEV(rmbs.home_or_over_stake_percentage) OVER (
            PARTITION BY rmbs.game_id, rmbs.source, rmbs.book, rmbs.split_type
        ) as volatility,
        
        go.home_win,
        go.home_cover_spread,
        go.over,
        
        ROW_NUMBER() OVER (PARTITION BY rmbs.game_id, rmbs.source, rmbs.book, rmbs.split_type 
                          ORDER BY rmbs.last_updated DESC) as line_rank
        
    FROM mlb_betting.splits.raw_mlb_betting_splits rmbs
    JOIN mlb_betting.main.game_outcomes go ON rmbs.game_id = go.game_id
    WHERE rmbs.last_updated < rmbs.game_datetime
      AND rmbs.split_value IS NOT NULL
),

signal_combinations AS (
    SELECT 
        CONCAT(source, '-', book, '-', split_type) as source_book_type,
        split_type,
        
        -- Combination 1: Moderate differential + Low volatility
        COUNT(CASE WHEN ABS(differential) >= 10 AND ABS(differential) < 20 AND volatility < 15 THEN 1 END) as mod_diff_low_vol_signals,
        SUM(CASE 
            WHEN ABS(differential) >= 10 AND ABS(differential) < 20 AND volatility < 15 THEN
                CASE split_type
                    WHEN 'moneyline' THEN 
                        CASE WHEN differential > 0 AND home_win = true THEN 1
                             WHEN differential < 0 AND home_win = false THEN 1
                             ELSE 0 END
                    WHEN 'spread' THEN 
                        CASE WHEN differential > 0 AND home_cover_spread = true THEN 1
                             WHEN differential < 0 AND home_cover_spread = false THEN 1
                             ELSE 0 END
                    WHEN 'total' THEN 
                        CASE WHEN differential > 0 AND over = true THEN 1
                             WHEN differential < 0 AND over = false THEN 1
                             ELSE 0 END
                END
            ELSE 0
        END) as mod_diff_low_vol_wins,
        
        -- Combination 2: Strong differential + High volatility (fade)
        COUNT(CASE WHEN ABS(differential) >= 20 AND volatility > 25 THEN 1 END) as strong_diff_high_vol_signals,
        SUM(CASE 
            WHEN ABS(differential) >= 20 AND volatility > 25 THEN
                CASE split_type
                    WHEN 'moneyline' THEN 
                        CASE WHEN differential < 0 AND home_win = true THEN 1  -- Fade the signal
                             WHEN differential > 0 AND home_win = false THEN 1  -- Fade the signal
                             ELSE 0 END
                    WHEN 'spread' THEN 
                        CASE WHEN differential < 0 AND home_cover_spread = true THEN 1
                             WHEN differential > 0 AND home_cover_spread = false THEN 1
                             ELSE 0 END
                    WHEN 'total' THEN 
                        CASE WHEN differential < 0 AND over = true THEN 1
                             WHEN differential > 0 AND over = false THEN 1
                             ELSE 0 END
                END
            ELSE 0
        END) as strong_diff_high_vol_wins
        
    FROM combined_signals
    WHERE line_rank = 1
    GROUP BY source, book, split_type
)

SELECT 
    source_book_type,
    split_type,
    
    -- Combination results
    mod_diff_low_vol_signals,
    mod_diff_low_vol_wins,
    CASE WHEN mod_diff_low_vol_signals > 0 
         THEN ROUND(100.0 * mod_diff_low_vol_wins / mod_diff_low_vol_signals, 1) 
         ELSE 0 END as mod_diff_low_vol_rate,
    
    strong_diff_high_vol_signals,
    strong_diff_high_vol_wins,
    CASE WHEN strong_diff_high_vol_signals > 0 
         THEN ROUND(100.0 * strong_diff_high_vol_wins / strong_diff_high_vol_signals, 1) 
         ELSE 0 END as strong_diff_high_vol_rate,
    
    -- Best combination
    CASE 
        WHEN mod_diff_low_vol_signals >= 3 AND (100.0 * mod_diff_low_vol_wins / mod_diff_low_vol_signals) > 60 THEN '🟢 MOD DIFF + LOW VOL EXCELLENT'
        WHEN strong_diff_high_vol_signals >= 3 AND (100.0 * strong_diff_high_vol_wins / strong_diff_high_vol_signals) > 60 THEN '🟢 FADE STRONG DIFF + HIGH VOL'
        WHEN mod_diff_low_vol_signals >= 3 AND (100.0 * mod_diff_low_vol_wins / mod_diff_low_vol_signals) > 52.4 THEN '🟡 MOD DIFF + LOW VOL PROFITABLE'
        WHEN strong_diff_high_vol_signals >= 3 AND (100.0 * strong_diff_high_vol_wins / strong_diff_high_vol_signals) > 52.4 THEN '🟡 FADE STRONG + HIGH VOL PROFITABLE'
        ELSE '🔴 NO PROFITABLE COMBINATIONS'
    END as recommendation
    
FROM signal_combinations
WHERE mod_diff_low_vol_signals > 0 OR strong_diff_high_vol_signals > 0
ORDER BY 
    GREATEST(
        CASE WHEN mod_diff_low_vol_signals > 0 THEN 100.0 * mod_diff_low_vol_wins / mod_diff_low_vol_signals ELSE 0 END,
        CASE WHEN strong_diff_high_vol_signals > 0 THEN 100.0 * strong_diff_high_vol_wins / strong_diff_high_vol_signals ELSE 0 END
    ) DESC;