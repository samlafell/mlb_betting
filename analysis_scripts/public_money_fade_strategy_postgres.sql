-- Public Money Fade Strategy Analysis
-- Tests the hypothesis that when there's way too much money on one side across multiple books,
-- it's often a fade signal (contrarian betting opportunity)
-- This strategy specifically looks for heavy public consensus as a contrarian indicator

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

-- Identify games with heavy public money consensus across books
public_heavy_games AS (
    SELECT 
        home_team, away_team, game_datetime, split_type,
        COUNT(DISTINCT source || '-' || book) as num_books,
        AVG(money_pct) as avg_money_pct,
        AVG(bet_pct) as avg_bet_pct,
        MIN(money_pct) as min_money_pct,
        MAX(money_pct) as max_money_pct,
        STDDEV(money_pct) as money_pct_stddev,
        -- Count how many books show heavy public money (>80% or <20%)
        SUM(CASE WHEN money_pct >= 80 THEN 1 ELSE 0 END) as books_heavy_home,
        SUM(CASE WHEN money_pct <= 20 THEN 1 ELSE 0 END) as books_heavy_away,
        -- Check for consensus across books
        CASE 
            WHEN AVG(money_pct) >= 85 AND COUNT(DISTINCT source || '-' || book) >= 2 THEN 'HEAVY_PUBLIC_HOME'
            WHEN AVG(money_pct) <= 15 AND COUNT(DISTINCT source || '-' || book) >= 2 THEN 'HEAVY_PUBLIC_AWAY'
            WHEN AVG(money_pct) >= 75 AND MIN(money_pct) >= 70 AND COUNT(DISTINCT source || '-' || book) >= 3 THEN 'MODERATE_PUBLIC_HOME'
            WHEN AVG(money_pct) <= 25 AND MAX(money_pct) <= 30 AND COUNT(DISTINCT source || '-' || book) >= 3 THEN 'MODERATE_PUBLIC_AWAY'
            ELSE NULL
        END as public_consensus_type
    FROM latest_splits 
    WHERE rn = 1
    GROUP BY home_team, away_team, game_datetime, split_type
    HAVING COUNT(DISTINCT source || '-' || book) >= 2  -- Need data from multiple books
),

-- Create fade signals for heavy public money
fade_signals AS (
    SELECT 
        phg.*,
        CASE 
            WHEN public_consensus_type = 'HEAVY_PUBLIC_HOME' THEN away_team
            WHEN public_consensus_type = 'HEAVY_PUBLIC_AWAY' THEN home_team
            WHEN public_consensus_type = 'MODERATE_PUBLIC_HOME' THEN away_team
            WHEN public_consensus_type = 'MODERATE_PUBLIC_AWAY' THEN home_team
            ELSE NULL
        END as fade_recommendation,
        CASE 
            WHEN public_consensus_type LIKE 'HEAVY_%' THEN 'HIGH'
            WHEN public_consensus_type LIKE 'MODERATE_%' THEN 'MODERATE'
            ELSE 'LOW'
        END as fade_confidence,
        -- Calculate signal strength
        CASE 
            WHEN public_consensus_type LIKE '%_HOME' THEN avg_money_pct
            WHEN public_consensus_type LIKE '%_AWAY' THEN 100 - avg_money_pct
            ELSE 50
        END as public_consensus_strength
    FROM public_heavy_games phg
    WHERE public_consensus_type IS NOT NULL
),

-- Join with game outcomes for backtesting
fade_with_outcomes AS (
    SELECT 
        fs.*,
        go.home_score, go.away_score, go.home_win,
        -- Determine if fade was successful
        CASE 
            WHEN fs.split_type = 'moneyline' AND fs.fade_recommendation = fs.home_team THEN 
                CASE WHEN go.home_win = true THEN 1 ELSE 0 END
            WHEN fs.split_type = 'moneyline' AND fs.fade_recommendation = fs.away_team THEN 
                CASE WHEN go.home_win = false THEN 1 ELSE 0 END
            -- For spread and totals, we'd need the actual lines which we don't have in split_value consistently
            -- So focusing on moneyline for now where we have clear win/loss
            ELSE NULL
        END as fade_successful,
        -- Also track if following public would have worked (opposite of fade)
        CASE 
            WHEN fs.split_type = 'moneyline' AND fs.fade_recommendation = fs.home_team THEN 
                CASE WHEN go.home_win = false THEN 1 ELSE 0 END
            WHEN fs.split_type = 'moneyline' AND fs.fade_recommendation = fs.away_team THEN 
                CASE WHEN go.home_win = true THEN 1 ELSE 0 END
            ELSE NULL
        END as follow_public_successful
    FROM fade_signals fs
    LEFT JOIN game_outcomes go ON fs.home_team = go.home_team 
        AND fs.away_team = go.away_team 
        AND DATE(fs.game_datetime) = DATE(go.game_date)
    WHERE go.game_date IS NOT NULL  -- Only include games with outcomes
      AND go.home_score IS NOT NULL     -- Only games with scores
      AND go.away_score IS NOT NULL
      AND DATE(go.game_date) >= CURRENT_DATE - INTERVAL '7 days'  -- Recent games only (shorter window)
)

-- Final analysis: Public Money Fade Strategy Performance
SELECT 
    'PUBLIC_MONEY_FADE' as strategy_name,
    split_type,
    public_consensus_type as strategy_variant,
    fade_confidence,
    'fade-strategy' as source_book_type,
    COUNT(*) as total_bets,
    SUM(fade_successful) as wins,
    ROUND((AVG(fade_successful) * 100)::NUMERIC, 1) as win_rate,
    ROUND((((SUM(fade_successful) * 100.0) - ((COUNT(*) - SUM(fade_successful)) * 110.0)) / (COUNT(*) * 110.0) * 100)::NUMERIC, 1) as roi_per_100_unit,
    
    -- Compare to following public
    SUM(follow_public_successful) as follow_public_wins,
    ROUND((AVG(follow_public_successful) * 100)::NUMERIC, 1) as follow_public_win_rate_pct,
    ROUND((((SUM(follow_public_successful) * 100.0) - ((COUNT(*) - SUM(follow_public_successful)) * 110.0)) / (COUNT(*) * 110.0) * 100)::NUMERIC, 1) as follow_public_roi_per_100,
    
    -- Signal quality metrics
    ROUND((AVG(public_consensus_strength))::NUMERIC, 1) as avg_public_consensus_pct,
    ROUND((AVG(num_books))::NUMERIC, 1) as avg_books_per_signal,
    ROUND((AVG(money_pct_stddev))::NUMERIC, 1) as avg_book_agreement,
    COUNT(DISTINCT home_team || '-' || away_team || '-' || DATE(game_datetime)) as unique_games,
    
    -- Performance edge calculation
    ROUND((AVG(fade_successful) - AVG(follow_public_successful))::NUMERIC, 3) as fade_edge_over_public,
    
    -- Recent examples for validation
    STRING_AGG(
        home_team || ' vs ' || away_team || ' (Fade: ' || fade_recommendation || 
        ', Public: ' || ROUND((avg_money_pct)::NUMERIC, 0) || '%, Result: ' || 
        CASE WHEN fade_successful = 1 THEN 'WIN' ELSE 'LOSS' END || ')', 
        '; '
    ) as recent_examples

FROM fade_with_outcomes
WHERE fade_successful IS NOT NULL
GROUP BY split_type, public_consensus_type, fade_confidence
HAVING COUNT(*) >= 3  -- Minimum sample size

UNION ALL

-- Summary across all variants
SELECT 
    'PUBLIC_MONEY_FADE_SUMMARY' as strategy_name,
    'ALL' as split_type,
    'ALL_VARIANTS' as strategy_variant,
    'ALL' as fade_confidence,
    'fade-strategy-summary' as source_book_type,
    COUNT(*) as total_bets,
    SUM(fade_successful) as wins,
    ROUND((AVG(fade_successful) * 100)::NUMERIC, 1) as win_rate,
    ROUND((((SUM(fade_successful) * 100.0) - ((COUNT(*) - SUM(fade_successful)) * 110.0)) / (COUNT(*) * 110.0) * 100)::NUMERIC, 1) as roi_per_100_unit,
    
    SUM(follow_public_successful) as follow_public_wins,
    ROUND((AVG(follow_public_successful) * 100)::NUMERIC, 1) as follow_public_win_rate_pct,
    ROUND((((SUM(follow_public_successful) * 100.0) - ((COUNT(*) - SUM(follow_public_successful)) * 110.0)) / (COUNT(*) * 110.0) * 100)::NUMERIC, 1) as follow_public_roi_per_100,
    
    ROUND((AVG(public_consensus_strength))::NUMERIC, 1) as avg_public_consensus_pct,
    ROUND((AVG(num_books))::NUMERIC, 1) as avg_books_per_signal,
    ROUND((AVG(money_pct_stddev))::NUMERIC, 1) as avg_book_agreement,
    COUNT(DISTINCT home_team || '-' || away_team || '-' || DATE(game_datetime)) as unique_games,
    
    ROUND((AVG(fade_successful) - AVG(follow_public_successful))::NUMERIC, 3) as fade_edge_over_public,
    
    'Summary of all public money fade opportunities' as recent_examples

FROM fade_with_outcomes
WHERE fade_successful IS NOT NULL

ORDER BY roi_per_100_unit DESC; 