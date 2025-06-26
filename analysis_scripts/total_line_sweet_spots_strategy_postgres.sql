-- Simplified Total Line Sweet Spots Strategy Analysis for PostgreSQL
-- Focuses on public bias patterns at key total numbers with sharp money disagreement
-- Simplified to work with current VSIN data structure (no volume counts available)

WITH totals_data AS (
    SELECT 
        rmbs.game_id,
        rmbs.source,
        COALESCE(rmbs.book, 'UNKNOWN') as book,
        rmbs.home_team,
        rmbs.away_team,
        rmbs.game_datetime,
        rmbs.last_updated,
        
        -- Extract total line value with safe casting
        CASE 
            WHEN rmbs.split_type = 'total' AND rmbs.split_value IS NOT NULL 
                 AND rmbs.split_value ~ '^-?[0-9]+\.?[0-9]*$' THEN
                rmbs.split_value::DOUBLE PRECISION
            ELSE NULL
        END as total_line,
        
        rmbs.home_or_over_stake_percentage as over_stake_pct,
        rmbs.home_or_over_bets_percentage as over_bet_pct,
        rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage as over_differential,
        
        go.over,
        go.home_score + go.away_score as total_runs,
        
        -- Get latest data per game/source/book
        ROW_NUMBER() OVER (
            PARTITION BY rmbs.game_id, rmbs.source, rmbs.book 
            ORDER BY rmbs.last_updated DESC
        ) as latest_rank
        
    FROM splits.raw_mlb_betting_splits rmbs
    JOIN public.game_outcomes go ON rmbs.game_id = go.game_id
    WHERE rmbs.split_type = 'total'
      AND rmbs.last_updated < rmbs.game_datetime
      AND rmbs.split_value IS NOT NULL
      AND rmbs.split_value ~ '^-?[0-9]+\.?[0-9]*$'
      AND rmbs.home_or_over_stake_percentage IS NOT NULL
      AND rmbs.home_or_over_bets_percentage IS NOT NULL
      AND go.home_score IS NOT NULL
      AND go.away_score IS NOT NULL
),

-- Analyze sweet spot patterns
sweet_spot_analysis AS (
    SELECT 
        source,
        book,
        game_id,
        home_team,
        away_team,
        total_line,
        over_stake_pct,
        over_bet_pct,
        over_differential,
        total_runs,
        over as went_over,
        
        -- Simplified key number classification (broader groupings)
        CASE 
            WHEN total_line IN (7.5, 8.5, 9.5) THEN 'KEY_NUMBERS'
            WHEN total_line IN (8.0, 9.0, 10.0) THEN 'WHOLE_NUMBERS'
            ELSE 'OTHER_TOTALS'
        END as line_category,
        
        -- Public bias detection (simplified thresholds)
        CASE 
            WHEN over_bet_pct >= 65 THEN 'PUBLIC_OVER'
            WHEN over_bet_pct <= 35 THEN 'PUBLIC_UNDER'
            ELSE 'BALANCED_PUBLIC'
        END as public_bias,
        
        -- Sharp money detection (simplified)
        CASE 
            WHEN over_differential >= 10 THEN 'SHARP_OVER'
            WHEN over_differential <= -10 THEN 'SHARP_UNDER'
            ELSE 'NO_SHARP_SIGNAL'
        END as sharp_signal
        
    FROM totals_data
    WHERE latest_rank = 1
      AND total_line IS NOT NULL
      AND total_line BETWEEN 6.0 AND 12.0  -- Focus on realistic total range
),

-- Generate betting signals (broader categories)
betting_signals AS (
    SELECT 
        *,
        -- Simplified value signal generation
        CASE 
            -- Fade public when sharp money disagrees
            WHEN public_bias = 'PUBLIC_OVER' AND sharp_signal = 'SHARP_UNDER' THEN 'FADE_PUBLIC_OVER'
            WHEN public_bias = 'PUBLIC_UNDER' AND sharp_signal = 'SHARP_OVER' THEN 'FADE_PUBLIC_UNDER'
            
            -- Public bias without opposing sharp money (weaker signals)
            WHEN public_bias = 'PUBLIC_OVER' AND sharp_signal != 'SHARP_OVER' AND line_category = 'KEY_NUMBERS' THEN 'WEAK_FADE_OVER'
            WHEN public_bias = 'PUBLIC_UNDER' AND sharp_signal != 'SHARP_UNDER' AND line_category = 'KEY_NUMBERS' THEN 'WEAK_FADE_UNDER'
            
            ELSE 'NO_VALUE_SIGNAL'
        END as value_signal
        
    FROM sweet_spot_analysis
),

-- Strategy performance analysis
strategy_results AS (
    SELECT 
        CONCAT(source, '-', book) as source_book_type,
        'total' as split_type,
        line_category,
        value_signal,
        public_bias,
        sharp_signal,
        
        COUNT(*) as total_bets,
        
        -- Calculate wins based on value signal
        SUM(CASE 
            WHEN value_signal IN ('FADE_PUBLIC_OVER', 'WEAK_FADE_OVER') AND went_over = false THEN 1
            WHEN value_signal IN ('FADE_PUBLIC_UNDER', 'WEAK_FADE_UNDER') AND went_over = true THEN 1
            ELSE 0
        END) as wins,
        
        -- Supporting metrics
        AVG(total_line) as avg_total_line,
        AVG(over_stake_pct) as avg_over_stake_pct,
        AVG(over_bet_pct) as avg_over_bet_pct,
        AVG(over_differential) as avg_over_differential,
        AVG(total_runs) as avg_total_runs,
        
        -- Success rate metrics
        AVG(CASE WHEN went_over = true THEN 1.0 ELSE 0.0 END) as over_hit_rate
        
    FROM betting_signals
    WHERE value_signal != 'NO_VALUE_SIGNAL'
    GROUP BY source, book, line_category, value_signal, public_bias, sharp_signal
    HAVING COUNT(*) >= 2  -- Lowered minimum sample size
),

-- Final performance calculations
final_performance AS (
    SELECT 
        source_book_type,
        split_type,
        'TOTAL_SWEET_SPOTS' as strategy_name,
        CASE 
            WHEN line_category = 'KEY_NUMBERS' THEN 'KEY_' || value_signal
            ELSE line_category || '_' || value_signal
        END as strategy_variant,
        
        total_bets,
        wins,
        
        -- Core performance metrics
        ROUND(100.0 * wins / total_bets, 1) as win_rate,
        
        -- ROI calculation (assuming -110 juice)
        ROUND(((wins * 100) - ((total_bets - wins) * 110)) / (total_bets * 110.0) * 100, 1) as roi_per_100,
        
        -- Supporting metrics
        ROUND(avg_total_line, 1) as avg_line,
        ROUND(avg_over_stake_pct, 1) as avg_stake_pct,
        ROUND(avg_over_bet_pct, 1) as avg_bet_pct,
        ROUND(avg_over_differential, 1) as avg_differential,
        ROUND(avg_total_runs, 1) as avg_runs,
        ROUND(over_hit_rate * 100, 1) as over_percentage,
        
        -- Strategy rating
        CASE 
            WHEN (100.0 * wins / total_bets) >= 65 AND total_bets >= 10 THEN '🟢 ELITE'
            WHEN (100.0 * wins / total_bets) >= 60 AND total_bets >= 8 THEN '🟢 EXCELLENT'
            WHEN (100.0 * wins / total_bets) >= 57 AND total_bets >= 5 THEN '🟢 VERY GOOD'
            WHEN (100.0 * wins / total_bets) >= 54 AND total_bets >= 3 THEN '🟢 GOOD'  
            WHEN (100.0 * wins / total_bets) >= 52.4 AND total_bets >= 2 THEN '🟡 PROFITABLE'
            WHEN (100.0 * wins / total_bets) >= 50 AND total_bets >= 2 THEN '🟡 SLIGHT EDGE'
            ELSE '🔴 UNPROFITABLE'
        END as strategy_rating,
        
        -- Confidence level
        CASE 
            WHEN total_bets >= 10 THEN 'HIGH'
            WHEN total_bets >= 5 THEN 'MEDIUM'
            WHEN total_bets >= 3 THEN 'LOW'
            ELSE 'VERY_LOW'
        END as confidence_level,
        
        public_bias,
        sharp_signal
        
    FROM strategy_results
    WHERE total_bets >= 2  -- Lowered threshold
)

-- Main results
SELECT 
    source_book_type,
    split_type,
    strategy_name,
    strategy_variant,
    total_bets,
    wins,
    win_rate,
    roi_per_100,
    avg_line,
    avg_stake_pct,
    avg_bet_pct,
    avg_differential,
    avg_runs,
    over_percentage,
    strategy_rating,
    confidence_level,
    
    -- Strategy insights
    CASE 
        WHEN strategy_variant LIKE 'KEY_FADE_PUBLIC_OVER%' THEN 'Fade public Over bias at key totals (7.5, 8.5, 9.5) with sharp disagreement'
        WHEN strategy_variant LIKE 'KEY_FADE_PUBLIC_UNDER%' THEN 'Fade public Under bias at key totals with sharp disagreement'
        WHEN strategy_variant LIKE 'KEY_WEAK_FADE_OVER%' THEN 'Weak fade public Over bias at key totals'
        WHEN strategy_variant LIKE 'KEY_WEAK_FADE_UNDER%' THEN 'Weak fade public Under bias at key totals'
        WHEN strategy_variant LIKE 'WHOLE_NUMBERS%' THEN 'Public bias opportunities at whole number totals'
        ELSE 'General totals value opportunity'
    END as strategy_insight,
    
    -- Context
    CONCAT('PUBLIC: ', public_bias, ' | SHARP: ', sharp_signal) as bias_context
    
FROM final_performance
ORDER BY 
    CASE 
        WHEN strategy_rating LIKE '🟢 ELITE%' THEN 1
        WHEN strategy_rating LIKE '🟢 EXCELLENT%' THEN 2
        WHEN strategy_rating LIKE '🟢 VERY%' THEN 3
        WHEN strategy_rating LIKE '🟢 GOOD%' THEN 4
        WHEN strategy_rating LIKE '🟡 PROFITABLE%' THEN 5
        WHEN strategy_rating LIKE '🟡 SLIGHT%' THEN 6
        ELSE 7
    END,
    roi_per_100 DESC,
    total_bets DESC; 