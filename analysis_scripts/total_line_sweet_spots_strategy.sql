-- Enhanced Total Line Sweet Spots Strategy Analysis
-- Implements betting expert recommendations for more sophisticated total betting
-- Key Enhancements:
-- 1. Enhanced key number classification beyond main three
-- 2. Ballpark factor integration using existing team data
-- 3. Game timing factors (day/night games)
-- 4. Line movement validation
-- 5. Volume-adjusted public bias detection
-- 6. Multi-factor value signal combinations
-- 7. Enhanced strategy performance classification
--
-- Tests performance around key total numbers where public bias creates value
-- MLB totals often have inefficiencies around psychologically important numbers

WITH enhanced_total_data AS (
    SELECT 
        rmbs.game_id,
        rmbs.source,
        COALESCE(rmbs.book, 'UNKNOWN') as book,
        rmbs.home_team,
        rmbs.away_team,
        rmbs.game_datetime,
        rmbs.last_updated,
        rmbs.home_or_over_stake_percentage as over_stake_pct,
        rmbs.home_or_over_bets_percentage as over_bet_pct,
        rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage as over_differential,
        
        -- Enhanced volume context
        COALESCE(rmbs.home_or_over_bets + rmbs.away_or_under_bets, 0) as total_bet_volume,
        
        -- Extract total line value with safe casting
        CASE 
            WHEN rmbs.split_type = 'total' AND rmbs.split_value IS NOT NULL 
                 AND rmbs.split_value ~ '^-?[0-9]+\.?[0-9]*$' THEN
                rmbs.split_value::DOUBLE PRECISION
            ELSE NULL
        END as total_line,
        
        -- Line movement tracking with safe casting
        LAG(CASE WHEN rmbs.split_value ~ '^-?[0-9]+\.?[0-9]*$' 
                 THEN rmbs.split_value::DOUBLE PRECISION 
                 ELSE NULL END) OVER (
            PARTITION BY rmbs.game_id, rmbs.source, rmbs.book 
            ORDER BY rmbs.last_updated
        ) as prev_total_line,
        
        -- Game timing context (day vs night games)
        CASE 
            WHEN EXTRACT(HOUR FROM rmbs.game_datetime) <= 16 THEN 'DAY_GAME'
            WHEN EXTRACT(HOUR FROM rmbs.game_datetime) >= 19 THEN 'NIGHT_GAME'
            ELSE 'TWILIGHT_GAME'
        END as game_timing,
        
        -- Ballpark factor classification (using existing team data)
        CASE rmbs.home_team
            WHEN 'COL' THEN 'EXTREME_HITTERS_PARK'  -- Coors Field
            WHEN 'BOS' THEN 'HITTERS_PARK'          -- Fenway Park
            WHEN 'TEX' THEN 'HITTERS_PARK'          -- Globe Life Field
            WHEN 'CIN' THEN 'HITTERS_PARK'          -- Great American Ball Park
            WHEN 'MIA' THEN 'PITCHERS_PARK'         -- loanDepot Park
            WHEN 'OAK' THEN 'PITCHERS_PARK'         -- Oakland Coliseum
            WHEN 'SEA' THEN 'PITCHERS_PARK'         -- T-Mobile Park
            WHEN 'SD' THEN 'PITCHERS_PARK'          -- Petco Park
            WHEN 'SF' THEN 'PITCHERS_PARK'          -- Oracle Park
            WHEN 'DET' THEN 'MODERATE_PITCHERS_PARK' -- Comerica Park
            WHEN 'NYY' THEN 'MODERATE_HITTERS_PARK'  -- Yankee Stadium
            WHEN 'CHC' THEN 'WIND_DEPENDENT'        -- Wrigley Field
            ELSE 'NEUTRAL_PARK'
        END as ballpark_factor,
        
        -- Calculate hours before game
        EXTRACT('epoch' FROM (rmbs.game_datetime - rmbs.last_updated)) / 3600 AS hours_before_game,
        
        go.over,
        go.home_score + go.away_score as total_runs,
        
        ROW_NUMBER() OVER (
            PARTITION BY rmbs.game_id, rmbs.source, rmbs.book 
            ORDER BY rmbs.last_updated DESC
        ) as latest_rank
        
    FROM splits.raw_mlb_betting_splits rmbs
    JOIN public.game_outcomes go ON rmbs.game_id = go.game_id
    WHERE rmbs.split_type = 'total'
      AND rmbs.last_updated < rmbs.game_datetime
      AND rmbs.split_value IS NOT NULL
      AND rmbs.home_or_over_stake_percentage IS NOT NULL
      AND rmbs.home_or_over_bets_percentage IS NOT NULL
      AND go.home_score IS NOT NULL
      AND go.away_score IS NOT NULL
),

-- Enhanced sweet spot analysis with more sophisticated factors
enhanced_sweet_spot_analysis AS (
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
        game_timing,
        ballpark_factor,
        total_bet_volume,
        
        -- Calculate line movement
        CASE 
            WHEN prev_total_line IS NOT NULL AND total_line IS NOT NULL THEN
                total_line - prev_total_line
            ELSE NULL
        END as line_movement,
        
        -- Enhanced key number classification
        CASE 
            WHEN total_line = 7.5 THEN 'PRIME_LOW'      -- Most important low total
            WHEN total_line = 8.5 THEN 'PRIME_MID'      -- Most common total
            WHEN total_line = 9.5 THEN 'PRIME_HIGH'     -- Most important high total
            WHEN total_line IN (8.0, 9.0, 10.0) THEN 'SECONDARY_WHOLE'
            WHEN total_line IN (7.0, 10.5, 11.0) THEN 'TERTIARY_KEY'
            WHEN total_line BETWEEN 6.5 AND 11.5 THEN 'NORMAL_RANGE'
            ELSE 'EXTREME_TOTAL'
        END as key_number_tier,
        
        -- Volume-adjusted public bias detection
        CASE 
            WHEN over_bet_pct >= 80 AND total_bet_volume >= 1000 THEN 'EXTREME_PUBLIC_OVER'
            WHEN over_bet_pct >= 75 AND total_bet_volume >= 500 THEN 'HEAVY_PUBLIC_OVER'
            WHEN over_bet_pct >= 65 AND total_bet_volume >= 200 THEN 'MODERATE_PUBLIC_OVER'
            WHEN over_bet_pct >= 58 AND total_bet_volume >= 100 THEN 'SLIGHT_PUBLIC_OVER'
            WHEN over_bet_pct <= 20 AND total_bet_volume >= 1000 THEN 'EXTREME_PUBLIC_UNDER'
            WHEN over_bet_pct <= 25 AND total_bet_volume >= 500 THEN 'HEAVY_PUBLIC_UNDER'
            WHEN over_bet_pct <= 35 AND total_bet_volume >= 200 THEN 'MODERATE_PUBLIC_UNDER'
            WHEN over_bet_pct <= 42 AND total_bet_volume >= 100 THEN 'SLIGHT_PUBLIC_UNDER'
            ELSE 'BALANCED_PUBLIC'
        END as volume_adjusted_bias,
        
        -- Enhanced sharp money detection
        CASE 
            WHEN over_differential >= 20 THEN 'STRONG_SHARP_OVER'
            WHEN over_differential >= 15 THEN 'MODERATE_SHARP_OVER'
            WHEN over_differential >= 10 THEN 'WEAK_SHARP_OVER'
            WHEN over_differential <= -20 THEN 'STRONG_SHARP_UNDER'
            WHEN over_differential <= -15 THEN 'MODERATE_SHARP_UNDER'
            WHEN over_differential <= -10 THEN 'WEAK_SHARP_UNDER'
            ELSE 'NO_SHARP_SIGNAL'
        END as sharp_signal,
        
        -- Ballpark-adjusted value detection
        CASE 
            WHEN ballpark_factor = 'EXTREME_HITTERS_PARK' AND total_line <= 8.5 THEN 'BALLPARK_VALUE_OVER'
            WHEN ballpark_factor IN ('HITTERS_PARK', 'MODERATE_HITTERS_PARK') AND total_line <= 8.0 THEN 'BALLPARK_VALUE_OVER'
            WHEN ballpark_factor IN ('PITCHERS_PARK', 'MODERATE_PITCHERS_PARK') AND total_line >= 9.0 THEN 'BALLPARK_VALUE_UNDER'
            WHEN ballpark_factor = 'WIND_DEPENDENT' THEN 'WEATHER_DEPENDENT'
            ELSE 'NEUTRAL_BALLPARK'
        END as ballpark_value,
        
        -- Game timing adjustments (day games often play under)
        CASE 
            WHEN game_timing = 'DAY_GAME' AND total_line >= 9.0 AND over_bet_pct >= 65 THEN 'TIMING_VALUE_UNDER'
            WHEN game_timing = 'NIGHT_GAME' AND total_line <= 8.0 AND over_bet_pct <= 35 THEN 'TIMING_VALUE_OVER'
            ELSE 'NEUTRAL_TIMING'
        END as timing_value
        
    FROM enhanced_total_data
    WHERE latest_rank = 1
      AND total_line IS NOT NULL
      AND total_line BETWEEN 6.0 AND 12.0  -- Focus on realistic total range
),

-- Multi-factor value signal generation
multi_factor_value_signals AS (
    SELECT 
        *,
        -- Line movement validation
        CASE 
            WHEN line_movement IS NOT NULL AND ABS(line_movement) >= 0.5 THEN
                CASE 
                    WHEN line_movement > 0 AND volume_adjusted_bias LIKE '%PUBLIC_OVER%' THEN 'VALIDATED_LINE_MOVEMENT'
                    WHEN line_movement < 0 AND volume_adjusted_bias LIKE '%PUBLIC_UNDER%' THEN 'VALIDATED_LINE_MOVEMENT'
                    ELSE 'CONTRARIAN_LINE_MOVEMENT'
                END
            ELSE 'NO_SIGNIFICANT_MOVEMENT'
        END as movement_validation,
        
        -- Enhanced value signal combinations
        CASE 
            -- Premium value: Multiple factors align
            WHEN key_number_tier IN ('PRIME_LOW', 'PRIME_MID', 'PRIME_HIGH') 
                 AND volume_adjusted_bias IN ('HEAVY_PUBLIC_OVER', 'EXTREME_PUBLIC_OVER')
                 AND sharp_signal LIKE '%SHARP_UNDER%'
                 AND ballpark_value != 'BALLPARK_VALUE_OVER' THEN 'PREMIUM_VALUE_UNDER'
                 
            WHEN key_number_tier IN ('PRIME_LOW', 'PRIME_MID', 'PRIME_HIGH') 
                 AND volume_adjusted_bias IN ('HEAVY_PUBLIC_UNDER', 'EXTREME_PUBLIC_UNDER')
                 AND sharp_signal LIKE '%SHARP_OVER%'
                 AND ballpark_value != 'BALLPARK_VALUE_UNDER' THEN 'PREMIUM_VALUE_OVER'
            
            -- Strong value: Key numbers with clear bias
            WHEN key_number_tier IN ('PRIME_LOW', 'PRIME_MID', 'PRIME_HIGH') 
                 AND volume_adjusted_bias IN ('MODERATE_PUBLIC_OVER', 'HEAVY_PUBLIC_OVER')
                 AND timing_value = 'TIMING_VALUE_UNDER' THEN 'STRONG_VALUE_UNDER'
                 
            WHEN key_number_tier IN ('PRIME_LOW', 'PRIME_MID', 'PRIME_HIGH') 
                 AND volume_adjusted_bias IN ('MODERATE_PUBLIC_UNDER', 'HEAVY_PUBLIC_UNDER')
                 AND ballpark_value = 'BALLPARK_VALUE_OVER' THEN 'STRONG_VALUE_OVER'
            
            -- Moderate value: Single factor advantage
            WHEN key_number_tier IN ('PRIME_LOW', 'PRIME_MID', 'PRIME_HIGH') 
                 AND volume_adjusted_bias LIKE '%PUBLIC_OVER%' THEN 'MODERATE_VALUE_UNDER'
                 
            WHEN key_number_tier IN ('PRIME_LOW', 'PRIME_MID', 'PRIME_HIGH') 
                 AND volume_adjusted_bias LIKE '%PUBLIC_UNDER%' THEN 'MODERATE_VALUE_OVER'
            
            -- Secondary opportunities
            WHEN key_number_tier = 'SECONDARY_WHOLE' 
                 AND volume_adjusted_bias IN ('HEAVY_PUBLIC_OVER', 'EXTREME_PUBLIC_OVER')
                 AND sharp_signal LIKE '%SHARP_UNDER%' THEN 'SECONDARY_VALUE_UNDER'
                 
            WHEN key_number_tier = 'SECONDARY_WHOLE' 
                 AND volume_adjusted_bias IN ('HEAVY_PUBLIC_UNDER', 'EXTREME_PUBLIC_UNDER')
                 AND sharp_signal LIKE '%SHARP_OVER%' THEN 'SECONDARY_VALUE_OVER'
            
            ELSE 'NO_VALUE_SIGNAL'
        END as enhanced_value_signal
        
    FROM enhanced_sweet_spot_analysis
),

-- Strategy performance analysis with enhanced metrics
enhanced_strategy_results AS (
    SELECT 
        CONCAT(source, '-', book) as source_book_type,
        'total' as split_type,
        key_number_tier,
        enhanced_value_signal,
        volume_adjusted_bias,
        sharp_signal,
        ballpark_value,
        timing_value,
        game_timing,
        movement_validation,
        
        COUNT(*) as total_bets,
        
        -- Calculate wins based on enhanced value signal
        SUM(CASE 
            WHEN enhanced_value_signal LIKE '%VALUE_OVER' AND went_over = true THEN 1
            WHEN enhanced_value_signal LIKE '%VALUE_UNDER' AND went_over = false THEN 1
            ELSE 0
        END) as wins,
        
        -- Enhanced metrics
        AVG(total_line) as avg_total_line,
        AVG(over_stake_pct) as avg_over_stake_pct,
        AVG(over_bet_pct) as avg_over_bet_pct,
        AVG(over_differential) as avg_over_differential,
        AVG(total_runs) as avg_total_runs,
        AVG(total_bet_volume) as avg_bet_volume,
        AVG(ABS(COALESCE(line_movement, 0))) as avg_line_movement,
        
        -- Success rate metrics
        AVG(CASE WHEN went_over = true THEN 1.0 ELSE 0.0 END) as over_hit_rate,
        
        -- Validation metrics
        COUNT(CASE WHEN movement_validation = 'VALIDATED_LINE_MOVEMENT' THEN 1 END) as validated_movements,
        COUNT(CASE WHEN ballpark_value != 'NEUTRAL_BALLPARK' THEN 1 END) as ballpark_advantages,
        COUNT(CASE WHEN timing_value != 'NEUTRAL_TIMING' THEN 1 END) as timing_advantages
        
    FROM multi_factor_value_signals
    WHERE enhanced_value_signal != 'NO_VALUE_SIGNAL'
    GROUP BY source, book, key_number_tier, enhanced_value_signal, volume_adjusted_bias, 
             sharp_signal, ballpark_value, timing_value, game_timing, movement_validation
    HAVING COUNT(*) >= 3  -- Minimum sample size for enhanced analysis
),

-- Final enhanced performance calculations
final_enhanced_performance AS (
    SELECT 
        source_book_type,
        split_type,
        'ENHANCED_TOTAL_SWEET_SPOTS' as strategy_name,
        CASE 
            WHEN key_number_tier IN ('PRIME_LOW', 'PRIME_MID', 'PRIME_HIGH') 
                 THEN key_number_tier || '_' || enhanced_value_signal
            ELSE 'SECONDARY_' || enhanced_value_signal
        END as strategy_variant,
        
        total_bets,
        wins,
        
        -- Core performance metrics
        ROUND(100.0 * wins / total_bets, 1) as win_rate,
        
        -- Enhanced ROI calculation considering line quality
        ROUND(CASE 
            WHEN enhanced_value_signal LIKE 'PREMIUM%' THEN
                -- Premium signals likely get better lines
                ((wins * 105) - ((total_bets - wins) * 105)) / (total_bets * 105.0) * 100
            WHEN enhanced_value_signal LIKE 'STRONG%' THEN
                -- Strong signals get standard lines
                ((wins * 100) - ((total_bets - wins) * 110)) / (total_bets * 110.0) * 100
            ELSE
                -- Moderate signals may get slightly worse lines
                ((wins * 95) - ((total_bets - wins) * 110)) / (total_bets * 110.0) * 100
        END, 1) as enhanced_roi,
        
        -- Supporting metrics
        ROUND(avg_total_line, 1) as avg_line,
        ROUND(avg_over_stake_pct, 1) as avg_stake_pct,
        ROUND(avg_over_bet_pct, 1) as avg_bet_pct,
        ROUND(avg_over_differential, 1) as avg_differential,
        ROUND(avg_total_runs, 1) as avg_runs,
        ROUND(avg_bet_volume, 0) as avg_volume,
        ROUND(avg_line_movement, 2) as avg_movement,
        ROUND(over_hit_rate * 100, 1) as over_percentage,
        
        -- Enhanced validation metrics
        ROUND(100.0 * validated_movements / total_bets, 1) as movement_validation_rate,
        ROUND(100.0 * ballpark_advantages / total_bets, 1) as ballpark_advantage_rate,
        ROUND(100.0 * timing_advantages / total_bets, 1) as timing_advantage_rate,
        
        -- Enhanced strategy rating
        CASE 
            WHEN (100.0 * wins / total_bets) >= 65 AND total_bets >= 15 AND enhanced_value_signal LIKE 'PREMIUM%' THEN '🟢 ELITE EDGE'
            WHEN (100.0 * wins / total_bets) >= 62 AND total_bets >= 12 AND validated_movements >= 3 THEN '🟢 EXCELLENT'
            WHEN (100.0 * wins / total_bets) >= 60 AND total_bets >= 10 THEN '🟢 VERY GOOD'
            WHEN (100.0 * wins / total_bets) >= 57 AND total_bets >= 8 THEN '🟢 GOOD'  
            WHEN (100.0 * wins / total_bets) >= 54 AND total_bets >= 5 THEN '🟡 PROFITABLE'
            WHEN (100.0 * wins / total_bets) >= 52.4 AND total_bets >= 5 THEN '🟡 SLIGHT EDGE'
            ELSE '🔴 UNPROFITABLE'
        END as enhanced_strategy_rating,
        
        -- Enhanced confidence level
        CASE 
            WHEN total_bets >= 30 AND movement_validation_rate >= 60 THEN 'VERY_HIGH'
            WHEN total_bets >= 20 AND (ballpark_advantage_rate >= 40 OR timing_advantage_rate >= 40) THEN 'HIGH'
            WHEN total_bets >= 15 THEN 'MEDIUM'
            WHEN total_bets >= 8 THEN 'LOW'
            ELSE 'VERY_LOW'
        END as confidence_level,
        
        volume_adjusted_bias,
        sharp_signal,
        ballpark_value,
        timing_value,
        game_timing,
        movement_validation
        
    FROM enhanced_strategy_results
    WHERE total_bets >= 3
)

-- Main results with enhanced insights
SELECT 
    source_book_type,
    split_type,
    strategy_name,
    strategy_variant,
    total_bets,
    wins,
    win_rate,
    enhanced_roi,
    avg_line,
    avg_stake_pct,
    avg_bet_pct,
    avg_differential,
    avg_runs,
    avg_volume,
    avg_movement,
    over_percentage,
    movement_validation_rate,
    ballpark_advantage_rate,
    timing_advantage_rate,
    enhanced_strategy_rating,
    confidence_level,
    
    -- Enhanced insights for each variant
    CASE 
        WHEN strategy_variant LIKE 'PRIME_LOW%' THEN 'Low total sweet spot (7.5) - public overreacts to pitching matchups'
        WHEN strategy_variant LIKE 'PRIME_MID%' THEN 'MLB average total (8.5) - most balanced but still exploitable'  
        WHEN strategy_variant LIKE 'PRIME_HIGH%' THEN 'High total sweet spot (9.5) - ballpark/weather factors critical'
        WHEN strategy_variant LIKE '%PREMIUM_VALUE_UNDER%' THEN 'Elite fade public Over - multiple factors align'
        WHEN strategy_variant LIKE '%PREMIUM_VALUE_OVER%' THEN 'Elite fade public Under - multiple factors align'
        WHEN strategy_variant LIKE '%STRONG_VALUE_UNDER%' THEN 'Strong fade public Over - timing/ballpark advantage'
        WHEN strategy_variant LIKE '%STRONG_VALUE_OVER%' THEN 'Strong fade public Under - ballpark factors favor Over'
        WHEN strategy_variant LIKE '%MODERATE_VALUE%' THEN 'Basic public fade at key numbers'
        WHEN strategy_variant LIKE 'SECONDARY%' THEN 'Non-prime totals with sharp money disagreement'
        ELSE 'General sweet spot value opportunity'
    END as enhanced_strategy_insight,
    
    -- Context factors for decision making
    CONCAT(
        CASE WHEN ballpark_value != 'NEUTRAL_BALLPARK' THEN 'BALLPARK: ' || ballpark_value || ' | ' ELSE '' END,
        CASE WHEN timing_value != 'NEUTRAL_TIMING' THEN 'TIMING: ' || timing_value || ' | ' ELSE '' END,
        CASE WHEN movement_validation != 'NO_SIGNIFICANT_MOVEMENT' THEN 'MOVEMENT: ' || movement_validation ELSE 'STATIC_LINES' END
    ) as context_factors
    
FROM final_enhanced_performance
ORDER BY 
    CASE 
        WHEN enhanced_strategy_rating LIKE '🟢 ELITE%' THEN 1
        WHEN enhanced_strategy_rating LIKE '🟢 EXCELLENT%' THEN 2
        WHEN enhanced_strategy_rating LIKE '🟢 VERY%' THEN 3
        WHEN enhanced_strategy_rating LIKE '🟢 GOOD%' THEN 4
        WHEN enhanced_strategy_rating LIKE '🟡 PROFITABLE%' THEN 5
        WHEN enhanced_strategy_rating LIKE '🟡 SLIGHT%' THEN 6
        ELSE 7
    END,
    enhanced_roi DESC,
    total_bets DESC 