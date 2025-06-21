-- Enhanced Sharp Action Detection Strategy
-- Implements professional betting expert recommendations for more robust sharp detection
-- Key Enhancements:
-- 1. Volume-adjusted sharp detection
-- 2. Book-specific reliability weighting  
-- 3. Line movement correlation validation
-- 4. Multi-book consensus detection
-- 5. Enhanced ROI calculations with timing adjustments
-- 6. Situational context filters
-- 7. Steam move detection
-- 8. Contrarian validation
--
-- DEDUPLICATION APPROACH:
-- This script preserves ALL line movement data for analysis but implements 
-- recommendation-level deduplication to ensure only ONE final bet per game per market.
-- Final recommendations use data closest to 5 minutes before first pitch.

WITH enhanced_sharp_data AS (
    SELECT 
        rmbs.game_id,
        rmbs.source,
        rmbs.book,
        rmbs.split_type,
        rmbs.home_team,
        rmbs.away_team,
        rmbs.game_datetime,
        rmbs.last_updated,
        rmbs.split_value,
        rmbs.home_or_over_stake_percentage as stake_pct,
        rmbs.home_or_over_bets_percentage as bet_pct,
        rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage as raw_differential,
        
        -- Enhanced volume context
        COALESCE(rmbs.home_or_over_bets + rmbs.away_or_under_bets, 0) as total_bet_count,
        CASE 
            WHEN COALESCE(rmbs.home_or_over_bets + rmbs.away_or_under_bets, 0) < 100 THEN 'LOW_VOLUME'
            WHEN COALESCE(rmbs.home_or_over_bets + rmbs.away_or_under_bets, 0) < 500 THEN 'MEDIUM_VOLUME'  
            ELSE 'HIGH_VOLUME'
        END as volume_tier,
        
        -- Book reliability weighting
        CASE rmbs.book
            WHEN 'Pinnacle' THEN (rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage) * 1.5
            WHEN 'BookMaker' THEN (rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage) * 1.3
            WHEN 'Circa' THEN (rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage) * 1.2
            WHEN 'BetMGM' THEN (rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage) * 1.1
            WHEN 'DraftKings' THEN (rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage) * 0.9
            WHEN 'FanDuel' THEN (rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage) * 0.9
            ELSE (rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage)
        END as weighted_differential,
        
        -- Line movement tracking
        LAG(rmbs.split_value) OVER (
            PARTITION BY rmbs.game_id, rmbs.split_type, rmbs.book 
            ORDER BY rmbs.last_updated
        ) as prev_line,
        
        -- Calculate hours and minutes before game
        EXTRACT('epoch' FROM (rmbs.game_datetime - rmbs.last_updated)) / 3600 AS hours_before_game,
        EXTRACT('epoch' FROM (rmbs.game_datetime - rmbs.last_updated)) / 60 AS minutes_before_game,
        
        -- Game context (day of week, primetime, etc.)
        EXTRACT(DOW FROM rmbs.game_datetime) as day_of_week,
        CASE 
            WHEN EXTRACT(DOW FROM rmbs.game_datetime) IN (5,6,0) THEN 'WEEKEND'  -- Fri/Sat/Sun
            WHEN EXTRACT(HOUR FROM rmbs.game_datetime) BETWEEN 19 AND 22 THEN 'PRIMETIME'
            ELSE 'REGULAR'
        END as game_context,
        
        -- Timing classification
        CASE 
            WHEN EXTRACT('epoch' FROM (rmbs.game_datetime - rmbs.last_updated)) / 3600 <= 2 THEN 'CLOSING'
            WHEN EXTRACT('epoch' FROM (rmbs.game_datetime - rmbs.last_updated)) / 3600 <= 6 THEN 'LATE'
            WHEN EXTRACT('epoch' FROM (rmbs.game_datetime - rmbs.last_updated)) / 3600 <= 24 THEN 'EARLY'
            ELSE 'VERY_EARLY'
        END as timing_category,
        
        go.home_win,
        go.home_cover_spread,
        go.over
        
    FROM mlb_betting.splits.raw_mlb_betting_splits rmbs
    JOIN mlb_betting.main.game_outcomes go ON rmbs.game_id = go.game_id
    WHERE rmbs.last_updated < rmbs.game_datetime
      AND rmbs.split_value IS NOT NULL
      AND rmbs.game_datetime IS NOT NULL
      AND rmbs.home_or_over_stake_percentage IS NOT NULL
      AND rmbs.home_or_over_bets_percentage IS NOT NULL
),

line_movement_analysis AS (
    SELECT 
        *,
        -- Calculate line movement
        CASE 
            WHEN prev_line IS NOT NULL AND split_value IS NOT NULL THEN
                TRY_CAST(split_value AS FLOAT) - TRY_CAST(prev_line AS FLOAT)
            ELSE NULL
        END as line_movement,
        
        -- Volume-adjusted sharp detection with enhanced logic
        CASE 
            WHEN ABS(weighted_differential) >= 20 AND total_bet_count >= 100 THEN 'PREMIUM_SHARP'
            WHEN ABS(weighted_differential) >= 15 THEN 'STRONG_SHARP'
            WHEN ABS(weighted_differential) >= 10 AND total_bet_count >= 50 THEN 'MODERATE_SHARP'
            WHEN ABS(weighted_differential) >= 7 AND timing_category = 'CLOSING' THEN 'LATE_SHARP'
            -- Volume-adjusted thresholds
            WHEN volume_tier = 'HIGH_VOLUME' AND ABS(weighted_differential) >= 8 THEN 'VOLUME_SHARP'
            WHEN volume_tier = 'MEDIUM_VOLUME' AND ABS(weighted_differential) >= 10 THEN 'VOLUME_SHARP'
            WHEN volume_tier = 'LOW_VOLUME' AND ABS(weighted_differential) >= 12 THEN 'VOLUME_SHARP'
            ELSE 'NO_SHARP'
        END as enhanced_sharp_indicator,
        
        -- Direction for consensus analysis
        CASE 
            WHEN weighted_differential > 7 THEN 'HOME_OVER_SHARP'
            WHEN weighted_differential < -7 THEN 'AWAY_UNDER_SHARP'
            ELSE 'NO_SHARP'
        END as sharp_direction,
        
        -- Contrarian validation
        CASE 
            WHEN weighted_differential > 15 AND bet_pct > 70 THEN 'STRONG_CONTRARIAN'
            WHEN weighted_differential < -15 AND bet_pct < 30 THEN 'STRONG_CONTRARIAN'
            WHEN weighted_differential > 10 AND bet_pct > 65 THEN 'MODERATE_CONTRARIAN'
            WHEN weighted_differential < -10 AND bet_pct < 35 THEN 'MODERATE_CONTRARIAN'
            ELSE 'NORMAL'
        END as contrarian_indicator
        
    FROM enhanced_sharp_data
),

validated_sharp_action AS (
    SELECT 
        *,
        -- Sharp validation: does money differential align with line movement?
        CASE 
            WHEN weighted_differential > 10 AND line_movement > 0 THEN 'VALIDATED_SHARP_HOME'
            WHEN weighted_differential < -10 AND line_movement < 0 THEN 'VALIDATED_SHARP_AWAY'
            WHEN ABS(weighted_differential) > 10 AND ABS(line_movement) < 0.5 THEN 'UNVALIDATED_STATIC'
            WHEN ABS(weighted_differential) > 10 THEN 'UNVALIDATED_OPPOSITE'
            ELSE 'NO_VALIDATION_NEEDED'
        END as sharp_validation,
        
        -- Steam move detection (multiple books moving within 10 minutes)
        COUNT(*) OVER (
            PARTITION BY game_id, split_type, sharp_direction
            ORDER BY last_updated 
            RANGE BETWEEN INTERVAL '10 minutes' PRECEDING AND CURRENT ROW
        ) as concurrent_moves,
        
        -- Multi-book consensus count
        COUNT(*) OVER (
            PARTITION BY game_id, split_type, sharp_direction
        ) as consensus_count
        
    FROM line_movement_analysis
    WHERE enhanced_sharp_indicator != 'NO_SHARP'
),

closing_enhanced_sharp AS (
    SELECT 
        game_id,
        source,
        book,
        split_type,
        home_team,
        away_team,
        timing_category,
        game_context,
        volume_tier,
        
        -- Final sharp indicators (closest to game time)
        LAST_VALUE(enhanced_sharp_indicator) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as final_sharp_indicator,
        
        LAST_VALUE(weighted_differential) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as final_weighted_differential,
        
        LAST_VALUE(raw_differential) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as final_raw_differential,
        
        LAST_VALUE(stake_pct) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as final_stake_pct,
        
        LAST_VALUE(bet_pct) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as final_bet_pct,
        
        LAST_VALUE(sharp_validation) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as final_validation,
        
        LAST_VALUE(contrarian_indicator) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as final_contrarian,
        
        LAST_VALUE(consensus_count) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as final_consensus_count,
        
        MAX(concurrent_moves) OVER (
            PARTITION BY game_id, source, book, split_type
        ) as max_steam_moves,
        
        home_win,
        home_cover_spread,
        over,
        
        -- RECOMMENDATION-LEVEL DEDUPLICATION:
        -- Select the record closest to 5 minutes before game time for final betting recommendation
        ROW_NUMBER() OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY 
                ABS(minutes_before_game - 5) ASC,  -- Closest to 5 minutes before game
                last_updated DESC                  -- Most recent if tied
        ) as rn
        
    FROM validated_sharp_action
),

enhanced_strategy_analysis AS (
    SELECT 
        CONCAT(source, '-', book, '-', split_type) as source_book_type,
        split_type,
        final_sharp_indicator,
        timing_category,
        volume_tier,
        game_context,
        final_validation,
        final_contrarian,
        
        -- Count total bets for each enhanced sharp indicator
        COUNT(*) as total_bets,
        
        -- Calculate win rates for following enhanced sharp action
        SUM(CASE 
            WHEN final_weighted_differential > 0 THEN  -- Sharp money on home/over
                CASE split_type
                    WHEN 'moneyline' THEN CASE WHEN home_win = true THEN 1 ELSE 0 END
                    WHEN 'spread' THEN CASE WHEN home_cover_spread = true THEN 1 ELSE 0 END
                    WHEN 'total' THEN CASE WHEN over = true THEN 1 ELSE 0 END
                END
            WHEN final_weighted_differential < 0 THEN  -- Sharp money on away/under
                CASE split_type
                    WHEN 'moneyline' THEN CASE WHEN home_win = false THEN 1 ELSE 0 END
                    WHEN 'spread' THEN CASE WHEN home_cover_spread = false THEN 1 ELSE 0 END
                    WHEN 'total' THEN CASE WHEN over = false THEN 1 ELSE 0 END
                END
            ELSE 0
        END) as sharp_wins,
        
        -- Enhanced metrics
        AVG(final_weighted_differential) as avg_weighted_differential,
        AVG(final_raw_differential) as avg_raw_differential,
        AVG(final_stake_pct) as avg_stake_pct,
        AVG(final_bet_pct) as avg_bet_pct,
        AVG(final_consensus_count) as avg_consensus,
        AVG(max_steam_moves) as avg_steam_moves,
        
        -- Validation metrics
        COUNT(CASE WHEN final_validation LIKE 'VALIDATED%' THEN 1 END) as validated_count,
        COUNT(CASE WHEN final_contrarian IN ('STRONG_CONTRARIAN', 'MODERATE_CONTRARIAN') THEN 1 END) as contrarian_count
        
    FROM closing_enhanced_sharp
    WHERE rn = 1 
      AND final_sharp_indicator NOT IN ('NO_SHARP')
    GROUP BY source, book, split_type, final_sharp_indicator, timing_category, volume_tier, game_context, final_validation, final_contrarian
    HAVING COUNT(*) >= 5  -- Minimum sample size
)

SELECT 
    source_book_type,
    split_type,
    final_sharp_indicator,
    timing_category,
    volume_tier,
    game_context,
    final_validation,
    final_contrarian,
    total_bets,
    sharp_wins,
    
    -- Win rate
    ROUND(100.0 * sharp_wins / total_bets, 1) as win_rate,
    
    -- Enhanced ROI calculation with timing adjustments
    ROUND(CASE 
        WHEN timing_category = 'CLOSING' THEN 
            -- Closing sharp money often gets worse odds
            ((sharp_wins * 91) - ((total_bets - sharp_wins) * 110)) / (total_bets * 110.0) * 100
        WHEN timing_category = 'EARLY' THEN
            -- Early sharp money gets better odds  
            ((sharp_wins * 100) - ((total_bets - sharp_wins) * 105)) / (total_bets * 105.0) * 100
        ELSE 
            -- Standard calculation for LATE timing
            ((sharp_wins * 95) - ((total_bets - sharp_wins) * 110)) / (total_bets * 110.0) * 100
    END, 1) as adjusted_roi,
    
    -- Metrics
    ROUND(avg_weighted_differential, 1) as avg_weighted_diff,
    ROUND(avg_raw_differential, 1) as avg_raw_diff,
    ROUND(avg_consensus, 1) as avg_consensus_books,
    ROUND(avg_steam_moves, 1) as avg_steam_moves,
    
    -- Validation percentages
    ROUND(100.0 * validated_count / total_bets, 1) as validation_rate,
    ROUND(100.0 * contrarian_count / total_bets, 1) as contrarian_rate,
    
    -- Enhanced strategy classification
    CASE 
        WHEN (100.0 * sharp_wins / total_bets) >= 65 AND total_bets >= 15 AND avg_consensus >= 2 THEN '🟢 ELITE SHARP EDGE'
        WHEN (100.0 * sharp_wins / total_bets) >= 60 AND total_bets >= 10 AND validation_rate >= 70 THEN '🟢 STRONG SHARP EDGE'
        WHEN (100.0 * sharp_wins / total_bets) >= 57 AND total_bets >= 10 AND contrarian_rate >= 50 THEN '🟡 CONTRARIAN EDGE'
        WHEN (100.0 * sharp_wins / total_bets) >= 55 AND total_bets >= 10 THEN '🟡 MODERATE SHARP EDGE'
        WHEN (100.0 * sharp_wins / total_bets) >= 52.4 AND total_bets >= 10 THEN '🟡 SLIGHT EDGE'
        ELSE '🔴 NO EDGE'
    END as enhanced_strategy_rating,
    
    -- Enhanced confidence level
    CASE 
        WHEN total_bets >= 50 AND validation_rate >= 70 THEN 'VERY_HIGH'
        WHEN total_bets >= 30 AND validation_rate >= 60 THEN 'HIGH'
        WHEN total_bets >= 20 THEN 'MEDIUM'
        WHEN total_bets >= 10 THEN 'LOW'
        ELSE 'VERY_LOW'
    END as confidence_level
    
FROM enhanced_strategy_analysis
WHERE total_bets >= 5
ORDER BY 
    CASE 
        WHEN enhanced_strategy_rating LIKE '🟢 ELITE%' THEN 1
        WHEN enhanced_strategy_rating LIKE '🟢 STRONG%' THEN 2
        WHEN enhanced_strategy_rating LIKE '🟡 CONTRARIAN%' THEN 3
        WHEN enhanced_strategy_rating LIKE '🟡 MODERATE%' THEN 4
        WHEN enhanced_strategy_rating LIKE '🟡 SLIGHT%' THEN 5
        ELSE 6
    END,
    (100.0 * sharp_wins / total_bets) DESC,
    total_bets DESC; 