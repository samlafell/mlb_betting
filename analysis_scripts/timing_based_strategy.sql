-- Enhanced Timing-Based Sharp Action Strategy
-- Incorporates expert analysis recommendations for sophisticated timing validation
-- Key enhancements: line movement validation, volume weighting, book credibility, 
-- multi-book consensus, reverse line movement detection, and game context

WITH enhanced_timing_data AS (
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
        rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage as differential,
        
        -- Volume calculation for weighting
        COALESCE(rmbs.home_or_over_bets + rmbs.away_or_under_bets, 0) as total_volume,
        
        -- Enhanced timing categories (more granular)
        CASE 
            WHEN EXTRACT('epoch' FROM (rmbs.game_datetime - rmbs.last_updated)) / 3600 <= 0.5 THEN 'ULTRA_LATE'
            WHEN EXTRACT('epoch' FROM (rmbs.game_datetime - rmbs.last_updated)) / 3600 <= 1 THEN 'CLOSING_HOUR'
            WHEN EXTRACT('epoch' FROM (rmbs.game_datetime - rmbs.last_updated)) / 3600 <= 2 THEN 'CLOSING_2H'
            WHEN EXTRACT('epoch' FROM (rmbs.game_datetime - rmbs.last_updated)) / 3600 <= 4 THEN 'LATE_AFTERNOON'
            WHEN EXTRACT('epoch' FROM (rmbs.game_datetime - rmbs.last_updated)) / 3600 <= 6 THEN 'LATE_6H'
            WHEN EXTRACT('epoch' FROM (rmbs.game_datetime - rmbs.last_updated)) / 3600 <= 12 THEN 'SAME_DAY'
            WHEN EXTRACT('epoch' FROM (rmbs.game_datetime - rmbs.last_updated)) / 3600 <= 24 THEN 'EARLY_24H'
            WHEN EXTRACT('epoch' FROM (rmbs.game_datetime - rmbs.last_updated)) / 3600 <= 48 THEN 'OPENING_48H'
            ELSE 'VERY_EARLY'
        END as precise_timing_category,
        
        -- Line movement tracking with safe casting
        LAG(CASE WHEN rmbs.split_value ~ '^-?[0-9]+\.?[0-9]*$' 
                 THEN rmbs.split_value::DOUBLE PRECISION 
                 ELSE NULL END) OVER (
            PARTITION BY rmbs.game_id, rmbs.source, rmbs.book, rmbs.split_type 
            ORDER BY rmbs.last_updated
        ) as prev_line,
        
        -- Calculate hours before game
        EXTRACT('epoch' FROM (rmbs.game_datetime - rmbs.last_updated)) / 3600 AS hours_before_game,
        
        -- Sharp action indicators
        CASE 
            WHEN rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage >= 15 THEN 'STRONG_SHARP_HOME_OVER'
            WHEN rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage >= 10 THEN 'MODERATE_SHARP_HOME_OVER'
            WHEN rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage >= 5 THEN 'WEAK_SHARP_HOME_OVER'
            WHEN rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage <= -15 THEN 'STRONG_SHARP_AWAY_UNDER'
            WHEN rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage <= -10 THEN 'MODERATE_SHARP_AWAY_UNDER'
            WHEN rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage <= -5 THEN 'WEAK_SHARP_AWAY_UNDER'
            ELSE 'NO_SHARP_ACTION'
        END as sharp_indicator,
        
        -- Volume reliability classification
        CASE 
            WHEN COALESCE(rmbs.home_or_over_bets + rmbs.away_or_under_bets, 0) >= 1000 THEN 'RELIABLE_VOLUME'
            WHEN COALESCE(rmbs.home_or_over_bets + rmbs.away_or_under_bets, 0) >= 500 THEN 'MODERATE_VOLUME'  
            ELSE 'INSUFFICIENT_VOLUME'
        END as volume_reliability,
        
        -- Book timing credibility scoring
        CASE COALESCE(rmbs.book, 'UNKNOWN')
            WHEN 'Pinnacle' THEN 4.0      -- Premium sportsbook
            WHEN 'Circa' THEN 3.5         -- Vegas sharp book
            WHEN 'BetMGM' THEN 2.5         -- Major book
            WHEN 'FanDuel' THEN 2.0        -- Public book
            WHEN 'DraftKings' THEN 2.0     -- Public book
            WHEN 'Caesars' THEN 2.0        -- Major book
            WHEN 'Bet365' THEN 2.5         -- International book
            ELSE 1.5                       -- Unknown/other books
        END as base_book_credibility,
        
        -- Game context integration
        CASE 
            WHEN EXTRACT(DOW FROM rmbs.game_datetime) IN (5,6,0) THEN 'WEEKEND_GAME'
            WHEN EXTRACT(HOUR FROM rmbs.game_datetime) BETWEEN 19 AND 22 THEN 'PRIMETIME'
            WHEN rmbs.home_team IN ('NYY', 'LAD', 'BOS', 'NYM', 'PHI', 'ATL') THEN 'MAJOR_MARKET'
            ELSE 'REGULAR_GAME'
        END as game_context,
        
        go.home_win,
        go.home_cover_spread,
        go.over
        
    FROM splits.raw_mlb_betting_splits rmbs
    JOIN public.game_outcomes go ON rmbs.game_id = go.game_id
    WHERE rmbs.last_updated < rmbs.game_datetime
      AND rmbs.game_datetime < CURRENT_TIMESTAMP - INTERVAL '6 hours'  -- Only completed games
      AND rmbs.split_value IS NOT NULL
      AND rmbs.game_datetime IS NOT NULL
      AND rmbs.home_or_over_stake_percentage IS NOT NULL
      AND rmbs.home_or_over_bets_percentage IS NOT NULL
),

enhanced_movement_analysis AS (
    SELECT 
        *,
        -- Calculate line movement with safe casting
        CASE WHEN split_value ~ '^-?[0-9]+\.?[0-9]*$' 
             THEN split_value::DOUBLE PRECISION 
             ELSE NULL END - prev_line as line_movement,
        
        -- Reverse line movement detection with safe casting
        CASE 
            WHEN differential > 10 AND (CASE WHEN split_value ~ '^-?[0-9]+\.?[0-9]*$' 
                                             THEN split_value::DOUBLE PRECISION 
                                             ELSE NULL END - prev_line) < 0 THEN 'SHARP_MONEY_VS_LINE_MOVE'
            WHEN differential < -10 AND (CASE WHEN split_value ~ '^-?[0-9]+\.?[0-9]*$' 
                                              THEN split_value::DOUBLE PRECISION 
                                              ELSE NULL END - prev_line) > 0 THEN 'SHARP_MONEY_VS_LINE_MOVE'
            WHEN differential > 15 AND ABS(CASE WHEN split_value ~ '^-?[0-9]+\.?[0-9]*$' 
                                                THEN split_value::DOUBLE PRECISION 
                                                ELSE NULL END - prev_line) < 0.25 THEN 'SHARP_MONEY_NO_MOVEMENT'
            ELSE 'NORMAL_CORRELATION'
        END as line_movement_correlation,
        
        -- Dynamic book credibility based on timing and context
        CASE precise_timing_category
            WHEN 'ULTRA_LATE' THEN base_book_credibility * 1.5      -- Ultra-late moves more valuable
            WHEN 'CLOSING_HOUR' THEN base_book_credibility * 1.3    -- Closing hour enhanced
            WHEN 'CLOSING_2H' THEN base_book_credibility * 1.2      -- Standard closing enhanced
            WHEN 'SAME_DAY' THEN base_book_credibility * 0.9        -- Same day slightly reduced
            WHEN 'OPENING_48H' THEN base_book_credibility * 0.8     -- Opening moves less reliable
            ELSE base_book_credibility
        END * 
        CASE game_context
            WHEN 'PRIMETIME' THEN 1.2      -- Primetime games get more sharp attention
            WHEN 'WEEKEND_GAME' THEN 1.1   -- Weekend games enhanced
            WHEN 'MAJOR_MARKET' THEN 1.05  -- Major market slight enhancement
            ELSE 1.0
        END as timing_credibility_score
        
    FROM enhanced_timing_data
),

timing_aggregated AS (
    SELECT 
        game_id,
        source,
        book,
        split_type,
        home_team,
        away_team,
        home_win,
        home_cover_spread,
        over,
        
        -- Find earliest sharp action (if any)
        MIN(CASE WHEN sharp_indicator != 'NO_SHARP_ACTION' THEN hours_before_game END) as earliest_sharp_hours,
        
        -- Count distinct sharp indicators
        COUNT(DISTINCT CASE WHEN sharp_indicator != 'NO_SHARP_ACTION' THEN sharp_indicator END) as distinct_sharp_count,
        
        -- Find timing of strong sharp action
        MIN(CASE WHEN sharp_indicator LIKE 'STRONG_SHARP_%' THEN hours_before_game END) as earliest_strong_sharp_hours,
        
        -- Maximum credibility score achieved
        MAX(timing_credibility_score) as max_credibility_score,
        
        -- Volume metrics
        MAX(total_volume) as max_volume,
        MAX(CASE WHEN volume_reliability = 'RELIABLE_VOLUME' THEN 1 ELSE 0 END) as has_reliable_volume,
        
        -- Movement validation
        MAX(CASE WHEN line_movement_correlation IN ('SHARP_MONEY_VS_LINE_MOVE', 'SHARP_MONEY_NO_MOVEMENT') THEN 1 ELSE 0 END) as has_reverse_movement,
        MAX(ABS(line_movement)) as max_line_movement
        
    FROM enhanced_movement_analysis
    WHERE prev_line IS NOT NULL  -- Only include records where we can calculate movement
    GROUP BY game_id, source, book, split_type, home_team, away_team, home_win, home_cover_spread, over
),

multi_book_consensus AS (
    SELECT 
        ema.game_id,
        ema.split_type,
        ema.precise_timing_category,
        ema.sharp_indicator,
        
        -- Count books showing same timing pattern and sharp action
        COUNT(DISTINCT ema.book) as books_showing_pattern,
        
        -- Average credibility across books
        AVG(ema.timing_credibility_score) as avg_credibility,
        
        -- Volume-weighted consensus
        SUM(ema.total_volume * ema.timing_credibility_score) / NULLIF(SUM(ema.total_volume), 0) as volume_weighted_credibility,
        
        -- Game outcomes (should be same across all books for same game)
        MAX(ema.home_win) as home_win,
        MAX(ema.home_cover_spread) as home_cover_spread,
        MAX(ema.over) as over
        
    FROM enhanced_movement_analysis ema
    WHERE ema.sharp_indicator != 'NO_SHARP_ACTION'
      AND ema.volume_reliability != 'INSUFFICIENT_VOLUME'
      AND ema.precise_timing_category IN ('ULTRA_LATE', 'CLOSING_HOUR', 'CLOSING_2H', 'LATE_AFTERNOON')
    GROUP BY ema.game_id, ema.split_type, ema.precise_timing_category, ema.sharp_indicator
),

enhanced_timing_patterns AS (
    SELECT 
        ta.*,
        
        -- Get closing sharp action (latest data point with line movement validation)
        LAST_VALUE(ema.sharp_indicator) OVER (
            PARTITION BY ta.game_id, ta.source, ta.book, ta.split_type 
            ORDER BY ema.last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as closing_sharp_indicator,
        
        -- Get closing differential
        LAST_VALUE(ema.differential) OVER (
            PARTITION BY ta.game_id, ta.source, ta.book, ta.split_type 
            ORDER BY ema.last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as closing_differential,
        
        -- Enhanced timing pattern classification
        CASE 
            -- Premium signals: Strong validation + movement + volume
            WHEN ta.earliest_strong_sharp_hours > 12 AND ta.has_reliable_volume = 1 
                 AND ta.max_line_movement >= 1.0 THEN 'PREMIUM_EARLY_PERSISTENT'
                 
            WHEN ta.earliest_strong_sharp_hours <= 2 AND ta.has_reverse_movement = 1 
                 AND ta.max_credibility_score >= 3.0 THEN 'PREMIUM_REVERSE_STEAM'
                 
            WHEN ta.earliest_strong_sharp_hours <= 6 AND ta.earliest_strong_sharp_hours > 2 
                 AND ta.max_line_movement >= 0.5 THEN 'VALIDATED_LATE_SHARP'
                 
            -- Standard patterns with some validation
            WHEN ta.earliest_strong_sharp_hours > 12 AND ta.max_line_movement >= 0.5 THEN 'EARLY_PERSISTENT_SHARP'
            WHEN ta.earliest_strong_sharp_hours <= 6 AND ta.earliest_strong_sharp_hours > 2 THEN 'LATE_SHARP_ACTION'
            WHEN ta.earliest_strong_sharp_hours <= 2 THEN 'STEAM_MOVE'
            
            -- Questionable patterns
            WHEN ta.distinct_sharp_count > 1 THEN 'CONFLICTING_SHARP'
            WHEN ta.has_reliable_volume = 0 THEN 'LOW_VOLUME_QUESTIONABLE'
            
            ELSE 'NO_CLEAR_PATTERN'
        END as enhanced_timing_pattern,
        
        -- Consensus strength from multi-book analysis
        COALESCE(mbc.books_showing_pattern, 1) as consensus_books,
        COALESCE(mbc.volume_weighted_credibility, ta.max_credibility_score) as final_credibility_score,
        
        ROW_NUMBER() OVER (PARTITION BY ta.game_id, ta.source, ta.book, ta.split_type ORDER BY ema.last_updated DESC) as rn
        
    FROM timing_aggregated ta
    JOIN enhanced_movement_analysis ema ON ta.game_id = ema.game_id AND ta.source = ema.source 
         AND ta.book = ema.book AND ta.split_type = ema.split_type
    LEFT JOIN multi_book_consensus mbc ON ta.game_id = mbc.game_id AND ta.split_type = mbc.split_type
),

tier_based_strategy AS (
    SELECT 
        *,
        -- Create strategy tiers based on multiple validation factors
        CASE 
            WHEN final_credibility_score >= 3.5 AND max_line_movement >= 1.0 
                 AND has_reliable_volume = 1 AND consensus_books >= 2 THEN 'TIER_1_PREMIUM'
                 
            WHEN final_credibility_score >= 2.5 AND consensus_books >= 2 
                 AND has_reliable_volume = 1 AND max_line_movement >= 0.5 THEN 'TIER_2_GOOD'
                 
            WHEN final_credibility_score >= 2.0 AND max_line_movement >= 0.3 
                 AND has_reliable_volume = 1 THEN 'TIER_3_DECENT'
                 
            ELSE 'TIER_4_AVOID'
        END as timing_strategy_tier,
        
        -- Dynamic bet sizing based on tier
        CASE 
            WHEN final_credibility_score >= 3.5 AND consensus_books >= 3 THEN 3.0    -- 3x normal bet
            WHEN final_credibility_score >= 3.0 AND consensus_books >= 2 THEN 2.0    -- 2x normal bet  
            WHEN final_credibility_score >= 2.5 THEN 1.5                             -- 1.5x normal bet
            WHEN final_credibility_score >= 2.0 THEN 1.0                             -- Normal bet
            ELSE 0.0                                                                  -- No bet
        END as suggested_bet_multiplier,
        
        -- Enhanced bet recommendations with validation
        CASE split_type
            WHEN 'moneyline' THEN
                CASE 
                    WHEN enhanced_timing_pattern LIKE 'PREMIUM_%' AND closing_sharp_indicator LIKE '%HOME_OVER' THEN 'BET_HOME_PREMIUM'
                    WHEN enhanced_timing_pattern LIKE 'PREMIUM_%' AND closing_sharp_indicator LIKE '%AWAY_UNDER' THEN 'BET_AWAY_PREMIUM'
                    WHEN enhanced_timing_pattern = 'VALIDATED_LATE_SHARP' AND closing_sharp_indicator LIKE '%HOME_OVER' THEN 'BET_HOME_VALIDATED'
                    WHEN enhanced_timing_pattern = 'VALIDATED_LATE_SHARP' AND closing_sharp_indicator LIKE '%AWAY_UNDER' THEN 'BET_AWAY_VALIDATED'
                    WHEN enhanced_timing_pattern = 'EARLY_PERSISTENT_SHARP' AND closing_sharp_indicator LIKE '%HOME_OVER' THEN 'BET_HOME_EARLY'
                    WHEN enhanced_timing_pattern = 'EARLY_PERSISTENT_SHARP' AND closing_sharp_indicator LIKE '%AWAY_UNDER' THEN 'BET_AWAY_EARLY'
                    WHEN enhanced_timing_pattern = 'STEAM_MOVE' AND closing_sharp_indicator LIKE '%HOME_OVER' THEN 'BET_HOME_STEAM'
                    WHEN enhanced_timing_pattern = 'STEAM_MOVE' AND closing_sharp_indicator LIKE '%AWAY_UNDER' THEN 'BET_AWAY_STEAM'
                    ELSE 'NO_BET'
                END
            WHEN 'spread' THEN
                CASE 
                    WHEN enhanced_timing_pattern LIKE 'PREMIUM_%' AND closing_sharp_indicator LIKE '%HOME_OVER' THEN 'BET_HOME_PREMIUM'
                    WHEN enhanced_timing_pattern LIKE 'PREMIUM_%' AND closing_sharp_indicator LIKE '%AWAY_UNDER' THEN 'BET_AWAY_PREMIUM'
                    WHEN enhanced_timing_pattern = 'VALIDATED_LATE_SHARP' AND closing_sharp_indicator LIKE '%HOME_OVER' THEN 'BET_HOME_VALIDATED'
                    WHEN enhanced_timing_pattern = 'VALIDATED_LATE_SHARP' AND closing_sharp_indicator LIKE '%AWAY_UNDER' THEN 'BET_AWAY_VALIDATED'
                    WHEN enhanced_timing_pattern = 'EARLY_PERSISTENT_SHARP' AND closing_sharp_indicator LIKE '%HOME_OVER' THEN 'BET_HOME_EARLY'
                    WHEN enhanced_timing_pattern = 'EARLY_PERSISTENT_SHARP' AND closing_sharp_indicator LIKE '%AWAY_UNDER' THEN 'BET_AWAY_EARLY'
                    ELSE 'NO_BET'
                END
            WHEN 'total' THEN
                CASE 
                    WHEN enhanced_timing_pattern LIKE 'PREMIUM_%' AND closing_sharp_indicator LIKE '%HOME_OVER' THEN 'BET_OVER_PREMIUM'
                    WHEN enhanced_timing_pattern LIKE 'PREMIUM_%' AND closing_sharp_indicator LIKE '%AWAY_UNDER' THEN 'BET_UNDER_PREMIUM'
                    WHEN enhanced_timing_pattern = 'VALIDATED_LATE_SHARP' AND closing_sharp_indicator LIKE '%HOME_OVER' THEN 'BET_OVER_VALIDATED'
                    WHEN enhanced_timing_pattern = 'VALIDATED_LATE_SHARP' AND closing_sharp_indicator LIKE '%AWAY_UNDER' THEN 'BET_UNDER_VALIDATED'
                    WHEN enhanced_timing_pattern = 'EARLY_PERSISTENT_SHARP' AND closing_sharp_indicator LIKE '%HOME_OVER' THEN 'BET_OVER_EARLY'
                    WHEN enhanced_timing_pattern = 'EARLY_PERSISTENT_SHARP' AND closing_sharp_indicator LIKE '%AWAY_UNDER' THEN 'BET_UNDER_EARLY'
                    ELSE 'NO_BET'
                END
        END as enhanced_bet_recommendation
        
    FROM enhanced_timing_patterns
    WHERE rn = 1 
      AND enhanced_timing_pattern NOT IN ('NO_CLEAR_PATTERN', 'CONFLICTING_SHARP', 'LOW_VOLUME_QUESTIONABLE')
      AND closing_sharp_indicator != 'NO_SHARP_ACTION'
),

final_performance_analysis AS (
    SELECT 
        CONCAT(source, '-', COALESCE(book, 'UNKNOWN'), '-', split_type) as source_book_type,
        split_type,
        enhanced_timing_pattern,
        timing_strategy_tier,
        enhanced_bet_recommendation,
        
        COUNT(*) as total_bets,
        
        -- Calculate wins with enhanced logic
        SUM(CASE 
            WHEN enhanced_bet_recommendation LIKE 'BET_HOME%' AND home_win = true THEN 1
            WHEN enhanced_bet_recommendation LIKE 'BET_AWAY%' AND home_win = false THEN 1
            WHEN enhanced_bet_recommendation LIKE 'BET_HOME%' AND split_type = 'spread' AND home_cover_spread = true THEN 1
            WHEN enhanced_bet_recommendation LIKE 'BET_AWAY%' AND split_type = 'spread' AND home_cover_spread = false THEN 1
            WHEN enhanced_bet_recommendation LIKE 'BET_OVER%' AND over = true THEN 1
            WHEN enhanced_bet_recommendation LIKE 'BET_UNDER%' AND over = false THEN 1
            ELSE 0
        END) as wins,
        
        -- Enhanced metrics
        AVG(closing_differential) as avg_closing_differential,
        AVG(final_credibility_score) as avg_credibility_score,
        AVG(max_line_movement) as avg_line_movement,
        AVG(consensus_books) as avg_consensus_books,
        AVG(suggested_bet_multiplier) as avg_bet_multiplier
        
    FROM tier_based_strategy
    WHERE enhanced_bet_recommendation != 'NO_BET'
      AND timing_strategy_tier != 'TIER_4_AVOID'
    GROUP BY source, book, split_type, enhanced_timing_pattern, timing_strategy_tier, enhanced_bet_recommendation
    HAVING COUNT(*) >= 3  -- Minimum sample size for enhanced timing analysis
)

SELECT 
    source_book_type,
    split_type,
    enhanced_timing_pattern,
    timing_strategy_tier,
    enhanced_bet_recommendation,
    total_bets,
    wins,
    
    -- Enhanced performance metrics
    ROUND(100.0 * wins / total_bets, 1) as win_rate,
    
    -- ROI calculation for $100 unit bets (assuming -110 odds)
    ROUND(((wins * 100) - ((total_bets - wins) * 110)) / (total_bets * 110) * 100, 1) as roi_per_100_unit,
    
    -- Profit per bet with dynamic sizing
    ROUND(((wins * 100 * avg_bet_multiplier) - ((total_bets - wins) * 110 * avg_bet_multiplier)) / total_bets, 2) as profit_per_bet_enhanced,
    
    -- Enhanced validation metrics
    ROUND(avg_credibility_score, 2) as avg_credibility,
    ROUND(avg_line_movement, 2) as avg_movement,
    ROUND(avg_consensus_books, 1) as avg_books,
    ROUND(avg_closing_differential, 1) as avg_closing_diff,
    ROUND(avg_bet_multiplier, 1) as avg_multiplier,
    
    -- Enhanced strategy effectiveness
    CASE 
        WHEN timing_strategy_tier = 'TIER_1_PREMIUM' AND (100.0 * wins / total_bets) >= 65 THEN '🏆 ELITE PREMIUM'
        WHEN timing_strategy_tier = 'TIER_1_PREMIUM' AND (100.0 * wins / total_bets) >= 60 THEN '🟢 EXCELLENT PREMIUM'
        WHEN timing_strategy_tier = 'TIER_2_GOOD' AND (100.0 * wins / total_bets) >= 58 THEN '🟢 EXCELLENT VALIDATED'
        WHEN timing_strategy_tier = 'TIER_2_GOOD' AND (100.0 * wins / total_bets) >= 55 THEN '🟢 GOOD VALIDATED'
        WHEN (100.0 * wins / total_bets) >= 55 AND total_bets >= 5 THEN '🟡 PROFITABLE'
        WHEN (100.0 * wins / total_bets) >= 52.4 AND total_bets >= 5 THEN '🟡 BREAK EVEN'
        ELSE '🔴 UNPROFITABLE'
    END as enhanced_effectiveness,
    
    -- Specific enhanced timing insights
    CASE 
        WHEN enhanced_timing_pattern = 'PREMIUM_EARLY_PERSISTENT' THEN 'Elite: Early sharp + volume + movement validation'
        WHEN enhanced_timing_pattern = 'PREMIUM_REVERSE_STEAM' THEN 'Elite: Reverse line movement with high credibility'
        WHEN enhanced_timing_pattern = 'VALIDATED_LATE_SHARP' THEN 'Validated: Late sharp action with line movement'
        WHEN enhanced_timing_pattern = 'EARLY_PERSISTENT_SHARP' THEN 'Good: Early sharp money that persists'
        WHEN enhanced_timing_pattern = 'LATE_SHARP_ACTION' THEN 'Decent: Sharp money appears in final hours'
        WHEN enhanced_timing_pattern = 'STEAM_MOVE' THEN 'Caution: Last-minute action needs validation'
        ELSE 'Review timing pattern classification'
    END as enhanced_timing_insight
    
FROM final_performance_analysis
WHERE enhanced_bet_recommendation != 'NO_BET'
  AND total_bets >= 3
ORDER BY 
    CASE timing_strategy_tier
        WHEN 'TIER_1_PREMIUM' THEN 1
        WHEN 'TIER_2_GOOD' THEN 2
        WHEN 'TIER_3_DECENT' THEN 3
        ELSE 4
    END,
    (100.0 * wins / total_bets) DESC,
    total_bets DESC; 