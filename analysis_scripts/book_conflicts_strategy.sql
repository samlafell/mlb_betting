-- Enhanced Book Conflicts Strategy Analysis
-- Implements betting expert recommendations for more sophisticated conflict detection
-- Key Enhancements:
-- 1. Book reliability weighting (Pinnacle > Circa > Public books)
-- 2. Volume-adjusted conflict detection
-- 3. Timing-based conflict analysis (closing vs early conflicts)
-- 4. Line movement validation
-- 5. Enhanced recommended action logic
-- 6. Conflict strength scoring system
-- 7. Improved data quality controls
--
-- Tests scenarios where different books show contradictory signals
-- Example: Pinnacle has 30% money + 80% bets vs DraftKings has 80% money + 30% bets
-- This identifies situations where books disagree on sharp vs public action

WITH enhanced_latest_splits AS (
    SELECT 
        home_team, away_team, game_datetime, split_type, split_value,
        home_or_over_stake_percentage as money_pct,
        home_or_over_bets_percentage as bet_pct,
        COALESCE(home_or_over_bets + away_or_under_bets, 0) as total_volume,
        source, COALESCE(book, 'UNKNOWN') as book, last_updated,
        
        -- Book credibility scoring (sharp vs public customer base)
        CASE book
            WHEN 'Pinnacle' THEN 3.0      -- Sharpest book, lowest margins
            WHEN 'BookMaker' THEN 2.5     -- Sharp book
            WHEN 'Circa' THEN 2.3         -- Sharp Vegas book
            WHEN 'BetMGM' THEN 1.8        -- Decent book
            WHEN 'Caesars' THEN 1.7       -- Decent book
            WHEN 'PointsBet' THEN 1.6     -- Mid-tier book
            WHEN 'DraftKings' THEN 1.5    -- Public-heavy book
            WHEN 'FanDuel' THEN 1.5       -- Public-heavy book
            WHEN 'BetRivers' THEN 1.2     -- Smaller book
            WHEN 'Barstool' THEN 1.0      -- Very public book
            ELSE 1.0
        END as book_credibility,
        
        -- Volume tier classification (relaxed thresholds)
        CASE 
            WHEN COALESCE(home_or_over_bets + away_or_under_bets, 0) >= 1000 THEN 'HIGH_VOLUME'
            WHEN COALESCE(home_or_over_bets + away_or_under_bets, 0) >= 300 THEN 'MEDIUM_VOLUME'
            WHEN COALESCE(home_or_over_bets + away_or_under_bets, 0) >= 50 THEN 'LOW_VOLUME'
            ELSE 'MINIMAL_VOLUME'
        END as volume_tier,
        
        -- Timing context
        COALESCE(EXTRACT('epoch' FROM (game_datetime - last_updated)) / 3600, 24) as hours_before_game,
        CASE 
            WHEN COALESCE(EXTRACT('epoch' FROM (game_datetime - last_updated)) / 3600, 24) <= 2 THEN 'CLOSING_CONFLICT'
            WHEN COALESCE(EXTRACT('epoch' FROM (game_datetime - last_updated)) / 3600, 24) <= 6 THEN 'LATE_CONFLICT'
            WHEN COALESCE(EXTRACT('epoch' FROM (game_datetime - last_updated)) / 3600, 24) <= 24 THEN 'EARLY_CONFLICT'
            ELSE 'VERY_EARLY_CONFLICT'
        END as conflict_timing,
        
        -- Line movement tracking
        LAG(TRY_CAST(split_value AS DOUBLE)) OVER (
            PARTITION BY home_team, away_team, game_datetime, split_type, source, COALESCE(book, 'UNKNOWN')
            ORDER BY last_updated
        ) as prev_line,
        
        ROW_NUMBER() OVER (
            PARTITION BY home_team, away_team, game_datetime, split_type, source, COALESCE(book, 'UNKNOWN')
            ORDER BY last_updated DESC
        ) as rn
    FROM mlb_betting.splits.raw_mlb_betting_splits
    WHERE home_or_over_stake_percentage IS NOT NULL 
      AND home_or_over_bets_percentage IS NOT NULL
      AND game_datetime IS NOT NULL
),

-- Enhanced book signals with weighting and movement analysis
enhanced_book_signals AS (
    SELECT 
        home_team, away_team, game_datetime, split_type,
        source, book, money_pct, bet_pct, total_volume, book_credibility, volume_tier, conflict_timing,
        
        -- Calculate line movement
        CASE 
            WHEN prev_line IS NOT NULL AND TRY_CAST(split_value AS DOUBLE) IS NOT NULL THEN
                TRY_CAST(split_value AS DOUBLE) - prev_line
            ELSE 0.0
        END as line_movement,
        
        -- Raw sharp differential
        COALESCE(money_pct - bet_pct, 0) as raw_sharp_differential,
        
        -- Book credibility-weighted sharp signal
        COALESCE((money_pct - bet_pct) * book_credibility, 0) as weighted_sharp_signal,
        
        -- Enhanced sharp signal classification (relaxed thresholds)
        CASE 
            WHEN COALESCE((money_pct - bet_pct) * book_credibility, 0) >= 20 THEN 'PREMIUM_SHARP_HOME'
            WHEN COALESCE((money_pct - bet_pct) * book_credibility, 0) >= 15 THEN 'STRONG_SHARP_HOME'
            WHEN COALESCE((money_pct - bet_pct) * book_credibility, 0) >= 10 THEN 'MODERATE_SHARP_HOME'
            WHEN COALESCE((money_pct - bet_pct) * book_credibility, 0) >= 7 THEN 'WEAK_SHARP_HOME'
            WHEN COALESCE((money_pct - bet_pct) * book_credibility, 0) <= -20 THEN 'PREMIUM_SHARP_AWAY'
            WHEN COALESCE((money_pct - bet_pct) * book_credibility, 0) <= -15 THEN 'STRONG_SHARP_AWAY'
            WHEN COALESCE((money_pct - bet_pct) * book_credibility, 0) <= -10 THEN 'MODERATE_SHARP_AWAY'
            WHEN COALESCE((money_pct - bet_pct) * book_credibility, 0) <= -7 THEN 'WEAK_SHARP_AWAY'
            ELSE 'NO_SHARP_SIGNAL'
        END as enhanced_sharp_signal,
        
        -- Line movement validation (relaxed thresholds)
        CASE 
            WHEN COALESCE(money_pct - bet_pct, 0) > 10 AND COALESCE(line_movement, 0) > 0 THEN 'VALIDATED_SHARP_HOME'
            WHEN COALESCE(money_pct - bet_pct, 0) < -10 AND COALESCE(line_movement, 0) < 0 THEN 'VALIDATED_SHARP_AWAY'
            WHEN ABS(COALESCE(money_pct - bet_pct, 0)) > 10 AND ABS(COALESCE(line_movement, 0)) < 0.5 THEN 'UNVALIDATED_SIGNAL'
            WHEN ABS(COALESCE(money_pct - bet_pct, 0)) > 10 THEN 'OPPOSITE_MOVEMENT'
            ELSE 'NORMAL_MOVEMENT'
        END as movement_validation,
        
        -- Signal source quality (relaxed thresholds)
        CASE 
            WHEN book = 'Pinnacle' AND ABS(COALESCE(money_pct - bet_pct, 0)) >= 10 THEN 'PINNACLE_SHARP_SIGNAL'
            WHEN book IN ('DraftKings', 'FanDuel') AND ABS(COALESCE(money_pct - bet_pct, 0)) >= 15 THEN 'PUBLIC_BOOK_SIGNAL'
            WHEN book_credibility >= 2.0 AND ABS(COALESCE(money_pct - bet_pct, 0)) >= 8 THEN 'SHARP_BOOK_SIGNAL'
            ELSE 'NORMAL_SIGNAL'
        END as signal_source_quality,
        
        -- Overall directional bias
        CASE 
            WHEN COALESCE(money_pct, 50) >= 60 THEN 'MONEY_FAVORS_HOME'
            WHEN COALESCE(money_pct, 50) <= 40 THEN 'MONEY_FAVORS_AWAY'
            ELSE 'MONEY_BALANCED'
        END as money_direction,
        
        CASE 
            WHEN COALESCE(bet_pct, 50) >= 60 THEN 'BETS_FAVOR_HOME'
            WHEN COALESCE(bet_pct, 50) <= 40 THEN 'BETS_FAVOR_AWAY'
            ELSE 'BETS_BALANCED'
        END as bet_direction
    FROM enhanced_latest_splits 
    WHERE rn = 1
),

-- Enhanced conflict detection with volume and credibility weighting (relaxed thresholds)
enhanced_book_conflicts AS (
    SELECT 
        home_team, away_team, game_datetime, split_type,
        COUNT(DISTINCT source || '-' || book) as num_books,
        
        -- Book coverage quality
        CASE 
            WHEN COUNT(DISTINCT source || '-' || book) < 2 THEN 'INSUFFICIENT_COVERAGE'
            WHEN COUNT(DISTINCT source || '-' || book) < 4 THEN 'ADEQUATE_COVERAGE'
            ELSE 'GOOD_COVERAGE'
        END as book_coverage_quality,
        
        -- Collect all signals and directions
        STRING_AGG(DISTINCT enhanced_sharp_signal, '|') as all_sharp_signals,
        STRING_AGG(DISTINCT money_direction, '|') as all_money_directions,
        STRING_AGG(DISTINCT bet_direction, '|') as all_bet_directions,
        STRING_AGG(DISTINCT signal_source_quality, '|') as all_signal_qualities,
        STRING_AGG(DISTINCT movement_validation, '|') as all_movement_validations,
        
        -- Enhanced statistical measures weighted by credibility
        COALESCE(STDDEV(weighted_sharp_signal), 0) as weighted_sharp_variance,
        COALESCE(STDDEV(raw_sharp_differential), 0) as raw_sharp_variance,
        COALESCE(STDDEV(money_pct), 0) as money_pct_variance,
        COALESCE(STDDEV(bet_pct), 0) as bet_pct_variance,
        
        -- Volume and credibility metrics
        COALESCE(AVG(total_volume), 0) as avg_volume,
        COALESCE(MIN(total_volume), 0) as min_volume,
        COALESCE(AVG(book_credibility), 1.0) as avg_credibility,
        COALESCE(MAX(book_credibility), 1.0) as max_credibility,
        COALESCE(MIN(book_credibility), 1.0) as min_credibility,
        
        -- Timing analysis (using most common timing instead of MODE)
        (SELECT conflict_timing 
         FROM enhanced_book_signals ebs2 
         WHERE ebs2.home_team = ebs.home_team 
           AND ebs2.away_team = ebs.away_team 
           AND ebs2.game_datetime = ebs.game_datetime 
           AND ebs2.split_type = ebs.split_type
         GROUP BY conflict_timing 
         ORDER BY COUNT(*) DESC 
         LIMIT 1) as primary_conflict_timing,
        COUNT(DISTINCT conflict_timing) as timing_diversity,
        
        -- Average values
        COALESCE(AVG(money_pct), 50) as avg_money_pct,
        COALESCE(AVG(bet_pct), 50) as avg_bet_pct,
        COALESCE(AVG(raw_sharp_differential), 0) as avg_raw_sharp_diff,
        COALESCE(AVG(weighted_sharp_signal), 0) as avg_weighted_sharp_signal,
        
        -- Enhanced conflict detection
        COUNT(DISTINCT enhanced_sharp_signal) as unique_enhanced_signals,
        COUNT(DISTINCT money_direction) as unique_money_directions,
        COUNT(DISTINCT bet_direction) as unique_bet_directions,
        COUNT(DISTINCT signal_source_quality) as unique_signal_qualities,
        
        -- Movement validation metrics
        COUNT(CASE WHEN movement_validation LIKE 'VALIDATED%' THEN 1 END) as validated_signals,
        COUNT(CASE WHEN movement_validation = 'UNVALIDATED_SIGNAL' THEN 1 END) as unvalidated_signals,
        
        -- Volume-adjusted conflict classification (relaxed thresholds)
        CASE 
            WHEN COUNT(DISTINCT enhanced_sharp_signal) >= 3 AND COALESCE(STDDEV(weighted_sharp_signal), 0) >= 15 AND COALESCE(MIN(total_volume), 0) >= 200 THEN 'PREMIUM_HIGH_CONFLICT'
            WHEN COUNT(DISTINCT enhanced_sharp_signal) >= 2 AND COALESCE(STDDEV(weighted_sharp_signal), 0) >= 10 AND COALESCE(MIN(total_volume), 0) >= 100 THEN 'HIGH_SHARP_CONFLICT'
            WHEN COUNT(DISTINCT enhanced_sharp_signal) >= 2 AND COALESCE(STDDEV(weighted_sharp_signal), 0) >= 7 AND COALESCE(MIN(total_volume), 0) >= 50 THEN 'MODERATE_SHARP_CONFLICT'
            WHEN COUNT(DISTINCT money_direction) >= 2 AND COUNT(DISTINCT bet_direction) >= 2 AND COALESCE(AVG(total_volume), 0) >= 100 THEN 'DIRECTIONAL_CONFLICT'
            WHEN COALESCE(STDDEV(money_pct), 0) >= 20 OR COALESCE(STDDEV(bet_pct), 0) >= 20 THEN 'HIGH_VARIANCE_CONFLICT'
            WHEN COUNT(DISTINCT enhanced_sharp_signal) >= 2 OR COALESCE(STDDEV(raw_sharp_differential), 0) >= 8 THEN 'BASIC_CONFLICT'
            ELSE 'LOW_QUALITY_CONFLICT'
        END as enhanced_conflict_type,
        
        -- Conflict strength scoring (0-100 scale) with more generous scoring
        LEAST(100, GREATEST(0,
            (COALESCE(STDDEV(weighted_sharp_signal), 0) / 20.0 * 25) +                    -- Sharp disagreement (25 points max, lower threshold)
            (COUNT(DISTINCT enhanced_sharp_signal) / 4.0 * 20) +                          -- Signal diversity (20 points max, lower threshold)  
            (COALESCE(AVG(book_credibility), 1.0) / 2.0 * 15) +                          -- Book quality (15 points max, lower threshold)
            (CASE WHEN COALESCE(MIN(total_volume), 0) >= 300 THEN 20 
                  WHEN COALESCE(MIN(total_volume), 0) >= 100 THEN 15
                  WHEN COALESCE(MIN(total_volume), 0) >= 50 THEN 10
                  ELSE COALESCE(MIN(total_volume), 0)/50*10 END) +                        -- Volume (20 points max, more generous)
            (CASE COALESCE((SELECT conflict_timing 
                           FROM enhanced_book_signals ebs2 
                           WHERE ebs2.home_team = ebs.home_team 
                             AND ebs2.away_team = ebs.away_team 
                             AND ebs2.game_datetime = ebs.game_datetime 
                             AND ebs2.split_type = ebs.split_type
                           GROUP BY conflict_timing 
                           ORDER BY COUNT(*) DESC 
                           LIMIT 1), 'EARLY_CONFLICT')
                WHEN 'CLOSING_CONFLICT' THEN 20
                WHEN 'LATE_CONFLICT' THEN 15 
                WHEN 'EARLY_CONFLICT' THEN 10 
                ELSE 5 END)                                                               -- Timing (20 points max, more generous)
        )) as conflict_strength_score
        
    FROM enhanced_book_signals ebs
    GROUP BY home_team, away_team, game_datetime, split_type
    HAVING COUNT(DISTINCT source || '-' || book) >= 2  -- Need multiple books
      AND (COUNT(DISTINCT enhanced_sharp_signal) >= 2 OR COALESCE(STDDEV(weighted_sharp_signal), 0) >= 5)  -- Relaxed conflict requirements
),

-- Enhanced recommended actions with more lenient logic
conflicts_with_enhanced_actions AS (
    SELECT 
        *,
        -- Opportunity grading (more generous thresholds)
        CASE 
            WHEN conflict_strength_score >= 70 THEN 'PREMIUM_OPPORTUNITY'
            WHEN conflict_strength_score >= 55 THEN 'EXCELLENT_OPPORTUNITY'
            WHEN conflict_strength_score >= 40 THEN 'GOOD_OPPORTUNITY'  
            WHEN conflict_strength_score >= 25 THEN 'MODERATE_OPPORTUNITY'
            ELSE 'WEAK_OPPORTUNITY'
        END as opportunity_grade,
        
        -- Enhanced recommended action logic with more accessible thresholds
        CASE 
            -- Premium signals: Sharp book vs public books with volume and validation
            WHEN max_credibility >= 2.0 AND min_credibility <= 1.5 AND weighted_sharp_variance >= 10 AND min_volume >= 100 THEN
                CASE WHEN avg_weighted_sharp_signal > 0 THEN 'FOLLOW_SHARP_BOOK_HOME' ELSE 'FOLLOW_SHARP_BOOK_AWAY' END
            
            -- Late timing premium (closing line moves often indicate inside info)
            WHEN COALESCE(primary_conflict_timing, 'EARLY_CONFLICT') = 'CLOSING_CONFLICT' AND weighted_sharp_variance >= 8 AND validated_signals >= 1 THEN
                CASE WHEN avg_weighted_sharp_signal > 0 THEN 'FOLLOW_LATE_MONEY_HOME' ELSE 'FOLLOW_LATE_MONEY_AWAY' END
            
            -- Movement validated signals (when sharp money actually moved lines)
            WHEN validated_signals >= 1 AND weighted_sharp_variance >= 7 THEN
                CASE WHEN avg_weighted_sharp_signal > 0 THEN 'FOLLOW_VALIDATED_HOME' ELSE 'FOLLOW_VALIDATED_AWAY' END
            
            -- High volume directional conflicts (fade heavy public when volume is real)
            WHEN unique_money_directions >= 2 AND min_volume >= 200 AND ABS(avg_money_pct - 50) >= 12 THEN
                CASE WHEN avg_money_pct > 62 THEN 'FADE_HIGH_VOLUME_PUBLIC_HOME' ELSE 'FADE_HIGH_VOLUME_PUBLIC_AWAY' END
            
            -- Pinnacle vs public book conflicts (Pinnacle customers are sharper)
            WHEN COALESCE(all_signal_qualities, '') LIKE '%PINNACLE_SHARP_SIGNAL%' AND COALESCE(all_signal_qualities, '') LIKE '%PUBLIC_BOOK_SIGNAL%' THEN
                CASE WHEN avg_weighted_sharp_signal > 0 THEN 'FOLLOW_PINNACLE_HOME' ELSE 'FOLLOW_PINNACLE_AWAY' END
            
            -- Sharp book consensus vs public books
            WHEN avg_credibility >= 1.8 AND weighted_sharp_variance >= 8 THEN
                CASE WHEN avg_weighted_sharp_signal > 3 THEN 'FOLLOW_SHARP_CONSENSUS_HOME' ELSE 'FOLLOW_SHARP_CONSENSUS_AWAY' END
            
            -- Early line inefficiencies (when books open with different views)
            WHEN COALESCE(primary_conflict_timing, 'EARLY_CONFLICT') = 'EARLY_CONFLICT' AND raw_sharp_variance >= 12 AND min_volume >= 50 THEN
                CASE WHEN avg_raw_sharp_diff > 0 THEN 'EXPLOIT_OPENING_LINE_HOME' ELSE 'EXPLOIT_OPENING_LINE_AWAY' END
            
            -- Basic conflict resolution (lower threshold fallback)
            WHEN enhanced_conflict_type IN ('HIGH_SHARP_CONFLICT', 'MODERATE_SHARP_CONFLICT', 'BASIC_CONFLICT') THEN
                CASE WHEN avg_weighted_sharp_signal > 0 THEN 'FOLLOW_CONSENSUS_HOME' ELSE 'FOLLOW_CONSENSUS_AWAY' END
                
            -- Directional conflicts
            WHEN enhanced_conflict_type = 'DIRECTIONAL_CONFLICT' THEN
                CASE WHEN avg_money_pct > 55 THEN 'FADE_PUBLIC_HOME' ELSE 'FADE_PUBLIC_AWAY' END
                
            ELSE 'NO_CLEAR_ACTION'
        END as enhanced_recommended_action
        
    FROM enhanced_book_conflicts
),

-- Add game outcomes for backtesting with relaxed filtering
conflicts_with_outcomes AS (
    SELECT 
        cwea.*,
        go.home_score, go.away_score, go.home_win, go.home_cover_spread, go.over,
        
        -- Determine if enhanced recommended action was successful
        CASE 
            WHEN cwea.split_type = 'moneyline' AND cwea.enhanced_recommended_action LIKE '%_HOME' THEN 
                CASE WHEN go.home_win = true THEN 1 ELSE 0 END
            WHEN cwea.split_type = 'moneyline' AND cwea.enhanced_recommended_action LIKE '%_AWAY' THEN 
                CASE WHEN go.home_win = false THEN 1 ELSE 0 END
            WHEN cwea.split_type = 'spread' AND cwea.enhanced_recommended_action LIKE '%_HOME' THEN 
                CASE WHEN go.home_cover_spread = true THEN 1 ELSE 0 END
            WHEN cwea.split_type = 'spread' AND cwea.enhanced_recommended_action LIKE '%_AWAY' THEN 
                CASE WHEN go.home_cover_spread = false THEN 1 ELSE 0 END
            WHEN cwea.split_type = 'total' AND cwea.enhanced_recommended_action LIKE '%_OVER' THEN 
                CASE WHEN go.over = true THEN 1 ELSE 0 END
            WHEN cwea.split_type = 'total' AND cwea.enhanced_recommended_action LIKE '%_UNDER' THEN 
                CASE WHEN go.over = false THEN 1 ELSE 0 END
            ELSE NULL
        END as enhanced_action_successful,
        
        -- Test contrarian approach
        CASE 
            WHEN cwea.split_type = 'moneyline' AND cwea.enhanced_recommended_action LIKE '%_HOME' THEN 
                CASE WHEN go.home_win = false THEN 1 ELSE 0 END
            WHEN cwea.split_type = 'moneyline' AND cwea.enhanced_recommended_action LIKE '%_AWAY' THEN 
                CASE WHEN go.home_win = true THEN 1 ELSE 0 END
            WHEN cwea.split_type = 'spread' AND cwea.enhanced_recommended_action LIKE '%_HOME' THEN 
                CASE WHEN go.home_cover_spread = false THEN 1 ELSE 0 END
            WHEN cwea.split_type = 'spread' AND cwea.enhanced_recommended_action LIKE '%_AWAY' THEN 
                CASE WHEN go.home_cover_spread = true THEN 1 ELSE 0 END
            WHEN cwea.split_type = 'total' AND cwea.enhanced_recommended_action LIKE '%_OVER' THEN 
                CASE WHEN go.over = false THEN 1 ELSE 0 END
            WHEN cwea.split_type = 'total' AND cwea.enhanced_recommended_action LIKE '%_UNDER' THEN 
                CASE WHEN go.over = true THEN 1 ELSE 0 END
            ELSE NULL
        END as contrarian_successful
        
    FROM conflicts_with_enhanced_actions cwea
    LEFT JOIN mlb_betting.main.game_outcomes go ON cwea.home_team = go.home_team 
        AND cwea.away_team = go.away_team 
        AND DATE(cwea.game_datetime) = DATE(go.game_date)
    WHERE go.game_date IS NOT NULL  -- Only include games with outcomes
      AND go.home_score IS NOT NULL
      AND go.away_score IS NOT NULL
      AND DATE(go.game_date) >= CURRENT_DATE - INTERVAL '60 days'  -- Extended timeframe for better samples
      AND cwea.enhanced_recommended_action != 'NO_CLEAR_ACTION'
      -- REMOVED: overly restrictive opportunity grade filter
)

-- Enhanced final analysis with detailed performance metrics
SELECT 
    'ENHANCED_BOOK_CONFLICTS' as strategy_name,
    split_type,
    enhanced_conflict_type as strategy_variant,
    enhanced_recommended_action as action_type,
    COALESCE(opportunity_grade, 'UNKNOWN') as opportunity_grade,
    COALESCE(primary_conflict_timing, 'UNKNOWN') as timing,
    'enhanced-book-conflicts' as source_book_type,
    
    COUNT(*) as total_bets,
    COALESCE(SUM(enhanced_action_successful), 0) as wins,
    ROUND(COALESCE(AVG(enhanced_action_successful), 0) * 100, 1) as win_rate,
    
    -- Enhanced ROI calculation based on opportunity grade with NULL protection
    ROUND(CASE 
        WHEN COALESCE(opportunity_grade, 'GOOD') = 'PREMIUM_OPPORTUNITY' AND COUNT(*) > 0 THEN
            -- Premium opportunities likely get better lines
            ((COALESCE(SUM(enhanced_action_successful), 0) * 105.0) - ((COUNT(*) - COALESCE(SUM(enhanced_action_successful), 0)) * 105.0)) / (COUNT(*) * 105.0) * 100
        WHEN COALESCE(opportunity_grade, 'GOOD') = 'EXCELLENT_OPPORTUNITY' AND COUNT(*) > 0 THEN
            -- Excellent opportunities get standard lines
            ((COALESCE(SUM(enhanced_action_successful), 0) * 100.0) - ((COUNT(*) - COALESCE(SUM(enhanced_action_successful), 0)) * 110.0)) / (COUNT(*) * 110.0) * 100
        WHEN COUNT(*) > 0 THEN
            -- Good opportunities may get slightly worse lines
            ((COALESCE(SUM(enhanced_action_successful), 0) * 95.0) - ((COUNT(*) - COALESCE(SUM(enhanced_action_successful), 0)) * 110.0)) / (COUNT(*) * 110.0) * 100
        ELSE 0
    END, 1) as enhanced_roi,
    
    -- Compare to contrarian approach
    COALESCE(SUM(contrarian_successful), 0) as contrarian_wins,
    ROUND(COALESCE(AVG(contrarian_successful), 0) * 100, 1) as contrarian_win_rate_pct,
    ROUND(CASE 
        WHEN COUNT(*) > 0 THEN ((COALESCE(SUM(contrarian_successful), 0) * 100.0) - ((COUNT(*) - COALESCE(SUM(contrarian_successful), 0)) * 110.0)) / (COUNT(*) * 110.0) * 100
        ELSE 0
    END, 1) as contrarian_roi,
    
    -- Enhanced conflict quality metrics with NULL protection
    ROUND(COALESCE(AVG(num_books), 0), 1) as avg_books_per_conflict,
    ROUND(COALESCE(AVG(unique_enhanced_signals), 0), 1) as avg_unique_signals,
    ROUND(COALESCE(AVG(weighted_sharp_variance), 0), 1) as avg_weighted_variance,
    ROUND(COALESCE(AVG(conflict_strength_score), 0), 1) as avg_strength_score,
    ROUND(COALESCE(AVG(avg_credibility), 1.0), 2) as avg_book_credibility,
    ROUND(COALESCE(AVG(min_volume), 0), 0) as avg_min_volume,
    ROUND(COALESCE(AVG(validated_signals), 0), 1) as avg_validated_signals,
    
    -- Performance edge calculation
    ROUND(COALESCE(AVG(enhanced_action_successful), 0) - COALESCE(AVG(contrarian_successful), 0), 3) as strategy_edge,
    
    -- Coverage and quality metrics
    COUNT(DISTINCT home_team || '-' || away_team || '-' || DATE(game_datetime)) as unique_games,
    COALESCE((SELECT book_coverage_quality 
             FROM conflicts_with_outcomes cwo2 
             WHERE cwo2.enhanced_conflict_type = cwo.enhanced_conflict_type 
               AND cwo2.enhanced_recommended_action = cwo.enhanced_recommended_action
               AND cwo2.opportunity_grade = cwo.opportunity_grade
             GROUP BY book_coverage_quality 
             ORDER BY COUNT(*) DESC 
             LIMIT 1), 'MIXED') as typical_coverage,
    
    -- Confidence level based on multiple factors
    CASE 
        WHEN COUNT(*) >= 20 AND COALESCE(AVG(conflict_strength_score), 0) >= 60 AND COALESCE(AVG(validated_signals), 0) >= 1.0 THEN 'VERY_HIGH'
        WHEN COUNT(*) >= 10 AND COALESCE(AVG(conflict_strength_score), 0) >= 45 AND COALESCE(AVG(validated_signals), 0) >= 0.5 THEN 'HIGH'
        WHEN COUNT(*) >= 5 AND COALESCE(AVG(conflict_strength_score), 0) >= 30 THEN 'MEDIUM'
        WHEN COUNT(*) >= 3 THEN 'LOW'
        ELSE 'VERY_LOW'
    END as confidence_level

FROM conflicts_with_outcomes cwo
WHERE enhanced_action_successful IS NOT NULL
GROUP BY split_type, enhanced_conflict_type, enhanced_recommended_action, opportunity_grade, primary_conflict_timing
HAVING COUNT(*) >= 1  -- Very low minimum for initial testing

UNION ALL

-- Enhanced summary across all conflict types and grades
SELECT 
    'ENHANCED_BOOK_CONFLICTS_SUMMARY' as strategy_name,
    'ALL' as split_type,
    'ALL_ENHANCED_CONFLICTS' as strategy_variant,
    'ALL_ENHANCED_ACTIONS' as action_type,
    'ALL_GRADES' as opportunity_grade,
    'ALL_TIMING' as timing,
    'enhanced-book-conflicts-summary' as source_book_type,
    
    COUNT(*) as total_bets,
    COALESCE(SUM(enhanced_action_successful), 0) as wins,
    ROUND(COALESCE(AVG(enhanced_action_successful), 0) * 100, 1) as win_rate,
    ROUND(CASE 
        WHEN COUNT(*) > 0 THEN ((COALESCE(SUM(enhanced_action_successful), 0) * 100.0) - ((COUNT(*) - COALESCE(SUM(enhanced_action_successful), 0)) * 110.0)) / (COUNT(*) * 110.0) * 100
        ELSE 0
    END, 1) as enhanced_roi,
    
    COALESCE(SUM(contrarian_successful), 0) as contrarian_wins,
    ROUND(COALESCE(AVG(contrarian_successful), 0) * 100, 1) as contrarian_win_rate_pct,
    ROUND(CASE 
        WHEN COUNT(*) > 0 THEN ((COALESCE(SUM(contrarian_successful), 0) * 100.0) - ((COUNT(*) - COALESCE(SUM(contrarian_successful), 0)) * 110.0)) / (COUNT(*) * 110.0) * 100
        ELSE 0
    END, 1) as contrarian_roi,
    
    ROUND(COALESCE(AVG(num_books), 0), 1) as avg_books_per_conflict,
    ROUND(COALESCE(AVG(unique_enhanced_signals), 0), 1) as avg_unique_signals,
    ROUND(COALESCE(AVG(weighted_sharp_variance), 0), 1) as avg_weighted_variance,
    ROUND(COALESCE(AVG(conflict_strength_score), 0), 1) as avg_strength_score,
    ROUND(COALESCE(AVG(avg_credibility), 1.0), 2) as avg_book_credibility,
    ROUND(COALESCE(AVG(min_volume), 0), 0) as avg_min_volume,
    ROUND(COALESCE(AVG(validated_signals), 0), 1) as avg_validated_signals,
    
    ROUND(COALESCE(AVG(enhanced_action_successful), 0) - COALESCE(AVG(contrarian_successful), 0), 3) as strategy_edge,
    
    COUNT(DISTINCT home_team || '-' || away_team || '-' || DATE(game_datetime)) as unique_games,
    'MIXED' as typical_coverage,
    
    CASE 
        WHEN COUNT(*) >= 30 AND COALESCE(AVG(conflict_strength_score), 0) >= 50 THEN 'VERY_HIGH'
        WHEN COUNT(*) >= 15 AND COALESCE(AVG(conflict_strength_score), 0) >= 35 THEN 'HIGH'
        WHEN COUNT(*) >= 5 THEN 'MEDIUM'
        ELSE 'LOW'
    END as confidence_level

FROM conflicts_with_outcomes
WHERE enhanced_action_successful IS NOT NULL

ORDER BY enhanced_roi DESC, total_bets DESC; 