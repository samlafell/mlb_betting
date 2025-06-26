-- ENHANCED Late Sharp Flip Strategy (PostgreSQL)
-- ==============================================
-- Enhanced strategy to detect late sharp flips across different bet types (cross-market analysis)
-- Addresses the specific case where early sharp action on CWS spread flips to late ARI moneyline action
-- Strategy: When sharp money flips direction OR markets in the final hours before game time,
-- follow the unified early signal and fade the late contradictory action

WITH game_timeline_all_markets AS (
    -- Get all betting data with timeline information across ALL split types
    SELECT 
        s.game_id,
        s.home_team,
        s.away_team,
        s.game_datetime,
        s.split_type,
        s.source,
        s.book,
        s.home_or_over_stake_percentage,
        s.home_or_over_bets_percentage,
        (s.home_or_over_stake_percentage - s.home_or_over_bets_percentage) as differential,
        s.last_updated,
        EXTRACT(EPOCH FROM (s.game_datetime - s.last_updated))/3600 as hours_before_game,
        
        -- Classify time periods (more granular)
        CASE 
            WHEN EXTRACT(EPOCH FROM (s.game_datetime - s.last_updated))/3600 >= 6 THEN 'EARLY'
            WHEN EXTRACT(EPOCH FROM (s.game_datetime - s.last_updated))/3600 >= 3 THEN 'MEDIUM' 
            WHEN EXTRACT(EPOCH FROM (s.game_datetime - s.last_updated))/3600 >= 1 THEN 'LATE'
            ELSE 'VERY_LATE'
        END as time_period,
        
        -- Determine recommended side based on split type and differential
        CASE 
            WHEN s.split_type = 'moneyline' THEN
                CASE WHEN s.home_or_over_stake_percentage > s.home_or_over_bets_percentage THEN s.home_team ELSE s.away_team END
            WHEN s.split_type = 'spread' THEN  
                CASE WHEN s.home_or_over_stake_percentage > s.home_or_over_bets_percentage THEN s.home_team ELSE s.away_team END
            WHEN s.split_type = 'total' THEN
                CASE WHEN s.home_or_over_stake_percentage > s.home_or_over_bets_percentage THEN 'OVER' ELSE 'UNDER' END
            ELSE NULL
        END as recommended_side,
        
        ROW_NUMBER() OVER (
            PARTITION BY s.game_id, s.split_type, s.source, s.book 
            ORDER BY s.last_updated ASC
        ) as sequence_number,
        COUNT(*) OVER (
            PARTITION BY s.game_id, s.split_type, s.source, s.book
        ) as total_updates
    FROM splits.raw_mlb_betting_splits s
    WHERE ABS(s.home_or_over_stake_percentage - s.home_or_over_bets_percentage) >= 8  -- Minimum sharp threshold
    AND s.game_datetime >= '2025-06-16'  -- Recent data for testing
    AND s.split_type IN ('moneyline', 'spread', 'total')  -- All market types
),

early_late_cross_market_analysis AS (
    -- Analyze early vs late signals across ALL markets for each game
    SELECT 
        gt.game_id,
        gt.home_team,
        gt.away_team,
        gt.game_datetime,
        gt.source,
        gt.book,
        
        -- EARLY signals (6+ hours before game)
        STRING_AGG(
            CASE WHEN gt.time_period = 'EARLY' AND ABS(gt.differential) >= 12 
            THEN gt.split_type || ':' || gt.recommended_side || '(' || ROUND(gt.differential::numeric, 1) || '%)' 
            ELSE NULL END, 
            ', '
        ) as early_signals,
        
        -- Count of early signals
        COUNT(CASE WHEN gt.time_period = 'EARLY' AND ABS(gt.differential) >= 12 THEN 1 END) as early_signal_count,
        
        -- Strongest early signal
        (ARRAY_AGG(
            CASE WHEN gt.time_period = 'EARLY' AND ABS(gt.differential) >= 12 
            THEN gt.split_type || ':' || gt.recommended_side 
            ELSE NULL END
            ORDER BY ABS(gt.differential) DESC
        ))[1] as strongest_early_signal,
        
        MAX(CASE WHEN gt.time_period = 'EARLY' THEN ABS(gt.differential) END) as strongest_early_differential,
        MIN(CASE WHEN gt.time_period = 'EARLY' THEN gt.last_updated END) as earliest_timestamp,
        
        -- LATE signals (1-3 hours before game)  
        STRING_AGG(
            CASE WHEN gt.time_period IN ('LATE', 'VERY_LATE') AND ABS(gt.differential) >= 12
            THEN gt.split_type || ':' || gt.recommended_side || '(' || ROUND(gt.differential::numeric, 1) || '%)' 
            ELSE NULL END,
            ', '
        ) as late_signals,
        
        -- Count of late signals
        COUNT(CASE WHEN gt.time_period IN ('LATE', 'VERY_LATE') AND ABS(gt.differential) >= 12 THEN 1 END) as late_signal_count,
        
        -- Strongest late signal
        (ARRAY_AGG(
            CASE WHEN gt.time_period IN ('LATE', 'VERY_LATE') AND ABS(gt.differential) >= 12
            THEN gt.split_type || ':' || gt.recommended_side 
            ELSE NULL END
            ORDER BY ABS(gt.differential) DESC  
        ))[1] as strongest_late_signal,
        
        MAX(CASE WHEN gt.time_period IN ('LATE', 'VERY_LATE') THEN ABS(gt.differential) END) as strongest_late_differential,
        MAX(CASE WHEN gt.time_period IN ('LATE', 'VERY_LATE') THEN gt.last_updated END) as latest_timestamp
        
    FROM game_timeline_all_markets gt
    WHERE gt.total_updates >= 2  -- Need at least some market activity
    GROUP BY gt.game_id, gt.home_team, gt.away_team, gt.game_datetime, gt.source, gt.book
    HAVING COUNT(CASE WHEN gt.time_period = 'EARLY' AND ABS(gt.differential) >= 12 THEN 1 END) >= 1
       AND COUNT(CASE WHEN gt.time_period IN ('LATE', 'VERY_LATE') AND ABS(gt.differential) >= 12 THEN 1 END) >= 1
),

flip_detection_enhanced AS (
    -- Detect various types of flips including cross-market contradictions
    SELECT 
        elc.*,
        
        -- Extract team recommendations from signals
        CASE 
            WHEN elc.strongest_early_signal LIKE '%:' || elc.home_team THEN elc.home_team
            WHEN elc.strongest_early_signal LIKE '%:' || elc.away_team THEN elc.away_team
            ELSE NULL
        END as early_recommended_team,
        
        CASE 
            WHEN elc.strongest_late_signal LIKE '%:' || elc.home_team THEN elc.home_team
            WHEN elc.strongest_late_signal LIKE '%:' || elc.away_team THEN elc.away_team  
            ELSE NULL
        END as late_recommended_team,
        
        -- Detect flip types
        CASE 
            -- Same market, opposite team (traditional flip)
            WHEN SPLIT_PART(elc.strongest_early_signal, ':', 1) = SPLIT_PART(elc.strongest_late_signal, ':', 1)
                 AND SPLIT_PART(elc.strongest_early_signal, ':', 2) != SPLIT_PART(elc.strongest_late_signal, ':', 2)
                 AND SPLIT_PART(elc.strongest_early_signal, ':', 2) IN (elc.home_team, elc.away_team)
                 AND SPLIT_PART(elc.strongest_late_signal, ':', 2) IN (elc.home_team, elc.away_team)
            THEN 'SAME_MARKET_FLIP'
            
            -- Cross-market contradiction (e.g., early spread CWS, late moneyline ARI)
            WHEN SPLIT_PART(elc.strongest_early_signal, ':', 1) != SPLIT_PART(elc.strongest_late_signal, ':', 1)
                 AND SPLIT_PART(elc.strongest_early_signal, ':', 2) != SPLIT_PART(elc.strongest_late_signal, ':', 2)
                 AND SPLIT_PART(elc.strongest_early_signal, ':', 2) IN (elc.home_team, elc.away_team)
                 AND SPLIT_PART(elc.strongest_late_signal, ':', 2) IN (elc.home_team, elc.away_team)
            THEN 'CROSS_MARKET_CONTRADICTION'
            
            -- Early strong signal, late weak contradiction
            WHEN elc.strongest_early_differential >= 15 
                 AND elc.strongest_late_differential BETWEEN 8 AND 15
                 AND SPLIT_PART(elc.strongest_early_signal, ':', 2) != SPLIT_PART(elc.strongest_late_signal, ':', 2)
                 AND SPLIT_PART(elc.strongest_early_signal, ':', 2) IN (elc.home_team, elc.away_team)
                 AND SPLIT_PART(elc.strongest_late_signal, ':', 2) IN (elc.home_team, elc.away_team)
            THEN 'WEAK_LATE_CONTRADICTION'
            
            ELSE 'NO_FLIP_DETECTED'
        END as flip_type,
        
        -- Calculate confidence in early signal
        CASE 
            WHEN elc.strongest_early_differential >= 20 THEN 'HIGH_CONFIDENCE'
            WHEN elc.strongest_early_differential >= 15 THEN 'MEDIUM_CONFIDENCE'  
            WHEN elc.strongest_early_differential >= 10 THEN 'LOW_CONFIDENCE'
            ELSE 'VERY_LOW_CONFIDENCE'
        END as early_signal_confidence,
        
        -- Time gap analysis
        EXTRACT(EPOCH FROM (elc.latest_timestamp - elc.earliest_timestamp))/3600 as hours_between_signals
        
    FROM early_late_cross_market_analysis elc
    WHERE elc.strongest_early_signal IS NOT NULL 
      AND elc.strongest_late_signal IS NOT NULL
),

game_outcomes_with_strategy AS (
    -- Join with outcomes and apply strategy
    SELECT 
        fd.*,
        go.home_score,
        go.away_score,
        
        -- Determine actual winner
        CASE 
            WHEN go.home_score > go.away_score THEN fd.home_team
            WHEN go.away_score > go.home_score THEN fd.away_team
            ELSE 'TIE'
        END as actual_winner,
        
        -- Strategy recommendation: Follow early signal, ignore late contradictions
        fd.early_recommended_team as strategy_pick,
        
        -- Result of strategy
        CASE 
            WHEN fd.early_recommended_team IS NULL THEN NULL
            WHEN go.home_score IS NULL OR go.away_score IS NULL THEN NULL
            WHEN go.home_score = go.away_score THEN 0  -- Push
            WHEN fd.early_recommended_team = fd.home_team AND go.home_score > go.away_score THEN 1  -- Win
            WHEN fd.early_recommended_team = fd.away_team AND go.away_score > go.home_score THEN 1  -- Win
            ELSE 0  -- Loss
        END as strategy_result,
        
        -- For comparison: what if we followed late signal?
        CASE 
            WHEN fd.late_recommended_team IS NULL THEN NULL
            WHEN go.home_score IS NULL OR go.away_score IS NULL THEN NULL
            WHEN go.home_score = go.away_score THEN 0  -- Push
            WHEN fd.late_recommended_team = fd.home_team AND go.home_score > go.away_score THEN 1  -- Win
            WHEN fd.late_recommended_team = fd.away_team AND go.away_score > go.home_score THEN 1  -- Win
            ELSE 0  -- Loss
        END as late_signal_result
        
    FROM flip_detection_enhanced fd
    LEFT JOIN game_outcomes go ON fd.game_id = go.game_id
    WHERE fd.flip_type IN ('SAME_MARKET_FLIP', 'CROSS_MARKET_CONTRADICTION', 'WEAK_LATE_CONTRADICTION')
    AND go.home_score IS NOT NULL   -- Only completed games
    AND go.away_score IS NOT NULL
)

-- Final enhanced strategy performance analysis
SELECT 
    'ENHANCED_LATE_SHARP_FLIP_FADE' as strategy_name,
    gows.source as source,
    COALESCE(gows.book, 'NULL') as book,
    gows.source || '-' || COALESCE(gows.book, 'NULL') as source_book_combo,
    
    -- Overall performance
    COUNT(*) as total_games,
    COUNT(*) FILTER (WHERE gows.flip_type = 'CROSS_MARKET_CONTRADICTION') as cross_market_contradictions,
    COUNT(*) FILTER (WHERE gows.flip_type = 'SAME_MARKET_FLIP') as same_market_flips,
    COUNT(*) FILTER (WHERE gows.flip_type = 'WEAK_LATE_CONTRADICTION') as weak_late_contradictions,
    
    -- Strategy performance (follow early signal)
    SUM(gows.strategy_result) as strategy_wins,
    ROUND(AVG(gows.strategy_result::decimal), 3) as strategy_win_rate,
    
    -- Comparison: late signal performance  
    SUM(gows.late_signal_result) as late_signal_wins,
    ROUND(AVG(gows.late_signal_result::decimal), 3) as late_signal_win_rate,
    
    -- Performance by flip type
    ROUND(AVG(gows.strategy_result::decimal) FILTER (WHERE gows.flip_type = 'CROSS_MARKET_CONTRADICTION'), 3) as cross_market_win_rate,
    ROUND(AVG(gows.strategy_result::decimal) FILTER (WHERE gows.flip_type = 'SAME_MARKET_FLIP'), 3) as same_market_win_rate,
    ROUND(AVG(gows.strategy_result::decimal) FILTER (WHERE gows.flip_type = 'WEAK_LATE_CONTRADICTION'), 3) as weak_contradiction_win_rate,
    
    -- Performance by early signal confidence
    ROUND(AVG(gows.strategy_result::decimal) FILTER (WHERE gows.early_signal_confidence = 'HIGH_CONFIDENCE'), 3) as high_confidence_win_rate,
    ROUND(AVG(gows.strategy_result::decimal) FILTER (WHERE gows.early_signal_confidence = 'MEDIUM_CONFIDENCE'), 3) as medium_confidence_win_rate,
    
    -- ROI calculations (assuming -110 odds)
    ROUND((SUM(gows.strategy_result) * 0.909 - (COUNT(*) - SUM(gows.strategy_result))) * 100.0 / COUNT(*), 2) as strategy_roi_percent,
    
    -- Enhanced metrics
    ROUND(AVG(gows.strongest_early_differential)::numeric, 1) as avg_early_signal_strength,
    ROUND(AVG(gows.strongest_late_differential)::numeric, 1) as avg_late_signal_strength,
    ROUND(AVG(gows.hours_between_signals)::numeric, 1) as avg_hours_between_signals,
    
    -- Recent examples for validation (last 30 days)
    STRING_AGG(
        CASE WHEN gows.game_datetime >= CURRENT_DATE - INTERVAL '30 days' 
        THEN gows.away_team || '@' || gows.home_team || 
             ' (Early:' || COALESCE(SPLIT_PART(gows.strongest_early_signal, ':', 1), 'N/A') || 
             ' Late:' || COALESCE(SPLIT_PART(gows.strongest_late_signal, ':', 1), 'N/A') || 
             ' ' || CASE WHEN gows.strategy_result = 1 THEN 'W' ELSE 'L' END || ')'
        ELSE NULL END, 
        ', ' 
        ORDER BY gows.game_datetime DESC
    ) as recent_examples
    
FROM game_outcomes_with_strategy gows  
GROUP BY gows.source, gows.book
HAVING COUNT(*) >= 3  -- Minimum sample size for enhanced analysis
ORDER BY strategy_win_rate DESC, total_games DESC; 