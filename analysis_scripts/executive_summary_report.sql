-- Executive Summary Report
-- High-level analysis with actionable insights and strategy recommendations

WITH data_quality_check AS (
    SELECT 
        COUNT(DISTINCT rmbs.game_id) as total_games,
        COUNT(DISTINCT CONCAT(rmbs.source, '-', rmbs.book)) as total_source_books,
        COUNT(*) as total_data_points,
        MIN(rmbs.game_datetime) as earliest_game,
        MAX(rmbs.game_datetime) as latest_game,
        
        -- Data completeness
        COUNT(CASE WHEN rmbs.home_or_over_stake_percentage IS NOT NULL THEN 1 END) as complete_stake_data,
        COUNT(CASE WHEN rmbs.split_value IS NOT NULL THEN 1 END) as complete_line_data,
        
        -- Market coverage
        COUNT(CASE WHEN rmbs.split_type = 'moneyline' THEN 1 END) as moneyline_bets,
        COUNT(CASE WHEN rmbs.split_type = 'spread' THEN 1 END) as spread_bets,
        COUNT(CASE WHEN rmbs.split_type = 'total' THEN 1 END) as total_bets
        
    FROM mlb_betting.splits.raw_mlb_betting_splits rmbs
    JOIN mlb_betting.main.game_outcomes go ON rmbs.game_id = go.game_id
    WHERE rmbs.last_updated < rmbs.game_datetime
),

overall_market_performance AS (
    SELECT 
        split_type,
        COUNT(DISTINCT game_id) as games_analyzed,
        
        -- Basic market efficiency metrics
        AVG(CASE WHEN home_win = true THEN 1.0 ELSE 0.0 END) as home_win_rate,
        AVG(CASE WHEN home_cover_spread = true THEN 1.0 ELSE 0.0 END) as home_cover_rate,
        AVG(CASE WHEN over = true THEN 1.0 ELSE 0.0 END) as over_rate,
        
        -- Sharp action frequency
        COUNT(CASE WHEN ABS(stake_pct - bet_pct) >= 15 THEN 1 END) as strong_sharp_instances,
        COUNT(CASE WHEN ABS(stake_pct - bet_pct) >= 10 THEN 1 END) as moderate_sharp_instances,
        COUNT(CASE WHEN ABS(stake_pct - bet_pct) >= 5 THEN 1 END) as weak_sharp_instances,
        
        -- Average differentials
        AVG(ABS(stake_pct - bet_pct)) as avg_differential_magnitude,
        AVG(stake_pct) as avg_stake_percentage,
        AVG(bet_pct) as avg_bet_percentage
        
    FROM (
        SELECT 
            rmbs.game_id,
            rmbs.split_type,
            LAST_VALUE(rmbs.home_or_over_stake_percentage) OVER (
                PARTITION BY rmbs.game_id, rmbs.source, rmbs.book, rmbs.split_type 
                ORDER BY rmbs.last_updated ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as stake_pct,
            LAST_VALUE(rmbs.home_or_over_bets_percentage) OVER (
                PARTITION BY rmbs.game_id, rmbs.source, rmbs.book, rmbs.split_type 
                ORDER BY rmbs.last_updated ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as bet_pct,
            go.home_win,
            go.home_cover_spread,
            go.over,
            ROW_NUMBER() OVER (PARTITION BY rmbs.game_id, rmbs.source, rmbs.book, rmbs.split_type ORDER BY rmbs.last_updated DESC) as rn
        FROM mlb_betting.splits.raw_mlb_betting_splits rmbs
        JOIN mlb_betting.main.game_outcomes go ON rmbs.game_id = go.game_id
        WHERE rmbs.last_updated < rmbs.game_datetime
          AND rmbs.home_or_over_stake_percentage IS NOT NULL
          AND rmbs.home_or_over_bets_percentage IS NOT NULL
    ) ranked_data
    WHERE rn = 1
    GROUP BY split_type
),

top_strategies AS (
    SELECT 
        strategy_name,
        split_type,
        COUNT(*) as strategy_instances,
        SUM(total_bets) as total_strategy_bets,
        SUM(wins) as total_strategy_wins,
        
        -- Weighted averages
        ROUND(SUM(wins * 100.0) / SUM(total_bets), 1) as overall_win_rate,
        ROUND(SUM((wins * 100) - ((total_bets - wins) * 110)), 2) as total_roi_110,
        ROUND(SUM((wins * 100) - ((total_bets - wins) * 110)) / SUM(total_bets * 110) * 100, 1) as roi_percentage,
        
        -- Best performing instances
        MAX(ROUND(100.0 * wins / total_bets, 1)) as best_win_rate,
        MAX(ROUND(((wins * 100) - ((total_bets - wins) * 110)) / (total_bets * 110) * 100, 1)) as best_roi_percentage
        
    FROM (
        -- Simplified strategy results for summary
        SELECT 
            'SHARP_ACTION' as strategy_name,
            split_type,
            COUNT(*) as total_bets,
            SUM(CASE 
                WHEN sharp_signal = 'HOME_OVER' AND ((split_type = 'moneyline' AND home_win = true) OR (split_type = 'spread' AND home_cover_spread = true) OR (split_type = 'total' AND over = true)) THEN 1
                WHEN sharp_signal = 'AWAY_UNDER' AND ((split_type = 'moneyline' AND home_win = false) OR (split_type = 'spread' AND home_cover_spread = false) OR (split_type = 'total' AND over = false)) THEN 1
                ELSE 0
            END) as wins
        FROM (
            SELECT 
                rmbs.game_id,
                rmbs.source,
                rmbs.book,
                rmbs.split_type,
                CASE 
                    WHEN LAST_VALUE(rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage) OVER (ORDER BY rmbs.last_updated ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) >= 10 THEN 'HOME_OVER'
                    WHEN LAST_VALUE(rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage) OVER (ORDER BY rmbs.last_updated ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) <= -10 THEN 'AWAY_UNDER'
                    ELSE 'NO_SIGNAL'
                END as sharp_signal,
                go.home_win,
                go.home_cover_spread,
                go.over,
                ROW_NUMBER() OVER (PARTITION BY rmbs.game_id, rmbs.source, rmbs.book, rmbs.split_type ORDER BY rmbs.last_updated DESC) as rn
            FROM mlb_betting.splits.raw_mlb_betting_splits rmbs
            JOIN mlb_betting.main.game_outcomes go ON rmbs.game_id = go.game_id
            WHERE rmbs.last_updated < rmbs.game_datetime
              AND rmbs.home_or_over_stake_percentage IS NOT NULL
              AND rmbs.home_or_over_bets_percentage IS NOT NULL
        ) sharp_data
        WHERE rn = 1 AND sharp_signal != 'NO_SIGNAL'
        GROUP BY source, book, split_type
        HAVING COUNT(*) >= 5
    ) strategy_summary
    GROUP BY strategy_name, split_type
),

summary_data AS (
    SELECT 
        '=== DATA QUALITY OVERVIEW ===' as section,
        'Data Coverage' as category,
        'Total Games Analyzed' as metric,
        CAST(total_games as VARCHAR) as value,
        CASE WHEN total_games >= 100 THEN 'Excellent sample size' ELSE 'Limited sample - use caution' END as insight,
        1 as sort_order
    FROM data_quality_check
    
    UNION ALL
    
    SELECT 
        'Data Quality',
        'Data Coverage',
        'Source-Book Combinations',
        CAST(total_source_books as VARCHAR),
        CASE WHEN total_source_books >= 3 THEN 'Good market coverage' ELSE 'Limited market data' END,
        2
    FROM data_quality_check
    
    UNION ALL
    
    SELECT 
        '=== MARKET EFFICIENCY ANALYSIS ===',
        'Market Analysis',
        split_type || ' Home Win Rate',
        CAST(ROUND(home_win_rate * 100, 1) as VARCHAR) || '%',
        CASE 
            WHEN ABS(home_win_rate - 0.5) < 0.05 THEN 'Balanced market'
            WHEN home_win_rate > 0.55 THEN 'Home advantage present'
            ELSE 'Away team advantage'
        END,
        3
    FROM overall_market_performance
    
    UNION ALL
    
    SELECT 
        'Sharp Activity',
        'Professional Activity',
        split_type || ' Strong Sharp Instances',
        CAST(strong_sharp_instances as VARCHAR),
        CASE 
            WHEN strong_sharp_instances >= 20 THEN 'High professional activity'
            WHEN strong_sharp_instances >= 10 THEN 'Moderate professional activity'
            ELSE 'Limited professional activity'
        END,
        4
    FROM overall_market_performance
    
    UNION ALL
    
    SELECT 
        '=== STRATEGY PERFORMANCE SUMMARY ===',
        'Strategy Results',
        strategy_name || ' (' || split_type || ')',
        CAST(overall_win_rate as VARCHAR) || '% win rate, ' || CAST(roi_percentage as VARCHAR) || '% ROI',
        CASE 
            WHEN overall_win_rate >= 60 THEN 'EXCELLENT - Strong edge identified'
            WHEN overall_win_rate >= 55 THEN 'VERY GOOD - Profitable opportunity'
            WHEN overall_win_rate >= 52.4 THEN 'PROFITABLE - Beat the juice'
            ELSE 'UNPROFITABLE - Avoid this approach'
        END,
        5
    FROM top_strategies
    WHERE total_strategy_bets >= 5
    
    UNION ALL
    
    SELECT 
        '=== KEY INSIGHTS ===',
        'Insights',
        'Sharp Action Analysis',
        'Based on current data',
        'Professional money indicators show limited effectiveness in current dataset',
        6
    
    UNION ALL
    
    SELECT 
        '=== ACTIONABLE RECOMMENDATIONS ===',
        'Recommendations',
        'Primary Focus',
        'Risk Management',
        'Use small bet sizes (0.5-1% bankroll) until profitable patterns emerge. Focus on data quality improvement.',
        7
)

SELECT 
    section,
    category,
    metric,
    value,
    insight
FROM summary_data
ORDER BY sort_order; 