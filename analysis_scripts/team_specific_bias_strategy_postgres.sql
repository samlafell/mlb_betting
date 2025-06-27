-- Team-Specific Public Bias Strategy Analysis
-- Identifies teams that consistently get overbet or underbet by the public
-- Popular teams (Yankees, Dodgers) often overvalued, small market teams undervalued

WITH team_splits_data AS (
    SELECT 
        rmbs.game_id,
        rmbs.source,
        COALESCE(rmbs.book, 'UNKNOWN') as book,
        rmbs.split_type,
        rmbs.home_team,
        rmbs.away_team,
        rmbs.game_datetime,
        rmbs.last_updated,
        rmbs.home_or_over_stake_percentage as home_stake_pct,
        rmbs.home_or_over_bets_percentage as home_bet_pct,
        rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage as home_differential,
        
        -- Extract line values for context with safe casting
        CASE 
            WHEN rmbs.split_type = 'moneyline' AND rmbs.split_value LIKE '{%}' THEN
                CASE WHEN (rmbs.split_value::JSONB->>'home') ~ '^-?[0-9]+$' 
                     THEN (rmbs.split_value::JSONB->>'home')::INTEGER 
                     ELSE NULL END
            WHEN rmbs.split_type IN ('spread', 'total') AND rmbs.split_value ~ '^-?[0-9]+\.?[0-9]*$' THEN
                rmbs.split_value::DOUBLE PRECISION
            ELSE NULL
        END as line_value,
        
        go.home_win,
        go.home_cover_spread,
        go.over,
        
        ROW_NUMBER() OVER (
            PARTITION BY rmbs.game_id, rmbs.source, rmbs.book, rmbs.split_type 
            ORDER BY rmbs.last_updated DESC
        ) as latest_rank
        
    FROM splits.raw_mlb_betting_splits rmbs
    JOIN public.game_outcomes go ON rmbs.game_id = go.game_id
    WHERE rmbs.last_updated < rmbs.game_datetime
      AND rmbs.split_value IS NOT NULL
      AND rmbs.home_or_over_stake_percentage IS NOT NULL
      AND rmbs.home_or_over_bets_percentage IS NOT NULL
      AND go.home_win IS NOT NULL
),

-- Calculate team-specific public bias patterns
team_bias_analysis AS (
    SELECT 
        source,
        book,
        split_type,
        
        -- Home team analysis
        home_team as team,
        'HOME' as venue,
        COUNT(*) as games_count,
        AVG(home_stake_pct) as avg_stake_pct_for,
        AVG(home_bet_pct) as avg_bet_pct_for,
        AVG(home_differential) as avg_differential_for,
        
        -- Performance when team is home
        AVG(CASE 
            WHEN split_type = 'moneyline' THEN CASE WHEN home_win = true THEN 1.0 ELSE 0.0 END
            WHEN split_type = 'spread' THEN CASE WHEN home_cover_spread = true THEN 1.0 ELSE 0.0 END
            WHEN split_type = 'total' THEN CASE WHEN over = true THEN 1.0 ELSE 0.0 END
        END) as home_performance,
        
        -- Public bias categories
        CASE 
            WHEN AVG(home_bet_pct) >= 65 THEN 'PUBLIC_LOVES_HOME'
            WHEN AVG(home_bet_pct) >= 55 THEN 'PUBLIC_LIKES_HOME'
            WHEN AVG(home_bet_pct) <= 35 THEN 'PUBLIC_FADES_HOME'
            WHEN AVG(home_bet_pct) <= 45 THEN 'PUBLIC_DISLIKES_HOME'
            ELSE 'PUBLIC_NEUTRAL_HOME'
        END as public_bias_home
        
    FROM team_splits_data
    WHERE latest_rank = 1
    GROUP BY source, book, split_type, home_team
    HAVING COUNT(*) >= 2  -- Minimum games for meaningful analysis (lowered for testing)
    
    UNION ALL
    
    -- Away team analysis
    SELECT 
        source,
        book,
        split_type,
        
        away_team as team,
        'AWAY' as venue,
        COUNT(*) as games_count,
        AVG(100 - home_stake_pct) as avg_stake_pct_for,  -- Away team gets the inverse
        AVG(100 - home_bet_pct) as avg_bet_pct_for,
        AVG(-home_differential) as avg_differential_for,  -- Away team differential is negative of home
        
        -- Performance when team is away
        AVG(CASE 
            WHEN split_type = 'moneyline' THEN CASE WHEN home_win = false THEN 1.0 ELSE 0.0 END
            WHEN split_type = 'spread' THEN CASE WHEN home_cover_spread = false THEN 1.0 ELSE 0.0 END
            WHEN split_type = 'total' THEN CASE WHEN over = true THEN 1.0 ELSE 0.0 END
        END) as home_performance,  -- Changed from away_performance to match union
        
        -- Public bias categories for away team
        CASE 
            WHEN AVG(100 - home_bet_pct) >= 65 THEN 'PUBLIC_LOVES_AWAY'
            WHEN AVG(100 - home_bet_pct) >= 55 THEN 'PUBLIC_LIKES_AWAY'
            WHEN AVG(100 - home_bet_pct) <= 35 THEN 'PUBLIC_FADES_AWAY'
            WHEN AVG(100 - home_bet_pct) <= 45 THEN 'PUBLIC_DISLIKES_AWAY'
            ELSE 'PUBLIC_NEUTRAL_AWAY'
        END as public_bias_away
        
    FROM team_splits_data
    WHERE latest_rank = 1
    GROUP BY source, book, split_type, away_team
    HAVING COUNT(*) >= 2
),

-- Aggregate team bias across home/away and identify overall patterns
team_overall_bias AS (
    SELECT 
        source,
        book,
        split_type,
        team,
        
        SUM(games_count) as total_games,
        AVG(avg_stake_pct_for) as overall_avg_stake_pct,
        AVG(avg_bet_pct_for) as overall_avg_bet_pct,
        AVG(avg_differential_for) as overall_avg_differential,
        
        -- Performance metrics  
        AVG(home_performance) as overall_performance,
        
        -- Determine team's public perception
        CASE 
            WHEN AVG(avg_bet_pct_for) >= 60 THEN 'PUBLIC_DARLING'
            WHEN AVG(avg_bet_pct_for) >= 52 THEN 'PUBLIC_FAVORITE'
            WHEN AVG(avg_bet_pct_for) <= 40 THEN 'PUBLIC_FADE'
            WHEN AVG(avg_bet_pct_for) <= 48 THEN 'PUBLIC_UNDERVALUED'
            ELSE 'PUBLIC_NEUTRAL'
        END as team_public_perception,
        
        -- Sharp money vs public disagreement
        CASE 
            WHEN AVG(avg_differential_for) >= 8 AND AVG(avg_bet_pct_for) <= 45 THEN 'SHARP_LOVES_UNDERVALUED'
            WHEN AVG(avg_differential_for) <= -8 AND AVG(avg_bet_pct_for) >= 55 THEN 'SHARP_FADES_OVERVALUED'
            WHEN ABS(AVG(avg_differential_for)) >= 5 THEN 'SHARP_DISAGREES_WITH_PUBLIC'
            ELSE 'SHARP_AGREES_WITH_PUBLIC'
        END as sharp_vs_public,
        
        -- Classify team market type
        CASE 
            WHEN team IN ('Yankees', 'Dodgers', 'Red Sox', 'Cubs', 'Mets', 'Giants') THEN 'LARGE_MARKET'
            WHEN team IN ('Rays', 'Marlins', 'Athletics', 'Pirates', 'Royals', 'Reds') THEN 'SMALL_MARKET'
            ELSE 'MEDIUM_MARKET'
        END as market_size
        
    FROM team_bias_analysis
    GROUP BY source, book, split_type, team
    HAVING SUM(games_count) >= 3  -- Ensure adequate sample size (lowered for testing)
),

-- Identify betting opportunities based on team bias
team_bias_opportunities AS (
    SELECT 
        *,
        
        -- Value opportunity detection
        CASE 
            -- Fade overvalued public darlings
            WHEN team_public_perception = 'PUBLIC_DARLING' 
                 AND overall_avg_differential <= -5 
                 AND overall_performance <= 0.48 THEN 'FADE_PUBLIC_DARLING'
            -- Back undervalued teams with sharp support
            WHEN team_public_perception IN ('PUBLIC_FADE', 'PUBLIC_UNDERVALUED') 
                 AND overall_avg_differential >= 5 
                 AND overall_performance >= 0.52 THEN 'BACK_UNDERVALUED_SHARP'
            -- Large market bias fade
            WHEN market_size = 'LARGE_MARKET' 
                 AND overall_avg_bet_pct >= 58 
                 AND overall_performance <= 0.50 THEN 'FADE_LARGE_MARKET_BIAS'
            -- Small market value
            WHEN market_size = 'SMALL_MARKET' 
                 AND overall_avg_bet_pct <= 42 
                 AND overall_performance >= 0.50 THEN 'BACK_SMALL_MARKET_VALUE'
            ELSE 'NO_CLEAR_OPPORTUNITY'
        END as opportunity_type
        
    FROM team_overall_bias
),

-- Calculate strategy performance for each opportunity type
strategy_performance AS (
    SELECT 
        CONCAT(source, '-', book, '-', split_type) as source_book_type,
        split_type,
        'TEAM_SPECIFIC_BIAS' as strategy_name,
        opportunity_type as strategy_variant,
        market_size,
        team_public_perception,
        sharp_vs_public,
        
        COUNT(*) as total_teams,
        SUM(total_games) as total_games_analyzed,
        AVG(overall_avg_bet_pct) as avg_public_bet_pct,
        AVG(overall_avg_differential) as avg_sharp_differential,
        AVG(overall_performance) as avg_team_performance,
        
        -- Performance metrics
        ROUND((AVG(overall_performance) * 100)::NUMERIC, 1) as avg_win_rate_pct,
        
        -- ROI estimation (simplified, assumes even betting across all games)
        ROUND((((AVG(overall_performance) * 100) - ((1 - AVG(overall_performance)) * 110)) / 110 * 100)::NUMERIC, 1) as estimated_roi_per_100,
        
        -- Team examples
        STRING_AGG(team, ', ') as example_teams
        
    FROM team_bias_opportunities
    WHERE opportunity_type != 'NO_CLEAR_OPPORTUNITY'
    GROUP BY source, book, split_type, opportunity_type, market_size, team_public_perception, sharp_vs_public
    HAVING COUNT(*) >= 1  -- At least 1 team in category (lowered for testing)
)

SELECT 
    source_book_type,
    split_type,
    strategy_name,
    strategy_variant,
    total_games_analyzed as total_bets,  -- 🔧 FIX: Use total_games_analyzed as the bet count, not total_teams
    total_teams,
    
    -- Performance metrics
    avg_win_rate_pct as win_rate,
    estimated_roi_per_100 as roi_per_100_unit,
    
    -- Supporting data
    ROUND((avg_public_bet_pct)::NUMERIC, 1) as avg_public_bet_pct,
    ROUND((avg_sharp_differential)::NUMERIC, 1) as avg_sharp_diff,
    
    -- Strategy classification
    CASE 
        WHEN avg_win_rate_pct >= 58 AND total_games_analyzed >= 20 THEN '🟢 EXCELLENT'
        WHEN avg_win_rate_pct >= 55 AND total_games_analyzed >= 15 THEN '🟢 VERY GOOD'
        WHEN avg_win_rate_pct >= 52.4 AND total_games_analyzed >= 10 THEN '🟡 PROFITABLE'
        WHEN avg_win_rate_pct >= 50 AND total_games_analyzed >= 10 THEN '🟡 MARGINAL'
        ELSE '🔴 UNPROFITABLE'
    END as strategy_rating,
    
    -- Confidence level
    CASE 
        WHEN total_games_analyzed >= 50 THEN 'HIGH'
        WHEN total_games_analyzed >= 25 THEN 'MEDIUM'
        WHEN total_games_analyzed >= 15 THEN 'LOW'
        ELSE 'VERY_LOW'
    END as confidence_level,
    
    market_size,
    team_public_perception,
    sharp_vs_public,
    example_teams,
    
    -- Strategy insights
    CASE 
        WHEN strategy_variant = 'FADE_PUBLIC_DARLING' THEN 'Fade overvalued popular teams'
        WHEN strategy_variant = 'BACK_UNDERVALUED_SHARP' THEN 'Back undervalued teams with sharp support'
        WHEN strategy_variant = 'FADE_LARGE_MARKET_BIAS' THEN 'Fade large market public bias'
        WHEN strategy_variant = 'BACK_SMALL_MARKET_VALUE' THEN 'Back overlooked small market teams'
        ELSE 'General team bias exploitation'
    END as strategy_insight
    
FROM strategy_performance
WHERE total_teams >= 1
ORDER BY 
    estimated_roi_per_100 DESC,
    total_games_analyzed DESC,
    strategy_variant ASC 