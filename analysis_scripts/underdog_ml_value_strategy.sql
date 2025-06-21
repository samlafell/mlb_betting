-- Underdog ML Value Strategy Analysis  
-- Tests the hypothesis that public loves betting favorites, creating systematic value on underdogs
-- Combines ML dog analysis with spread betting patterns to identify enhanced value spots

WITH moneyline_data AS (
    SELECT 
        rmbs.game_id,
        rmbs.source,
        COALESCE(rmbs.book, 'UNKNOWN') as book,
        rmbs.home_team,
        rmbs.away_team,
        rmbs.game_datetime,
        rmbs.last_updated,
        rmbs.home_or_over_stake_percentage as home_stake_pct,
        rmbs.home_or_over_bets_percentage as home_bet_pct,
        rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage as home_differential,
        
        -- Extract moneyline odds
        CASE 
            WHEN rmbs.split_type = 'moneyline' AND rmbs.split_value LIKE '{%}' THEN
                TRY_CAST(json_extract_string(rmbs.split_value, '$.home') AS INTEGER)
            ELSE NULL
        END as home_ml_odds,
        
        CASE 
            WHEN rmbs.split_type = 'moneyline' AND rmbs.split_value LIKE '{%}' THEN
                TRY_CAST(json_extract_string(rmbs.split_value, '$.away') AS INTEGER)
            ELSE NULL
        END as away_ml_odds,
        
        go.home_win,
        
        ROW_NUMBER() OVER (
            PARTITION BY rmbs.game_id, rmbs.source, rmbs.book 
            ORDER BY rmbs.last_updated DESC
        ) as latest_rank
        
    FROM mlb_betting.splits.raw_mlb_betting_splits rmbs
    JOIN mlb_betting.main.game_outcomes go ON rmbs.game_id = go.game_id
    WHERE rmbs.split_type = 'moneyline'
      AND rmbs.last_updated < rmbs.game_datetime
      AND rmbs.split_value IS NOT NULL
      AND rmbs.home_or_over_stake_percentage IS NOT NULL
      AND rmbs.home_or_over_bets_percentage IS NOT NULL
      AND go.home_win IS NOT NULL
),

-- Get corresponding spread data for the same games
spread_data AS (
    SELECT 
        rmbs.game_id,
        rmbs.source,
        COALESCE(rmbs.book, 'UNKNOWN') as book,
        rmbs.home_or_over_stake_percentage as spread_home_stake_pct,
        rmbs.home_or_over_bets_percentage as spread_home_bet_pct,
        rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage as spread_home_differential,
        
        TRY_CAST(rmbs.split_value AS DOUBLE) as spread_line,
        
        ROW_NUMBER() OVER (
            PARTITION BY rmbs.game_id, rmbs.source, rmbs.book 
            ORDER BY rmbs.last_updated DESC
        ) as latest_rank
        
    FROM mlb_betting.splits.raw_mlb_betting_splits rmbs
    WHERE rmbs.split_type = 'spread'
      AND rmbs.split_value IS NOT NULL
      AND rmbs.home_or_over_stake_percentage IS NOT NULL
      AND rmbs.home_or_over_bets_percentage IS NOT NULL
),

-- Combine ML and spread data for comprehensive analysis
combined_analysis AS (
    SELECT 
        ml.source,
        ml.book,
        ml.game_id,
        ml.home_team,
        ml.away_team,
        ml.home_ml_odds,
        ml.away_ml_odds,
        ml.home_stake_pct as ml_home_stake_pct,
        ml.home_bet_pct as ml_home_bet_pct,
        ml.home_differential as ml_home_differential,
        ml.home_win,
        
        -- Spread data
        sp.spread_home_stake_pct,
        sp.spread_home_bet_pct,
        sp.spread_home_differential,
        sp.spread_line,
        
        -- Determine favorites and underdogs
        CASE 
            WHEN ml.home_ml_odds < ml.away_ml_odds THEN 'HOME_FAVORITE'
            WHEN ml.away_ml_odds < ml.home_ml_odds THEN 'AWAY_FAVORITE'
            ELSE 'PICK_EM'
        END as favorite_team,
        
        -- Calculate implied win probabilities from odds
        CASE 
            WHEN ml.home_ml_odds > 0 THEN 100.0 / (ml.home_ml_odds + 100)
            WHEN ml.home_ml_odds < 0 THEN ABS(ml.home_ml_odds) / (ABS(ml.home_ml_odds) + 100)
            ELSE 0.5
        END * 100 as home_implied_prob,
        
        CASE 
            WHEN ml.away_ml_odds > 0 THEN 100.0 / (ml.away_ml_odds + 100)
            WHEN ml.away_ml_odds < 0 THEN ABS(ml.away_ml_odds) / (ABS(ml.away_ml_odds) + 100)
            ELSE 0.5
        END * 100 as away_implied_prob,
        
        -- Categorize odds ranges
        CASE 
            WHEN ml.home_ml_odds BETWEEN 100 AND 200 THEN 'SMALL_HOME_DOG'
            WHEN ml.home_ml_odds > 200 THEN 'BIG_HOME_DOG'
            WHEN ml.home_ml_odds BETWEEN -160 AND -100 THEN 'SMALL_HOME_FAV'
            WHEN ml.home_ml_odds < -160 THEN 'BIG_HOME_FAV'
            ELSE 'PICK_EM_HOME'
        END as home_odds_category,
        
        CASE 
            WHEN ml.away_ml_odds BETWEEN 100 AND 200 THEN 'SMALL_AWAY_DOG'
            WHEN ml.away_ml_odds > 200 THEN 'BIG_AWAY_DOG'
            WHEN ml.away_ml_odds BETWEEN -160 AND -100 THEN 'SMALL_AWAY_FAV'
            WHEN ml.away_ml_odds < -160 THEN 'BIG_AWAY_FAV'
            ELSE 'PICK_EM_AWAY'
        END as away_odds_category
        
    FROM moneyline_data ml
    LEFT JOIN spread_data sp ON ml.game_id = sp.game_id 
        AND ml.source = sp.source 
        AND ml.book = sp.book 
        AND sp.latest_rank = 1
    WHERE ml.latest_rank = 1
      AND ml.home_ml_odds IS NOT NULL 
      AND ml.away_ml_odds IS NOT NULL
),

-- Identify underdog value opportunities
underdog_value_spots AS (
    SELECT 
        *,
        
        -- Public bias analysis
        CASE 
            WHEN favorite_team = 'HOME_FAVORITE' AND ml_home_bet_pct >= 70 THEN 'HEAVY_PUBLIC_ON_HOME_FAV'
            WHEN favorite_team = 'AWAY_FAVORITE' AND ml_home_bet_pct <= 30 THEN 'HEAVY_PUBLIC_ON_AWAY_FAV'
            WHEN favorite_team = 'HOME_FAVORITE' AND ml_home_bet_pct >= 60 THEN 'MODERATE_PUBLIC_ON_HOME_FAV'
            WHEN favorite_team = 'AWAY_FAVORITE' AND ml_home_bet_pct <= 40 THEN 'MODERATE_PUBLIC_ON_AWAY_FAV'
            ELSE 'BALANCED_PUBLIC'
        END as public_bias_pattern,
        
        -- Spread vs ML disagreement
        CASE 
            WHEN spread_home_bet_pct IS NOT NULL THEN
                CASE 
                    WHEN ABS(ml_home_bet_pct - spread_home_bet_pct) >= 20 THEN 'HIGH_ML_SPREAD_DISAGREEMENT'
                    WHEN ABS(ml_home_bet_pct - spread_home_bet_pct) >= 10 THEN 'MODERATE_ML_SPREAD_DISAGREEMENT'
                    ELSE 'ML_SPREAD_AGREEMENT'
                END
            ELSE 'NO_SPREAD_DATA'
        END as ml_spread_agreement,
        
        -- Value signal detection
        CASE 
            -- Home underdog value when public heavily on away favorite
            WHEN home_odds_category IN ('SMALL_HOME_DOG', 'BIG_HOME_DOG') 
                 AND ml_home_bet_pct <= 35 
                 AND favorite_team = 'AWAY_FAVORITE' THEN 'VALUE_HOME_DOG'
            -- Away underdog value when public heavily on home favorite     
            WHEN away_odds_category IN ('SMALL_AWAY_DOG', 'BIG_AWAY_DOG') 
                 AND ml_home_bet_pct >= 65 
                 AND favorite_team = 'HOME_FAVORITE' THEN 'VALUE_AWAY_DOG'
            -- Enhanced value when spread betting disagrees with ML
            WHEN home_odds_category IN ('SMALL_HOME_DOG', 'BIG_HOME_DOG') 
                 AND ml_home_bet_pct <= 40 
                 AND spread_home_bet_pct >= 55 THEN 'ENHANCED_VALUE_HOME_DOG'
            WHEN away_odds_category IN ('SMALL_AWAY_DOG', 'BIG_AWAY_DOG') 
                 AND ml_home_bet_pct >= 60 
                 AND spread_home_bet_pct <= 45 THEN 'ENHANCED_VALUE_AWAY_DOG'
            ELSE 'NO_VALUE_SIGNAL'
        END as value_signal,
        
        -- Sharp money confirmation
        CASE 
            WHEN home_odds_category IN ('SMALL_HOME_DOG', 'BIG_HOME_DOG') 
                 AND ml_home_differential >= 10 THEN 'SHARP_SUPPORTS_HOME_DOG'
            WHEN away_odds_category IN ('SMALL_AWAY_DOG', 'BIG_AWAY_DOG') 
                 AND ml_home_differential <= -10 THEN 'SHARP_SUPPORTS_AWAY_DOG'
            ELSE 'NO_SHARP_CONFIRMATION'
        END as sharp_confirmation
        
    FROM combined_analysis
),

-- Strategy performance analysis
strategy_results AS (
    SELECT 
        CONCAT(source, '-', book) as source_book_type,
        'moneyline' as split_type,
        value_signal,
        public_bias_pattern,
        sharp_confirmation,
        
        COUNT(*) as total_bets,
        
        -- Calculate wins based on value signal
        SUM(CASE 
            WHEN value_signal = 'VALUE_HOME_DOG' AND home_win = true THEN 1
            WHEN value_signal = 'VALUE_AWAY_DOG' AND home_win = false THEN 1
            WHEN value_signal = 'ENHANCED_VALUE_HOME_DOG' AND home_win = true THEN 1
            WHEN value_signal = 'ENHANCED_VALUE_AWAY_DOG' AND home_win = false THEN 1
            ELSE 0
        END) as wins,
        
        -- Average metrics
        AVG(CASE 
            WHEN value_signal LIKE '%HOME_DOG' THEN home_ml_odds
            WHEN value_signal LIKE '%AWAY_DOG' THEN away_ml_odds
            ELSE NULL
        END) as avg_underdog_odds,
        
        AVG(CASE 
            WHEN value_signal LIKE '%HOME_DOG' THEN home_implied_prob
            WHEN value_signal LIKE '%AWAY_DOG' THEN away_implied_prob
            ELSE NULL
        END) as avg_implied_prob,
        
        AVG(ml_home_bet_pct) as avg_home_bet_pct,
        AVG(ml_home_differential) as avg_ml_differential,
        AVG(COALESCE(spread_home_bet_pct, 50)) as avg_spread_bet_pct
        
    FROM underdog_value_spots
    WHERE value_signal != 'NO_VALUE_SIGNAL'
    GROUP BY source, book, value_signal, public_bias_pattern, sharp_confirmation
    HAVING COUNT(*) >= 3  -- Minimum sample size
)

SELECT 
    source_book_type,
    split_type,
    'UNDERDOG_ML_VALUE' as strategy_name,
    value_signal as strategy_variant,
    total_bets,
    wins,
    
    -- Core performance metrics
    ROUND(100.0 * wins / total_bets, 1) as win_rate,
    
    -- ROI calculations - using actual odds would be more accurate, but using -110 for consistency
    ROUND(((wins * 100) - ((total_bets - wins) * 110)) / (total_bets * 110) * 100, 1) as roi_per_100_unit,
    
    -- Underdog-specific metrics
    ROUND(avg_underdog_odds, 0) as avg_dog_odds,
    ROUND(avg_implied_prob, 1) as avg_implied_prob_pct,
    ROUND(avg_home_bet_pct, 1) as avg_home_bet_pct,
    ROUND(avg_ml_differential, 1) as avg_ml_diff,
    ROUND(avg_spread_bet_pct, 1) as avg_spread_bet_pct,
    
    -- Strategy rating
    CASE 
        WHEN (100.0 * wins / total_bets) >= 60 AND total_bets >= 10 THEN '🟢 EXCELLENT'
        WHEN (100.0 * wins / total_bets) >= 55 AND total_bets >= 8 THEN '🟢 VERY GOOD'
        WHEN (100.0 * wins / total_bets) >= 52 AND total_bets >= 5 THEN '🟡 GOOD'  
        WHEN (100.0 * wins / total_bets) >= 48 AND total_bets >= 5 THEN '🟡 PROFITABLE'
        ELSE '🔴 UNPROFITABLE'
    END as strategy_rating,
    
    -- Confidence level
    CASE 
        WHEN total_bets >= 25 THEN 'HIGH'
        WHEN total_bets >= 15 THEN 'MEDIUM'
        WHEN total_bets >= 8 THEN 'LOW'
        ELSE 'VERY_LOW'
    END as confidence_level,
    
    public_bias_pattern,
    sharp_confirmation,
    
    -- Strategy insights
    CASE 
        WHEN value_signal = 'VALUE_HOME_DOG' THEN 'Home underdog when public loves away favorite'
        WHEN value_signal = 'VALUE_AWAY_DOG' THEN 'Away underdog when public loves home favorite'
        WHEN value_signal = 'ENHANCED_VALUE_HOME_DOG' THEN 'Home dog + spread betting disagrees with ML'
        WHEN value_signal = 'ENHANCED_VALUE_AWAY_DOG' THEN 'Away dog + spread betting disagrees with ML'
        ELSE 'General underdog value'
    END as strategy_insight
    
FROM strategy_results
WHERE total_bets >= 3
ORDER BY 
    roi_per_100_unit DESC,
    total_bets DESC,
    strategy_variant ASC 