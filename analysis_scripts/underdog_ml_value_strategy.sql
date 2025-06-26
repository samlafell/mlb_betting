-- Simplified Underdog ML Value Strategy Analysis  
-- Tests the hypothesis that public loves betting favorites, creating systematic value on underdogs
-- Focuses on core underdog value detection with simplified logic

WITH underdog_analysis AS (
    SELECT 
        rmbs.game_id,
        rmbs.source,
        COALESCE(rmbs.book, 'UNKNOWN') as book,
        rmbs.home_team,
        rmbs.away_team,
        rmbs.game_datetime,
        rmbs.home_or_over_stake_percentage as home_stake_pct,
        rmbs.home_or_over_bets_percentage as home_bet_pct,
        rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage as home_differential,
        
        -- Extract moneyline odds from JSON
        (rmbs.split_value::JSONB->>'home')::INTEGER as home_ml_odds,
        (rmbs.split_value::JSONB->>'away')::INTEGER as away_ml_odds,
        
        go.home_win,
        
        -- Determine favorites and underdogs
        CASE 
            WHEN (rmbs.split_value::JSONB->>'home')::INTEGER < (rmbs.split_value::JSONB->>'away')::INTEGER THEN 'HOME_FAVORITE'
            WHEN (rmbs.split_value::JSONB->>'away')::INTEGER < (rmbs.split_value::JSONB->>'home')::INTEGER THEN 'AWAY_FAVORITE'
            ELSE 'PICK_EM'
        END as favorite_team,
        
        -- Categorize away team odds (our primary underdog target)
        CASE 
            WHEN (rmbs.split_value::JSONB->>'away')::INTEGER BETWEEN 100 AND 200 THEN 'SMALL_AWAY_DOG'
            WHEN (rmbs.split_value::JSONB->>'away')::INTEGER > 200 THEN 'BIG_AWAY_DOG'
            WHEN (rmbs.split_value::JSONB->>'away')::INTEGER BETWEEN -160 AND -100 THEN 'SMALL_AWAY_FAV'
            WHEN (rmbs.split_value::JSONB->>'away')::INTEGER < -160 THEN 'BIG_AWAY_FAV'
            ELSE 'PICK_EM_AWAY'
        END as away_odds_category,
        
        -- Categorize home team odds
        CASE 
            WHEN (rmbs.split_value::JSONB->>'home')::INTEGER BETWEEN 100 AND 200 THEN 'SMALL_HOME_DOG'
            WHEN (rmbs.split_value::JSONB->>'home')::INTEGER > 200 THEN 'BIG_HOME_DOG'
            WHEN (rmbs.split_value::JSONB->>'home')::INTEGER BETWEEN -160 AND -100 THEN 'SMALL_HOME_FAV'
            WHEN (rmbs.split_value::JSONB->>'home')::INTEGER < -160 THEN 'BIG_HOME_FAV'
            ELSE 'PICK_EM_HOME'
        END as home_odds_category,
        
        ROW_NUMBER() OVER (
            PARTITION BY rmbs.game_id, rmbs.source, rmbs.book 
            ORDER BY rmbs.last_updated DESC
        ) as latest_rank
        
    FROM splits.raw_mlb_betting_splits rmbs
    JOIN public.game_outcomes go ON rmbs.game_id = go.game_id
    WHERE rmbs.split_type = 'moneyline'
      AND rmbs.split_value LIKE '{%}'
      AND rmbs.home_or_over_stake_percentage IS NOT NULL
      AND rmbs.home_or_over_bets_percentage IS NOT NULL
      AND go.home_win IS NOT NULL
),

-- Identify value opportunities
value_spots AS (
    SELECT 
        *,
        
        -- Value signal detection - simplified criteria
        CASE 
            -- Away underdog value: public heavily on home favorite (≥65%), away is underdog
            WHEN away_odds_category IN ('SMALL_AWAY_DOG', 'BIG_AWAY_DOG') 
                 AND home_bet_pct >= 65 
                 AND favorite_team = 'HOME_FAVORITE' THEN 'VALUE_AWAY_DOG'
            
            -- Home underdog value: public heavily on away favorite (≤35%), home is underdog
            WHEN home_odds_category IN ('SMALL_HOME_DOG', 'BIG_HOME_DOG') 
                 AND home_bet_pct <= 35 
                 AND favorite_team = 'AWAY_FAVORITE' THEN 'VALUE_HOME_DOG'
            
            -- Moderate value spots with lower thresholds
            WHEN away_odds_category IN ('SMALL_AWAY_DOG', 'BIG_AWAY_DOG') 
                 AND home_bet_pct >= 60 
                 AND favorite_team = 'HOME_FAVORITE' THEN 'MODERATE_VALUE_AWAY_DOG'
            
            WHEN home_odds_category IN ('SMALL_HOME_DOG', 'BIG_HOME_DOG') 
                 AND home_bet_pct <= 40 
                 AND favorite_team = 'AWAY_FAVORITE' THEN 'MODERATE_VALUE_HOME_DOG'
            
            ELSE 'NO_VALUE_SIGNAL'
        END as value_signal,
        
        -- Sharp money confirmation
        CASE 
            WHEN home_odds_category IN ('SMALL_HOME_DOG', 'BIG_HOME_DOG') 
                 AND home_differential >= 10 THEN 'SHARP_SUPPORTS_HOME_DOG'
            WHEN away_odds_category IN ('SMALL_AWAY_DOG', 'BIG_AWAY_DOG') 
                 AND home_differential <= -10 THEN 'SHARP_SUPPORTS_AWAY_DOG'
            ELSE 'NO_SHARP_CONFIRMATION'
        END as sharp_confirmation
        
    FROM underdog_analysis
    WHERE latest_rank = 1  -- Only latest odds for each game/source/book
),

-- Strategy performance analysis
strategy_performance AS (
    SELECT 
        CONCAT(source, '-', book) as source_book_type,
        'moneyline' as split_type,
        value_signal,
        sharp_confirmation,
        
        COUNT(*) as total_bets,
        
        -- Calculate wins based on value signal
        SUM(CASE 
            WHEN value_signal LIKE '%HOME_DOG%' AND home_win = true THEN 1
            WHEN value_signal LIKE '%AWAY_DOG%' AND home_win = false THEN 1
            ELSE 0
        END) as wins,
        
        -- Average underdog odds for ROI calculation
        AVG(CASE 
            WHEN value_signal LIKE '%HOME_DOG%' THEN home_ml_odds
            WHEN value_signal LIKE '%AWAY_DOG%' THEN away_ml_odds
            ELSE NULL
        END) as avg_underdog_odds,
        
        -- Other metrics
        AVG(home_bet_pct) as avg_home_bet_pct,
        AVG(home_differential) as avg_differential
        
    FROM value_spots
    WHERE value_signal != 'NO_VALUE_SIGNAL'
    GROUP BY source, book, value_signal, sharp_confirmation
    HAVING COUNT(*) >= 2  -- Minimum sample size (reduced from 3)
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
    
    -- ROI calculation using average underdog odds (more accurate than assuming -110)
    ROUND(
        CASE 
            WHEN avg_underdog_odds > 0 THEN
                -- For positive odds: profit = (odds/100) * bet_amount for wins, -bet_amount for losses
                ((wins * (avg_underdog_odds/100.0) * 100) - ((total_bets - wins) * 100)) / (total_bets * 100) * 100
            ELSE
                -- Fallback to -110 calculation if odds are negative (shouldn't happen for underdogs)
                ((wins * 100) - ((total_bets - wins) * 110)) / (total_bets * 110) * 100
        END, 1
    ) as roi_per_100_unit,
    
    -- Underdog-specific metrics
    ROUND(avg_underdog_odds, 0) as avg_dog_odds,
    ROUND(avg_home_bet_pct, 1) as avg_home_bet_pct,
    ROUND(avg_differential, 1) as avg_differential,
    
    -- Strategy rating based on ROI (not win rate)
    CASE 
        WHEN ((wins * (avg_underdog_odds/100.0) * 100) - ((total_bets - wins) * 100)) / (total_bets * 100) * 100 >= 15 
             AND total_bets >= 5 THEN '🟢 EXCELLENT'
        WHEN ((wins * (avg_underdog_odds/100.0) * 100) - ((total_bets - wins) * 100)) / (total_bets * 100) * 100 >= 10 
             AND total_bets >= 3 THEN '🟢 VERY GOOD'
        WHEN ((wins * (avg_underdog_odds/100.0) * 100) - ((total_bets - wins) * 100)) / (total_bets * 100) * 100 >= 5 
             AND total_bets >= 2 THEN '🟡 GOOD'  
        WHEN ((wins * (avg_underdog_odds/100.0) * 100) - ((total_bets - wins) * 100)) / (total_bets * 100) * 100 > 0 
             AND total_bets >= 2 THEN '🟡 PROFITABLE'
        ELSE '🔴 UNPROFITABLE'
    END as strategy_rating,
    
    -- Confidence level
    CASE 
        WHEN total_bets >= 15 THEN 'HIGH'
        WHEN total_bets >= 8 THEN 'MEDIUM'
        WHEN total_bets >= 5 THEN 'LOW'
        ELSE 'VERY_LOW'
    END as confidence_level,
    
    sharp_confirmation,
    
    -- Strategy insights
    CASE 
        WHEN value_signal = 'VALUE_HOME_DOG' THEN 'Home underdog when public loves away favorite (≤35%)'
        WHEN value_signal = 'VALUE_AWAY_DOG' THEN 'Away underdog when public loves home favorite (≥65%)'
        WHEN value_signal = 'MODERATE_VALUE_HOME_DOG' THEN 'Moderate home underdog value (≤40%)'
        WHEN value_signal = 'MODERATE_VALUE_AWAY_DOG' THEN 'Moderate away underdog value (≥60%)'
        ELSE 'General underdog value'
    END as strategy_insight
    
FROM strategy_performance
WHERE total_bets >= 2  -- Final minimum threshold (reduced from 3)
ORDER BY 
    roi_per_100_unit DESC,
    total_bets DESC,
    strategy_variant ASC 