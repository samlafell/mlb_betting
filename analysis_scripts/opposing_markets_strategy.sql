-- OPPOSING MARKETS STRATEGY
-- =========================
-- Detects games where moneyline and spread splits point to opposite teams.
-- This often indicates sharp vs public money creating potential value.

WITH latest_splits AS (
    SELECT 
        game_id,
        home_team,
        away_team,
        game_datetime,
        split_type,
        split_value,
        home_or_over_stake_percentage as stake_pct,
        home_or_over_bets_percentage as bet_pct,
        (home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
        source,
        COALESCE(book, 'UNKNOWN') as book,
        last_updated,
        ROW_NUMBER() OVER (
            PARTITION BY game_id, split_type, source, COALESCE(book, 'UNKNOWN')
            ORDER BY last_updated DESC
        ) as rn
    FROM mlb_betting.splits.raw_mlb_betting_splits
    WHERE home_or_over_stake_percentage IS NOT NULL 
      AND home_or_over_bets_percentage IS NOT NULL
      AND game_datetime IS NOT NULL
      AND split_type IN ('moneyline', 'spread')  -- Only ML and spread
),

clean_splits AS (
    SELECT *
    FROM latest_splits
    WHERE rn = 1  -- Most recent split for each game/source/book/type
),

ml_recommendations AS (
    SELECT 
        game_id,
        home_team,
        away_team,
        game_datetime,
        source,
        book,
        differential as ml_differential,
        stake_pct as ml_stake_pct,
        bet_pct as ml_bet_pct,
        split_value as ml_split_value,
        CASE 
            WHEN differential > 0 THEN home_team
            ELSE away_team
        END as ml_recommended_team,
        ABS(differential) as ml_signal_strength,
        last_updated as ml_last_updated
    FROM clean_splits
    WHERE split_type = 'moneyline'
),

spread_recommendations AS (
    SELECT 
        game_id,
        home_team,
        away_team,
        game_datetime,
        source,
        book,
        differential as spread_differential,
        stake_pct as spread_stake_pct,
        bet_pct as spread_bet_pct,
        split_value as spread_split_value,
        CASE 
            WHEN differential > 0 THEN home_team
            ELSE away_team
        END as spread_recommended_team,
        ABS(differential) as spread_signal_strength,
        last_updated as spread_last_updated
    FROM clean_splits
    WHERE split_type = 'spread'
),

opposing_markets AS (
    SELECT 
        ml.game_id,
        ml.home_team,
        ml.away_team,
        ml.game_datetime,
        ml.source,
        ml.book,
        
        -- Moneyline data
        ml.ml_recommended_team,
        ml.ml_differential,
        ml.ml_signal_strength,
        ml.ml_stake_pct,
        ml.ml_bet_pct,
        ml.ml_split_value,
        ml.ml_last_updated,
        
        -- Spread data
        sp.spread_recommended_team,
        sp.spread_differential,
        sp.spread_signal_strength,
        sp.spread_stake_pct,
        sp.spread_bet_pct,
        sp.spread_split_value,
        sp.spread_last_updated,
        
        -- Opposition analysis
        CASE 
            WHEN ml.ml_recommended_team != sp.spread_recommended_team THEN 'OPPOSING'
            ELSE 'ALIGNED'
        END as market_relationship,
        
        -- Combined signal strength (average of absolute differentials)
        (ml.ml_signal_strength + sp.spread_signal_strength) / 2 as combined_signal_strength,
        
        -- Determine which market has stronger signal
        CASE 
            WHEN ml.ml_signal_strength > sp.spread_signal_strength THEN 'ML_STRONGER'
            WHEN sp.spread_signal_strength > ml.ml_signal_strength THEN 'SPREAD_STRONGER'
            ELSE 'EQUAL_STRENGTH'
        END as dominant_market,
        
        -- Opposition strength (how different the signals are)
        ABS(ml.ml_differential - sp.spread_differential) as opposition_strength
        
    FROM ml_recommendations ml
    INNER JOIN spread_recommendations sp
        ON ml.game_id = sp.game_id
        AND ml.source = sp.source
        AND ml.book = sp.book
    WHERE ml.ml_recommended_team != sp.spread_recommended_team  -- Only opposing markets
),

game_outcomes AS (
    SELECT DISTINCT
        game_id,
        home_team,
        away_team,
        game_date,
        home_score,
        away_score,
        home_win,
        home_cover_spread,
        over,
        CASE 
            WHEN home_score > away_score THEN home_team
            ELSE away_team
        END as winning_team,
        ABS(home_score - away_score) as run_differential
    FROM mlb_betting.main.game_outcomes
    WHERE home_score IS NOT NULL AND away_score IS NOT NULL
),

strategy_results AS (
    SELECT 
        om.*,
        go.winning_team,
        go.run_differential,
        go.home_score,
        go.away_score,
        
        -- Determine strategy performance
        -- Strategy 1: Follow the stronger signal
        CASE 
            WHEN om.dominant_market = 'ML_STRONGER' AND om.ml_recommended_team = go.winning_team THEN 1
            WHEN om.dominant_market = 'SPREAD_STRONGER' AND om.spread_recommended_team = go.winning_team THEN 1
            WHEN om.dominant_market = 'EQUAL_STRENGTH' AND om.ml_recommended_team = go.winning_team THEN 1
            ELSE 0
        END as stronger_signal_win,
        
        -- Strategy 2: Always follow moneyline when opposing
        CASE 
            WHEN om.ml_recommended_team = go.winning_team THEN 1
            ELSE 0
        END as ml_preference_win,
        
        -- Strategy 3: Always follow spread when opposing  
        CASE 
            WHEN om.spread_recommended_team = go.winning_team THEN 1
            ELSE 0
        END as spread_preference_win,
        
        -- Strategy 4: Contrarian - fade the weaker signal
        CASE 
            WHEN om.dominant_market = 'ML_STRONGER' AND om.spread_recommended_team = go.winning_team THEN 1
            WHEN om.dominant_market = 'SPREAD_STRONGER' AND om.ml_recommended_team = go.winning_team THEN 1
            ELSE 0
        END as contrarian_win
        
    FROM opposing_markets om
    LEFT JOIN game_outcomes go
        ON om.game_id = go.game_id
),

-- Final results with strategy performance analysis - restructured for backtesting service
strategy_performance AS (
    SELECT 
        source || '-' || book as source_book_type,
        'opposing_markets' as split_type,
        COUNT(winning_team) as total_bets,
        
        -- Strategy 1: Follow stronger signal
        SUM(stronger_signal_win) as stronger_signal_wins,
        ROUND(AVG(stronger_signal_win) * 100, 2) as stronger_signal_win_rate,
        ROUND((AVG(stronger_signal_win) * 2.1 - 1) * 100, 2) as stronger_signal_roi_per_100,
        
        -- Strategy 2: ML preference
        SUM(ml_preference_win) as ml_preference_wins,
        ROUND(AVG(ml_preference_win) * 100, 2) as ml_preference_win_rate,
        ROUND((AVG(ml_preference_win) * 2.1 - 1) * 100, 2) as ml_preference_roi_per_100,
        
        -- Strategy 3: Spread preference
        SUM(spread_preference_win) as spread_preference_wins,
        ROUND(AVG(spread_preference_win) * 100, 2) as spread_preference_win_rate,
        ROUND((AVG(spread_preference_win) * 2.1 - 1) * 100, 2) as spread_preference_roi_per_100,
        
        -- Strategy 4: Contrarian
        SUM(contrarian_win) as contrarian_wins,
        ROUND(AVG(contrarian_win) * 100, 2) as contrarian_win_rate,
        ROUND((AVG(contrarian_win) * 2.1 - 1) * 100, 2) as contrarian_roi_per_100
        
    FROM strategy_results
    WHERE winning_team IS NOT NULL  -- Only completed games
    GROUP BY source, book
    HAVING COUNT(winning_team) >= 10  -- Minimum sample size
)

-- Create separate rows for each strategy (backtesting service expects one row per strategy)
SELECT 
    source_book_type,
    split_type,
    'follow_stronger' as strategy_variant,
    total_bets,
    stronger_signal_wins as wins,
    stronger_signal_win_rate as win_rate,
    stronger_signal_roi_per_100 as roi_per_100_unit
FROM strategy_performance

UNION ALL

SELECT 
    source_book_type,
    split_type,
    'ml_preference' as strategy_variant,
    total_bets,
    ml_preference_wins as wins,
    ml_preference_win_rate as win_rate,
    ml_preference_roi_per_100 as roi_per_100_unit
FROM strategy_performance

UNION ALL

SELECT 
    source_book_type,
    split_type,
    'spread_preference' as strategy_variant,
    total_bets,
    spread_preference_wins as wins,
    spread_preference_win_rate as win_rate,
    spread_preference_roi_per_100 as roi_per_100_unit
FROM strategy_performance

UNION ALL

SELECT 
    source_book_type,
    split_type,
    'contrarian' as strategy_variant,
    total_bets,
    contrarian_wins as wins,
    contrarian_win_rate as win_rate,
    contrarian_roi_per_100 as roi_per_100_unit
FROM strategy_performance

ORDER BY roi_per_100_unit DESC; 