-- Action Network Historical Line Movement Schema
-- 
-- This schema stores historical line movement data from Action Network
-- to track how betting lines change over time for MLB games.

-- Create schema for Action Network data
CREATE SCHEMA IF NOT EXISTS action_network;

-- Historical line movement data table
CREATE TABLE IF NOT EXISTS action_network.historical_line_movement (
    id SERIAL PRIMARY KEY,
    
    -- Game identification
    game_id INTEGER NOT NULL,
    home_team VARCHAR(100) NOT NULL,
    away_team VARCHAR(100) NOT NULL,
    game_datetime TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Historical entry metadata
    entry_index INTEGER NOT NULL, -- Index in the historical array
    period VARCHAR(20) NOT NULL CHECK (period IN ('pregame', 'live')),
    entry_timestamp TIMESTAMP WITH TIME ZONE,
    
    -- Moneyline data
    moneyline_home_american INTEGER,
    moneyline_home_decimal DECIMAL(8,4),
    moneyline_away_american INTEGER,
    moneyline_away_decimal DECIMAL(8,4),
    
    -- Spread data
    spread_line DECIMAL(5,2),
    spread_home_american INTEGER,
    spread_home_decimal DECIMAL(8,4),
    spread_away_american INTEGER,
    spread_away_decimal DECIMAL(8,4),
    
    -- Total data
    total_line DECIMAL(5,2),
    total_over_american INTEGER,
    total_over_decimal DECIMAL(8,4),
    total_under_american INTEGER,
    total_under_decimal DECIMAL(8,4),
    
    -- Raw event data (for debugging and future analysis)
    raw_event_data JSONB,
    
    -- Extraction metadata
    extracted_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    history_url TEXT NOT NULL,
    
    -- Constraints
    UNIQUE(game_id, entry_index),
    
    -- Indexes for performance
    INDEX idx_historical_movement_game_id (game_id),
    INDEX idx_historical_movement_teams (home_team, away_team),
    INDEX idx_historical_movement_datetime (game_datetime),
    INDEX idx_historical_movement_period (period),
    INDEX idx_historical_movement_extracted (extracted_at)
);

-- Game summary table for quick lookups
CREATE TABLE IF NOT EXISTS action_network.game_summary (
    id SERIAL PRIMARY KEY,
    
    -- Game identification
    game_id INTEGER NOT NULL UNIQUE,
    home_team VARCHAR(100) NOT NULL,
    away_team VARCHAR(100) NOT NULL,
    game_datetime TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Summary statistics
    total_entries INTEGER NOT NULL DEFAULT 0,
    pregame_entries INTEGER NOT NULL DEFAULT 0,
    live_entries INTEGER NOT NULL DEFAULT 0,
    
    -- Line movement summary
    moneyline_movements INTEGER NOT NULL DEFAULT 0,
    moneyline_significant_moves INTEGER NOT NULL DEFAULT 0,
    spread_movements INTEGER NOT NULL DEFAULT 0,
    spread_significant_moves INTEGER NOT NULL DEFAULT 0,
    total_movements INTEGER NOT NULL DEFAULT 0,
    total_significant_moves INTEGER NOT NULL DEFAULT 0,
    
    -- Opening lines (first entry)
    opening_moneyline_home INTEGER,
    opening_moneyline_away INTEGER,
    opening_spread_line DECIMAL(5,2),
    opening_spread_home INTEGER,
    opening_spread_away INTEGER,
    opening_total_line DECIMAL(5,2),
    opening_total_over INTEGER,
    opening_total_under INTEGER,
    
    -- Closing lines (last pregame entry)
    closing_moneyline_home INTEGER,
    closing_moneyline_away INTEGER,
    closing_spread_line DECIMAL(5,2),
    closing_spread_home INTEGER,
    closing_spread_away INTEGER,
    closing_total_line DECIMAL(5,2),
    closing_total_over INTEGER,
    closing_total_under INTEGER,
    
    -- Metadata
    extracted_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    history_url TEXT NOT NULL,
    
    -- Indexes
    INDEX idx_game_summary_game_id (game_id),
    INDEX idx_game_summary_teams (home_team, away_team),
    INDEX idx_game_summary_datetime (game_datetime),
    INDEX idx_game_summary_extracted (extracted_at)
);

-- Line movement analysis view
CREATE OR REPLACE VIEW action_network.line_movement_analysis AS
SELECT 
    gs.game_id,
    gs.home_team,
    gs.away_team,
    gs.game_datetime,
    gs.total_entries,
    gs.pregame_entries,
    gs.live_entries,
    
    -- Moneyline movement
    gs.moneyline_movements,
    gs.moneyline_significant_moves,
    CASE 
        WHEN gs.moneyline_movements > 0 
        THEN ROUND((gs.moneyline_significant_moves::DECIMAL / gs.moneyline_movements) * 100, 2)
        ELSE 0 
    END as moneyline_significant_pct,
    
    -- Spread movement
    gs.spread_movements,
    gs.spread_significant_moves,
    CASE 
        WHEN gs.spread_movements > 0 
        THEN ROUND((gs.spread_significant_moves::DECIMAL / gs.spread_movements) * 100, 2)
        ELSE 0 
    END as spread_significant_pct,
    
    -- Total movement
    gs.total_movements,
    gs.total_significant_moves,
    CASE 
        WHEN gs.total_movements > 0 
        THEN ROUND((gs.total_significant_moves::DECIMAL / gs.total_movements) * 100, 2)
        ELSE 0 
    END as total_significant_pct,
    
    -- Line movement magnitude
    ABS(gs.closing_moneyline_home - gs.opening_moneyline_home) as moneyline_home_movement,
    ABS(gs.closing_moneyline_away - gs.opening_moneyline_away) as moneyline_away_movement,
    ABS(gs.closing_spread_line - gs.opening_spread_line) as spread_line_movement,
    ABS(gs.closing_total_line - gs.opening_total_line) as total_line_movement,
    
    -- Movement direction
    CASE 
        WHEN gs.closing_moneyline_home > gs.opening_moneyline_home THEN 'home_stronger'
        WHEN gs.closing_moneyline_home < gs.opening_moneyline_home THEN 'home_weaker'
        ELSE 'no_change'
    END as moneyline_direction,
    
    CASE 
        WHEN gs.closing_spread_line > gs.opening_spread_line THEN 'home_favored_more'
        WHEN gs.closing_spread_line < gs.opening_spread_line THEN 'home_favored_less'
        ELSE 'no_change'
    END as spread_direction,
    
    CASE 
        WHEN gs.closing_total_line > gs.opening_total_line THEN 'higher'
        WHEN gs.closing_total_line < gs.opening_total_line THEN 'lower'
        ELSE 'no_change'
    END as total_direction,
    
    gs.extracted_at
FROM action_network.game_summary gs;

-- Function to calculate line movement summary
CREATE OR REPLACE FUNCTION action_network.calculate_line_movement_summary(p_game_id INTEGER)
RETURNS TABLE (
    moneyline_movements INTEGER,
    moneyline_significant_moves INTEGER,
    spread_movements INTEGER,
    spread_significant_moves INTEGER,
    total_movements INTEGER,
    total_significant_moves INTEGER
) AS $$
BEGIN
    RETURN QUERY
    WITH movement_analysis AS (
        SELECT 
            game_id,
            LAG(moneyline_home_american) OVER (ORDER BY entry_index) as prev_ml_home,
            LAG(moneyline_away_american) OVER (ORDER BY entry_index) as prev_ml_away,
            LAG(spread_line) OVER (ORDER BY entry_index) as prev_spread_line,
            LAG(spread_home_american) OVER (ORDER BY entry_index) as prev_spread_home,
            LAG(total_line) OVER (ORDER BY entry_index) as prev_total_line,
            LAG(total_over_american) OVER (ORDER BY entry_index) as prev_total_over,
            
            moneyline_home_american,
            moneyline_away_american,
            spread_line,
            spread_home_american,
            total_line,
            total_over_american
        FROM action_network.historical_line_movement
        WHERE game_id = p_game_id
        ORDER BY entry_index
    ),
    movement_counts AS (
        SELECT 
            -- Moneyline movements
            SUM(CASE 
                WHEN prev_ml_home IS NOT NULL AND 
                     (moneyline_home_american != prev_ml_home OR moneyline_away_american != prev_ml_away)
                THEN 1 ELSE 0 
            END) as ml_moves,
            
            SUM(CASE 
                WHEN prev_ml_home IS NOT NULL AND 
                     (ABS(moneyline_home_american - prev_ml_home) >= 10 OR 
                      ABS(moneyline_away_american - prev_ml_away) >= 10)
                THEN 1 ELSE 0 
            END) as ml_significant,
            
            -- Spread movements
            SUM(CASE 
                WHEN prev_spread_line IS NOT NULL AND 
                     (spread_line != prev_spread_line OR spread_home_american != prev_spread_home)
                THEN 1 ELSE 0 
            END) as spread_moves,
            
            SUM(CASE 
                WHEN prev_spread_line IS NOT NULL AND 
                     (ABS(spread_line - prev_spread_line) >= 0.5 OR 
                      ABS(spread_home_american - prev_spread_home) >= 10)
                THEN 1 ELSE 0 
            END) as spread_significant,
            
            -- Total movements
            SUM(CASE 
                WHEN prev_total_line IS NOT NULL AND 
                     (total_line != prev_total_line OR total_over_american != prev_total_over)
                THEN 1 ELSE 0 
            END) as total_moves,
            
            SUM(CASE 
                WHEN prev_total_line IS NOT NULL AND 
                     (ABS(total_line - prev_total_line) >= 0.5 OR 
                      ABS(total_over_american - prev_total_over) >= 10)
                THEN 1 ELSE 0 
            END) as total_significant
        FROM movement_analysis
    )
    SELECT 
        ml_moves::INTEGER,
        ml_significant::INTEGER,
        spread_moves::INTEGER,
        spread_significant::INTEGER,
        total_moves::INTEGER,
        total_significant::INTEGER
    FROM movement_counts;
END;
$$ LANGUAGE plpgsql;

-- Trigger to update game summary when historical data is inserted
CREATE OR REPLACE FUNCTION action_network.update_game_summary()
RETURNS TRIGGER AS $$
BEGIN
    -- Insert or update game summary
    INSERT INTO action_network.game_summary (
        game_id, home_team, away_team, game_datetime, history_url,
        total_entries, pregame_entries, live_entries
    )
    SELECT 
        NEW.game_id,
        NEW.home_team,
        NEW.away_team,
        NEW.game_datetime,
        NEW.history_url,
        COUNT(*),
        SUM(CASE WHEN period = 'pregame' THEN 1 ELSE 0 END),
        SUM(CASE WHEN period = 'live' THEN 1 ELSE 0 END)
    FROM action_network.historical_line_movement
    WHERE game_id = NEW.game_id
    GROUP BY game_id, home_team, away_team, game_datetime, history_url
    
    ON CONFLICT (game_id) DO UPDATE SET
        total_entries = EXCLUDED.total_entries,
        pregame_entries = EXCLUDED.pregame_entries,
        live_entries = EXCLUDED.live_entries,
        extracted_at = NOW();
    
    -- Update line movement summary
    UPDATE action_network.game_summary gs
    SET 
        (moneyline_movements, moneyline_significant_moves, 
         spread_movements, spread_significant_moves,
         total_movements, total_significant_moves) = 
        (SELECT * FROM action_network.calculate_line_movement_summary(NEW.game_id))
    WHERE gs.game_id = NEW.game_id;
    
    -- Update opening and closing lines
    WITH first_entry AS (
        SELECT * FROM action_network.historical_line_movement
        WHERE game_id = NEW.game_id
        ORDER BY entry_index ASC
        LIMIT 1
    ),
    last_pregame_entry AS (
        SELECT * FROM action_network.historical_line_movement
        WHERE game_id = NEW.game_id AND period = 'pregame'
        ORDER BY entry_index DESC
        LIMIT 1
    )
    UPDATE action_network.game_summary gs
    SET 
        opening_moneyline_home = fe.moneyline_home_american,
        opening_moneyline_away = fe.moneyline_away_american,
        opening_spread_line = fe.spread_line,
        opening_spread_home = fe.spread_home_american,
        opening_spread_away = fe.spread_away_american,
        opening_total_line = fe.total_line,
        opening_total_over = fe.total_over_american,
        opening_total_under = fe.total_under_american,
        
        closing_moneyline_home = lpe.moneyline_home_american,
        closing_moneyline_away = lpe.moneyline_away_american,
        closing_spread_line = lpe.spread_line,
        closing_spread_home = lpe.spread_home_american,
        closing_spread_away = lpe.spread_away_american,
        closing_total_line = lpe.total_line,
        closing_total_over = lpe.total_over_american,
        closing_total_under = lpe.total_under_american
    FROM first_entry fe, last_pregame_entry lpe
    WHERE gs.game_id = NEW.game_id;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger
DROP TRIGGER IF EXISTS trigger_update_game_summary ON action_network.historical_line_movement;
CREATE TRIGGER trigger_update_game_summary
    AFTER INSERT ON action_network.historical_line_movement
    FOR EACH ROW
    EXECUTE FUNCTION action_network.update_game_summary();

-- Grant permissions (adjust as needed for your setup)
GRANT USAGE ON SCHEMA action_network TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA action_network TO PUBLIC;
GRANT INSERT, UPDATE ON action_network.historical_line_movement TO PUBLIC;
GRANT INSERT, UPDATE ON action_network.game_summary TO PUBLIC;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA action_network TO PUBLIC;

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_historical_movement_composite 
ON action_network.historical_line_movement (game_id, entry_index, period);

CREATE INDEX IF NOT EXISTS idx_historical_movement_lines 
ON action_network.historical_line_movement (spread_line, total_line);

CREATE INDEX IF NOT EXISTS idx_historical_movement_odds 
ON action_network.historical_line_movement (moneyline_home_american, moneyline_away_american);

-- Comments for documentation
COMMENT ON SCHEMA action_network IS 'Schema for Action Network historical line movement data';

COMMENT ON TABLE action_network.historical_line_movement IS 
'Stores detailed historical line movement data for each game entry from Action Network';

COMMENT ON TABLE action_network.game_summary IS 
'Aggregated summary data for each game including line movement statistics';

COMMENT ON VIEW action_network.line_movement_analysis IS 
'Analysis view providing line movement insights and percentages';

COMMENT ON FUNCTION action_network.calculate_line_movement_summary(INTEGER) IS 
'Calculates line movement summary statistics for a specific game';

COMMENT ON FUNCTION action_network.update_game_summary() IS 
'Trigger function to automatically update game summary when historical data is inserted'; 