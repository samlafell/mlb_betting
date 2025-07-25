-- Filter Placeholder/Test Games from Staging Data
-- Purpose: Remove test/placeholder game records that lack proper game information
-- Date: 2025-07-25

-- ================================
-- Phase 1: Create View to Identify Valid Games
-- ================================

CREATE OR REPLACE VIEW staging.v_valid_games AS
SELECT 
    s.external_game_id,
    g.away_team,
    g.home_team,
    g.start_time,
    g.game_status,
    g.raw_response->>'table' as table_marker,
    g.raw_response->>'real_data' as real_data_marker,
    CASE 
        WHEN g.away_team IS NOT NULL 
             AND g.home_team IS NOT NULL 
             AND g.raw_response->>'table' != 'action_network_history'
        THEN 'valid'
        ELSE 'placeholder'
    END as game_type,
    COUNT(*) as staging_records_count
FROM staging.action_network_odds_historical s
LEFT JOIN raw_data.action_network_games g ON s.external_game_id = g.external_game_id
GROUP BY 
    s.external_game_id, g.away_team, g.home_team, g.start_time, 
    g.game_status, g.raw_response->>'table', g.raw_response->>'real_data'
ORDER BY game_type, s.external_game_id;

-- ================================
-- Phase 2: Create Cleanup Summary Function
-- ================================

CREATE OR REPLACE FUNCTION staging.analyze_placeholder_cleanup()
RETURNS TABLE (
    game_type TEXT,
    unique_games BIGINT,
    total_staging_records BIGINT,
    avg_records_per_game NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        v.game_type,
        COUNT(DISTINCT v.external_game_id) as unique_games,
        SUM(v.staging_records_count) as total_staging_records,
        ROUND(AVG(v.staging_records_count), 1) as avg_records_per_game
    FROM staging.v_valid_games v
    GROUP BY v.game_type
    ORDER BY v.game_type;
END;
$$ LANGUAGE plpgsql;

-- ================================
-- Phase 3: Create Safe Cleanup Function
-- ================================

CREATE OR REPLACE FUNCTION staging.cleanup_placeholder_games(dry_run BOOLEAN DEFAULT TRUE)
RETURNS TABLE (
    action_type TEXT,
    affected_records BIGINT,
    affected_games TEXT[]
) AS $$
DECLARE
    placeholder_game_ids TEXT[];
    deleted_count BIGINT;
BEGIN
    -- Get list of placeholder game IDs
    SELECT ARRAY_AGG(DISTINCT external_game_id)
    INTO placeholder_game_ids
    FROM staging.v_valid_games
    WHERE game_type = 'placeholder';
    
    IF dry_run THEN
        -- Dry run: just return what would be deleted
        SELECT COUNT(*) INTO deleted_count
        FROM staging.action_network_odds_historical
        WHERE external_game_id = ANY(placeholder_game_ids);
        
        RETURN QUERY SELECT 
            'DRY RUN - Would delete'::TEXT,
            deleted_count,
            placeholder_game_ids;
    ELSE
        -- Actually delete the records
        DELETE FROM staging.action_network_odds_historical
        WHERE external_game_id = ANY(placeholder_game_ids);
        
        GET DIAGNOSTICS deleted_count = ROW_COUNT;
        
        RETURN QUERY SELECT 
            'DELETED'::TEXT,
            deleted_count,
            placeholder_game_ids;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- ================================
-- Phase 4: Enhanced Betting Data View (Valid Games Only)
-- ================================

CREATE OR REPLACE VIEW staging.v_bet_money_divergence_valid AS
SELECT 
    h.external_game_id,
    h.sportsbook_name,
    h.market_type,
    h.side,
    h.bet_percent_tickets,
    h.bet_percent_money,
    ABS(COALESCE(h.bet_percent_tickets, 50) - COALESCE(h.bet_percent_money, 50)) as percentage_divergence,
    CASE 
        WHEN h.bet_percent_money > COALESCE(h.bet_percent_tickets, 50) + 15 THEN 'Sharp Money Heavy'
        WHEN h.bet_percent_tickets > COALESCE(h.bet_percent_money, 50) + 15 THEN 'Public Money Heavy'  
        WHEN ABS(COALESCE(h.bet_percent_tickets, 50) - COALESCE(h.bet_percent_money, 50)) <= 5 THEN 'Aligned'
        ELSE 'Moderate Divergence'
    END as sharp_indicator,
    h.updated_at,
    h.bet_info_available,
    v.game_type,
    v.away_team,
    v.home_team,
    v.start_time
FROM staging.action_network_odds_historical h
INNER JOIN staging.v_valid_games v ON h.external_game_id = v.external_game_id
WHERE h.bet_info_available = TRUE 
  AND v.game_type = 'valid'  -- Only include valid games
ORDER BY percentage_divergence DESC;

-- ================================
-- Analysis Queries
-- ================================

-- Check current state
COMMENT ON VIEW staging.v_valid_games IS 
'View to distinguish valid games from test/placeholder records based on game data completeness';

COMMENT ON FUNCTION staging.analyze_placeholder_cleanup() IS 
'Analyze the impact of cleaning up placeholder game records from staging data';

COMMENT ON FUNCTION staging.cleanup_placeholder_games(BOOLEAN) IS 
'Safely clean up placeholder game records. Use dry_run=TRUE to preview changes.';

COMMENT ON VIEW staging.v_bet_money_divergence_valid IS 
'Enhanced betting divergence analysis excluding test/placeholder games';

/*
USAGE EXAMPLES:

-- 1. Analyze current state
SELECT * FROM staging.analyze_placeholder_cleanup();

-- 2. Preview cleanup (dry run)
SELECT * FROM staging.cleanup_placeholder_games(TRUE);

-- 3. Execute cleanup (CAUTION: This deletes data!)
-- SELECT * FROM staging.cleanup_placeholder_games(FALSE);

-- 4. View only valid game betting data
SELECT * FROM staging.v_bet_money_divergence_valid 
WHERE percentage_divergence >= 10 
ORDER BY percentage_divergence DESC 
LIMIT 10;

-- 5. Check specific games
SELECT * FROM staging.v_valid_games 
WHERE external_game_id IN ('258062', '258083');
*/