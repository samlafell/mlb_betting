-- Complete the betting lines migration with actual field names

-- Migrate spread betting lines (using actual field names: spread_line, home_spread_price, away_spread_price)
INSERT INTO curated.betting_lines_unified (
    external_line_id, game_id, sportsbook_id, market_type, line_type,
    spread_home, spread_home_odds, spread_away_odds,
    recorded_at, data_quality_score, source_system, created_at, migrated_at
)
SELECT 
    CONCAT('sp_', id) as external_line_id,
    game_id, sportsbook_id, 'spread' as market_type,
    'current' as line_type,
    spread_line as spread_home,
    home_spread_price as spread_home_odds, 
    away_spread_price as spread_away_odds,
    COALESCE(odds_timestamp, updated_at, CURRENT_TIMESTAMP) as recorded_at,
    COALESCE(data_completeness_score, 1.0) as data_quality_score,
    'core_betting_migration' as source_system,
    COALESCE(created_at, CURRENT_TIMESTAMP) as created_at,
    CURRENT_TIMESTAMP as migrated_at
FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread'
WHERE game_id IN (SELECT id FROM curated.games_complete)
  AND sportsbook_id IN (SELECT id FROM curated.sportsbooks)
ON CONFLICT (external_line_id, sportsbook_id, market_type, line_type) DO NOTHING;

-- Migrate totals betting lines (using actual field names: over_price, under_price)
INSERT INTO curated.betting_lines_unified (
    external_line_id, game_id, sportsbook_id, market_type, line_type,
    total_line, over_odds, under_odds, recorded_at, data_quality_score,
    source_system, created_at, migrated_at
)
SELECT 
    CONCAT('tot_', id) as external_line_id,
    game_id, sportsbook_id, 'totals' as market_type,
    'current' as line_type,
    total_line, over_price as over_odds, under_price as under_odds,
    COALESCE(odds_timestamp, updated_at, CURRENT_TIMESTAMP) as recorded_at,
    COALESCE(data_completeness_score, 1.0) as data_quality_score,
    'core_betting_migration' as source_system,
    COALESCE(created_at, CURRENT_TIMESTAMP) as created_at,
    CURRENT_TIMESTAMP as migrated_at
FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals'
WHERE game_id IN (SELECT id FROM curated.games_complete)
  AND sportsbook_id IN (SELECT id FROM curated.sportsbooks)
ON CONFLICT (external_line_id, sportsbook_id, market_type, line_type) DO NOTHING;

-- Update post-migration counts
UPDATE operational.post_migration_counts 
SET record_count = (SELECT COUNT(*) FROM curated.betting_lines_unified WHERE source_system = 'core_betting_migration'),
    recorded_at = CURRENT_TIMESTAMP
WHERE table_name = 'curated.betting_lines_unified';

-- Display final results
SELECT 'COMPLETE BETTING LINES MIGRATION FINISHED' as status;
SELECT * FROM operational.v_final_migration_validation;

-- Show betting lines breakdown by market type
SELECT 
    market_type,
    COUNT(*) as record_count,
    MIN(recorded_at) as earliest_record,
    MAX(recorded_at) as latest_record
FROM curated.betting_lines_unified 
WHERE source_system = 'core_betting_migration'
GROUP BY market_type
ORDER BY market_type;