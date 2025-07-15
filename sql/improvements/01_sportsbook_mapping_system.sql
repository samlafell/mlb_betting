-- Betting Lines Data Quality Improvement: Phase 1
-- Sportsbook External Mapping System
-- Resolves the critical sportsbook_id null issue (99-100% null values)

-- Create external mapping table for sportsbook identifiers
CREATE TABLE IF NOT EXISTS core_betting.sportsbook_external_mappings (
    id SERIAL PRIMARY KEY,
    sportsbook_id INTEGER NOT NULL REFERENCES core_betting.sportsbooks(id) ON DELETE CASCADE,
    external_source VARCHAR(50) NOT NULL,
    external_id VARCHAR(100) NOT NULL,
    external_name VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(external_source, external_id)
);

-- Add indexes for performance
CREATE INDEX IF NOT EXISTS idx_sportsbook_external_mappings_source_id 
ON core_betting.sportsbook_external_mappings(external_source, external_id);

CREATE INDEX IF NOT EXISTS idx_sportsbook_external_mappings_name 
ON core_betting.sportsbook_external_mappings(external_source, external_name);

-- First, let's check what sportsbooks exist in the system
-- This will help us map the Action Network IDs correctly

-- Common Action Network sportsbook mappings based on typical IDs
-- Note: These mappings should be verified against actual Action Network data
INSERT INTO core_betting.sportsbook_external_mappings 
(sportsbook_id, external_source, external_id, external_name) 
SELECT 
    s.id,
    'ACTION_NETWORK',
    CASE s.name
        WHEN 'DraftKings' THEN '15'
        WHEN 'FanDuel' THEN '30' 
        WHEN 'BetMGM' THEN '68'
        WHEN 'Caesars' THEN '79'
        WHEN 'BetRivers' THEN '83'
        WHEN 'PointsBet' THEN '84'
        WHEN 'WynnBET' THEN '85'
        WHEN 'Unibet' THEN '86'
        WHEN 'FOX Bet' THEN '87'
        WHEN 'Barstool' THEN '88'
    END,
    s.name
FROM core_betting.sportsbooks s
WHERE s.name IN ('DraftKings', 'FanDuel', 'BetMGM', 'Caesars', 'BetRivers', 
                 'PointsBet', 'WynnBET', 'Unibet', 'FOX Bet', 'Barstool')
ON CONFLICT (external_source, external_id) DO NOTHING;

-- Add SportsbookReview mappings (using sportsbook names as IDs)
INSERT INTO core_betting.sportsbook_external_mappings 
(sportsbook_id, external_source, external_id, external_name)
SELECT 
    s.id,
    'SPORTSBOOKREVIEW',
    LOWER(REPLACE(s.name, ' ', '_')),
    s.name
FROM core_betting.sportsbooks s
ON CONFLICT (external_source, external_id) DO NOTHING;

-- Create function to resolve sportsbook ID from external identifiers
CREATE OR REPLACE FUNCTION core_betting.resolve_sportsbook_id(
    p_external_id TEXT,
    p_external_name TEXT DEFAULT NULL,
    p_source TEXT DEFAULT 'ACTION_NETWORK'
) RETURNS INTEGER AS $$
DECLARE
    v_sportsbook_id INTEGER;
BEGIN
    -- First try exact external_id match
    SELECT sportsbook_id INTO v_sportsbook_id
    FROM core_betting.sportsbook_external_mappings
    WHERE external_source = p_source 
    AND external_id = p_external_id;
    
    -- If not found and external_name provided, try name match
    IF v_sportsbook_id IS NULL AND p_external_name IS NOT NULL THEN
        SELECT sportsbook_id INTO v_sportsbook_id
        FROM core_betting.sportsbook_external_mappings
        WHERE external_source = p_source 
        AND external_name = p_external_name;
    END IF;
    
    -- If still not found and external_name provided, try direct sportsbook name match
    IF v_sportsbook_id IS NULL AND p_external_name IS NOT NULL THEN
        SELECT id INTO v_sportsbook_id
        FROM core_betting.sportsbooks
        WHERE name ILIKE p_external_name;
    END IF;
    
    RETURN v_sportsbook_id;
END;
$$ LANGUAGE plpgsql;

-- Create trigger function for automatic sportsbook ID resolution
CREATE OR REPLACE FUNCTION core_betting.auto_resolve_sportsbook_id()
RETURNS TRIGGER AS $$
BEGIN
    -- Only resolve if sportsbook_id is null but we have identifying information
    IF NEW.sportsbook_id IS NULL THEN
        -- Try to resolve using external identifier patterns
        IF NEW.source IS NOT NULL THEN
            -- For Action Network, look for sportsbook field or external patterns
            IF NEW.source = 'ACTION_NETWORK' AND NEW.sportsbook IS NOT NULL THEN
                NEW.sportsbook_id := core_betting.resolve_sportsbook_id(
                    NEW.sportsbook, -- Use sportsbook field as external_id
                    NEW.sportsbook, -- Also try as name
                    'ACTION_NETWORK'
                );
            END IF;
            
            -- For SportsbookReview, use sportsbook name
            IF NEW.source = 'SPORTSBOOKREVIEW' AND NEW.sportsbook IS NOT NULL THEN
                NEW.sportsbook_id := core_betting.resolve_sportsbook_id(
                    LOWER(REPLACE(NEW.sportsbook, ' ', '_')),
                    NEW.sportsbook,
                    'SPORTSBOOKREVIEW'
                );
            END IF;
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply the trigger to all betting lines tables
DROP TRIGGER IF EXISTS auto_resolve_sportsbook_moneyline ON core_betting.betting_lines_moneyline;
CREATE TRIGGER auto_resolve_sportsbook_moneyline
    BEFORE INSERT OR UPDATE ON core_betting.betting_lines_moneyline
    FOR EACH ROW EXECUTE FUNCTION core_betting.auto_resolve_sportsbook_id();

DROP TRIGGER IF EXISTS auto_resolve_sportsbook_spreads ON core_betting.betting_lines_spreads;
CREATE TRIGGER auto_resolve_sportsbook_spreads
    BEFORE INSERT OR UPDATE ON core_betting.betting_lines_spreads
    FOR EACH ROW EXECUTE FUNCTION core_betting.auto_resolve_sportsbook_id();

DROP TRIGGER IF EXISTS auto_resolve_sportsbook_totals ON core_betting.betting_lines_totals;
CREATE TRIGGER auto_resolve_sportsbook_totals
    BEFORE INSERT OR UPDATE ON core_betting.betting_lines_totals
    FOR EACH ROW EXECUTE FUNCTION core_betting.auto_resolve_sportsbook_id();

-- Create a view to check mapping effectiveness
CREATE OR REPLACE VIEW core_betting.sportsbook_mapping_status AS
SELECT 
    'moneyline' as table_name,
    COUNT(*) as total_rows,
    COUNT(sportsbook_id) as mapped_rows,
    ROUND(COUNT(sportsbook_id) * 100.0 / COUNT(*), 2) as mapping_percentage,
    COUNT(DISTINCT sportsbook_id) as unique_sportsbooks,
    COUNT(DISTINCT source) as data_sources
FROM core_betting.betting_lines_moneyline
UNION ALL
SELECT 
    'spreads' as table_name,
    COUNT(*) as total_rows,
    COUNT(sportsbook_id) as mapped_rows,
    ROUND(COUNT(sportsbook_id) * 100.0 / COUNT(*), 2) as mapping_percentage,
    COUNT(DISTINCT sportsbook_id) as unique_sportsbooks,
    COUNT(DISTINCT source) as data_sources
FROM core_betting.betting_lines_spreads
UNION ALL
SELECT 
    'totals' as table_name,
    COUNT(*) as total_rows,
    COUNT(sportsbook_id) as mapped_rows,
    ROUND(COUNT(sportsbook_id) * 100.0 / COUNT(*), 2) as mapping_percentage,
    COUNT(DISTINCT sportsbook_id) as unique_sportsbooks,
    COUNT(DISTINCT source) as data_sources
FROM core_betting.betting_lines_totals;

-- Create helper view to see unmapped sportsbook names
CREATE OR REPLACE VIEW core_betting.unmapped_sportsbook_analysis AS
SELECT 
    source,
    sportsbook,
    COUNT(*) as occurrence_count,
    MIN(created_at) as first_seen,
    MAX(created_at) as last_seen
FROM (
    SELECT source, sportsbook, created_at FROM core_betting.betting_lines_moneyline WHERE sportsbook_id IS NULL
    UNION ALL
    SELECT source, sportsbook, created_at FROM core_betting.betting_lines_spreads WHERE sportsbook_id IS NULL
    UNION ALL
    SELECT source, sportsbook, created_at FROM core_betting.betting_lines_totals WHERE sportsbook_id IS NULL
) unmapped
WHERE sportsbook IS NOT NULL
GROUP BY source, sportsbook
ORDER BY occurrence_count DESC;

-- Add updated_at trigger for the mapping table
CREATE OR REPLACE FUNCTION core_betting.update_mapping_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_sportsbook_mapping_timestamp
    BEFORE UPDATE ON core_betting.sportsbook_external_mappings
    FOR EACH ROW EXECUTE FUNCTION core_betting.update_mapping_timestamp();