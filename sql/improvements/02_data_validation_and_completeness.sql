-- Betting Lines Data Quality Improvement: Phase 2
-- Data Validation Triggers and Completeness Scoring
-- Addresses data quality validation and tracking improvements

-- Add data completeness score columns to all betting lines tables
ALTER TABLE core_betting.betting_lines_moneyline 
ADD COLUMN IF NOT EXISTS data_completeness_score DECIMAL(3,2) DEFAULT 0.0;

ALTER TABLE core_betting.betting_lines_spreads 
ADD COLUMN IF NOT EXISTS data_completeness_score DECIMAL(3,2) DEFAULT 0.0;

ALTER TABLE core_betting.betting_lines_totals 
ADD COLUMN IF NOT EXISTS data_completeness_score DECIMAL(3,2) DEFAULT 0.0;

-- Create comprehensive data validation function
CREATE OR REPLACE FUNCTION core_betting.validate_and_score_betting_lines_data()
RETURNS TRIGGER AS $$
DECLARE
    total_fields INTEGER := 0;
    filled_fields INTEGER := 0;
    completeness_score DECIMAL(3,2);
BEGIN
    -- Resolve sportsbook_id if null but sportsbook name provided
    IF NEW.sportsbook_id IS NULL AND NEW.sportsbook IS NOT NULL THEN
        NEW.sportsbook_id := core_betting.resolve_sportsbook_id(
            NEW.sportsbook,
            NEW.sportsbook,
            NEW.source
        );
    END IF;
    
    -- Calculate data completeness score based on table type
    IF TG_TABLE_NAME = 'betting_lines_moneyline' THEN
        -- Key fields for moneyline completeness
        total_fields := 7;
        filled_fields := 0;
        
        IF NEW.sportsbook_id IS NOT NULL THEN filled_fields := filled_fields + 1; END IF;
        IF NEW.home_ml IS NOT NULL THEN filled_fields := filled_fields + 1; END IF;
        IF NEW.away_ml IS NOT NULL THEN filled_fields := filled_fields + 1; END IF;
        IF NEW.home_bets_percentage IS NOT NULL THEN filled_fields := filled_fields + 1; END IF;
        IF NEW.away_bets_percentage IS NOT NULL THEN filled_fields := filled_fields + 1; END IF;
        IF NEW.home_money_percentage IS NOT NULL THEN filled_fields := filled_fields + 1; END IF;
        IF NEW.away_money_percentage IS NOT NULL THEN filled_fields := filled_fields + 1; END IF;
        
    ELSIF TG_TABLE_NAME = 'betting_lines_spreads' THEN
        -- Key fields for spreads completeness
        total_fields := 8;
        filled_fields := 0;
        
        IF NEW.sportsbook_id IS NOT NULL THEN filled_fields := filled_fields + 1; END IF;
        IF NEW.home_spread IS NOT NULL THEN filled_fields := filled_fields + 1; END IF;
        IF NEW.away_spread IS NOT NULL THEN filled_fields := filled_fields + 1; END IF;
        IF NEW.home_spread_odds IS NOT NULL THEN filled_fields := filled_fields + 1; END IF;
        IF NEW.away_spread_odds IS NOT NULL THEN filled_fields := filled_fields + 1; END IF;
        IF NEW.home_bets_percentage IS NOT NULL THEN filled_fields := filled_fields + 1; END IF;
        IF NEW.away_bets_percentage IS NOT NULL THEN filled_fields := filled_fields + 1; END IF;
        IF NEW.sharp_action IS NOT NULL THEN filled_fields := filled_fields + 1; END IF;
        
    ELSIF TG_TABLE_NAME = 'betting_lines_totals' THEN
        -- Key fields for totals completeness
        total_fields := 7;
        filled_fields := 0;
        
        IF NEW.sportsbook_id IS NOT NULL THEN filled_fields := filled_fields + 1; END IF;
        IF NEW.total_line IS NOT NULL THEN filled_fields := filled_fields + 1; END IF;
        IF NEW.over_odds IS NOT NULL THEN filled_fields := filled_fields + 1; END IF;
        IF NEW.under_odds IS NOT NULL THEN filled_fields := filled_fields + 1; END IF;
        IF NEW.over_bets_percentage IS NOT NULL THEN filled_fields := filled_fields + 1; END IF;
        IF NEW.under_bets_percentage IS NOT NULL THEN filled_fields := filled_fields + 1; END IF;
        IF NEW.sharp_action IS NOT NULL THEN filled_fields := filled_fields + 1; END IF;
    END IF;
    
    -- Calculate completeness score
    completeness_score := CASE 
        WHEN total_fields > 0 THEN ROUND((filled_fields::DECIMAL / total_fields::DECIMAL), 2)
        ELSE 0.0
    END;
    
    NEW.data_completeness_score := completeness_score;
    
    -- Set data quality based on completeness and sportsbook ID availability
    NEW.data_quality := CASE 
        WHEN NEW.sportsbook_id IS NOT NULL AND completeness_score >= 0.70 THEN 'HIGH'
        WHEN NEW.sportsbook_id IS NOT NULL AND completeness_score >= 0.40 THEN 'MEDIUM'
        WHEN NEW.sportsbook_id IS NOT NULL THEN 'LOW'
        WHEN completeness_score >= 0.60 THEN 'MEDIUM'  -- High completeness but no sportsbook ID
        ELSE 'LOW'
    END;
    
    -- Validate critical fields based on table type
    IF TG_TABLE_NAME = 'betting_lines_moneyline' THEN
        -- Validate moneyline odds
        IF NEW.home_ml IS NOT NULL AND (NEW.home_ml < -10000 OR NEW.home_ml > 10000) THEN
            RAISE WARNING 'Moneyline odds out of reasonable range: home_ml=%', NEW.home_ml;
        END IF;
        IF NEW.away_ml IS NOT NULL AND (NEW.away_ml < -10000 OR NEW.away_ml > 10000) THEN
            RAISE WARNING 'Moneyline odds out of reasonable range: away_ml=%', NEW.away_ml;
        END IF;
        
    ELSIF TG_TABLE_NAME = 'betting_lines_spreads' THEN
        -- Validate spread values
        IF NEW.home_spread IS NOT NULL AND (NEW.home_spread < -50 OR NEW.home_spread > 50) THEN
            RAISE WARNING 'Spread value out of reasonable range: home_spread=%', NEW.home_spread;
        END IF;
        
    ELSIF TG_TABLE_NAME = 'betting_lines_totals' THEN
        -- Validate total values
        IF NEW.total_line IS NOT NULL AND (NEW.total_line < 0 OR NEW.total_line > 50) THEN
            RAISE WARNING 'Total line out of reasonable range: total_line=%', NEW.total_line;
        END IF;
    END IF;
    
    -- Validate percentage fields (should be between 0 and 100)
    IF NEW.home_bets_percentage IS NOT NULL AND (NEW.home_bets_percentage < 0 OR NEW.home_bets_percentage > 100) THEN
        RAISE WARNING 'Betting percentage out of range: home_bets_percentage=%', NEW.home_bets_percentage;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply the enhanced validation trigger to all betting lines tables
DROP TRIGGER IF EXISTS validate_moneyline_data_enhanced ON core_betting.betting_lines_moneyline;
CREATE TRIGGER validate_moneyline_data_enhanced
    BEFORE INSERT OR UPDATE ON core_betting.betting_lines_moneyline
    FOR EACH ROW EXECUTE FUNCTION core_betting.validate_and_score_betting_lines_data();

DROP TRIGGER IF EXISTS validate_spreads_data_enhanced ON core_betting.betting_lines_spreads;
CREATE TRIGGER validate_spreads_data_enhanced
    BEFORE INSERT OR UPDATE ON core_betting.betting_lines_spreads
    FOR EACH ROW EXECUTE FUNCTION core_betting.validate_and_score_betting_lines_data();

DROP TRIGGER IF EXISTS validate_totals_data_enhanced ON core_betting.betting_lines_totals;
CREATE TRIGGER validate_totals_data_enhanced
    BEFORE INSERT OR UPDATE ON core_betting.betting_lines_totals
    FOR EACH ROW EXECUTE FUNCTION core_betting.validate_and_score_betting_lines_data();

-- Create comprehensive data quality monitoring views
CREATE OR REPLACE VIEW core_betting.data_quality_dashboard AS
SELECT 
    'moneyline' as table_name,
    COUNT(*) as total_rows,
    COUNT(sportsbook_id) as mapped_sportsbooks,
    ROUND(AVG(CASE WHEN sportsbook_id IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100, 2) as sportsbook_id_pct,
    ROUND(AVG(CASE WHEN sharp_action IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100, 2) as sharp_action_pct,
    ROUND(AVG(CASE WHEN home_bets_percentage IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100, 2) as betting_pct_pct,
    ROUND(AVG(CASE WHEN home_money_percentage IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100, 2) as money_pct_pct,
    ROUND(AVG(data_completeness_score), 3) as avg_completeness,
    COUNT(CASE WHEN data_quality = 'HIGH' THEN 1 END) as high_quality_count,
    COUNT(CASE WHEN data_quality = 'MEDIUM' THEN 1 END) as medium_quality_count,
    COUNT(CASE WHEN data_quality = 'LOW' THEN 1 END) as low_quality_count
FROM core_betting.betting_lines_moneyline
UNION ALL
SELECT 
    'spreads' as table_name,
    COUNT(*) as total_rows,
    COUNT(sportsbook_id) as mapped_sportsbooks,
    ROUND(AVG(CASE WHEN sportsbook_id IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100, 2) as sportsbook_id_pct,
    ROUND(AVG(CASE WHEN sharp_action IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100, 2) as sharp_action_pct,
    ROUND(AVG(CASE WHEN home_bets_percentage IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100, 2) as betting_pct_pct,
    ROUND(AVG(CASE WHEN home_money_percentage IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100, 2) as money_pct_pct,
    ROUND(AVG(data_completeness_score), 3) as avg_completeness,
    COUNT(CASE WHEN data_quality = 'HIGH' THEN 1 END) as high_quality_count,
    COUNT(CASE WHEN data_quality = 'MEDIUM' THEN 1 END) as medium_quality_count,
    COUNT(CASE WHEN data_quality = 'LOW' THEN 1 END) as low_quality_count
FROM core_betting.betting_lines_spreads
UNION ALL
SELECT 
    'totals' as table_name,
    COUNT(*) as total_rows,
    COUNT(sportsbook_id) as mapped_sportsbooks,
    ROUND(AVG(CASE WHEN sportsbook_id IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100, 2) as sportsbook_id_pct,
    ROUND(AVG(CASE WHEN sharp_action IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100, 2) as sharp_action_pct,
    ROUND(AVG(CASE WHEN over_bets_percentage IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100, 2) as betting_pct_pct,
    ROUND(AVG(CASE WHEN over_money_percentage IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100, 2) as money_pct_pct,
    ROUND(AVG(data_completeness_score), 3) as avg_completeness,
    COUNT(CASE WHEN data_quality = 'HIGH' THEN 1 END) as high_quality_count,
    COUNT(CASE WHEN data_quality = 'MEDIUM' THEN 1 END) as medium_quality_count,
    COUNT(CASE WHEN data_quality = 'LOW' THEN 1 END) as low_quality_count
FROM core_betting.betting_lines_totals;

-- Create quality trend monitoring view
CREATE OR REPLACE VIEW core_betting.data_quality_trend AS
SELECT 
    table_name,
    date_trunc('day', created_at) as quality_date,
    COUNT(*) as daily_records,
    ROUND(AVG(data_completeness_score), 3) as avg_completeness,
    ROUND(AVG(CASE WHEN sportsbook_id IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100, 2) as sportsbook_mapping_pct,
    COUNT(CASE WHEN data_quality = 'HIGH' THEN 1 END) as high_quality_count,
    ROUND(COUNT(CASE WHEN data_quality = 'HIGH' THEN 1 END) * 100.0 / COUNT(*), 2) as high_quality_pct
FROM (
    SELECT 'moneyline' as table_name, created_at, data_completeness_score, sportsbook_id, data_quality
    FROM core_betting.betting_lines_moneyline
    WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
    UNION ALL
    SELECT 'spreads' as table_name, created_at, data_completeness_score, sportsbook_id, data_quality
    FROM core_betting.betting_lines_spreads
    WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
    UNION ALL
    SELECT 'totals' as table_name, created_at, data_completeness_score, sportsbook_id, data_quality
    FROM core_betting.betting_lines_totals
    WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
) combined_data
GROUP BY table_name, date_trunc('day', created_at)
ORDER BY quality_date DESC, table_name;

-- Create helper view to identify problematic data sources
CREATE OR REPLACE VIEW core_betting.data_source_quality_analysis AS
SELECT 
    source,
    COUNT(*) as total_records,
    ROUND(AVG(CASE WHEN sportsbook_id IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100, 2) as sportsbook_mapping_success_pct,
    ROUND(AVG(data_completeness_score), 3) as avg_completeness,
    COUNT(DISTINCT sportsbook) as distinct_sportsbooks_found,
    COUNT(DISTINCT CASE WHEN sportsbook_id IS NOT NULL THEN sportsbook_id END) as distinct_sportsbooks_mapped,
    MIN(created_at) as first_record,
    MAX(created_at) as latest_record
FROM (
    SELECT source, sportsbook_id, sportsbook, data_completeness_score, created_at
    FROM core_betting.betting_lines_moneyline
    UNION ALL
    SELECT source, sportsbook_id, sportsbook, data_completeness_score, created_at
    FROM core_betting.betting_lines_spreads
    UNION ALL
    SELECT source, sportsbook_id, sportsbook, data_completeness_score, created_at
    FROM core_betting.betting_lines_totals
) combined_sources
GROUP BY source
ORDER BY avg_completeness DESC;

-- Create indexes for performance on new columns
CREATE INDEX IF NOT EXISTS idx_betting_lines_moneyline_completeness 
ON core_betting.betting_lines_moneyline(data_completeness_score);

CREATE INDEX IF NOT EXISTS idx_betting_lines_spreads_completeness 
ON core_betting.betting_lines_spreads(data_completeness_score);

CREATE INDEX IF NOT EXISTS idx_betting_lines_totals_completeness 
ON core_betting.betting_lines_totals(data_completeness_score);

-- Create composite indexes for quality monitoring
CREATE INDEX IF NOT EXISTS idx_betting_lines_moneyline_quality_date 
ON core_betting.betting_lines_moneyline(data_quality, created_at);

CREATE INDEX IF NOT EXISTS idx_betting_lines_spreads_quality_date 
ON core_betting.betting_lines_spreads(data_quality, created_at);

CREATE INDEX IF NOT EXISTS idx_betting_lines_totals_quality_date 
ON core_betting.betting_lines_totals(data_quality, created_at);

-- Update existing records with completeness scores (run this as a separate maintenance task)
-- This should be run carefully on existing data
-- UPDATE core_betting.betting_lines_moneyline SET updated_at = updated_at WHERE id > 0;
-- UPDATE core_betting.betting_lines_spreads SET updated_at = updated_at WHERE id > 0;
-- UPDATE core_betting.betting_lines_totals SET updated_at = updated_at WHERE id > 0;