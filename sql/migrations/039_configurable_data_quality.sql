-- Migration: 039_configurable_data_quality.sql
-- Purpose: Make data quality confidence scores configurable
-- Related: Improvements to games_population_service.py security fixes
-- Date: 2025-08-22

-- Description:
-- This migration creates a configuration table for data quality scores and updates
-- the migration process to use configurable values instead of hard-coded ones.
-- Addresses security improvements in games_population_service.py.

BEGIN;

-- Create configuration table for data quality settings
CREATE TABLE IF NOT EXISTS config.data_quality_settings (
    id SERIAL PRIMARY KEY,
    setting_name VARCHAR(100) NOT NULL UNIQUE,
    setting_value DECIMAL(5,4) NOT NULL,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Insert default confidence scores (matching previous hard-coded values)
INSERT INTO config.data_quality_settings (setting_name, setting_value, description) VALUES
    ('high_confidence_score', 0.9500, 'Confidence score for games with complete data (scores + external ID + venue)'),
    ('medium_confidence_score', 0.8000, 'Confidence score for games with scores and external ID but missing venue'),
    ('low_confidence_score', 0.5000, 'Confidence score for games with minimal data'),
    ('batch_size_limit', 1000, 'Maximum number of records to process in a single batch'),
    ('connection_timeout', 180, 'Database connection timeout in seconds')
ON CONFLICT (setting_name) DO UPDATE SET
    setting_value = EXCLUDED.setting_value,
    updated_at = NOW();

-- Create function to get configuration values with defaults
CREATE OR REPLACE FUNCTION config.get_data_quality_setting(
    p_setting_name VARCHAR(100),
    p_default_value DECIMAL(5,4) DEFAULT 0.5000
) RETURNS DECIMAL(5,4) AS $$
DECLARE
    setting_val DECIMAL(5,4);
BEGIN
    SELECT setting_value INTO setting_val
    FROM config.data_quality_settings
    WHERE setting_name = p_setting_name;
    
    -- Return default if setting not found
    IF setting_val IS NULL THEN
        RETURN p_default_value;
    END IF;
    
    RETURN setting_val;
END;
$$ LANGUAGE plpgsql;

-- Create improved data quality update function with configurable scores
CREATE OR REPLACE FUNCTION curated.update_games_data_quality(
    p_max_games INTEGER DEFAULT NULL
) RETURNS INTEGER AS $$
DECLARE
    high_confidence DECIMAL(5,4);
    medium_confidence DECIMAL(5,4);
    low_confidence DECIMAL(5,4);
    updated_count INTEGER;
    limit_clause TEXT := '';
BEGIN
    -- Get configurable confidence scores
    SELECT config.get_data_quality_setting('high_confidence_score', 0.9500) INTO high_confidence;
    SELECT config.get_data_quality_setting('medium_confidence_score', 0.8000) INTO medium_confidence;
    SELECT config.get_data_quality_setting('low_confidence_score', 0.5000) INTO low_confidence;
    
    -- Validate max_games parameter to prevent SQL injection
    IF p_max_games IS NOT NULL THEN
        IF p_max_games <= 0 THEN
            RAISE EXCEPTION 'max_games must be a positive integer, got: %', p_max_games;
        END IF;
        limit_clause := format(' AND id <= %L', p_max_games);
    END IF;
    
    -- Execute the update with parameterized confidence scores
    EXECUTE format('
        UPDATE curated.games_complete
        SET 
            data_quality = CASE 
                WHEN home_score IS NOT NULL 
                 AND away_score IS NOT NULL 
                 AND action_network_game_id IS NOT NULL 
                 AND venue_name IS NOT NULL 
                THEN ''HIGH''
                WHEN home_score IS NOT NULL 
                 AND away_score IS NOT NULL 
                 AND action_network_game_id IS NOT NULL 
                THEN ''MEDIUM''
                WHEN home_score IS NOT NULL 
                 AND away_score IS NOT NULL 
                THEN ''LOW''
                ELSE ''MINIMAL''
            END,
            has_mlb_enrichment = CASE 
                WHEN action_network_game_id IS NOT NULL THEN true
                ELSE false
            END,
            mlb_correlation_confidence = CASE 
                WHEN home_score IS NOT NULL 
                 AND away_score IS NOT NULL 
                 AND action_network_game_id IS NOT NULL 
                THEN %L
                WHEN home_score IS NOT NULL 
                 AND away_score IS NOT NULL 
                THEN %L
                ELSE %L
            END,
            updated_at = NOW()
        WHERE updated_at >= (NOW() - INTERVAL ''1 hour'')%s
    ', high_confidence, medium_confidence, low_confidence, limit_clause);
    
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    
    RETURN updated_count;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Create batch processing function for large datasets
CREATE OR REPLACE FUNCTION curated.batch_update_games_data_quality(
    p_total_limit INTEGER DEFAULT NULL,
    p_batch_size INTEGER DEFAULT NULL
) RETURNS TABLE (
    batch_number INTEGER,
    records_processed INTEGER,
    total_updated INTEGER
) AS $$
DECLARE
    batch_size_setting INTEGER;
    current_batch INTEGER := 1;
    offset_value INTEGER := 0;
    batch_updated INTEGER;
    total_updated_count INTEGER := 0;
    remaining_limit INTEGER;
BEGIN
    -- Get batch size from configuration
    SELECT config.get_data_quality_setting('batch_size_limit', 1000)::INTEGER INTO batch_size_setting;
    
    -- Use provided batch size or default from config
    IF p_batch_size IS NOT NULL THEN
        batch_size_setting := p_batch_size;
    END IF;
    
    -- Validate batch size
    IF batch_size_setting <= 0 OR batch_size_setting > 5000 THEN
        RAISE EXCEPTION 'Invalid batch size: %. Must be between 1 and 5000', batch_size_setting;
    END IF;
    
    -- Initialize remaining limit
    remaining_limit := p_total_limit;
    
    LOOP
        -- Calculate current batch size (respect total limit)
        DECLARE
            current_batch_size INTEGER := batch_size_setting;
        BEGIN
            IF remaining_limit IS NOT NULL AND remaining_limit < batch_size_setting THEN
                current_batch_size := remaining_limit;
            END IF;
            
            -- Exit if no more records to process
            IF current_batch_size <= 0 THEN
                EXIT;
            END IF;
            
            -- Process current batch
            SELECT curated.update_games_data_quality(offset_value + current_batch_size) INTO batch_updated;
            
            -- Update counters
            total_updated_count := total_updated_count + batch_updated;
            
            -- Return batch results
            batch_number := current_batch;
            records_processed := current_batch_size;
            total_updated := batch_updated;
            RETURN NEXT;
            
            -- Update for next iteration
            offset_value := offset_value + current_batch_size;
            current_batch := current_batch + 1;
            
            IF remaining_limit IS NOT NULL THEN
                remaining_limit := remaining_limit - current_batch_size;
            END IF;
            
            -- Exit if no records were updated (end of data)
            IF batch_updated = 0 THEN
                EXIT;
            END IF;
        END;
    END LOOP;
    
    RAISE NOTICE 'Batch processing complete: % batches, % total records updated', current_batch - 1, total_updated_count;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Update the analytics view to show configuration status
CREATE OR REPLACE VIEW analytics.games_complete_data_quality_config AS
SELECT 
    -- Data quality metrics
    COUNT(*) as total_games,
    COUNT(CASE WHEN home_score IS NOT NULL THEN 1 END) as games_with_scores,
    COUNT(CASE WHEN action_network_game_id IS NOT NULL THEN 1 END) as games_with_external_ids,
    COUNT(CASE WHEN venue_name IS NOT NULL THEN 1 END) as games_with_venue,
    COUNT(CASE WHEN weather_condition IS NOT NULL THEN 1 END) as games_with_weather,
    COUNT(CASE WHEN data_quality = 'HIGH' THEN 1 END) as high_quality_games,
    COUNT(CASE WHEN data_quality = 'MEDIUM' THEN 1 END) as medium_quality_games,
    COUNT(CASE WHEN data_quality = 'LOW' THEN 1 END) as low_quality_games,
    
    -- Configuration values currently in use
    config.get_data_quality_setting('high_confidence_score') as high_confidence_score,
    config.get_data_quality_setting('medium_confidence_score') as medium_confidence_score,
    config.get_data_quality_setting('low_confidence_score') as low_confidence_score,
    config.get_data_quality_setting('batch_size_limit') as batch_size_limit,
    
    -- Calculated percentages
    ROUND((COUNT(CASE WHEN home_score IS NOT NULL THEN 1 END)::DECIMAL / COUNT(*)) * 100, 1) as score_completion_pct,
    ROUND((COUNT(CASE WHEN data_quality = 'HIGH' THEN 1 END)::DECIMAL / COUNT(*)) * 100, 1) as high_quality_pct
FROM curated.games_complete;

-- Create security audit log for configuration changes
CREATE TABLE IF NOT EXISTS audit.data_quality_config_changes (
    id SERIAL PRIMARY KEY,
    setting_name VARCHAR(100) NOT NULL,
    old_value DECIMAL(5,4),
    new_value DECIMAL(5,4) NOT NULL,
    changed_by VARCHAR(100) DEFAULT CURRENT_USER,
    changed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    reason TEXT
);

-- Create trigger to log configuration changes
CREATE OR REPLACE FUNCTION audit.log_data_quality_config_change()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        INSERT INTO audit.data_quality_config_changes (
            setting_name, old_value, new_value, reason
        ) VALUES (
            NEW.setting_name, OLD.setting_value, NEW.setting_value, 
            'Configuration updated via SQL'
        );
    ELSIF TG_OP = 'INSERT' THEN
        INSERT INTO audit.data_quality_config_changes (
            setting_name, old_value, new_value, reason
        ) VALUES (
            NEW.setting_name, NULL, NEW.setting_value, 
            'Configuration created via SQL'
        );
    END IF;
    
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Apply trigger to configuration table
DROP TRIGGER IF EXISTS tr_data_quality_config_audit ON config.data_quality_settings;
CREATE TRIGGER tr_data_quality_config_audit
    AFTER INSERT OR UPDATE ON config.data_quality_settings
    FOR EACH ROW EXECUTE FUNCTION audit.log_data_quality_config_change();

-- Grant necessary permissions
GRANT SELECT ON config.data_quality_settings TO PUBLIC;
GRANT EXECUTE ON FUNCTION config.get_data_quality_setting(VARCHAR, DECIMAL) TO PUBLIC;
GRANT EXECUTE ON FUNCTION curated.update_games_data_quality(INTEGER) TO PUBLIC;
GRANT EXECUTE ON FUNCTION curated.batch_update_games_data_quality(INTEGER, INTEGER) TO PUBLIC;

COMMIT;

-- Verify configuration
DO $$
BEGIN
    RAISE NOTICE 'Data quality configuration migration completed successfully';
    RAISE NOTICE 'Configuration settings created:';
    RAISE NOTICE '  - high_confidence_score: %', config.get_data_quality_setting('high_confidence_score');
    RAISE NOTICE '  - medium_confidence_score: %', config.get_data_quality_setting('medium_confidence_score');
    RAISE NOTICE '  - low_confidence_score: %', config.get_data_quality_setting('low_confidence_score');
    RAISE NOTICE '  - batch_size_limit: %', config.get_data_quality_setting('batch_size_limit');
    RAISE NOTICE 'New functions available:';
    RAISE NOTICE '  - curated.update_games_data_quality(max_games)';
    RAISE NOTICE '  - curated.batch_update_games_data_quality(total_limit, batch_size)';
    RAISE NOTICE 'Security features:';
    RAISE NOTICE '  - Input validation and SQL injection prevention';
    RAISE NOTICE '  - Audit logging for configuration changes';
    RAISE NOTICE '  - Parameterized queries with configurable values';
END $$;