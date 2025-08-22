-- Migration 040: ML Pipeline Integration Fix
-- Addresses Issue #55: ML Pipeline Integration Crisis - Production Data Disconnect

-- Create function to populate ML features from production data
CREATE OR REPLACE FUNCTION populate_ml_features_from_production_data()
RETURNS TABLE(
    processed_games INTEGER,
    inserted_features INTEGER,
    skipped_existing INTEGER,
    processing_errors INTEGER
) AS $$
DECLARE
    _processed_games INTEGER := 0;
    _inserted_features INTEGER := 0;
    _skipped_existing INTEGER := 0;
    _processing_errors INTEGER := 0;
    _game_record RECORD;
    _betting_data RECORD;
    _sharp_data RECORD;
    _feature_quality_score NUMERIC := 1.0;
    _missing_features_count INTEGER := 0;
BEGIN
    -- Process each completed game in enhanced_games
    FOR _game_record IN 
        SELECT 
            eg.id as enhanced_game_id,
            eg.mlb_stats_api_game_id,
            eg.action_network_game_id,
            eg.home_team,
            eg.away_team,
            eg.game_date,
            eg.game_datetime,
            eg.home_score,
            eg.away_score,
            eg.winning_team,
            eg.venue_name,
            eg.temperature_fahrenheit,
            eg.wind_speed_mph,
            eg.wind_direction,
            eg.weather_condition,
            eg.home_pitcher_era,
            eg.away_pitcher_era
        FROM curated.enhanced_games eg
        WHERE eg.home_score IS NOT NULL 
          AND eg.away_score IS NOT NULL
          AND eg.game_status = 'final'
    LOOP
        _processed_games := _processed_games + 1;
        
        BEGIN
            -- Check if ML features already exist for this game
            IF EXISTS (
                SELECT 1 FROM curated.ml_features 
                WHERE game_id = _game_record.enhanced_game_id::VARCHAR
            ) THEN
                _skipped_existing := _skipped_existing + 1;
                CONTINUE;
            END IF;

            -- Initialize quality tracking
            _feature_quality_score := 1.0;
            _missing_features_count := 0;

            -- Get aggregated betting data for this game
            -- Use team and date matching since IDs don't align
            SELECT 
                AVG(CASE WHEN market_type = 'moneyline' THEN home_ml END) as avg_home_ml,
                AVG(CASE WHEN market_type = 'moneyline' THEN away_ml END) as avg_away_ml,
                AVG(CASE WHEN market_type = 'spread' THEN home_spread END) as avg_home_spread,
                AVG(CASE WHEN market_type = 'total' THEN total_line END) as avg_total_line,
                COUNT(DISTINCT sportsbook) as sportsbook_count,
                MIN(odds_timestamp) as first_odds_time,
                MAX(odds_timestamp) as last_odds_time
            INTO _betting_data
            FROM curated.betting_lines_unified blu
            WHERE blu.game_id LIKE '%' || _game_record.home_team || '%' 
              AND blu.game_id LIKE '%' || _game_record.away_team || '%'
              AND DATE(blu.odds_timestamp) = _game_record.game_date;

            -- Get sharp action data if available
            SELECT 
                AVG(confidence) as avg_sharp_percentage,
                bool_or(indicator_type ILIKE '%reverse%' OR indicator_type ILIKE '%rlm%') as has_rlm,
                bool_or(indicator_type ILIKE '%steam%') as has_steam_move,
                CASE WHEN COUNT(*) > 0 THEN 1.0 ELSE 0.0 END as avg_movement_magnitude
            INTO _sharp_data
            FROM curated.sharp_action_indicators sai
            WHERE sai.game_id = _game_record.enhanced_game_id;

            -- Adjust quality score based on missing data
            IF _betting_data.avg_home_ml IS NULL THEN 
                _missing_features_count := _missing_features_count + 1;
                _feature_quality_score := _feature_quality_score - 0.1;
            END IF;
            
            IF _betting_data.avg_home_spread IS NULL THEN 
                _missing_features_count := _missing_features_count + 1;
                _feature_quality_score := _feature_quality_score - 0.1;
            END IF;

            IF _sharp_data.avg_sharp_percentage IS NULL THEN 
                _missing_features_count := _missing_features_count + 1;
                _feature_quality_score := _feature_quality_score - 0.15;
            END IF;

            -- Insert ML features record
            INSERT INTO curated.ml_features (
                game_id,
                mlb_stats_api_game_id,
                home_team,
                away_team,
                game_date,
                game_start_time,
                opening_moneyline_home,
                opening_moneyline_away,
                opening_spread,
                opening_total,
                sharp_money_percentage_home,
                reverse_line_movement,
                steam_move_detected,
                line_movement_magnitude,
                venue_name,
                weather_conditions,
                temperature,
                wind_speed,
                wind_direction,
                data_quality_score,
                missing_features_count
            ) VALUES (
                _game_record.enhanced_game_id::VARCHAR,
                _game_record.mlb_stats_api_game_id,
                _game_record.home_team,
                _game_record.away_team,
                _game_record.game_date,
                _game_record.game_datetime,
                _betting_data.avg_home_ml::INTEGER,
                _betting_data.avg_away_ml::INTEGER,
                _betting_data.avg_home_spread,
                _betting_data.avg_total_line,
                _sharp_data.avg_sharp_percentage,
                COALESCE(_sharp_data.has_rlm, false),
                COALESCE(_sharp_data.has_steam_move, false),
                _sharp_data.avg_movement_magnitude,
                _game_record.venue_name,
                _game_record.weather_condition,
                _game_record.temperature_fahrenheit,
                _game_record.wind_speed_mph,
                _game_record.wind_direction,
                GREATEST(_feature_quality_score, 0.0),
                _missing_features_count
            );

            _inserted_features := _inserted_features + 1;

        EXCEPTION 
            WHEN OTHERS THEN
                _processing_errors := _processing_errors + 1;
                RAISE WARNING 'Error processing game ID %: %', _game_record.enhanced_game_id, SQLERRM;
        END;
    END LOOP;

    -- Return summary statistics
    processed_games := _processed_games;
    inserted_features := _inserted_features;
    skipped_existing := _skipped_existing;
    processing_errors := _processing_errors;
    
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- Create trigger function to automatically populate ML features for new completed games
CREATE OR REPLACE FUNCTION trigger_populate_ml_features_on_game_completion()
RETURNS TRIGGER AS $$
BEGIN
    -- Only process when a game transitions to completed status with scores
    IF NEW.game_status = 'final' 
       AND NEW.home_score IS NOT NULL 
       AND NEW.away_score IS NOT NULL 
       AND (OLD.game_status != 'final' OR OLD.home_score IS NULL OR OLD.away_score IS NULL) THEN
        
        -- Call the population function for this specific game
        PERFORM populate_ml_features_from_production_data();
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger on enhanced_games table
DROP TRIGGER IF EXISTS trigger_ml_features_on_game_completion ON curated.enhanced_games;
CREATE TRIGGER trigger_ml_features_on_game_completion
    AFTER UPDATE ON curated.enhanced_games
    FOR EACH ROW
    EXECUTE FUNCTION trigger_populate_ml_features_on_game_completion();

-- Create function to validate ML pipeline data flow
CREATE OR REPLACE FUNCTION validate_ml_pipeline_data_flow()
RETURNS TABLE(
    metric_name VARCHAR,
    current_value INTEGER,
    expected_minimum INTEGER,
    status VARCHAR,
    details TEXT
) AS $$
BEGIN
    -- Check completed games count
    RETURN QUERY
    SELECT 
        'completed_games'::VARCHAR as metric_name,
        COUNT(*)::INTEGER as current_value,
        10::INTEGER as expected_minimum,
        CASE WHEN COUNT(*) >= 10 THEN 'PASS' ELSE 'FAIL' END::VARCHAR as status,
        'Games with final scores in enhanced_games'::TEXT as details
    FROM curated.enhanced_games 
    WHERE home_score IS NOT NULL AND away_score IS NOT NULL;

    -- Check ML features count
    RETURN QUERY
    SELECT 
        'ml_features_count'::VARCHAR as metric_name,
        COUNT(*)::INTEGER as current_value,
        10::INTEGER as expected_minimum,
        CASE WHEN COUNT(*) >= 10 THEN 'PASS' ELSE 'FAIL' END::VARCHAR as status,
        'ML features generated from production data'::TEXT as details
    FROM curated.ml_features;

    -- Check betting data availability
    RETURN QUERY
    SELECT 
        'betting_lines_count'::VARCHAR as metric_name,
        COUNT(*)::INTEGER as current_value,
        1::INTEGER as expected_minimum,
        CASE WHEN COUNT(*) >= 1 THEN 'PASS' ELSE 'FAIL' END::VARCHAR as status,
        'Betting lines in unified table'::TEXT as details
    FROM curated.betting_lines_unified;

    -- Check data quality score
    RETURN QUERY
    SELECT 
        'avg_data_quality'::VARCHAR as metric_name,
        (AVG(data_quality_score) * 100)::INTEGER as current_value,
        70::INTEGER as expected_minimum,
        CASE WHEN AVG(data_quality_score) >= 0.7 THEN 'PASS' ELSE 'FAIL' END::VARCHAR as status,
        'Average data quality score for ML features'::TEXT as details
    FROM curated.ml_features
    WHERE data_quality_score IS NOT NULL;
END;
$$ LANGUAGE plpgsql;

-- Add helpful indexes for ML pipeline performance
CREATE INDEX IF NOT EXISTS idx_ml_features_game_completion_lookup 
    ON curated.ml_features(game_id, feature_extraction_date);

CREATE INDEX IF NOT EXISTS idx_enhanced_games_ml_ready 
    ON curated.enhanced_games(game_status, home_score, away_score) 
    WHERE game_status = 'final' AND home_score IS NOT NULL AND away_score IS NOT NULL;

-- Create view for ML pipeline monitoring
CREATE OR REPLACE VIEW analytics.ml_pipeline_health AS
SELECT 
    'ML Features' as component,
    COUNT(*) as record_count,
    AVG(data_quality_score) as avg_quality_score,
    MIN(feature_extraction_date) as earliest_record,
    MAX(feature_extraction_date) as latest_record,
    COUNT(*) FILTER (WHERE data_quality_score >= 0.8) as high_quality_count
FROM curated.ml_features
UNION ALL
SELECT 
    'Completed Games' as component,
    COUNT(*) as record_count,
    AVG(data_quality_score) as avg_quality_score,
    MIN(created_at) as earliest_record,
    MAX(created_at) as latest_record,
    COUNT(*) FILTER (WHERE data_quality_score >= 0.8) as high_quality_count
FROM curated.enhanced_games
WHERE home_score IS NOT NULL AND away_score IS NOT NULL
UNION ALL
SELECT 
    'Betting Lines' as component,
    COUNT(*) as record_count,
    AVG(source_reliability_score) as avg_quality_score,
    MIN(created_at) as earliest_record,
    MAX(created_at) as latest_record,
    COUNT(*) FILTER (WHERE source_reliability_score >= 0.8) as high_quality_count
FROM curated.betting_lines_unified;

COMMENT ON FUNCTION populate_ml_features_from_production_data() IS 
'Populates ML features table from production betting and game data. Addresses Issue #55 ML Pipeline Integration Crisis.';

COMMENT ON FUNCTION validate_ml_pipeline_data_flow() IS 
'Validates that ML pipeline is receiving and processing production data correctly.';

COMMENT ON VIEW analytics.ml_pipeline_health IS 
'Monitoring view for ML pipeline data flow and quality metrics.';