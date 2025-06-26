-- PostgreSQL Compatibility Functions for MLB Sharp Betting System
-- PostgreSQL-specific functionality with PostgreSQL equivalents

-- =============================================================================
-- TYPE CASTING FUNCTIONS (replacing TRY_CAST)
-- =============================================================================

-- Safe integer casting that returns NULL on failure
CREATE OR REPLACE FUNCTION try_cast_int(input_text TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    RETURN CAST(input_text AS INTEGER);
EXCEPTION
    WHEN invalid_text_representation THEN
        RETURN NULL;
    WHEN numeric_value_out_of_range THEN
        RETURN NULL;
END;
$$;

-- Safe double precision casting that returns NULL on failure
CREATE OR REPLACE FUNCTION try_cast_double(input_text TEXT)
RETURNS DOUBLE PRECISION
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    RETURN CAST(input_text AS DOUBLE PRECISION);
EXCEPTION
    WHEN invalid_text_representation THEN
        RETURN NULL;
    WHEN numeric_value_out_of_range THEN
        RETURN NULL;
END;
$$;

-- Safe numeric casting that returns NULL on failure
CREATE OR REPLACE FUNCTION try_cast_numeric(input_text TEXT)
RETURNS NUMERIC
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    RETURN CAST(input_text AS NUMERIC);
EXCEPTION
    WHEN invalid_text_representation THEN
        RETURN NULL;
    WHEN numeric_value_out_of_range THEN
        RETURN NULL;
END;
$$;

-- Safe boolean casting that returns NULL on failure
CREATE OR REPLACE FUNCTION try_cast_boolean(input_text TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    RETURN CAST(input_text AS BOOLEAN);
EXCEPTION
    WHEN invalid_text_representation THEN
        RETURN NULL;
END;
$$;

-- =============================================================================
-- JSON EXTRACTION FUNCTIONS (with error handling)
-- =============================================================================

-- Safe JSON text extraction that returns NULL on failure
CREATE OR REPLACE FUNCTION safe_json_extract_text(input_json JSONB, json_path TEXT)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    IF input_json IS NULL OR json_path IS NULL THEN
        RETURN NULL;
    END IF;
    
    -- Remove leading $. if present
    IF json_path LIKE '$.%' THEN
        json_path := SUBSTRING(json_path FROM 3);
    END IF;
    
    RETURN (input_json ->> json_path);
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
END;
$$;

-- Safe JSON integer extraction that returns NULL on failure
CREATE OR REPLACE FUNCTION safe_json_extract_int(input_json JSONB, json_path TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    text_value TEXT;
BEGIN
    text_value := safe_json_extract_text(input_json, json_path);
    RETURN try_cast_int(text_value);
END;
$$;

-- Safe JSON double extraction that returns NULL on failure
CREATE OR REPLACE FUNCTION safe_json_extract_double(input_json JSONB, json_path TEXT)
RETURNS DOUBLE PRECISION
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    text_value TEXT;
BEGIN
    text_value := safe_json_extract_text(input_json, json_path);
    RETURN try_cast_double(text_value);
END;
$$;

-- =============================================================================
-- AGGREGATE FUNCTIONS (PostgreSQL aggregates)
-- =============================================================================

-- Replace MAX(boolean) with proper boolean aggregation
CREATE OR REPLACE FUNCTION bool_max(boolean_array BOOLEAN[])
RETURNS BOOLEAN AS $$
BEGIN
    IF boolean_array IS NULL OR array_length(boolean_array, 1) = 0 THEN
        RETURN NULL;
    END IF;
    
    FOR i IN 1..array_length(boolean_array, 1) LOOP
        IF boolean_array[i] = TRUE THEN
            RETURN TRUE;
        END IF;
    END LOOP;
    
    RETURN FALSE;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- =============================================================================
-- UTILITY FUNCTIONS
-- =============================================================================

-- Calculate win percentage with proper null handling
CREATE OR REPLACE FUNCTION calculate_win_rate(wins INTEGER, total_games INTEGER)
RETURNS DOUBLE PRECISION AS $$
BEGIN
    IF total_games IS NULL OR total_games = 0 THEN
        RETURN NULL;
    END IF;
    IF wins IS NULL THEN
        RETURN 0.0;
    END IF;
    RETURN (wins::DOUBLE PRECISION / total_games::DOUBLE PRECISION) * 100.0;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Calculate ROI with proper null handling
CREATE OR REPLACE FUNCTION calculate_roi(profit DOUBLE PRECISION, total_wagered DOUBLE PRECISION)
RETURNS DOUBLE PRECISION AS $$
BEGIN
    IF total_wagered IS NULL OR total_wagered = 0 THEN
        RETURN NULL;
    END IF;
    IF profit IS NULL THEN
        RETURN -100.0;
    END IF;
    RETURN (profit / total_wagered) * 100.0;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Convert American odds to decimal odds
CREATE OR REPLACE FUNCTION american_to_decimal(american_odds INTEGER)
RETURNS DOUBLE PRECISION AS $$
BEGIN
    IF american_odds IS NULL THEN
        RETURN NULL;
    END IF;
    
    IF american_odds > 0 THEN
        RETURN (american_odds::DOUBLE PRECISION / 100.0) + 1.0;
    ELSE
        RETURN (100.0 / ABS(american_odds::DOUBLE PRECISION)) + 1.0;
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Calculate implied probability from American odds
CREATE OR REPLACE FUNCTION american_to_probability(american_odds INTEGER)
RETURNS DOUBLE PRECISION AS $$
BEGIN
    IF american_odds IS NULL THEN
        RETURN NULL;
    END IF;
    
    IF american_odds > 0 THEN
        RETURN 100.0 / (american_odds::DOUBLE PRECISION + 100.0);
    ELSE
        RETURN ABS(american_odds::DOUBLE PRECISION) / (ABS(american_odds::DOUBLE PRECISION) + 100.0);
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- =============================================================================
-- SCHEMA COMPATIBILITY VIEWS
-- =============================================================================

-- Create view for missing validation_rate column (if needed)
-- This will be created dynamically based on available data

-- Create view for missing movement_validation_rate column (if needed)
-- This will be created dynamically based on available data

-- Create view for missing winning_team column (if needed)
-- This can be derived from game outcomes

-- =============================================================================
-- COMMENTS AND DOCUMENTATION
-- =============================================================================

COMMENT ON FUNCTION try_cast_int(TEXT) IS 'Safe integer casting that returns NULL on failure, PostgreSQL safe casting';
COMMENT ON FUNCTION try_cast_double(TEXT) IS 'Safe double precision casting that returns NULL on failure, PostgreSQL safe casting';
COMMENT ON FUNCTION try_cast_numeric(TEXT) IS 'Safe numeric casting that returns NULL on failure, PostgreSQL safe casting';
COMMENT ON FUNCTION try_cast_boolean(TEXT) IS 'Safe boolean casting that returns NULL on failure, PostgreSQL safe casting';

COMMENT ON FUNCTION safe_json_extract_text(JSONB, TEXT) IS 'Safe JSON text extraction with error handling';
COMMENT ON FUNCTION safe_json_extract_int(JSONB, TEXT) IS 'Safe JSON integer extraction with error handling';
COMMENT ON FUNCTION safe_json_extract_double(JSONB, TEXT) IS 'Safe JSON double extraction with error handling';

COMMENT ON FUNCTION calculate_win_rate(INTEGER, INTEGER) IS 'Calculate win percentage with proper null handling';
COMMENT ON FUNCTION calculate_roi(DOUBLE PRECISION, DOUBLE PRECISION) IS 'Calculate ROI with proper null handling';
COMMENT ON FUNCTION american_to_decimal(INTEGER) IS 'Convert American odds to decimal odds';
COMMENT ON FUNCTION american_to_probability(INTEGER) IS 'Calculate implied probability from American odds'; 