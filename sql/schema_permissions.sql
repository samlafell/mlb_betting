-- MLB Sharp Betting System - Schema Permissions Configuration
-- This file sets up proper permissions for the consolidated 4-schema structure
-- 
-- Schema Structure:
-- 1. raw_data - External data ingestion and raw storage
-- 2. core_betting - Clean, processed betting data and core business entities
-- 3. analytics - Derived analytics, signals, and strategy outputs
-- 4. operational - System operations, monitoring, and validation

-- ==============================================================================
-- CREATE ROLES FOR DIFFERENT ACCESS LEVELS
-- ==============================================================================

-- Data collection role - can insert raw data
CREATE ROLE IF NOT EXISTS data_collectors;

-- Data processing role - can read raw data and write to core_betting
CREATE ROLE IF NOT EXISTS betting_processors;

-- Analytics role - can read core_betting and write to analytics
CREATE ROLE IF NOT EXISTS analytics_processors;

-- Strategy processors - can read all and write to analytics/operational
CREATE ROLE IF NOT EXISTS strategy_processors;

-- Monitoring role - can read operational schema
CREATE ROLE IF NOT EXISTS monitoring_users;

-- System administrators - full access
CREATE ROLE IF NOT EXISTS system_administrators;

-- Read-only analytics users - can read analytics and core_betting
CREATE ROLE IF NOT EXISTS analytics_users;

-- Application role - main application user with broad access
CREATE ROLE IF NOT EXISTS mlb_betting_app;

-- ==============================================================================
-- SCHEMA-LEVEL PERMISSIONS
-- ==============================================================================

-- RAW_DATA SCHEMA PERMISSIONS
-- Data collectors can insert raw data
GRANT USAGE ON SCHEMA raw_data TO data_collectors, betting_processors, system_administrators, mlb_betting_app;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA raw_data TO data_collectors;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA raw_data TO betting_processors, mlb_betting_app;
GRANT ALL ON ALL TABLES IN SCHEMA raw_data TO system_administrators;

-- Grant sequence permissions for auto-incrementing IDs
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA raw_data TO data_collectors, betting_processors, mlb_betting_app;

-- CORE_BETTING SCHEMA PERMISSIONS
-- Betting processors can read/write core betting data
GRANT USAGE ON SCHEMA core_betting TO betting_processors, analytics_processors, analytics_users, strategy_processors, monitoring_users, system_administrators, mlb_betting_app;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA core_betting TO betting_processors, mlb_betting_app;
GRANT SELECT ON ALL TABLES IN SCHEMA core_betting TO analytics_processors, analytics_users, strategy_processors, monitoring_users;
GRANT ALL ON ALL TABLES IN SCHEMA core_betting TO system_administrators;

-- Grant sequence permissions
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA core_betting TO betting_processors, analytics_processors, strategy_processors, mlb_betting_app;

-- ANALYTICS SCHEMA PERMISSIONS
-- Analytics processors and strategy processors can write analytics
GRANT USAGE ON SCHEMA analytics TO analytics_processors, analytics_users, strategy_processors, monitoring_users, system_administrators, mlb_betting_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA analytics TO analytics_processors, strategy_processors, mlb_betting_app;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO analytics_users, monitoring_users;
GRANT ALL ON ALL TABLES IN SCHEMA analytics TO system_administrators;

-- Grant sequence permissions
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA analytics TO analytics_processors, strategy_processors, mlb_betting_app;

-- OPERATIONAL SCHEMA PERMISSIONS
-- Strategy processors and system administrators can manage operational data
GRANT USAGE ON SCHEMA operational TO strategy_processors, monitoring_users, system_administrators, mlb_betting_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA operational TO strategy_processors, mlb_betting_app;
GRANT SELECT ON ALL TABLES IN SCHEMA operational TO monitoring_users;
GRANT ALL ON ALL TABLES IN SCHEMA operational TO system_administrators;

-- Grant sequence permissions
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA operational TO strategy_processors, mlb_betting_app;

-- ==============================================================================
-- FUNCTION AND PROCEDURE PERMISSIONS
-- ==============================================================================

-- Grant execute permissions on schema functions
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA raw_data TO data_collectors, betting_processors, system_administrators, mlb_betting_app;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA core_betting TO betting_processors, analytics_processors, strategy_processors, system_administrators, mlb_betting_app;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA analytics TO analytics_processors, strategy_processors, system_administrators, mlb_betting_app;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA operational TO strategy_processors, system_administrators, mlb_betting_app;

-- ==============================================================================
-- VIEW PERMISSIONS
-- ==============================================================================

-- Grant view permissions (views inherit table permissions, but explicit grants for clarity)
GRANT SELECT ON operational.betting_performance_summary TO analytics_users, monitoring_users, strategy_processors, system_administrators, mlb_betting_app;
GRANT SELECT ON operational.active_strategies TO analytics_users, monitoring_users, strategy_processors, system_administrators, mlb_betting_app;

-- ==============================================================================
-- DEFAULT PRIVILEGES FOR FUTURE OBJECTS
-- ==============================================================================

-- Set default privileges for future tables, sequences, and functions

-- RAW_DATA schema defaults
ALTER DEFAULT PRIVILEGES IN SCHEMA raw_data GRANT SELECT, INSERT TO data_collectors;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw_data GRANT SELECT, INSERT, UPDATE TO betting_processors, mlb_betting_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw_data GRANT ALL TO system_administrators;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw_data GRANT USAGE, SELECT ON SEQUENCES TO data_collectors, betting_processors, mlb_betting_app;

-- CORE_BETTING schema defaults
ALTER DEFAULT PRIVILEGES IN SCHEMA core_betting GRANT SELECT, INSERT, UPDATE TO betting_processors, mlb_betting_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA core_betting GRANT SELECT TO analytics_processors, analytics_users, strategy_processors, monitoring_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA core_betting GRANT ALL TO system_administrators;
ALTER DEFAULT PRIVILEGES IN SCHEMA core_betting GRANT USAGE, SELECT ON SEQUENCES TO betting_processors, analytics_processors, strategy_processors, mlb_betting_app;

-- ANALYTICS schema defaults
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT SELECT, INSERT, UPDATE, DELETE TO analytics_processors, strategy_processors, mlb_betting_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT SELECT TO analytics_users, monitoring_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT ALL TO system_administrators;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT USAGE, SELECT ON SEQUENCES TO analytics_processors, strategy_processors, mlb_betting_app;

-- OPERATIONAL schema defaults
ALTER DEFAULT PRIVILEGES IN SCHEMA operational GRANT SELECT, INSERT, UPDATE, DELETE TO strategy_processors, mlb_betting_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA operational GRANT SELECT TO monitoring_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA operational GRANT ALL TO system_administrators;
ALTER DEFAULT PRIVILEGES IN SCHEMA operational GRANT USAGE, SELECT ON SEQUENCES TO strategy_processors, mlb_betting_app;

-- Function defaults
ALTER DEFAULT PRIVILEGES IN SCHEMA raw_data GRANT EXECUTE ON FUNCTIONS TO data_collectors, betting_processors, system_administrators, mlb_betting_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA core_betting GRANT EXECUTE ON FUNCTIONS TO betting_processors, analytics_processors, strategy_processors, system_administrators, mlb_betting_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT EXECUTE ON FUNCTIONS TO analytics_processors, strategy_processors, system_administrators, mlb_betting_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA operational GRANT EXECUTE ON FUNCTIONS TO strategy_processors, system_administrators, mlb_betting_app;

-- ==============================================================================
-- SECURITY POLICIES (ROW LEVEL SECURITY)
-- ==============================================================================

-- Example RLS policies (uncomment and modify as needed for production)

-- Restrict data access by date range for some roles
-- ALTER TABLE analytics.betting_recommendations ENABLE ROW LEVEL SECURITY;
-- CREATE POLICY analytics_recent_data ON analytics.betting_recommendations 
--   FOR SELECT TO analytics_users 
--   USING (created_at >= CURRENT_DATE - INTERVAL '90 days');

-- Restrict operational data access
-- ALTER TABLE operational.system_health_checks ENABLE ROW LEVEL SECURITY;
-- CREATE POLICY operational_monitoring_access ON operational.system_health_checks
--   FOR SELECT TO monitoring_users
--   USING (check_category IN ('database', 'api'));

-- ==============================================================================
-- UTILITY FUNCTIONS FOR PERMISSION MANAGEMENT
-- ==============================================================================

-- Function to grant user to appropriate roles based on their function
CREATE OR REPLACE FUNCTION grant_user_permissions(
    username VARCHAR(100),
    user_type VARCHAR(50) -- 'collector', 'processor', 'analyst', 'admin', 'monitor', 'app'
) RETURNS void AS $$
BEGIN
    CASE user_type
        WHEN 'collector' THEN
            EXECUTE format('GRANT data_collectors TO %I', username);
        WHEN 'processor' THEN
            EXECUTE format('GRANT betting_processors TO %I', username);
        WHEN 'analyst' THEN
            EXECUTE format('GRANT analytics_users TO %I', username);
        WHEN 'strategy' THEN
            EXECUTE format('GRANT strategy_processors TO %I', username);
        WHEN 'monitor' THEN
            EXECUTE format('GRANT monitoring_users TO %I', username);
        WHEN 'admin' THEN
            EXECUTE format('GRANT system_administrators TO %I', username);
        WHEN 'app' THEN
            EXECUTE format('GRANT mlb_betting_app TO %I', username);
        ELSE
            RAISE EXCEPTION 'Invalid user type: %', user_type;
    END CASE;
    
    RAISE NOTICE 'Granted % permissions to user %', user_type, username;
END;
$$ LANGUAGE plpgsql;

-- Function to revoke user permissions
CREATE OR REPLACE FUNCTION revoke_user_permissions(
    username VARCHAR(100),
    user_type VARCHAR(50)
) RETURNS void AS $$
BEGIN
    CASE user_type
        WHEN 'collector' THEN
            EXECUTE format('REVOKE data_collectors FROM %I', username);
        WHEN 'processor' THEN
            EXECUTE format('REVOKE betting_processors FROM %I', username);
        WHEN 'analyst' THEN
            EXECUTE format('REVOKE analytics_users FROM %I', username);
        WHEN 'strategy' THEN
            EXECUTE format('REVOKE strategy_processors FROM %I', username);
        WHEN 'monitor' THEN
            EXECUTE format('REVOKE monitoring_users FROM %I', username);
        WHEN 'admin' THEN
            EXECUTE format('REVOKE system_administrators FROM %I', username);
        WHEN 'app' THEN
            EXECUTE format('REVOKE mlb_betting_app FROM %I', username);
        ELSE
            RAISE EXCEPTION 'Invalid user type: %', user_type;
    END CASE;
    
    RAISE NOTICE 'Revoked % permissions from user %', user_type, username;
END;
$$ LANGUAGE plpgsql;

-- Function to check user permissions
CREATE OR REPLACE FUNCTION check_user_permissions(username VARCHAR(100))
RETURNS TABLE(schema_name VARCHAR, table_name VARCHAR, privilege_type VARCHAR) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        t.table_schema::VARCHAR,
        t.table_name::VARCHAR,
        p.privilege_type::VARCHAR
    FROM information_schema.table_privileges p
    JOIN information_schema.tables t ON (
        p.table_schema = t.table_schema 
        AND p.table_name = t.table_name
    )
    WHERE p.grantee = username
      AND t.table_schema IN ('raw_data', 'core_betting', 'analytics', 'operational')
    ORDER BY t.table_schema, t.table_name, p.privilege_type;
END;
$$ LANGUAGE plpgsql;

-- ==============================================================================
-- COMMENTS FOR DOCUMENTATION
-- ==============================================================================

COMMENT ON ROLE data_collectors IS 'Can insert raw data into raw_data schema';
COMMENT ON ROLE betting_processors IS 'Can process raw data and write to core_betting schema';
COMMENT ON ROLE analytics_processors IS 'Can read core_betting and write analytics';
COMMENT ON ROLE strategy_processors IS 'Can read all data and write to analytics/operational schemas';
COMMENT ON ROLE monitoring_users IS 'Read-only access to operational monitoring data';
COMMENT ON ROLE analytics_users IS 'Read-only access to analytics and core_betting data';
COMMENT ON ROLE system_administrators IS 'Full administrative access to all schemas';
COMMENT ON ROLE mlb_betting_app IS 'Main application role with broad read/write access';

-- Log the completion
DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'SCHEMA PERMISSIONS CONFIGURATION COMPLETED';
    RAISE NOTICE '========================================';
    RAISE NOTICE '';
    RAISE NOTICE 'Roles created:';
    RAISE NOTICE '- data_collectors: Raw data insertion';
    RAISE NOTICE '- betting_processors: Core betting data processing';
    RAISE NOTICE '- analytics_processors: Analytics generation';
    RAISE NOTICE '- strategy_processors: Strategy and operational management';
    RAISE NOTICE '- monitoring_users: Operational monitoring';
    RAISE NOTICE '- analytics_users: Analytics read access';
    RAISE NOTICE '- system_administrators: Full access';
    RAISE NOTICE '- mlb_betting_app: Main application role';
    RAISE NOTICE '';
    RAISE NOTICE 'Usage examples:';
    RAISE NOTICE '1. Grant user permissions: SELECT grant_user_permissions(''username'', ''processor'');';
    RAISE NOTICE '2. Check permissions: SELECT * FROM check_user_permissions(''username'');';
    RAISE NOTICE '3. Revoke permissions: SELECT revoke_user_permissions(''username'', ''processor'');';
    RAISE NOTICE '========================================';
END $$; 