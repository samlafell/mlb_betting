-- Migration 202: Create Rate Limiting Tables
-- Creates database tables for comprehensive rate limiting system
-- supporting sliding window, fixed window, and token bucket algorithms.

-- Rate limit request tracking for sliding window algorithm
CREATE TABLE auth.rate_limit_requests (
    id BIGSERIAL PRIMARY KEY,
    cache_key VARCHAR(512) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    weight INTEGER NOT NULL DEFAULT 1,
    metadata JSONB DEFAULT '{}'::jsonb
);

-- Create indexes for efficient sliding window queries
CREATE INDEX idx_auth_rate_limit_requests_cache_key_timestamp 
ON auth.rate_limit_requests(cache_key, timestamp DESC);

CREATE INDEX idx_auth_rate_limit_requests_timestamp 
ON auth.rate_limit_requests(timestamp) WHERE timestamp > NOW() - INTERVAL '24 hours';

-- Rate limit windows for fixed window algorithm
CREATE TABLE auth.rate_limit_windows (
    cache_key VARCHAR(512) PRIMARY KEY,
    request_count INTEGER NOT NULL DEFAULT 0,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create index for window cleanup
CREATE INDEX idx_auth_rate_limit_windows_window_end 
ON auth.rate_limit_windows(window_end);

-- Token buckets for token bucket algorithm
CREATE TABLE auth.rate_limit_buckets (
    cache_key VARCHAR(512) PRIMARY KEY,
    tokens DECIMAL(10,2) NOT NULL DEFAULT 0,
    last_refill TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create index for bucket cleanup
CREATE INDEX idx_auth_rate_limit_buckets_last_refill 
ON auth.rate_limit_buckets(last_refill);

-- Rate limit penalties for escalating restrictions
CREATE TABLE auth.rate_limit_penalties (
    cache_key VARCHAR(512) PRIMARY KEY,
    penalty_until TIMESTAMPTZ NOT NULL,
    penalty_level INTEGER DEFAULT 1,
    escalation_count INTEGER DEFAULT 1,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create index for penalty lookup and cleanup
CREATE INDEX idx_auth_rate_limit_penalties_penalty_until 
ON auth.rate_limit_penalties(penalty_until);

CREATE INDEX idx_auth_rate_limit_penalties_active 
ON auth.rate_limit_penalties(penalty_until) WHERE penalty_until > NOW();

-- Rate limit configuration for dynamic rule management
CREATE TABLE auth.rate_limit_rules (
    id SERIAL PRIMARY KEY,
    rule_name VARCHAR(100) NOT NULL UNIQUE,
    scope VARCHAR(20) NOT NULL CHECK (scope IN ('ip', 'user', 'api_key', 'endpoint', 'global')),
    algorithm VARCHAR(20) NOT NULL CHECK (algorithm IN ('sliding_window', 'fixed_window', 'token_bucket')),
    max_requests INTEGER NOT NULL,
    window_seconds INTEGER NOT NULL,
    burst_allowance INTEGER DEFAULT 0,
    penalty_seconds INTEGER DEFAULT 0,
    enabled BOOLEAN DEFAULT true,
    description TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Insert default rate limiting rules
INSERT INTO auth.rate_limit_rules (rule_name, scope, algorithm, max_requests, window_seconds, burst_allowance, penalty_seconds, description) VALUES
    ('login', 'ip', 'sliding_window', 5, 300, 0, 900, 'Login rate limit per IP address'),
    ('password_reset', 'ip', 'sliding_window', 3, 3600, 0, 3600, 'Password reset rate limit per IP address'),
    ('api_general', 'user', 'token_bucket', 1000, 3600, 100, 0, 'General API rate limit per user'),
    ('api_key', 'api_key', 'fixed_window', 10000, 3600, 0, 0, 'API key rate limit'),
    ('mfa_verification', 'ip', 'sliding_window', 10, 600, 0, 1800, 'MFA verification rate limit per IP'),
    ('user_registration', 'ip', 'sliding_window', 3, 1800, 0, 3600, 'User registration rate limit per IP'),
    ('email_verification', 'user', 'sliding_window', 5, 3600, 0, 0, 'Email verification rate limit per user'),
    ('api_admin', 'user', 'token_bucket', 500, 3600, 50, 0, 'Admin API rate limit per user');

-- Rate limit statistics and monitoring
CREATE TABLE auth.rate_limit_stats (
    id BIGSERIAL PRIMARY KEY,
    rule_name VARCHAR(100) NOT NULL,
    identifier VARCHAR(512) NOT NULL,
    requests_allowed INTEGER DEFAULT 0,
    requests_blocked INTEGER DEFAULT 0,
    total_requests INTEGER DEFAULT 0,
    last_request TIMESTAMPTZ,
    stat_date DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(rule_name, identifier, stat_date)
);

-- Create indexes for statistics
CREATE INDEX idx_auth_rate_limit_stats_rule_date 
ON auth.rate_limit_stats(rule_name, stat_date DESC);

CREATE INDEX idx_auth_rate_limit_stats_date 
ON auth.rate_limit_stats(stat_date DESC);

-- Function to update rate limit statistics
CREATE OR REPLACE FUNCTION auth.update_rate_limit_stats(
    p_rule_name VARCHAR(100),
    p_identifier VARCHAR(512),
    p_allowed BOOLEAN
) RETURNS VOID AS $$
BEGIN
    INSERT INTO auth.rate_limit_stats (rule_name, identifier, requests_allowed, requests_blocked, total_requests, last_request)
    VALUES (
        p_rule_name, 
        p_identifier, 
        CASE WHEN p_allowed THEN 1 ELSE 0 END,
        CASE WHEN p_allowed THEN 0 ELSE 1 END,
        1,
        NOW()
    )
    ON CONFLICT (rule_name, identifier, stat_date)
    DO UPDATE SET
        requests_allowed = auth.rate_limit_stats.requests_allowed + CASE WHEN p_allowed THEN 1 ELSE 0 END,
        requests_blocked = auth.rate_limit_stats.requests_blocked + CASE WHEN p_allowed THEN 0 ELSE 1 END,
        total_requests = auth.rate_limit_stats.total_requests + 1,
        last_request = NOW(),
        updated_at = NOW();
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Function to get rate limit analytics
CREATE OR REPLACE FUNCTION auth.get_rate_limit_analytics(
    p_rule_name VARCHAR(100) DEFAULT NULL,
    p_days INTEGER DEFAULT 7
) RETURNS TABLE(
    rule_name VARCHAR(100),
    stat_date DATE,
    total_requests BIGINT,
    allowed_requests BIGINT,
    blocked_requests BIGINT,
    block_rate DECIMAL(5,2),
    unique_identifiers BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        rls.rule_name,
        rls.stat_date,
        SUM(rls.total_requests) as total_requests,
        SUM(rls.requests_allowed) as allowed_requests,
        SUM(rls.requests_blocked) as blocked_requests,
        ROUND(
            CASE 
                WHEN SUM(rls.total_requests) > 0 
                THEN (SUM(rls.requests_blocked)::DECIMAL / SUM(rls.total_requests) * 100)
                ELSE 0 
            END, 2
        ) as block_rate,
        COUNT(DISTINCT rls.identifier) as unique_identifiers
    FROM auth.rate_limit_stats rls
    WHERE 
        (p_rule_name IS NULL OR rls.rule_name = p_rule_name)
        AND rls.stat_date >= CURRENT_DATE - INTERVAL '%s days' % p_days
    GROUP BY rls.rule_name, rls.stat_date
    ORDER BY rls.rule_name, rls.stat_date DESC;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Function to clean up old rate limit data
CREATE OR REPLACE FUNCTION auth.cleanup_rate_limit_data() RETURNS TABLE(
    table_name TEXT,
    rows_deleted BIGINT
) AS $$
DECLARE
    v_cutoff_1h TIMESTAMPTZ := NOW() - INTERVAL '1 hour';
    v_cutoff_1d TIMESTAMPTZ := NOW() - INTERVAL '1 day';
    v_cutoff_7d TIMESTAMPTZ := NOW() - INTERVAL '7 days';
    v_cutoff_30d TIMESTAMPTZ := NOW() - INTERVAL '30 days';
    v_deleted BIGINT;
BEGIN
    -- Clean up old requests (keep 1 day for sliding windows)
    DELETE FROM auth.rate_limit_requests WHERE timestamp < v_cutoff_1d;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    RETURN QUERY SELECT 'rate_limit_requests'::TEXT, v_deleted;
    
    -- Clean up expired windows
    DELETE FROM auth.rate_limit_windows WHERE window_end < v_cutoff_1h;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    RETURN QUERY SELECT 'rate_limit_windows'::TEXT, v_deleted;
    
    -- Clean up expired penalties
    DELETE FROM auth.rate_limit_penalties WHERE penalty_until < NOW();
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    RETURN QUERY SELECT 'rate_limit_penalties'::TEXT, v_deleted;
    
    -- Clean up stale buckets (not updated in 7 days)
    DELETE FROM auth.rate_limit_buckets WHERE last_refill < v_cutoff_7d;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    RETURN QUERY SELECT 'rate_limit_buckets'::TEXT, v_deleted;
    
    -- Archive old statistics (keep 30 days, archive older)
    -- In practice, you might want to move to archive table instead of delete
    DELETE FROM auth.rate_limit_stats WHERE stat_date < CURRENT_DATE - INTERVAL '30 days';
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    RETURN QUERY SELECT 'rate_limit_stats'::TEXT, v_deleted;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Create comprehensive view for rate limit monitoring
CREATE OR REPLACE VIEW auth.rate_limit_monitoring AS
SELECT 
    rr.rule_name,
    rr.scope,
    rr.algorithm,
    rr.max_requests,
    rr.window_seconds,
    rr.enabled,
    
    -- Current active penalties
    COUNT(DISTINCT rp.cache_key) FILTER (WHERE rp.penalty_until > NOW()) as active_penalties,
    
    -- Recent request stats (last hour)
    COUNT(DISTINCT rlr.cache_key) FILTER (WHERE rlr.timestamp > NOW() - INTERVAL '1 hour') as active_identifiers_1h,
    COUNT(rlr.id) FILTER (WHERE rlr.timestamp > NOW() - INTERVAL '1 hour') as total_requests_1h,
    
    -- Today's stats
    COALESCE(SUM(rls.total_requests) FILTER (WHERE rls.stat_date = CURRENT_DATE), 0) as requests_today,
    COALESCE(SUM(rls.requests_blocked) FILTER (WHERE rls.stat_date = CURRENT_DATE), 0) as blocked_today,
    
    -- Block rate today
    ROUND(
        CASE 
            WHEN SUM(rls.total_requests) FILTER (WHERE rls.stat_date = CURRENT_DATE) > 0 
            THEN (SUM(rls.requests_blocked) FILTER (WHERE rls.stat_date = CURRENT_DATE)::DECIMAL / 
                  SUM(rls.total_requests) FILTER (WHERE rls.stat_date = CURRENT_DATE) * 100)
            ELSE 0 
        END, 2
    ) as block_rate_today
    
FROM auth.rate_limit_rules rr
LEFT JOIN auth.rate_limit_penalties rp ON rp.cache_key LIKE 'rate_limit:' || rr.rule_name || ':%'
LEFT JOIN auth.rate_limit_requests rlr ON rlr.cache_key LIKE 'rate_limit:' || rr.rule_name || ':%'
LEFT JOIN auth.rate_limit_stats rls ON rls.rule_name = rr.rule_name
GROUP BY rr.rule_name, rr.scope, rr.algorithm, rr.max_requests, rr.window_seconds, rr.enabled
ORDER BY rr.rule_name;

-- Create triggers for automatic timestamp updates
CREATE TRIGGER tr_auth_rate_limit_windows_updated
    BEFORE UPDATE ON auth.rate_limit_windows
    FOR EACH ROW
    EXECUTE FUNCTION auth.update_timestamp();

CREATE TRIGGER tr_auth_rate_limit_buckets_updated
    BEFORE UPDATE ON auth.rate_limit_buckets
    FOR EACH ROW
    EXECUTE FUNCTION auth.update_timestamp();

CREATE TRIGGER tr_auth_rate_limit_penalties_updated
    BEFORE UPDATE ON auth.rate_limit_penalties
    FOR EACH ROW
    EXECUTE FUNCTION auth.update_timestamp();

CREATE TRIGGER tr_auth_rate_limit_rules_updated
    BEFORE UPDATE ON auth.rate_limit_rules
    FOR EACH ROW
    EXECUTE FUNCTION auth.update_timestamp();

CREATE TRIGGER tr_auth_rate_limit_stats_updated
    BEFORE UPDATE ON auth.rate_limit_stats
    FOR EACH ROW
    EXECUTE FUNCTION auth.update_timestamp();

-- Grant permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON auth.rate_limit_requests TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON auth.rate_limit_windows TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON auth.rate_limit_buckets TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON auth.rate_limit_penalties TO PUBLIC;
GRANT SELECT, INSERT, UPDATE ON auth.rate_limit_rules TO PUBLIC;
GRANT SELECT, INSERT, UPDATE ON auth.rate_limit_stats TO PUBLIC;
GRANT USAGE, SELECT ON auth.rate_limit_requests_id_seq TO PUBLIC;
GRANT USAGE, SELECT ON auth.rate_limit_rules_id_seq TO PUBLIC;
GRANT USAGE, SELECT ON auth.rate_limit_stats_id_seq TO PUBLIC;

GRANT EXECUTE ON FUNCTION auth.update_rate_limit_stats(VARCHAR, VARCHAR, BOOLEAN) TO PUBLIC;
GRANT EXECUTE ON FUNCTION auth.get_rate_limit_analytics(VARCHAR, INTEGER) TO PUBLIC;
GRANT EXECUTE ON FUNCTION auth.cleanup_rate_limit_data() TO PUBLIC;

GRANT SELECT ON auth.rate_limit_monitoring TO PUBLIC;

-- Log the migration
INSERT INTO auth.audit_log (
    event_type,
    event_category,
    event_description,
    success,
    metadata
) VALUES (
    'migration_completed',
    'security',
    'Rate limiting system database schema created',
    true,
    '{"migration": "202_create_rate_limiting_tables", "tables_created": 5, "functions_created": 3, "views_created": 1, "algorithms": ["sliding_window", "fixed_window", "token_bucket"]}'::jsonb
);

COMMENT ON TABLE auth.rate_limit_requests IS 'Request tracking for sliding window rate limiting';
COMMENT ON TABLE auth.rate_limit_windows IS 'Window tracking for fixed window rate limiting';  
COMMENT ON TABLE auth.rate_limit_buckets IS 'Token bucket state for token bucket rate limiting';
COMMENT ON TABLE auth.rate_limit_penalties IS 'Penalty tracking for rate limit violations';
COMMENT ON TABLE auth.rate_limit_rules IS 'Dynamic rate limiting rule configuration';
COMMENT ON TABLE auth.rate_limit_stats IS 'Rate limiting statistics and monitoring data';
COMMENT ON VIEW auth.rate_limit_monitoring IS 'Comprehensive rate limiting monitoring dashboard';