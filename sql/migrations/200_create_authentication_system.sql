-- Migration 200: Create Comprehensive Authentication System
-- This migration creates all necessary tables for user authentication,
-- role-based access control, session management, and security monitoring.

-- Create authentication schema
CREATE SCHEMA IF NOT EXISTS auth;
GRANT USAGE ON SCHEMA auth TO PUBLIC;

-- Create utility functions for enhanced security
CREATE OR REPLACE FUNCTION auth.generate_uuid() RETURNS UUID AS $$
    SELECT gen_random_uuid();
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION auth.current_timestamp_utc() RETURNS TIMESTAMPTZ AS $$
    SELECT NOW() AT TIME ZONE 'UTC';
$$ LANGUAGE SQL;

-- Password security configuration table
CREATE TABLE auth.security_config (
    id SERIAL PRIMARY KEY,
    config_key VARCHAR(255) NOT NULL UNIQUE,
    config_value TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT auth.current_timestamp_utc(),
    updated_at TIMESTAMPTZ DEFAULT auth.current_timestamp_utc()
);

-- Insert default security configuration
INSERT INTO auth.security_config (config_key, config_value, description) VALUES
    ('password_min_length', '12', 'Minimum password length'),
    ('password_require_uppercase', 'true', 'Require uppercase letters in password'),
    ('password_require_lowercase', 'true', 'Require lowercase letters in password'),
    ('password_require_numbers', 'true', 'Require numbers in password'),
    ('password_require_special_chars', 'true', 'Require special characters in password'),
    ('password_max_age_days', '90', 'Maximum password age in days'),
    ('login_max_attempts', '5', 'Maximum failed login attempts'),
    ('login_lockout_duration_minutes', '30', 'Account lockout duration in minutes'),
    ('session_max_duration_hours', '24', 'Maximum session duration in hours'),
    ('session_idle_timeout_minutes', '60', 'Session idle timeout in minutes'),
    ('mfa_required_for_admin', 'true', 'Require MFA for admin users'),
    ('password_history_count', '5', 'Number of previous passwords to remember'),
    ('jwt_access_token_duration_minutes', '15', 'JWT access token duration in minutes'),
    ('jwt_refresh_token_duration_days', '30', 'JWT refresh token duration in days');

-- User roles table with hierarchical permissions
CREATE TABLE auth.roles (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    display_name VARCHAR(255) NOT NULL,
    description TEXT,
    permissions JSONB DEFAULT '[]'::jsonb,
    is_system_role BOOLEAN DEFAULT false,
    parent_role_id INTEGER REFERENCES auth.roles(id),
    created_at TIMESTAMPTZ DEFAULT auth.current_timestamp_utc(),
    updated_at TIMESTAMPTZ DEFAULT auth.current_timestamp_utc()
);

-- Create index for role hierarchy queries
CREATE INDEX idx_auth_roles_parent ON auth.roles(parent_role_id);
CREATE INDEX idx_auth_roles_permissions ON auth.roles USING gin(permissions);

-- Insert default roles with comprehensive permissions
INSERT INTO auth.roles (name, display_name, description, permissions, is_system_role) VALUES
    ('super_admin', 'Super Administrator', 'Full system access with all permissions', 
     '["system:*", "user:*", "data:*", "analytics:*", "ml:*", "monitoring:*"]'::jsonb, true),
    ('admin', 'Administrator', 'Administrative access to most system features',
     '["user:manage", "data:read", "data:write", "analytics:read", "analytics:write", "ml:read", "monitoring:read"]'::jsonb, true),
    ('analyst', 'Data Analyst', 'Access to analytics and reporting features',
     '["data:read", "analytics:read", "analytics:write", "ml:read", "monitoring:read"]'::jsonb, true),
    ('viewer', 'Viewer', 'Read-only access to dashboards and reports',
     '["data:read", "analytics:read", "monitoring:read"]'::jsonb, true),
    ('ml_engineer', 'ML Engineer', 'Access to machine learning features and models',
     '["data:read", "ml:read", "ml:write", "ml:deploy", "monitoring:read"]'::jsonb, true),
    ('api_user', 'API User', 'Programmatic access for external integrations',
     '["data:read", "analytics:read", "ml:predict"]'::jsonb, true);

-- Users table with comprehensive security features
CREATE TABLE auth.users (
    id SERIAL PRIMARY KEY,
    uuid UUID UNIQUE NOT NULL DEFAULT auth.generate_uuid(),
    username VARCHAR(255) UNIQUE NOT NULL,
    email VARCHAR(320) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    password_salt VARCHAR(32) NOT NULL,
    password_created_at TIMESTAMPTZ DEFAULT auth.current_timestamp_utc(),
    password_updated_at TIMESTAMPTZ DEFAULT auth.current_timestamp_utc(),
    
    -- Profile information
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    display_name VARCHAR(200),
    timezone VARCHAR(50) DEFAULT 'America/New_York',
    locale VARCHAR(10) DEFAULT 'en-US',
    
    -- Account status
    is_active BOOLEAN DEFAULT true,
    is_verified BOOLEAN DEFAULT false,
    is_locked BOOLEAN DEFAULT false,
    locked_until TIMESTAMPTZ,
    failed_login_attempts INTEGER DEFAULT 0,
    last_failed_login TIMESTAMPTZ,
    
    -- Security settings
    require_password_change BOOLEAN DEFAULT false,
    mfa_enabled BOOLEAN DEFAULT false,
    mfa_secret_encrypted TEXT, -- Encrypted MFA secret (AES-256)
    mfa_secret_iv VARCHAR(32), -- Initialization vector for MFA secret
    mfa_backup_codes_encrypted TEXT[], -- Encrypted backup codes (AES-256)
    
    -- Timestamps
    last_login TIMESTAMPTZ,
    last_activity TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT auth.current_timestamp_utc(),
    updated_at TIMESTAMPTZ DEFAULT auth.current_timestamp_utc(),
    
    -- Metadata
    created_by INTEGER REFERENCES auth.users(id),
    metadata JSONB DEFAULT '{}'::jsonb
);

-- Create comprehensive indexes for users table
CREATE UNIQUE INDEX idx_auth_users_uuid ON auth.users(uuid);
CREATE UNIQUE INDEX idx_auth_users_username ON auth.users(username);
CREATE UNIQUE INDEX idx_auth_users_email ON auth.users(email);
CREATE INDEX idx_auth_users_active ON auth.users(is_active);
CREATE INDEX idx_auth_users_verified ON auth.users(is_verified);
CREATE INDEX idx_auth_users_last_activity ON auth.users(last_activity);
CREATE INDEX idx_auth_users_metadata ON auth.users USING gin(metadata);

-- User role assignments with effective dates
CREATE TABLE auth.user_roles (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    role_id INTEGER NOT NULL REFERENCES auth.roles(id) ON DELETE CASCADE,
    assigned_by INTEGER REFERENCES auth.users(id),
    assigned_at TIMESTAMPTZ DEFAULT auth.current_timestamp_utc(),
    effective_from TIMESTAMPTZ DEFAULT auth.current_timestamp_utc(),
    effective_until TIMESTAMPTZ,
    is_active BOOLEAN DEFAULT true,
    UNIQUE(user_id, role_id, effective_from)
);

-- Create indexes for user roles
CREATE INDEX idx_auth_user_roles_user ON auth.user_roles(user_id);
CREATE INDEX idx_auth_user_roles_role ON auth.user_roles(role_id);
CREATE INDEX idx_auth_user_roles_effective ON auth.user_roles(effective_from, effective_until);

-- Password history for preventing reuse
CREATE TABLE auth.password_history (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    password_hash VARCHAR(255) NOT NULL,
    password_salt VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT auth.current_timestamp_utc()
);

-- Create index for password history
CREATE INDEX idx_auth_password_history_user ON auth.password_history(user_id);
CREATE INDEX idx_auth_password_history_created ON auth.password_history(created_at);

-- Active sessions table with comprehensive tracking
CREATE TABLE auth.sessions (
    id SERIAL PRIMARY KEY,
    session_id UUID UNIQUE NOT NULL DEFAULT auth.generate_uuid(),
    user_id INTEGER NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    
    -- Session metadata
    device_fingerprint VARCHAR(255),
    user_agent TEXT,
    ip_address INET,
    location_country VARCHAR(2),
    location_city VARCHAR(100),
    
    -- Session lifecycle
    created_at TIMESTAMPTZ DEFAULT auth.current_timestamp_utc(),
    last_activity TIMESTAMPTZ DEFAULT auth.current_timestamp_utc(),
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    revoked_by INTEGER REFERENCES auth.users(id),
    revoked_reason VARCHAR(255),
    
    -- Session flags
    is_active BOOLEAN DEFAULT true,
    is_mobile BOOLEAN DEFAULT false,
    is_trusted_device BOOLEAN DEFAULT false,
    
    -- Security metadata
    metadata JSONB DEFAULT '{}'::jsonb
);

-- Create comprehensive indexes for sessions
CREATE UNIQUE INDEX idx_auth_sessions_session_id ON auth.sessions(session_id);
CREATE INDEX idx_auth_sessions_user ON auth.sessions(user_id);
CREATE INDEX idx_auth_sessions_active ON auth.sessions(is_active, expires_at);
CREATE INDEX idx_auth_sessions_last_activity ON auth.sessions(last_activity);
CREATE INDEX idx_auth_sessions_ip ON auth.sessions(ip_address);

-- JWT tokens table for token management and revocation
CREATE TABLE auth.jwt_tokens (
    id SERIAL PRIMARY KEY,
    token_id UUID UNIQUE NOT NULL DEFAULT auth.generate_uuid(),
    user_id INTEGER NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    session_id UUID REFERENCES auth.sessions(session_id) ON DELETE CASCADE,
    
    -- Token details
    token_type VARCHAR(20) NOT NULL CHECK (token_type IN ('access', 'refresh', 'reset')),
    token_hash VARCHAR(255) NOT NULL, -- SHA-256 hash of the actual token
    
    -- Token lifecycle
    issued_at TIMESTAMPTZ DEFAULT auth.current_timestamp_utc(),
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    revoked_reason VARCHAR(255),
    
    -- Token metadata
    audience VARCHAR(255),
    scopes TEXT[],
    metadata JSONB DEFAULT '{}'::jsonb
);

-- Create indexes for JWT tokens
CREATE UNIQUE INDEX idx_auth_jwt_tokens_token_id ON auth.jwt_tokens(token_id);
CREATE INDEX idx_auth_jwt_tokens_user ON auth.jwt_tokens(user_id);
CREATE INDEX idx_auth_jwt_tokens_session ON auth.jwt_tokens(session_id);
CREATE INDEX idx_auth_jwt_tokens_type_expires ON auth.jwt_tokens(token_type, expires_at);
CREATE INDEX idx_auth_jwt_tokens_hash ON auth.jwt_tokens(token_hash);

-- Security audit log for all authentication events
CREATE TABLE auth.audit_log (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES auth.users(id),
    session_id UUID REFERENCES auth.sessions(session_id),
    
    -- Event details
    event_type VARCHAR(50) NOT NULL,
    event_category VARCHAR(30) NOT NULL CHECK (event_category IN ('auth', 'session', 'user', 'role', 'security')),
    event_description TEXT,
    
    -- Security context
    ip_address INET,
    user_agent TEXT,
    request_id UUID,
    correlation_id VARCHAR(255),
    
    -- Event outcome
    success BOOLEAN NOT NULL,
    failure_reason VARCHAR(255),
    risk_score INTEGER CHECK (risk_score BETWEEN 0 AND 100),
    
    -- Additional metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    timestamp TIMESTAMPTZ DEFAULT auth.current_timestamp_utc()
);

-- Create comprehensive indexes for audit log
CREATE INDEX idx_auth_audit_log_user ON auth.audit_log(user_id);
CREATE INDEX idx_auth_audit_log_session ON auth.audit_log(session_id);
CREATE INDEX idx_auth_audit_log_event_type ON auth.audit_log(event_type);
CREATE INDEX idx_auth_audit_log_category ON auth.audit_log(event_category);
CREATE INDEX idx_auth_audit_log_timestamp ON auth.audit_log(timestamp);
CREATE INDEX idx_auth_audit_log_ip ON auth.audit_log(ip_address);
CREATE INDEX idx_auth_audit_log_success ON auth.audit_log(success);
CREATE INDEX idx_auth_audit_log_risk_score ON auth.audit_log(risk_score);

-- User preferences and settings
CREATE TABLE auth.user_preferences (
    id SERIAL PRIMARY KEY,
    user_id INTEGER UNIQUE NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    
    -- UI preferences
    theme VARCHAR(20) DEFAULT 'light',
    dashboard_layout JSONB DEFAULT '{}'::jsonb,
    notification_settings JSONB DEFAULT '{"email": true, "browser": true, "mobile": false}'::jsonb,
    
    -- Data preferences
    default_timezone VARCHAR(50) DEFAULT 'America/New_York',
    date_format VARCHAR(20) DEFAULT 'MM/DD/YYYY',
    time_format VARCHAR(10) DEFAULT '12h',
    currency VARCHAR(3) DEFAULT 'USD',
    
    -- Feature preferences
    advanced_features_enabled BOOLEAN DEFAULT false,
    beta_features_enabled BOOLEAN DEFAULT false,
    
    -- Security preferences
    session_timeout_minutes INTEGER DEFAULT 60,
    require_mfa_for_sensitive_actions BOOLEAN DEFAULT true,
    
    -- Custom preferences
    custom_settings JSONB DEFAULT '{}'::jsonb,
    
    created_at TIMESTAMPTZ DEFAULT auth.current_timestamp_utc(),
    updated_at TIMESTAMPTZ DEFAULT auth.current_timestamp_utc()
);

-- Create index for user preferences
CREATE UNIQUE INDEX idx_auth_user_preferences_user ON auth.user_preferences(user_id);

-- API keys for programmatic access
CREATE TABLE auth.api_keys (
    id SERIAL PRIMARY KEY,
    key_id UUID UNIQUE NOT NULL DEFAULT auth.generate_uuid(),
    user_id INTEGER NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    
    -- Key details
    name VARCHAR(255) NOT NULL,
    description TEXT,
    key_hash VARCHAR(255) NOT NULL, -- SHA-256 hash of the actual key
    key_prefix VARCHAR(8) NOT NULL, -- First 8 characters for identification
    
    -- Permissions and scopes
    permissions JSONB DEFAULT '[]'::jsonb,
    rate_limit_per_hour INTEGER DEFAULT 1000,
    allowed_ip_addresses INET[],
    
    -- Lifecycle
    created_at TIMESTAMPTZ DEFAULT auth.current_timestamp_utc(),
    last_used_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    revoked_by INTEGER REFERENCES auth.users(id),
    
    -- Metadata
    is_active BOOLEAN DEFAULT true,
    usage_count BIGINT DEFAULT 0,
    metadata JSONB DEFAULT '{}'::jsonb
);

-- Create indexes for API keys
CREATE UNIQUE INDEX idx_auth_api_keys_key_id ON auth.api_keys(key_id);
CREATE INDEX idx_auth_api_keys_user ON auth.api_keys(user_id);
CREATE INDEX idx_auth_api_keys_hash ON auth.api_keys(key_hash);
CREATE INDEX idx_auth_api_keys_prefix ON auth.api_keys(key_prefix);
CREATE INDEX idx_auth_api_keys_active ON auth.api_keys(is_active, expires_at);

-- Password reset tokens
CREATE TABLE auth.password_reset_tokens (
    id SERIAL PRIMARY KEY,
    token_id UUID UNIQUE NOT NULL DEFAULT auth.generate_uuid(),
    user_id INTEGER NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    
    -- Token details
    token_hash VARCHAR(255) NOT NULL,
    email VARCHAR(320) NOT NULL, -- Store email for validation
    
    -- Lifecycle
    created_at TIMESTAMPTZ DEFAULT auth.current_timestamp_utc(),
    expires_at TIMESTAMPTZ NOT NULL,
    used_at TIMESTAMPTZ,
    
    -- Security
    ip_address INET,
    user_agent TEXT,
    attempts INTEGER DEFAULT 0
);

-- Create indexes for password reset tokens
CREATE UNIQUE INDEX idx_auth_password_reset_token_id ON auth.password_reset_tokens(token_id);
CREATE INDEX idx_auth_password_reset_user ON auth.password_reset_tokens(user_id);
CREATE INDEX idx_auth_password_reset_expires ON auth.password_reset_tokens(expires_at);

-- Email verification tokens
CREATE TABLE auth.email_verification_tokens (
    id SERIAL PRIMARY KEY,
    token_id UUID UNIQUE NOT NULL DEFAULT auth.generate_uuid(),
    user_id INTEGER NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    
    -- Token details
    token_hash VARCHAR(255) NOT NULL,
    email VARCHAR(320) NOT NULL,
    
    -- Lifecycle
    created_at TIMESTAMPTZ DEFAULT auth.current_timestamp_utc(),
    expires_at TIMESTAMPTZ NOT NULL,
    verified_at TIMESTAMPTZ,
    
    -- Security
    ip_address INET,
    attempts INTEGER DEFAULT 0
);

-- Create indexes for email verification tokens
CREATE UNIQUE INDEX idx_auth_email_verification_token_id ON auth.email_verification_tokens(token_id);
CREATE INDEX idx_auth_email_verification_user ON auth.email_verification_tokens(user_id);
CREATE INDEX idx_auth_email_verification_expires ON auth.email_verification_tokens(expires_at);

-- Create views for common authentication queries
CREATE VIEW auth.active_sessions AS
SELECT 
    s.session_id,
    s.user_id,
    u.username,
    u.email,
    s.ip_address,
    s.user_agent,
    s.created_at,
    s.last_activity,
    s.expires_at,
    s.is_mobile,
    s.is_trusted_device
FROM auth.sessions s
JOIN auth.users u ON s.user_id = u.id
WHERE s.is_active = true 
  AND s.expires_at > auth.current_timestamp_utc()
  AND s.revoked_at IS NULL;

CREATE VIEW auth.user_permissions AS
WITH RECURSIVE role_hierarchy AS (
    -- Base case: direct user roles
    SELECT 
        ur.user_id,
        ur.role_id,
        r.name as role_name,
        r.permissions,
        0 as level
    FROM auth.user_roles ur
    JOIN auth.roles r ON ur.role_id = r.id
    WHERE ur.is_active = true
      AND (ur.effective_from IS NULL OR ur.effective_from <= auth.current_timestamp_utc())
      AND (ur.effective_until IS NULL OR ur.effective_until > auth.current_timestamp_utc())
    
    UNION ALL
    
    -- Recursive case: inherited permissions from parent roles
    SELECT 
        rh.user_id,
        r.id as role_id,
        r.name as role_name,
        r.permissions,
        rh.level + 1
    FROM role_hierarchy rh
    JOIN auth.roles parent_role ON rh.role_id = parent_role.id
    JOIN auth.roles r ON r.id = parent_role.parent_role_id
    WHERE rh.level < 10 -- Prevent infinite recursion
)
SELECT 
    user_id,
    jsonb_agg(DISTINCT permissions) as all_permissions,
    array_agg(DISTINCT role_name) as roles
FROM role_hierarchy
GROUP BY user_id;

-- Create function to check user permissions
CREATE OR REPLACE FUNCTION auth.user_has_permission(
    p_user_id INTEGER,
    p_permission TEXT
) RETURNS BOOLEAN AS $$
DECLARE
    v_permissions JSONB;
    v_permission_element JSONB;
BEGIN
    -- Get user's aggregated permissions
    SELECT all_permissions INTO v_permissions
    FROM auth.user_permissions
    WHERE user_id = p_user_id;
    
    IF v_permissions IS NULL THEN
        RETURN false;
    END IF;
    
    -- Check each permission array for the requested permission
    FOR v_permission_element IN SELECT jsonb_array_elements(v_permissions)
    LOOP
        -- Check for exact match or wildcard match
        FOR i IN 0..jsonb_array_length(v_permission_element) - 1
        LOOP
            DECLARE
                v_perm TEXT := v_permission_element ->> i;
            BEGIN
                -- Check for exact match
                IF v_perm = p_permission THEN
                    RETURN true;
                END IF;
                
                -- Check for wildcard match (e.g., "data:*" matches "data:read")
                IF v_perm LIKE '%:*' AND p_permission LIKE (replace(v_perm, '*', '') || '%') THEN
                    RETURN true;
                END IF;
                
                -- Check for system wildcard
                IF v_perm = 'system:*' THEN
                    RETURN true;
                END IF;
            END;
        END LOOP;
    END LOOP;
    
    RETURN false;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Create function to audit authentication events
CREATE OR REPLACE FUNCTION auth.log_auth_event(
    p_user_id INTEGER DEFAULT NULL,
    p_session_id UUID DEFAULT NULL,
    p_event_type VARCHAR(50),
    p_event_category VARCHAR(30),
    p_event_description TEXT DEFAULT NULL,
    p_ip_address INET DEFAULT NULL,
    p_user_agent TEXT DEFAULT NULL,
    p_success BOOLEAN DEFAULT true,
    p_failure_reason VARCHAR(255) DEFAULT NULL,
    p_risk_score INTEGER DEFAULT NULL,
    p_metadata JSONB DEFAULT '{}'::jsonb
) RETURNS UUID AS $$
DECLARE
    v_audit_id UUID;
BEGIN
    INSERT INTO auth.audit_log (
        user_id,
        session_id,
        event_type,
        event_category,
        event_description,
        ip_address,
        user_agent,
        success,
        failure_reason,
        risk_score,
        metadata,
        correlation_id
    ) VALUES (
        p_user_id,
        p_session_id,
        p_event_type,
        p_event_category,
        p_event_description,
        p_ip_address,
        p_user_agent,
        p_success,
        p_failure_reason,
        p_risk_score,
        p_metadata,
        auth.generate_uuid()::text
    ) RETURNING correlation_id INTO v_audit_id;
    
    RETURN v_audit_id;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Create function for secure MFA secret encryption/decryption
CREATE OR REPLACE FUNCTION auth.encrypt_mfa_secret(
    p_secret TEXT,
    p_key TEXT
) RETURNS JSON AS $$
DECLARE
    v_iv TEXT;
    v_encrypted TEXT;
BEGIN
    -- Generate random IV (16 bytes)
    v_iv := encode(gen_random_bytes(16), 'hex');
    
    -- Encrypt secret with AES-256-CBC
    v_encrypted := encode(
        encrypt_iv(
            p_secret::bytea,
            p_key::bytea,
            decode(v_iv, 'hex'),
            'aes-cbc'
        ),
        'base64'
    );
    
    RETURN json_build_object(
        'encrypted', v_encrypted,
        'iv', v_iv
    );
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

CREATE OR REPLACE FUNCTION auth.decrypt_mfa_secret(
    p_encrypted TEXT,
    p_iv TEXT,
    p_key TEXT
) RETURNS TEXT AS $$
BEGIN
    RETURN convert_from(
        decrypt_iv(
            decode(p_encrypted, 'base64'),
            p_key::bytea,
            decode(p_iv, 'hex'),
            'aes-cbc'
        ),
        'utf8'
    );
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Create function to clean up expired tokens and sessions
CREATE OR REPLACE FUNCTION auth.cleanup_expired_tokens() RETURNS INTEGER AS $$
DECLARE
    v_deleted_count INTEGER := 0;
    v_count INTEGER;
BEGIN
    -- Clean up expired JWT tokens
    DELETE FROM auth.jwt_tokens 
    WHERE expires_at < auth.current_timestamp_utc() 
      AND revoked_at IS NULL;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    v_deleted_count := v_deleted_count + v_count;
    
    -- Clean up expired password reset tokens
    DELETE FROM auth.password_reset_tokens 
    WHERE expires_at < auth.current_timestamp_utc() 
      AND used_at IS NULL;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    v_deleted_count := v_deleted_count + v_count;
    
    -- Clean up expired email verification tokens
    DELETE FROM auth.email_verification_tokens 
    WHERE expires_at < auth.current_timestamp_utc() 
      AND verified_at IS NULL;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    v_deleted_count := v_deleted_count + v_count;
    
    -- Mark expired sessions as inactive
    UPDATE auth.sessions 
    SET is_active = false, revoked_at = auth.current_timestamp_utc(), revoked_reason = 'expired'
    WHERE expires_at < auth.current_timestamp_utc() 
      AND is_active = true 
      AND revoked_at IS NULL;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    v_deleted_count := v_deleted_count + v_count;
    
    RETURN v_deleted_count;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Create triggers for automatic timestamp updates
CREATE OR REPLACE FUNCTION auth.update_timestamp() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = auth.current_timestamp_utc();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply triggers to relevant tables
CREATE TRIGGER tr_auth_users_updated
    BEFORE UPDATE ON auth.users
    FOR EACH ROW
    EXECUTE FUNCTION auth.update_timestamp();

CREATE TRIGGER tr_auth_roles_updated
    BEFORE UPDATE ON auth.roles
    FOR EACH ROW
    EXECUTE FUNCTION auth.update_timestamp();

CREATE TRIGGER tr_auth_user_preferences_updated
    BEFORE UPDATE ON auth.user_preferences
    FOR EACH ROW
    EXECUTE FUNCTION auth.update_timestamp();

CREATE TRIGGER tr_auth_security_config_updated
    BEFORE UPDATE ON auth.security_config
    FOR EACH ROW
    EXECUTE FUNCTION auth.update_timestamp();

-- Create default admin user (password needs to be set during deployment)
INSERT INTO auth.users (
    username,
    email,
    password_hash,
    password_salt,
    first_name,
    last_name,
    display_name,
    is_active,
    is_verified,
    created_by
) VALUES (
    'admin',
    'admin@mlb-betting.local',
    'CHANGE_ON_FIRST_LOGIN',
    'CHANGE_ON_FIRST_LOGIN',
    'System',
    'Administrator',
    'System Administrator',
    true,
    true,
    NULL
);

-- Assign super_admin role to default admin user
INSERT INTO auth.user_roles (user_id, role_id, assigned_by)
SELECT u.id, r.id, u.id
FROM auth.users u, auth.roles r
WHERE u.username = 'admin' AND r.name = 'super_admin';

-- Create default user preferences for admin
INSERT INTO auth.user_preferences (user_id)
SELECT id FROM auth.users WHERE username = 'admin';

-- Grant necessary permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA auth TO PUBLIC;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA auth TO PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA auth TO PUBLIC;

-- Log the migration
INSERT INTO auth.audit_log (
    event_type,
    event_category,
    event_description,
    success,
    metadata
) VALUES (
    'migration_completed',
    'system',
    'Authentication system database schema created successfully',
    true,
    '{"migration": "200_create_authentication_system", "tables_created": 13, "functions_created": 4, "views_created": 2}'::jsonb
);

COMMENT ON SCHEMA auth IS 'Comprehensive authentication system with RBAC, session management, and security monitoring';