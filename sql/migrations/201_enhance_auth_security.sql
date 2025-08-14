-- Migration 201: Enhance Authentication Security
-- Improves MFA secret encryption, password history verification,
-- and adds comprehensive security enhancements.

-- Enable pgcrypto extension for encryption functions
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Create enhanced password history verification function
CREATE OR REPLACE FUNCTION auth.verify_password_not_reused(
    p_user_id INTEGER,
    p_new_password TEXT,
    p_history_count INTEGER DEFAULT 5
) RETURNS BOOLEAN AS $$
DECLARE
    v_history_record RECORD;
    v_salt TEXT;
    v_hashed_password TEXT;
    v_count INTEGER := 0;
BEGIN
    -- Get the user's current password salt pattern for consistent hashing
    SELECT password_salt INTO v_salt FROM auth.users WHERE id = p_user_id;
    
    IF v_salt IS NULL THEN
        RAISE EXCEPTION 'User not found: %', p_user_id;
    END IF;
    
    -- Check against current password first
    SELECT password_hash INTO v_hashed_password FROM auth.users WHERE id = p_user_id;
    IF v_hashed_password IS NOT NULL THEN
        -- In practice, this would use bcrypt verification
        -- For now, we'll check if the hashed versions match
        IF encode(digest(p_new_password || v_salt, 'sha256'), 'hex') = v_hashed_password THEN
            RETURN FALSE;
        END IF;
    END IF;
    
    -- Check against password history
    FOR v_history_record IN 
        SELECT password_hash, password_salt 
        FROM auth.password_history 
        WHERE user_id = p_user_id 
        ORDER BY created_at DESC 
        LIMIT p_history_count
    LOOP
        v_count := v_count + 1;
        
        -- Verify against historical password
        -- In practice, this would use proper bcrypt verification
        IF encode(digest(p_new_password || v_history_record.password_salt, 'sha256'), 'hex') = v_history_record.password_hash THEN
            RETURN FALSE;
        END IF;
    END LOOP;
    
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Create function to securely store password in history
CREATE OR REPLACE FUNCTION auth.add_password_to_history(
    p_user_id INTEGER,
    p_password_hash TEXT,
    p_password_salt TEXT
) RETURNS VOID AS $$
DECLARE
    v_history_count INTEGER;
    v_max_history INTEGER := 5;
BEGIN
    -- Insert new password into history
    INSERT INTO auth.password_history (user_id, password_hash, password_salt)
    VALUES (p_user_id, p_password_hash, p_password_salt);
    
    -- Clean up old password history entries (keep only last N)
    SELECT COUNT(*) INTO v_history_count 
    FROM auth.password_history 
    WHERE user_id = p_user_id;
    
    IF v_history_count > v_max_history THEN
        DELETE FROM auth.password_history 
        WHERE id IN (
            SELECT id 
            FROM auth.password_history 
            WHERE user_id = p_user_id 
            ORDER BY created_at ASC 
            LIMIT (v_history_count - v_max_history)
        );
    END IF;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Create enhanced MFA backup code encryption function
CREATE OR REPLACE FUNCTION auth.encrypt_backup_codes(
    p_codes TEXT[],
    p_encryption_key TEXT
) RETURNS TEXT[] AS $$
DECLARE
    v_encrypted_codes TEXT[] := '{}';
    v_code TEXT;
    v_iv TEXT;
    v_encrypted TEXT;
BEGIN
    FOREACH v_code IN ARRAY p_codes
    LOOP
        -- Generate unique IV for each code
        v_iv := encode(gen_random_bytes(16), 'hex');
        
        -- Encrypt the code
        v_encrypted := encode(
            encrypt_iv(
                v_code::bytea,
                p_encryption_key::bytea,
                decode(v_iv, 'hex'),
                'aes-cbc'
            ),
            'base64'
        );
        
        -- Store as iv:encrypted format
        v_encrypted_codes := array_append(v_encrypted_codes, v_iv || ':' || v_encrypted);
    END LOOP;
    
    RETURN v_encrypted_codes;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Create function to decrypt backup codes
CREATE OR REPLACE FUNCTION auth.decrypt_backup_codes(
    p_encrypted_codes TEXT[],
    p_encryption_key TEXT
) RETURNS TEXT[] AS $$
DECLARE
    v_decrypted_codes TEXT[] := '{}';
    v_encrypted_code TEXT;
    v_iv TEXT;
    v_encrypted TEXT;
    v_decrypted TEXT;
    v_parts TEXT[];
BEGIN
    FOREACH v_encrypted_code IN ARRAY p_encrypted_codes
    LOOP
        -- Split iv:encrypted format
        v_parts := string_to_array(v_encrypted_code, ':');
        IF array_length(v_parts, 1) != 2 THEN
            CONTINUE; -- Skip malformed entries
        END IF;
        
        v_iv := v_parts[1];
        v_encrypted := v_parts[2];
        
        -- Decrypt the code
        v_decrypted := convert_from(
            decrypt_iv(
                decode(v_encrypted, 'base64'),
                p_encryption_key::bytea,
                decode(v_iv, 'hex'),
                'aes-cbc'
            ),
            'utf8'
        );
        
        v_decrypted_codes := array_append(v_decrypted_codes, v_decrypted);
    END LOOP;
    
    RETURN v_decrypted_codes;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Create function for secure API key generation
CREATE OR REPLACE FUNCTION auth.generate_secure_api_key() RETURNS JSON AS $$
DECLARE
    v_key_bytes BYTEA;
    v_key TEXT;
    v_prefix TEXT;
    v_hash TEXT;
BEGIN
    -- Generate 32 random bytes for the key
    v_key_bytes := gen_random_bytes(32);
    
    -- Create base64 encoded key
    v_key := 'sk_' || encode(v_key_bytes, 'base64');
    
    -- Extract prefix (first 8 characters after 'sk_')
    v_prefix := substring(v_key from 1 for 11); -- 'sk_' + 8 chars
    
    -- Create SHA-256 hash for storage
    v_hash := encode(digest(v_key, 'sha256'), 'hex');
    
    RETURN json_build_object(
        'key', v_key,
        'prefix', v_prefix,
        'hash', v_hash
    );
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Create improved session security validation
CREATE OR REPLACE FUNCTION auth.validate_session_security(
    p_user_id INTEGER,
    p_ip_address INET,
    p_user_agent TEXT,
    p_device_fingerprint TEXT DEFAULT NULL
) RETURNS JSON AS $$
DECLARE
    v_risk_score INTEGER := 0;
    v_risk_factors TEXT[] := '{}';
    v_last_ip INET;
    v_session_count INTEGER;
    v_suspicious_activity BOOLEAN := FALSE;
BEGIN
    -- Check for IP address changes
    SELECT ip_address INTO v_last_ip 
    FROM auth.sessions 
    WHERE user_id = p_user_id 
      AND is_active = true 
    ORDER BY last_activity DESC 
    LIMIT 1;
    
    IF v_last_ip IS NOT NULL AND v_last_ip != p_ip_address THEN
        v_risk_score := v_risk_score + 20;
        v_risk_factors := array_append(v_risk_factors, 'IP address change');
    END IF;
    
    -- Check for multiple active sessions
    SELECT COUNT(*) INTO v_session_count 
    FROM auth.sessions 
    WHERE user_id = p_user_id 
      AND is_active = true 
      AND expires_at > NOW();
    
    IF v_session_count > 3 THEN
        v_risk_score := v_risk_score + 15;
        v_risk_factors := array_append(v_risk_factors, 'Multiple active sessions');
    END IF;
    
    -- Check for rapid login attempts
    SELECT COUNT(*) > 5 INTO v_suspicious_activity
    FROM auth.audit_log 
    WHERE user_id = p_user_id 
      AND event_type = 'login_attempt'
      AND timestamp > NOW() - INTERVAL '1 hour';
    
    IF v_suspicious_activity THEN
        v_risk_score := v_risk_score + 30;
        v_risk_factors := array_append(v_risk_factors, 'Rapid login attempts');
    END IF;
    
    -- Determine risk level
    RETURN json_build_object(
        'risk_score', v_risk_score,
        'risk_level', CASE 
            WHEN v_risk_score >= 50 THEN 'high'
            WHEN v_risk_score >= 25 THEN 'medium'
            ELSE 'low'
        END,
        'risk_factors', v_risk_factors,
        'require_mfa', v_risk_score >= 25,
        'allow_login', v_risk_score < 75
    );
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Create comprehensive security configuration view
CREATE OR REPLACE VIEW auth.security_policy AS
SELECT 
    config_key,
    config_value::INTEGER AS int_value,
    config_value::BOOLEAN AS bool_value,
    config_value AS text_value,
    description,
    CASE 
        WHEN config_key LIKE '%_days' THEN INTERVAL '1 day' * config_value::INTEGER
        WHEN config_key LIKE '%_minutes' THEN INTERVAL '1 minute' * config_value::INTEGER  
        WHEN config_key LIKE '%_hours' THEN INTERVAL '1 hour' * config_value::INTEGER
        ELSE NULL
    END AS interval_value
FROM auth.security_config;

-- Create comprehensive audit trail view for security monitoring
CREATE OR REPLACE VIEW auth.security_audit_trail AS
SELECT 
    al.id,
    al.user_id,
    u.username,
    al.event_type,
    al.event_category,
    al.event_description,
    al.ip_address,
    al.success,
    al.failure_reason,
    al.risk_score,
    al.timestamp,
    al.correlation_id,
    -- Security flags
    CASE WHEN al.risk_score >= 75 THEN 'critical'
         WHEN al.risk_score >= 50 THEN 'high'
         WHEN al.risk_score >= 25 THEN 'medium'
         ELSE 'low'
    END AS security_level,
    
    -- Geographic indicators (placeholder for real geolocation)
    CASE WHEN al.ip_address IS NOT NULL THEN 
        host(al.ip_address) 
    END AS ip_string,
    
    -- Time-based patterns
    EXTRACT(hour FROM al.timestamp) AS hour_of_day,
    EXTRACT(dow FROM al.timestamp) AS day_of_week,
    
    -- Session context
    s.device_fingerprint,
    s.user_agent,
    s.is_mobile,
    s.is_trusted_device
FROM auth.audit_log al
LEFT JOIN auth.users u ON al.user_id = u.id
LEFT JOIN auth.sessions s ON al.session_id = s.session_id
ORDER BY al.timestamp DESC;

-- Add indexes for new security functions
CREATE INDEX IF NOT EXISTS idx_auth_password_history_user_created 
ON auth.password_history(user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_auth_audit_log_user_timestamp 
ON auth.audit_log(user_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_auth_audit_log_risk_timestamp 
ON auth.audit_log(risk_score DESC, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_auth_sessions_user_active_expires 
ON auth.sessions(user_id, is_active, expires_at);

-- Grant permissions for new functions
GRANT EXECUTE ON FUNCTION auth.verify_password_not_reused(INTEGER, TEXT, INTEGER) TO PUBLIC;
GRANT EXECUTE ON FUNCTION auth.add_password_to_history(INTEGER, TEXT, TEXT) TO PUBLIC;
GRANT EXECUTE ON FUNCTION auth.encrypt_backup_codes(TEXT[], TEXT) TO PUBLIC;
GRANT EXECUTE ON FUNCTION auth.decrypt_backup_codes(TEXT[], TEXT) TO PUBLIC;
GRANT EXECUTE ON FUNCTION auth.generate_secure_api_key() TO PUBLIC;
GRANT EXECUTE ON FUNCTION auth.validate_session_security(INTEGER, INET, TEXT, TEXT) TO PUBLIC;

-- Grant permissions for new views
GRANT SELECT ON auth.security_policy TO PUBLIC;
GRANT SELECT ON auth.security_audit_trail TO PUBLIC;

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
    'Enhanced authentication security features deployed',
    true,
    '{"migration": "201_enhance_auth_security", "features": ["mfa_encryption", "password_history_verification", "api_key_generation", "session_security_validation"], "functions_created": 6, "views_created": 2}'::jsonb
);

COMMENT ON FUNCTION auth.verify_password_not_reused(INTEGER, TEXT, INTEGER) IS 'Verifies new password is not in recent password history';
COMMENT ON FUNCTION auth.add_password_to_history(INTEGER, TEXT, TEXT) IS 'Securely stores password hash in history table';
COMMENT ON FUNCTION auth.encrypt_backup_codes(TEXT[], TEXT) IS 'Encrypts MFA backup codes with AES-256';
COMMENT ON FUNCTION auth.decrypt_backup_codes(TEXT[], TEXT) IS 'Decrypts MFA backup codes';
COMMENT ON FUNCTION auth.generate_secure_api_key() IS 'Generates cryptographically secure API key with hash';
COMMENT ON FUNCTION auth.validate_session_security(INTEGER, INET, TEXT, TEXT) IS 'Validates session security and calculates risk score';
COMMENT ON VIEW auth.security_policy IS 'Unified view of security configuration with type conversions';
COMMENT ON VIEW auth.security_audit_trail IS 'Comprehensive security audit trail with risk assessment';