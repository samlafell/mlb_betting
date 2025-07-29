# MLB Betting System - Production Security Guide

This document provides security configuration guidance for production deployments of the MLB betting system.

## 🔐 Critical Security Configuration

### 1. Environment Variables Security

**❌ NEVER commit .env files to version control**

```bash
# Remove any accidentally committed .env files
git rm --cached .env
git commit -m "Remove .env from tracking"

# Ensure .env is in .gitignore
echo ".env" >> .gitignore
```

**✅ Use .env.example as template**
```bash
cp .env.example .env
# Edit .env with your actual values
```

### 2. Break-Glass Endpoint Security

Break-glass endpoints provide emergency access to system controls and must be secured:

#### API Key Authentication
```bash
# Generate a strong API key (32+ characters)
DASHBOARD_API_KEY=$(openssl rand -hex 32)
ENABLE_AUTH=true
```

#### Rate Limiting
```bash
# Conservative rate limiting for production
BREAK_GLASS_RATE_LIMIT=3  # 3 requests per hour
ENABLE_RATE_LIMIT=true
```

#### IP Whitelisting (Recommended for Production)
```bash
# Enable IP whitelisting
ENABLE_IP_WHITELIST=true

# Whitelist specific IPs/networks
BREAK_GLASS_IP_WHITELIST=10.0.1.100,10.0.1.0/24,192.168.1.0/24
```

### 3. Redis Rate Limiting (Production)

For multi-instance deployments, use Redis for shared rate limiting:

```bash
# Install Redis
sudo apt-get install redis-server  # Ubuntu/Debian
brew install redis                  # macOS

# Configure Redis rate limiting
ENABLE_REDIS_RATE_LIMITING=true
REDIS_URL=redis://localhost:6379/0
```

**Redis Installation for Python:**
```bash
uv add redis
```

## 🛡️ Security Headers

The system automatically applies security headers to all responses:

- `X-Content-Type-Options: nosniff`
- `X-Frame-Options: DENY`
- `X-XSS-Protection: 1; mode=block`
- `Referrer-Policy: strict-origin-when-cross-origin` 
- `Content-Security-Policy: default-src 'self'`

## 📊 Security Audit Logging

All security events are logged with comprehensive audit information:

### Authentication Events
- API key validation attempts
- Rate limit violations
- IP whitelist violations
- Successful authentications

### Audit Trail Information
- Correlation IDs for request tracking
- Client IP addresses (proxy-aware)
- User agents and request headers
- Endpoint access patterns
- Timestamp precision

### Example Log Entry
```json
{
  "level": "WARNING",
  "message": "Invalid API key for break-glass endpoint",
  "correlation_id": "auth_1706558400123456",
  "endpoint": "/api/control/pipeline/execute",
  "client_ip": "192.168.1.100",
  "user_agent": "curl/7.68.0",
  "security_violation": "invalid_api_key",
  "api_key_prefix": "abc12345..."
}
```

## 🔧 Production Configuration

### Environment-Specific Settings

**Development:**
```bash
ENVIRONMENT=development
ENABLE_IP_WHITELIST=false
ENABLE_REDIS_RATE_LIMITING=false
BREAK_GLASS_RATE_LIMIT=5
```

**Production:**
```bash
ENVIRONMENT=production
ENABLE_IP_WHITELIST=true
ENABLE_REDIS_RATE_LIMITING=true
BREAK_GLASS_RATE_LIMIT=2
DASHBOARD_API_KEY=<secure-random-key>
BREAK_GLASS_IP_WHITELIST=<authorized-ips>
REDIS_URL=redis://redis-server:6379/0
```

### Database Connection Pooling

The system includes comprehensive database connection pooling:

```python
# Connection pool is automatically configured from settings
DATABASE_POOL_SIZE=10
DATABASE_MAX_OVERFLOW=20
DATABASE_POOL_TIMEOUT=30
DATABASE_POOL_RECYCLE=3600
```

**Production Recommendations:**
- Pool size: 10-20 connections
- Max overflow: 50% of pool size
- Timeout: 30 seconds
- Recycle: 1 hour

## 🚨 Incident Response

### If Credentials Are Exposed

1. **Immediate Actions:**
   ```bash
   # Remove from git
   git rm --cached .env
   git commit -m "Remove exposed credentials"
   
   # Invalidate exposed credentials
   # - Regenerate API keys
   # - Change database passwords
   # - Rotate email app passwords
   ```

2. **Generate New Credentials:**
   ```bash
   # New API key
   openssl rand -hex 32
   
   # New database password
   openssl rand -base64 24
   ```

3. **Update Configuration:**
   - Update all environment variables
   - Restart all services
   - Monitor logs for unauthorized access

### Security Monitoring

Monitor these log patterns for security issues:

```bash
# Failed authentication attempts
grep "security_violation" logs/security.log

# Rate limit violations  
grep "Rate limit exceeded" logs/security.log

# IP whitelist violations
grep "IP not whitelisted" logs/security.log
```

## 📋 Security Checklist

### Pre-Production
- [ ] .env file removed from version control
- [ ] Strong API key generated (32+ characters)
- [ ] IP whitelisting configured for authorized networks
- [ ] Redis configured for rate limiting
- [ ] Database connection pooling optimized
- [ ] Security logging enabled and monitored

### Post-Deployment
- [ ] Security headers verified in browser dev tools
- [ ] Rate limiting tested and working
- [ ] IP whitelisting tested and working
- [ ] Authentication tested with valid/invalid keys
- [ ] Security logs are being generated
- [ ] Database connections are pooled properly

### Regular Maintenance
- [ ] Rotate API keys quarterly
- [ ] Review and update IP whitelists
- [ ] Monitor security logs for anomalies
- [ ] Update Redis and database credentials
- [ ] Review rate limiting effectiveness

## 🛠️ Troubleshooting

### Common Issues

**Redis Connection Failed:**
```bash
# Check Redis status
redis-cli ping

# Check Redis configuration
redis-cli config get "*"

# Test connection from Python
python -c "import redis; r=redis.from_url('redis://localhost:6379'); print(r.ping())"
```

**IP Whitelist Not Working:**
- Check proxy headers: `X-Forwarded-For`, `X-Real-IP`
- Verify IP format (IPv4/IPv6/CIDR)
- Test with curl: `curl -H "X-Real-IP: 192.168.1.100" ...`

**Rate Limiting Issues:**
- Check Redis connectivity
- Verify time synchronization
- Monitor rate limit keys: `redis-cli keys "break_glass:*"`

## 📚 Additional Resources

- [FastAPI Security Documentation](https://fastapi.tiangolo.com/tutorial/security/)
- [Redis Security Guidelines](https://redis.io/topics/security)
- [OWASP Security Headers](https://owasp.org/www-project-secure-headers/)
- [Database Connection Pool Best Practices](https://www.postgresql.org/docs/current/runtime-config-connection.html)