# 🔒 ML Pipeline Security Implementation Guide

**MLB Betting ML Pipeline - Production Security Standards**

## Overview

This document outlines comprehensive security measures implemented for the MLB ML prediction pipeline, addressing authentication, authorization, data protection, monitoring, and operational security requirements for production deployment.

## 🛡️ Security Architecture

### Multi-Layer Security Model

```
┌─────────────────────────────────────────────────────────────┐
│                    Network Security Layer                   │
├─────────────────────────────────────────────────────────────┤
│                  Application Security Layer                 │
├─────────────────────────────────────────────────────────────┤
│                     Data Security Layer                     │
├─────────────────────────────────────────────────────────────┤
│                Infrastructure Security Layer                │
└─────────────────────────────────────────────────────────────┘
```

### Core Security Principles

1. **Zero Trust Architecture**: Verify every request, encrypt all data
2. **Defense in Depth**: Multiple security layers with no single point of failure
3. **Principle of Least Privilege**: Minimal access permissions for each component
4. **Security by Default**: Secure configurations out-of-the-box
5. **Continuous Monitoring**: Real-time threat detection and response

## 🔐 Authentication & Authorization

### API Authentication Strategy

**Primary Authentication**: Bearer Token (API Key)
- **Production**: HMAC-SHA256 signed tokens with 256-bit entropy
- **Rotation**: Automated 30-day rotation with 7-day overlap window
- **Storage**: Environment variables with secrets management integration

```python
# Environment Configuration
API_SECRET_KEY=<256-bit-hex-key>
API_KEY_ROTATION_DAYS=30
REQUIRE_AUTH=true  # production
ALLOWED_API_VERSIONS=v1,v2
```

### Access Control Matrix

| Role | Endpoints | Permissions | Rate Limits |
|------|-----------|-------------|-------------|
| **Public** | `/health`, `/` | Read-only | 100 req/min |
| **Consumer** | `/api/v1/predict` | Predictions only | 60 req/min |
| **Premium** | `/api/v1/predict`, `/api/v1/models` | Full prediction access | 300 req/min |
| **Admin** | All endpoints | Full access | 1000 req/min |

### Token-Based Authentication Implementation

```python
class EnhancedAuthenticator:
    """Production-grade API authentication"""
    
    def __init__(self):
        self.secret_key = os.getenv("API_SECRET_KEY")
        self.token_expiry = int(os.getenv("TOKEN_EXPIRY_HOURS", "24")) * 3600
        
    def generate_token(self, user_id: str, role: str) -> str:
        """Generate HMAC-signed JWT token"""
        payload = {
            "user_id": user_id,
            "role": role,
            "issued_at": time.time(),
            "expires_at": time.time() + self.token_expiry,
            "api_version": "v1"
        }
        return jwt.encode(payload, self.secret_key, algorithm="HS256")
        
    def verify_token(self, token: str) -> Dict[str, Any]:
        """Verify and decode JWT token"""
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=["HS256"])
            
            # Check expiration
            if time.time() > payload.get("expires_at", 0):
                raise HTTPException(401, "Token expired")
                
            return payload
        except jwt.InvalidTokenError:
            raise HTTPException(401, "Invalid token")
```

## 🔒 Data Protection

### Encryption Standards

**Data at Rest**:
- **Database**: AES-256 encryption for sensitive columns
- **Redis**: TLS encryption + AUTH password protection
- **File Storage**: GPG encryption for model artifacts
- **Backups**: Encrypted with customer-managed keys

**Data in Transit**:
- **API Communication**: TLS 1.3 minimum
- **Internal Services**: mTLS (mutual TLS) authentication
- **Database Connections**: SSL/TLS with certificate validation
- **Redis Connections**: TLS with AUTH and ACL controls

### Redis Security Configuration

```yaml
# redis.conf - Production Security Settings
requirepass <strong-redis-password>
tls-port 6380
tls-cert-file /etc/redis/tls/redis.crt
tls-key-file /etc/redis/tls/redis.key
tls-ca-cert-file /etc/redis/tls/ca.crt
tls-auth-clients yes

# Access Control Lists
user ml-api on >ml-api-password ~ml:* +@read +@write +@connection
user ml-monitor on >monitor-password ~ml:* +@read +@connection
user default off
```

### Database Security

```python
# Enhanced Database Connection with Security
class SecureDBConfig:
    def __init__(self):
        self.host = os.getenv("DB_HOST", "localhost")
        self.port = int(os.getenv("DB_PORT", "5432"))
        self.database = os.getenv("DB_NAME", "mlb_betting")
        self.username = os.getenv("DB_USER", "ml_user")
        self.password = os.getenv("DB_PASSWORD")
        
        # Security settings
        self.ssl_mode = "require"
        self.ssl_cert = os.getenv("DB_SSL_CERT")
        self.ssl_key = os.getenv("DB_SSL_KEY") 
        self.ssl_rootcert = os.getenv("DB_SSL_ROOTCERT")
        
        # Connection security
        self.pool_size = 5
        self.max_overflow = 10
        self.pool_timeout = 30
        self.pool_recycle = 3600  # 1 hour
        
    def get_secure_url(self) -> str:
        """Generate secure database connection URL"""
        return (
            f"postgresql+asyncpg://{self.username}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}"
            f"?ssl=require&sslcert={self.ssl_cert}"
            f"&sslkey={self.ssl_key}&sslrootcert={self.ssl_rootcert}"
        )
```

## 🚨 Monitoring & Alerting

### Security Monitoring Framework

**Real-time Security Metrics**:
- Authentication failure rates (>5% triggers alert)
- Rate limiting violations (>10/hour triggers review)
- Suspicious IP activity (geolocation anomalies)
- API abuse patterns (unusual request sequences)
- Data access anomalies (off-hours access, bulk downloads)

### Security Event Logging

```python
class SecurityLogger:
    """Centralized security event logging"""
    
    def __init__(self):
        self.logger = logging.getLogger("security")
        self.handler = logging.handlers.SysLogHandler(address=('localhost', 514))
        self.formatter = logging.Formatter(
            '%(asctime)s SECURITY [%(levelname)s] %(message)s'
        )
        self.handler.setFormatter(self.formatter)
        self.logger.addHandler(self.handler)
        
    def log_auth_attempt(self, ip: str, user_agent: str, success: bool, user_id: str = None):
        """Log authentication attempts"""
        event = {
            "event_type": "auth_attempt",
            "ip_address": ip,
            "user_agent": user_agent,
            "success": success,
            "user_id": user_id,
            "timestamp": time.time()
        }
        
        if success:
            self.logger.info(f"AUTH_SUCCESS: {json.dumps(event)}")
        else:
            self.logger.warning(f"AUTH_FAILURE: {json.dumps(event)}")
            
    def log_rate_limit_violation(self, ip: str, endpoint: str, limit: int):
        """Log rate limit violations"""
        event = {
            "event_type": "rate_limit_violation",
            "ip_address": ip,
            "endpoint": endpoint,
            "limit_exceeded": limit,
            "timestamp": time.time()
        }
        self.logger.warning(f"RATE_LIMIT_VIOLATION: {json.dumps(event)}")
```

### Alert Thresholds

| Security Event | Warning Threshold | Critical Threshold | Response |
|----------------|-------------------|-------------------|----------|
| **Failed Auth** | 10/hour from IP | 50/hour from IP | Temporary IP ban |
| **Rate Limit** | 5 violations/hour | 20 violations/hour | Account suspension |
| **Data Access** | Off-hours activity | Bulk data download | Admin notification |
| **API Abuse** | Suspicious patterns | Automated attacks | WAF blocking |

## 🛠️ Error Handling & Circuit Breakers

### Comprehensive Error Handling Matrix

```python
class SecurityAwareErrorHandler:
    """Security-conscious error handling"""
    
    ERROR_RESPONSES = {
        # Don't reveal internal structure
        400: "Invalid request format",
        401: "Authentication required", 
        403: "Access denied",
        404: "Resource not found",
        429: "Rate limit exceeded",
        500: "Internal server error"
    }
    
    def __init__(self):
        self.logger = SecurityLogger()
        
    def handle_error(self, error: Exception, request: Request) -> JSONResponse:
        """Handle errors without information disclosure"""
        
        # Log detailed error internally
        self.logger.log_error(
            error_type=type(error).__name__,
            error_message=str(error),
            request_path=request.url.path,
            ip_address=request.client.host,
            user_agent=request.headers.get("user-agent")
        )
        
        # Return sanitized error to client
        if isinstance(error, HTTPException):
            return JSONResponse(
                status_code=error.status_code,
                content={
                    "error": self.ERROR_RESPONSES.get(error.status_code, "Unknown error"),
                    "request_id": generate_request_id(),
                    "timestamp": time.time()
                }
            )
```

### Circuit Breaker Implementation

```python
class CircuitBreaker:
    """Circuit breaker for external service protection"""
    
    def __init__(self, failure_threshold: int = 5, timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        
    async def call(self, func, *args, **kwargs):
        """Execute function with circuit breaker protection"""
        
        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.timeout:
                self.state = "HALF_OPEN"
            else:
                raise HTTPException(503, "Service temporarily unavailable")
                
        try:
            result = await func(*args, **kwargs)
            
            # Reset on success
            if self.state == "HALF_OPEN":
                self.state = "CLOSED"
                self.failure_count = 0
                
            return result
            
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.failure_threshold:
                self.state = "OPEN"
                
            raise e
```

## 🏗️ Infrastructure Security

### Container Security

```dockerfile
# Dockerfile security best practices
FROM python:3.10-slim

# Create non-root user
RUN groupadd -r mlapi && useradd -r -g mlapi mlapi

# Install security updates
RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Set secure permissions
COPY --chown=mlapi:mlapi . /app
WORKDIR /app

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Switch to non-root user
USER mlapi

# Security labels
LABEL security.scan="required" \
      security.compliance="SOC2"

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["uvicorn", "src.ml.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### Network Security

```yaml
# docker-compose.yml - Production Network Security
version: '3.8'

services:
  ml-api:
    build: .
    ports:
      - "443:8000"  # HTTPS only
    networks:
      - ml-internal
    environment:
      - ENVIRONMENT=production
      - TLS_CERT_PATH=/certs/server.crt
      - TLS_KEY_PATH=/certs/server.key
    volumes:
      - ./certs:/certs:ro
      - ./logs:/app/logs
    deploy:
      resources:
        limits:
          memory: 1G
          cpus: '0.5'
        reservations:
          memory: 512M
          cpus: '0.25'

  redis:
    image: redis:7-alpine
    command: redis-server /usr/local/etc/redis/redis.conf
    volumes:
      - ./redis/redis.conf:/usr/local/etc/redis/redis.conf:ro
      - ./redis/certs:/certs:ro
    networks:
      - ml-internal
    ports:
      - "6380:6380"  # TLS port only

networks:
  ml-internal:
    driver: bridge
    internal: true
    ipam:
      config:
        - subnet: 172.20.0.0/16
```

## 📊 Compliance & Auditing

### Security Compliance Framework

**Standards Compliance**:
- **SOC 2 Type II**: Security, availability, and confidentiality
- **ISO 27001**: Information security management
- **GDPR**: Data protection and privacy (if applicable)
- **PCI DSS**: Payment card data security (if applicable)

### Audit Trail Requirements

```python
class AuditLogger:
    """Comprehensive audit logging"""
    
    def __init__(self):
        self.audit_db = AuditDatabase()
        
    async def log_data_access(self, user_id: str, resource: str, action: str, 
                             ip_address: str, success: bool):
        """Log all data access events"""
        audit_event = {
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.utcnow(),
            "user_id": user_id,
            "resource": resource,
            "action": action,
            "ip_address": ip_address,
            "success": success,
            "user_agent": request.headers.get("user-agent"),
            "session_id": request.headers.get("x-session-id")
        }
        
        await self.audit_db.insert_event(audit_event)
        
    async def log_model_prediction(self, user_id: str, model_name: str, 
                                  game_id: str, prediction: Dict[str, Any]):
        """Log model predictions for audit purposes"""
        prediction_event = {
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.utcnow(),
            "user_id": user_id,
            "model_name": model_name,
            "game_id": game_id,
            "prediction_hash": hashlib.sha256(
                str(prediction).encode()
            ).hexdigest(),
            "ip_address": request.client.host
        }
        
        await self.audit_db.insert_prediction_event(prediction_event)
```

## 🚀 Deployment Security

### Production Deployment Checklist

**Pre-Deployment Security Validation**:

- [ ] **Secrets Management**: All secrets in environment variables or vault
- [ ] **TLS Configuration**: Valid certificates with 2048-bit+ keys
- [ ] **Database Security**: SSL connections, encrypted storage
- [ ] **Redis Security**: AUTH enabled, TLS configured, ACL rules
- [ ] **Container Security**: Non-root user, minimal image, security scanning
- [ ] **Network Security**: Firewall rules, internal networks, load balancer
- [ ] **Monitoring**: Security logs, alerting, anomaly detection
- [ ] **Backup Security**: Encrypted backups, access controls
- [ ] **Incident Response**: Runbooks, contact lists, escalation procedures

### Environment-Specific Configurations

```python
# config/production.py
SECURITY_CONFIG = {
    "require_https": True,
    "require_auth": True,
    "enable_rate_limiting": True,
    "log_level": "INFO",
    "enable_audit_logging": True,
    "session_timeout": 3600,  # 1 hour
    "token_expiry": 86400,    # 24 hours
    "max_request_size": 1024 * 1024,  # 1MB
    "trusted_proxies": ["10.0.0.0/8", "172.16.0.0/12"],
    "cors_origins": ["https://yourdomain.com"],
    "csp_policy": "default-src 'self'; script-src 'self' 'unsafe-inline'",
}

# config/staging.py  
SECURITY_CONFIG = {
    "require_https": True,
    "require_auth": True,
    "enable_rate_limiting": True,
    "log_level": "DEBUG",
    "enable_audit_logging": True,
    "session_timeout": 7200,  # 2 hours
    "token_expiry": 86400,    # 24 hours
    "cors_origins": ["https://staging.yourdomain.com"],
}

# config/development.py
SECURITY_CONFIG = {
    "require_https": False,
    "require_auth": False,
    "enable_rate_limiting": False,
    "log_level": "DEBUG",
    "enable_audit_logging": False,
    "cors_origins": ["http://localhost:3000", "http://localhost:8080"],
}
```

## 🔧 Security Testing

### Security Test Suite

```python
class SecurityTestSuite:
    """Comprehensive security testing"""
    
    async def test_authentication_bypass(self):
        """Test for authentication bypass vulnerabilities"""
        # Test endpoints without authentication
        # Test malformed tokens
        # Test expired tokens
        # Test token reuse
        
    async def test_authorization_escalation(self):
        """Test for privilege escalation"""
        # Test role-based access controls
        # Test horizontal privilege escalation
        # Test vertical privilege escalation
        
    async def test_injection_attacks(self):
        """Test for injection vulnerabilities"""
        # SQL injection tests
        # NoSQL injection tests
        # Command injection tests
        # Header injection tests
        
    async def test_rate_limiting(self):
        """Test rate limiting effectiveness"""
        # Test rate limit enforcement
        # Test rate limit bypass attempts
        # Test distributed rate limiting
        
    async def test_data_protection(self):
        """Test data protection measures"""
        # Test encryption at rest
        # Test encryption in transit
        # Test sensitive data exposure
        # Test data leakage
```

## 📈 Security Metrics & KPIs

### Security Performance Indicators

| Metric | Target | Warning | Critical |
|--------|--------|---------|----------|
| **Authentication Success Rate** | >99% | <97% | <95% |
| **Failed Login Rate** | <1% | >3% | >5% |
| **API Response Time** | <100ms | >500ms | >1s |
| **Rate Limit Violations** | <5/day | >20/day | >50/day |
| **Security Alert Response** | <15min | >30min | >1hour |
| **Vulnerability Patching** | <24h | >72h | >1week |
| **Security Scan Failures** | 0 | >0 high | >0 critical |

### Continuous Security Monitoring

```python
class SecurityMetricsCollector:
    """Collect and report security metrics"""
    
    def __init__(self):
        self.metrics_client = PrometheusClient()
        
    def record_auth_attempt(self, success: bool, method: str):
        """Record authentication attempt metrics"""
        self.metrics_client.counter(
            "auth_attempts_total",
            labels={"success": success, "method": method}
        ).inc()
        
    def record_rate_limit_violation(self, endpoint: str, ip: str):
        """Record rate limit violations"""
        self.metrics_client.counter(
            "rate_limit_violations_total",
            labels={"endpoint": endpoint, "ip_hash": hash(ip) % 1000}
        ).inc()
        
    def record_security_event(self, event_type: str, severity: str):
        """Record security events"""
        self.metrics_client.counter(
            "security_events_total",
            labels={"type": event_type, "severity": severity}
        ).inc()
```

## 🚑 Incident Response

### Security Incident Response Plan

**Phase 1: Detection & Analysis (0-15 minutes)**
1. Automated alert triggers
2. Security team notification
3. Initial impact assessment
4. Incident classification

**Phase 2: Containment (15-60 minutes)**
1. Isolate affected systems
2. Preserve evidence
3. Implement temporary fixes
4. Update stakeholders

**Phase 3: Eradication & Recovery (1-24 hours)**
1. Remove threat vectors
2. Patch vulnerabilities
3. Restore services
4. Implement monitoring

**Phase 4: Post-Incident (24-72 hours)**
1. Incident report
2. Lessons learned
3. Process improvements
4. Security updates

### Emergency Contacts

```yaml
# Incident Response Team
security_team:
  primary: security@company.com
  phone: +1-XXX-XXX-XXXX
  
engineering_team:
  primary: engineering@company.com
  phone: +1-XXX-XXX-XXXX
  
management:
  primary: management@company.com
  phone: +1-XXX-XXX-XXXX

# External Resources
security_vendor: vendor@security-company.com
legal_counsel: legal@law-firm.com
insurance: claims@cyber-insurance.com
```

---

## 📚 Additional Resources

- [OWASP API Security Top 10](https://owasp.org/www-project-api-security/)
- [NIST Cybersecurity Framework](https://www.nist.gov/cyberframework)
- [FastAPI Security Documentation](https://fastapi.tiangolo.com/tutorial/security/)
- [Redis Security Guidelines](https://redis.io/topics/security)
- [PostgreSQL Security Guide](https://www.postgresql.org/docs/current/security.html)

---

**Document Version**: 1.0  
**Last Updated**: 2025-01-30  
**Next Review**: 2025-04-30  
**Classification**: Internal Use Only