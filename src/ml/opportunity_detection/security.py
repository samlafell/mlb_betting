"""
Security and input validation for ML Opportunity Detection System

Provides comprehensive security measures including:
- Input validation and sanitization
- Rate limiting and abuse prevention
- Audit logging with correlation IDs
- Security monitoring and alerting
"""

import re
import time
import hashlib
import logging
import asyncio
from typing import Dict, Any, List, Optional, Union, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from collections import defaultdict, deque
from enum import Enum
import uuid

from src.ml.opportunity_detection.config import get_security_config, SecurityConfig
from src.analysis.models.unified_models import UnifiedBettingSignal
from src.core.logging import get_logger, LogComponent


class SecurityViolationType(Enum):
    """Types of security violations"""
    RATE_LIMIT_EXCEEDED = "RATE_LIMIT_EXCEEDED"
    INVALID_INPUT = "INVALID_INPUT"
    MALICIOUS_PATTERN = "MALICIOUS_PATTERN"
    RESOURCE_ABUSE = "RESOURCE_ABUSE"
    AUTHENTICATION_FAILURE = "AUTHENTICATION_FAILURE"
    DATA_TAMPERING = "DATA_TAMPERING"
    SUSPICIOUS_ACTIVITY = "SUSPICIOUS_ACTIVITY"


class SecurityThreatLevel(Enum):
    """Security threat severity levels"""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


@dataclass
class SecurityEvent:
    """Security event data structure"""
    violation_type: SecurityViolationType
    threat_level: SecurityThreatLevel
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    user_id: Optional[str] = None
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    request_details: Dict[str, Any] = field(default_factory=dict)
    detected_at: datetime = field(default_factory=datetime.utcnow)
    correlation_id: Optional[str] = None


@dataclass 
class ValidationResult:
    """Input validation result"""
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    sanitized_data: Optional[Dict[str, Any]] = None
    threat_detected: bool = False


class RateLimiter:
    """Rate limiting implementation with sliding window"""
    
    def __init__(self, max_requests: int, window_minutes: int):
        self.max_requests = max_requests
        self.window_seconds = window_minutes * 60
        self.requests: Dict[str, deque] = defaultdict(deque)
        self.cleanup_interval = 300  # 5 minutes
        self.last_cleanup = time.time()
    
    def is_allowed(self, identifier: str) -> bool:
        """Check if request is allowed under rate limit"""
        now = time.time()
        
        # Cleanup old entries periodically
        if now - self.last_cleanup > self.cleanup_interval:
            self._cleanup_old_entries()
            self.last_cleanup = now
        
        # Get request history for this identifier
        request_times = self.requests[identifier]
        
        # Remove requests outside the window
        cutoff_time = now - self.window_seconds
        while request_times and request_times[0] < cutoff_time:
            request_times.popleft()
        
        # Check if under limit
        if len(request_times) >= self.max_requests:
            return False
        
        # Record this request
        request_times.append(now)
        return True
    
    def get_remaining_requests(self, identifier: str) -> int:
        """Get remaining requests for identifier"""
        now = time.time()
        request_times = self.requests[identifier]
        
        # Count requests in current window
        cutoff_time = now - self.window_seconds
        current_requests = sum(1 for t in request_times if t > cutoff_time)
        
        return max(0, self.max_requests - current_requests)
    
    def get_reset_time(self, identifier: str) -> Optional[datetime]:
        """Get when rate limit resets for identifier"""
        request_times = self.requests[identifier]
        if not request_times:
            return None
        
        oldest_request = request_times[0]
        reset_time = oldest_request + self.window_seconds
        return datetime.fromtimestamp(reset_time)
    
    def _cleanup_old_entries(self):
        """Clean up old rate limit entries"""
        now = time.time()
        cutoff_time = now - self.window_seconds * 2  # Keep some extra history
        
        for identifier, request_times in list(self.requests.items()):
            # Remove old requests
            while request_times and request_times[0] < cutoff_time:
                request_times.popleft()
            
            # Remove empty entries
            if not request_times:
                del self.requests[identifier]


class InputValidator:
    """Comprehensive input validation and sanitization"""
    
    def __init__(self, security_config: SecurityConfig):
        self.config = security_config
        self.logger = get_logger("ml.security", LogComponent.ANALYSIS)
        
        # Compile regex patterns for performance
        self.malicious_patterns = [
            re.compile(r'<script.*?>.*?</script>', re.IGNORECASE | re.DOTALL),
            re.compile(r'javascript:', re.IGNORECASE),
            re.compile(r'on\w+\s*=', re.IGNORECASE),
            re.compile(r'(union|select|insert|update|delete|drop|create|alter)\s', re.IGNORECASE),
            re.compile(r'[\'";]', re.IGNORECASE),
            re.compile(r'\\x[0-9a-f]{2}', re.IGNORECASE),
        ]
        
        self.game_id_pattern = re.compile(r'^[a-zA-Z0-9_\-]{1,50}$')
        self.user_id_pattern = re.compile(r'^[a-zA-Z0-9_\-\.@]{1,100}$')
    
    def validate_game_id(self, game_id: str) -> ValidationResult:
        """Validate game ID input"""
        result = ValidationResult(is_valid=True)
        
        if not game_id:
            result.is_valid = False
            result.errors.append("Game ID is required")
            return result
        
        if not isinstance(game_id, str):
            result.is_valid = False
            result.errors.append("Game ID must be a string")
            return result
        
        if len(game_id) > self.config.max_game_id_length:
            result.is_valid = False
            result.errors.append(f"Game ID too long (max {self.config.max_game_id_length} characters)")
        
        if not self.game_id_pattern.match(game_id):
            result.is_valid = False
            result.errors.append("Game ID contains invalid characters")
        
        # Check for malicious patterns
        for pattern in self.malicious_patterns:
            if pattern.search(game_id):
                result.is_valid = False
                result.threat_detected = True
                result.errors.append("Game ID contains potentially malicious content")
                break
        
        if result.is_valid:
            result.sanitized_data = {'game_id': game_id.strip()}
        
        return result
    
    def validate_user_id(self, user_id: Optional[str]) -> ValidationResult:
        """Validate user ID input"""
        result = ValidationResult(is_valid=True)
        
        if user_id is None:
            result.sanitized_data = {'user_id': None}
            return result
        
        if not isinstance(user_id, str):
            result.is_valid = False
            result.errors.append("User ID must be a string")
            return result
        
        if len(user_id) > 100:  # Reasonable limit
            result.is_valid = False
            result.errors.append("User ID too long")
        
        if not self.user_id_pattern.match(user_id):
            result.is_valid = False
            result.errors.append("User ID contains invalid characters")
        
        # Check for malicious patterns
        for pattern in self.malicious_patterns:
            if pattern.search(user_id):
                result.is_valid = False
                result.threat_detected = True
                result.errors.append("User ID contains potentially malicious content")
                break
        
        if result.is_valid:
            result.sanitized_data = {'user_id': user_id.strip()}
        
        return result
    
    def validate_signals(self, signals: List[UnifiedBettingSignal]) -> ValidationResult:
        """Validate betting signals input"""
        result = ValidationResult(is_valid=True)
        errors = []
        warnings = []
        
        if not signals:
            result.errors.append("At least one signal is required")
            result.is_valid = False
            return result
        
        if not isinstance(signals, list):
            result.errors.append("Signals must be a list")
            result.is_valid = False
            return result
        
        if len(signals) > self.config.max_signals_per_game:
            result.errors.append(f"Too many signals (max {self.config.max_signals_per_game})")
            result.is_valid = False
        
        validated_signals = []
        for i, signal in enumerate(signals):
            if not isinstance(signal, UnifiedBettingSignal):
                errors.append(f"Signal {i} is not a UnifiedBettingSignal instance")
                continue
            
            # Validate signal strength
            if not isinstance(signal.strength, (int, float)):
                errors.append(f"Signal {i} strength must be numeric")
                continue
            
            if not (0.0 <= signal.strength <= self.config.max_signal_strength):
                errors.append(f"Signal {i} strength out of valid range (0.0 to {self.config.max_signal_strength})")
                continue
            
            # Validate confidence
            if not isinstance(signal.confidence, (int, float)):
                errors.append(f"Signal {i} confidence must be numeric")
                continue
            
            if not (0.0 <= signal.confidence <= 1.0):
                errors.append(f"Signal {i} confidence must be between 0.0 and 1.0")
                continue
            
            # Validate timestamp
            if not isinstance(signal.timestamp, datetime):
                errors.append(f"Signal {i} timestamp must be a datetime object")
                continue
            
            # Check for suspiciously old signals
            age_hours = (datetime.utcnow() - signal.timestamp).total_seconds() / 3600
            if age_hours > 48:
                warnings.append(f"Signal {i} is {age_hours:.1f} hours old")
            
            validated_signals.append(signal)
        
        if errors:
            result.is_valid = False
            result.errors = errors
        else:
            result.sanitized_data = {'signals': validated_signals}
        
        result.warnings = warnings
        return result
    
    def validate_opportunity_request(self, 
                                   signals_by_game: Dict[str, List[UnifiedBettingSignal]],
                                   user_id: Optional[str] = None,
                                   market_data: Optional[Dict[str, Any]] = None) -> ValidationResult:
        """Validate complete opportunity discovery request"""
        result = ValidationResult(is_valid=True)
        sanitized_data = {}
        
        # Validate signals by game
        if not isinstance(signals_by_game, dict):
            result.is_valid = False
            result.errors.append("signals_by_game must be a dictionary")
            return result
        
        if not signals_by_game:
            result.is_valid = False
            result.errors.append("At least one game with signals is required")
            return result
        
        if len(signals_by_game) > self.config.max_games_per_request:
            result.is_valid = False
            result.errors.append(f"Too many games in request (max {self.config.max_games_per_request})")
            return result
        
        validated_games = {}
        for game_id, signals in signals_by_game.items():
            # Validate game ID
            game_validation = self.validate_game_id(game_id)
            if not game_validation.is_valid:
                result.is_valid = False
                result.errors.extend([f"Game {game_id}: {error}" for error in game_validation.errors])
                if game_validation.threat_detected:
                    result.threat_detected = True
                continue
            
            # Validate signals for this game
            signals_validation = self.validate_signals(signals)
            if not signals_validation.is_valid:
                result.is_valid = False
                result.errors.extend([f"Game {game_id}: {error}" for error in signals_validation.errors])
                continue
            
            validated_games[game_id] = signals_validation.sanitized_data['signals']
            result.warnings.extend([f"Game {game_id}: {warning}" for warning in signals_validation.warnings])
        
        # Validate user ID if provided
        if user_id is not None:
            user_validation = self.validate_user_id(user_id)
            if not user_validation.is_valid:
                result.is_valid = False
                result.errors.extend(user_validation.errors)
                if user_validation.threat_detected:
                    result.threat_detected = True
            else:
                sanitized_data['user_id'] = user_validation.sanitized_data['user_id']
        
        # Basic validation of market data if provided
        if market_data is not None:
            if not isinstance(market_data, dict):
                result.errors.append("market_data must be a dictionary")
                result.is_valid = False
            else:
                sanitized_data['market_data'] = market_data
        
        if result.is_valid:
            sanitized_data['signals_by_game'] = validated_games
            result.sanitized_data = sanitized_data
        
        return result


class SecurityMonitor:
    """Security monitoring and threat detection"""
    
    def __init__(self, security_config: SecurityConfig):
        self.config = security_config
        self.logger = get_logger("ml.security", LogComponent.ANALYSIS)
        self.rate_limiter = RateLimiter(
            max_requests=security_config.max_requests_per_minute,
            window_minutes=1
        )
        
        # Threat detection patterns
        self.suspicious_patterns = [
            ('high_frequency_requests', self._detect_high_frequency_requests),
            ('unusual_game_ids', self._detect_unusual_game_ids),
            ('resource_exhaustion', self._detect_resource_exhaustion),
            ('malformed_requests', self._detect_malformed_requests),
        ]
        
        # Event storage (in production, use external storage)
        self.security_events: List[SecurityEvent] = []
        self.max_stored_events = 10000
    
    def check_rate_limit(self, identifier: str) -> Tuple[bool, Optional[SecurityEvent]]:
        """Check rate limit and return security event if violated"""
        if self.rate_limiter.is_allowed(identifier):
            return True, None
        
        # Rate limit exceeded
        event = SecurityEvent(
            violation_type=SecurityViolationType.RATE_LIMIT_EXCEEDED,
            threat_level=SecurityThreatLevel.MEDIUM,
            user_id=identifier,
            request_details={
                'max_requests': self.config.max_requests_per_minute,
                'window_minutes': 1
            }
        )
        
        self.record_security_event(event)
        return False, event
    
    def validate_request_security(self, 
                                 validation_result: ValidationResult,
                                 user_id: Optional[str] = None,
                                 ip_address: Optional[str] = None,
                                 correlation_id: Optional[str] = None) -> Optional[SecurityEvent]:
        """Perform security validation on request"""
        
        if validation_result.threat_detected:
            event = SecurityEvent(
                violation_type=SecurityViolationType.MALICIOUS_PATTERN,
                threat_level=SecurityThreatLevel.HIGH,
                user_id=user_id,
                ip_address=ip_address,
                correlation_id=correlation_id,
                request_details={
                    'validation_errors': validation_result.errors,
                    'threat_indicators': 'Malicious pattern detected in input'
                }
            )
            
            self.record_security_event(event)
            return event
        
        return None
    
    def record_security_event(self, event: SecurityEvent):
        """Record security event for analysis"""
        self.security_events.append(event)
        
        # Limit stored events
        if len(self.security_events) > self.max_stored_events:
            self.security_events = self.security_events[-self.max_stored_events:]
        
        # Log based on threat level
        if event.threat_level in [SecurityThreatLevel.HIGH, SecurityThreatLevel.CRITICAL]:
            self.logger.error(f"Security Event [{event.event_id}]: {event.violation_type.value} - "
                            f"Threat Level: {event.threat_level.value} - User: {event.user_id}")
        elif event.threat_level == SecurityThreatLevel.MEDIUM:
            self.logger.warning(f"Security Event [{event.event_id}]: {event.violation_type.value} - User: {event.user_id}")
        else:
            self.logger.info(f"Security Event [{event.event_id}]: {event.violation_type.value} - User: {event.user_id}")
        
        # In production, you would also:
        # - Send alerts for HIGH/CRITICAL events
        # - Store events in a database
        # - Trigger automated responses
    
    def get_security_events(self, 
                           hours_back: int = 24,
                           threat_level: Optional[SecurityThreatLevel] = None,
                           user_id: Optional[str] = None) -> List[SecurityEvent]:
        """Get security events with filtering"""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours_back)
        
        filtered_events = []
        for event in self.security_events:
            if event.detected_at < cutoff_time:
                continue
            
            if threat_level and event.threat_level != threat_level:
                continue
            
            if user_id and event.user_id != user_id:
                continue
            
            filtered_events.append(event)
        
        return sorted(filtered_events, key=lambda e: e.detected_at, reverse=True)
    
    def get_security_summary(self, hours_back: int = 24) -> Dict[str, Any]:
        """Get security summary statistics"""
        events = self.get_security_events(hours_back)
        
        summary = {
            'total_events': len(events),
            'by_threat_level': defaultdict(int),
            'by_violation_type': defaultdict(int),
            'unique_users_affected': set(),
            'top_violation_types': [],
            'time_period_hours': hours_back
        }
        
        for event in events:
            summary['by_threat_level'][event.threat_level.value] += 1
            summary['by_violation_type'][event.violation_type.value] += 1
            if event.user_id:
                summary['unique_users_affected'].add(event.user_id)
        
        summary['unique_users_affected'] = len(summary['unique_users_affected'])
        
        # Get top violation types
        violation_counts = list(summary['by_violation_type'].items())
        violation_counts.sort(key=lambda x: x[1], reverse=True)
        summary['top_violation_types'] = violation_counts[:5]
        
        return dict(summary)
    
    # Threat detection methods
    def _detect_high_frequency_requests(self, user_id: str, window_minutes: int = 5) -> bool:
        """Detect unusually high frequency requests"""
        # This would analyze request patterns
        return False  # Placeholder implementation
    
    def _detect_unusual_game_ids(self, game_ids: List[str]) -> bool:
        """Detect unusual or suspicious game ID patterns"""
        # Look for patterns that might indicate scanning or probing
        for game_id in game_ids:
            if len(set(game_id)) < 3:  # Too repetitive
                return True
            if any(char in game_id for char in ['<', '>', '"', "'"]):  # Suspicious characters
                return True
        return False
    
    def _detect_resource_exhaustion(self, request_details: Dict[str, Any]) -> bool:
        """Detect requests designed to exhaust resources"""
        # Check for excessive resource requests
        if request_details.get('max_games', 0) > 100:
            return True
        if request_details.get('max_signals_per_game', 0) > 1000:
            return True
        return False
    
    def _detect_malformed_requests(self, request_data: Any) -> bool:
        """Detect intentionally malformed requests"""
        # This would implement various heuristics
        return False  # Placeholder implementation


class AuditLogger:
    """Audit logging for security compliance"""
    
    def __init__(self, security_config: SecurityConfig):
        self.config = security_config
        self.logger = get_logger("ml.security.audit", LogComponent.ANALYSIS)
        
    def log_request(self,
                   operation: str,
                   user_id: Optional[str] = None,
                   ip_address: Optional[str] = None,
                   request_details: Optional[Dict[str, Any]] = None,
                   correlation_id: Optional[str] = None,
                   duration_ms: Optional[float] = None,
                   success: bool = True):
        """Log API request for audit trail"""
        
        if not self.config.log_all_requests:
            return
        
        audit_data = {
            'timestamp': datetime.utcnow().isoformat(),
            'operation': operation,
            'user_id': user_id,
            'ip_address': ip_address,
            'correlation_id': correlation_id or str(uuid.uuid4()),
            'duration_ms': duration_ms,
            'success': success,
            'request_details': request_details or {}
        }
        
        # Remove sensitive data
        if 'signals' in audit_data['request_details']:
            audit_data['request_details']['signals_count'] = len(audit_data['request_details']['signals'])
            del audit_data['request_details']['signals']
        
        self.logger.info(f"AUDIT: {audit_data}")
    
    def log_security_event(self, event: SecurityEvent):
        """Log security event for audit trail"""
        audit_data = {
            'event_type': 'SECURITY_EVENT',
            'event_id': event.event_id,
            'violation_type': event.violation_type.value,
            'threat_level': event.threat_level.value,
            'user_id': event.user_id,
            'ip_address': event.ip_address,
            'correlation_id': event.correlation_id,
            'timestamp': event.detected_at.isoformat(),
            'details': event.request_details
        }
        
        self.logger.warning(f"SECURITY_AUDIT: {audit_data}")


# Global instances
_security_monitor: Optional[SecurityMonitor] = None
_input_validator: Optional[InputValidator] = None
_audit_logger: Optional[AuditLogger] = None


def get_security_monitor() -> SecurityMonitor:
    """Get global security monitor instance"""
    global _security_monitor
    if _security_monitor is None:
        _security_monitor = SecurityMonitor(get_security_config())
    return _security_monitor


def get_input_validator() -> InputValidator:
    """Get global input validator instance"""
    global _input_validator
    if _input_validator is None:
        _input_validator = InputValidator(get_security_config())
    return _input_validator


def get_audit_logger() -> AuditLogger:
    """Get global audit logger instance"""
    global _audit_logger
    if _audit_logger is None:
        _audit_logger = AuditLogger(get_security_config())
    return _audit_logger


# Convenience functions for common security operations
def validate_and_secure_request(signals_by_game: Dict[str, List[UnifiedBettingSignal]],
                               user_id: Optional[str] = None,
                               ip_address: Optional[str] = None,
                               correlation_id: Optional[str] = None) -> Tuple[bool, ValidationResult, Optional[SecurityEvent]]:
    """
    Perform complete validation and security check on request
    
    Returns:
        (is_allowed, validation_result, security_event)
    """
    validator = get_input_validator()
    monitor = get_security_monitor()
    
    # Rate limiting check
    identifier = user_id or ip_address or 'anonymous'
    rate_allowed, rate_event = monitor.check_rate_limit(identifier)
    
    if not rate_allowed:
        return False, ValidationResult(is_valid=False, errors=["Rate limit exceeded"]), rate_event
    
    # Input validation
    validation_result = validator.validate_opportunity_request(
        signals_by_game=signals_by_game,
        user_id=user_id
    )
    
    # Security validation
    security_event = monitor.validate_request_security(
        validation_result=validation_result,
        user_id=user_id,
        ip_address=ip_address,
        correlation_id=correlation_id
    )
    
    is_allowed = validation_result.is_valid and security_event is None
    
    return is_allowed, validation_result, security_event