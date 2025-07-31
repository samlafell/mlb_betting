"""
Secure logging utilities for testing.

Provides credential sanitization and secure logging patterns for test output.
"""

import logging
import re
from typing import Any, Dict, Optional
from unittest.mock import Mock


class CredentialSanitizingFormatter(logging.Formatter):
    """
    Logging formatter that sanitizes sensitive information from log messages.
    """
    
    # Patterns for sensitive information
    SENSITIVE_PATTERNS = [
        # Database passwords in connection strings
        (r'(password=)[^&;\s]*', r'\1****'),
        (r'(passwd=)[^&;\s]*', r'\1****'),
        (r'(://[^:]*:)[^@]*(@)', r'\1****\2'),
        
        # API keys and tokens
        (r'(api[_-]?key["\']?\s*[:=]\s*["\']?)[^"\'\s&]*', r'\1****'),
        (r'(token["\']?\s*[:=]\s*["\']?)[^"\'\s&]*', r'\1****'),
        (r'(secret["\']?\s*[:=]\s*["\']?)[^"\'\s&]*', r'\1****'),
        (r'(authorization["\']?\s*[:=]\s*["\']?bearer\s+)[^"\'\s&]*', r'\1****'),
        
        # Credit card numbers
        (r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b', r'****-****-****-****'),
        
        # Social Security Numbers
        (r'\b\d{3}-\d{2}-\d{4}\b', r'***-**-****'),
        
        # Generic sensitive data in JSON/dict format
        (r'"(password|passwd|secret|token|key)":\s*"[^"]*"', r'"\1": "****"'),
        (r"'(password|passwd|secret|token|key)':\s*'[^']*'", r"'\1': '****'"),
    ]
    
    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record with credential sanitization.
        
        Args:
            record: Log record to format
            
        Returns:
            Formatted and sanitized log message
        """
        # Format the message first
        formatted = super().format(record)
        
        # Apply sanitization patterns
        for pattern, replacement in self.SENSITIVE_PATTERNS:
            formatted = re.sub(pattern, replacement, formatted, flags=re.IGNORECASE)
        
        return formatted


def setup_secure_test_logging(
    log_level: str = "INFO",
    include_sanitization: bool = True
) -> None:
    """
    Set up secure logging configuration for tests.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        include_sanitization: Whether to include credential sanitization
    """
    # Remove any existing handlers
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create handler with secure formatter
    handler = logging.StreamHandler()
    
    if include_sanitization:
        formatter = CredentialSanitizingFormatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    else:
        formatter = logging.Formatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    handler.setFormatter(formatter)
    
    # Configure root logger
    root_logger.setLevel(getattr(logging, log_level.upper()))
    root_logger.addHandler(handler)
    
    # Ensure asyncpg and other database libraries use secure logging
    logging.getLogger('asyncpg').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)


def sanitize_dict(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Sanitize a dictionary by masking sensitive field values.
    
    Args:
        data: Dictionary that may contain sensitive information
        
    Returns:
        Sanitized dictionary with masked sensitive values
    """
    if not isinstance(data, dict):
        return data
    
    sensitive_keys = {
        'password', 'passwd', 'pass', 'secret', 'token', 'key', 'api_key',
        'auth', 'authorization', 'credential', 'credentials', 'private_key',
        'access_token', 'refresh_token', 'session_id', 'cookie'
    }
    
    sanitized = {}
    for key, value in data.items():
        key_lower = key.lower()
        
        # Check if key contains sensitive information
        if any(sensitive_key in key_lower for sensitive_key in sensitive_keys):
            sanitized[key] = '****'
        elif isinstance(value, dict):
            sanitized[key] = sanitize_dict(value)
        elif isinstance(value, list):
            sanitized[key] = [
                sanitize_dict(item) if isinstance(item, dict) else item
                for item in value
            ]
        else:
            sanitized[key] = value
    
    return sanitized


def sanitize_test_output(text: str) -> str:
    """
    Sanitize test output text to remove sensitive information.
    
    Args:
        text: Text that may contain sensitive information
        
    Returns:
        Sanitized text with masked sensitive information
    """
    for pattern, replacement in CredentialSanitizingFormatter.SENSITIVE_PATTERNS:
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
    
    return text


class SecureTestLogger:
    """
    Test logger that automatically sanitizes sensitive information.
    """
    
    def __init__(self, name: str):
        self.logger = logging.getLogger(f"test.{name}")
        self.logger.setLevel(logging.DEBUG)
    
    def debug(self, message: str, *args, **kwargs) -> None:
        """Log debug message with sanitization."""
        self.logger.debug(sanitize_test_output(str(message)), *args, **kwargs)
    
    def info(self, message: str, *args, **kwargs) -> None:
        """Log info message with sanitization."""
        self.logger.info(sanitize_test_output(str(message)), *args, **kwargs)
    
    def warning(self, message: str, *args, **kwargs) -> None:
        """Log warning message with sanitization."""
        self.logger.warning(sanitize_test_output(str(message)), *args, **kwargs)
    
    def error(self, message: str, *args, **kwargs) -> None:
        """Log error message with sanitization."""
        self.logger.error(sanitize_test_output(str(message)), *args, **kwargs)
    
    def log_dict(self, level: str, message: str, data: Dict[str, Any]) -> None:
        """
        Log a dictionary with automatic sanitization.
        
        Args:
            level: Log level (debug, info, warning, error)
            message: Log message
            data: Dictionary to log (will be sanitized)
        """
        sanitized_data = sanitize_dict(data)
        log_method = getattr(self.logger, level.lower())
        log_method(f"{message}: {sanitized_data}")


def create_test_logger(name: str) -> SecureTestLogger:
    """
    Create a secure test logger instance.
    
    Args:
        name: Logger name
        
    Returns:
        SecureTestLogger instance
    """
    return SecureTestLogger(name)