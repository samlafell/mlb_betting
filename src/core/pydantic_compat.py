"""
Pydantic v2 Compatibility Module

Provides compatibility wrappers for Pydantic v2 features that may not be available
in all installations, including computed_field functionality.

Temporary fix for computed_field import issues.
"""

from typing import Any, Callable, TypeVar

try:
    from pydantic import computed_field as pydantic_computed_field
    COMPUTED_FIELD_AVAILABLE = True
except ImportError:
    COMPUTED_FIELD_AVAILABLE = False
    
    # Create a fallback decorator that works as a regular property
    def pydantic_computed_field(func: Callable) -> Any:
        """Fallback computed_field decorator that works as a regular property."""
        return property(func)

try:
    from pydantic import field_validator as pydantic_field_validator
    FIELD_VALIDATOR_AVAILABLE = True
except ImportError:
    FIELD_VALIDATOR_AVAILABLE = False
    
    # Create a fallback decorator that works as a no-op
    def pydantic_field_validator(*args, **kwargs):
        """Fallback field_validator decorator that works as a no-op."""
        def decorator(func: Callable) -> Callable:
            return func
        return decorator if args else decorator(*args, **kwargs)

try:
    from pydantic import ValidationInfo as pydantic_validation_info
    VALIDATION_INFO_AVAILABLE = True
except ImportError:
    VALIDATION_INFO_AVAILABLE = False
    
    # Create a fallback class
    class pydantic_validation_info:
        """Fallback ValidationInfo class."""
        pass

try:
    from pydantic import model_validator as pydantic_model_validator
    MODEL_VALIDATOR_AVAILABLE = True
except ImportError:
    MODEL_VALIDATOR_AVAILABLE = False
    
    # Create a fallback decorator that works as a no-op
    def pydantic_model_validator(*args, **kwargs):
        """Fallback model_validator decorator that works as a no-op."""
        def decorator(func: Callable) -> Callable:
            return func
        return decorator if args else decorator(*args, **kwargs)

# Type variable for decorator return type
T = TypeVar('T')

def computed_field(func: Callable[..., T]) -> T:
    """
    Compatibility wrapper for Pydantic's computed_field.
    
    If computed_field is available, uses it. Otherwise, falls back to
    a regular property decorator.
    
    Args:
        func: The function to decorate
        
    Returns:
        Either a computed_field or property decorator
    """
    if COMPUTED_FIELD_AVAILABLE:
        return pydantic_computed_field(func)
    else:
        return property(func)

def field_validator(*args, **kwargs):
    """
    Compatibility wrapper for Pydantic's field_validator.
    
    If field_validator is available, uses it. Otherwise, falls back to
    a no-op decorator.
    
    Returns:
        Either a field_validator decorator or no-op decorator
    """
    if FIELD_VALIDATOR_AVAILABLE:
        return pydantic_field_validator(*args, **kwargs)
    else:
        return pydantic_field_validator(*args, **kwargs)

def model_validator(*args, **kwargs):
    """
    Compatibility wrapper for Pydantic's model_validator.
    
    If model_validator is available, uses it. Otherwise, falls back to
    a no-op decorator.
    
    Returns:
        Either a model_validator decorator or no-op decorator
    """
    if MODEL_VALIDATOR_AVAILABLE:
        return pydantic_model_validator(*args, **kwargs)
    else:
        return pydantic_model_validator(*args, **kwargs)

# ValidationInfo compatibility
ValidationInfo = pydantic_validation_info if VALIDATION_INFO_AVAILABLE else pydantic_validation_info

# Export compatibility info for debugging
__all__ = [
    'computed_field', 
    'field_validator', 
    'model_validator',
    'ValidationInfo',
    'COMPUTED_FIELD_AVAILABLE', 
    'FIELD_VALIDATOR_AVAILABLE',
    'MODEL_VALIDATOR_AVAILABLE',
    'VALIDATION_INFO_AVAILABLE'
]