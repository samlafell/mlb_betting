"""
Backward compatibility module for sharp_action_processor.

This module provides backward compatibility for imports from the old location.
The actual implementation has been moved to analysis/processors/real_time_processor.py
as part of Phase 2 refactoring.
"""

import warnings

# Import from new location with alias for backward compatibility
from ..analysis.processors.real_time_processor import RealTimeProcessor as SharpActionProcessor

# Emit deprecation warning
warnings.warn(
    "Importing SharpActionProcessor from analyzers.sharp_action_processor is deprecated. "
    "Use mlb_sharp_betting.analysis.processors.real_time_processor.RealTimeProcessor instead.",
    DeprecationWarning,
    stacklevel=2
)

__all__ = ["SharpActionProcessor"]
