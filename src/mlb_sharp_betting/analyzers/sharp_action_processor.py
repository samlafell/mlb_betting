"""
DEPRECATED - Use SharpActionProcessor directly

This module is deprecated. Use the SharpActionProcessor from the processors module instead.
"""

import warnings
from ..analysis.processors.sharpaction_processor import SharpActionProcessor

warnings.warn(
    "mlb_sharp_betting.analyzers.sharp_action_processor is deprecated. "
    "Use mlb_sharp_betting.analysis.processors.sharpaction_processor.SharpActionProcessor instead.",
    DeprecationWarning,
    stacklevel=2
)

# For backward compatibility
__all__ = ["SharpActionProcessor"]
