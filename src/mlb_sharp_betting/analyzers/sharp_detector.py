"""
Backward compatibility module for sharp_detector.

This module provides backward compatibility for imports from the old location.
The actual implementation has been moved to analysis/detectors/sharp_detector.py
as part of Phase 2 refactoring.
"""

import warnings

# Import from new location
from ..analysis.detectors.sharp_detector import SharpDetector

# Emit deprecation warning
warnings.warn(
    "Importing SharpDetector from analyzers.sharp_detector is deprecated. "
    "Use mlb_sharp_betting.analysis.detectors.sharp_detector instead.",
    DeprecationWarning,
    stacklevel=2
)

__all__ = ["SharpDetector"] 