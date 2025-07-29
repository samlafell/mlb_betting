#!/usr/bin/env python3
"""
SBR Collector Registration

Auto-registration system for SportsbookReview collector with the CollectorFactory.
This module ensures the SBR collector is properly registered when imported.
"""

import structlog

from .base import CollectorFactory, DataSource
from .sbr_unified_collector import SBRUnifiedCollector

logger = structlog.get_logger(__name__)


def register_sbr_collector():
    """
    Register SBR collector with the factory.
    
    Note: This function is deprecated. Use the centralized registry system
    from src.data.collection.registry instead.
    """
    logger.warning(
        "SBR collector auto-registration is deprecated",
        message="Use centralized registry system instead"
    )
    
    # Registration is now handled by the centralized registry
    # This function is kept for backward compatibility only


# DEPRECATED: Auto-registration removed to prevent duplicates
# Use centralized registry system from src.data.collection.registry instead
