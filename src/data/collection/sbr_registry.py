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
    """Register SBR collector with the factory."""
    try:
        # Register for both SPORTS_BOOK_REVIEW and SBR aliases
        CollectorFactory.register_collector(DataSource.SPORTS_BOOK_REVIEW, SBRUnifiedCollector)
        CollectorFactory.register_collector(DataSource.SBR, SBRUnifiedCollector)

        logger.info(
            "SBR collector registered successfully",
            collector="SBRUnifiedCollector",
            sources=["SPORTS_BOOK_REVIEW", "SBR"]
        )

    except Exception as e:
        logger.error("Failed to register SBR collector", error=str(e))
        raise


# Auto-register when module is imported
register_sbr_collector()
