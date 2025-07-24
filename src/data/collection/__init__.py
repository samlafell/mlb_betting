"""
Unified Data Collection System for MLB Betting Analytics

This package consolidates data collectors from three legacy modules:
- mlb_sharp_betting/scrapers/ (VSIN, SBD, Pinnacle)
- sportsbookreview/parsers/ (SBR HTML/JSON parsing)
- action/scrapers/ (Action Network)

Provides:
- Unified collector base classes with rate limiting and retry logic
- Centralized data quality validation and deduplication
- Collection orchestration with monitoring and error handling
- Source-specific collectors with consistent interfaces
"""

from .base import BaseCollector, CollectionMetrics, CollectionResult, CollectorConfig
from .collectors import (
    ActionNetworkCollector,  # Now points to ConsolidatedActionNetworkCollector
    MLBStatsAPICollector,
    OddsAPICollector,
    SBDCollector,
    SportsBettingReportCollector,  # DEPRECATED: Use SBRUnifiedCollector instead
    VSINCollector,
)

# Define __all__ first
__all__ = [
    # Base classes
    "BaseCollector",
    "CollectionResult",
    "CollectorConfig",
    "CollectionMetrics",
    # Rate limiting
    "UnifiedRateLimiter",
    "RateLimitConfig",
    "RateLimitResult",
    "TokenBucket",
    "CircuitBreaker",
    # Validation & deduplication
    "DataQualityValidator",
    "ValidationResult",
    "ValidationRule",
    "DeduplicationService",
    # Source collectors
    "VSINCollector",
    "SBDCollector",
    "SportsBettingReportCollector",  # DEPRECATED: Use SBRUnifiedCollector instead
    "ActionNetworkCollector",
    "MLBStatsAPICollector",
    "OddsAPICollector",
    # Orchestration
    "CollectionOrchestrator",
    "CollectionPlan",
    "CollectionStatus",
    "SourceConfig",
]

# Import refactored collectors directly for better compatibility
try:
    from .consolidated_action_network_collector import (
        ActionNetworkCollector as ConsolidatedActionNetworkCollector,
    )
    from .sbd_unified_collector_api import SBDUnifiedCollectorAPI
    from .vsin_unified_collector import VSINUnifiedCollector

    # Add refactored collectors to exports
    __all__.extend(
        [
            "SBDUnifiedCollectorAPI",
            "VSINUnifiedCollector",
            "ConsolidatedActionNetworkCollector",
        ]
    )
except ImportError:
    # Refactored collectors not available, stick with legacy ones
    pass
from .orchestrator import (
    CollectionOrchestrator,
    CollectionPlan,
    CollectionStatus,
    SourceConfig,
)
from .rate_limiter import (
    CircuitBreaker,
    RateLimitConfig,
    RateLimitResult,
    TokenBucket,
    UnifiedRateLimiter,
)
from .validators import (
    DataQualityValidator,
    DeduplicationService,
    ValidationResult,
    ValidationRule,
)
