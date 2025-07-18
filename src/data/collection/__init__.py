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
    ActionNetworkCollector,
    MLBStatsAPICollector,
    OddsAPICollector,
    SBDCollector,
    SportsBettingReportCollector,  # DEPRECATED: Use SBRUnifiedCollector instead
    VSINCollector,
)
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
