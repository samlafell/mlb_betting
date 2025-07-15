"""
Unified Data Layer

Consolidates data operations and models from all legacy systems:
- src/mlb_sharp_betting/models/
- sportsbookreview/models/
- action/models/

Provides unified data models, database operations, and data access patterns
for the entire MLB betting analytics system.
"""

# Import unified models
from .models.unified.base import (
    AnalysisEntity,
    IdentifiedModel,
    SimpleEntity,
    SourcedModel,
    TimestampedModel,
    UnifiedBaseModel,
    UnifiedEntity,
    ValidatedModel,
)
from .models.unified.game import (
    GameContext,
    GameStatus,
    GameType,
    PitcherInfo,
    PitcherMatchup,
    Team,
    UnifiedGame,
    VenueInfo,
    WeatherCondition,
    WeatherData,
)
from .models.unified.odds import (
    BettingMarket,
    BookType,
    LineMovement,
    LineStatus,
    MarketConsensus,
    MarketSide,
    MarketType,
    OddsData,
    OddsFormat,
    OddsMovement,
    OddsSnapshot,
)

# Temporarily commented out until models are fully implemented
# from .models.unified.betting_analysis import (
#     BettingAnalysis,
#     SharpAction,
#     BettingSplit,
#     BettingSignalType,
#     SignalStrength,
#     BettingRecommendation,
#     RiskLevel,
#     ConfidenceLevel,
#     TimingInfo,
#     KellyCriterion,
# )
# from .models.unified.sharp_data import (
#     SharpSignal,
#     SharpMoney,
#     SharpConsensus,
#     SharpDirection,
#     ConfidenceLevel as SharpConfidenceLevel,
#     SharpIndicatorType,
#     SharpImpact,
#     ConsensusStrength,
# )

# Temporarily commented out until database layer is fully implemented
# from .database import (
#     # Connection management
#     DatabaseConnection,
#     ConnectionPool,
#     get_connection,
#     get_connection_pool,
#     close_all_connections,
#
#     # Schema management
#     DatabaseSchema,
#     TableDefinition,
#     IndexDefinition,
#     ColumnDefinition,
#     ConstraintDefinition,
#     MigrationDefinition,
#     ColumnType,
#     IndexType,
#     ConstraintType,
#     create_schema,
#     migrate_schema,
#     get_schema_version,
#
#     # Base classes
#     BaseRepository,
#     BaseModel,
#     TransactionManager,
#
#     # Exceptions
#     DatabaseError,
#     ConnectionError,
#     QueryError,
#     TransactionError,
#
#     # Repositories
#     GameRepository,
#     OddsRepository,
#     BettingAnalysisRepository,
#     SharpDataRepository,
#     UnifiedRepository,
#
#     # Schemas
#     GameCreateSchema,
#     GameUpdateSchema,
#     OddsCreateSchema,
#     OddsUpdateSchema,
#     BettingAnalysisCreateSchema,
#     BettingAnalysisUpdateSchema,
#     SharpDataCreateSchema,
#     SharpDataUpdateSchema,
# )

# Collection system exports - Temporarily commented out until fully implemented
# from .collection import (
#     # Base collection classes
#     BaseCollector,
#     CollectionResult,
#     CollectionConfig,
#     CollectionMetrics,
#     CollectionStatus,
#
#     # Rate limiting
#     UnifiedRateLimiter,
#     RateLimitConfig,
#     RateLimitResult,
#     RateLimitStrategy,
#     TokenBucket,
#     CircuitBreaker,
#     get_rate_limiter,
#
#     # Validation and data quality
#     DataQualityValidator,
#     ValidationResult,
#     ValidationRule,
#     ValidationIssue,
#     ValidationSeverity,
#     ValidationRuleType,
#     DeduplicationService,
#
#     # Source collectors
#     VSINCollector,
#     SBDCollector,
#     PinnacleCollector,
#     SportsbookReviewCollector,
#     ActionNetworkCollector,
#
#     # Orchestration
#     CollectionOrchestrator,
#     CollectionPlan,
#     CollectionTask,
#     SourceConfig,
#     CollectionPriority
# )

__all__ = [
    # Base models
    "UnifiedBaseModel",
    "TimestampedModel",
    "IdentifiedModel",
    "ValidatedModel",
    "SourcedModel",
    "UnifiedEntity",
    "SimpleEntity",
    "AnalysisEntity",
    # Game models
    "UnifiedGame",
    "GameStatus",
    "GameType",
    "WeatherCondition",
    "Team",
    "VenueInfo",
    "WeatherData",
    "PitcherInfo",
    "PitcherMatchup",
    "GameContext",
    # Odds models
    "OddsData",
    "OddsSnapshot",
    "LineMovement",
    "MarketConsensus",
    "BettingMarket",
    "MarketType",
    "OddsFormat",
    "BookType",
    "OddsMovement",
    "LineStatus",
    "MarketSide",
    # Additional models and services will be added as they are implemented
]
