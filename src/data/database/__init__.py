"""
Unified Database Layer

Consolidates database operations from:
- src/mlb_sharp_betting/db/
- sportsbookreview/db/ (if exists)
- action/db/ (if exists)

Provides unified database connection management, schema handling,
and data access patterns for all betting system components.
"""

from .base import (
    BaseModel,
    BaseRepository,
    ConnectionError,
    DatabaseError,
    QueryError,
    TransactionError,
    TransactionManager,
)
from .connection import (
    ConnectionPool,
    DatabaseConnection,
    close_all_connections,
    get_connection,
    get_connection_pool,
)
from .repositories import (
    BettingAnalysisCreateSchema,
    BettingAnalysisRepository,
    BettingAnalysisUpdateSchema,
    GameCreateSchema,
    GameRepository,
    GameUpdateSchema,
    OddsCreateSchema,
    OddsRepository,
    OddsUpdateSchema,
    SharpDataCreateSchema,
    SharpDataRepository,
    SharpDataUpdateSchema,
    UnifiedRepository,
    get_unified_repository,
)
from .schema import (
    ColumnDefinition,
    ColumnType,
    ConstraintDefinition,
    ConstraintType,
    DatabaseSchema,
    IndexDefinition,
    IndexType,
    MigrationDefinition,
    TableDefinition,
    create_schema,
    get_schema_version,
    migrate_schema,
)

__all__ = [
    # Connection management
    "DatabaseConnection",
    "ConnectionPool",
    "get_connection",
    "get_connection_pool",
    "close_all_connections",
    # Schema management
    "DatabaseSchema",
    "TableDefinition",
    "IndexDefinition",
    "ColumnDefinition",
    "ConstraintDefinition",
    "MigrationDefinition",
    "ColumnType",
    "IndexType",
    "ConstraintType",
    "create_schema",
    "migrate_schema",
    "get_schema_version",
    # Base classes
    "BaseRepository",
    "BaseModel",
    "TransactionManager",
    # Exceptions
    "DatabaseError",
    "ConnectionError",
    "QueryError",
    "TransactionError",
    # Repositories
    "GameRepository",
    "OddsRepository",
    "BettingAnalysisRepository",
    "SharpDataRepository",
    "UnifiedRepository",
    # Schemas
    "GameCreateSchema",
    "GameUpdateSchema",
    "OddsCreateSchema",
    "OddsUpdateSchema",
    "BettingAnalysisCreateSchema",
    "BettingAnalysisUpdateSchema",
    "SharpDataCreateSchema",
    "SharpDataUpdateSchema",
]
