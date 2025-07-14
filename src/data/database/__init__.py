"""
Unified Database Layer

Consolidates database operations from:
- src/mlb_sharp_betting/db/
- sportsbookreview/db/ (if exists)
- action/db/ (if exists)

Provides unified database connection management, schema handling,
and data access patterns for all betting system components.
"""

from .connection import (
    DatabaseConnection,
    ConnectionPool,
    get_connection,
    get_connection_pool,
    close_all_connections,
)
from .schema import (
    DatabaseSchema,
    TableDefinition,
    IndexDefinition,
    ColumnDefinition,
    ConstraintDefinition,
    MigrationDefinition,
    ColumnType,
    IndexType,
    ConstraintType,
    create_schema,
    migrate_schema,
    get_schema_version,
)
from .base import (
    BaseRepository,
    BaseModel,
    DatabaseError,
    ConnectionError,
    QueryError,
    TransactionError,
    TransactionManager,
)
from .repositories import (
    GameRepository,
    OddsRepository,
    BettingAnalysisRepository,
    SharpDataRepository,
    UnifiedRepository,
    get_unified_repository,
    GameCreateSchema,
    GameUpdateSchema,
    OddsCreateSchema,
    OddsUpdateSchema,
    BettingAnalysisCreateSchema,
    BettingAnalysisUpdateSchema,
    SharpDataCreateSchema,
    SharpDataUpdateSchema,
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