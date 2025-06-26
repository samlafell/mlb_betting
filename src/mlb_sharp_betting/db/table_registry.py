"""
Centralized Table Registry Service

This service provides a single source of truth for all database table names
and schema mappings used throughout the application. This eliminates hardcoded
table names and makes schema changes much easier to manage.

Usage:
    from mlb_sharp_betting.db.table_registry import get_table_registry
    
    registry = get_table_registry()
    table_name = registry.get_table('betting_splits')
    # Returns: 'splits.raw_mlb_betting_splits'
    
    # For queries
    query = f"SELECT * FROM {registry.get_table('betting_splits')}"
"""

from typing import Dict, Optional
from enum import Enum
import structlog

logger = structlog.get_logger(__name__)


class DatabaseType(Enum):
    """Supported database types."""
    POSTGRESQL = "postgresql"


class TableRegistry:
    """
    Centralized registry for database table names and schema mappings.
    
    This class provides a single source of truth for all table names used
    throughout the application, making it easy to:
    - Change table names without updating multiple files
    - Switch between different database schemas
    - Maintain consistency across the codebase
    """
    
    def __init__(self, database_type: DatabaseType = DatabaseType.POSTGRESQL):
        """
        Initialize the table registry.
        
        Args:
            database_type: The type of database being used
        """
        self.database_type = database_type
        self.logger = logger.bind(component="table_registry")
        
        # Define table mappings based on database type
        self._initialize_table_mappings()
    
    def _initialize_table_mappings(self) -> None:
        """Initialize table mappings based on database type."""
        if self.database_type == DatabaseType.POSTGRESQL:
            self._table_mappings = {
                # Raw betting data
                'raw_betting_splits': 'splits.raw_mlb_betting_splits',
                'betting_splits': 'splits.betting_splits',
                
                # Game data
                'games': 'splits.games',
                'game_outcomes': 'public.game_outcomes',
                
                # Sharp action analysis
                'sharp_actions': 'splits.sharp_actions',
                
                # Backtesting and strategy performance
                'strategy_performance': 'backtesting.strategy_performance',
                
                # Clean/processed data (if using deduplication service)
                'clean_betting_recommendations': 'clean.betting_recommendations',
                
                # Legacy/migration tables
                'legacy_games': 'public.games',
                'legacy_betting_splits': 'public.betting_splits',
                'legacy_mlb_betting_splits': 'public.MLB_BETTING_SPLITS',
                'legacy_splits': 'public.SPLITS',
            }
            
            # Schema mappings for easier schema-level operations
            self._schema_mappings = {
                'splits': 'splits',
                'backtesting': 'backtesting',
                'clean': 'clean',
                'public': 'public'
            }

        else:
            raise ValueError(f"Unsupported database type: {self.database_type}")
    
    def get_table(self, logical_name: str) -> str:
        """
        Get the actual table name for a logical table name.
        
        Args:
            logical_name: The logical name of the table
            
        Returns:
            The actual database table name with schema
            
        Raises:
            KeyError: If the logical name is not found
        """
        if logical_name not in self._table_mappings:
            available_tables = list(self._table_mappings.keys())
            raise KeyError(
                f"Table '{logical_name}' not found in registry. "
                f"Available tables: {available_tables}"
            )
        
        table_name = self._table_mappings[logical_name]
        self.logger.debug("Table name resolved", 
                         logical_name=logical_name, 
                         actual_name=table_name)
        return table_name
    
    def get_schema(self, schema_name: str) -> str:
        """
        Get the actual schema name.
        
        Args:
            schema_name: The logical schema name
            
        Returns:
            The actual database schema name
        """
        if schema_name not in self._schema_mappings:
            available_schemas = list(self._schema_mappings.keys())
            raise KeyError(
                f"Schema '{schema_name}' not found in registry. "
                f"Available schemas: {available_schemas}"
            )
        
        return self._schema_mappings[schema_name]
    
    def list_tables(self) -> Dict[str, str]:
        """
        List all available table mappings.
        
        Returns:
            Dictionary of logical_name -> actual_name mappings
        """
        return self._table_mappings.copy()
    
    def list_schemas(self) -> Dict[str, str]:
        """
        List all available schema mappings.
        
        Returns:
            Dictionary of logical_name -> actual_name mappings
        """
        return self._schema_mappings.copy()
    
    def add_table(self, logical_name: str, actual_name: str) -> None:
        """
        Add a new table mapping (useful for dynamic tables).
        
        Args:
            logical_name: The logical name for the table
            actual_name: The actual database table name
        """
        self._table_mappings[logical_name] = actual_name
        self.logger.info("Table mapping added", 
                        logical_name=logical_name, 
                        actual_name=actual_name)
    
    def update_table(self, logical_name: str, actual_name: str) -> None:
        """
        Update an existing table mapping.
        
        Args:
            logical_name: The logical name for the table
            actual_name: The new actual database table name
        """
        if logical_name not in self._table_mappings:
            raise KeyError(f"Table '{logical_name}' not found in registry")
        
        old_name = self._table_mappings[logical_name]
        self._table_mappings[logical_name] = actual_name
        self.logger.info("Table mapping updated", 
                        logical_name=logical_name, 
                        old_name=old_name,
                        new_name=actual_name)
    
    def get_qualified_columns(self, logical_table: str, columns: list) -> list:
        """
        Get fully qualified column names for a table.
        
        Args:
            logical_table: The logical table name
            columns: List of column names
            
        Returns:
            List of fully qualified column names
        """
        table_name = self.get_table(logical_table)
        return [f"{table_name}.{col}" for col in columns]
    
    def build_query_with_tables(self, query_template: str, **table_kwargs) -> str:
        """
        Build a query by replacing table placeholders with actual table names.
        
        Args:
            query_template: SQL query template with {table_name} placeholders
            **table_kwargs: Mapping of placeholder names to logical table names
            
        Returns:
            SQL query with actual table names
            
        Example:
            query = registry.build_query_with_tables(
                "SELECT * FROM {betting_splits} bs JOIN {games} g ON bs.game_id = g.game_id",
                betting_splits='raw_betting_splits',
                games='games'
            )
        """
        table_replacements = {}
        for placeholder, logical_name in table_kwargs.items():
            table_replacements[placeholder] = self.get_table(logical_name)
        
        return query_template.format(**table_replacements)


# Global registry instance
_global_registry: Optional[TableRegistry] = None


def get_table_registry(database_type: DatabaseType = DatabaseType.POSTGRESQL) -> TableRegistry:
    """
    Get the global table registry instance.
    
    Args:
        database_type: The type of database being used
        
    Returns:
        The global TableRegistry instance
    """
    global _global_registry
    
    if _global_registry is None or _global_registry.database_type != database_type:
        _global_registry = TableRegistry(database_type)
        logger.info("Table registry initialized", database_type=database_type.value)
    
    return _global_registry


def reset_table_registry() -> None:
    """Reset the global table registry (useful for testing)."""
    global _global_registry
    _global_registry = None


# Convenience functions for common operations
def get_table(logical_name: str) -> str:
    """Convenience function to get a table name."""
    return get_table_registry().get_table(logical_name)


def get_schema(schema_name: str) -> str:
    """Convenience function to get a schema name."""
    return get_table_registry().get_schema(schema_name)


def build_query(query_template: str, **table_kwargs) -> str:
    """Convenience function to build a query with table names."""
    return get_table_registry().build_query_with_tables(query_template, **table_kwargs)


# Common table name constants for IDE autocomplete
class Tables:
    """Constants for logical table names (for IDE autocomplete and type safety)."""
    RAW_BETTING_SPLITS = 'raw_betting_splits'
    BETTING_SPLITS = 'betting_splits'
    GAMES = 'games'
    GAME_OUTCOMES = 'game_outcomes'
    SHARP_ACTIONS = 'sharp_actions'
    STRATEGY_PERFORMANCE = 'strategy_performance'
    CLEAN_BETTING_RECOMMENDATIONS = 'clean_betting_recommendations'


class Schemas:
    """Constants for logical schema names."""
    SPLITS = 'splits'
    BACKTESTING = 'backtesting'
    CLEAN = 'clean'
    PUBLIC = 'public'


if __name__ == "__main__":
    # Example usage and testing
    print("🗃️  Table Registry Example Usage")
    print("=" * 50)
    
    # Initialize registry
    registry = get_table_registry(DatabaseType.POSTGRESQL)
    
    # Show all available tables
    print("\n📋 Available Tables:")
    for logical, actual in registry.list_tables().items():
        print(f"  {logical:<25} -> {actual}")
    
    # Example query building
    print(f"\n🔍 Example Query:")
    query = registry.build_query_with_tables(
        """
        SELECT bs.*, g.home_team, g.away_team 
        FROM {betting_splits} bs 
        JOIN {games} g ON bs.game_id = g.game_id 
        WHERE bs.split_type = 'moneyline'
        """,
        betting_splits=Tables.RAW_BETTING_SPLITS,
        games=Tables.GAMES
    )
    print(query)
    
    # Example individual table lookup
    print(f"\n📊 Individual Table Lookups:")
    print(f"  Raw betting splits: {registry.get_table(Tables.RAW_BETTING_SPLITS)}")
    print(f"  Game outcomes: {registry.get_table(Tables.GAME_OUTCOMES)}")
    print(f"  Strategy performance: {registry.get_table(Tables.STRATEGY_PERFORMANCE)}")