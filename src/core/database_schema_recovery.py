"""
Database Schema Recovery Utilities

Handles missing columns, tables, and other schema mismatches gracefully.
Provides fallback queries and automatic schema detection.
"""

import structlog
from typing import Dict, List, Optional, Any

logger = structlog.get_logger(__name__)


class SchemaRecovery:
    """Utilities for handling database schema mismatches."""
    
    @staticmethod
    async def get_table_columns(connection, schema: str, table: str) -> List[str]:
        """Get list of columns for a table."""
        try:
            result = await connection.fetch("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2
                ORDER BY ordinal_position
            """, schema, table)
            
            return [row[0] for row in result]
        except Exception as e:
            logger.error(f"Failed to get columns for {schema}.{table}", error=str(e))
            return []
    
    @staticmethod
    async def check_column_exists(connection, schema: str, table: str, column: str) -> bool:
        """Check if a specific column exists in a table."""
        try:
            result = await connection.fetchval("""
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2 AND column_name = $3
            """, schema, table, column)
            
            return result is not None
        except Exception as e:
            logger.error(f"Failed to check column {schema}.{table}.{column}", error=str(e))
            return False
    
    @staticmethod
    def build_safe_select_query(
        table: str,
        required_columns: List[str],
        available_columns: List[str],
        where_clause: str = "",
        order_clause: str = ""
    ) -> str:
        """
        Build a SELECT query using only available columns.
        
        Args:
            table: Table name
            required_columns: Columns we want to select
            available_columns: Columns that actually exist
            where_clause: WHERE clause (optional)
            order_clause: ORDER BY clause (optional)
            
        Returns:
            Safe SQL query string
        """
        # Find intersection of required and available columns
        safe_columns = []
        for col in required_columns:
            if col in available_columns:
                safe_columns.append(col)
            else:
                # Try common alternative column names
                alternatives = {
                    'game_datetime': ['start_time', 'game_time', 'scheduled_start'],
                    'start_time': ['game_datetime', 'game_time'],
                    'created_at': ['created_date', 'creation_time'],
                    'updated_at': ['updated_date', 'last_modified']
                }
                
                if col in alternatives:
                    for alt in alternatives[col]:
                        if alt in available_columns:
                            safe_columns.append(f"{alt} as {col}")  # Alias to expected name
                            break
                    else:
                        # No alternative found, use NULL placeholder
                        safe_columns.append(f"NULL as {col}")
                        logger.warning(f"Column {col} not found, using NULL placeholder")
                else:
                    # Unknown column, use NULL
                    safe_columns.append(f"NULL as {col}")
                    logger.warning(f"Unknown column {col}, using NULL placeholder")
        
        # Build query
        query_parts = [f"SELECT {', '.join(safe_columns)}", f"FROM {table}"]
        
        if where_clause:
            query_parts.append(where_clause)
        
        if order_clause:
            # Check if ORDER BY column exists
            order_col = order_clause.replace("ORDER BY ", "").split()[0]
            if order_col not in available_columns:
                logger.warning(f"ORDER BY column {order_col} not found, removing ORDER clause")
                order_clause = ""
            
            if order_clause:
                query_parts.append(order_clause)
        
        return " ".join(query_parts)


class GameOutcomeSchemaFallback:
    """Schema fallback strategies specifically for game outcome service."""
    
    @staticmethod
    async def get_games_query_with_fallback(
        connection,
        base_conditions: str,
        force_update: bool,
        params: List[Any]
    ) -> tuple[str, List[Any]]:
        """
        Get a safe query for games that handles missing columns.
        
        Returns:
            Tuple of (query_string, parameters)
        """
        # Check available columns in games_complete table
        available_columns = await SchemaRecovery.get_table_columns(
            connection, 'curated', 'games_complete'
        )
        
        if not available_columns:
            logger.error("Cannot access curated.games_complete table")
            return "SELECT 1 WHERE 1=0", []  # Empty result
        
        logger.info(f"Available columns in curated.games_complete: {available_columns}")
        
        # Required columns for game outcome service
        required_columns = [
            'id', 'mlb_stats_api_game_id', 'home_team', 'away_team',
            'game_datetime', 'game_status'
        ]
        
        if force_update:
            # Get all games in date range
            query = SchemaRecovery.build_safe_select_query(
                table="curated.games_complete g",
                required_columns=required_columns,
                available_columns=available_columns,
                where_clause=base_conditions,
                order_clause="ORDER BY g.id DESC"  # Fallback to id if game_datetime missing
            )
        else:
            # Only get games without outcomes - more complex
            if 'id' in available_columns:
                # Use safe column selection
                safe_select = []
                for col in required_columns:
                    if col in available_columns:
                        safe_select.append(f"g.{col}")
                    elif col == 'game_datetime' and 'start_time' in available_columns:
                        safe_select.append(f"g.start_time as game_datetime")
                    else:
                        safe_select.append(f"NULL as {col}")
                
                query = f"""
                SELECT {', '.join(safe_select)}
                FROM curated.games_complete g
                LEFT JOIN curated.game_outcomes go ON g.id = go.game_id
                {base_conditions}
                  AND go.game_id IS NULL
                ORDER BY g.id DESC
                """
            else:
                logger.error("Critical column 'id' missing from games_complete")
                return "SELECT 1 WHERE 1=0", []
        
        return query, params