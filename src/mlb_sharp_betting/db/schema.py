"""
Database schema management for the MLB Sharp Betting system.

This module provides schema creation, migration, and management functionality
to ensure proper storage of BettingSplit objects and related data.
"""

from datetime import datetime
from pathlib import Path
from typing import List, Optional

import structlog

from .connection import DatabaseManager
from ..core.exceptions import DatabaseError

logger = structlog.get_logger(__name__)


class SchemaManager:
    """
    Database schema manager for creating and maintaining database structure.
    """

    def __init__(self, db_manager: Optional[DatabaseManager] = None) -> None:
        """Initialize schema manager."""
        self.db_manager = db_manager or DatabaseManager()
        self.logger = logger.bind(component="SchemaManager")

    def create_schema(self) -> None:
        """Create the main database schema."""
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute("CREATE SCHEMA IF NOT EXISTS splits")
                self.logger.info("Created schema 'splits'")
        except Exception as e:
            self.logger.error("Failed to create schema", error=str(e))
            raise DatabaseError(f"Failed to create schema: {e}")

    def create_betting_splits_table(self) -> None:
        """Create the main betting splits table."""
        try:
            with self.db_manager.get_cursor() as cursor:
                # Create the main table based on BettingSplit model structure
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS splits.betting_splits (
                        id VARCHAR PRIMARY KEY,
                        game_id VARCHAR NOT NULL,
                        home_team VARCHAR NOT NULL,
                        away_team VARCHAR NOT NULL,
                        game_datetime TIMESTAMP NOT NULL,
                        split_type VARCHAR NOT NULL,
                        split_value DOUBLE,
                        source VARCHAR NOT NULL,
                        book VARCHAR NOT NULL,
                        last_updated TIMESTAMP NOT NULL,
                        
                        -- Home/Over betting data
                        home_or_over_bets INTEGER,
                        home_or_over_bets_percentage DOUBLE,
                        home_or_over_stake_percentage DOUBLE,
                        
                        -- Away/Under betting data
                        away_or_under_bets INTEGER,
                        away_or_under_bets_percentage DOUBLE,
                        away_or_under_stake_percentage DOUBLE,
                        
                        -- Analysis results
                        sharp_action VARCHAR,
                        outcome VARCHAR,
                        
                        -- Metadata
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                self.logger.info("Created betting_splits table")
        except Exception as e:
            self.logger.error("Failed to create betting_splits table", error=str(e))
            raise DatabaseError(f"Failed to create betting_splits table: {e}")

    def create_games_table(self) -> None:
        """Create the games table for game metadata."""
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS splits.games (
                        id VARCHAR PRIMARY KEY,
                        game_id VARCHAR UNIQUE NOT NULL,
                        home_team VARCHAR NOT NULL,
                        away_team VARCHAR NOT NULL,
                        game_datetime TIMESTAMP NOT NULL,
                        status VARCHAR DEFAULT 'scheduled',
                        home_score INTEGER,
                        away_score INTEGER,
                        weather_conditions VARCHAR,
                        venue VARCHAR,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                self.logger.info("Created games table")
        except Exception as e:
            self.logger.error("Failed to create games table", error=str(e))
            raise DatabaseError(f"Failed to create games table: {e}")

    def create_sharp_actions_table(self) -> None:
        """Create the sharp actions table for analysis results."""
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS splits.sharp_actions (
                        id VARCHAR PRIMARY KEY,
                        game_id VARCHAR NOT NULL,
                        split_type VARCHAR NOT NULL,
                        direction VARCHAR NOT NULL,
                        overall_confidence VARCHAR NOT NULL,
                        signals JSON,
                        analysis JSON,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                self.logger.info("Created sharp_actions table")
        except Exception as e:
            self.logger.error("Failed to create sharp_actions table", error=str(e))
            raise DatabaseError(f"Failed to create sharp_actions table: {e}")

    def create_indexes(self) -> None:
        """Create performance indexes on key fields."""
        indexes = [
            # Betting splits indexes
            ("idx_betting_splits_game_id", "splits.betting_splits", "game_id"),
            ("idx_betting_splits_datetime", "splits.betting_splits", "game_datetime"),
            ("idx_betting_splits_source_book", "splits.betting_splits", "source, book"),
            ("idx_betting_splits_split_type", "splits.betting_splits", "split_type"),
            ("idx_betting_splits_sharp_action", "splits.betting_splits", "sharp_action"),
            ("idx_betting_splits_last_updated", "splits.betting_splits", "last_updated"),
            
            # Games indexes
            ("idx_games_game_id", "splits.games", "game_id"),
            ("idx_games_datetime", "splits.games", "game_datetime"),
            ("idx_games_teams", "splits.games", "home_team, away_team"),
            ("idx_games_status", "splits.games", "status"),
            
            # Sharp actions indexes
            ("idx_sharp_actions_game_id", "splits.sharp_actions", "game_id"),
            ("idx_sharp_actions_confidence", "splits.sharp_actions", "overall_confidence"),
            ("idx_sharp_actions_split_type", "splits.sharp_actions", "split_type"),
        ]

        try:
            with self.db_manager.get_cursor() as cursor:
                for index_name, table_name, columns in indexes:
                    cursor.execute(f"""
                        CREATE INDEX IF NOT EXISTS {index_name} 
                        ON {table_name}({columns})
                    """)
                    self.logger.debug("Created index", index=index_name, table=table_name)
                
                self.logger.info("Created all database indexes")
        except Exception as e:
            self.logger.error("Failed to create indexes", error=str(e))
            raise DatabaseError(f"Failed to create indexes: {e}")

    def create_triggers(self) -> None:
        """Create database triggers for maintaining data integrity."""
        try:
            with self.db_manager.get_cursor() as cursor:
                # Update timestamp trigger for betting_splits
                cursor.execute("""
                    CREATE OR REPLACE TRIGGER update_betting_splits_timestamp
                    BEFORE UPDATE ON splits.betting_splits
                    FOR EACH ROW
                    EXECUTE (
                        UPDATE splits.betting_splits 
                        SET updated_at = CURRENT_TIMESTAMP 
                        WHERE id = NEW.id
                    )
                """)
                
                # Update timestamp trigger for games
                cursor.execute("""
                    CREATE OR REPLACE TRIGGER update_games_timestamp
                    BEFORE UPDATE ON splits.games
                    FOR EACH ROW
                    EXECUTE (
                        UPDATE splits.games 
                        SET updated_at = CURRENT_TIMESTAMP 
                        WHERE id = NEW.id
                    )
                """)
                
                self.logger.info("Created database triggers")
        except Exception as e:
            # Triggers might not be supported in all DuckDB versions
            self.logger.warning("Failed to create triggers (may not be supported)", error=str(e))

    def setup_complete_schema(self) -> None:
        """Set up the complete database schema."""
        self.logger.info("Setting up complete database schema")
        
        try:
            # Create schema
            self.create_schema()
            
            # Create tables
            self.create_betting_splits_table()
            self.create_games_table()
            self.create_sharp_actions_table()
            
            # Create indexes
            self.create_indexes()
            
            # Create triggers (if supported)
            self.create_triggers()
            
            self.logger.info("Database schema setup completed successfully")
            
        except Exception as e:
            self.logger.error("Failed to setup complete schema", error=str(e))
            raise DatabaseError(f"Failed to setup database schema: {e}")

    def verify_schema(self) -> bool:
        """Verify that the schema is properly set up."""
        required_tables = [
            "splits.betting_splits",
            "splits.games", 
            "splits.sharp_actions"
        ]
        
        try:
            with self.db_manager.get_cursor() as cursor:
                for table in required_tables:
                    result = cursor.execute(f"""
                        SELECT table_name FROM information_schema.tables 
                        WHERE table_schema = 'splits' 
                        AND table_name = '{table.split('.')[1]}'
                    """).fetchall()
                    
                    if not result:
                        self.logger.error("Missing required table", table=table)
                        return False
                
                self.logger.info("Schema verification passed")
                return True
                
        except Exception as e:
            self.logger.error("Schema verification failed", error=str(e))
            return False

    def get_schema_info(self) -> dict:
        """Get information about the current schema."""
        try:
            with self.db_manager.get_cursor() as cursor:
                # Get table information
                tables_info = {}
                
                result = cursor.execute("""
                    SELECT table_name, table_type 
                    FROM information_schema.tables 
                    WHERE table_schema = 'splits'
                """).fetchall()
                
                for table_name, table_type in result:
                    # Get column info for each table
                    columns = cursor.execute(f"""
                        SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns
                        WHERE table_schema = 'splits' 
                        AND table_name = '{table_name}'
                        ORDER BY ordinal_position
                    """).fetchall()
                    
                    tables_info[table_name] = {
                        "type": table_type,
                        "columns": [
                            {
                                "name": col[0],
                                "type": col[1], 
                                "nullable": col[2]
                            }
                            for col in columns
                        ]
                    }
                
                return {
                    "schema": "splits",
                    "tables": tables_info,
                    "verification_time": datetime.now().isoformat()
                }
                
        except Exception as e:
            self.logger.error("Failed to get schema info", error=str(e))
            raise DatabaseError(f"Failed to get schema info: {e}")

    def drop_schema(self, confirm: bool = False) -> None:
        """
        Drop the entire schema (use with caution).
        
        Args:
            confirm: Must be True to actually drop the schema
        """
        if not confirm:
            raise ValueError("Must confirm schema drop with confirm=True")
            
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS splits CASCADE")
                self.logger.warning("Dropped schema 'splits' and all its tables")
        except Exception as e:
            self.logger.error("Failed to drop schema", error=str(e))
            raise DatabaseError(f"Failed to drop schema: {e}")


def get_schema_manager(db_manager: Optional[DatabaseManager] = None) -> SchemaManager:
    """Get a SchemaManager instance."""
    return SchemaManager(db_manager)


def setup_database_schema(db_manager: Optional[DatabaseManager] = None) -> None:
    """Convenience function to set up the complete database schema."""
    schema_manager = get_schema_manager(db_manager)
    schema_manager.setup_complete_schema()


def verify_database_schema(db_manager: Optional[DatabaseManager] = None) -> bool:
    """Convenience function to verify the database schema."""
    schema_manager = get_schema_manager(db_manager)
    return schema_manager.verify_schema() 