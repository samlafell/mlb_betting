"""
Database Schema Management and Migration System

Provides comprehensive schema management for PostgreSQL 17 including:
- Schema definition and creation
- Migration management and versioning
- Index management and optimization
- Constraint management
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union
import re

from ...core.config import UnifiedSettings
from ...core.exceptions import DatabaseError
from ...core.logging import LogComponent, get_logger
from .connection import DatabaseConnection

logger = get_logger(__name__, LogComponent.DATABASE)


class ColumnType(str, Enum):
    """PostgreSQL column types."""
    # Numeric types
    SMALLINT = "SMALLINT"
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    DECIMAL = "DECIMAL"
    NUMERIC = "NUMERIC"
    REAL = "REAL"
    DOUBLE_PRECISION = "DOUBLE PRECISION"
    SMALLSERIAL = "SMALLSERIAL"
    SERIAL = "SERIAL"
    BIGSERIAL = "BIGSERIAL"
    
    # Character types
    CHAR = "CHAR"
    VARCHAR = "VARCHAR"
    TEXT = "TEXT"
    
    # Binary types
    BYTEA = "BYTEA"
    
    # Date/time types
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMPTZ = "TIMESTAMPTZ"
    DATE = "DATE"
    TIME = "TIME"
    TIMETZ = "TIMETZ"
    INTERVAL = "INTERVAL"
    
    # Boolean type
    BOOLEAN = "BOOLEAN"
    
    # Network types
    INET = "INET"
    CIDR = "CIDR"
    MACADDR = "MACADDR"
    
    # JSON types
    JSON = "JSON"
    JSONB = "JSONB"
    
    # UUID type
    UUID = "UUID"
    
    # Array types
    ARRAY = "ARRAY"
    
    # Geometric types
    POINT = "POINT"
    LINE = "LINE"
    LSEG = "LSEG"
    BOX = "BOX"
    PATH = "PATH"
    POLYGON = "POLYGON"
    CIRCLE = "CIRCLE"


class IndexType(str, Enum):
    """PostgreSQL index types."""
    BTREE = "BTREE"
    HASH = "HASH"
    GIN = "GIN"
    GIST = "GIST"
    SPGIST = "SPGIST"
    BRIN = "BRIN"


class ConstraintType(str, Enum):
    """Database constraint types."""
    PRIMARY_KEY = "PRIMARY KEY"
    FOREIGN_KEY = "FOREIGN KEY"
    UNIQUE = "UNIQUE"
    CHECK = "CHECK"
    NOT_NULL = "NOT NULL"
    DEFAULT = "DEFAULT"


@dataclass
class ColumnDefinition:
    """Database column definition."""
    name: str
    type: ColumnType
    length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    nullable: bool = True
    default: Optional[str] = None
    primary_key: bool = False
    unique: bool = False
    check_constraint: Optional[str] = None
    comment: Optional[str] = None
    
    def to_sql(self) -> str:
        """Convert column definition to SQL."""
        sql_parts = [self.name]
        
        # Column type with optional length/precision
        if self.type in [ColumnType.VARCHAR, ColumnType.CHAR]:
            if self.length:
                sql_parts.append(f"{self.type.value}({self.length})")
            else:
                sql_parts.append(self.type.value)
        elif self.type in [ColumnType.DECIMAL, ColumnType.NUMERIC]:
            if self.precision and self.scale:
                sql_parts.append(f"{self.type.value}({self.precision},{self.scale})")
            elif self.precision:
                sql_parts.append(f"{self.type.value}({self.precision})")
            else:
                sql_parts.append(self.type.value)
        else:
            sql_parts.append(self.type.value)
        
        # Constraints
        if not self.nullable:
            sql_parts.append("NOT NULL")
        
        if self.default:
            sql_parts.append(f"DEFAULT {self.default}")
        
        if self.unique:
            sql_parts.append("UNIQUE")
        
        if self.check_constraint:
            sql_parts.append(f"CHECK ({self.check_constraint})")
        
        return " ".join(sql_parts)


@dataclass
class IndexDefinition:
    """Database index definition."""
    name: str
    table_name: str
    columns: List[str]
    index_type: IndexType = IndexType.BTREE
    unique: bool = False
    partial: Optional[str] = None
    include: Optional[List[str]] = None
    comment: Optional[str] = None
    
    def to_sql(self) -> str:
        """Convert index definition to SQL."""
        sql_parts = ["CREATE"]
        
        if self.unique:
            sql_parts.append("UNIQUE")
        
        sql_parts.extend([
            "INDEX",
            self.name,
            "ON",
            self.table_name
        ])
        
        if self.index_type != IndexType.BTREE:
            sql_parts.append(f"USING {self.index_type.value}")
        
        # Columns
        columns_sql = f"({', '.join(self.columns)})"
        sql_parts.append(columns_sql)
        
        # Include columns (covering index)
        if self.include:
            sql_parts.append(f"INCLUDE ({', '.join(self.include)})")
        
        # Partial index
        if self.partial:
            sql_parts.append(f"WHERE {self.partial}")
        
        return " ".join(sql_parts)


@dataclass
class ConstraintDefinition:
    """Database constraint definition."""
    name: str
    table_name: str
    constraint_type: ConstraintType
    columns: List[str]
    reference_table: Optional[str] = None
    reference_columns: Optional[List[str]] = None
    check_expression: Optional[str] = None
    on_delete: Optional[str] = None
    on_update: Optional[str] = None
    
    def to_sql(self) -> str:
        """Convert constraint definition to SQL."""
        sql_parts = [
            "ALTER TABLE",
            self.table_name,
            "ADD CONSTRAINT",
            self.name
        ]
        
        if self.constraint_type == ConstraintType.PRIMARY_KEY:
            sql_parts.append(f"PRIMARY KEY ({', '.join(self.columns)})")
        
        elif self.constraint_type == ConstraintType.FOREIGN_KEY:
            sql_parts.append(f"FOREIGN KEY ({', '.join(self.columns)})")
            sql_parts.append(f"REFERENCES {self.reference_table}")
            sql_parts.append(f"({', '.join(self.reference_columns)})")
            
            if self.on_delete:
                sql_parts.append(f"ON DELETE {self.on_delete}")
            if self.on_update:
                sql_parts.append(f"ON UPDATE {self.on_update}")
        
        elif self.constraint_type == ConstraintType.UNIQUE:
            sql_parts.append(f"UNIQUE ({', '.join(self.columns)})")
        
        elif self.constraint_type == ConstraintType.CHECK:
            sql_parts.append(f"CHECK ({self.check_expression})")
        
        return " ".join(sql_parts)


@dataclass
class TableDefinition:
    """Database table definition."""
    name: str
    columns: List[ColumnDefinition]
    constraints: List[ConstraintDefinition] = field(default_factory=list)
    indexes: List[IndexDefinition] = field(default_factory=list)
    comment: Optional[str] = None
    schema: str = "public"
    
    def to_sql(self) -> List[str]:
        """Convert table definition to SQL statements."""
        statements = []
        
        # Create table statement
        table_sql = f"CREATE TABLE {self.schema}.{self.name} (\n"
        column_definitions = [f"    {col.to_sql()}" for col in self.columns]
        table_sql += ",\n".join(column_definitions)
        table_sql += "\n)"
        
        statements.append(table_sql)
        
        # Table comment
        if self.comment:
            statements.append(
                f"COMMENT ON TABLE {self.schema}.{self.name} IS '{self.comment}'"
            )
        
        # Column comments
        for col in self.columns:
            if col.comment:
                statements.append(
                    f"COMMENT ON COLUMN {self.schema}.{self.name}.{col.name} IS '{col.comment}'"
                )
        
        # Constraints
        for constraint in self.constraints:
            statements.append(constraint.to_sql())
        
        # Indexes
        for index in self.indexes:
            statements.append(index.to_sql())
        
        return statements


@dataclass
class MigrationDefinition:
    """Database migration definition."""
    version: str
    name: str
    up_sql: List[str]
    down_sql: List[str]
    description: Optional[str] = None
    created_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()


class DatabaseSchema:
    """
    Database schema management system.
    
    Handles schema creation, migration, and versioning for PostgreSQL 17.
    """
    
    def __init__(self, connection: DatabaseConnection, schema_name: str = "public"):
        """
        Initialize database schema manager.
        
        Args:
            connection: Database connection instance
            schema_name: Schema name to manage
        """
        self.connection = connection
        self.schema_name = schema_name
        self.logger = logger.with_context(schema=schema_name)
        
        # Migration tracking table
        self.migration_table = "schema_migrations"
        self.version_table = "schema_version"
    
    async def initialize_migration_tracking(self) -> None:
        """Initialize migration tracking tables."""
        start_time = self.logger.log_operation_start("initialize_migration_tracking")
        
        try:
            # Create schema_migrations table
            migrations_sql = f"""
                CREATE TABLE IF NOT EXISTS {self.migration_table} (
                    id SERIAL PRIMARY KEY,
                    version VARCHAR(255) NOT NULL UNIQUE,
                    name VARCHAR(255) NOT NULL,
                    description TEXT,
                    applied_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    execution_time_ms INTEGER,
                    checksum VARCHAR(64)
                )
            """
            
            # Create schema_version table
            version_sql = f"""
                CREATE TABLE IF NOT EXISTS {self.version_table} (
                    id SERIAL PRIMARY KEY,
                    current_version VARCHAR(255) NOT NULL,
                    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                )
            """
            
            # Create indexes
            index_sql = f"""
                CREATE INDEX IF NOT EXISTS idx_{self.migration_table}_version 
                ON {self.migration_table}(version);
                
                CREATE INDEX IF NOT EXISTS idx_{self.migration_table}_applied_at 
                ON {self.migration_table}(applied_at);
            """
            
            await self.connection.execute_async(migrations_sql)
            await self.connection.execute_async(version_sql)
            await self.connection.execute_async(index_sql)
            
            # Initialize version if empty
            version_count = await self.connection.execute_async(
                f"SELECT COUNT(*) FROM {self.version_table}",
                fetch="one"
            )
            
            if version_count[0] == 0:
                await self.connection.execute_async(
                    f"INSERT INTO {self.version_table} (current_version) VALUES ('0.0.0')"
                )
            
            self.logger.log_operation_end("initialize_migration_tracking", start_time, success=True)
            
        except Exception as e:
            self.logger.log_operation_end("initialize_migration_tracking", start_time, success=False, error=e)
            raise DatabaseError(
                f"Failed to initialize migration tracking: {str(e)}",
                operation="initialize_migration_tracking",
                cause=e
            )
    
    async def create_table(self, table_def: TableDefinition) -> None:
        """
        Create table from definition.
        
        Args:
            table_def: Table definition
        """
        start_time = self.logger.log_operation_start(
            "create_table",
            extra={"table_name": table_def.name}
        )
        
        try:
            statements = table_def.to_sql()
            
            for statement in statements:
                await self.connection.execute_async(
                    statement,
                    table=table_def.name
                )
            
            self.logger.log_operation_end(
                "create_table",
                start_time,
                success=True,
                extra={"table_name": table_def.name, "statements_count": len(statements)}
            )
            
        except Exception as e:
            self.logger.log_operation_end("create_table", start_time, success=False, error=e)
            raise DatabaseError(
                f"Failed to create table {table_def.name}: {str(e)}",
                operation="create_table",
                cause=e,
                details={"table_name": table_def.name}
            )
    
    async def drop_table(self, table_name: str, cascade: bool = False) -> None:
        """
        Drop table.
        
        Args:
            table_name: Table name to drop
            cascade: Whether to use CASCADE option
        """
        start_time = self.logger.log_operation_start(
            "drop_table",
            extra={"table_name": table_name, "cascade": cascade}
        )
        
        try:
            cascade_sql = " CASCADE" if cascade else ""
            sql = f"DROP TABLE IF EXISTS {self.schema_name}.{table_name}{cascade_sql}"
            
            await self.connection.execute_async(sql, table=table_name)
            
            self.logger.log_operation_end(
                "drop_table",
                start_time,
                success=True,
                extra={"table_name": table_name}
            )
            
        except Exception as e:
            self.logger.log_operation_end("drop_table", start_time, success=False, error=e)
            raise DatabaseError(
                f"Failed to drop table {table_name}: {str(e)}",
                operation="drop_table",
                cause=e,
                details={"table_name": table_name}
            )
    
    async def create_index(self, index_def: IndexDefinition) -> None:
        """
        Create index from definition.
        
        Args:
            index_def: Index definition
        """
        start_time = self.logger.log_operation_start(
            "create_index",
            extra={"index_name": index_def.name, "table_name": index_def.table_name}
        )
        
        try:
            sql = index_def.to_sql()
            await self.connection.execute_async(sql, table=index_def.table_name)
            
            # Add comment if provided
            if index_def.comment:
                comment_sql = f"COMMENT ON INDEX {index_def.name} IS '{index_def.comment}'"
                await self.connection.execute_async(comment_sql)
            
            self.logger.log_operation_end(
                "create_index",
                start_time,
                success=True,
                extra={"index_name": index_def.name}
            )
            
        except Exception as e:
            self.logger.log_operation_end("create_index", start_time, success=False, error=e)
            raise DatabaseError(
                f"Failed to create index {index_def.name}: {str(e)}",
                operation="create_index",
                cause=e,
                details={"index_name": index_def.name}
            )
    
    async def drop_index(self, index_name: str, cascade: bool = False) -> None:
        """
        Drop index.
        
        Args:
            index_name: Index name to drop
            cascade: Whether to use CASCADE option
        """
        start_time = self.logger.log_operation_start(
            "drop_index",
            extra={"index_name": index_name, "cascade": cascade}
        )
        
        try:
            cascade_sql = " CASCADE" if cascade else ""
            sql = f"DROP INDEX IF EXISTS {index_name}{cascade_sql}"
            
            await self.connection.execute_async(sql)
            
            self.logger.log_operation_end(
                "drop_index",
                start_time,
                success=True,
                extra={"index_name": index_name}
            )
            
        except Exception as e:
            self.logger.log_operation_end("drop_index", start_time, success=False, error=e)
            raise DatabaseError(
                f"Failed to drop index {index_name}: {str(e)}",
                operation="drop_index",
                cause=e,
                details={"index_name": index_name}
            )
    
    async def apply_migration(self, migration: MigrationDefinition) -> None:
        """
        Apply a migration.
        
        Args:
            migration: Migration definition
        """
        start_time = self.logger.log_operation_start(
            "apply_migration",
            extra={"version": migration.version, "name": migration.name}
        )
        
        try:
            # Check if migration already applied
            existing = await self.connection.execute_async(
                f"SELECT version FROM {self.migration_table} WHERE version = $1",
                migration.version,
                fetch="one"
            )
            
            if existing:
                self.logger.warning(
                    f"Migration {migration.version} already applied",
                    operation="apply_migration",
                    extra={"version": migration.version}
                )
                return
            
            migration_start = datetime.now()
            
            # Execute migration statements
            for statement in migration.up_sql:
                await self.connection.execute_async(statement)
            
            execution_time = int((datetime.now() - migration_start).total_seconds() * 1000)
            
            # Record migration
            await self.connection.execute_async(
                f"""
                INSERT INTO {self.migration_table} 
                (version, name, description, execution_time_ms)
                VALUES ($1, $2, $3, $4)
                """,
                migration.version,
                migration.name,
                migration.description,
                execution_time
            )
            
            # Update current version
            await self.connection.execute_async(
                f"""
                UPDATE {self.version_table} 
                SET current_version = $1, updated_at = CURRENT_TIMESTAMP
                """,
                migration.version
            )
            
            self.logger.log_operation_end(
                "apply_migration",
                start_time,
                success=True,
                extra={
                    "version": migration.version,
                    "execution_time_ms": execution_time
                }
            )
            
        except Exception as e:
            self.logger.log_operation_end("apply_migration", start_time, success=False, error=e)
            raise DatabaseError(
                f"Failed to apply migration {migration.version}: {str(e)}",
                operation="apply_migration",
                cause=e,
                details={"version": migration.version, "name": migration.name}
            )
    
    async def rollback_migration(self, migration: MigrationDefinition) -> None:
        """
        Rollback a migration.
        
        Args:
            migration: Migration definition
        """
        start_time = self.logger.log_operation_start(
            "rollback_migration",
            extra={"version": migration.version, "name": migration.name}
        )
        
        try:
            # Execute rollback statements
            for statement in migration.down_sql:
                await self.connection.execute_async(statement)
            
            # Remove migration record
            await self.connection.execute_async(
                f"DELETE FROM {self.migration_table} WHERE version = $1",
                migration.version
            )
            
            # Update current version to previous
            previous_version = await self.connection.execute_async(
                f"""
                SELECT version FROM {self.migration_table} 
                ORDER BY applied_at DESC 
                LIMIT 1
                """,
                fetch="one"
            )
            
            current_version = previous_version[0] if previous_version else "0.0.0"
            
            await self.connection.execute_async(
                f"""
                UPDATE {self.version_table} 
                SET current_version = $1, updated_at = CURRENT_TIMESTAMP
                """,
                current_version
            )
            
            self.logger.log_operation_end(
                "rollback_migration",
                start_time,
                success=True,
                extra={"version": migration.version, "new_current_version": current_version}
            )
            
        except Exception as e:
            self.logger.log_operation_end("rollback_migration", start_time, success=False, error=e)
            raise DatabaseError(
                f"Failed to rollback migration {migration.version}: {str(e)}",
                operation="rollback_migration",
                cause=e,
                details={"version": migration.version, "name": migration.name}
            )
    
    async def get_current_version(self) -> str:
        """Get current schema version."""
        try:
            result = await self.connection.execute_async(
                f"SELECT current_version FROM {self.version_table} ORDER BY updated_at DESC LIMIT 1",
                fetch="one"
            )
            return result[0] if result else "0.0.0"
        except Exception as e:
            raise DatabaseError(
                f"Failed to get current schema version: {str(e)}",
                operation="get_current_version",
                cause=e
            )
    
    async def get_applied_migrations(self) -> List[Dict[str, Any]]:
        """Get list of applied migrations."""
        try:
            results = await self.connection.execute_async(
                f"""
                SELECT version, name, description, applied_at, execution_time_ms
                FROM {self.migration_table}
                ORDER BY applied_at ASC
                """,
                fetch="all"
            )
            
            return [dict(row) for row in results] if results else []
        except Exception as e:
            raise DatabaseError(
                f"Failed to get applied migrations: {str(e)}",
                operation="get_applied_migrations",
                cause=e
            )
    
    async def table_exists(self, table_name: str) -> bool:
        """Check if table exists."""
        try:
            result = await self.connection.execute_async(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = $1 AND table_name = $2
                )
                """,
                self.schema_name,
                table_name,
                fetch="one"
            )
            return result[0] if result else False
        except Exception as e:
            raise DatabaseError(
                f"Failed to check if table {table_name} exists: {str(e)}",
                operation="table_exists",
                cause=e,
                details={"table_name": table_name}
            )
    
    async def index_exists(self, index_name: str) -> bool:
        """Check if index exists."""
        try:
            result = await self.connection.execute_async(
                """
                SELECT EXISTS (
                    SELECT FROM pg_indexes 
                    WHERE schemaname = $1 AND indexname = $2
                )
                """,
                self.schema_name,
                index_name,
                fetch="one"
            )
            return result[0] if result else False
        except Exception as e:
            raise DatabaseError(
                f"Failed to check if index {index_name} exists: {str(e)}",
                operation="index_exists",
                cause=e,
                details={"index_name": index_name}
            )


def create_schema(connection: DatabaseConnection, schema_name: str = "public") -> DatabaseSchema:
    """
    Create database schema manager.
    
    Args:
        connection: Database connection instance
        schema_name: Schema name to manage
        
    Returns:
        DatabaseSchema instance
    """
    return DatabaseSchema(connection, schema_name)


async def migrate_schema(
    schema: DatabaseSchema,
    migrations: List[MigrationDefinition],
    target_version: Optional[str] = None
) -> None:
    """
    Migrate schema to target version.
    
    Args:
        schema: Database schema manager
        migrations: List of migrations
        target_version: Target version (latest if None)
    """
    start_time = logger.log_operation_start(
        "migrate_schema",
        extra={"target_version": target_version, "migrations_count": len(migrations)}
    )
    
    try:
        await schema.initialize_migration_tracking()
        current_version = await schema.get_current_version()
        applied_migrations = await schema.get_applied_migrations()
        applied_versions = {m["version"] for m in applied_migrations}
        
        # Sort migrations by version
        sorted_migrations = sorted(migrations, key=lambda m: m.version)
        
        # Determine target
        if target_version is None:
            target_version = sorted_migrations[-1].version if sorted_migrations else current_version
        
        # Apply pending migrations
        for migration in sorted_migrations:
            if migration.version not in applied_versions:
                if migration.version <= target_version:
                    await schema.apply_migration(migration)
                    logger.info(
                        f"Applied migration {migration.version}: {migration.name}",
                        operation="migrate_schema",
                        extra={"version": migration.version}
                    )
                else:
                    break
        
        final_version = await schema.get_current_version()
        
        logger.log_operation_end(
            "migrate_schema",
            start_time,
            success=True,
            extra={
                "initial_version": current_version,
                "final_version": final_version,
                "target_version": target_version
            }
        )
        
    except Exception as e:
        logger.log_operation_end("migrate_schema", start_time, success=False, error=e)
        raise DatabaseError(
            f"Failed to migrate schema: {str(e)}",
            operation="migrate_schema",
            cause=e,
            details={"target_version": target_version}
        )


async def get_schema_version(schema: DatabaseSchema) -> str:
    """
    Get current schema version.
    
    Args:
        schema: Database schema manager
        
    Returns:
        Current schema version
    """
    return await schema.get_current_version() 