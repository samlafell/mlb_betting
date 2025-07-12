"""
Database Base Classes and Repository Patterns

Provides base classes for database operations, repository patterns,
and comprehensive error handling for the unified betting system.
"""

import asyncio
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional, Type, TypeVar, Union, Generic
from uuid import UUID

from pydantic import BaseModel, ValidationError
import asyncpg
from psycopg2.extras import RealDictCursor

from ...core.config import UnifiedSettings
from ...core.exceptions import DatabaseError, ValidationError as UnifiedValidationError
from ...core.logging import LogComponent, get_logger
from ..models.unified.base import UnifiedBaseModel

logger = get_logger(__name__, LogComponent.DATABASE)

# Type variables for generic repository
T = TypeVar('T', bound=UnifiedBaseModel)
CreateSchemaType = TypeVar('CreateSchemaType', bound=BaseModel)
UpdateSchemaType = TypeVar('UpdateSchemaType', bound=BaseModel)


class DatabaseError(Exception):
    """Base database error - re-exported for convenience."""
    pass


class ConnectionError(DatabaseError):
    """Database connection error."""
    pass


class QueryError(DatabaseError):
    """Database query error."""
    pass


class TransactionError(DatabaseError):
    """Database transaction error."""
    pass


class BaseModel(UnifiedBaseModel):
    """Base model for database entities - re-exported for convenience."""
    pass


class BaseRepository(ABC, Generic[T, CreateSchemaType, UpdateSchemaType]):
    """
    Base repository class providing common database operations.
    
    Implements the repository pattern with async support, comprehensive
    error handling, and consistent logging.
    """
    
    def __init__(
        self,
        connection,
        model_class: Type[T],
        table_name: str,
        *,
        primary_key: str = "id",
        created_at_field: str = "created_at",
        updated_at_field: str = "updated_at",
    ):
        """
        Initialize base repository.
        
        Args:
            connection: Database connection instance
            model_class: Pydantic model class
            table_name: Database table name
            primary_key: Primary key field name
            created_at_field: Created timestamp field name
            updated_at_field: Updated timestamp field name
        """
        self.connection = connection
        self.model_class = model_class
        self.table_name = table_name
        self.primary_key = primary_key
        self.created_at_field = created_at_field
        self.updated_at_field = updated_at_field
        
        # Create logger with repository context
        self.logger = logger.with_context(
            repository=self.__class__.__name__,
            table=table_name,
            model=model_class.__name__
        )
    
    def _build_select_query(
        self,
        where_clause: Optional[str] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> str:
        """Build SELECT query with optional clauses."""
        query = f"SELECT * FROM {self.table_name}"
        
        if where_clause:
            query += f" WHERE {where_clause}"
        
        if order_by:
            query += f" ORDER BY {order_by}"
        
        if limit:
            query += f" LIMIT {limit}"
        
        if offset:
            query += f" OFFSET {offset}"
        
        return query
    
    def _build_insert_query(self, data: Dict[str, Any]) -> tuple[str, List[Any]]:
        """Build INSERT query with parameters."""
        fields = list(data.keys())
        placeholders = ", ".join(f"${i+1}" for i in range(len(fields)))
        values = list(data.values())
        
        query = f"""
            INSERT INTO {self.table_name} ({', '.join(fields)})
            VALUES ({placeholders})
            RETURNING *
        """
        
        return query, values
    
    def _build_update_query(
        self,
        data: Dict[str, Any],
        where_clause: str,
        where_params: List[Any]
    ) -> tuple[str, List[Any]]:
        """Build UPDATE query with parameters."""
        set_clauses = []
        values = []
        
        for i, (field, value) in enumerate(data.items()):
            set_clauses.append(f"{field} = ${i+1}")
            values.append(value)
        
        # Add where parameters
        where_param_start = len(values) + 1
        where_clause_with_params = where_clause
        for i, _ in enumerate(where_params):
            where_clause_with_params = where_clause_with_params.replace(
                f"${i+1}", f"${where_param_start + i}"
            )
        
        values.extend(where_params)
        
        query = f"""
            UPDATE {self.table_name}
            SET {', '.join(set_clauses)}
            WHERE {where_clause_with_params}
            RETURNING *
        """
        
        return query, values
    
    def _model_to_dict(self, model: T) -> Dict[str, Any]:
        """Convert model to dictionary for database operations."""
        data = model.model_dump()
        
        # Handle special fields
        if hasattr(model, 'created_at') and model.created_at:
            data[self.created_at_field] = model.created_at
        if hasattr(model, 'updated_at') and model.updated_at:
            data[self.updated_at_field] = model.updated_at
        
        return data
    
    def _dict_to_model(self, data: Dict[str, Any]) -> T:
        """Convert dictionary to model instance."""
        try:
            return self.model_class(**data)
        except ValidationError as e:
            raise UnifiedValidationError(
                f"Failed to create {self.model_class.__name__} from database data",
                validation_errors=[str(error) for error in e.errors()],
                field_value=data,
                operation="dict_to_model"
            )
    
    async def create(self, data: CreateSchemaType) -> T:
        """
        Create a new record.
        
        Args:
            data: Create schema instance
            
        Returns:
            Created model instance
        """
        start_time = self.logger.log_operation_start(
            "repository_create",
            extra={"model": self.model_class.__name__}
        )
        
        try:
            # Convert to dict and add timestamps
            create_data = data.model_dump()
            create_data[self.created_at_field] = datetime.now()
            create_data[self.updated_at_field] = datetime.now()
            
            # Build and execute query
            query, values = self._build_insert_query(create_data)
            
            result = await self.connection.execute_async(
                query,
                *values,
                fetch="one",
                table=self.table_name
            )
            
            if not result:
                raise QueryError(f"Failed to create {self.model_class.__name__}")
            
            # Convert result to model
            created_model = self._dict_to_model(dict(result))
            
            self.logger.log_operation_end(
                "repository_create",
                start_time,
                success=True,
                extra={
                    "model": self.model_class.__name__,
                    "record_id": getattr(created_model, self.primary_key, None)
                }
            )
            
            return created_model
            
        except Exception as e:
            self.logger.log_operation_end(
                "repository_create",
                start_time,
                success=False,
                error=e
            )
            raise DatabaseError(
                f"Failed to create {self.model_class.__name__}: {str(e)}",
                operation="repository_create",
                cause=e,
                details={
                    "table": self.table_name,
                    "model": self.model_class.__name__
                }
            )
    
    async def get_by_id(self, record_id: Union[str, int, UUID]) -> Optional[T]:
        """
        Get record by ID.
        
        Args:
            record_id: Record ID
            
        Returns:
            Model instance or None if not found
        """
        start_time = self.logger.log_operation_start(
            "repository_get_by_id",
            extra={"record_id": str(record_id)}
        )
        
        try:
            query = self._build_select_query(
                where_clause=f"{self.primary_key} = $1"
            )
            
            result = await self.connection.execute_async(
                query,
                record_id,
                fetch="one",
                table=self.table_name
            )
            
            if not result:
                self.logger.log_operation_end(
                    "repository_get_by_id",
                    start_time,
                    success=True,
                    extra={"record_id": str(record_id), "found": False}
                )
                return None
            
            model = self._dict_to_model(dict(result))
            
            self.logger.log_operation_end(
                "repository_get_by_id",
                start_time,
                success=True,
                extra={"record_id": str(record_id), "found": True}
            )
            
            return model
            
        except Exception as e:
            self.logger.log_operation_end(
                "repository_get_by_id",
                start_time,
                success=False,
                error=e
            )
            raise DatabaseError(
                f"Failed to get {self.model_class.__name__} by ID: {str(e)}",
                operation="repository_get_by_id",
                cause=e,
                details={
                    "table": self.table_name,
                    "record_id": str(record_id)
                }
            )
    
    async def get_all(
        self,
        *,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: Optional[str] = None,
    ) -> List[T]:
        """
        Get all records with optional pagination and ordering.
        
        Args:
            limit: Maximum number of records to return
            offset: Number of records to skip
            order_by: Order by clause
            
        Returns:
            List of model instances
        """
        start_time = self.logger.log_operation_start(
            "repository_get_all",
            extra={"limit": limit, "offset": offset, "order_by": order_by}
        )
        
        try:
            query = self._build_select_query(
                order_by=order_by or f"{self.created_at_field} DESC",
                limit=limit,
                offset=offset
            )
            
            results = await self.connection.execute_async(
                query,
                fetch="all",
                table=self.table_name
            )
            
            models = [self._dict_to_model(dict(row)) for row in results]
            
            self.logger.log_operation_end(
                "repository_get_all",
                start_time,
                success=True,
                extra={"count": len(models)}
            )
            
            return models
            
        except Exception as e:
            self.logger.log_operation_end(
                "repository_get_all",
                start_time,
                success=False,
                error=e
            )
            raise DatabaseError(
                f"Failed to get all {self.model_class.__name__}: {str(e)}",
                operation="repository_get_all",
                cause=e,
                details={"table": self.table_name}
            )
    
    async def update(
        self,
        record_id: Union[str, int, UUID],
        data: UpdateSchemaType
    ) -> Optional[T]:
        """
        Update record by ID.
        
        Args:
            record_id: Record ID
            data: Update schema instance
            
        Returns:
            Updated model instance or None if not found
        """
        start_time = self.logger.log_operation_start(
            "repository_update",
            extra={"record_id": str(record_id)}
        )
        
        try:
            # Convert to dict and add updated timestamp
            update_data = data.model_dump(exclude_unset=True)
            update_data[self.updated_at_field] = datetime.now()
            
            # Build and execute query
            query, values = self._build_update_query(
                update_data,
                f"{self.primary_key} = $1",
                [record_id]
            )
            
            result = await self.connection.execute_async(
                query,
                *values,
                fetch="one",
                table=self.table_name
            )
            
            if not result:
                self.logger.log_operation_end(
                    "repository_update",
                    start_time,
                    success=True,
                    extra={"record_id": str(record_id), "found": False}
                )
                return None
            
            updated_model = self._dict_to_model(dict(result))
            
            self.logger.log_operation_end(
                "repository_update",
                start_time,
                success=True,
                extra={"record_id": str(record_id), "found": True}
            )
            
            return updated_model
            
        except Exception as e:
            self.logger.log_operation_end(
                "repository_update",
                start_time,
                success=False,
                error=e
            )
            raise DatabaseError(
                f"Failed to update {self.model_class.__name__}: {str(e)}",
                operation="repository_update",
                cause=e,
                details={
                    "table": self.table_name,
                    "record_id": str(record_id)
                }
            )
    
    async def delete(self, record_id: Union[str, int, UUID]) -> bool:
        """
        Delete record by ID.
        
        Args:
            record_id: Record ID
            
        Returns:
            True if deleted, False if not found
        """
        start_time = self.logger.log_operation_start(
            "repository_delete",
            extra={"record_id": str(record_id)}
        )
        
        try:
            query = f"DELETE FROM {self.table_name} WHERE {self.primary_key} = $1"
            
            result = await self.connection.execute_async(
                query,
                record_id,
                table=self.table_name
            )
            
            # Check if any rows were affected
            deleted = "DELETE 1" in str(result) if result else False
            
            self.logger.log_operation_end(
                "repository_delete",
                start_time,
                success=True,
                extra={"record_id": str(record_id), "deleted": deleted}
            )
            
            return deleted
            
        except Exception as e:
            self.logger.log_operation_end(
                "repository_delete",
                start_time,
                success=False,
                error=e
            )
            raise DatabaseError(
                f"Failed to delete {self.model_class.__name__}: {str(e)}",
                operation="repository_delete",
                cause=e,
                details={
                    "table": self.table_name,
                    "record_id": str(record_id)
                }
            )
    
    async def count(self, where_clause: Optional[str] = None) -> int:
        """
        Count records with optional filter.
        
        Args:
            where_clause: Optional WHERE clause
            
        Returns:
            Number of records
        """
        start_time = self.logger.log_operation_start("repository_count")
        
        try:
            query = f"SELECT COUNT(*) FROM {self.table_name}"
            if where_clause:
                query += f" WHERE {where_clause}"
            
            result = await self.connection.execute_async(
                query,
                fetch="one",
                table=self.table_name
            )
            
            count = result[0] if result else 0
            
            self.logger.log_operation_end(
                "repository_count",
                start_time,
                success=True,
                extra={"count": count}
            )
            
            return count
            
        except Exception as e:
            self.logger.log_operation_end(
                "repository_count",
                start_time,
                success=False,
                error=e
            )
            raise DatabaseError(
                f"Failed to count {self.model_class.__name__}: {str(e)}",
                operation="repository_count",
                cause=e,
                details={"table": self.table_name}
            )
    
    async def exists(self, record_id: Union[str, int, UUID]) -> bool:
        """
        Check if record exists by ID.
        
        Args:
            record_id: Record ID
            
        Returns:
            True if exists, False otherwise
        """
        start_time = self.logger.log_operation_start(
            "repository_exists",
            extra={"record_id": str(record_id)}
        )
        
        try:
            query = f"SELECT 1 FROM {self.table_name} WHERE {self.primary_key} = $1 LIMIT 1"
            
            result = await self.connection.execute_async(
                query,
                record_id,
                fetch="one",
                table=self.table_name
            )
            
            exists = result is not None
            
            self.logger.log_operation_end(
                "repository_exists",
                start_time,
                success=True,
                extra={"record_id": str(record_id), "exists": exists}
            )
            
            return exists
            
        except Exception as e:
            self.logger.log_operation_end(
                "repository_exists",
                start_time,
                success=False,
                error=e
            )
            raise DatabaseError(
                f"Failed to check existence of {self.model_class.__name__}: {str(e)}",
                operation="repository_exists",
                cause=e,
                details={
                    "table": self.table_name,
                    "record_id": str(record_id)
                }
            )
    
    @abstractmethod
    async def find_by_criteria(self, **criteria) -> List[T]:
        """
        Find records by custom criteria.
        
        Args:
            **criteria: Search criteria
            
        Returns:
            List of matching model instances
        """
        pass


class TransactionManager:
    """
    Transaction manager for database operations.
    
    Provides context manager for database transactions with
    automatic rollback on errors.
    """
    
    def __init__(self, connection):
        """
        Initialize transaction manager.
        
        Args:
            connection: Database connection instance
        """
        self.connection = connection
        self.transaction = None
        self.logger = logger.with_context(component="transaction_manager")
    
    async def __aenter__(self):
        """Start transaction."""
        start_time = self.logger.log_operation_start("transaction_begin")
        
        try:
            if hasattr(self.connection, 'get_async_connection'):
                async with self.connection.get_async_connection() as conn:
                    self.transaction = conn.transaction()
                    await self.transaction.start()
            else:
                # For direct connection objects
                self.transaction = self.connection.transaction()
                await self.transaction.start()
            
            self.logger.log_operation_end("transaction_begin", start_time, success=True)
            return self
            
        except Exception as e:
            self.logger.log_operation_end("transaction_begin", start_time, success=False, error=e)
            raise TransactionError(
                f"Failed to start transaction: {str(e)}",
                operation="transaction_begin",
                cause=e
            )
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """End transaction with commit or rollback."""
        if not self.transaction:
            return
        
        if exc_type is None:
            # No exception, commit transaction
            start_time = self.logger.log_operation_start("transaction_commit")
            try:
                await self.transaction.commit()
                self.logger.log_operation_end("transaction_commit", start_time, success=True)
            except Exception as e:
                self.logger.log_operation_end("transaction_commit", start_time, success=False, error=e)
                raise TransactionError(
                    f"Failed to commit transaction: {str(e)}",
                    operation="transaction_commit",
                    cause=e
                )
        else:
            # Exception occurred, rollback transaction
            start_time = self.logger.log_operation_start("transaction_rollback")
            try:
                await self.transaction.rollback()
                self.logger.log_operation_end("transaction_rollback", start_time, success=True)
            except Exception as e:
                self.logger.log_operation_end("transaction_rollback", start_time, success=False, error=e)
                # Log rollback error but don't raise to avoid masking original exception
                self.logger.error(
                    "Failed to rollback transaction",
                    operation="transaction_rollback",
                    error=e
                )
    
    async def commit(self):
        """Manually commit transaction."""
        if self.transaction:
            await self.transaction.commit()
    
    async def rollback(self):
        """Manually rollback transaction."""
        if self.transaction:
            await self.transaction.rollback() 