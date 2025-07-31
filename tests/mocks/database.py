"""
Mock database implementations for unit testing.

Provides in-memory database mocks and fixtures for testing without real database dependencies.
"""

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from unittest.mock import Mock, AsyncMock
from uuid import uuid4


class MockConnection:
    """Mock database connection."""
    
    def __init__(self):
        self.data_store: Dict[str, List[Dict[str, Any]]] = {
            # Raw data tables
            "raw_data.action_network_odds": [],
            "raw_data.action_network_history": [],
            "raw_data.action_network_games": [],
            "raw_data.sbd_odds": [],
            "raw_data.vsin_reports": [],
            
            # Staging tables
            "staging.action_network_games": [],
            "staging.action_network_odds_historical": [],
            
            # Analytics tables
            "analytics.sharp_action_signals": [],
            "analytics.line_movement_analysis": [],
        }
        
        self.transaction_active = False
        self.call_history = []
    
    async def execute(self, query: str, *params) -> str:
        """Mock execute method."""
        self.call_history.append({"method": "execute", "query": query, "params": params})
        
        # Handle DELETE operations
        if query.strip().upper().startswith("DELETE"):
            return await self._handle_delete(query, params)
        
        # Handle INSERT operations
        if query.strip().upper().startswith("INSERT"):
            return await self._handle_insert(query, params)
        
        # Handle UPDATE operations
        if query.strip().upper().startswith("UPDATE"):
            return await self._handle_update(query, params)
        
        return "MOCK_RESULT"
    
    async def fetch(self, query: str, *params) -> List[Dict[str, Any]]:
        """Mock fetch method."""
        self.call_history.append({"method": "fetch", "query": query, "params": params})
        
        # Simple mock - return empty list or sample data based on query
        if "action_network_games" in query:
            return [
                {
                    "id": 1,
                    "external_game_id": "test_game_123",
                    "home_team": "Yankees",
                    "away_team": "Red Sox",
                    "game_date": datetime.utcnow().date(),
                    "game_status": "scheduled"
                }
            ]
        
        if "action_network_odds_historical" in query:
            return [
                {
                    "id": 1,
                    "external_game_id": "test_game_123",
                    "sportsbook_name": "DraftKings",
                    "market_type": "moneyline",
                    "side": "home",
                    "odds": -150,
                    "updated_at": datetime.utcnow()
                }
            ]
        
        # Default empty result
        return []
    
    async def fetchrow(self, query: str, *params) -> Optional[Dict[str, Any]]:
        """Mock fetchrow method."""
        self.call_history.append({"method": "fetchrow", "query": query, "params": params})
        
        results = await self.fetch(query, *params)
        return results[0] if results else None
    
    async def fetchval(self, query: str, *params) -> Any:
        """Mock fetchval method."""
        self.call_history.append({"method": "fetchval", "query": query, "params": params})
        
        # Handle COUNT queries
        if "COUNT(*)" in query.upper():
            if "action_network_odds" in query and "processed_at IS NULL" in query:
                return 5  # Mock unprocessed records
            return 10  # Default count
        
        # Handle ID return for INSERT...RETURNING
        if "RETURNING" in query.upper():
            return 123  # Mock ID
        
        # Default value
        return None
    
    async def _handle_delete(self, query: str, params: tuple) -> str:
        """Handle DELETE operations."""
        # Extract table name
        query_upper = query.upper()
        if "FROM" in query_upper:
            table_part = query_upper.split("FROM")[1].split("WHERE")[0].strip()
            table_name = table_part.split()[0]
            
            if table_name in self.data_store:
                # Simple implementation - clear data for test IDs if ANY() is used
                if params and isinstance(params[0], list):
                    test_ids = params[0]
                    original_count = len(self.data_store[table_name])
                    self.data_store[table_name] = [
                        row for row in self.data_store[table_name]
                        if row.get("external_game_id") not in test_ids
                    ]
                    deleted_count = original_count - len(self.data_store[table_name])
                    return f"DELETE {deleted_count}"
        
        return "DELETE 0"
    
    async def _handle_insert(self, query: str, params: tuple) -> str:
        """Handle INSERT operations."""
        # Extract table name
        query_upper = query.upper()
        if "INTO" in query_upper:
            table_part = query_upper.split("INTO")[1].split("(")[0].strip()
            
            if table_part in self.data_store:
                # Create mock record
                mock_record = {
                    "id": len(self.data_store[table_part]) + 1,
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow()
                }
                
                # Add parameters as fields (simplified)
                if params:
                    mock_record["external_game_id"] = params[0] if params else f"mock_{uuid4().hex[:8]}"
                
                self.data_store[table_part].append(mock_record)
                return "INSERT 0 1"
        
        return "INSERT 0 1"
    
    async def _handle_update(self, query: str, params: tuple) -> str:
        """Handle UPDATE operations."""
        return "UPDATE 1"
    
    async def begin(self):
        """Mock transaction begin."""
        self.transaction_active = True
    
    async def commit(self):
        """Mock transaction commit."""
        self.transaction_active = False
    
    async def rollback(self):
        """Mock transaction rollback."""
        self.transaction_active = False
    
    def get_call_history(self) -> List[Dict[str, Any]]:
        """Get history of database calls."""
        return self.call_history.copy()
    
    def clear_call_history(self):
        """Clear call history."""
        self.call_history.clear()
    
    def get_table_data(self, table_name: str) -> List[Dict[str, Any]]:
        """Get data from a mock table."""
        return self.data_store.get(table_name, []).copy()
    
    def clear_table_data(self, table_name: str):
        """Clear data from a mock table."""
        if table_name in self.data_store:
            self.data_store[table_name].clear()
    
    def add_table_data(self, table_name: str, data: List[Dict[str, Any]]):
        """Add data to a mock table."""
        if table_name not in self.data_store:
            self.data_store[table_name] = []
        self.data_store[table_name].extend(data)


class MockConnectionPool:
    """Mock database connection pool."""
    
    def __init__(self):
        self.connections = []
        self.max_connections = 10
        self.active_connections = 0
    
    async def acquire(self):
        """Acquire a connection from the pool."""
        self.active_connections += 1
        connection = MockConnection()
        self.connections.append(connection)
        return MockConnectionContext(connection, self)
    
    async def close(self):
        """Close the connection pool."""
        self.connections.clear()
        self.active_connections = 0
    
    def release_connection(self, connection: MockConnection):
        """Release a connection back to the pool."""
        if self.active_connections > 0:
            self.active_connections -= 1
    
    def get_pool_stats(self) -> Dict[str, Any]:
        """Get pool statistics."""
        return {
            "active_connections": self.active_connections,
            "total_connections": len(self.connections),
            "max_connections": self.max_connections
        }


class MockConnectionContext:
    """Context manager for mock database connections."""
    
    def __init__(self, connection: MockConnection, pool: MockConnectionPool):
        self.connection = connection
        self.pool = pool
    
    async def __aenter__(self):
        return self.connection
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.pool.release_connection(self.connection)


class InMemoryDatabase:
    """In-memory database for testing."""
    
    def __init__(self):
        self.tables: Dict[str, List[Dict[str, Any]]] = {}
        self.sequences: Dict[str, int] = {}
    
    def create_table(self, table_name: str, schema: Dict[str, str]):
        """Create a table with schema."""
        self.tables[table_name] = []
        if "id" in schema:
            self.sequences[f"{table_name}_id_seq"] = 0
    
    def insert(self, table_name: str, data: Dict[str, Any]) -> int:
        """Insert data into table."""
        if table_name not in self.tables:
            self.tables[table_name] = []
        
        # Auto-generate ID if not provided
        if "id" not in data:
            seq_name = f"{table_name}_id_seq"
            self.sequences[seq_name] = self.sequences.get(seq_name, 0) + 1
            data["id"] = self.sequences[seq_name]
        
        # Add timestamps
        now = datetime.utcnow()
        if "created_at" not in data:
            data["created_at"] = now
        if "updated_at" not in data:
            data["updated_at"] = now
        
        self.tables[table_name].append(data.copy())
        return data["id"]
    
    def select(self, table_name: str, where: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Select data from table."""
        if table_name not in self.tables:
            return []
        
        results = self.tables[table_name].copy()
        
        if where:
            filtered_results = []
            for row in results:
                match = all(row.get(key) == value for key, value in where.items())
                if match:
                    filtered_results.append(row)
            results = filtered_results
        
        return results
    
    def update(self, table_name: str, where: Dict[str, Any], updates: Dict[str, Any]) -> int:
        """Update data in table."""
        if table_name not in self.tables:
            return 0
        
        updated_count = 0
        for row in self.tables[table_name]:
            match = all(row.get(key) == value for key, value in where.items())
            if match:
                row.update(updates)
                row["updated_at"] = datetime.utcnow()
                updated_count += 1
        
        return updated_count
    
    def delete(self, table_name: str, where: Dict[str, Any]) -> int:
        """Delete data from table."""
        if table_name not in self.tables:
            return 0
        
        original_count = len(self.tables[table_name])
        self.tables[table_name] = [
            row for row in self.tables[table_name]
            if not all(row.get(key) == value for key, value in where.items())
        ]
        
        return original_count - len(self.tables[table_name])
    
    def count(self, table_name: str, where: Optional[Dict[str, Any]] = None) -> int:
        """Count rows in table."""
        return len(self.select(table_name, where))
    
    def clear_table(self, table_name: str):
        """Clear all data from table."""
        if table_name in self.tables:
            self.tables[table_name].clear()
    
    def clear_all(self):
        """Clear all data from all tables."""
        for table_name in self.tables:
            self.tables[table_name].clear()
        self.sequences.clear()


def create_mock_db_pool() -> MockConnectionPool:
    """Create a mock database connection pool."""
    return MockConnectionPool()


def create_in_memory_db() -> InMemoryDatabase:
    """Create an in-memory database for testing."""
    db = InMemoryDatabase()
    
    # Create common tables
    db.create_table("raw_data.action_network_odds", {
        "id": "SERIAL",
        "external_game_id": "VARCHAR",
        "raw_response": "JSONB", 
        "collected_at": "TIMESTAMP",
        "processed_at": "TIMESTAMP"
    })
    
    db.create_table("staging.action_network_games", {
        "id": "SERIAL",
        "external_game_id": "VARCHAR",
        "home_team": "VARCHAR",
        "away_team": "VARCHAR",
        "game_date": "DATE",
        "game_status": "VARCHAR"
    })
    
    db.create_table("staging.action_network_odds_historical", {
        "id": "SERIAL",
        "external_game_id": "VARCHAR",
        "sportsbook_name": "VARCHAR",
        "market_type": "VARCHAR",
        "side": "VARCHAR",
        "odds": "INTEGER",
        "line_value": "DECIMAL",
        "updated_at": "TIMESTAMP"
    })
    
    return db