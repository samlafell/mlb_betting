# Action Network Database Integration Solution

## Overview

I've implemented a comprehensive solution for tracking Action Network betting line movements in PostgreSQL with automatic incremental updates. The solution includes database schema, repository layer, enhanced data service, and CLI management tools.

## 🗄️ Database Schema (`sql/action_network_betting_tables.sql`)

### Tables Created

1. **`action_network.sportsbooks`** - Reference table for Action Network sportsbooks
   - Maps book IDs (15=DraftKings, 30=FanDuel, etc.) to names
   - Pre-populated with common sportsbooks

2. **`action_network.betting_lines`** - Main table for line movement history
   - Stores every historical betting line with timestamps
   - Tracks moneyline, spread, and total markets
   - Includes betting splits data (Bet% and Bet$) [[memory:2997555]]
   - Supports both pregame and live periods
   - Unique constraint prevents duplicates

3. **`action_network.extraction_log`** - Tracks extraction status for incremental updates
   - Records when each game was last processed
   - Enables checking for new movements since last run
   - Tracks success/failure status and error messages

4. **`action_network.line_movement_summary`** - Aggregated data for fast queries
   - Automatically maintained via triggers
   - Stores opening/closing lines, movement statistics
   - Tracks sharp action indicators (RLM, steam moves)

### Key Features

- **Automatic Triggers**: Updates summary table when new lines are inserted
- **PostgreSQL Functions**: Helper functions for incremental updates
- **Comprehensive Indexing**: Optimized for common query patterns
- **Views**: Easy-to-use views with sportsbook names joined

## 🔧 Repository Layer (`src/data/database/action_network_repository.py`)

### ActionNetworkRepository Class

```python
class ActionNetworkRepository:
    async def save_historical_data(historical_data) -> Dict[str, Any]
    async def get_last_extraction_time(game_id) -> Optional[datetime]
    async def get_new_lines_since_last_extraction(game_id, book_id, market_type) -> List[Dict]
    async def get_line_movement_summary(game_id, book_id, market_type) -> List[Dict]
    async def health_check() -> Dict[str, Any]
```

### Features

- **Smart Data Parsing**: Handles the new Action Network API format (dict with sportsbook IDs as keys)
- **Comprehensive Line Extraction**: Processes moneyline, spread, and total markets
- **Betting Splits Integration**: Records Bet% and Bet$ data [[memory:2997555]]
- **Conflict Resolution**: Uses ON CONFLICT DO UPDATE for upserts
- **Error Handling**: Detailed logging and graceful error recovery

## 🚀 Enhanced UnifiedDataService (`src/services/data/unified_data_service.py`)

### New Features Added

1. **Automatic Database Saving**: All Action Network data is automatically saved to database
2. **Incremental Update Detection**: Checks for new movements since last extraction
3. **Duplicate Prevention**: Skips saves if recent extraction exists (within 5 minutes)
4. **Database Connection Management**: Handles connection lifecycle and cleanup

### New Methods

```python
async def _save_to_database(historical_data) -> None
async def get_new_lines_since_last_extraction(game_id, book_id, market_type) -> List[Dict]
async def get_line_movement_summary(game_id, book_id, market_type) -> List[Dict]
async def check_for_new_movements(games_data) -> Dict[str, Any]
async def cleanup() -> None
```

### Automatic Integration

- **Pipeline Integration**: All existing Action Network collection automatically saves to database
- **Statistics Tracking**: Enhanced stats include database save metrics
- **Resource Management**: Proper cleanup of database connections

## 🖥️ CLI Management Tools (`src/interfaces/cli/commands/setup_database.py`)

### Database Commands

```bash
# Set up Action Network schema
uv run python -m src.interfaces.cli database setup-action-network --test-connection

# Check database health and data
uv run python -m src.interfaces.cli database check-data

# Check specific game data
uv run python -m src.interfaces.cli database check-data -g 257653 -m moneyline

# Test database connection
uv run python -m src.interfaces.cli database test-connection
```

### Features

- **Schema Setup**: Executes the SQL schema file with error handling
- **Health Checks**: Validates tables exist and shows record counts
- **Data Inspection**: View line movement summaries and statistics
- **Connection Testing**: Verify PostgreSQL connectivity

## 📊 Incremental Update Logic

### How It Works

1. **First Run**: All historical data is collected and saved
2. **Subsequent Runs**: System checks `extraction_log` for last extraction time
3. **New Data Detection**: Uses PostgreSQL function to find lines newer than last extraction
4. **Selective Processing**: Only processes games with new movements
5. **Automatic Triggers**: Summary tables are updated automatically

### Key Benefits

- **Efficiency**: Only processes new data, not entire history
- **Completeness**: Never misses line movements
- **Performance**: Fast queries using indexed timestamps
- **Reliability**: Duplicate detection prevents data corruption

## 🎯 Example Usage

### Miami Marlins Example (From Our Test)

The system successfully tracked the Miami Marlins vs Baltimore Orioles game:

- **Game ID**: 257653
- **Opening Line**: Miami -134 (July 12, 7:13 PM UTC)
- **Closing Line**: Miami -146 (July 13, 7:34 PM UTC)
- **Movement**: 12-point movement against Miami (became bigger favorites)
- **Data Points**: 6 historical entries from 6 different sportsbooks
- **Betting Splits**: Tracked ticket % and money % for each line

### Database Storage

```sql
-- All line movements stored with full detail
SELECT * FROM action_network.betting_lines 
WHERE game_id = 257653 AND market_type = 'moneyline' AND side = 'home';

-- Quick summary view
SELECT * FROM action_network.line_movement_summary 
WHERE game_id = 257653 AND market_type = 'moneyline';

-- Check for new movements since last run
SELECT * FROM action_network.get_new_lines_since_last_extraction(257653, 15, 'moneyline', '2025-07-13 20:00:00');
```

## 🔄 Automatic Pipeline Integration

### Enhanced Action Network Pipeline

The existing pipeline command now automatically:

1. **Collects** Action Network data as before
2. **Saves** all data to PostgreSQL database
3. **Tracks** extraction status in `extraction_log`
4. **Updates** summary tables via triggers
5. **Prevents** duplicate processing

### Command Usage

```bash
# Run pipeline with automatic database saving
uv run python -m src.interfaces.cli action-network pipeline

# Data is automatically saved to database
# Check results with:
uv run python -m src.interfaces.cli database check-data
```

## 🛠️ Setup Instructions

### 1. Database Setup

```bash
# Set up PostgreSQL database (ensure PostgreSQL 17 is running)
createdb mlb_betting

# Run the schema setup
uv run python -m src.interfaces.cli database setup-action-network --test-connection
```

### 2. Environment Configuration

Set these environment variables or update `.env`:

```bash
DB_HOST=localhost
DB_PORT=5432
DB_NAME=mlb_betting
DB_USER=postgres
DB_PASSWORD=your_password
```

### 3. Test the Integration

```bash
# Test database connection
uv run python -m src.interfaces.cli database test-connection

# Run Action Network pipeline (will auto-save to database)
uv run python -m src.interfaces.cli action-network pipeline --max-games 1

# Check saved data
uv run python -m src.interfaces.cli database check-data
```

## 📈 Benefits Achieved

### 1. Complete Line Movement Tracking
- ✅ All 3 major markets (ML/Spread/Total) tracked
- ✅ All Action Network sportsbooks supported
- ✅ Betting splits data preserved [[memory:2997555]]
- ✅ Pregame and live periods distinguished

### 2. Incremental Updates
- ✅ Detects new movements since last extraction
- ✅ Prevents duplicate processing
- ✅ Efficient resource usage
- ✅ Automatic deduplication

### 3. Automatic Integration
- ✅ No code changes needed for existing pipelines
- ✅ All Action Network data automatically saved
- ✅ Backward compatible with existing workflows
- ✅ Enhanced with database persistence

### 4. Query Performance
- ✅ Optimized indexes for common patterns
- ✅ Summary tables for fast aggregations
- ✅ PostgreSQL functions for complex queries
- ✅ Views for easy data access

### 5. Management Tools
- ✅ CLI commands for database operations
- ✅ Health checks and monitoring
- ✅ Data inspection tools
- ✅ Setup automation

## 🔮 Next Steps

1. **Production Deployment**: Set up PostgreSQL 17 database
2. **Monitoring**: Add alerts for extraction failures
3. **Analytics**: Build dashboards on the line movement data
4. **Expansion**: Extend to other data sources (VSIN, SBD)
5. **Sharp Action Detection**: Implement algorithms using the stored data

## 🏆 Summary

This solution provides a complete, production-ready system for tracking Action Network betting line movements with:

- **Robust database schema** optimized for line movement analysis
- **Automatic data collection** and storage without manual intervention
- **Incremental updates** to efficiently handle ongoing data collection
- **Management tools** for setup, monitoring, and data inspection
- **Full integration** with existing Action Network pipeline

The system is now ready to automatically track and analyze betting line movements across all Action Network sportsbooks and markets, providing the foundation for advanced sharp action detection and betting strategy development.

General Balls 