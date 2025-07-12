# Game Outcome Service Integration

## Overview

The Game Outcome Service automatically checks for completed MLB games and updates the `core_betting.game_outcomes` table with final scores and betting outcomes. This service is now integrated with the Action Network flow to ensure game outcomes are updated whenever the Action Network pipeline runs.

## Features

- **Automatic Game Outcome Checking**: Fetches completed games from MLB-StatsAPI
- **Betting Outcome Calculations**: Calculates home_win, over/under, and spread cover results
- **Database Integration**: Updates the `core_betting.game_outcomes` table
- **Time Zone Handling**: Converts UTC times to EST as required
- **Action Network Integration**: Seamlessly integrates with existing Action Network workflow
- **CLI Commands**: Provides comprehensive CLI interface for manual operations

## Integration Points

### 1. Action Network Quickstart Integration

The game outcome service is now integrated into the Action Network quickstart script (`action_network_quickstart.py`):

```python
# Step 1: Extract Game URLs
urls_file = self._extract_game_urls(date)

# Step 2: Check for completed games and update outcomes
asyncio.run(self._check_game_outcomes())

# Step 3: Display URLs and opportunities
self._display_games_and_opportunities(urls_file)
```

### 2. CLI Commands

New CLI commands are available under the `outcomes` group:

```bash
# Check outcomes for the last 7 days
uv run python -m src.interfaces.cli outcomes check --days 7

# Show recent outcomes in table format
uv run python -m src.interfaces.cli outcomes recent --days 7

# Check a specific game outcome
uv run python -m src.interfaces.cli outcomes single --game-id 123

# Force update outcomes (even if they already exist)
uv run python -m src.interfaces.cli outcomes check --force
```

### 3. Service Architecture

The service is implemented in `src/services/game_outcome_service.py` with the following components:

- **GameOutcomeService**: Main service class
- **MLBStatsAPIClient**: MLB Stats API client for fetching game data
- **GameOutcome**: Data class representing a completed game's outcome

## Database Schema

The service updates the `core_betting.game_outcomes` table with the following structure:

```sql
CREATE TABLE core_betting.game_outcomes (
    id SERIAL PRIMARY KEY,
    game_id INTEGER NOT NULL REFERENCES core_betting.games(id),
    home_team VARCHAR(5) NOT NULL,
    away_team VARCHAR(5) NOT NULL,
    home_score INTEGER NOT NULL,
    away_score INTEGER NOT NULL,
    
    -- Betting outcomes
    home_win BOOLEAN NOT NULL,
    over BOOLEAN NOT NULL,
    home_cover_spread BOOLEAN DEFAULT NULL,
    
    -- Additional context
    total_line DOUBLE PRECISION DEFAULT NULL,
    home_spread_line DOUBLE PRECISION DEFAULT NULL,
    game_date TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(game_id)
);
```

## MLB Stats API Integration

The service uses the MLB Stats API to fetch game data:

- **Base URL**: `https://statsapi.mlb.com/api/v1`
- **Schedule Endpoint**: `/schedule?sportId=1&date=YYYY-MM-DD`
- **Game Details Endpoint**: `/game/{gamePk}/feed/live`

### Game Status Codes

Games are considered completed when the status code is:
- `F` (Final)
- `O` (Official)

## Betting Outcome Calculations

### Home Win
```python
home_win = home_score > away_score
```

### Over/Under
```python
total_score = home_score + away_score
over = total_score > total_line  # if total_line is available
```

### Spread Cover
```python
# If home spread is negative, they're favored
# If positive, they're getting points
home_cover_spread = (home_score + home_spread_line) > away_score
```

## Team Name Mapping

The service includes comprehensive team name mapping from MLB Stats API names to our abbreviations:

```python
team_mappings = {
    "Los Angeles Angels": "LAA",
    "Houston Astros": "HOU",
    "Oakland Athletics": "OAK",
    # ... complete mapping for all 30 teams
}
```

## Usage Examples

### Programmatic Usage

```python
from src.services.game_outcome_service import check_game_outcomes, game_outcome_service

# Check outcomes for a specific date range
results = await check_game_outcomes(
    date_range=("2024-01-01", "2024-01-07"),
    force_update=False
)

# Get recent outcomes
recent = await game_outcome_service.get_recent_outcomes(days=7)
```

### CLI Usage

```bash
# Basic outcome check
uv run python -m src.interfaces.cli outcomes check

# Check specific date range
uv run python -m src.interfaces.cli outcomes check --start-date 2024-01-01 --end-date 2024-01-07

# Show recent outcomes as JSON
uv run python -m src.interfaces.cli outcomes recent --format json

# Check single game with force update
uv run python -m src.interfaces.cli outcomes single --game-id 123 --force
```

### Action Network Integration

```bash
# Run Action Network pipeline with game outcome checking
uv run python action_network_quickstart.py
```

## Error Handling

The service includes comprehensive error handling:

- **Database Connection Errors**: Graceful handling with retry logic
- **MLB API Errors**: Timeout handling and fallback mechanisms
- **Data Validation Errors**: Comprehensive validation of game data
- **Team Mapping Errors**: Fallback to existing team abbreviations

## Performance Considerations

- **Async Operations**: All database and API operations are asynchronous
- **Connection Pooling**: Uses connection pooling for database operations
- **Rate Limiting**: Respects MLB API rate limits
- **Caching**: Implements intelligent caching for repeated requests

## Configuration

The service uses the unified configuration system from `src/core/config.py`:

```python
# MLB Stats API configuration
mlb_stats_base_url: str = "https://statsapi.mlb.com/api/v1"
request_timeout: int = 30
max_retries: int = 3
```

## Testing

Test the integration with the provided test script:

```bash
uv run python test_game_outcomes.py
```

This script demonstrates:
1. Basic game outcome checking
2. Recent outcome retrieval
3. Integration with Action Network flow

## Logging

The service uses structured logging with contextual information:

```python
logger = structlog.get_logger(__name__)
logger.info("Updated game outcome",
           game_id=outcome.game_id,
           home_team=outcome.home_team,
           away_team=outcome.away_team,
           home_score=outcome.home_score,
           away_score=outcome.away_score)
```

## Future Enhancements

Potential future improvements:

1. **Real-time Updates**: WebSocket integration for live game updates
2. **Advanced Metrics**: Additional betting outcome calculations
3. **Historical Analysis**: Integration with backtesting systems
4. **Alert System**: Notifications for completed games
5. **Performance Optimization**: Batch processing for large date ranges

## Troubleshooting

### Common Issues

1. **Database Connection Errors**
   - Ensure PostgreSQL is running
   - Check connection string in configuration

2. **MLB API Timeouts**
   - Check internet connection
   - Verify MLB API is accessible

3. **Missing Game Data**
   - Ensure games exist in `core_betting.games` table
   - Check `mlb_stats_api_game_id` is populated

4. **Import Errors**
   - Ensure all dependencies are installed
   - Check Python path configuration

### Debug Commands

```bash
# Test database connection
uv run python -c "from src.data.database.connection import get_connection; import asyncio; asyncio.run(get_connection().__aenter__())"

# Test MLB API connection
uv run python -c "from src.services.game_outcome_service import MLBStatsAPIClient; import asyncio; asyncio.run(MLBStatsAPIClient().__aenter__())"

# Check recent games in database
uv run python -c "import asyncio; from src.data.database.connection import get_connection; asyncio.run(get_connection())"
```

## Support

For issues or questions regarding the Game Outcome Service integration:

1. Check the logs for detailed error messages
2. Run the test script to verify basic functionality
3. Use the CLI commands for manual testing
4. Review the database schema for data consistency

General Balls 