# Action Network Games Data Improvement

## Problem Solved
The `raw_data.action_network_games` table previously stored only raw JSON data, making it impossible to quickly identify games without parsing complex JSONB structures.

## Solution Implemented
Added readable columns and a user-friendly view to extract game information automatically.

### New Columns Added
- `home_team` - Full team name (e.g., "Texas Rangers")
- `away_team` - Full team name (e.g., "Athletics") 
- `home_team_abbr` - Team abbreviation (e.g., "TEX")
- `away_team_abbr` - Team abbreviation (e.g., "ATH")
- `game_status` - Game status (scheduled, complete, in_progress, etc.)
- `start_time` - Scheduled game start time in UTC

### New View Created
`raw_data.v_action_network_games_readable` - Provides formatted game descriptions

## Before vs After

### Before (Problematic)
```sql
SELECT * FROM raw_data.action_network_games WHERE external_game_id = '258097';
```
**Result**: Only showed `id`, `external_game_id`, complex `raw_response` JSONB, etc.
**Problem**: User had to manually parse JSON to identify the game

### After (Improved)
```sql
SELECT external_game_id, away_team, home_team, home_team_abbr, away_team_abbr, game_status, start_time 
FROM raw_data.action_network_games 
WHERE external_game_id = '258097';
```
**Result**: 
- Game 258097: Athletics @ Texas Rangers (ATH @ TEX)
- Status: scheduled  
- Start: 2025-07-24 00:05:00+00:00

### Best Practice (Recommended)
```sql
SELECT * FROM raw_data.v_action_network_games_readable 
WHERE external_game_id = '258097';
```
**Result**: `Athletics @ Texas Rangers - scheduled at 2025-07-24 00:05:00+00:00`

## Implementation Details

### Database Migration
- **File**: `sql/migrations/005_add_readable_game_info.sql`
- **Changes**: Added columns, indexes, view, and backfilled existing data

### Code Changes  
- **File**: `src/data/collection/consolidated_action_network_collector.py`
- **Enhancement**: Modified `_store_raw_game_data()` to extract readable fields from API response

### Data Extraction Logic
- Away team = `teams[0]` (Action Network convention)
- Home team = `teams[1]` (Action Network convention) 
- Extracts both full names and abbreviations
- Preserves original raw JSON data for full compatibility

## Benefits
1. **Immediate Game Identification**: No JSON parsing required
2. **Better Query Performance**: Indexed columns for fast lookups
3. **User-Friendly Views**: Formatted game descriptions
4. **Backward Compatibility**: All existing functionality preserved
5. **Database Efficiency**: Proper indexing for common lookup patterns

## Usage Examples

```sql
-- Find today's games
SELECT game_description, game_status, start_time 
FROM raw_data.v_action_network_games_readable 
WHERE game_date = CURRENT_DATE;

-- Find games by team
SELECT * FROM raw_data.v_action_network_games_readable 
WHERE home_team LIKE '%Rangers%' OR away_team LIKE '%Rangers%';

-- Find games by abbreviation  
SELECT * FROM raw_data.v_action_network_games_readable 
WHERE home_team_abbr = 'TEX' OR away_team_abbr = 'TEX';
```

## Migration Status
✅ Database schema updated  
✅ Code updated to extract readable data  
✅ Existing data backfilled  
✅ New view created  
✅ Tested and verified working  

The improvement is now live and all future Action Network game collections will automatically include readable team information.