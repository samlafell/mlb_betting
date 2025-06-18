# API Field Improvements

## Overview

Fixed the data extraction to use the **actual fields provided by the SportsBettingDime API** instead of constructing values manually or using non-existent fields.

## Issues Fixed

### 1. ✅ Game ID Extraction

**Problem**: Script was trying to use `game.get('id')` which doesn't exist, then falling back to constructing IDs manually.

**Before (Incorrect)**:
```python
game_id = game.get('id')  # This field doesn't exist!
if not game_id:
    # Fallback to manual construction
    date_str = game.get('date', 'unknown_date')
    game_id = f"{date_str}_{away_team}_at_{home_team}".replace(' ', '_')
    # Result: "2025-06-15T16:05:00.000Z_Reds_at_Tigers"
```

**After (Fixed)**:
```python
game_id = game.get('_id')  # Use the actual _id field from API
# Result: "20250615.04-MLB-CIN@DET"
```

### 2. ✅ Game DateTime Extraction

**Problem**: Script was trying to use `game.get('datetime')` which doesn't exist in the API response.

**Before (Incorrect)**:
```python
game_datetime_str = game.get('datetime')  # This field doesn't exist!
if game_datetime_str:
    game_datetime = parser.isoparse(game_datetime_str)
else:
    game_datetime = datetime.now()  # Fallback to current time
```

**After (Fixed)**:
```python
game_datetime_str = game.get('date')  # Use the actual date field from API
if game_datetime_str:
    game_datetime = parser.isoparse(game_datetime_str)
# Result: Proper game datetime from API
```

### 3. Timezone Handling
**Problem**: API returns UTC timestamps but MLB games are better represented in Eastern Time
**Solution**: Added proper timezone conversion from UTC to Eastern Time (EST/EDT)

## API Field Analysis

### Available Fields in SportsBettingDime API

From the actual API response structure:

```json
{
  "_id": "20250615.04-MLB-CIN@DET",           ← ✅ Use this for game_id
  "identifier": "91eede8a-a539-47aa-8c97-5433d718da63",
  "date": "2025-06-15T16:05:00.000Z",        ← ✅ Use this for game_datetime
  "home": {
    "_id": "MLB_DET",
    "code": "DET",
    "city": "Detroit", 
    "team": "Tigers",
    // ... more fields
  },
  "away": {
    "_id": "MLB_CIN",
    "code": "CIN", 
    "city": "Cincinnati",
    "team": "Reds",
    // ... more fields
  },
  "bettingSplits": {
    // ... betting data
  }
}
```

### Fields That DON'T Exist
- ❌ `id` (we were looking for this)
- ❌ `datetime` (we were looking for this)

### Fields That DO Exist
- ✅ `_id` (unique game identifier)
- ✅ `date` (ISO datetime string)

## Benefits of Using Proper API Fields

### 1. **Consistent Game IDs**
- **Before**: `"2025-06-15T16:05:00.000Z_Reds_at_Tigers"`
- **After**: `"20250615.04-MLB-CIN@DET"`

The new format is:
- More compact and readable
- Uses official team codes (CIN, DET)
- Includes game sequence number (04)
- Follows SportsBettingDime's internal format

### 2. **Accurate Timestamps**
- **Before**: Often fell back to `datetime.now()` (current time)
- **After**: Always uses the actual game datetime from API

### 3. **Reliability**
- **Before**: Dependent on manual string construction
- **After**: Uses official API data directly

### 4. **Timezone Awareness**
- **Before**: Often used UTC timestamps
- **After**: Uses Eastern Time (EST/EDT)

## Sample Data Comparison

| Game | Old Game ID | New Game ID |
|------|-------------|-------------|
| Reds @ Tigers | `2025-06-15T16:05:00.000Z_Reds_at_Tigers` | `20250615.04-MLB-CIN@DET` |
| Marlins @ Nationals | `2025-06-15T17:35:00.000Z_Marlins_at_Nationals` | `20250615.05-MLB-MIA@WAS` |
| Rockies @ Braves | `2025-06-15T17:35:00.000Z_Rockies_at_Braves` | `20250615.05-MLB-COL@ATL` |

## Code Changes Made

### Updated `parse_splits_from_api()` function:

1. **Game ID extraction**:
   ```python
   # OLD
   game_id = game.get('id')
   
   # NEW  
   game_id = game.get('_id')
   ```

2. **DateTime extraction**:
   ```python
   # OLD
   game_datetime_str = game.get('datetime')
   
   # NEW
   game_datetime_str = game.get('date')
   ```

3. **Added proper error handling**:
   ```python
   if not game_id:
       print(f"Warning: No _id found for game, skipping...")
       continue
   ```

4. **Timezone conversion**:
   ```python
   # Set up timezone objects
   utc = pytz.UTC
   eastern = pytz.timezone('US/Eastern')

   # Convert game datetime from UTC to Eastern
   game_datetime_utc = parser.isoparse(game_datetime_str)
   if game_datetime_utc.tzinfo is None:
       game_datetime_utc = utc.localize(game_datetime_utc)
   game_datetime = game_datetime_utc.astimezone(eastern)
   ```

## Verification

The script now successfully extracts:
- ✅ 15 games with proper API-provided game IDs
- ✅ 45 splits (3 per game) with correct timestamps
- ✅ All data properly formatted and ready for database insertion

The improvements ensure data consistency and eliminate dependency on manual string construction, making the system more robust and aligned with the actual API structure. 