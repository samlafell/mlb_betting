# The Odds API Integration Guide

## Overview

This guide explains how to use The Odds API to retrieve MLB betting odds while intelligently managing your monthly usage quota.

## Key Features

- ✅ **Smart Budget Management**: Automatically tracks usage against 480 credit monthly limit
- ✅ **Flexible Market Selection**: Choose between essential, standard, or comprehensive market configs
- ✅ **Usage Optimization**: Recommends optimal configurations based on remaining budget
- ✅ **Automatic Tracking**: Monitors API calls and costs with detailed logging
- ✅ **Monthly Reset**: Usage automatically resets each calendar month

## API Cost Structure

Based on [The Odds API pricing](https://the-odds-api.com/liveapi/guides/v4/#usage-quota-costs-2):

**Formula**: `10 × [markets] × [regions]`

### Market Configurations

| Configuration | Markets | Cost/Call | Max Games/Month* |
|---------------|---------|-----------|------------------|
| `essential` | Moneyline only | 10 credits | 48 games |
| `standard` | ML + Spreads + Totals | 30 credits | 16 games |
| `comprehensive` | ML + Spreads + Totals + Lay | 40 credits | 12 games |

*Based on 480 credit monthly limit

## Setup

1. **Get API Key**: Sign up at [The Odds API](https://the-odds-api.com)
2. **Add to Environment**: Add `ODDS_API_KEY=your_key_here` to your `.env` file
3. **Install Dependencies**: `uv add requests` (if not already installed)

## Usage Examples

### Basic Usage

```python
from mlb_sharp_betting.services.odds_api_service import OddsAPIService, OddsData

# Initialize service
odds_service = OddsAPIService()

# Check current usage
status = odds_service.get_usage_status()
print(f"Used: {status['used']}/{status['remaining']} credits")

# Get today's games (free)
games = odds_service.get_today_games()

# Get odds with budget optimization
recommended_config = odds_service.optimize_for_budget(games_needed=5)
odds_data = odds_service.get_mlb_odds(market_config=recommended_config)
```

### Smart Budget Management

```python
# Check if you can afford a call before making it
def safe_odds_fetch(odds_service, market_config="standard"):
    """Safely fetch odds with budget checking."""
    
    # Get current usage
    status = odds_service.get_usage_status()
    
    # Calculate estimated cost
    cost_map = {"essential": 10, "standard": 30, "comprehensive": 40}
    estimated_cost = cost_map[market_config]
    
    if status['remaining'] < estimated_cost:
        print(f"⚠️ Insufficient budget. Need {estimated_cost}, have {status['remaining']}")
        return None
    
    # Make the call
    return odds_service.get_mlb_odds(market_config=market_config)
```

### Processing Odds Data

```python
def process_game_odds(odds_data):
    """Extract and process odds from API response."""
    
    for game_data in odds_data:
        odds_obj = OddsData.from_odds_api(game_data)
        
        print(f"Game: {odds_obj.away_team} @ {odds_obj.home_team}")
        
        # Get moneyline odds
        ml_odds = odds_obj.get_moneyline_odds()
        if ml_odds:
            print("Moneyline:")
            for outcome in ml_odds['outcomes']:
                print(f"  {outcome['name']}: {outcome['price']}")
        
        # Get spread odds
        spread_odds = odds_obj.get_spread_odds()
        if spread_odds:
            print("Spread:")
            for outcome in spread_odds['outcomes']:
                point = outcome.get('point', 'N/A')
                print(f"  {outcome['name']} {point}: {outcome['price']}")
        
        # Get totals
        total_odds = odds_obj.get_total_odds()
        if total_odds:
            print("Totals:")
            for outcome in total_odds['outcomes']:
                point = outcome.get('point', 'N/A')
                print(f"  {outcome['name']} {point}: {outcome['price']}")
```

## Integration with Existing Workflow

### Replace VSIN with Odds API for Specific Games

```python
def get_comprehensive_betting_data(game_info):
    """Get betting data from multiple sources."""
    
    # Use VSIN for splits data (existing)
    vsin_data = get_vsin_splits(game_info)
    
    # Use Odds API for precise odds
    odds_service = OddsAPIService()
    
    # Check budget and get appropriate market config
    config = odds_service.optimize_for_budget(1)  # Just this game
    
    if odds_service.usage_tracker.can_make_call(30):  # Assuming standard
        odds_data = odds_service.get_mlb_odds(market_config=config)
        # Match game and extract odds
        matching_game = find_matching_game(odds_data, game_info)
    else:
        print("Using VSIN odds due to budget constraints")
        # Fall back to VSIN odds parsing
        matching_game = None
    
    return {
        "splits": vsin_data,
        "odds": matching_game,
        "source": "odds_api" if matching_game else "vsin"
    }
```

### Daily Odds Collection

```python
def daily_odds_collection():
    """Collect odds for today's games with budget management."""
    
    odds_service = OddsAPIService()
    
    # Get today's games (free)
    games = odds_service.get_today_games()
    
    if not games:
        print("No games today")
        return
    
    print(f"Found {len(games)} games today")
    
    # Determine optimal strategy based on budget
    status = odds_service.get_usage_status()
    
    if status['remaining'] >= len(games) * 30:
        # Can afford standard config for all games
        config = "standard"
        print(f"Using standard config for all {len(games)} games")
    elif status['remaining'] >= len(games) * 10:
        # Can afford essential config for all games
        config = "essential"
        print(f"Using essential config for all {len(games)} games")
    else:
        # Limited budget - prioritize games
        max_games = status['remaining'] // 10
        print(f"Budget limited to {max_games} games with essential config")
        games = games[:max_games]  # Take first N games
        config = "essential"
    
    # Fetch odds
    odds_data = odds_service.get_mlb_odds(market_config=config)
    
    # Process and store
    if odds_data:
        store_odds_data(odds_data)
        print(f"Successfully collected odds for {len(odds_data)} games")
```

## Usage Monitoring

### View Usage Status

```python
def print_usage_report(odds_service):
    """Print detailed usage report."""
    
    status = odds_service.get_usage_status()
    
    print(f"📊 The Odds API Usage Report")
    print(f"   Month: {status['month']}")
    print(f"   Used: {status['used']} credits")
    print(f"   Remaining: {status['remaining']} credits")
    print(f"   Usage: {status['percentage_used']:.1f}%")
    
    # Show what's possible with remaining budget
    remaining = status['remaining']
    print(f"\n💰 Remaining Budget Options:")
    print(f"   Essential (ML only): {remaining // 10} games")
    print(f"   Standard (ML+Spread+Total): {remaining // 30} games")
    print(f"   Comprehensive: {remaining // 40} games")
```

### Usage History

The service automatically tracks all API calls in `data/odds_api_usage.json`:

```json
{
  "month": "2025-01",
  "used": 60,
  "calls": [
    {
      "timestamp": "2025-01-15T10:30:00",
      "cost": 30,
      "endpoint": "sports/baseball_mlb/odds",
      "markets": ["h2h", "spreads", "totals"]
    }
  ]
}
```

## Best Practices

### 1. **Start with Free Calls**
```python
# Always check today's games first (free)
games = odds_service.get_today_games()
```

### 2. **Use Budget Optimization**
```python
# Let the service recommend optimal config
config = odds_service.optimize_for_budget(games_needed)
```

### 3. **Check Before Expensive Calls**
```python
# Always verify budget before comprehensive calls
if not odds_service.usage_tracker.can_make_call(40):
    print("Switching to essential config due to budget")
    config = "essential"
```

### 4. **Monitor Usage Regularly**
```python
# Check usage weekly
status = odds_service.get_usage_status()
if status['percentage_used'] > 75:
    print("⚠️ Over 75% of monthly budget used")
```

### 5. **Fallback Strategy**
```python
# Have a fallback when budget is exhausted
if status['remaining'] < 10:
    print("Using VSIN as fallback for odds data")
    return get_vsin_odds_data()
```

## Error Handling

```python
def robust_odds_fetch(odds_service, market_config="standard"):
    """Robust odds fetching with error handling."""
    
    try:
        # Check budget first
        if not odds_service.usage_tracker.can_make_call(30):
            raise ValueError("Insufficient budget for API call")
        
        # Make API call
        odds_data = odds_service.get_mlb_odds(market_config=market_config)
        
        if not odds_data:
            raise ValueError("No odds data returned")
        
        return odds_data
        
    except ValueError as e:
        logger.warning(f"Budget constraint: {e}")
        return None
    except Exception as e:
        logger.error(f"Odds API error: {e}")
        return None
```

## Testing

Run the test script to verify integration:

```bash
uv run test_odds_api_integration.py
```

This will:
- ✅ Test API key setup
- ✅ Show current usage status  
- ✅ Demonstrate budget optimization
- ✅ Fetch today's games (free)
- ✅ Optionally make a test odds call

## Integration Points

### With Existing VSIN Scraper
- Use Odds API for precise, current odds
- Use VSIN for betting splits and handle percentages
- Combine both for comprehensive betting intelligence

### With Sharp Action Detection
- Use Odds API odds as baseline for line movement detection
- Compare with VSIN historical data for movement patterns
- Trigger alerts when significant line movements occur

### With Database Storage
- Store Odds API data in same schema as existing odds data
- Add source tracking to distinguish between VSIN and Odds API
- Use for backtesting and performance analysis

---

**General Balls** 