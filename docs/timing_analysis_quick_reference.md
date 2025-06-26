# Timing Analysis Quick Reference

## Most Common Commands

### Daily Workflow
```bash
# 1. Update outcomes from yesterday's games
uv run python -m mlb_sharp_betting.cli timing update-outcomes --days-back 1

# 2. Get current performance summary
uv run python -m mlb_sharp_betting.cli timing summary

# 3. Check timing recommendation before placing bet
uv run python -m mlb_sharp_betting.cli timing recommend \
  --source "fanduel" \
  --strategy "value_betting" \
  --bet-type "moneyline"
```

### Weekly Analysis
```bash
# Generate weekly performance report
uv run python -m mlb_sharp_betting.cli timing analyze \
  --days-back 7 \
  --output csv > weekly_timing_analysis.csv

# Check specific strategy performance
uv run python -m mlb_sharp_betting.cli timing analyze \
  --strategy "sharp_money" \
  --days-back 7
```

### Track New Recommendation
```bash
# Track a bet you're about to place
uv run python -m mlb_sharp_betting.cli timing track \
  --source "draftkings" \
  --strategy "value_betting" \
  --bet-type "moneyline" \
  --game-start "2024-01-20 20:00:00" \
  --odds -120 \
  --stake 100
```

## Time Buckets Explained

- **0-2 hours**: Last-minute bets (high risk, potential for sharp moves)
- **2-6 hours**: Pre-game bets (balanced timing)
- **6-24 hours**: Day-of bets (good information, stable lines)
- **24+ hours**: Early bets (best value, less information)

## Performance Grades

- **EXCELLENT**: ROI > 15%
- **GOOD**: ROI 10-15%
- **PROFITABLE**: ROI 5-10%
- **BREAKEVEN**: ROI 0-5%
- **UNPROFITABLE**: ROI < 0%

## Confidence Levels

- **LOW** (1): < 20 bets
- **MODERATE** (2): 20-49 bets
- **HIGH** (3): 50-99 bets
- **VERY_HIGH** (4): 100+ bets

## Output Formats

- `console`: Pretty formatted table (default)
- `json`: JSON for programmatic use
- `csv`: CSV for Excel/analysis

## Common Filters

```bash
# By sportsbook
--source "fanduel"

# By strategy
--strategy "value_betting"

# By bet type
--bet-type "moneyline"  # or "spread", "over_under"

# Minimum confidence
--min-confidence 3  # Only high-confidence results
```

## Integration Examples

### Python Service
```python
from mlb_sharp_betting.services.timing_analysis_service import TimingAnalysisService

service = TimingAnalysisService()
recommendation = await service.get_realtime_timing_recommendation(
    source="fanduel",
    strategy="value_betting", 
    bet_type="moneyline"
)
```

### Automated Tracking
```python
from mlb_sharp_betting.analyzers.timing_recommendation_tracker import TimingRecommendationTracker

tracker = TimingRecommendationTracker()
await tracker.track_recommendation(
    source="fanduel",
    strategy="value_betting",
    bet_type="moneyline",
    game_start_time=game_time,
    odds=-120,
    stake=100
)
```

**General Balls** 