# MLB Sharp Betting Analysis Scripts

This directory contains comprehensive analysis scripts for identifying profitable MLB betting strategies using betting splits data combined with game outcomes.

## 📊 Overview

Our analysis combines **line movement patterns** with **sharp action detection** to identify edges in MLB betting markets. We analyze when professional money (indicated by stake percentage exceeding bet percentage) aligns with line movement to find profitable betting opportunities.

## 🎯 **BETTING OPPORTUNITY FINDERS** (Production Tools)

### **PRIMARY TOOL: `validated_betting_detector.py`** ⭐
**The main production tool for finding betting opportunities**
```bash
# Find opportunities in games starting within 5 minutes
uv run analysis_scripts/validated_betting_detector.py --minutes 5

# Look further ahead (e.g., 4 hours)  
uv run analysis_scripts/validated_betting_detector.py --minutes 240
```

**Key Features:**
- ✅ **Correct timezone handling** (fixes SQL timezone bugs)
- ✅ **Validated strategies only** (proven profitable through backtesting)
- ✅ **No duplicates** (one recommendation per game/source/book)
- ✅ **VSIN signals ≥15%** differential (proven profitable)
- ✅ **SBD signals ≥25%** differential (higher threshold due to lower performance)
- ✅ **Estimated win rates** and ROI calculations

### **DEMO TOOL: `demo_betting_finder.py`** 🎮
**Educational tool showing how the system works**
```bash
# See a demonstration of how betting opportunities are detected
uv run analysis_scripts/demo_betting_finder.py
```

**Purpose:**
- Creates safe demo data to show system functionality
- Good for onboarding new users
- Demonstrates signal detection without real betting data
- Cleans up demo data automatically

## 🔄 **RECOMMENDED WORKFLOW**

```bash
# 1. Collect current betting splits data (run every 15-30 minutes)
uv run src/mlb_sharp_betting/entrypoint.py --sportsbook circa

# 2. Find validated opportunities (within 5 minutes of game time)
uv run analysis_scripts/validated_betting_detector.py --minutes 5

# 3. Act on HIGH confidence signals immediately
# 4. Verify odds at your sportsbook before betting
```

---

## 📈 **BACKTESTING & ANALYSIS SCRIPTS** (SQL-based)

*These scripts analyze historical data to validate strategies and understand market patterns*

### 1. `line_movement_strategy.sql`
**Purpose**: Analyzes line movement patterns and their predictive value
- Tracks opening vs closing lines across different bet types
- Categories line movement magnitude (big, medium, small moves)
- Tests both "follow the movement" and "fade the movement" strategies
- **Key Insight**: Big line movements (≥10 points) often indicate informed money

### 2. `sharp_action_detector.sql` 
**Purpose**: Identifies when professional money is on one side of a bet
- Detects when stake % significantly exceeds bet % (indicating larger average bets)
- Categories sharp action strength: Strong (≥15%), Moderate (≥10%), Weak (≥5%)
- Analyzes timing of sharp action (early vs late)
- **Key Insight**: Strong differentials (≥15%) typically indicate professional involvement

### 3. `hybrid_line_sharp_strategy.sql`
**Purpose**: Combines line movement with sharp action for confirmation
- Identifies when line movement and sharp action align (confirmation)
- Detects conflicting signals between line movement and money flow
- Categories: Strong Confirmation, Steam Plays, Public Moves, Reverse Line Movement
- **Key Insight**: Best results when both indicators agree on direction

### 4. `timing_based_strategy.sql`
**Purpose**: Analyzes the timing of sharp action and line movement
- Compares early persistent sharp action vs late developing action
- Identifies "steam moves" (last-minute professional action)
- Tests whether early or late sharp action is more predictive
- **Key Insight**: Timing of sharp action affects reliability

### 5. `strategy_comparison_roi.sql`
**Purpose**: Comprehensive comparison of all strategies with detailed ROI calculations
- Compares performance across different approaches
- Calculates ROI for $100 unit bets at various odds (-110, -105)
- Includes Kelly Criterion recommendations for bet sizing
- **Key Insight**: Provides head-to-head strategy comparison

### 6. `executive_summary_report.sql`
**Purpose**: High-level executive summary with actionable recommendations
- Data quality assessment and market coverage analysis
- Key insights and strategy effectiveness summary
- Specific actionable recommendations with risk management
- **Key Insight**: Executive-level decision making framework

## 🏦 ROI Calculations

All strategies calculate ROI assuming standard betting odds:
- **-110 odds**: Risk $110 to win $100 (standard juice)
- **-105 odds**: Risk $105 to win $100 (reduced juice)
- **Break-even rate**: 52.38% at -110 odds, 51.22% at -105 odds

### ROI Formula
```
ROI = (Wins × Payout) - (Losses × Risk)
ROI Percentage = ROI / Total Risk × 100
```

## 📈 Strategy Classifications

### Sharp Action Levels
- **Strong Sharp**: Stake % - Bet % ≥ 15% (highest confidence)
- **Moderate Sharp**: Stake % - Bet % ≥ 10% (good confidence)  
- **Weak Sharp**: Stake % - Bet % ≥ 5% (low confidence)

### Line Movement Categories
- **Big Movement**: ≥10 points (significant market adjustment)
- **Medium Movement**: 5-9 points (moderate adjustment)
- **Small Movement**: 1-4 points (minor adjustment)

### Hybrid Strategy Types
- **Strong Confirmation**: Big line movement + strong sharp action (same direction)
- **Steam Play**: Strong sharp action without significant line movement
- **Public Move**: Big line movement without sharp confirmation
- **Reverse Line Movement**: Line moves opposite to public money

## 🎲 How to Run Analysis

### Option 1: Run Individual Scripts
```bash
# Connect to your DuckDB database and execute any script
duckdb data/raw/mlb_betting.duckdb < analysis_scripts/line_movement_strategy.sql
```

### Option 2: Comprehensive Analysis (Recommended)
```bash
# Run all analyses with detailed reporting
uv run python run_comprehensive_analysis.py
```

This will:
- Execute all 6 analysis scripts in sequence
- Generate detailed console output with results
- Save results to CSV files in `analysis_results/` directory
- Create comprehensive log in `analysis_results.log`

## 📋 Required Database Schema

The scripts expect these tables in your DuckDB database:

### `mlb_betting.splits.raw_mlb_betting_splits`
- `game_id`: Unique game identifier
- `source`: Data source (e.g., 'vsin')
- `book`: Sportsbook name (e.g., 'circa', 'dk')
- `split_type`: Bet type ('moneyline', 'spread', 'total')
- `home_team`, `away_team`: Team names
- `game_datetime`: Game start time
- `last_updated`: When data was collected
- `split_value`: Line value (JSON for moneyline, numeric for spread/total)
- `home_or_over_stake_percentage`: % of money on home/over
- `home_or_over_bets_percentage`: % of bets on home/over

### `mlb_betting.main.game_outcomes`
- `game_id`: Links to splits table
- `home_win`: Boolean, did home team win
- `home_cover_spread`: Boolean, did home team cover spread
- `over`: Boolean, did total go over

## 🎯 Key Success Metrics

### Minimum Performance Thresholds
- **Break-even**: >52.4% win rate (beats -110 juice)
- **Profitable**: >55% win rate (solid edge)
- **Excellent**: >60% win rate (strong edge)

### Sample Size Requirements
- **Minimum**: 5 bets (very low confidence)
- **Low Confidence**: 10+ bets
- **Medium Confidence**: 25+ bets  
- **High Confidence**: 50+ bets

### ROI Targets
- **Break-even**: $0 ROI
- **Good**: >5% ROI
- **Excellent**: >10% ROI

## ⚠️ Risk Management Guidelines

1. **Bet Sizing**: Use 1-3% of bankroll per bet based on edge strength
2. **Stop Loss**: Set monthly stop-loss at -20% of bankroll
3. **Profit Taking**: Take profits at +15% monthly gains
4. **Sample Size**: Don't bet on strategies with <10 historical examples
5. **Market Limits**: Avoid betting on stale or low-limit markets

## 🔄 Continuous Improvement

The analysis framework is designed for ongoing optimization:
- Add new data regularly to improve sample sizes
- Monitor strategy performance over time
- Adjust thresholds based on changing market conditions
- Incorporate new data sources (books, split types)

## 📞 Support

For questions about the analysis or implementing strategies:
1. Check the detailed logs in `analysis_results.log`
2. Review individual CSV outputs for specific strategy details
3. Ensure your database schema matches the requirements above

---

**General Balls** 🏈⚾️
*Professional Sports Betting Analytics* 