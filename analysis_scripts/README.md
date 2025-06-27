# Analysis Scripts Directory - CONSOLIDATED ARCHITECTURE

## 🎯 NEW ARCHITECTURE (Post-Consolidation)

**SINGLE ENTRY POINT:** All betting analysis now flows through `master_betting_detector.py`

### CLI Commands:
```bash
# 🎯 PRIMARY COMMAND - All betting opportunities
uv run python -m mlb_sharp_betting.cli detect-opportunities --minutes 300

# 🤖 Strategy Management  
uv run python -m mlb_sharp_betting.cli show-active-strategies
uv run python -m mlb_sharp_betting.cli auto-integrate-strategies

# 📊 Performance Analysis
uv run python -m mlb_sharp_betting.cli performance --date 2024-01-15
```

## 📁 FOLDER CONTENTS

### ✅ SQL Strategy Files (KEEP)
These files define the mathematical logic for each betting strategy:
- `*_strategy_postgres.sql` - All strategy SQL files
- `sharp_action_detector_postgres.sql` - Sharp action detection logic
- `signal_combinations_postgres.sql` - Signal combination analysis

### 🔥 ORCHESTRATOR SYSTEM (PHASE 3)
- Phase 3 Orchestrator provides **UNIFIED STRATEGY EXECUTION** for all betting analysis
  - ✅ Strategy performance feedback loop (backtesting → live detection)
  - ✅ Dynamic threshold adjustment based on ROI
  - ✅ Unified processor architecture (same logic in backtesting and live)
  - ✅ Adaptive confidence scoring with performance weighting
  - ✅ Ensemble conflict resolution for competing signals
  - ✅ Automatic poor strategy filtering

### ❌ DEPRECATED FILES (TO REMOVE)
These files are now redundant since all logic is in master detector:
- `opposing_markets_detector.py` - Logic moved to master detector
- `validated_betting_detector.py` - Logic moved to master detector
- `demo_betting_finder.py` - Replaced by CLI commands
- Other standalone Python detection scripts

## 🧹 CLEANUP PLAN

1. **Keep**: All `*.sql` files (strategy definitions)
2. **Keep**: Phase 3 Orchestrator system (unified strategy execution)
3. **Remove**: All other `*.py` files (redundant detectors)
4. **Update**: Master detector to include ALL strategy logic

## 💡 USAGE PHILOSOPHY

- **SQL files** = Strategy mathematical definitions
- **Orchestrator** = Performance-driven adaptive execution engine for all strategies
- **CLI** = User interface for all betting analysis
- **No script proliferation** = One controller rules them all

## ⚡ Quick Start

```bash
# Find all betting opportunities (replaces all other detectors)
uv run python -m mlb_sharp_betting.cli detect-opportunities

# Show what strategies are currently active
uv run python -m mlb_sharp_betting.cli show-active-strategies

# Auto-integrate new profitable strategies
uv run python -m mlb_sharp_betting.cli auto-integrate-strategies
```

## 🎯 BENEFITS OF CONSOLIDATION

1. **Single Source of Truth** - No confusion about which detector to use
2. **Unified Strategy Integration** - All strategies flow through one pipeline
3. **Consistent Confidence Scoring** - Same scoring logic for all strategies
4. **Centralized Juice Filtering** - No duplicate filtering logic
5. **Easier Maintenance** - Update logic in one place
6. **CLI Integration** - Professional command interface
7. **JSON Output Support** - Structured data export for automation

---

**🚨 MIGRATION STATUS: COMPLETE**
All detection logic evolved into Phase 3 Orchestrator system with performance feedback loops.

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

### 7. `total_line_sweet_spots_strategy.sql` ⭐ **NEW**
**Purpose**: Identifies value around key total numbers (7.5, 8.5, 9.5)
- Analyzes public bias patterns at psychologically important total lines
- Tests Over performance when public heavily on Under at key numbers
- Detects sharp money disagreement with public at sweet spots
- **Key Insight**: Public often over-reacts to key total thresholds

### 8. `underdog_ml_value_strategy.sql` ⭐ **NEW**
**Purpose**: Systematic underdog value when public loves favorites
- Identifies ML underdog opportunities when public heavily favors favorites
- Combines with spread betting disagreement for enhanced value spots
- Tests value at different odds ranges (small dogs vs big dogs)
- **Key Insight**: Public consistently overvalues favorites, creating dog value

### 9. `team_specific_bias_strategy.sql` ⭐ **NEW**
**Purpose**: Team-specific public bias analysis
- Tracks which teams consistently get overbet/underbet by public
- Identifies large market bias (Yankees, Dodgers) vs small market value
- Combines team bias with sharp money confirmation
- **Key Insight**: Popular teams often overvalued, small market teams undervalued

## 🎯 **RUNNING THE NEW PHASE 1 STRATEGIES**

### Test the New Strategies
```bash
# Test total line sweet spots
psql -h localhost -d mlb_betting -f < analysis_scripts/total_line_sweet_spots_strategy.sql

# Test underdog ML value  
psql -h localhost -d mlb_betting -f < analysis_scripts/underdog_ml_value_strategy.sql

# Test team-specific bias
psql -h localhost -d mlb_betting -f < analysis_scripts/team_specific_bias_strategy.sql
```

### Run All Phase 1 Strategies Together
```bash
# Create combined Phase 1 analysis
cat analysis_scripts/total_line_sweet_spots_strategy.sql \
    analysis_scripts/underdog_ml_value_strategy.sql \
    analysis_scripts/team_specific_bias_strategy.sql | \
    psql -h localhost -d mlb_betting -f
```

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
# Connect to your PostgreSQL database and execute any script
psql -h localhost -d mlb_betting -f < analysis_scripts/line_movement_strategy.sql
```

### Option 2: Comprehensive Analysis (Recommended)
```bash
# Run all analyses with detailed reporting
uv run python run_comprehensive_analysis.py
```

This will:
- Execute all 9 analysis scripts in sequence
- Generate detailed console output with results
- Save results to CSV files in `analysis_results/` directory
- Create comprehensive log in `analysis_results.log`

## 📋 Required Database Schema

The scripts expect these tables in your PostgreSQL database:

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

# MLB Betting Strategy Analysis Scripts

This directory contains SQL scripts for analyzing MLB betting strategies and identifying profitable opportunities.

## 📊 Strategy Categories

### Phase 1: Expert-Recommended Strategies ✅ IMPLEMENTED
These three strategies were recommended by MLB betting experts and cover missing gaps in our system:

1. **Total Line Sweet Spots** (`total_line_sweet_spots_strategy.sql`)
   - Analyzes public bias at psychologically important total lines (7.5, 8.5, 9.5)
   - Tests Over performance when public heavily bets Under at key numbers
   - Detects sharp money disagreement with public at sweet spots

2. **Underdog ML Value** (`underdog_ml_value_strategy.sql`) 
   - Identifies ML underdog opportunities when public heavily favors favorites
   - Combines with spread betting disagreement for enhanced value
   - Tests different odds ranges (small dogs vs big dogs)

3. **Team-Specific Public Bias** (`team_specific_bias_strategy.sql`)
   - Tracks teams consistently overbet/underbet by public
   - Classifies teams by market size (Yankees/Dodgers vs Rays/Pirates)
   - Combines team bias with sharp money confirmation

### Existing Strategies
- **Sharp Action Detection** (`sharp_action_detector.sql`)
- **Line Movement Analysis** (`line_movement_strategy.sql`)
- **Timing-Based Strategies** (`timing_based_strategy.sql`)
- **Signal Combinations** (`signal_combinations.sql`)
- **Public Money Fade** (`public_money_fade_strategy.sql`)
- **Book Conflicts** (`book_conflicts_strategy.sql`)
- **Opposing Markets** (`opposing_markets_strategy.sql`)

## 🔧 Running the Analysis

### Individual Strategy Testing
```bash
# Test all Phase 1 strategies
uv run python analysis_scripts/test_phase1_strategies.py

# Run specific strategies
uv run python analysis_scripts/run_phase1_strategies.py
```

### Automated Backtesting Integration ✅ COMPLETED
The Phase 1 strategies are now fully integrated into the automated backtesting system:

```bash
# Run full backtesting analysis (includes Phase 1 strategies)
uv run -m mlb_sharp_betting.cli.commands.backtesting --mode single-run

# Run backtesting scheduler (monitors Phase 1 strategies daily)
uv run -m mlb_sharp_betting.cli.commands.backtesting --mode scheduler
```

**Key Features:**
- **Actionable Window Filtering**: Only analyzes bets within 30 minutes of game time
- **Sample Size Monitoring**: Tracks when strategies reach reliable sample sizes (≥50 bets)
- **Performance Alerts**: Automatically alerts when strategies show strong performance
- **Threshold Recommendations**: Suggests optimal parameter adjustments
- **Statistical Validation**: Includes confidence intervals and significance testing

**Strategy Names in Backtesting:**
- `total_line_sweet_spots_strategy` → "Total Sweet Spots"
- `underdog_ml_value_strategy` → "Underdog ML Value" 
- `team_specific_bias_strategy` → "Team Bias"

**Variants Tracked:**
- Sweet Spots: `VALUE_OVER_SWEET_SPOT`, `VALUE_UNDER_SWEET_SPOT`, `SHARP_SWEET_SPOT_OVER`, `SHARP_SWEET_SPOT_UNDER`
- Underdog Value: `VALUE_AWAY_DOG`, `VALUE_HOME_DOG`, `SHARP_AWAY_DOG`, `SHARP_HOME_DOG`
- Team Bias: `FADE_BIG_MARKET_HOME`, `FADE_BIG_MARKET_AWAY`, `BACK_SMALL_MARKET_HOME`, `BACK_SMALL_MARKET_AWAY`

## 📈 Current Status

### Data Requirements
All strategies require:
- Recent game data with outcomes
- Betting splits from VSIN and SBD sources
- Minimum sample sizes for statistical reliability

### Sample Size Thresholds
- **Basic Analysis**: ≥8 bets
- **Reliable Analysis**: ≥50 bets (recommended for live betting)
- **Robust Analysis**: ≥100 bets (highest confidence)

### Next Steps
1. **Data Accumulation**: Let strategies collect data over 2-4 weeks
2. **Sample Size Monitoring**: Watch for strategies reaching ≥50 bet threshold
3. **Performance Validation**: Review backtesting alerts for profitable strategies
4. **Live Testing**: Start with 0.5-1% bankroll units when strategies show consistent performance

## 🎯 Expert Recommendations Covered

✅ **Sharp Money Tracking**: Covered by existing sharp action detection  
✅ **Contrarian Public Betting**: Covered by public money fade strategies  
✅ **Temporal/Timing-Based**: Covered by timing-based strategies  
✅ **Market Inefficiency**: Covered by book conflicts and opposing markets  
✅ **Total Line Sweet Spots**: NEW - Phase 1 implementation ✅  
✅ **Underdog ML Value**: NEW - Phase 1 implementation ✅  
✅ **Team-Specific Public Bias**: NEW - Phase 1 implementation ✅  

## 🚨 Important Notes

- **Actionable Window**: Backtesting only includes data collected within 30 minutes of game time
- **Sample Size Warning**: Strategies with <50 bets have limited statistical reliability
- **Threshold Adjustments**: Will be automatically recommended once adequate sample sizes are reached
- **Performance Monitoring**: Automated alerts will notify when strategies show strong/declining performance

---
*General Balls* 