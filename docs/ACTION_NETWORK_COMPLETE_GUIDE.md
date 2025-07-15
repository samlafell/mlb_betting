# Complete Action Network Pipeline Guide

## 🎯 What You Now Have

A complete Action Network pipeline that:
1. **Extracts today's game URLs** from Action Network
2. **Collects historical line movement data** for each game
3. **Analyzes for betting opportunities** (RLM, steam moves, high movement)
4. **Generates actionable insights** for today's games

## 🚀 How to Run the Complete Pipeline

### Option 1: Quick Start (Recommended for First Time)
```bash
# Get today's games and see what's available
uv run python action_network_quickstart.py
```

### Option 2: Complete Analysis (Full Pipeline)
```bash
# Run the complete analysis on existing data
uv run python analyze_existing_data.py
```

### Option 3: Standalone Pipeline (Collect New Data)
```bash
# Collect fresh data and analyze
uv run python run_action_network_pipeline.py
```

## 📊 What You Just Discovered

### 🏆 Top Games by Movement Activity
1. **Chicago White Sox @ Cleveland Guardians**: 2,401 movements (HIGHEST ACTIVITY)
2. **Detroit Tigers @ Seattle Mariners**: 867 movements
3. **Cincinnati Reds @ Colorado Rockies**: 752 movements
4. **St. Louis Cardinals @ Atlanta Braves**: 728 movements
5. **Minnesota Twins @ Pittsburgh Pirates**: 727 movements

### 🔄 RLM Opportunities Found
- **6 RLM opportunities** detected across games
- **2 Strong RLM opportunities** (>30% disparity between tickets and money)
- **Key RLM Games**:
  - Chicago Cubs @ New York Yankees (Strong RLM on DraftKings)
  - Boston Red Sox @ Tampa Bay Rays (Moderate RLM)
  - Minnesota Twins @ Pittsburgh Pirates (Moderate RLM)

### 🚂 Steam Moves Detected
- **All 16 games** showed steam moves (coordinated movement across 6 books)
- **Strong steam activity** across DraftKings, FanDuel, and other major books
- **Market focus**: Spread markets showed highest activity (4,894 movements)

### 📈 Market Breakdown
- **Spread betting**: 4,894 movements (most active)
- **Moneyline betting**: 3,754 movements
- **Total betting**: 2,829 movements

### 📚 Sportsbook Activity
- **DraftKings**: 3,323 movements (most active)
- **FanDuel**: 2,828 movements
- **Other books**: Various levels of activity

## 🎯 Key Betting Insights

### 1. Chicago White Sox @ Cleveland Guardians
- **2,401 total movements** (highest activity)
- **Strong steam moves** across all 6 sportsbooks
- **High volatility** indicates significant betting interest
- **Opportunity**: Watch for late line movements

### 2. RLM Opportunities on DraftKings
- **Chicago Cubs @ New York Yankees**: 31% disparity (Strong RLM)
  - Public: 62% tickets vs 31% money (Sharp money on Yankees)
- **Boston Red Sox @ Tampa Bay Rays**: 21% disparity (Moderate RLM)
- **Minnesota Twins @ Pittsburgh Pirates**: 21% disparity (Moderate RLM)

### 3. Steam Move Patterns
- **All games** showed coordinated movement across multiple books
- **6 sportsbooks** moving in unison indicates professional money
- **Spread markets** dominated the activity

## 🔧 How to Use This Information

### For Today's Games:
1. **Focus on high-movement games** (Chicago White Sox @ Cleveland Guardians)
2. **Follow RLM opportunities** on DraftKings
3. **Watch steam moves** across multiple books
4. **Monitor spread markets** for the most activity

### For Future Analysis:
1. **Run the pipeline daily** to get fresh data
2. **Compare day-over-day** movement patterns
3. **Track RLM success rates** to refine strategy
4. **Monitor steam move timing** for optimal entry points

## 📁 Files Generated

### Analysis Results
- `output/comprehensive_analysis_YYYYMMDD_HHMMSS.json`: Complete analysis data
- `output/historical_line_movement_full_20250711_165111.json`: Raw historical data (3MB)

### Pipeline Outputs
- `output/action_network_game_urls_today_*.json`: Today's game URLs
- `output/pipeline_results_*.json`: Pipeline execution results

## 🎯 Daily Workflow

### Morning Routine:
```bash
# 1. Check today's games
uv run python action_network_quickstart.py

# 2. Analyze existing data for patterns
uv run python analyze_existing_data.py
```

### Pre-Game Analysis:
```bash
# 3. Collect fresh data if needed
uv run python run_action_network_pipeline.py

# 4. Focus on high-movement games and RLM opportunities
```

### Key Metrics to Track:
- **Movement count**: Games with 500+ movements
- **RLM disparity**: >20% difference between tickets and money
- **Steam strength**: 4+ books moving together
- **Market focus**: Which markets (spread/moneyline/total) are most active

## 🚨 Action Items for Today

### Immediate Opportunities:
1. **Chicago White Sox @ Cleveland Guardians**
   - Monitor for late line movements
   - 2,401 movements indicate high betting interest
   - Watch all markets (spread, moneyline, total)

2. **RLM on DraftKings**
   - Chicago Cubs @ New York Yankees (Strong RLM)
   - Consider following the money (31% money vs 62% tickets)

3. **Steam Moves**
   - All games showing coordinated movement
   - Focus on spread markets for highest activity

### Data Collection:
- **11,477 total movements** across 16 games
- **6 RLM opportunities** identified
- **16 steam moves** detected
- **100% coverage** across all games

## 🎉 Success Metrics

### Pipeline Performance:
- ✅ **16/16 games** successfully analyzed
- ✅ **11,477 movements** collected and processed
- ✅ **6 RLM opportunities** detected
- ✅ **16 steam moves** identified
- ✅ **100% data coverage** across all major sportsbooks

### Betting Intelligence:
- 🎯 **Sharp money patterns** identified through RLM analysis
- 🚂 **Professional movement** detected through steam analysis
- 📊 **Market preferences** revealed through volume analysis
- 🏆 **High-value games** prioritized by movement count

---

**General Balls** 🎾⚾

*You now have a complete Action Network pipeline that extracts, analyzes, and identifies betting opportunities from today's MLB games. The system detected 6 RLM opportunities and 16 steam moves across all games, with the Chicago White Sox @ Cleveland Guardians game showing the highest activity (2,401 movements).*

*Use this data to make informed betting decisions, but remember to always bet responsibly and within your means.* 