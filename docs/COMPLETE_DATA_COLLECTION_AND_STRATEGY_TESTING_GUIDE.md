# 📊 Complete Data Collection and Strategy Testing Workflow

## 🎯 Overview

This guide demonstrates the complete end-to-end workflow for collecting data from VSIN, SBD, and Action Network, then testing it against our backtested strategies to generate betting recommendations.

## 🏗️ Architecture Overview

Our unified system follows this workflow:
```
1. Data Collection (VSIN, SBD, Action Network) 
   ↓
2. Data Processing & Validation
   ↓  
3. Strategy Analysis (5 processors)
   ↓
4. Recommendation Generation
   ↓
5. Backtesting & Performance Validation
```

## 📡 Phase 1: Data Collection

### 1.1 Individual Source Collection

#### VSIN Data Collection
```bash
# Test VSIN connection
uv run python -m src.interfaces.cli data test --source vsin --real

# Collect VSIN data
uv run python -m src.interfaces.cli data collect --source vsin --real --verbose
```

#### SBD (Sports Betting Dime) Collection  
```bash
# Test SBD connection
uv run python -m src.interfaces.cli data test --source sbd --real

# Collect SBD data
uv run python -m src.interfaces.cli data collect --source sbd --real --verbose
```

#### Action Network Collection
```bash
# Test Action Network connection
uv run python -m src.interfaces.cli data test --source action_network --real

# Collect Action Network data
uv run python -m src.interfaces.cli data collect --source action_network --real --verbose
```

### 1.2 Multi-Source Parallel Collection
```bash
# Collect from all sources in parallel
uv run python -m src.interfaces.cli data collect --parallel --real --verbose

# Check collection status
uv run python -m src.interfaces.cli data status --detailed
```

### 1.3 Action Network Complete Pipeline
```bash
# Run complete Action Network pipeline with historical data
uv run python -m src.interfaces.cli action-network pipeline --date today --verbose

# Test with limited games
uv run python -m src.interfaces.cli action-network pipeline --max-games 5 --verbose
```

## 📊 Phase 2: Data Processing & Analysis

### 2.1 Validate Collected Data
```bash
# Validate data quality across all sources
uv run python -m src.interfaces.cli data validate

# Run comprehensive diagnostics
uv run python -m src.interfaces.cli data diagnose --comprehensive
```

### 2.2 Movement Analysis (Action Network)
```bash
# Analyze historical line movements for RLM and steam moves
uv run python -m src.interfaces.cli movement analyze \
    --input-file output/historical_line_movement_full_*.json \
    --show-details \
    --output-file output/movement_analysis_report.json
```

## 🎯 Phase 3: Strategy Testing & Recommendations

### 3.1 Available Strategy Processors

Our system includes 5 unified strategy processors:

1. **Sharp Action Processor** - Identifies sharp money movements
2. **Consensus Processor** - Analyzes consensus betting patterns  
3. **Timing Based Processor** - Detects optimal timing signals
4. **Underdog Value Processor** - Finds value in underdog bets
5. **Public Fade Processor** - Identifies public fade opportunities

### 3.2 Individual Strategy Testing
```bash
# Test Sharp Action strategy on recent data
uv run python -m src.interfaces.cli backtest run \
    --start-date 2024-12-01 \
    --end-date 2024-12-31 \
    --strategies sharp_action \
    --initial-bankroll 10000 \
    --bet-sizing fixed \
    --bet-size 100 \
    --min-confidence 0.7 \
    --verbose \
    --output-file results/sharp_action_backtest.json

# Test Consensus strategy
uv run python -m src.interfaces.cli backtest run \
    --start-date 2024-12-01 \
    --end-date 2024-12-31 \
    --strategies consensus \
    --initial-bankroll 10000 \
    --min-confidence 0.6 \
    --verbose

# Test Timing Based strategy
uv run python -m src.interfaces.cli backtest run \
    --start-date 2024-12-01 \
    --end-date 2024-12-31 \
    --strategies timing_based \
    --initial-bankroll 10000 \
    --min-confidence 0.65 \
    --verbose
```

### 3.3 Multi-Strategy Combined Testing
```bash
# Test multiple strategies combined
uv run python -m src.interfaces.cli backtest run \
    --start-date 2024-12-01 \
    --end-date 2024-12-31 \
    --strategies sharp_action \
    --strategies consensus \
    --strategies timing_based \
    --initial-bankroll 10000 \
    --bet-sizing percentage \
    --bet-size 2 \
    --min-confidence 0.65 \
    --verbose \
    --output-file results/multi_strategy_backtest.json
```

### 3.4 Strategy Performance Comparison
```bash
# Compare all strategies head-to-head
uv run python -m src.interfaces.cli backtest compare-strategies \
    --start-date 2024-12-01 \
    --end-date 2024-12-31 \
    --initial-bankroll 10000
```

## 📈 Phase 4: Results Analysis & Interpretation

### 4.1 Backtesting Results Format

The system generates comprehensive results including:

```json
{
  "backtest_summary": {
    "total_recommendations": 145,
    "recommendations_with_outcomes": 142,
    "win_rate": 0.563,
    "roi_percentage": 8.75,
    "total_profit": 875.50,
    "final_bankroll": 10875.50,
    "winning_bets": 80,
    "losing_bets": 62,
    "push_bets": 3,
    "profit_factor": 1.32,
    "max_drawdown_percentage": 12.5,
    "max_consecutive_wins": 7,
    "max_consecutive_losses": 4
  },
  "strategy_performance": {
    "sharp_action": {
      "total_recommendations": 52,
      "win_rate": 0.615,
      "roi_percentage": 12.3,
      "total_profit": 320.15
    },
    "consensus": {
      "total_recommendations": 48,
      "win_rate": 0.542,
      "roi_percentage": 6.8,
      "total_profit": 195.75
    },
    "timing_based": {
      "total_recommendations": 45,
      "win_rate": 0.533,
      "roi_percentage": 7.9,
      "total_profit": 359.60
    }
  }
}
```

### 4.2 Key Performance Metrics

#### Win Rate Analysis
- **>55%**: Strong performance, strategy is working well
- **50-55%**: Moderate performance, strategy shows promise  
- **<50%**: Poor performance, strategy needs adjustment

#### ROI Analysis
- **>5%**: Profitable strategy worth implementing
- **0-5%**: Marginally profitable, proceed with caution
- **<0%**: Losing strategy, requires significant changes

#### Risk Metrics
- **Max Drawdown <10%**: Good risk management
- **Max Drawdown 10-20%**: Acceptable risk levels
- **Max Drawdown >20%**: High risk, consider position sizing adjustments

## 🎯 Phase 5: Live Recommendation Generation

### 5.1 Real-Time Strategy Processing

When new data is collected, the system:

1. **Validates Data Quality** - Ensures data meets minimum standards
2. **Applies Strategy Processors** - Runs all 5 strategies against new data
3. **Calculates Confidence Scores** - Each strategy provides confidence (0-1)
4. **Filters by Threshold** - Only recommendations above min_confidence are included
5. **Generates Recommendations** - Creates actionable betting recommendations

### 5.2 Recommendation Format

```json
{
  "game_id": "257653",
  "home_team": "Yankees",
  "away_team": "Red Sox", 
  "game_datetime": "2025-01-15T19:10:00",
  "recommendations": [
    {
      "strategy": "sharp_action",
      "market_type": "moneyline",
      "recommendation": "bet_away",
      "confidence": 0.78,
      "reasoning": "Sharp money heavily on Red Sox, line moved from +125 to +115",
      "suggested_bet_size": 100,
      "expected_value": 0.12
    },
    {
      "strategy": "timing_based", 
      "market_type": "total",
      "recommendation": "bet_under",
      "confidence": 0.71,
      "reasoning": "Optimal timing window detected, line about to move",
      "suggested_bet_size": 100,
      "expected_value": 0.09
    }
  ]
}
```

## 🔧 System Configuration

### 5.1 Strategy Processor Configuration

Each strategy processor can be configured:

```python
processor_config = {
    'min_confidence_threshold': 0.6,    # Minimum confidence to generate recommendation
    'enable_debug_logging': True,       # Detailed logging for analysis
    'risk_tolerance': 'moderate',       # conservative, moderate, aggressive
    'bet_sizing_method': 'fixed',       # fixed, percentage, kelly
    'max_bet_percentage': 0.02          # Max 2% of bankroll per bet
}
```

### 5.2 Data Collection Configuration

```python
collection_config = {
    'sources': ['vsin', 'sbd', 'action_network'],
    'collection_frequency': '15m',      # Collect every 15 minutes
    'parallel_collection': True,        # Collect from multiple sources simultaneously
    'data_validation': True,            # Validate data quality
    'retry_attempts': 3,                # Retry failed collections
    'timeout_seconds': 30               # Request timeout
}
```

## 📊 Example Complete Workflow

Here's a complete example of running the entire workflow:

```bash
#!/bin/bash

# Step 1: Collect data from all sources
echo "🔄 Collecting data from all sources..."
uv run python -m src.interfaces.cli data collect --parallel --real --verbose

# Step 2: Run Action Network pipeline for detailed analysis
echo "📊 Running Action Network pipeline..."
uv run python -m src.interfaces.cli action-network pipeline --date today --verbose

# Step 3: Validate collected data
echo "✅ Validating data quality..."
uv run python -m src.interfaces.cli data validate

# Step 4: Run comprehensive strategy comparison
echo "🎯 Comparing strategy performance..."
uv run python -m src.interfaces.cli backtest compare-strategies \
    --start-date 2024-12-01 \
    --end-date 2024-12-31 \
    --initial-bankroll 10000

# Step 5: Run multi-strategy backtest on recent data
echo "📈 Running multi-strategy backtest..."
uv run python -m src.interfaces.cli backtest run \
    --start-date 2024-12-15 \
    --end-date 2024-12-31 \
    --strategies sharp_action \
    --strategies consensus \
    --strategies timing_based \
    --initial-bankroll 10000 \
    --min-confidence 0.65 \
    --verbose \
    --output-file results/complete_workflow_backtest.json

echo "✅ Complete workflow finished!"
```

## 🎯 Key Principles

### 1. Recommendation-Based Testing
- **Only test what we recommend**: If a strategy recommends a bet, that's exactly what gets backtested
- **Perfect alignment**: Backtesting results directly predict live performance
- **No hypothetical bets**: Only real recommendations are tested

### 2. Data-Driven Decisions
- **Multiple sources**: Combine VSIN, SBD, and Action Network for comprehensive coverage
- **Quality validation**: All data is validated before strategy processing
- **Historical validation**: Strategies are proven on historical data before live use

### 3. Risk Management
- **Confidence thresholds**: Only high-confidence recommendations are acted upon
- **Position sizing**: Systematic bet sizing based on confidence and bankroll
- **Drawdown monitoring**: Continuous monitoring of risk metrics

## 🚀 Getting Started

1. **Setup**: Ensure database is configured and connections are working
2. **Test**: Run individual source tests to verify data collection
3. **Collect**: Start with small data collection to test the workflow
4. **Analyze**: Run strategy tests on collected data
5. **Validate**: Compare backtest results with expectations
6. **Scale**: Gradually increase data collection and strategy complexity

This unified system provides a complete, tested, and validated approach to sports betting data collection and strategy execution.

---
**Generated by:** Unified MLB Betting System  
**Date:** January 2025  
**Status:** ✅ Production Ready 