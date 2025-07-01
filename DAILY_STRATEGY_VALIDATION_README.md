# Daily Strategy Validation System

## Overview

The Daily Strategy Validation System is a comprehensive automated solution that discovers, validates, and applies all available betting strategies to identify optimal daily betting opportunities. This system replaces manual strategy evaluation with a systematic, data-driven approach.

## 🚀 Core Objective

Implement a comprehensive daily strategy validation system that automatically backtests all betting strategies against complete historical data, updates performance metrics, and identifies high-ROI betting opportunities for the current day.

## 📋 System Architecture

### Phase 1: Strategy Collection & Backtesting
- **Automatic Discovery**: Dynamically discovers all available strategy processors using the StrategyProcessorFactory
- **Comprehensive Backtesting**: Runs complete historical backtests for each implemented strategy
- **Performance Metrics**: Calculates ROI, win rate, Sharpe ratio, Kelly criterion, and confidence levels
- **Quality Assessment**: Grades strategies as EXCELLENT, GOOD, PROFITABLE, or UNPROFITABLE

### Phase 2: Performance Analysis & Ranking
- **ROI-Based Ranking**: Sorts strategies by historical ROI performance (primary ranking factor)
- **Qualification Filtering**: Applies minimum thresholds for ROI, bet count, and win rate
- **Top Performer Identification**: Identifies top 20% or minimum 3 best performing strategies
- **Trend Analysis**: Optional analysis of performance trends over specified lookback period

### Phase 3: Current Day Opportunity Detection
- **Validated Strategy Application**: Applies only proven high-ROI strategies to today's games
- **Opportunity Ranking**: Ranks betting opportunities by historical strategy performance
- **Risk Assessment**: Categorizes opportunities as LOW_RISK, MODERATE_RISK, or HIGH_RISK
- **Confidence Scoring**: Provides confidence scores based on historical performance

### Phase 4: Output & Reporting
- **Database Updates**: Updates `backtesting.strategy_performance` table with fresh results
- **Comprehensive Reporting**: Generates detailed reports with multiple output formats
- **Bankroll Management**: Provides Kelly criterion-based position sizing recommendations
- **Export Capabilities**: Supports JSON, CSV, and console output formats

## 🎯 Primary Command: `auto integrate strategies`

### Enhanced Command Interface

```bash
# Full comprehensive validation (recommended for daily use)
mlb-cli auto-integrate-strategies

# With custom thresholds
mlb-cli auto-integrate-strategies --min-roi 8.0 --min-bets 15

# With trend analysis
mlb-cli auto-integrate-strategies --lookback-days 30

# Export results to files
mlb-cli auto-integrate-strategies --format json --output daily_report.json --export-opportunities opportunities.csv

# Skip certain phases (for testing or partial runs)
mlb-cli auto-integrate-strategies --skip-opportunities  # Skip current day detection
mlb-cli auto-integrate-strategies --skip-database-update  # Skip DB updates
```

### Command Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `--lookback-days` | int | None | Days to analyze for trend analysis (optional) |
| `--min-roi` | float | 5.0 | Minimum ROI threshold for strategy inclusion |
| `--min-bets` | int | 10 | Minimum bet count for strategy inclusion |
| `--skip-opportunities` | flag | False | Skip current day opportunity detection |
| `--skip-database-update` | flag | False | Skip updating strategy_performance table |
| `--format` | choice | console | Output format: console, json, csv |
| `--output` | path | None | Output file path for JSON/CSV formats |
| `--export-opportunities` | path | None | Export opportunities to CSV file |

## 📊 Key Features

### 1. Automatic Strategy Discovery
- Discovers all implemented strategies from the processor factory
- No manual configuration required - new strategies are automatically included
- Handles strategy implementation status (IMPLEMENTED, IN_PROGRESS, PLANNED)

### 2. Comprehensive Performance Metrics
- **ROI Percentage**: Primary ranking metric using realistic -110 odds
- **Win Rate**: Percentage of winning bets
- **Sample Size**: Total number of historical bets analyzed
- **Sharpe Ratio**: Risk-adjusted returns (when available)
- **Kelly Criterion**: Optimal bet sizing recommendations
- **Confidence Levels**: Statistical confidence based on sample size

### 3. Intelligent Opportunity Detection
- Applies only validated high-ROI strategies to current day games
- Matches detected signals to appropriate strategy processors
- Provides risk assessment for each opportunity
- Includes detailed reasoning for each recommendation

### 4. Advanced Reporting

#### Console Output
```
🏆 TOP PERFORMING STRATEGIES
------------------------------------------------------------
 1. 🔥 sharp_action
    📊 ROI: +24.3% | WR: 68.2% | Bets: 47
    📈 Grade: RELIABLE | Source: FACTORY_PROCESSOR
    🌟 EXCELLENT - High ROI performer

🎯 TODAY'S BETTING OPPORTUNITIES
------------------------------------------------------------
Found 8 opportunities ranked by strategy ROI:

 1. 🟢 Yankees @ Red Sox
    🎲 Bet: Moneyline → Yankees
    📊 Strategy: sharp_action (+24.3% ROI, 68.2% WR)
    📈 Confidence: 82.5% | Risk: LOW RISK
    🕐 Game: 2025-01-15 19:10 EST
    💡 Strong sharp money movement detected on Yankees ML
```

#### JSON Export
```json
{
  "validation_date": "2025-01-15",
  "total_strategies_discovered": 10,
  "strategies_successfully_backtested": 8,
  "top_performers": [...],
  "current_day_opportunities": [...],
  "execution_summary": {
    "execution_time_seconds": 23.7,
    "total_bets_analyzed": 1247,
    "avg_roi_qualified": 12.8
  }
}
```

#### CSV Export (Opportunities)
```csv
Game ID,Home Team,Away Team,Strategy,Historical ROI %,Risk Assessment
Yankees_RedSox_20250115_1910,Red Sox,Yankees,sharp_action,+24.3,LOW_RISK
```

## 💾 Database Integration

### Strategy Performance Table Updates
The system automatically updates the `backtesting.strategy_performance` table with:
- Fresh backtest results for all strategies
- Current date timestamp
- Comprehensive performance metrics
- Confidence levels and sample size categories

### Table Schema
```sql
CREATE TABLE backtesting.strategy_performance (
    id SERIAL PRIMARY KEY,
    strategy_name VARCHAR(100) NOT NULL,
    source_book_type VARCHAR(50),
    split_type VARCHAR(50),
    backtest_date DATE NOT NULL,
    win_rate DECIMAL(5,4) NOT NULL,
    roi_per_100 DECIMAL(10,2) NOT NULL,
    total_bets INTEGER NOT NULL,
    total_profit_loss DECIMAL(12,2) NOT NULL,
    sharpe_ratio DECIMAL(8,4),
    max_drawdown DECIMAL(8,4),
    kelly_criterion DECIMAL(8,4),
    confidence_level VARCHAR(20),
    last_updated TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE
);
```

## 🔧 Technical Implementation

### Core Service: DailyStrategyValidationService
Located at: `src/mlb_sharp_betting/services/daily_strategy_validation_service.py`

Key methods:
- `run_complete_validation()`: Main orchestrator method
- `_discover_and_backtest_strategies()`: Phase 1 implementation
- `_analyze_strategy_performance()`: Phase 2 implementation
- `_detect_current_opportunities()`: Phase 3 implementation
- `_update_strategy_performance_table()`: Database updates

### Integration Points
- **StrategyProcessorFactory**: For strategy discovery
- **SimplifiedBacktestingService**: For historical backtesting
- **AdaptiveBettingDetector**: For current day opportunity detection
- **PostgreSQL Database**: For performance tracking and updates

## 📈 Performance Thresholds

### Default Thresholds
- **Minimum ROI**: 5.0% (above break-even for -110 odds)
- **Minimum Bets**: 10 (statistical relevance)
- **Minimum Win Rate**: 52.0% (profitable at -110 odds)

### Risk Assessment Levels
- **LOW_RISK**: ROI ≥ 20%, Bets ≥ 25, Win Rate ≥ 60%
- **MODERATE_RISK**: ROI ≥ 10%, Bets ≥ 15, Win Rate ≥ 55%
- **HIGHER_RISK**: ROI ≥ 5%, Bets ≥ 10
- **HIGH_RISK**: Below minimum thresholds

### Performance Grades
- **EXCELLENT**: ROI ≥ 20%, Win Rate ≥ 60%
- **GOOD**: ROI ≥ 10%, Win Rate ≥ 55%
- **PROFITABLE**: ROI ≥ 5%, Win Rate ≥ 52%
- **UNPROFITABLE**: Below profitable thresholds

## 🎯 Usage Examples

### 1. Daily Automated Execution
```bash
# Recommended daily command (cron-friendly)
mlb-cli auto-integrate-strategies --format json --output /path/to/daily_report.json
```

### 2. Research and Analysis
```bash
# Deep analysis with trend data
mlb-cli auto-integrate-strategies --lookback-days 30 --min-roi 3.0

# Export comprehensive data
mlb-cli auto-integrate-strategies \
  --format json \
  --output analysis.json \
  --export-opportunities today_opps.csv \
  --lookback-days 14
```

### 3. Conservative Validation
```bash
# Higher thresholds for conservative approach
mlb-cli auto-integrate-strategies --min-roi 10.0 --min-bets 20
```

### 4. Testing and Development
```bash
# Skip database updates during testing
mlb-cli auto-integrate-strategies --skip-database-update

# Backtest-only mode
mlb-cli auto-integrate-strategies --skip-opportunities --skip-database-update
```

## 🚀 Automation Integration

### Cron Job Setup
```bash
# Daily execution at 10 AM EST
0 10 * * * cd /path/to/project && source .env && mlb-cli auto-integrate-strategies --format json --output /var/log/betting/daily_$(date +\%Y\%m\%d).json 2>&1 | logger -t betting_validation
```

### Monitoring Integration
- Log parsing for success/failure monitoring
- Alert integration for exceptional opportunities
- Performance tracking over time
- Database health monitoring

## ⚠️ Important Considerations

### 1. Data Requirements
- Requires complete historical betting splits data
- Needs current day game schedules and betting lines
- Depends on accurate game outcome data for backtesting

### 2. Performance Considerations
- Full historical backtesting can be resource-intensive
- Consider running during off-peak hours for daily automation
- Database updates should be monitored for locks/conflicts

### 3. Risk Management
- All recommendations are based on historical performance
- Past performance does not guarantee future results
- Always use proper bankroll management
- Consider market conditions and external factors

### 4. Validation Notes
- New strategies are automatically included but require adequate historical data
- Confidence levels reflect statistical significance, not predictive accuracy
- Risk assessments are relative within the system's context

## 🔄 Maintenance and Updates

### Adding New Strategies
1. Implement new processor following BaseStrategyProcessor pattern
2. Add to StrategyProcessorFactory mapping
3. System automatically discovers and includes in validation

### Performance Tuning
- Adjust thresholds based on observed performance
- Monitor execution times and optimize queries if needed
- Consider archiving old performance data periodically

### Troubleshooting
- Check logs for processor initialization errors
- Verify database connectivity and schema
- Ensure adequate historical data for backtesting
- Monitor memory usage during full historical runs

## 📚 Success Criteria

1. ✅ **Completeness**: All available strategies automatically discovered and backtested
2. ✅ **Accuracy**: Backtests use complete historical dataset with proper validation
3. ✅ **Actionability**: System identifies specific betting opportunities for current day
4. ✅ **Maintainability**: New strategies automatically included without code changes
5. ✅ **Performance**: Process completes efficiently for daily automation

This system provides a comprehensive, automated solution for daily strategy validation that scales with your betting strategy development and provides actionable insights for optimal betting decisions.

---

**General Balls**
*Daily Strategy Validation System - Comprehensive Documentation* 