# MLB Sharp Betting Strategy Business Rules Documentation

## Executive Summary

This document defines the specific business rules, thresholds, and conditions for all MLB betting strategies used in the system. These rules are implemented across SQL strategy files and Python detection scripts to identify profitable betting opportunities.

## Core System Rules

### 1. Juice Filter Service Rules
- **Maximum Juice Threshold**: -160 (moneyline favorites)
- **Rule**: Reject any moneyline bet on a favorite worse than -160
- **Rationale**: Avoid heavily juiced lines that require unrealistic win rates

### 2. Data Quality Rules
- **Minimum Sample Size**: 2-3 bets per strategy variant
- **Data Recency**: Focus on games within 7-30 days
- **Time Window**: Only analyze data from before game completion
- **Null Data Handling**: Exclude records with missing stake/bet percentages

### 3. Confidence Scoring Rules
- **High Confidence**: ≥75% confidence score + strong performance metrics
- **Moderate Confidence**: 60-74% confidence score
- **Low Confidence**: <60% confidence score
- **Break-even Threshold**: 52.4% win rate at -110 odds

---

## Strategy-Specific Business Rules

### SHARP ACTION DETECTOR STRATEGY

**Purpose**: Identify when professional money is significantly different from public betting patterns

#### Entry Conditions
- **Premium Sharp**: ≥20% differential + ≥100 total bets
- **Strong Sharp**: ≥15% differential
- **Moderate Sharp**: ≥10% differential + ≥50 total bets
- **Late Sharp**: ≥7% differential + closing window (≤2 hours)
- **Volume Sharp**: Volume-adjusted thresholds based on bet count

#### Book Reliability Weighting
- **Pinnacle**: 1.5x multiplier (highest reliability)
- **BookMaker**: 1.3x multiplier
- **Circa**: 1.2x multiplier
- **BetMGM**: 1.1x multiplier
- **DraftKings/FanDuel**: 0.9x multiplier (public books)

#### Volume Tiers
- **High Volume**: ≥500 total bets
- **Medium Volume**: 100-499 total bets
- **Low Volume**: <100 total bets (requires higher differential)

#### Timing Categories
- **Ultra Late**: ≤0.5 hours before game
- **Closing Hour**: 0.5-1 hours before game
- **Late**: 1-6 hours before game
- **Early**: 6-24 hours before game
- **Very Early**: >24 hours before game

### CONSENSUS MONEYLINE STRATEGY

**Purpose**: Identify when both public and sharp money align on moneyline bets

#### Signal Classifications
- **Consensus Heavy Home**: ≥90% money + ≥90% bets on home team
- **Consensus Heavy Away**: ≤10% money + ≤10% bets on home team
- **Mixed Consensus Home**: ≥80% money + ≥60% bets on home team
- **Mixed Consensus Away**: ≤20% money + ≤40% bets on home team

#### Minimum Requirements
- **Data Sources**: ≥2 different sportsbooks
- **Sample Size**: ≥3 bets per strategy variant
- **Bet Type**: Moneyline only

### UNDERDOG MONEYLINE VALUE STRATEGY

**Purpose**: Systematic underdog betting when public heavily favors favorites

#### Odds Categories
- **Small Dog**: +100 to +200 odds
- **Big Dog**: >+200 odds
- **Small Favorite**: -100 to -160 odds
- **Big Favorite**: <-160 odds

#### Value Signal Thresholds
- **Strong Value**: ≥65% public money on favorite + underdog odds
- **Moderate Value**: ≥60% public money on favorite + underdog odds
- **Minimum Public Bias**: ≤35% public money (for home dogs)

#### Sharp Confirmation
- **Home Dog Sharp**: ≥10% money differential favoring home underdog
- **Away Dog Sharp**: ≤-10% money differential favoring away underdog

### TIMING-BASED STRATEGY

**Purpose**: Analyze the timing of sharp action and line movement correlation

#### Timing Precision Categories
- **Ultra Late**: ≤30 minutes before game (1.5x credibility multiplier)
- **Closing Hour**: 30-60 minutes before game (1.3x credibility multiplier)
- **Closing 2H**: 1-2 hours before game (1.2x credibility multiplier)
- **Late Afternoon**: 2-4 hours before game
- **Same Day**: 4-12 hours before game (0.9x credibility multiplier)

#### Volume Reliability
- **Reliable Volume**: ≥1,000 total bets
- **Moderate Volume**: 500-999 total bets
- **Insufficient Volume**: <500 total bets

#### Line Movement Correlation
- **Sharp Money vs Line Move**: Sharp differential >10% opposing line movement
- **Sharp Money No Movement**: Sharp differential >15% with minimal line movement
- **Reverse Line Movement**: Strong indicator of sharp action

### PUBLIC MONEY FADE STRATEGY

**Purpose**: Contrarian betting when public consensus is overwhelming

#### Public Consensus Thresholds
- **Heavy Public Home**: ≥85% average money + ≥2 books
- **Heavy Public Away**: ≤15% average money + ≥2 books
- **Moderate Public Home**: ≥75% average money + ≥70% minimum + ≥3 books
- **Moderate Public Away**: ≤25% average money + ≤30% maximum + ≥3 books

#### Fade Confidence Levels
- **High Confidence**: Heavy public consensus signals
- **Moderate Confidence**: Moderate public consensus signals
- **Minimum Books**: ≥2 sportsbooks showing consensus

---

## Master Betting Detector Rules

### Dynamic Threshold Optimization

#### Performance-Based Thresholds
- **High Performers**: 20% minimum differential (aggressive)
- **Moderate Performers**: 25% minimum differential
- **Low Performers**: 30% minimum differential (conservative)
- **Unvalidated**: 35% minimum differential (very conservative)

#### Strategy Integration Rules
- **Primary Strategy**: Use highest confidence signal
- **Fallback Strategy**: If primary fails, use more conservative thresholds
- **Equal Strength**: Choose strategy with higher confidence threshold
- **Minimum Threshold**: All strategies must meet validated minimums

### Signal Combination Rules

#### Cross-Market Validation
- **Steam Move Threshold**: ≥20-30% differential within 2-hour window
- **Multi-Book Consensus**: ≥2 books showing same signal
- **Confidence Boost**: Multiple confirming signals increase confidence

#### Timing Windows
- **Immediate Action**: Within 5 minutes of game start
- **Pre-Game**: 5-60 minutes before game
- **Extended Window**: Up to 300 minutes (5 hours) for broader analysis

---

## Risk Management Rules

### Position Sizing Rules
- **High Confidence**: Standard unit size
- **Moderate Confidence**: Reduced unit size
- **Low Confidence**: Minimal unit size or no bet

### Juice Limits
- **Moneyline Favorites**: Maximum -160 odds
- **Spread Bets**: Prefer -110 to -115 range
- **Total Bets**: Prefer -110 to -115 range

### Volume Requirements
- **Strategy Validation**: ≥2-3 bets minimum sample
- **Confidence Scoring**: Higher volume = higher confidence
- **Book Reliability**: Weight by historical book performance

---

## Implementation Rules

### Data Processing Rules
- **Deduplication**: One recommendation per game/source/book combination
- **Timezone Handling**: All times converted to Eastern Standard Time
- **Latest Data**: Use most recent data point for each game/book/source

### API Usage Limits
- **The Odds API**: Maximum 480 calls per month (500 free limit with buffer)
- **Rate Limiting**: Respect all API rate limits and retry policies
- **Data Caching**: Cache data to minimize API calls

### Database Rules
- **Connection Pooling**: Use coordinated database access
- **Transaction Management**: Proper commit/rollback for data integrity
- **Schema Validation**: Ensure all data meets schema requirements

---

## Performance Validation Rules

### ROI Calculation Rules
- **Standard Odds**: Assume -110 for spread/total calculations
- **Moneyline Odds**: Use actual odds from split_value JSON
- **Unit Betting**: Calculate returns based on $100 units
- **Profit Formula**: (Wins × Payout) - (Losses × Risk)

### Strategy Rating System
- **🟢 Excellent**: ≥15% ROI + ≥5 bets
- **🟢 Very Good**: ≥10% ROI + ≥3 bets
- **🟡 Good**: ≥5% ROI + ≥2 bets
- **🟡 Profitable**: >0% ROI + ≥2 bets
- **🔴 Unprofitable**: ≤0% ROI

### Confidence Validation
- **High**: ≥15 bets sample size
- **Medium**: 8-14 bets sample size
- **Low**: 5-7 bets sample size
- **Very Low**: 2-4 bets sample size

---

## Edge Case Handling

### Data Quality Issues
- **Missing Data**: Exclude from analysis
- **Inconsistent Formats**: Standardize before processing
- **Timezone Mismatches**: Convert all to EST
- **Null Values**: Handle gracefully without errors

### Market Conditions
- **Low Volume Games**: Increase required differential thresholds
- **High Volume Games**: Can use lower differential thresholds
- **Primetime Games**: 1.2x credibility multiplier
- **Weekend Games**: 1.1x credibility multiplier

### System Maintenance
- **Strategy Updates**: Automatic threshold adjustments every 15 minutes
- **Performance Monitoring**: Continuous tracking of strategy effectiveness
- **Alert System**: Notifications for significant performance changes

---

## Compliance and Validation

### Testing Requirements
- **Backtesting**: All strategies must show historical profitability
- **Sample Size**: Minimum statistical significance requirements
- **Time Period**: Validate across multiple time periods
- **Market Conditions**: Test in various market environments

### Audit Trail
- **Decision Logging**: Record all betting recommendations
- **Performance Tracking**: Monitor actual vs expected results
- **Rule Changes**: Document all business rule modifications
- **Compliance Reporting**: Regular strategy performance reviews

---

*This document serves as the authoritative reference for all betting strategy business rules. Any changes to these rules must be documented and validated through backtesting before implementation.*

**Last Updated**: {{ current_date }}  
**Version**: 1.0  
**Author**: General Balls 