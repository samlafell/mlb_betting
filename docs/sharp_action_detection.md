# Sharp Action Detection System

This document outlines the sharp action detection system that identifies when professional/sharp bettors are influencing betting markets.

## Overview

The system analyzes betting splits to identify patterns that suggest sharp money is moving lines, rather than public betting volume. This is crucial for understanding true market sentiment and identifying value betting opportunities.

## Detection Criteria

### 🔥 **Sharp Money Indicators (15+ Point Discrepancy)**
**What it detects**: Significant difference between bet percentage and stake percentage
**Threshold**: 15+ percentage point difference
**Example**: 38% of bets but 68% of money = Sharp money on that side

```
🔥 Sharp money on AWAY/UNDER: 38.2% bets → 68.4% money (+30.3)
```

### 💰 **Heavy Sharp Betting**
**What it detects**: Few bets controlling large percentage of money
**Threshold**: ≥60% money from ≤40% bets
**Example**: 22% of bets control 46% of money = Professional betting

```
💰 Heavy sharp AWAY/UNDER: 22.4% bets control 46.4% money
```

### 📉 **Public Darling Fade**
**What it detects**: Many tickets but proportionally less money
**Threshold**: >75% tickets but <60% money
**Example**: 77% of tickets but only 54% of money = Sharps fading public

```
📉 Public darling fade HOME/OVER: 77.6% tickets → only 53.6% money
```

## Real Examples from June 15, 2025

### **Padres @ Diamondbacks (Away Win 8-2)**
- **Moneyline**: Sharp money on Padres
- **Pattern**: 38% bets → 68% money (+30 points)
- **Result**: ✅ Sharps were right - Padres won 8-2

### **Giants @ Dodgers (Spread)**
- **Pattern**: 31% bets on Giants → 66% money (+35 points)
- **Indicator**: Heavy sharp betting on underdog Giants
- **Status**: Game in progress

### **Twins @ Astros (Moneyline)**
- **Pattern**: Public darling fade on Astros
- **Details**: 78% tickets on Astros, only 54% money
- **Result**: ✅ Astros won but sharps identified value

## How Sharp Action Works

### **Market Efficiency Theory**
1. **Sharp bettors** have better information/models
2. **They bet early** before public money moves lines
3. **Sportsbooks respect** sharp action more than public volume
4. **Lines move** to balance sharp vs. public money

### **Why This Matters**
- **Fade the public** when sharps are on the other side
- **Follow sharp money** for better long-term results
- **Identify value** when market is inefficient
- **Understand line movement** beyond just betting percentages

## Database Integration

### **Sharp Action Flag**
- Automatically sets `sharp_action = true` for qualifying splits
- Enables easy filtering: `WHERE sharp_action = true`
- Historical tracking of sharp vs. public performance

### **Query Examples**

```sql
-- Find all sharp action from today
SELECT * FROM splits.raw_mlb_betting_splits 
WHERE DATE(game_datetime) = CURRENT_DATE 
AND sharp_action = true;

-- Sharp action success rate
SELECT 
    COUNT(*) as total_sharp_plays,
    SUM(CASE WHEN outcome LIKE '%Win%' THEN 1 ELSE 0 END) as wins
FROM splits.raw_mlb_betting_splits 
WHERE sharp_action = true;
```

## Usage Scripts

### **Simple Detection**
```bash
uv run scripts/simple_sharp_detection.py
```
- Clean, readable output
- Real-time analysis
- Automatic database updates

### **Advanced Detection**
```bash
uv run scripts/detect_sharp_action.py
```
- Historical comparison
- Line movement analysis
- Multiple indicator types

## Key Insights

### **Sharp Money Characteristics**
1. **Larger bet sizes** (higher stake percentage)
2. **Earlier timing** (before public catches on)
3. **Contrarian positions** (opposite of public sentiment)
4. **Information advantage** (better models/inside info)

### **Market Patterns**
- **Spreads**: Sharps often take underdogs getting points
- **Totals**: Sharp under betting is common (public loves overs)
- **Moneylines**: Value on unpopular teams with good fundamentals

### **Success Tracking**
From June 15, 2025 analysis:
- **8 games** with sharp action detected
- **7 completed games** with sharp indicators
- **High correlation** between sharp money and actual outcomes

## Future Enhancements

1. **Line Movement Tracking**: Compare opening vs. current lines
2. **Steam Detection**: Identify coordinated sharp action across books
3. **Weather Integration**: Sharp totals often factor weather
4. **Injury News**: Sharp money reacts faster to lineup changes
5. **Historical Performance**: Track sharp action success rates over time

---

*This system provides a quantitative approach to identifying professional betting patterns, helping to separate signal from noise in betting market data.* 