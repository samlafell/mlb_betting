# Enhanced CLI Usage Guide

## 🚨 Important: Command Deprecation

The old `detect-opportunities` (with hyphen) command has been **DEPRECATED**. 

❌ **Don't use:** `mlb-cli detect-opportunities`  
✅ **Use instead:** `mlb-cli detect opportunities`

## 📚 New Enhanced Commands

### 1. Get Intelligent Recommendations
```bash
# See what the system recommends you should run
mlb-cli detect recommendations
```

**Output example:**
```
💡 SYSTEM RECOMMENDATIONS
==================================================
🟢 System Health: EXCELLENT
🚨 Priority Level: LOW
⏱️  Estimated Runtime: 2 minutes

🚀 RECOMMENDED ACTIONS:
1. Detection
   📝 Reason: Find current betting opportunities
   ⏱️  Time: ~2 minutes
```

### 2. Smart Pipeline (Automatic)
```bash
# Let the system automatically decide what to run
mlb-cli detect smart-pipeline --minutes 1800
```

**What it does:**
- Analyzes system state
- Automatically runs data collection if needed
- Automatically runs backtesting if needed  
- Always runs detection
- Shows summary of opportunities found

### 3. Full Opportunities Analysis
```bash
# Get detailed betting opportunities and recommendations
mlb-cli detect opportunities --minutes 1800
```

**What you'll see:**
1. **Pipeline Metrics** - Data collection results
2. **Strategy Analysis** - Backtesting performance 
3. **🎯 OPPORTUNITY DETECTION RESULTS** - **THIS IS WHERE YOUR BETTING RECOMMENDATIONS ARE!**

## 🎯 Where to Find Your Betting Recommendations

The betting recommendations appear in the **🎯 OPPORTUNITY DETECTION RESULTS** section:

```
🎯 OPPORTUNITY DETECTION RESULTS
   🎮 Games Analyzed: 6
   🚨 Total Opportunities: 9

   🎲 ATL @ NYM
      📅 2025-06-26 19:10 EST
      🎯 Opportunities: 3
         🔪 Sharp Signals: 3

   🎲 MIA @ SF  
      📅 2025-06-26 15:45 EST
      🎯 Opportunities: 1
         🔪 Sharp Signals: 1
```

Each game shows:
- **🎲 Teams** - Away @ Home
- **📅 Game Time** - When the game starts (EST)
- **🎯 Opportunities** - Total number of betting signals
- **🔪 Sharp Signals** - Number of sharp money indicators
- **📚 Book Conflicts** - Line shopping opportunities  
- **⚔️ Opposing Markets** - Contrarian betting opportunities
- **🌊 Steam Moves** - Line movement signals

## 🔀 Cross-Market Flips

If there are cross-market flips (advanced betting opportunities), they appear in a separate section:

```
🔀 CROSS-MARKET FLIP ANALYSIS
Found 2 cross-market flips with ≥60.0% confidence

🎯 FLIP #1: LAD @ COL
   📅 Game: 2025-06-26 15:10 EST
   🔄 Type: Moneyline Sharp Flip
   📊 Confidence: 75.2%
   💡 RECOMMENDATION: Bet LAD moneyline based on late sharp action
   🧠 Reasoning: Early public money on COL, late sharp money flipped to LAD
```

## 📊 JSON Output for Analysis

For programmatic analysis or detailed data:

```bash
# Save detailed results to JSON file
mlb-cli detect opportunities --minutes 1800 --format json --output today_opportunities.json
```

## 🚀 Quick Commands Reference

| Command | Purpose | When to Use |
|---------|---------|-------------|
| `mlb-cli detect recommendations` | Get system recommendations | Start here to see what to run |
| `mlb-cli detect smart-pipeline` | Auto-orchestrated detection | Quick analysis with auto-decisions |
| `mlb-cli detect opportunities` | Full detailed analysis | When you want complete betting recommendations |

## 💡 Pro Tips

1. **Start with recommendations:** Always run `mlb-cli detect recommendations` first
2. **Use smart-pipeline for speed:** If you just want a quick check
3. **Use opportunities for details:** When you need specific betting recommendations
4. **Increase minutes for more games:** Use `--minutes 1800` to look 30 hours ahead
5. **JSON output for automation:** Use `--format json` for automated analysis

## ⚠️ Common Issues

- **"Failed to load" warnings:** Normal - some processors aren't implemented yet
- **"No opportunities found":** Games may be too far away or no sharp action detected
- **Slow execution:** First run collects fresh data and runs backtesting

## 🎯 Bottom Line

**To see your betting recommendations:**
```bash
mlb-cli detect opportunities --minutes 1800
```

Look for the **🎯 OPPORTUNITY DETECTION RESULTS** section - that's where your actionable betting opportunities are listed! 