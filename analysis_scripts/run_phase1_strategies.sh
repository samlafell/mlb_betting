#!/bin/bash

# Phase 1 MLB Betting Strategies Runner
# Executes all three new Phase 1 strategies and saves results

set -e

echo "🚀 PHASE 1 MLB BETTING STRATEGIES ANALYSIS"
echo "============================================================"
echo "📅 Analysis Date: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

# Create analysis results directory
mkdir -p analysis_results

# Database path
DB_CONNECTION="postgresql://localhost/mlb_betting"

if [ ! -f "$DB_PATH" ]; then
    echo "❌ Database not found at $DB_PATH"
    echo "Please ensure the database exists and try again."
    exit 1
fi

echo "🔍 Running Phase 1 Strategies..."
echo ""

# Strategy 1: Total Line Sweet Spots
echo "1️⃣  TOTAL LINE SWEET SPOTS STRATEGY"
echo "----------------------------------------"
duckdb "$DB_PATH" < analysis_scripts/total_line_sweet_spots_strategy.sql > analysis_results/total_line_sweet_spots_results.txt
SWEET_SPOTS_COUNT=$(duckdb "$DB_PATH" -c "
WITH results AS ($(cat analysis_scripts/total_line_sweet_spots_strategy.sql))
SELECT COUNT(*) FROM results
")
echo "✅ Found $SWEET_SPOTS_COUNT sweet spot opportunities"

# Show top result
TOP_SWEET_SPOT=$(duckdb "$DB_PATH" -c "
WITH results AS ($(cat analysis_scripts/total_line_sweet_spots_strategy.sql))
SELECT strategy_variant || ' (' || win_rate || '% WR, ' || roi_per_100_unit || '% ROI)' as top_result
FROM results 
ORDER BY roi_per_100_unit DESC 
LIMIT 1
")
if [ ! -z "$TOP_SWEET_SPOT" ]; then
    echo "🏆 Best: $TOP_SWEET_SPOT"
fi
echo ""

# Strategy 2: Underdog ML Value
echo "2️⃣  UNDERDOG ML VALUE STRATEGY"
echo "----------------------------------------"
duckdb "$DB_PATH" < analysis_scripts/underdog_ml_value_strategy.sql > analysis_results/underdog_ml_value_results.txt
UNDERDOG_COUNT=$(duckdb "$DB_PATH" -c "
WITH results AS ($(cat analysis_scripts/underdog_ml_value_strategy.sql))
SELECT COUNT(*) FROM results
")
echo "✅ Found $UNDERDOG_COUNT underdog value opportunities"

# Show top result
TOP_UNDERDOG=$(duckdb "$DB_PATH" -c "
WITH results AS ($(cat analysis_scripts/underdog_ml_value_strategy.sql))
SELECT strategy_variant || ' (' || win_rate || '% WR, ' || roi_per_100_unit || '% ROI)' as top_result
FROM results 
ORDER BY roi_per_100_unit DESC 
LIMIT 1
")
if [ ! -z "$TOP_UNDERDOG" ]; then
    echo "🏆 Best: $TOP_UNDERDOG"
fi
echo ""

# Strategy 3: Team Specific Bias
echo "3️⃣  TEAM SPECIFIC BIAS STRATEGY"
echo "----------------------------------------"
duckdb "$DB_PATH" < analysis_scripts/team_specific_bias_strategy.sql > analysis_results/team_specific_bias_results.txt
TEAM_BIAS_COUNT=$(duckdb "$DB_PATH" -c "
WITH results AS ($(cat analysis_scripts/team_specific_bias_strategy.sql))
SELECT COUNT(*) FROM results
")
echo "✅ Found $TEAM_BIAS_COUNT team bias opportunities"

# Show top result
TOP_TEAM_BIAS=$(duckdb "$DB_PATH" -c "
WITH results AS ($(cat analysis_scripts/team_specific_bias_strategy.sql))
SELECT strategy_variant || ' (' || win_rate || '% WR, ' || roi_per_100_unit || '% ROI)' as top_result
FROM results 
ORDER BY roi_per_100_unit DESC 
LIMIT 1
")
if [ ! -z "$TOP_TEAM_BIAS" ]; then
    echo "🏆 Best: $TOP_TEAM_BIAS"
fi
echo ""

# Summary Analysis
echo "📊 COMPREHENSIVE ANALYSIS"
echo "============================================================"

# Count total opportunities and best ROI
TOTAL_OPPORTUNITIES=$((SWEET_SPOTS_COUNT + UNDERDOG_COUNT + TEAM_BIAS_COUNT))
echo "✅ Total Strategies: 3/3 executed successfully"
echo "🎯 Total Opportunities Found: $TOTAL_OPPORTUNITIES"

# Find best overall opportunity
echo ""
echo "🏆 TOP BETTING OPPORTUNITIES"
echo "----------------------------------------"

# Combine all results and find top 5
duckdb "$DB_PATH" -c "
WITH sweet_spots AS ($(cat analysis_scripts/total_line_sweet_spots_strategy.sql)),
     underdog AS ($(cat analysis_scripts/underdog_ml_value_strategy.sql)),
     team_bias AS ($(cat analysis_scripts/team_specific_bias_strategy.sql)),
     all_results AS (
         SELECT strategy_name, strategy_variant, win_rate, roi_per_100_unit, total_bets, strategy_rating, strategy_insight
         FROM sweet_spots
         UNION ALL
         SELECT strategy_name, strategy_variant, win_rate, roi_per_100_unit, total_bets, strategy_rating, strategy_insight  
         FROM underdog
         UNION ALL
         SELECT strategy_name, strategy_variant, win_rate, roi_per_100_unit, total_bets, strategy_rating, strategy_insight
         FROM team_bias
     )
SELECT 
    ROW_NUMBER() OVER (ORDER BY roi_per_100_unit DESC) as rank,
    strategy_name || ' - ' || strategy_variant as opportunity,
    '💰 ' || roi_per_100_unit || '% ROI' as roi,
    '🎯 ' || win_rate || '% WR' as win_rate,
    '📊 ' || total_bets || ' bets' as sample_size,
    '⭐ ' || strategy_rating as rating
FROM all_results
WHERE roi_per_100_unit > 0  -- Only profitable
ORDER BY roi_per_100_unit DESC
LIMIT 5
" | column -t -s $'\t'

echo ""
echo "🔍 KEY INSIGHTS"
echo "----------------------------------------"

# Count profitable strategies
PROFITABLE_COUNT=$(duckdb "$DB_PATH" -c "
WITH sweet_spots AS ($(cat analysis_scripts/total_line_sweet_spots_strategy.sql)),
     underdog AS ($(cat analysis_scripts/underdog_ml_value_strategy.sql)),
     team_bias AS ($(cat analysis_scripts/team_specific_bias_strategy.sql)),
     all_results AS (
         SELECT roi_per_100_unit FROM sweet_spots
         UNION ALL SELECT roi_per_100_unit FROM underdog  
         UNION ALL SELECT roi_per_100_unit FROM team_bias
     )
SELECT COUNT(*) FROM all_results WHERE roi_per_100_unit > 0
")

HIGH_WIN_RATE_COUNT=$(duckdb "$DB_PATH" -c "
WITH sweet_spots AS ($(cat analysis_scripts/total_line_sweet_spots_strategy.sql)),
     underdog AS ($(cat analysis_scripts/underdog_ml_value_strategy.sql)),
     team_bias AS ($(cat analysis_scripts/team_specific_bias_strategy.sql)),
     all_results AS (
         SELECT win_rate FROM sweet_spots
         UNION ALL SELECT win_rate FROM underdog  
         UNION ALL SELECT win_rate FROM team_bias
     )
SELECT COUNT(*) FROM all_results WHERE win_rate > 60
")

echo "• 📈 Found $PROFITABLE_COUNT profitable strategies (ROI > 0%)"
echo "• 🎯 Found $HIGH_WIN_RATE_COUNT strategies with >60% win rate"

# Find best strategy by ROI
BEST_STRATEGY=$(duckdb "$DB_PATH" -c "
WITH sweet_spots AS ($(cat analysis_scripts/total_line_sweet_spots_strategy.sql)),
     underdog AS ($(cat analysis_scripts/underdog_ml_value_strategy.sql)),
     team_bias AS ($(cat analysis_scripts/team_specific_bias_strategy.sql)),
     all_results AS (
         SELECT strategy_name, strategy_variant, roi_per_100_unit FROM sweet_spots
         UNION ALL SELECT strategy_name, strategy_variant, roi_per_100_unit FROM underdog  
         UNION ALL SELECT strategy_name, strategy_variant, roi_per_100_unit FROM team_bias
     )
SELECT strategy_name || ' - ' || strategy_variant || ' (' || roi_per_100_unit || '% ROI)' 
FROM all_results 
ORDER BY roi_per_100_unit DESC 
LIMIT 1
")

if [ ! -z "$BEST_STRATEGY" ]; then
    echo "• 🏆 Best opportunity: $BEST_STRATEGY"
fi

echo ""
echo "💾 Results saved to analysis_results/ directory:"
echo "   • total_line_sweet_spots_results.txt"
echo "   • underdog_ml_value_results.txt" 
echo "   • team_specific_bias_results.txt"

echo ""
echo "🎯 NEXT STEPS RECOMMENDATIONS"
echo "----------------------------------------"

if [ "$PROFITABLE_COUNT" -gt 0 ]; then
    echo "1. 🚀 IMMEDIATE ACTION:"
    echo "   Focus on highest ROI strategy identified above"
    echo "   Start with small unit sizes (0.5-1% of bankroll)"
    echo ""
    echo "2. 📈 VALIDATION APPROACH:"
    echo "   • Paper trade for 1-2 weeks to validate"
    echo "   • Track actual vs predicted performance"
    echo "   • Gradually increase unit sizes if profitable"
    echo ""
    echo "3. 🔬 CONTINUOUS IMPROVEMENT:"
    echo "   • Collect more data to increase sample sizes"
    echo "   • Monitor strategy performance weekly"
    echo "   • Adjust thresholds based on live results"
else
    echo "1. 📊 DATA COLLECTION:"
    echo "   • Need more historical data for reliable patterns"
    echo "   • Consider expanding data sources"
    echo "   • Focus on data quality improvements"
fi

echo ""
echo "🏁 Phase 1 analysis complete!"
echo "============================================================"

# Make the script executable
chmod +x "$0" 