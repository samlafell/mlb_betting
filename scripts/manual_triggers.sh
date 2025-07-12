#!/bin/bash

# Manual Pre-Game Workflow Triggers for Remaining Games Today
# Run this script to trigger workflows 5 minutes before each game

echo "🏈 Manual Pre-Game Workflow Triggers"
echo "===================================="

# Source environment variables
set -a && source .env && set +a

# Function to trigger workflow
trigger_workflow() {
    local game_id=$1
    local game_name=$2
    local game_time=$3
    
    echo "🎯 Triggering workflow for: $game_name"
    echo "   Game Time: $game_time EST"
    echo "   Game ID: $game_id"
    
    uv run python src/mlb_sharp_betting/cli.py pregame test-workflow --game-pk $game_id
    
    echo "✅ Workflow completed for $game_name"
    echo ""
}

# Remaining games for today (7:00 PM EST onwards)
echo "📅 $(date)"
echo ""

# LAA @ NYY - 7:05 PM EST (trigger at 7:00 PM)
if [[ $(date +%H%M) -ge 1900 && $(date +%H%M) -lt 1905 ]]; then
    trigger_workflow 777466 "Los Angeles Angels @ New York Yankees" "07:05 PM"
fi

# ARI @ TOR - 7:07 PM EST (trigger at 7:02 PM)  
if [[ $(date +%H%M) -ge 1902 && $(date +%H%M) -lt 1907 ]]; then
    trigger_workflow 777462 "Arizona Diamondbacks @ Toronto Blue Jays" "07:07 PM"
fi

# MIN @ CIN - 7:10 PM EST (trigger at 7:05 PM)
if [[ $(date +%H%M) -ge 1905 && $(date +%H%M) -lt 1910 ]]; then
    trigger_workflow 777474 "Minnesota Twins @ Cincinnati Reds" "07:10 PM"
fi

# NYM @ ATL - 7:15 PM EST (trigger at 7:10 PM)
if [[ $(date +%H%M) -ge 1910 && $(date +%H%M) -lt 1915 ]]; then
    trigger_workflow 777470 "New York Mets @ Atlanta Braves" "07:15 PM"
fi

# BAL @ TB - 7:35 PM EST (trigger at 7:30 PM)
if [[ $(date +%H%M) -ge 1930 && $(date +%H%M) -lt 1935 ]]; then
    trigger_workflow 777463 "Baltimore Orioles @ Tampa Bay Rays" "07:35 PM"
fi

# KC @ TEX - 8:05 PM EST (trigger at 8:00 PM)
if [[ $(date +%H%M) -ge 2000 && $(date +%H%M) -lt 2005 ]]; then
    trigger_workflow 777460 "Kansas City Royals @ Texas Rangers" "08:05 PM"
fi

# CLE @ SF - 9:45 PM EST (trigger at 9:40 PM)
if [[ $(date +%H%M) -ge 2140 && $(date +%H%M) -lt 2145 ]]; then
    trigger_workflow 777454 "Cleveland Guardians @ San Francisco Giants" "09:45 PM"
fi

# HOU @ OAK - 10:05 PM EST (trigger at 10:00 PM)
if [[ $(date +%H%M) -ge 2200 && $(date +%H%M) -lt 2205 ]]; then
    trigger_workflow 777456 "Houston Astros @ Athletics" "10:05 PM"
fi

# SD @ LAD - 10:10 PM EST (trigger at 10:05 PM)
if [[ $(date +%H%M) -ge 2205 && $(date +%H%M) -lt 2210 ]]; then
    trigger_workflow 777457 "San Diego Padres @ Los Angeles Dodgers" "10:10 PM"
fi

echo "🎯 Manual trigger check complete at $(date)"
echo "💡 Run this script every few minutes to catch game times" 