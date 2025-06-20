#!/bin/bash

# Start Database Coordinator for DuckDB Concurrency Management
# This ensures all processes access DuckDB through a coordinated queue

set -e

echo "🚀 Starting Database Coordinator..."

# Ensure we're in the right directory
cd "$(dirname "$0")"

# Source environment
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# Check if coordinator is already running
COORD_PID_FILE="database_coordinator.pid"

if [ -f "$COORD_PID_FILE" ]; then
    PID=$(cat "$COORD_PID_FILE")
    if ps -p $PID > /dev/null 2>&1; then
        echo "⚠️  Database Coordinator already running (PID: $PID)"
        echo "   Use stop_database_coordinator.sh to stop it first"
        exit 1
    else
        echo "🧹 Removing stale PID file"
        rm -f "$COORD_PID_FILE"
    fi
fi

# Create logs directory
mkdir -p logs

# Start the coordinator in background
echo "📊 Initializing DuckDB process coordinator..."
nohup uv run -m mlb_sharp_betting.services.database_coordinator \
    > logs/database_coordinator.log 2>&1 &

# Save PID
COORD_PID=$!
echo $COORD_PID > "$COORD_PID_FILE"

# Wait a moment and check if it started successfully
sleep 2

if ps -p $COORD_PID > /dev/null 2>&1; then
    echo "✅ Database Coordinator started successfully!"
    echo "   PID: $COORD_PID"
    echo "   Log: logs/database_coordinator.log"
    echo "   Queue: /tmp/duckdb_write_queue"
    echo ""
    echo "📋 Now your analysis scripts will use coordinated database access"
    echo "   This eliminates DuckDB concurrency conflicts completely"
else
    echo "❌ Failed to start Database Coordinator"
    echo "   Check logs/database_coordinator.log for details"
    rm -f "$COORD_PID_FILE"
    exit 1
fi 