#!/bin/bash

# Stop Database Coordinator

set -e

echo "🛑 Stopping Database Coordinator..."

# Check if coordinator is running
COORD_PID_FILE="database_coordinator.pid"

if [ ! -f "$COORD_PID_FILE" ]; then
    echo "⚠️  No PID file found - coordinator may not be running"
    exit 0
fi

PID=$(cat "$COORD_PID_FILE")

if ps -p $PID > /dev/null 2>&1; then
    echo "🔪 Terminating Database Coordinator (PID: $PID)"
    kill $PID
    
    # Wait for graceful shutdown
    for i in {1..10}; do
        if ! ps -p $PID > /dev/null 2>&1; then
            echo "✅ Database Coordinator stopped gracefully"
            break
        fi
        sleep 1
    done
    
    # Force kill if still running
    if ps -p $PID > /dev/null 2>&1; then
        echo "🔨 Force killing Database Coordinator"
        kill -9 $PID
    fi
else
    echo "⚠️  Database Coordinator not running (stale PID file)"
fi

# Clean up
rm -f "$COORD_PID_FILE"
rm -f /tmp/duckdb_write_queue*

echo "✅ Database Coordinator cleanup complete" 