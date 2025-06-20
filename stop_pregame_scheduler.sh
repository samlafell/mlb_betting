#!/bin/bash

# Stop the pre-game workflow scheduler

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="${SCRIPT_DIR}/pregame_scheduler.pid"

if [[ -f "$PID_FILE" ]]; then
    pid=$(cat "$PID_FILE")
    if kill -0 "$pid" 2>/dev/null; then
        echo "Stopping scheduler (PID: $pid)..."
        kill "$pid"
        
        # Wait for graceful shutdown
        count=0
        while kill -0 "$pid" 2>/dev/null && [[ $count -lt 30 ]]; do
            sleep 1
            ((count++))
        done
        
        if kill -0 "$pid" 2>/dev/null; then
            echo "Graceful shutdown failed, forcing termination..."
            kill -9 "$pid"
        fi
        
        rm -f "$PID_FILE"
        echo "Scheduler stopped successfully"
    else
        echo "Scheduler not running (stale PID file)"
        rm -f "$PID_FILE"
    fi
else
    echo "Scheduler not running (no PID file)"
fi
