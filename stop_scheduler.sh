#!/bin/bash
# Stop the MLB Betting Scheduler

cd "$(dirname "$0")"

if [ -f scheduler.pid ]; then
    PID=$(cat scheduler.pid)
    echo "🛑 Stopping scheduler (PID $PID)..."
    
    if kill $PID 2>/dev/null; then
        echo "✅ Scheduler stopped successfully"
    else
        echo "⚠️  Process not found, may have already stopped"
    fi
    
    rm -f scheduler.pid
    echo "🧹 Cleaned up PID file"
else
    echo "⚠️  No PID file found. Scheduler may not be running."
    echo "   Check with: ps aux | grep run_scheduler.py"
fi

echo ""
echo "📊 To start again: ./start_scheduler.sh" 