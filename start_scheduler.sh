#!/bin/bash
# Start the MLB Betting Scheduler in background

cd "$(dirname "$0")"

# Check if already running
if [ -f scheduler.pid ]; then
    PID=$(cat scheduler.pid)
    if ps -p $PID > /dev/null 2>&1; then
        echo "⚠️  Scheduler is already running (PID $PID)"
        echo "   To stop: ./stop_scheduler.sh"
        echo "   To check logs: tail -f scheduler.log"
        exit 1
    else
        echo "🧹 Removing stale PID file"
        rm -f scheduler.pid
    fi
fi

echo "🚀 Starting MLB Betting Scheduler..."

# Start in background
nohup uv run run_scheduler.py > scheduler.log 2>&1 &
echo $! > scheduler.pid

echo "✅ Scheduler started with PID $(cat scheduler.pid)"
echo ""
echo "📋 The scheduler will:"
echo "   • Run your betting analysis every hour"  
echo "   • Alert you 5 minutes before each game"
echo "   • Get today's games at 6 AM EST"
echo ""
echo "📊 Management commands:"
echo "   • Check status: ./check_scheduler.sh"
echo "   • View logs: tail -f scheduler.log"
echo "   • Stop scheduler: ./stop_scheduler.sh"
echo ""
echo "🎰 Happy betting!" 