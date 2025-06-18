#!/bin/bash
# Check MLB Betting Scheduler status

cd "$(dirname "$0")"

echo "🔍 MLB Betting Scheduler Status"
echo "=============================="

# Check if PID file exists
if [ -f scheduler.pid ]; then
    PID=$(cat scheduler.pid)
    echo "📄 PID file found: $PID"
    
    # Check if process is running
    if ps -p $PID > /dev/null 2>&1; then
        echo "✅ Scheduler is running (PID $PID)"
        
        # Show process details
        echo ""
        echo "📊 Process details:"
        ps -p $PID -o pid,ppid,etime,cmd
        
    else
        echo "❌ Scheduler is not running (stale PID file)"
        echo "🧹 Removing stale PID file"
        rm -f scheduler.pid
    fi
else
    echo "❌ No PID file found - scheduler is not running"
fi

echo ""
echo "📝 Recent log entries:"
echo "--------------------"
if [ -f scheduler.log ]; then
    tail -n 10 scheduler.log
    echo ""
    echo "📊 Log file size: $(du -h scheduler.log | cut -f1)"
else
    echo "No log file found"
fi

echo ""
echo "🎯 Management commands:"
echo "   • Start: ./start_scheduler.sh"
echo "   • Stop: ./stop_scheduler.sh"
echo "   • Live logs: tail -f scheduler.log"
echo "   • Manual run: uv run src/mlb_sharp_betting/entrypoint.py"

# Check today's games
echo ""
echo "⚾ Today's MLB games:"
echo "-------------------"
uv run test_scheduler.py 2>/dev/null | grep -A 10 "Today's games:" | tail -n +2 || echo "Unable to fetch game info" 