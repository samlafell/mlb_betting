#!/bin/bash
# Check MLB Betting SchedulerEngine status (Phase 4)

cd "$(dirname "$0")"

echo "🔍 MLB Betting SchedulerEngine Status (Phase 4)"
echo "=============================================="

# Check if PID file exists
if [ -f pregame_scheduler.pid ]; then
    PID=$(cat pregame_scheduler.pid)
    echo "📄 PID file found: $PID"
    
    # Check if process is running
    if ps -p $PID > /dev/null 2>&1; then
        echo "✅ SchedulerEngine is running (PID $PID)"
        
        # Show process details
        echo ""
        echo "📊 Process details:"
        ps -p $PID -o pid,ppid,etime,cmd
        
        # Show detailed status using CLI
        echo ""
        echo "📈 SchedulerEngine Status:"
        echo "------------------------"
        uv run python -m mlb_sharp_betting.cli pregame status 2>/dev/null || echo "Unable to get detailed status (may still be initializing)"
        
    else
        echo "❌ SchedulerEngine is not running (stale PID file)"
        echo "🧹 Removing stale PID file"
        rm -f pregame_scheduler.pid
    fi
else
    echo "❌ No PID file found - SchedulerEngine is not running"
fi

echo ""
echo "📝 Recent log entries:"
echo "--------------------"
if [ -f pregame_scheduler.log ]; then
    tail -n 15 pregame_scheduler.log
    echo ""
    echo "📊 Log file size: $(du -h pregame_scheduler.log | cut -f1)"
else
    echo "No log file found"
fi

echo ""
echo "🎯 Management commands:"
echo "   • Start: ./start_pregame_scheduler.sh"
echo "   • Stop: ./stop_pregame_scheduler.sh"
echo "   • Restart: ./start_pregame_scheduler.sh --restart"
echo "   • Live logs: tail -f pregame_scheduler.log"
echo "   • Manual run: uv run python -m mlb_sharp_betting.cli pregame test-workflow"
echo "   • Status: uv run python -m mlb_sharp_betting.cli pregame status"

# Check today's games using the CLI
echo ""
echo "⚾ Today's MLB games:"
echo "-------------------"
uv run python -m mlb_sharp_betting.cli pregame list-games 2>/dev/null || echo "Unable to fetch game info"

echo ""
echo "💾 Database Integration:"
echo "   • Recommendations saved to: tracking.pre_game_recommendations"
echo "   • Game outcomes tracked in: public.game_outcomes"  
echo "   • Strategy performance in: backtesting.strategy_performance"

# Check if email is configured
echo ""
echo "📧 Email Configuration:"
echo "---------------------"
if [[ -n "${EMAIL_FROM_ADDRESS:-}" && -n "${EMAIL_APP_PASSWORD:-}" && -n "${EMAIL_TO_ADDRESSES:-}" ]]; then
    echo "✅ Email configuration appears complete"
    echo "   From: ${EMAIL_FROM_ADDRESS}"
    echo "   To: ${EMAIL_TO_ADDRESSES}"
else
    echo "⚠️  Email may not be fully configured"
    echo "   Run: uv run python -m mlb_sharp_betting.cli pregame configure-email"
fi 