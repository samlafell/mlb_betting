#!/bin/bash

# MLB Sharp Betting - Pre-Game Workflow Scheduler Startup Script
# This script starts the automated pre-game workflow system using the new SchedulerEngine (Phase 4)

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${SCRIPT_DIR}/.venv"
LOG_FILE="${SCRIPT_DIR}/pregame_scheduler.log"
PID_FILE="${SCRIPT_DIR}/pregame_scheduler.pid"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Utility functions
log() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Check if scheduler is already running
check_running() {
    if [[ -f "$PID_FILE" ]]; then
        local pid=$(cat "$PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            return 0  # Running
        else
            rm -f "$PID_FILE"
            return 1  # Not running
        fi
    fi
    return 1  # Not running
}

# Stop existing scheduler
stop_scheduler() {
    if check_running; then
        local pid=$(cat "$PID_FILE")
        log "Stopping existing scheduler (PID: $pid)..."
        kill "$pid"
        
        # Wait for graceful shutdown
        local count=0
        while kill -0 "$pid" 2>/dev/null && [[ $count -lt 30 ]]; do
            sleep 1
            ((count++))
        done
        
        if kill -0 "$pid" 2>/dev/null; then
            warn "Graceful shutdown failed, forcing termination..."
            kill -9 "$pid"
        fi
        
        rm -f "$PID_FILE"
        success "Scheduler stopped"
    fi
}

# Check dependencies
check_dependencies() {
    log "Checking dependencies..."
    
    # Check if uv is available
    if ! command -v uv &> /dev/null; then
        error "UV package manager not found. Please install UV first:"
        echo "  curl -LsSf https://astral.sh/uv/install.sh | sh"
        exit 1
    fi
    
    # Check Python version
    local python_version=$(uv python --version 2>/dev/null || echo "Unknown")
    log "Python version: $python_version"
    
    # Check if project dependencies are installed
    if [[ ! -f "$SCRIPT_DIR/pyproject.toml" ]]; then
        error "pyproject.toml not found. Are you in the correct directory?"
        exit 1
    fi
    
    success "Dependencies check passed"
}

# Check email configuration
check_email_config() {
    log "Checking email configuration..."
    
    local config_ok=true
    
    if [[ -z "${EMAIL_FROM_ADDRESS:-}" ]]; then
        warn "EMAIL_FROM_ADDRESS not set"
        config_ok=false
    fi
    
    if [[ -z "${EMAIL_APP_PASSWORD:-}" ]]; then
        warn "EMAIL_APP_PASSWORD not set"
        config_ok=false
    fi
    
    if [[ -z "${EMAIL_TO_ADDRESSES:-}" ]]; then
        warn "EMAIL_TO_ADDRESSES not set"
        config_ok=false
    fi
    
    if [[ "$config_ok" == "false" ]]; then
        warn "Email configuration incomplete. Run email setup:"
        echo "  uv run python -m mlb_sharp_betting.cli pregame configure-email"
        echo ""
        echo "Or set environment variables manually:"
        echo "  export EMAIL_FROM_ADDRESS='your-gmail@gmail.com'"
        echo "  export EMAIL_APP_PASSWORD='your-app-password'"
        echo "  export EMAIL_TO_ADDRESSES='recipient1@email.com,recipient2@email.com'"
        echo ""
        echo "Scheduler will start but email notifications will be disabled."
        sleep 3
    else
        success "Email configuration verified"
    fi
}

# Install/update dependencies
install_dependencies() {
    log "Installing/updating dependencies..."
    cd "$SCRIPT_DIR"
    uv sync --all-extras
    success "Dependencies installed"
}

# Start the scheduler
start_scheduler() {
    log "Starting MLB Pre-Game Workflow Scheduler (Phase 4 SchedulerEngine)..."
    
    cd "$SCRIPT_DIR"
    
    # Start scheduler in background using new CLI command
    nohup uv run python -m mlb_sharp_betting.cli pregame start-full \
        --notifications \
        > "$LOG_FILE" 2>&1 &
    
    local pid=$!
    echo "$pid" > "$PID_FILE"
    
    # Wait a moment to see if it starts successfully
    sleep 3
    
    if kill -0 "$pid" 2>/dev/null; then
        success "SchedulerEngine started successfully!"
        log "Process ID: $pid"
        log "Log file: $LOG_FILE"
        log "PID file: $PID_FILE"
        echo ""
        log "Phase 4 SchedulerEngine is now running and will:"
        echo "  • Check for MLB games daily at 6 AM EST"
        echo "  • Trigger workflow 5 minutes before each game"
        echo "  • Convert betting signals to trackable recommendations"
        echo "  • Save recommendations to tracking.pre_game_recommendations table"
        echo "  • Send email notifications with results"
        echo "  • Run automated backtesting (daily at 2 AM EST)"
        echo ""
        log "Monitor logs with: tail -f $LOG_FILE"
        log "Check status with: uv run python -m mlb_sharp_betting.cli pregame status"
        log "Stop scheduler with: ./stop_pregame_scheduler.sh"
    else
        error "SchedulerEngine failed to start. Check logs:"
        echo "  tail -n 50 $LOG_FILE"
        rm -f "$PID_FILE"
        exit 1
    fi
}

# Create stop script
create_stop_script() {
    local stop_script="$SCRIPT_DIR/stop_pregame_scheduler.sh"
    
    cat > "$stop_script" << 'EOF'
#!/bin/bash

# Stop the pre-game workflow scheduler (Phase 4 SchedulerEngine)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="${SCRIPT_DIR}/pregame_scheduler.pid"

if [[ -f "$PID_FILE" ]]; then
    pid=$(cat "$PID_FILE")
    if kill -0 "$pid" 2>/dev/null; then
        echo "Stopping SchedulerEngine (PID: $pid)..."
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
        echo "SchedulerEngine stopped successfully"
    else
        echo "SchedulerEngine not running (stale PID file)"
        rm -f "$PID_FILE"
    fi
else
    echo "SchedulerEngine not running (no PID file)"
fi
EOF
    
    chmod +x "$stop_script"
}

# Show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Start the MLB Pre-Game Workflow Scheduler (Phase 4 SchedulerEngine)"
    echo ""
    echo "Options:"
    echo "  --restart    Stop existing scheduler and start new one"
    echo "  --stop       Stop the scheduler only"
    echo "  --status     Show scheduler status"
    echo "  --help       Show this help message"
    echo ""
    echo "The Phase 4 SchedulerEngine will:"
    echo "  • Monitor MLB games and schedule workflows 5 minutes before each game"
    echo "  • Execute three-stage workflow: data collection, analysis, email notification"
    echo "  • Convert betting signals to recommendations saved in tracking.pre_game_recommendations"
    echo "  • Run daily setup at 6 AM EST to schedule new games"
    echo "  • Perform automated backtesting (daily at 2 AM EST, weekly on Mondays)"
    echo ""
}

# Show scheduler status
show_status() {
    if check_running; then
        local pid=$(cat "$PID_FILE")
        local start_time=$(ps -o lstart= -p "$pid" 2>/dev/null | xargs)
        success "SchedulerEngine is running"
        echo "  PID: $pid"
        echo "  Started: $start_time"
        echo "  Log file: $LOG_FILE"
        echo ""
        echo "Recent log entries:"
        tail -n 10 "$LOG_FILE" 2>/dev/null || echo "  (no log entries yet)"
        echo ""
        echo "Check detailed status with:"
        echo "  uv run python -m mlb_sharp_betting.cli pregame status"
    else
        warn "SchedulerEngine is not running"
        if [[ -f "$LOG_FILE" ]]; then
            echo ""
            echo "Last log entries:"
            tail -n 10 "$LOG_FILE"
        fi
    fi
}

# Main script logic
main() {
    case "${1:-}" in
        --help|-h)
            show_usage
            exit 0
            ;;
        --status)
            show_status
            exit 0
            ;;
        --stop)
            stop_scheduler
            exit 0
            ;;
        --restart)
            stop_scheduler
            ;;
        "")
            # Normal start
            ;;
        *)
            error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
    
    # Check if already running (unless restarting)
    if [[ "${1:-}" != "--restart" ]] && check_running; then
        warn "SchedulerEngine is already running!"
        show_status
        echo ""
        echo "Use --restart to restart or --stop to stop"
        exit 1
    fi
    
    # Header
    echo "🏈 MLB Sharp Betting - Pre-Game Workflow Scheduler (Phase 4)"
    echo "============================================================="
    
    # Run startup sequence
    check_dependencies
    check_email_config
    install_dependencies
    start_scheduler
    create_stop_script
}

# Trap to clean up on script exit
trap 'echo ""; log "Script interrupted"' INT TERM

# Run main function
main "$@" 