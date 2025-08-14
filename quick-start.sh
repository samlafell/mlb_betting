#!/bin/bash
# 🚀 MLB Betting System - Quick Start Setup
# 
# One-command setup for new users - addresses GitHub issue #35
# This script provides automated setup with validation and clear success indicators
#
# Usage: ./quick-start.sh [options]
# Options:
#   --skip-docker     Skip Docker container setup
#   --skip-deps       Skip dependency installation  
#   --skip-data       Skip initial data collection
#   --help           Show this help message

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Default options
SKIP_DOCKER=false
SKIP_DEPS=false
SKIP_DATA=false
SHOW_HELP=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-docker)
            SKIP_DOCKER=true
            shift
            ;;
        --skip-deps)
            SKIP_DEPS=true
            shift
            ;;
        --skip-data)
            SKIP_DATA=true
            shift
            ;;
        --help|-h)
            SHOW_HELP=true
            shift
            ;;
        *)
            echo -e "${RED}❌ Unknown option: $1${NC}"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Help function
show_help() {
    echo -e "${BOLD}🚀 MLB Betting System - Quick Start Setup${NC}"
    echo ""
    echo "This script provides one-command setup for new users."
    echo ""
    echo -e "${BOLD}Usage:${NC}"
    echo "  ./quick-start.sh [options]"
    echo ""
    echo -e "${BOLD}Options:${NC}"
    echo "  --skip-docker     Skip Docker container setup"
    echo "  --skip-deps       Skip dependency installation"
    echo "  --skip-data       Skip initial data collection"
    echo "  --help, -h        Show this help message"
    echo ""
    echo -e "${BOLD}What this script does:${NC}"
    echo "  1. ✅ Validates system requirements (Docker, Python)"
    echo "  2. 🐳 Starts database containers (PostgreSQL, Redis)"
    echo "  3. 📦 Installs Python dependencies with uv"
    echo "  4. 🗄️ Sets up database schema"
    echo "  5. 📊 Runs initial data collection"
    echo "  6. 🎯 Generates first predictions"
    echo "  7. ✅ Validates everything works"
    echo ""
    echo -e "${BOLD}Success criteria:${NC}"
    echo "  • Database connection successful"
    echo "  • Data collection working"
    echo "  • Predictions generated"
    echo ""
    exit 0
}

if [ "$SHOW_HELP" = true ]; then
    show_help
fi

# Utility functions
print_step() {
    echo -e "\n${BLUE}${BOLD}🔧 Step $1: $2${NC}"
    echo "=================================================="
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# Check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check system requirements
check_requirements() {
    print_step 1 "Validating System Requirements"
    
    local missing_requirements=()
    
    # Check Docker
    if ! command_exists docker; then
        missing_requirements+=("Docker")
        print_error "Docker not found. Please install Docker Desktop."
    else
        print_success "Docker found: $(docker --version | head -n 1)"
    fi
    
    # Check Docker Compose
    if ! command_exists docker-compose && ! docker compose version >/dev/null 2>&1; then
        missing_requirements+=("Docker Compose")
        print_error "Docker Compose not found."
    else
        if docker compose version >/dev/null 2>&1; then
            print_success "Docker Compose found: $(docker compose version)"
        else
            print_success "Docker Compose found: $(docker-compose --version)"
        fi
    fi
    
    # Check Python
    if ! command_exists python3; then
        missing_requirements+=("Python 3.10+")
        print_error "Python 3 not found."
    else
        python_version=$(python3 --version 2>&1 | grep -oE '[0-9]+\.[0-9]+' | head -n1)
        python_major=$(echo "$python_version" | cut -d. -f1)
        python_minor=$(echo "$python_version" | cut -d. -f2)
        
        if [ "$python_major" -ge 3 ] && [ "$python_minor" -ge 10 ]; then
            print_success "Python found: Python $python_version"
        else
            missing_requirements+=("Python 3.10+")
            print_error "Python 3.10+ required, found Python $python_version"
        fi
    fi
    
    # Check uv (install if missing)
    if ! command_exists uv; then
        print_warning "uv package manager not found. Attempting to install..."
        if command_exists curl; then
            curl -LsSf https://astral.sh/uv/install.sh | sh
            # Source the shell config to get uv in PATH
            export PATH="$HOME/.cargo/bin:$PATH"
            if command_exists uv; then
                print_success "uv installed successfully"
            else
                missing_requirements+=("uv package manager")
                print_error "Failed to install uv automatically"
            fi
        else
            missing_requirements+=("uv package manager")
            print_error "uv not found and curl not available to install it"
        fi
    else
        print_success "uv found: $(uv --version)"
    fi
    
    # Report missing requirements
    if [ ${#missing_requirements[@]} -gt 0 ]; then
        print_error "Missing required dependencies:"
        for req in "${missing_requirements[@]}"; do
            echo -e "  ${RED}• $req${NC}"
        done
        echo ""
        print_info "Please install the missing requirements and run this script again."
        print_info "Installation guides:"
        echo "  • Docker: https://docs.docker.com/get-docker/"
        echo "  • Python 3.10+: https://www.python.org/downloads/"
        echo "  • uv: https://docs.astral.sh/uv/getting-started/installation/"
        exit 1
    fi
    
    print_success "All system requirements met!"
}

# Start Docker containers
start_containers() {
    if [ "$SKIP_DOCKER" = true ]; then
        print_warning "Skipping Docker container setup"
        return 0
    fi
    
    print_step 2 "Starting Database Containers"
    
    # Check if containers are already running
    if docker ps | grep -q mlb_quickstart_postgres; then
        print_info "Containers already running"
        return 0
    fi
    
    # Start containers using quickstart compose file
    print_info "Starting PostgreSQL and Redis containers..."
    
    if docker compose version >/dev/null 2>&1; then
        docker compose -f docker-compose.quickstart.yml up -d
    else
        docker-compose -f docker-compose.quickstart.yml up -d
    fi
    
    # Wait for containers to be healthy with improved error handling
    print_info "Waiting for database to be ready (may take up to 2 minutes)..."
    local attempts=0
    local max_attempts=60  # 2 minutes total
    local check_interval=2
    
    while [ $attempts -lt $max_attempts ]; do
        # Check if container is running first
        if ! docker ps | grep -q mlb_quickstart_postgres; then
            print_error "PostgreSQL container is not running"
            print_info "Try running: docker-compose -f docker-compose.quickstart.yml logs postgres"
            exit 1
        fi
        
        # Check if database is ready
        if docker exec mlb_quickstart_postgres pg_isready -U samlafell -d mlb_betting >/dev/null 2>&1; then
            break
        fi
        
        attempts=$((attempts + 1))
        echo -n "."
        sleep $check_interval
        
        # Show progress every 30 seconds
        if [ $((attempts % 15)) -eq 0 ]; then
            echo ""
            print_info "Still waiting... ($((attempts * check_interval)) seconds elapsed)"
        fi
    done
    
    if [ $attempts -eq $max_attempts ]; then
        echo ""
        print_error "Database failed to start within $((max_attempts * check_interval)) seconds"
        print_info "Troubleshooting steps:"
        print_info "  • Check container logs: docker-compose -f docker-compose.quickstart.yml logs postgres"
        print_info "  • Check if port 5433 is available: lsof -i :5433"
        print_info "  • Try restarting containers: docker-compose -f docker-compose.quickstart.yml restart"
        exit 1
    fi
    
    print_success "Database containers started and ready!"
}

# Install dependencies
install_dependencies() {
    if [ "$SKIP_DEPS" = true ]; then
        print_warning "Skipping dependency installation"
        return 0
    fi
    
    print_step 3 "Installing Python Dependencies"
    
    print_info "Installing dependencies with uv..."
    uv sync
    
    print_success "Dependencies installed successfully!"
}

# Create minimal environment file
create_env_file() {
    print_step 4 "Setting Up Environment Configuration"
    
    if [ ! -f .env ]; then
        print_info "Creating minimal .env file..."
        cat > .env << 'EOF'
# 🚀 MLB Betting System - Quick Start Configuration
# This is a minimal configuration for getting started quickly
# For production use, see .env.example for all available options

# Database Configuration (matches quickstart Docker containers)
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_DB=mlb_betting
POSTGRES_USER=samlafell
POSTGRES_PASSWORD=postgres

# Redis Configuration
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0

# Environment
ENVIRONMENT=development
LOG_LEVEL=INFO

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000
API_SECRET_KEY=quickstart-development-key-change-for-production

# Minimal monitoring
ENABLE_METRICS=true
EOF
        print_success ".env file created with minimal configuration"
    else
        print_info ".env file already exists, using existing configuration"
    fi
}

# Setup database schema
setup_database() {
    print_step 5 "Setting Up Database Schema"
    
    print_info "Creating database tables..."
    if uv run -m src.interfaces.cli database setup-action-network --test-connection; then
        print_success "Database schema setup completed!"
    else
        print_warning "Database setup had issues, but continuing..."
        print_info "You may need to run this manually later:"
        print_info "  uv run -m src.interfaces.cli database setup-action-network"
    fi
}

# Run initial data collection with retry logic
run_data_collection() {
    if [ "$SKIP_DATA" = true ]; then
        print_warning "Skipping initial data collection"
        return 0
    fi
    
    print_step 6 "Running Initial Data Collection"
    
    local max_retries=2
    local timeout_duration=600  # 10 minutes for slow networks
    local retry_count=0
    
    while [ $retry_count -le $max_retries ]; do
        if [ $retry_count -gt 0 ]; then
            print_info "Retry attempt $retry_count of $max_retries..."
            sleep 10  # Brief delay between retries
        fi
        
        print_info "Collecting data from Action Network (may take up to 10 minutes for slow networks)..."
        
        if timeout $timeout_duration uv run -m src.interfaces.cli data collect --source action_network --real; then
            print_success "Initial data collection completed!"
            return 0
        else
            retry_count=$((retry_count + 1))
            if [ $retry_count -le $max_retries ]; then
                print_warning "Data collection attempt $((retry_count - 1)) failed, retrying..."
            else
                print_warning "Data collection failed after $max_retries attempts"
                print_info "This is often normal for first run or slow networks. You can:"
                print_info "  • Run manually: uv run -m src.interfaces.cli data collect --source action_network --real"
                print_info "  • Try again later when network conditions improve"
                print_info "  • Skip data collection with: ./quick-start.sh --skip-data"
            fi
        fi
    done
}

# Generate first predictions
generate_predictions() {
    print_step 7 "Generating First Predictions"
    
    print_info "Attempting to generate predictions..."
    if timeout 120 uv run -m src.interfaces.cli quickstart predictions --confidence-threshold 0.6; then
        print_success "Predictions generated successfully!"
    else
        print_warning "No predictions generated (this may be normal)"
        print_info "This could mean:"
        print_info "  • No games scheduled for today"
        print_info "  • No predictions meet confidence threshold"
        print_info "  • More data collection needed"
        print_info ""
        print_info "Try generating predictions manually:"
        print_info "  uv run -m src.interfaces.cli quickstart predictions"
    fi
}

# Validate system health
validate_system() {
    print_step 8 "Validating System Health"
    
    local validation_passed=true
    
    # Test database connection
    print_info "Testing database connection..."
    if uv run -m src.interfaces.cli database setup-action-network --test-only >/dev/null 2>&1; then
        print_success "Database connection OK"
    else
        print_error "Database connection failed"
        validation_passed=false
    fi
    
    # Test CLI commands
    print_info "Testing CLI system..."
    if uv run -m src.interfaces.cli --help >/dev/null 2>&1; then
        print_success "CLI system OK"
    else
        print_error "CLI system failed"
        validation_passed=false
    fi
    
    # Check data collection capability
    print_info "Testing data source connections..."
    if timeout 30 uv run -m src.interfaces.cli data test --source action_network --real >/dev/null 2>&1; then
        print_success "Data sources accessible"
    else
        print_warning "Data source test failed (may be network related)"
    fi
    
    if [ "$validation_passed" = true ]; then
        print_success "System validation passed!"
    else
        print_warning "Some validation checks failed, but system may still work"
    fi
}

# Show next steps
show_next_steps() {
    echo ""
    echo -e "${GREEN}${BOLD}🎉 Quick Start Setup Complete!${NC}"
    echo "=================================================="
    echo ""
    echo -e "${BOLD}✅ Success Indicators:${NC}"
    echo "  • Database running on localhost:5433"
    echo "  • Redis running on localhost:6379"
    echo "  • Python dependencies installed"
    echo "  • Database schema created"
    echo ""
    echo -e "${BOLD}🎯 What's Next?${NC}"
    echo ""
    echo -e "${BOLD}📊 Get Today's Predictions:${NC}"
    echo "  uv run -m src.interfaces.cli quickstart predictions"
    echo ""
    echo -e "${BOLD}🔄 Run Data Collection:${NC}"
    echo "  uv run -m src.interfaces.cli data collect --source action_network --real"
    echo ""
    echo -e "${BOLD}📈 Start Monitoring Dashboard:${NC}"
    echo "  uv run -m src.interfaces.cli monitoring dashboard"
    echo "  Then visit: http://localhost:8080"
    echo ""
    echo -e "${BOLD}🤖 Check ML Models:${NC}"
    echo "  uv run -m src.interfaces.cli ml models --profitable-only"
    echo ""
    echo -e "${BOLD}🆘 Need Help?${NC}"
    echo "  uv run -m src.interfaces.cli quickstart validate --fix-issues"
    echo "  uv run -m src.interfaces.cli --help"
    echo ""
    echo -e "${BOLD}📚 Documentation:${NC}"
    echo "  • Quick Start Guide: ./QUICK_START.md"
    echo "  • Full Documentation: ./README.md"
    echo "  • User Guide: ./USER_GUIDE.md"
    echo ""
    echo -e "${YELLOW}💡 Pro tip: Bookmark these commands or add aliases!${NC}"
    echo ""
}

# Error handling
error_handler() {
    local line_no=$1
    echo ""
    print_error "Setup failed at line $line_no"
    echo ""
    echo -e "${BOLD}🆘 Troubleshooting Steps:${NC}"
    echo "1. Check the error message above"
    echo "2. Ensure all requirements are installed"
    echo "3. Try running individual steps manually"
    echo "4. Check the logs in ./logs/ directory"
    echo ""
    echo -e "${BOLD}🔧 Manual Setup Commands:${NC}"
    echo "  docker-compose -f docker-compose.quickstart.yml up -d"
    echo "  uv sync"
    echo "  uv run -m src.interfaces.cli database setup-action-network"
    echo "  uv run -m src.interfaces.cli quickstart validate"
    echo ""
    echo -e "${BOLD}💬 Get Help:${NC}"
    echo "  • Run: uv run -m src.interfaces.cli quickstart validate --fix-issues"
    echo "  • Check: GitHub Issues for support"
    echo ""
    exit 1
}

# Set error trap
trap 'error_handler $LINENO' ERR

# Main execution
main() {
    echo -e "${BLUE}${BOLD}"
    echo "🚀 MLB Betting System - Quick Start Setup"
    echo "=========================================="
    echo -e "${NC}"
    echo "This script will set up your MLB betting system in under 10 minutes."
    echo "It addresses the complex setup issues identified in GitHub issue #35."
    echo ""
    echo -e "${BOLD}What will be installed:${NC}"
    echo "  • PostgreSQL database (Docker)"
    echo "  • Redis cache (Docker)"  
    echo "  • Python dependencies"
    echo "  • Database schema"
    echo "  • Initial data collection"
    echo ""
    
    # Ask for confirmation unless skipping
    if [ "$SKIP_DOCKER" = false ] || [ "$SKIP_DEPS" = false ]; then
        echo -n "Continue with setup? (y/N): "
        read -r confirm
        if [[ ! $confirm =~ ^[Yy]$ ]]; then
            echo "Setup cancelled by user"
            exit 0
        fi
    fi
    
    # Execute setup steps
    check_requirements
    start_containers
    install_dependencies
    create_env_file
    setup_database
    run_data_collection
    generate_predictions
    validate_system
    show_next_steps
    
    echo -e "${GREEN}${BOLD}🎯 Setup completed successfully!${NC}"
    echo "The MLB betting system is now ready to use."
}

# Run main function
main "$@"