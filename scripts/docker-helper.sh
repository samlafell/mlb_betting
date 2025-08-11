#!/bin/bash
# Docker Helper Script for MLB Betting Program
# Provides convenient commands for common Docker operations

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Service mappings for convenience
declare -A SERVICE_ALIASES=(
    ["mlflow-api"]="fastapi"
    ["ml-api"]="fastapi"
    ["api"]="fastapi"
    ["web"]="nginx"
    ["proxy"]="nginx"
    ["cache"]="redis"
    ["ml"]="mlflow"
    ["tracking"]="mlflow"
)

# Resolve service name
resolve_service() {
    local service="$1"
    if [[ -n "${SERVICE_ALIASES[$service]}" ]]; then
        echo "${SERVICE_ALIASES[$service]}"
    else
        echo "$service"
    fi
}

# Display available services
show_services() {
    log_info "Available services:"
    echo "  • fastapi     - ML Prediction API (aliases: mlflow-api, ml-api, api)"
    echo "  • mlflow      - Experiment tracking (aliases: ml, tracking)"
    echo "  • redis       - Feature cache (aliases: cache)"
    echo "  • nginx       - Reverse proxy (aliases: web, proxy)"
}

# Main script logic
case "${1:-help}" in
    "build")
        service=$(resolve_service "${2:-all}")
        if [[ "$service" == "all" ]]; then
            log_info "Building all services..."
            docker-compose build
        else
            log_info "Building service: $service"
            docker-compose build "$service"
        fi
        log_success "Build completed for $service"
        ;;
    
    "up")
        service=$(resolve_service "${2:-all}")
        if [[ "$service" == "all" ]]; then
            log_info "Starting all services..."
            docker-compose up -d
        else
            log_info "Starting service: $service"
            docker-compose up -d "$service"
        fi
        log_success "Service(s) started successfully"
        ;;
    
    "down")
        log_info "Stopping all services..."
        docker-compose down
        log_success "All services stopped"
        ;;
    
    "logs")
        service=$(resolve_service "${2:-all}")
        if [[ "$service" == "all" ]]; then
            log_info "Showing logs for all services..."
            docker-compose logs -f
        else
            log_info "Showing logs for service: $service"
            docker-compose logs -f "$service"
        fi
        ;;
    
    "status")
        log_info "Service status:"
        docker-compose ps
        ;;
    
    "health")
        log_info "Checking service health..."
        docker-compose ps --format table
        echo ""
        log_info "Health check details:"
        docker-compose exec fastapi curl -f http://localhost:8000/health 2>/dev/null && \
            log_success "FastAPI service is healthy" || \
            log_error "FastAPI service is unhealthy"
        ;;
    
    "shell")
        service=$(resolve_service "${2:-fastapi}")
        log_info "Opening shell in service: $service"
        docker-compose exec "$service" /bin/bash
        ;;
    
    "rebuild")
        service=$(resolve_service "${2:-fastapi}")
        log_info "Rebuilding and restarting service: $service"
        docker-compose stop "$service"
        docker-compose build "$service"
        docker-compose up -d "$service"
        log_success "Service $service rebuilt and restarted"
        ;;
    
    "clean")
        log_warning "This will remove all containers, volumes, and networks"
        read -p "Are you sure? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            docker-compose down -v --remove-orphans
            docker system prune -f
            log_success "Docker environment cleaned"
        else
            log_info "Operation cancelled"
        fi
        ;;
    
    "help"|*)
        echo "MLB Betting Program - Docker Helper"
        echo ""
        echo "Usage: $0 <command> [service]"
        echo ""
        echo "Commands:"
        echo "  build [service]    - Build Docker images"
        echo "  up [service]       - Start services"
        echo "  down              - Stop all services"
        echo "  logs [service]    - View service logs"
        echo "  status            - Show service status"
        echo "  health            - Check service health"
        echo "  shell [service]   - Open shell in service"
        echo "  rebuild [service] - Rebuild and restart service"
        echo "  clean             - Clean up all Docker resources"
        echo "  help              - Show this help"
        echo ""
        show_services
        echo ""
        echo "Examples:"
        echo "  $0 build fastapi              # Build FastAPI service"
        echo "  $0 build mlflow-api           # Same as above (alias)"
        echo "  $0 up                         # Start all services"
        echo "  $0 logs fastapi               # View FastAPI logs"
        echo "  $0 rebuild ml-api             # Rebuild ML API service"
        ;;
esac