#!/bin/bash
# Docker Container Recovery Script
# Handles container failures with intelligent recovery strategies

set -e

# Configuration
MAX_RETRIES=3
RETRY_DELAY=10
HEALTH_CHECK_TIMEOUT=120

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING: $1${NC}"
}

error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $1${NC}"
}

success() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] SUCCESS: $1${NC}"
}

# Function to check if a container is healthy
is_container_healthy() {
    local container_name=$1
    local status=$(docker inspect --format='{{.State.Health.Status}}' "$container_name" 2>/dev/null || echo "unhealthy")
    [ "$status" = "healthy" ]
}

# Function to get container status
get_container_status() {
    local container_name=$1
    docker inspect --format='{{.State.Status}}' "$container_name" 2>/dev/null || echo "not_found"
}

# Function to restart a container with recovery strategy
restart_container() {
    local container_name=$1
    local retry_count=0
    
    log "Attempting to restart container: $container_name"
    
    while [ $retry_count -lt $MAX_RETRIES ]; do
        log "Restart attempt $((retry_count + 1))/$MAX_RETRIES for $container_name"
        
        # Stop container gracefully
        if docker ps -q -f name="$container_name" | grep -q .; then
            log "Stopping $container_name gracefully..."
            docker stop "$container_name" || warn "Failed to stop $container_name gracefully"
        fi
        
        # Remove container if it exists
        if docker ps -aq -f name="$container_name" | grep -q .; then
            log "Removing existing $container_name container..."
            docker rm "$container_name" || warn "Failed to remove $container_name"
        fi
        
        # Start container using docker-compose
        log "Starting $container_name using docker-compose..."
        if docker-compose up -d "$container_name"; then
            log "Container $container_name started, waiting for health check..."
            
            # Wait for container to become healthy
            local wait_time=0
            while [ $wait_time -lt $HEALTH_CHECK_TIMEOUT ]; do
                if is_container_healthy "$container_name"; then
                    success "Container $container_name is healthy!"
                    return 0
                fi
                
                sleep 5
                wait_time=$((wait_time + 5))
                log "Waiting for $container_name health check... (${wait_time}s/${HEALTH_CHECK_TIMEOUT}s)"
            done
            
            warn "Container $container_name did not become healthy within ${HEALTH_CHECK_TIMEOUT}s"
        else
            error "Failed to start $container_name with docker-compose"
        fi
        
        retry_count=$((retry_count + 1))
        if [ $retry_count -lt $MAX_RETRIES ]; then
            log "Waiting ${RETRY_DELAY}s before next retry..."
            sleep $RETRY_DELAY
        fi
    done
    
    error "Failed to restart $container_name after $MAX_RETRIES attempts"
    return 1
}

# Function to perform full stack recovery
full_stack_recovery() {
    log "Performing full Docker stack recovery..."
    
    # Stop all containers
    log "Stopping all containers..."
    docker-compose down || warn "Failed to stop some containers"
    
    # Clean up any orphaned containers
    log "Cleaning up orphaned containers..."
    docker system prune -f || warn "Failed to clean up system"
    
    # Start the stack
    log "Starting Docker stack..."
    if docker-compose up -d; then
        success "Docker stack started successfully"
        
        # Run health checks
        log "Running health checks..."
        if ./scripts/test-docker-stack.sh; then
            success "Full stack recovery completed successfully!"
            return 0
        else
            error "Health checks failed after stack recovery"
            return 1
        fi
    else
        error "Failed to start Docker stack"
        return 1
    fi
}

# Function to diagnose container issues
diagnose_container() {
    local container_name=$1
    
    log "Diagnosing container: $container_name"
    
    # Check if container exists
    local status=$(get_container_status "$container_name")
    log "Container status: $status"
    
    if [ "$status" = "not_found" ]; then
        warn "Container $container_name does not exist"
        return 1
    fi
    
    # Get container logs
    log "Recent logs for $container_name:"
    docker logs --tail=20 "$container_name" 2>&1 | sed 's/^/  /'
    
    # Check container inspect
    if [ "$status" != "running" ]; then
        log "Container inspect details:"
        docker inspect "$container_name" --format='{{.State}}' | sed 's/^/  /'
    fi
    
    # Check health status if available
    local health_status=$(docker inspect --format='{{.State.Health.Status}}' "$container_name" 2>/dev/null || echo "no-health-check")
    if [ "$health_status" != "no-health-check" ]; then
        log "Health status: $health_status"
        if [ "$health_status" = "unhealthy" ]; then
            log "Health check logs:"
            docker inspect --format='{{range .State.Health.Log}}{{.Output}}{{end}}' "$container_name" | sed 's/^/  /'
        fi
    fi
}

# Main function
main() {
    local action=${1:-"check"}
    local container=${2:-"all"}
    
    case "$action" in
        "check")
            log "Checking Docker stack health..."
            if ./scripts/test-docker-stack.sh; then
                success "Docker stack is healthy!"
            else
                error "Docker stack health check failed"
                exit 1
            fi
            ;;
        "restart")
            if [ "$container" = "all" ]; then
                full_stack_recovery
            else
                restart_container "$container"
            fi
            ;;
        "diagnose")
            if [ "$container" = "all" ]; then
                for service in redis mlflow fastapi nginx; do
                    diagnose_container "mlb_$service"
                done
            else
                diagnose_container "$container"
            fi
            ;;
        "recover")
            log "Starting intelligent recovery process..."
            
            # First, try to identify failing containers
            failing_containers=()
            for service in redis mlflow fastapi nginx; do
                container_name="mlb_$service"
                if ! is_container_healthy "$container_name"; then
                    warn "Unhealthy container detected: $container_name"
                    failing_containers+=("$service")
                fi
            done
            
            if [ ${#failing_containers[@]} -eq 0 ]; then
                success "All containers are healthy!"
                exit 0
            fi
            
            # Try to restart individual failing containers first
            local individual_recovery_success=true
            for service in "${failing_containers[@]}"; do
                if ! restart_container "$service"; then
                    individual_recovery_success=false
                    break
                fi
            done
            
            # If individual recovery failed, try full stack recovery
            if [ "$individual_recovery_success" = false ]; then
                warn "Individual container recovery failed, attempting full stack recovery..."
                full_stack_recovery
            else
                success "Individual container recovery completed successfully!"
            fi
            ;;
        *)
            echo "Usage: $0 {check|restart|diagnose|recover} [container_name]"
            echo ""
            echo "Actions:"
            echo "  check                     - Run health checks on the entire stack"
            echo "  restart [container|all]   - Restart specific container or entire stack"
            echo "  diagnose [container|all]  - Diagnose container issues"
            echo "  recover                   - Intelligent recovery of failing containers"
            echo ""
            echo "Examples:"
            echo "  $0 check                  - Check stack health"
            echo "  $0 restart redis          - Restart Redis container"
            echo "  $0 restart all            - Restart entire stack"
            echo "  $0 diagnose fastapi       - Diagnose FastAPI container"
            echo "  $0 recover                - Auto-recover failing containers"
            exit 1
            ;;
    esac
}

# Handle script interruption
trap 'echo -e "${YELLOW}\n⚠️  Recovery interrupted${NC}"; exit 130' INT

# Run main function
main "$@"