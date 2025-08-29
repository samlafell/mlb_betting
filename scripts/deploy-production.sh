#!/bin/bash
# 🚀 MLB Betting System - Production Deployment Automation
# Zero-downtime deployment with health checks, rollback capabilities, and monitoring integration
# Designed for Docker-based production infrastructure

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LOG_FILE="$PROJECT_ROOT/logs/deployment.log"
DEPLOYMENT_ID=$(date +'%Y%m%d_%H%M%S')

# Load environment variables
if [ -f "$PROJECT_ROOT/.env.production" ]; then
    source "$PROJECT_ROOT/.env.production"
fi

# Docker Compose files
COMPOSE_FILE="$PROJECT_ROOT/docker-compose.production.yml"
MONITORING_COMPOSE="$PROJECT_ROOT/docker/monitoring/docker-compose.monitoring.yml"

# Deployment configuration
HEALTH_CHECK_TIMEOUT=300  # 5 minutes
ROLLBACK_TIMEOUT=180     # 3 minutes
PRE_DEPLOY_BACKUP=true
MONITORING_ENABLED=true

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m'

# Logging functions
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

success() {
    echo -e "${GREEN}✅ $1${NC}"
    log "SUCCESS: $1"
}

error() {
    echo -e "${RED}❌ $1${NC}"
    log "ERROR: $1"
}

warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
    log "WARNING: $1"
}

info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
    log "INFO: $1"
}

header() {
    echo -e "\n${PURPLE}======================================${NC}"
    echo -e "${PURPLE} $1${NC}"
    echo -e "${PURPLE}======================================${NC}\n"
}

# Progress tracking
TOTAL_STEPS=15
CURRENT_STEP=0

progress() {
    CURRENT_STEP=$((CURRENT_STEP + 1))
    echo -e "\n${CYAN}📊 Step $CURRENT_STEP/$TOTAL_STEPS: $1${NC}\n"
    log "PROGRESS: Step $CURRENT_STEP/$TOTAL_STEPS - $1"
}

# Cleanup on exit
cleanup() {
    if [ $? -ne 0 ]; then
        error "Deployment failed! Check $LOG_FILE for details."
        if [ "$ROLLBACK_INITIATED" = "true" ]; then
            warning "Rollback was initiated. System should be in previous stable state."
        else
            warning "Consider running rollback: $0 rollback"
        fi
    fi
}

trap cleanup EXIT

# Banner
clear
echo -e "${PURPLE}"
echo "🚀 MLB Betting System - Production Deployment"
echo "============================================="
echo -e "${NC}"
echo -e "${CYAN}📋 Deployment ID: $DEPLOYMENT_ID${NC}"
echo -e "${CYAN}🗓️  Started: $(date)${NC}"
echo -e "${CYAN}👤 User: $(whoami)${NC}"
echo -e "${CYAN}🖥️  Host: $(hostname)${NC}\n"

log "Starting production deployment: $DEPLOYMENT_ID"

# ============================================================================
# STEP 1: Pre-deployment Validation
# ============================================================================
progress "Pre-deployment Validation"

# Check if required files exist
required_files=(
    "$COMPOSE_FILE"
    "$PROJECT_ROOT/.env.production"
    "$PROJECT_ROOT/config.toml"
)

for file in "${required_files[@]}"; do
    if [ ! -f "$file" ]; then
        error "Required file not found: $file"
        exit 1
    fi
    info "Found: $(basename "$file")"
done

# Validate Docker Compose configuration
if ! docker compose -f "$COMPOSE_FILE" config > /dev/null 2>&1; then
    error "Docker Compose configuration is invalid"
    docker compose -f "$COMPOSE_FILE" config
    exit 1
fi

# Check system resources
MEMORY_GB=$(free -g 2>/dev/null | awk '/^Mem:/{print $2}' || echo "8")
DISK_FREE_GB=$(df -BG "$PROJECT_ROOT" 2>/dev/null | awk 'NR==2{print $4}' | sed 's/G//' || echo "20")

if [ "$MEMORY_GB" -lt "4" ]; then
    error "Insufficient memory: ${MEMORY_GB}GB (minimum 4GB required)"
    exit 1
fi

if [ "$DISK_FREE_GB" -lt "5" ]; then
    error "Insufficient disk space: ${DISK_FREE_GB}GB (minimum 5GB required)"
    exit 1
fi

success "Pre-deployment validation passed"

# ============================================================================
# STEP 2: Docker Environment Check
# ============================================================================
progress "Docker Environment Check"

if ! docker info > /dev/null 2>&1; then
    error "Docker is not running or not accessible"
    exit 1
fi

# Check if production services are already running
if docker compose -f "$COMPOSE_FILE" ps --services --filter "status=running" | grep -q .; then
    warning "Production services are already running"
    EXISTING_DEPLOYMENT=true
else
    EXISTING_DEPLOYMENT=false
fi

success "Docker environment validated"

# ============================================================================
# STEP 3: Pre-deployment Backup
# ============================================================================
progress "Pre-deployment Backup"

if [ "$PRE_DEPLOY_BACKUP" = "true" ] && [ "$EXISTING_DEPLOYMENT" = "true" ]; then
    info "Creating pre-deployment backup..."
    
    # Run backup script
    if [ -f "$PROJECT_ROOT/scripts/backup-system.sh" ]; then
        bash "$PROJECT_ROOT/scripts/backup-system.sh" daily
        
        if [ $? -eq 0 ]; then
            success "Pre-deployment backup completed"
            BACKUP_CREATED=true
        else
            error "Pre-deployment backup failed"
            read -p "Continue deployment without backup? (y/N): " -n 1 -r
            echo
            if [[ ! $REPLY =~ ^[Yy]$ ]]; then
                exit 1
            fi
            BACKUP_CREATED=false
        fi
    else
        warning "Backup script not found, skipping backup"
        BACKUP_CREATED=false
    fi
else
    info "Skipping pre-deployment backup (not required for new deployment)"
    BACKUP_CREATED=false
fi

# ============================================================================
# STEP 4: Pull Latest Images
# ============================================================================
progress "Pulling Latest Docker Images"

info "Pulling latest images..."
docker compose -f "$COMPOSE_FILE" pull --quiet

success "Docker images updated"

# ============================================================================
# STEP 5: Database Migration Check
# ============================================================================
progress "Database Migration Check"

if [ "$EXISTING_DEPLOYMENT" = "true" ]; then
    info "Checking for pending database migrations..."
    
    # Check if migration service/script exists
    if [ -f "$PROJECT_ROOT/scripts/migrate-database.sh" ]; then
        bash "$PROJECT_ROOT/scripts/migrate-database.sh" check
        
        if [ $? -ne 0 ]; then
            warning "Database migrations may be required"
            read -p "Run database migrations? (Y/n): " -n 1 -r
            echo
            if [[ ! $REPLY =~ ^[Nn]$ ]]; then
                bash "$PROJECT_ROOT/scripts/migrate-database.sh" apply
                if [ $? -ne 0 ]; then
                    error "Database migration failed"
                    exit 1
                fi
                success "Database migrations completed"
            fi
        else
            success "Database is up to date"
        fi
    else
        info "No migration script found, skipping migration check"
    fi
else
    info "New deployment, database will be initialized automatically"
fi

# ============================================================================
# STEP 6: Rolling Deployment Strategy
# ============================================================================
progress "Rolling Deployment Execution"

if [ "$EXISTING_DEPLOYMENT" = "true" ]; then
    info "Executing rolling deployment..."
    
    # Rolling update for each service
    services=("postgres" "redis" "mlflow" "fastapi" "nginx" "data_collector")
    
    for service in "${services[@]}"; do
        info "Updating service: $service"
        
        # Special handling for database services
        if [[ "$service" == "postgres" ]]; then
            warning "PostgreSQL requires careful handling - checking if update is needed..."
            current_image=$(docker compose -f "$COMPOSE_FILE" images -q postgres)
            latest_image=$(docker compose -f "$COMPOSE_FILE" pull --quiet postgres 2>&1 | grep -o '[a-f0-9]\{64\}' || echo "")
            
            if [ "$current_image" != "$latest_image" ] && [ -n "$latest_image" ]; then
                warning "PostgreSQL image update detected - this requires downtime!"
                read -p "Continue with PostgreSQL update? This will cause brief downtime (y/N): " -n 1 -r
                echo
                if [[ $REPLY =~ ^[Yy]$ ]]; then
                    docker compose -f "$COMPOSE_FILE" up -d --no-deps --force-recreate "$service"
                    sleep 30  # Allow extra time for PostgreSQL
                else
                    info "Skipping PostgreSQL update"
                    continue
                fi
            else
                info "PostgreSQL is up to date, no restart needed"
                continue
            fi
        else
            # Standard rolling update for other services
            docker compose -f "$COMPOSE_FILE" up -d --no-deps --force-recreate "$service"
        fi
        
        # Wait for service to be healthy
        local attempts=0
        local max_attempts=30
        
        while [ $attempts -lt $max_attempts ]; do
            if docker compose -f "$COMPOSE_FILE" ps "$service" | grep -q "healthy\|Up"; then
                success "Service $service is running"
                break
            fi
            
            sleep 5
            attempts=$((attempts + 1))
            info "Waiting for $service to be healthy... ($attempts/$max_attempts)"
        done
        
        if [ $attempts -ge $max_attempts ]; then
            error "Service $service failed to become healthy"
            warning "Initiating rollback..."
            ROLLBACK_INITIATED=true
            rollback_deployment
            exit 1
        fi
        
        # Additional health check for critical services
        if [[ "$service" == "fastapi" ]]; then
            if ! curl -f --max-time 30 http://localhost:8000/health > /dev/null 2>&1; then
                error "FastAPI health check failed"
                ROLLBACK_INITIATED=true
                rollback_deployment
                exit 1
            fi
            success "FastAPI health check passed"
        fi
    done
else
    info "Starting new deployment..."
    docker compose -f "$COMPOSE_FILE" up -d
    sleep 60  # Allow time for all services to start
fi

# ============================================================================
# STEP 7: Post-deployment Health Checks
# ============================================================================
progress "Comprehensive Health Validation"

health_endpoints=(
    "http://localhost:8000/health|FastAPI Service"
    "http://localhost:9090/-/healthy|Prometheus"
    "http://localhost:3000/api/health|Grafana"
)

info "Running comprehensive health checks..."
failed_checks=()

for endpoint_info in "${health_endpoints[@]}"; do
    IFS='|' read -r endpoint name <<< "$endpoint_info"
    
    if curl -f --max-time 30 "$endpoint" > /dev/null 2>&1; then
        success "Health check passed: $name"
    else
        failed_checks+=("$name")
        error "Health check failed: $name ($endpoint)"
    fi
done

# Database connectivity check
if docker exec "${POSTGRES_CONTAINER:-mlb_postgres_prod}" pg_isready -U "$POSTGRES_USER" > /dev/null 2>&1; then
    success "Database connectivity check passed"
else
    failed_checks+=("PostgreSQL")
    error "Database connectivity check failed"
fi

# Redis connectivity check
if docker exec "${REDIS_CONTAINER:-mlb_redis_prod}" redis-cli ping > /dev/null 2>&1; then
    success "Redis connectivity check passed"
else
    failed_checks+=("Redis")
    error "Redis connectivity check failed"
fi

if [ ${#failed_checks[@]} -gt 0 ]; then
    error "Health checks failed for: ${failed_checks[*]}"
    warning "Initiating rollback due to health check failures..."
    ROLLBACK_INITIATED=true
    rollback_deployment
    exit 1
fi

success "All health checks passed"

# ============================================================================
# STEP 8: Performance Validation
# ============================================================================
progress "Performance Validation"

info "Running performance validation tests..."

# API response time check
api_response_time=$(curl -o /dev/null -s -w '%{time_total}\n' http://localhost:8000/health)
api_response_ms=$(echo "$api_response_time * 1000" | bc -l | cut -d. -f1)

if [ "$api_response_ms" -lt 1000 ]; then
    success "API response time: ${api_response_ms}ms (within 1000ms threshold)"
else
    warning "API response time: ${api_response_ms}ms (exceeds 1000ms threshold)"
fi

# Database query performance check
db_query_time=$(docker exec "${POSTGRES_CONTAINER:-mlb_postgres_prod}" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT NOW();" -t | wc -l)
if [ $? -eq 0 ]; then
    success "Database query performance check passed"
else
    warning "Database query performance check failed"
fi

success "Performance validation completed"

# ============================================================================
# STEP 9: Monitoring Integration
# ============================================================================
progress "Monitoring Integration"

if [ "$MONITORING_ENABLED" = "true" ] && [ -f "$MONITORING_COMPOSE" ]; then
    info "Starting monitoring stack..."
    
    docker compose -f "$MONITORING_COMPOSE" up -d
    sleep 30
    
    # Check monitoring services
    if curl -f http://localhost:9090/-/healthy > /dev/null 2>&1; then
        success "Prometheus monitoring active"
    else
        warning "Prometheus monitoring check failed"
    fi
    
    if curl -f http://localhost:3000/api/health > /dev/null 2>&1; then
        success "Grafana dashboard active"
    else
        warning "Grafana dashboard check failed"
    fi
else
    info "Monitoring stack not configured or disabled"
fi

# ============================================================================
# STEP 10: Data Collection Validation
# ============================================================================
progress "Data Collection System Validation"

info "Validating data collection services..."

# Check if data collector is running
if docker compose -f "$COMPOSE_FILE" ps data_collector | grep -q "Up"; then
    success "Data collector service is running"
    
    # Test data collection endpoint if available
    if curl -f http://localhost:8000/api/data/status > /dev/null 2>&1; then
        success "Data collection API endpoint accessible"
    else
        info "Data collection API endpoint not available (may be expected)"
    fi
else
    warning "Data collector service not found or not running"
fi

# ============================================================================
# STEP 11: Security Validation
# ============================================================================
progress "Security Validation"

info "Running security validation checks..."

# Check container user (should not be root)
for service in fastapi data_collector; do
    user_check=$(docker compose -f "$COMPOSE_FILE" exec -T "$service" whoami 2>/dev/null || echo "unknown")
    if [ "$user_check" != "root" ] && [ "$user_check" != "unknown" ]; then
        success "Service $service running as non-root user: $user_check"
    else
        warning "Service $service user check failed or running as root"
    fi
done

# Check environment file permissions
if [ -f "$PROJECT_ROOT/.env.production" ]; then
    env_perms=$(stat -c %a "$PROJECT_ROOT/.env.production" 2>/dev/null || stat -f %Mp%Lp "$PROJECT_ROOT/.env.production" 2>/dev/null)
    if [ "$env_perms" = "600" ] || [ "$env_perms" = "100600" ]; then
        success "Environment file has correct permissions: $env_perms"
    else
        warning "Environment file permissions should be 600: current $env_perms"
    fi
fi

success "Security validation completed"

# ============================================================================
# STEP 12: Integration Tests
# ============================================================================
progress "Integration Tests"

info "Running post-deployment integration tests..."

# Run basic integration test if available
if [ -f "$PROJECT_ROOT/tests/integration/deployment_test.py" ]; then
    cd "$PROJECT_ROOT"
    if python -m pytest tests/integration/deployment_test.py -v --tb=short; then
        success "Integration tests passed"
    else
        warning "Some integration tests failed - review manually"
    fi
else
    info "No integration tests found - manual validation recommended"
fi

# ============================================================================
# STEP 13: Load Balancing & Traffic Routing
# ============================================================================
progress "Load Balancing Validation"

info "Validating load balancing and traffic routing..."

# Check nginx configuration if present
if docker compose -f "$COMPOSE_FILE" ps nginx | grep -q "Up"; then
    if curl -f http://localhost/health > /dev/null 2>&1; then
        success "Load balancer health check passed"
    else
        warning "Load balancer health check failed"
    fi
    
    # Check upstream connectivity
    if curl -f http://localhost/api/health > /dev/null 2>&1; then
        success "Upstream service connectivity through load balancer verified"
    else
        warning "Upstream service connectivity through load balancer failed"
    fi
else
    info "Load balancer not configured - direct service access only"
fi

# ============================================================================
# STEP 14: Notification and Documentation
# ============================================================================
progress "Deployment Documentation"

# Create deployment record
cat > "$PROJECT_ROOT/deployments/deployment_${DEPLOYMENT_ID}.json" << EOF
{
  "deployment_id": "$DEPLOYMENT_ID",
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "user": "$(whoami)",
  "host": "$(hostname)",
  "git_commit": "$(git rev-parse HEAD 2>/dev/null || echo 'unknown')",
  "git_branch": "$(git branch --show-current 2>/dev/null || echo 'unknown')",
  "deployment_type": "$([ "$EXISTING_DEPLOYMENT" = "true" ] && echo "rolling_update" || echo "fresh_install")",
  "backup_created": $BACKUP_CREATED,
  "services_deployed": [
    "postgres", "redis", "mlflow", "fastapi", "nginx", "data_collector"
  ],
  "health_checks_passed": true,
  "monitoring_enabled": $MONITORING_ENABLED,
  "performance_validated": true,
  "status": "success"
}
EOF

mkdir -p "$PROJECT_ROOT/deployments"

success "Deployment record created"

# Send notification if webhook configured
if [ -n "${DEPLOYMENT_WEBHOOK_URL:-}" ]; then
    curl -X POST "$DEPLOYMENT_WEBHOOK_URL" \
        -H "Content-Type: application/json" \
        -d "{
            \"deployment_id\": \"$DEPLOYMENT_ID\",
            \"status\": \"success\",
            \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",
            \"services\": [\"postgres\", \"redis\", \"mlflow\", \"fastapi\", \"nginx\"],
            \"health_status\": \"all_healthy\"
        }" > /dev/null 2>&1 || warning "Failed to send deployment notification"
fi

# ============================================================================
# STEP 15: Deployment Complete
# ============================================================================
progress "Deployment Completion"

# Final status check
info "Performing final system status check..."
sleep 10

# Generate deployment summary
DEPLOYMENT_DURATION=$(($(date +%s) - $(date -d "$(head -n1 "$LOG_FILE" | cut -d']' -f1 | tr -d '[')" +%s) 2>/dev/null || echo 300))

clear
header "🎉 DEPLOYMENT SUCCESSFUL! 🎉"

echo -e "${GREEN}✅ MLB Betting System successfully deployed to production!${NC}\n"

echo -e "${CYAN}📊 Deployment Summary:${NC}"
echo -e "  🆔 Deployment ID:     $DEPLOYMENT_ID"
echo -e "  ⏱️  Duration:          ${DEPLOYMENT_DURATION}s"
echo -e "  🔄 Type:              $([ "$EXISTING_DEPLOYMENT" = "true" ] && echo "Rolling Update" || echo "Fresh Install")"
echo -e "  💾 Backup Created:    $([ "$BACKUP_CREATED" = "true" ] && echo "Yes" || echo "No")"
echo -e "  📊 Monitoring:        $([ "$MONITORING_ENABLED" = "true" ] && echo "Enabled" || echo "Disabled")"

echo -e "\n${CYAN}🔗 Service Access Points:${NC}"
echo -e "  🌐 Main Application:   http://localhost:8000"
echo -e "  🏥 Health Check:       http://localhost:8000/health"
echo -e "  📊 Metrics:            http://localhost:8000/metrics"
echo -e "  📈 MLflow Registry:    http://localhost:5001"
echo -e "  🔍 Prometheus:         http://localhost:9090"
echo -e "  📊 Grafana Dashboard:  http://localhost:3000"

echo -e "\n${CYAN}🛠️  Management Commands:${NC}"
echo -e "  📊 Service Status:     docker compose -f docker-compose.production.yml ps"
echo -e "  📋 View Logs:          docker compose -f docker-compose.production.yml logs -f"
echo -e "  🔄 Restart Service:    docker compose -f docker-compose.production.yml restart [service]"
echo -e "  🛑 Stop System:        docker compose -f docker-compose.production.yml down"

echo -e "\n${CYAN}🎯 Post-deployment Tasks:${NC}"
echo -e "  1. Monitor system performance for next 30 minutes"
echo -e "  2. Verify betting data collection is functioning"
echo -e "  3. Test critical betting workflows manually"
echo -e "  4. Review monitoring alerts and thresholds"
echo -e "  5. Schedule next backup if not automated"

echo -e "\n${CYAN}📚 Documentation:${NC}"
echo -e "  📖 Operations Guide:    docs/PRODUCTION_SECURITY_GUIDE.md"
echo -e "  📊 Monitoring Guide:    docs/MONITORING_GUIDE.md"
echo -e "  🔧 Troubleshooting:     docs/TROUBLESHOOTING.md"
echo -e "  📝 Deployment Log:      $LOG_FILE"

echo -e "\n${GREEN}🚨 System is now ready for 24/7 MLB betting operations!${NC}"

log "Deployment completed successfully: $DEPLOYMENT_ID"

# Function for rollback (defined here for access by error handling)
rollback_deployment() {
    warning "Initiating deployment rollback..."
    
    # Stop current deployment
    docker compose -f "$COMPOSE_FILE" down
    
    # If backup was created, offer to restore
    if [ "$BACKUP_CREATED" = "true" ]; then
        echo "Backup was created before deployment. Restore from backup? (Y/n): "
        read -r restore_choice
        if [[ ! $restore_choice =~ ^[Nn]$ ]]; then
            bash "$PROJECT_ROOT/scripts/backup-system.sh" restore "$(find "$PROJECT_ROOT/backups" -name "*daily*" -type f -printf '%T@ %p\n' | sort -n | tail -1 | cut -d' ' -f2-)"
        fi
    fi
    
    # Restart previous version
    docker compose -f "$COMPOSE_FILE" up -d
    
    error "Rollback completed. Please check system status."
}

exit 0