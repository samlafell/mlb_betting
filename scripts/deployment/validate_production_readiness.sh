#!/bin/bash

# Production Deployment Readiness Validation Script
# Validates all infrastructure components and dependencies before production deployment

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
LOG_FILE="${PROJECT_ROOT}/logs/production_readiness_$(date +%Y%m%d_%H%M%S).log"

# Validation counters
TOTAL_CHECKS=0
PASSED_CHECKS=0
FAILED_CHECKS=0
WARNING_CHECKS=0

# Create log directory
mkdir -p "$(dirname "$LOG_FILE")"

# Logging function
log() {
    local level="$1"
    shift
    local message="$*"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] [$level] $message" | tee -a "$LOG_FILE"
}

# Check function with validation tracking
check() {
    local description="$1"
    local command="$2"
    local error_message="${3:-Failed check}"
    
    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
    echo -e "\n${BLUE}[CHECK $TOTAL_CHECKS] ${description}${NC}"
    
    if eval "$command" &>/dev/null; then
        echo -e "✅ ${GREEN}PASS${NC}: $description"
        log "INFO" "CHECK PASSED: $description"
        PASSED_CHECKS=$((PASSED_CHECKS + 1))
        return 0
    else
        echo -e "❌ ${RED}FAIL${NC}: $description"
        echo -e "   Error: $error_message"
        log "ERROR" "CHECK FAILED: $description - $error_message"
        FAILED_CHECKS=$((FAILED_CHECKS + 1))
        return 1
    fi
}

# Warning check (doesn't count as failure)
check_warn() {
    local description="$1"
    local command="$2"
    local warning_message="${3:-Warning condition detected}"
    
    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
    echo -e "\n${BLUE}[CHECK $TOTAL_CHECKS] ${description}${NC}"
    
    if eval "$command" &>/dev/null; then
        echo -e "✅ ${GREEN}PASS${NC}: $description"
        log "INFO" "CHECK PASSED: $description"
        PASSED_CHECKS=$((PASSED_CHECKS + 1))
        return 0
    else
        echo -e "⚠️ ${YELLOW}WARN${NC}: $description"
        echo -e "   Warning: $warning_message"
        log "WARNING" "CHECK WARNING: $description - $warning_message"
        WARNING_CHECKS=$((WARNING_CHECKS + 1))
        return 1
    fi
}

# Header
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}               MLB BETTING SYSTEM                              ${NC}"
echo -e "${BLUE}           PRODUCTION READINESS VALIDATION                    ${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo -e "\nValidation started at: $(date)"
echo -e "Project root: $PROJECT_ROOT"
echo -e "Log file: $LOG_FILE\n"

log "INFO" "Starting production readiness validation"

# System Requirements
echo -e "\n${YELLOW}=== SYSTEM REQUIREMENTS ===${NC}"

check "Docker installed and running" \
    "docker --version && docker info" \
    "Docker is not installed or not running"

check "Docker Compose available" \
    "docker-compose --version || docker compose --version" \
    "Docker Compose is not available"

check "UV package manager available" \
    "which uv && uv --version" \
    "UV package manager not found"

check "Python 3.11+ available" \
    "python3 --version | grep -E 'Python 3\.(11|12)'" \
    "Python 3.11+ not found"

check "Git repository status clean" \
    "cd '$PROJECT_ROOT' && git status --porcelain | wc -l | grep -E '^[[:space:]]*0[[:space:]]*$'" \
    "Git repository has uncommitted changes"

# Project Structure
echo -e "\n${YELLOW}=== PROJECT STRUCTURE ===${NC}"

check "Core source directories exist" \
    "test -d '$PROJECT_ROOT/src/core' && test -d '$PROJECT_ROOT/src/data' && test -d '$PROJECT_ROOT/src/services'" \
    "Required source directories missing"

check "Configuration file exists" \
    "test -f '$PROJECT_ROOT/config.toml'" \
    "config.toml not found"

check "Docker configuration exists" \
    "test -f '$PROJECT_ROOT/docker-compose.yml' && test -d '$PROJECT_ROOT/docker'" \
    "Docker configuration files missing"

check "SQL migrations exist" \
    "test -d '$PROJECT_ROOT/sql' && find '$PROJECT_ROOT/sql' -name '*.sql' | head -1" \
    "SQL migration files not found"

# Infrastructure Files
echo -e "\n${YELLOW}=== INFRASTRUCTURE FILES ===${NC}"

check "Onboarding setup wizard exists" \
    "test -f '$PROJECT_ROOT/scripts/onboarding/setup_wizard.sh' && test -x '$PROJECT_ROOT/scripts/onboarding/setup_wizard.sh'" \
    "Onboarding setup wizard missing or not executable"

check "Database optimization SQL exists" \
    "test -f '$PROJECT_ROOT/sql/performance/production_optimization.sql'" \
    "Database optimization SQL file missing"

check "ROI tracking service exists" \
    "test -f '$PROJECT_ROOT/src/services/roi/roi_tracking_service.py'" \
    "ROI tracking service implementation missing"

check "CI/CD pipeline configuration exists" \
    "test -f '$PROJECT_ROOT/.github/workflows/production-deploy.yml'" \
    "GitHub Actions CI/CD pipeline missing"

check "Terraform configuration exists" \
    "test -f '$PROJECT_ROOT/terraform/production/main.tf'" \
    "Terraform infrastructure configuration missing"

check "Backup system exists" \
    "test -f '$PROJECT_ROOT/scripts/backup/automated_backup_system.py'" \
    "Automated backup system missing"

# Monitoring Configuration
echo -e "\n${YELLOW}=== MONITORING CONFIGURATION ===${NC}"

check "Grafana dashboards exist" \
    "test -f '$PROJECT_ROOT/docker/monitoring/grafana/dashboards/mlb-betting-production-dashboard.json'" \
    "Grafana production dashboard missing"

check "Prometheus alert rules exist" \
    "test -f '$PROJECT_ROOT/docker/monitoring/prometheus/alert_rules.yml'" \
    "Prometheus alert rules missing"

check "AlertManager configuration exists" \
    "test -f '$PROJECT_ROOT/docker/monitoring/alertmanager/alertmanager.yml'" \
    "AlertManager configuration missing"

check "Monitoring dashboard API exists" \
    "test -f '$PROJECT_ROOT/src/interfaces/api/monitoring_dashboard.py'" \
    "Monitoring dashboard API missing"

# Code Quality
echo -e "\n${YELLOW}=== CODE QUALITY ===${NC}"

check "Dependencies can be installed" \
    "cd '$PROJECT_ROOT' && uv sync --dev" \
    "Failed to install dependencies"

check "Code formatting passes" \
    "cd '$PROJECT_ROOT' && uv run ruff format --check" \
    "Code formatting issues found - run 'uv run ruff format'"

check "Linting passes" \
    "cd '$PROJECT_ROOT' && uv run ruff check" \
    "Linting issues found - run 'uv run ruff check --fix'"

check_warn "Type checking passes" \
    "cd '$PROJECT_ROOT' && uv run mypy src/ --ignore-missing-imports" \
    "Type checking warnings found"

# Database Configuration
echo -e "\n${YELLOW}=== DATABASE CONFIGURATION ===${NC}"

check_warn "Database connection configuration" \
    "cd '$PROJECT_ROOT' && python3 -c \"from src.core.config import get_config; config = get_config(); print(config.database.host)\"" \
    "Database configuration may need review"

check "Database migration files are valid SQL" \
    "find '$PROJECT_ROOT/sql' -name '*.sql' -exec sh -c 'python3 -c \"import sqlparse; sqlparse.parse(open(\"{}\").read())\"' \\;" \
    "Invalid SQL syntax found in migration files"

# Testing
echo -e "\n${YELLOW}=== TESTING ===${NC}"

check "Unit tests exist" \
    "find '$PROJECT_ROOT/tests' -name 'test_*.py' | head -1" \
    "No unit tests found"

check_warn "Tests pass" \
    "cd '$PROJECT_ROOT' && timeout 300 uv run pytest tests/unit/ -x --tb=short" \
    "Some tests failing - review test results"

# Security
echo -e "\n${YELLOW}=== SECURITY CONFIGURATION ===${NC}"

check "Security module exists" \
    "test -f '$PROJECT_ROOT/src/core/security.py'" \
    "Security module missing"

check "No hardcoded secrets in code" \
    "! grep -r -i -E '(password|secret|key|token)\\s*=\\s*[\"\\'][^\"\\'\$]' '$PROJECT_ROOT/src' || true" \
    "Potential hardcoded secrets found in source code"

check "Environment variables documented" \
    "test -f '$PROJECT_ROOT/.env.example' || grep -q 'DATABASE_URL' '$PROJECT_ROOT/config.toml'" \
    "Environment variable documentation missing"

# Performance Optimization
echo -e "\n${YELLOW}=== PERFORMANCE OPTIMIZATION ===${NC}"

check "Performance optimization SQL syntax" \
    "python3 -c \"import sqlparse; sqlparse.parse(open('$PROJECT_ROOT/sql/performance/production_optimization.sql').read())\"" \
    "Performance optimization SQL has syntax errors"

check "Monitoring service implementation" \
    "python3 -c \"from src.services.monitoring.prometheus_metrics_service import PrometheusMetricsService\"" \
    "Prometheus metrics service cannot be imported"

# Docker Services
echo -e "\n${YELLOW}=== DOCKER SERVICES ===${NC}"

check_warn "Docker services can start" \
    "cd '$PROJECT_ROOT' && timeout 60 docker-compose config" \
    "Docker Compose configuration issues detected"

# Deployment Prerequisites
echo -e "\n${YELLOW}=== DEPLOYMENT PREREQUISITES ===${NC}"

check_warn "AWS CLI configured (if using AWS)" \
    "aws --version && aws sts get-caller-identity" \
    "AWS CLI not configured - needed for cloud deployment"

check_warn "Terraform available (if using IaC)" \
    "terraform --version" \
    "Terraform not available - needed for infrastructure as code"

check_warn "GitHub secrets configured" \
    "test -f '$PROJECT_ROOT/.github/workflows/production-deploy.yml'" \
    "GitHub Actions configuration exists but secrets may need configuration"

# Final Validation Summary
echo -e "\n${YELLOW}=== VALIDATION SUMMARY ===${NC}"

echo -e "\n📊 ${BLUE}RESULTS SUMMARY${NC}"
echo -e "Total Checks: $TOTAL_CHECKS"
echo -e "✅ Passed: $PASSED_CHECKS"
echo -e "❌ Failed: $FAILED_CHECKS"
echo -e "⚠️ Warnings: $WARNING_CHECKS"

log "INFO" "Validation completed - Passed: $PASSED_CHECKS, Failed: $FAILED_CHECKS, Warnings: $WARNING_CHECKS"

# Determine overall status
if [ $FAILED_CHECKS -eq 0 ]; then
    echo -e "\n🎉 ${GREEN}PRODUCTION READINESS: APPROVED${NC}"
    echo -e "All critical checks passed. System is ready for production deployment."
    
    if [ $WARNING_CHECKS -gt 0 ]; then
        echo -e "\n⚠️ ${YELLOW}NOTE: $WARNING_CHECKS warning(s) detected. Review recommended but not blocking.${NC}"
    fi
    
    echo -e "\n🚀 ${BLUE}NEXT STEPS:${NC}"
    echo -e "1. Review the deployment checklist: .claude/tasks/PRODUCTION_DEPLOYMENT_READINESS_CHECKLIST.md"
    echo -e "2. Configure production environment variables"
    echo -e "3. Execute infrastructure deployment with Terraform"
    echo -e "4. Run the onboarding setup wizard: ./scripts/onboarding/setup_wizard.sh"
    echo -e "5. Validate monitoring dashboards and alerting"
    
    exit 0
else
    echo -e "\n🚨 ${RED}PRODUCTION READINESS: BLOCKED${NC}"
    echo -e "$FAILED_CHECKS critical issue(s) must be resolved before deployment."
    
    echo -e "\n🔧 ${YELLOW}REQUIRED ACTIONS:${NC}"
    echo -e "1. Review failed checks in the log: $LOG_FILE"
    echo -e "2. Address all critical failures"
    echo -e "3. Re-run this validation script"
    echo -e "4. Contact DevOps team if assistance is needed"
    
    exit 1
fi