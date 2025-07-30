#!/bin/bash
# Docker Stack Integration Test
# Validates all Docker services are healthy and accessible

set -e

# Configuration
TIMEOUT=120                 # 2 minutes timeout
WAIT_INTERVAL=5            # 5 seconds between checks
MAX_ATTEMPTS=$((TIMEOUT / WAIT_INTERVAL))

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Starting Docker Stack Integration Tests${NC}"
echo -e "${BLUE}===========================================${NC}"

# Function to wait for service to be healthy
wait_for_service() {
    local service_name=$1
    local health_endpoint=$2
    local max_wait=${3:-$MAX_ATTEMPTS}
    
    echo -e "${YELLOW}⏳ Waiting for $service_name to be healthy...${NC}"
    
    for i in $(seq 1 $max_wait); do
        if curl -s -f "$health_endpoint" > /dev/null 2>&1; then
            echo -e "${GREEN}✅ $service_name is healthy${NC}"
            return 0
        fi
        
        if [ $i -eq $max_wait ]; then
            echo -e "${RED}❌ $service_name failed to become healthy after ${TIMEOUT}s${NC}"
            return 1
        fi
        
        echo -e "${YELLOW}   Attempt $i/$max_wait - waiting ${WAIT_INTERVAL}s...${NC}"
        sleep $WAIT_INTERVAL
    done
}

# Function to test service endpoint
test_endpoint() {
    local service_name=$1
    local endpoint=$2
    local expected_status=${3:-200}
    
    echo -e "${YELLOW}🧪 Testing $service_name endpoint: $endpoint${NC}"
    
    response=$(curl -s -w "%{http_code}" "$endpoint" -o /tmp/test_response)
    
    if [ "$response" = "$expected_status" ]; then
        echo -e "${GREEN}✅ $service_name endpoint test passed (HTTP $response)${NC}"
        return 0
    else
        echo -e "${RED}❌ $service_name endpoint test failed (HTTP $response, expected $expected_status)${NC}"
        echo -e "${RED}Response body:${NC}"
        cat /tmp/test_response
        return 1
    fi
}

# Function to test database connectivity
test_database() {
    echo -e "${YELLOW}🧪 Testing PostgreSQL database connectivity...${NC}"
    
    # Try to connect using docker exec
    if docker-compose exec -T postgres pg_isready -h localhost -p 5432 > /dev/null 2>&1; then
        echo -e "${GREEN}✅ PostgreSQL database is ready${NC}"
        return 0
    else
        echo -e "${RED}❌ PostgreSQL database connection failed${NC}"
        return 1
    fi
}

# Function to test Redis connectivity
test_redis() {
    echo -e "${YELLOW}🧪 Testing Redis connectivity...${NC}"
    
    # Try to ping Redis using docker exec
    if docker-compose exec -T redis redis-cli ping | grep -q "PONG"; then
        echo -e "${GREEN}✅ Redis is responding${NC}"
        return 0
    else
        echo -e "${RED}❌ Redis ping failed${NC}"
        return 1
    fi
}

# Main test execution
main() {
    local failed_tests=0
    
    echo -e "${BLUE}📋 Test Plan:${NC}"
    echo -e "   1. Docker Compose services status"
    echo -e "   2. PostgreSQL database connectivity"
    echo -e "   3. Redis connectivity" 
    echo -e "   4. MLflow tracking server health"
    echo -e "   5. FastAPI application health"
    echo -e "   6. Nginx proxy routing"
    echo ""
    
    # Check if docker-compose is running
    echo -e "${YELLOW}🔍 Checking Docker Compose status...${NC}"
    if ! docker-compose ps | grep -q "Up"; then
        echo -e "${RED}❌ Docker Compose services are not running${NC}"
        echo -e "${YELLOW}💡 Run: docker-compose up -d${NC}"
        exit 1
    fi
    echo -e "${GREEN}✅ Docker Compose services are running${NC}"
    
    # Test 1: Database connectivity
    if ! test_database; then
        ((failed_tests++))
    fi
    
    # Test 2: Redis connectivity
    if ! test_redis; then
        ((failed_tests++))
    fi
    
    # Test 3: MLflow health check
    if ! wait_for_service "MLflow" "http://localhost:5000/health" 15; then
        ((failed_tests++))
    fi
    
    # Test 4: FastAPI health check
    if ! wait_for_service "FastAPI" "http://localhost:8000/health" 15; then
        ((failed_tests++))
    fi
    
    # Test 5: Nginx proxy routing
    if ! wait_for_service "Nginx" "http://localhost/health" 10; then
        ((failed_tests++))
    fi
    
    # Test 6: API endpoints through proxy
    if ! test_endpoint "API via Nginx" "http://localhost/api/health"; then
        ((failed_tests++))
    fi
    
    # Test 7: MLflow UI through proxy  
    if ! test_endpoint "MLflow via Nginx" "http://localhost/mlflow/health"; then
        ((failed_tests++))
    fi
    
    echo ""
    echo -e "${BLUE}===========================================${NC}"
    
    if [ $failed_tests -eq 0 ]; then
        echo -e "${GREEN}🎉 All Docker stack tests passed!${NC}"
        echo -e "${GREEN}✅ Your ML prediction system is ready for use${NC}"
        echo ""
        echo -e "${BLUE}🔗 Available endpoints:${NC}"
        echo -e "   • API Documentation: http://localhost/docs"
        echo -e "   • MLflow UI: http://localhost/mlflow"
        echo -e "   • API Health: http://localhost/health"
        echo -e "   • Direct FastAPI: http://localhost:8000/docs"
        echo -e "   • Direct MLflow: http://localhost:5000"
        exit 0
    else
        echo -e "${RED}❌ $failed_tests test(s) failed${NC}"
        echo -e "${YELLOW}💡 Check docker-compose logs for details:${NC}"
        echo -e "   docker-compose logs"
        exit 1
    fi
}

# Handle script interruption
trap 'echo -e "${YELLOW}\n⚠️  Test interrupted${NC}"; exit 130' INT

# Run main function
main "$@"