# ML Pipeline Testing Suite

Comprehensive testing suite for the MLB betting ML pipeline with production-grade testing coverage.

## Test Structure

### Unit Tests
- **`test_api_security.py`**: API security, authentication, rate limiting, CORS validation
- **`test_feature_pipeline.py`**: Feature extraction, validation, ML cutoff enforcement  
- **`test_redis_atomic_store.py`**: Redis atomic operations, distributed locking, serialization

### Integration Tests
- **`test_end_to_end_integration.py`**: Complete ML pipeline workflows from feature extraction to model training

### Load Testing
- **`test_load_testing.py`**: High-concurrency scenarios, performance validation, resource usage

### Production Deployment
- **`test_production_deployment.py`**: Environment-specific security, deployment validation, production readiness

## Test Categories

### 🧪 Unit Tests (`@pytest.mark.unit`)
Fast, isolated tests for individual components:
```bash
# Run all unit tests
uv run pytest tests/ml/ -m unit

# Run specific unit tests
uv run pytest tests/ml/test_api_security.py::TestRateLimiter
```

### 🔗 Integration Tests (`@pytest.mark.integration`)
Tests requiring external services (Redis, Database):
```bash
# Run integration tests (requires Redis + Database)
uv run pytest tests/ml/ -m integration

# Skip integration tests
uv run pytest tests/ml/ -m "not integration"
```

### 📈 Load Tests (`@pytest.mark.load_test`)
Performance and concurrency testing:
```bash
# Run load tests
uv run pytest tests/ml/ -m load_test -v

# Run specific load test
uv run pytest tests/ml/test_load_testing.py::TestAPILoadTesting::test_concurrent_api_requests_with_rate_limiting
```

### 🚀 Deployment Tests (`@pytest.mark.deployment`)
Production deployment validation:
```bash
# Run deployment tests
uv run pytest tests/ml/ -m deployment

# Test specific environment
uv run pytest tests/ml/test_production_deployment.py::TestEnvironmentConfiguration
```

## Running Tests

### Quick Test Commands
```bash
# Run all ML tests
uv run pytest tests/ml/

# Run with coverage
uv run pytest tests/ml/ --cov=src.ml --cov-report=html

# Run fast tests only
uv run pytest tests/ml/ -m "not slow and not integration"

# Run security tests
uv run pytest tests/ml/ -m security

# Run performance benchmarks
uv run pytest tests/ml/ -m benchmark --benchmark-only
```

### Environment-Specific Testing
```bash
# Development environment
ENVIRONMENT=development uv run pytest tests/ml/test_production_deployment.py

# Production environment validation
ENVIRONMENT=production API_SECRET_KEY=test_key uv run pytest tests/ml/test_production_deployment.py

# Staging environment
ENVIRONMENT=staging uv run pytest tests/ml/test_production_deployment.py
```

### Prerequisites

#### Required Services
- **Redis**: For atomic store and rate limiting tests
  ```bash
  # Start Redis (Docker)
  docker run -d -p 6379:6379 redis:7-alpine
  
  # Or use existing Redis
  export TEST_REDIS_URL="redis://localhost:6379/15"
  ```

- **PostgreSQL**: For database integration tests
  ```bash
  # Start PostgreSQL (Docker)
  docker run -d -p 5432:5432 -e POSTGRES_DB=mlb_betting_test -e POSTGRES_PASSWORD=test postgres:15
  
  # Or use existing database
  export TEST_DB_HOST="localhost"
  export TEST_DB_NAME="mlb_betting_test"
  ```

#### Environment Variables
```bash
# Test environment
export ENVIRONMENT=testing
export API_SECRET_KEY=test_secret_key_for_testing
export TEST_REDIS_URL=redis://localhost:6379/15
export TEST_DB_HOST=localhost
export TEST_DB_NAME=mlb_betting_test
```

## Test Performance Targets

### API Performance
- **Health Check**: <100ms response time
- **Prediction Endpoint**: <500ms response time  
- **Authentication**: <1ms per verification
- **Rate Limiting**: <50ms per check

### Redis Performance
- **Cache Operations**: <50ms average
- **Retrieval Operations**: <25ms average
- **Batch Operations**: <100ms for 100 items
- **Atomic Operations**: <100ms with locking

### Database Performance
- **Connection Pool**: <10ms connection acquisition
- **Query Execution**: <100ms for feature queries
- **Transaction Commit**: <50ms average

### Memory Usage
- **Feature Extraction**: <200KB per game
- **Batch Processing**: <50MB for 1000 games
- **API Operations**: <500MB total memory

## Test Data and Mocking

### Mock Data Patterns
```python
# Sample game data for testing
sample_game_data = pl.DataFrame({
    'game_id': [12345] * 20,
    'timestamp': [base_time + timedelta(minutes=i*5) for i in range(20)],
    'sportsbook_name': ['DraftKings', 'FanDuel', 'BetMGM', 'Caesars'] * 5,
    'market_type': ['moneyline'] * 20,
    'home_team': ['Yankees'] * 20,
    'away_team': ['Red Sox'] * 20,
    'home_ml_odds': [-150 + i for i in range(20)],
    'away_ml_odds': [130 + i for i in range(20)]
})
```

### Security Test Patterns
```python
# API authentication testing
headers = {"Authorization": "Bearer test_api_key"}
response = client.post("/api/v1/predict", json=payload, headers=headers)

# Rate limiting testing
for i in range(rate_limit + 5):  # Exceed limit
    allowed = await rate_limiter.is_allowed(client_id, rate_limit)
```

## Continuous Integration

### GitHub Actions Integration
```yaml
# .github/workflows/ml-tests.yml
- name: Run ML Unit Tests
  run: uv run pytest tests/ml/ -m "unit and not slow" --cov=src.ml

- name: Run Integration Tests
  run: uv run pytest tests/ml/ -m integration
  env:
    TEST_REDIS_URL: redis://redis:6379/15
    TEST_DB_HOST: postgres
```

### Pre-commit Hooks
```yaml
# .pre-commit-config.yaml
- repo: local
  hooks:
    - id: ml-security-tests
      name: ML Security Tests
      entry: uv run pytest tests/ml/ -m security --tb=short
      language: system
      pass_filenames: false
```

## Performance Monitoring

### Benchmark Results Tracking
```bash
# Generate performance baseline
uv run pytest tests/ml/ -m benchmark --benchmark-json=benchmark_results.json

# Compare with previous results
uv run pytest tests/ml/ -m benchmark --benchmark-compare=baseline.json
```

### Load Test Reports
```bash
# Generate load test report
uv run pytest tests/ml/test_load_testing.py -v --tb=short > load_test_report.txt

# Monitor resource usage during tests
uv run pytest tests/ml/ -m load_test --capture=no
```

## Troubleshooting

### Common Issues

#### Redis Connection Errors
```bash
# Check Redis is running
redis-cli ping

# Use different Redis database for tests
export TEST_REDIS_URL="redis://localhost:6379/15"
```

#### Database Connection Issues
```bash
# Check PostgreSQL is running
pg_isready -h localhost -p 5432

# Create test database
createdb mlb_betting_test
```

#### Import Errors
```bash
# Install dependencies
uv sync --dev

# Check Python path
uv run python -c "import sys; print(sys.path)"
```

#### Permission Errors
```bash
# Fix file permissions
chmod +x scripts/run_tests.sh

# Run with proper permissions
sudo uv run pytest tests/ml/ -m integration
```

### Performance Issues
- **Slow Tests**: Use `-m "not slow"` to skip time-intensive tests
- **Memory Issues**: Run tests with `--maxfail=1` to stop on first failure
- **Timeout Issues**: Increase timeout with `--timeout=600`

## Contributing

### Adding New Tests
1. **Choose appropriate test category** (unit/integration/load/deployment)
2. **Add proper pytest markers** (`@pytest.mark.unit`, etc.)
3. **Follow naming conventions** (`test_*.py`, `Test*` classes)
4. **Include performance assertions** where relevant
5. **Add to appropriate test suite** documentation

### Test Quality Standards
- **Coverage**: Maintain >80% code coverage
- **Performance**: Include performance assertions
- **Security**: Test security controls and validation
- **Documentation**: Include docstrings and comments
- **Isolation**: Tests should be independent and repeatable

### Code Review Checklist
- [ ] All tests pass in isolation
- [ ] Tests cover both success and failure cases
- [ ] Performance targets are validated
- [ ] Security controls are tested
- [ ] Documentation updated
- [ ] Appropriate markers added