# MLB Betting System - User Guide

## Quick Start

### 1. Setup & Installation
```bash
# Install dependencies
uv sync

# Setup database (one-time)
uv run -m src.interfaces.cli database setup-action-network --test-connection

# If setup fails, try test-only mode to check connection:
uv run -m src.interfaces.cli database setup-action-network --test-only
```

**Database Setup Troubleshooting:**
- If you see "relation does not exist" errors, run the missing table migrations first
- For "trigger already exists" errors, use `--test-only` flag to skip schema setup
- Check that PostgreSQL is running on the correct port (5433 for Docker, 5432 for local)

### 2. Start Real-Time Monitoring Dashboard
```bash
# Launch the monitoring dashboard (recommended for new users)
uv run -m src.interfaces.cli monitoring dashboard

# The dashboard will be available at: http://localhost:8080
# Features live pipeline status, system health, and manual controls
```

### 3. Collect Data
```bash
# Collect data from primary sources
uv run -m src.interfaces.cli data collect --source action_network --real
uv run -m src.interfaces.cli data collect --source vsin --real
uv run -m src.interfaces.cli data collect --source sbd --real

# Check collection status
uv run -m src.interfaces.cli data status --detailed
```

### 4. Generate Analysis Data
```bash
# Generate historical data for analysis
uv run -m src.interfaces.cli action-network collect --date today
uv run -m src.interfaces.cli action-network history --days 30
```

### 5. Run Analysis & Find Opportunities
```bash
# Comprehensive movement analysis
uv run -m src.interfaces.cli movement analyze --input-file output/action_network_history.json --show-details

# Look for reverse line movement
uv run -m src.interfaces.cli movement rlm --input-file output/action_network_history.json --min-movements 50

# Find steam moves
uv run -m src.interfaces.cli movement steam --input-file output/action_network_history.json --show-details
```

### 6. Backtest Strategies
```bash
# Test strategy performance over time
uv run -m src.interfaces.cli backtest run \
  --start-date 2024-06-01 \
  --end-date 2024-06-30 \
  --strategies sharp_action consensus \
  --initial-bankroll 10000 \
  --bet-size 100
```

## Monitoring & Operations

### Real-Time Dashboard
The system includes a production-ready monitoring dashboard:

- **Web Interface**: http://localhost:8080 (when running `monitoring dashboard`)
- **Live Updates**: Real-time pipeline status via WebSockets
- **System Health**: Comprehensive health checks and dependency monitoring
- **Manual Controls**: Break-glass pipeline execution for emergency operations
- **Performance Metrics**: 40+ Prometheus metrics with business KPIs

### Command Line Monitoring
```bash
# Real-time terminal monitoring
uv run -m src.interfaces.cli monitoring live

# Check system health
uv run -m src.interfaces.cli monitoring status

# Check specific collector health
uv run -m src.interfaces.cli monitoring health-check --collector vsin

# Performance analysis
uv run -m src.interfaces.cli monitoring performance --hours 24

# Alert management
uv run -m src.interfaces.cli monitoring alerts --severity critical
```

### Emergency Operations
```bash
# Manual pipeline execution (break-glass)
uv run -m src.interfaces.cli monitoring execute

# Direct CLI fallback
uv run -m src.interfaces.cli data collect --source action_network --emergency
```

## Security Configuration

### Environment Setup
Copy the example environment file and configure:
```bash
cp .env.example .env
# Edit .env with your actual configuration values
```

### Key Security Settings
```bash
# Dashboard API Security
DASHBOARD_API_KEY=generate_secure_random_key_here
ENABLE_AUTH=true
ENABLE_RATE_LIMIT=true

# IP Whitelisting (optional for production)
ENABLE_IP_WHITELIST=true
BREAK_GLASS_IP_WHITELIST=127.0.0.1,::1,192.168.1.0/24

# Redis Rate Limiting (production)
ENABLE_REDIS_RATE_LIMITING=true
REDIS_URL=redis://localhost:6379/0
```

### Authentication
The monitoring dashboard and break-glass endpoints support:
- **Bearer Token Authentication**: `Authorization: Bearer <DASHBOARD_API_KEY>`
- **X-API-Key Header**: `X-API-Key: <DASHBOARD_API_KEY>`

## Data Sources & Pipeline

### Available Data Sources
- **Action Network**: Real-time betting lines and sharp action indicators
- **VSIN**: Professional betting insights and market analysis  
- **SportsBettingDime (SBD)**: Real-time betting splits from 9+ major sportsbooks
- **MLB Stats API**: Official game data and statistics

### Pipeline Flow
```
RAW (Source-Specific) → STAGING (Historical) → CURATED (Analysis-Ready)
```

### Data Quality
The system includes comprehensive data quality monitoring:
- Real-time completeness scoring
- Duplicate prevention with timestamp precision
- Automatic sportsbook ID resolution
- EST/EDT timezone consistency

## Data Validation & Testing

### Pipeline Health Check
```bash
# Quick pipeline assessment
uv run assess_pipeline_data.py

# Note: Expected warnings from pipeline assessment:
# - "N/A (no created_at column)" - Normal for some tables
# - "N/A (no processed_at column)" - Normal for raw data tables
# - "Table does not exist" - Normal for unused data sources

# Database connectivity test
uv run test_db_connectivity.py

# Run integration tests
uv run pytest tests/integration/ -v
```

### Data Source Validation
```bash
# Test individual data sources
uv run -m src.interfaces.cli data test --source action_network --real
uv run -m src.interfaces.cli data test --source vsin --real
uv run -m src.interfaces.cli data test --source sbd --real

# Check data freshness
uv run -m src.interfaces.cli data status --detailed
```

### Pipeline Processing Tests
```bash
# Test RAW → STAGING pipeline
uv run -m src.interfaces.cli pipeline run --zone staging --dry-run

# Test full pipeline
uv run -m src.interfaces.cli pipeline run --zone all --dry-run

# Check pipeline status
uv run -m src.interfaces.cli pipeline status --detailed
```

### Data Quality Validation
```bash
# Check for processing backlogs
uv run -c "
from src.data.database.connection import get_connection, initialize_connections
from src.core.config import get_settings
import asyncio

async def check_backlog():
    config = get_settings()
    initialize_connections(config)
    async with get_connection() as conn:
        backlog = await conn.fetchval('''
            SELECT COUNT(*) FROM raw_data.action_network_odds 
            WHERE processed_at IS NULL
        ''')
        print(f'Unprocessed records: {backlog}')

asyncio.run(check_backlog())
"

# Validate data completeness
uv run -m src.interfaces.cli data-quality status
```

### Expected Data Volumes
Based on current system state (post-unification):
- **RAW Zone**: ~19,500 total records, ~9,000 recent (7d)
- **STAGING Zone**: ~89,000 historical odds records
- **CURATED Zone**: ~16,000 betting lines, 134 unified games in master table

### Data Quality Thresholds
- **Action Network**: Should have data within 2 hours
- **SBD**: Should have data within 4 hours  
- **VSIN**: Should have data within 6 hours
- **Pipeline Backlog**: Should be < 100 unprocessed records

## Troubleshooting

### Recent Fixes (January 2025) ✅

**P0 Database Schema Fragmentation**: **RESOLVED**
- **Issue**: Two master game tables causing FK relationship fragmentation 
- **Fix**: Successfully unified `enhanced_games` and `games_complete` into `master_games`
- **Status**: ✅ Complete - Zero data loss, 134 games unified, all FK relationships fixed
- **Impact**: Database now in production-ready state with unified architecture

### Common Issues
1. **Database Connection Issues**
   ```bash
   uv run -m src.interfaces.cli database setup-action-network --test-connection
   ```

2. **Source Connection Problems**
   ```bash
   uv run -m src.interfaces.cli data test --source vsin --real
   ```

3. **Data Quality Issues**
   ```bash
   uv run -m src.interfaces.cli data-quality status
   ```

4. **Pipeline Problems**
   - Check the monitoring dashboard at http://localhost:8080
   - Use `uv run -m src.interfaces.cli monitoring status` for CLI diagnostics

5. **Pipeline Backlog Issues**
   ```bash
   # Check for processing delays
   uv run assess_pipeline_data.py
   
   # Manual pipeline processing
   uv run -m src.interfaces.cli pipeline run --zone staging --source action_network
   ```

6. **Stale Data Sources**
   ```bash
   # Restart data collection for stale sources
   uv run -m src.interfaces.cli data collect --source sbd --real
   uv run -m src.interfaces.cli data collect --source vsin --real
   ```

### Getting Help
- Check system logs in `logs/` directory
- Use the monitoring dashboard for real-time diagnostics
- Run data quality checks with detailed output
- Consult the security guide at `docs/PRODUCTION_SECURITY_GUIDE.md`

## Advanced Usage

### Custom Analysis
The system supports custom analysis workflows:
```bash
# Generate custom date ranges
uv run -m src.interfaces.cli action-network history --start-date 2024-01-01 --end-date 2024-12-31

# Advanced backtesting with filters
uv run -m src.interfaces.cli backtest run \
  --start-date 2024-06-01 \
  --end-date 2024-06-30 \
  --strategies sharp_action \
  --min-confidence 0.75 \
  --max-juice -150
```

### Performance Tuning
- Monitor system metrics via Prometheus endpoint: `/metrics`
- Use Redis for production rate limiting to support multiple instances
- Configure connection pooling for high-volume operations
- Enable distributed tracing for complex debugging

## Pipeline Backlog Monitoring & Resolution

### Identifying Backlogs
Pipeline backlogs occur when RAW → STAGING processing falls behind data collection:

```bash
# Check for unprocessed records
uv run assess_pipeline_data.py | grep "Pipeline backlog"

# Manual check for specific tables
PGPASSWORD=postgres psql -h localhost -p 5433 -U samlafell -d mlb_betting -c "
SELECT COUNT(*) as unprocessed_count 
FROM raw_data.action_network_odds 
WHERE processed_at IS NULL AND created_at > NOW() - INTERVAL '24 hours'
"
```

### Resolving Backlogs
```bash
# Run staging zone processing to clear backlog
uv run -m src.interfaces.cli pipeline run --zone staging

# For persistent backlogs, run specific processor
uv run -m src.data.pipeline.staging_zone process --source action_network --batch-size 100

# Monitor processing progress
uv run assess_pipeline_data.py
```

### Prevention & Monitoring
- **Automated Processing**: Set up cron jobs or scheduled tasks for regular pipeline runs
- **Resource Monitoring**: Monitor CPU/memory usage during peak collection times
- **Alert Thresholds**: Set up alerts when unprocessed records exceed 500
- **Performance Tuning**: Increase batch sizes for large backlogs (default 100)

### Production Deployment
- Follow the security guide in `docs/PRODUCTION_SECURITY_GUIDE.md`
- Enable all monitoring and observability features
- Configure proper environment variables for production
- Set up proper logging aggregation (ELK/Loki compatible)
- Use Redis for distributed rate limiting and caching