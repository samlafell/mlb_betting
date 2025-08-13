# Pipeline Disconnections and Improvement Notes

**Purpose**: Document service gaps and implementation requirements for autonomous AI agents
**Date**: 2025-08-13
**Context**: Full pipeline execution revealed several service disconnections requiring implementation

## Service Implementation Gaps

### 1. ML Features Batch Processor (HIGH PRIORITY)

**Status**: 🔴 NOT IMPLEMENTED
**Location**: `src/services/curated_zone/ml_temporal_features_service.py`
**CLI Command**: `uv run -m src.interfaces.cli curated process-ml-features`

**Current State**:
- CLI command exists and accepts parameters
- Infrastructure tables exist (`curated.ml_features`, `curated.ml_temporal_features`)
- Service skeleton may exist but batch processing not implemented

**Implementation Required**:
```python
class MLTemporalFeaturesService:
    """
    REQUIRED: Implement temporal feature engineering with 60-minute cutoff.
    
    Extract ML-ready features from enhanced games and staging data:
    - Team performance metrics (last 10 games)
    - Pitching matchup analysis
    - Line movement temporal patterns
    - Weather and venue factors
    - Sharp action indicators
    """
    
    async def process_batch_features(
        self, 
        days_back: int = 7, 
        limit: Optional[int] = None
    ) -> MLFeatureProcessingResult:
        """Process ML features for enhanced games."""
        # TODO: Implement feature extraction pipeline
        pass
```

**Data Sources to Integrate**:
- `curated.enhanced_games` - base game metadata
- `staging.betting_odds_unified` - market data and line movements
- `raw_data.action_network_odds` - temporal line movement data
- External APIs - weather, team stats, pitcher data

**Key Features to Extract**:
- Rolling team performance (10-game windows)
- Pitcher ERA, WHIP, matchup history
- Line movement velocity and magnitude
- Sharp money indicators and RLM detection
- Weather factors and venue effects

### 2. Betting Splits Aggregator (HIGH PRIORITY)

**Status**: 🔴 NOT IMPLEMENTED  
**Location**: `src/services/curated_zone/betting_splits_aggregator.py`
**CLI Command**: `uv run -m src.interfaces.cli curated process-betting-splits`

**Current State**:
- CLI command exists and accepts parameters
- Multiple data sources available (VSIN, SBD, Action Network)
- Aggregation logic not implemented

**Implementation Required**:
```python
class BettingSplitsAggregator:
    """
    REQUIRED: Implement multi-source betting splits aggregation.
    
    Aggregate betting splits from all available sources:
    - VSIN sharp action data
    - SBD public vs sharp percentages  
    - Action Network consensus data
    - Calculate weighted averages and confidence scores
    """
    
    async def aggregate_splits(
        self,
        days_back: int = 7,
        limit: Optional[int] = None
    ) -> BettingSplitsResult:
        """Aggregate betting splits from multiple sources."""
        # TODO: Implement multi-source aggregation
        pass
```

**Data Sources to Aggregate**:
- VSIN: Sharp action indicators, late money movements
- SBD: Public vs sharp betting percentages
- Action Network: Consensus and handle data
- Enhanced calculation of conflict/agreement scores

**Key Aggregations**:
- Weighted public/sharp percentages across sources
- Sharp action confidence scoring
- Consensus strength metrics
- Source reliability weighting

### 3. SBD Collector Async Issue (MEDIUM PRIORITY)

**Status**: 🔴 BROKEN  
**Location**: `src/data/collection/sbd_unified_collector_api.py`
**Error**: `TypeError: '>' not supported between instances of 'coroutine' and 'int'`

**Root Cause**: Async function called without await in comparison operation
**Impact**: SBD data collection fails, reducing data source diversity

**Fix Required**:
```python
# BROKEN CODE (somewhere in SBD collector):
if some_async_function() > threshold:  # ERROR: Missing await
    
# FIXED CODE:
if await some_async_function() > threshold:  # CORRECT: Proper await
```

**Investigation Steps**:
1. Search for coroutine comparison operations in SBD collector
2. Add proper await keywords to async function calls
3. Test data collection with SBD source
4. Update SERVICE_ISSUES_LOG.md when resolved

### 4. Enhanced Games Processing Optimization (LOW PRIORITY)

**Status**: 🟡 WORKING BUT IMPROVABLE
**Location**: `src/services/curated_zone/enhanced_games_service.py`
**Current Success Rate**: 40% (2/5 games)

**Issues**:
- VARCHAR(10) constraint causing team name truncation
- Some team names longer than 10 characters (e.g., "WASHINGTON")

**Improvement Required**:
```sql
-- Database schema update needed
ALTER TABLE curated.enhanced_games 
ALTER COLUMN home_team TYPE VARCHAR(20),
ALTER COLUMN away_team TYPE VARCHAR(20);
```

**Additional Optimizations**:
- Batch processing for multiple games
- Better error handling for data validation
- Enhanced team name normalization

## Infrastructure Improvements

### 1. Service Health Monitoring (MEDIUM PRIORITY)

**Status**: 🟡 BASIC MONITORING EXISTS
**Gap**: No comprehensive service health dashboard

**Implementation Needed**:
```python
class ServiceHealthMonitor:
    """
    Monitor health of all pipeline services:
    - Data collector availability
    - Processing service status  
    - Database connectivity
    - MLflow integration health
    """
    
    async def check_all_services(self) -> ServiceHealthReport:
        """Comprehensive service health check."""
        # TODO: Implement health monitoring
        pass
```

**Health Checks Required**:
- ✅ Database connectivity (implemented)
- ❌ MLflow service availability  
- ❌ Redis service connectivity
- ❌ All data collector health
- ❌ Processing service status

### 2. Data Quality Monitoring (LOW PRIORITY)

**Status**: 🟡 BASIC QUALITY SCORES EXIST
**Gap**: No automated quality monitoring and alerting

**Implementation Needed**:
- Automated data quality scoring across zones
- Quality trend monitoring and alerts
- Data completeness validation
- Source reliability tracking

### 3. Error Recovery and Retry Logic (LOW PRIORITY)

**Status**: 🟡 BASIC ERROR HANDLING EXISTS
**Gap**: No systematic retry and recovery mechanisms

**Implementation Needed**:
- Exponential backoff for failed operations
- Dead letter queues for persistent failures
- Automatic retry policies per service type
- Circuit breaker patterns for external APIs

## Quick Win Implementation Order

### Week 1: Core ML Pipeline
1. **Implement MLTemporalFeaturesService** (2-3 days)
   - Extract basic team performance features
   - Implement pitcher matchup analysis
   - Add line movement temporal features

2. **Implement BettingSplitsAggregator** (2 days)
   - Multi-source data aggregation
   - Weighted consensus calculations
   - Sharp action scoring

### Week 2: Service Reliability  
3. **Fix SBD Collector Async Issue** (1 day)
   - Debug and fix async/await patterns
   - Test data collection functionality

4. **Schema Improvements** (1 day)
   - Update VARCHAR constraints
   - Add missing indexes for performance

### Week 3: Enhanced Monitoring
5. **Service Health Monitoring** (2-3 days)
   - Comprehensive health check endpoints
   - Monitoring dashboard integration
   - Alert system implementation

## Testing Strategy for Implementations

### ML Features Testing
```bash
# Test ML features processing
uv run -m src.interfaces.cli curated process-ml-features --days-back 7 --limit 5 --dry-run
uv run -m src.interfaces.cli curated ml-features-health
uv run -m src.interfaces.cli curated ml-features-status
```

### Betting Splits Testing  
```bash
# Test betting splits aggregation
uv run -m src.interfaces.cli curated process-betting-splits --days-back 7 --limit 5 --dry-run
uv run -m src.interfaces.cli curated betting-splits-health
uv run -m src.interfaces.cli curated betting-splits-status
```

### SBD Collector Testing
```bash
# Test SBD data collection after fix
uv run -m src.interfaces.cli data collect --source sbd --real
uv run -m src.interfaces.cli data status --source sbd
```

## Development Notes for AI Agents

### Code Patterns to Follow
- **Async/Await**: All database and external API calls must use proper async patterns
- **Error Handling**: Wrap all operations in try/catch with specific error types
- **Logging**: Use structured logging with correlation IDs
- **Validation**: Validate all input data with Pydantic models
- **Configuration**: Use centralized config.toml for all settings

### Database Patterns to Follow
- **Transactions**: Use database transactions for multi-table operations
- **Connection Pooling**: Reuse existing connection pool infrastructure
- **Schema Validation**: Validate data against table schemas before insertion
- **Indexing**: Ensure queries use existing indexes for performance

### CLI Integration Patterns
- **Parameter Validation**: Use Click for command-line argument validation
- **Progress Indicators**: Show progress for long-running operations
- **Error Reporting**: Provide clear error messages with resolution guidance
- **Help Documentation**: Include comprehensive help text for all commands

## Expected Outcomes After Implementation

### Data Pipeline Completeness
- **100% Service Coverage**: All pipeline components operational
- **95%+ Success Rate**: Enhanced processing with robust error handling  
- **Multi-Source Integration**: Full data source diversity restored

### ML Pipeline Readiness
- **Complete Feature Set**: 50+ features available for model training
- **Temporal Analysis**: Time-series features for sophisticated models
- **Quality Metrics**: Comprehensive data quality scoring

### Operational Excellence
- **Real-time Monitoring**: Service health visibility
- **Automated Recovery**: Self-healing pipeline components
- **Performance Optimization**: Sub-second processing times

---
*Document Purpose*: Guide future development work and autonomous AI agent implementations
*Update Frequency*: After each implementation milestone  
*Owner*: Development team and AI agents working on pipeline improvements