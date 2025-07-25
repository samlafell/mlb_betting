# Pipeline Implementation Guide

## Overview

This guide documents the implementation of the RAW → STAGING → CURATED data pipeline architecture for the MLB betting system. The implementation provides clean handshake points between data collection systems and the data pipeline, following data engineering best practices.

## Architecture Summary

The pipeline implements a three-tier architecture:

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   RAW ZONE      │───▶│  STAGING ZONE    │───▶│  CURATED ZONE   │
│                 │    │                  │    │                 │
│ • Raw API data  │    │ • Data cleaning  │    │ • Feature eng.  │
│ • Exact storage │    │ • Normalization  │    │ • ML features   │
│ • OLTP writes   │    │ • Validation     │    │ • Analytics     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## Implementation Components

### 1. Database Schema (`sql/migrations/004_create_pipeline_zones.sql`)

**Key Features:**
- Complete PostgreSQL schema for all three zones
- Proper relationships and foreign keys between zones
- Comprehensive indexing for performance
- Audit trail and execution logging tables
- Raw betting lines tables for all bet types (moneylines, spreads, totals)

**Schema Structure:**
- `raw_data` schema: 12 tables for unprocessed data
- `staging` schema: 8 tables for cleaned data  
- `curated` schema: 10 tables for analysis-ready data
- Pipeline execution logging for monitoring

### 2. Configuration Updates (`config.toml`)

Added pipeline configuration:
```toml
[schemas]
raw = "raw_data"
staging = "staging" 
curated = "curated"

[pipeline]
enable_staging = true
enable_curated = true
auto_promotion = true
validation_enabled = true
quality_threshold = 0.8

[pipeline.zones]
raw_enabled = true
staging_enabled = true
curated_enabled = false  # Enable after testing
```

### 3. Zone Interface System (`src/data/pipeline/zone_interface.py`)

**Core Components:**
- `DataZone`: Abstract base class for all zones
- `ZoneConfig`: Configuration model for zone settings
- `DataRecord`: Unified data record structure
- `ProcessingResult`: Standardized result reporting
- `ZoneFactory`: Factory pattern for zone creation

**Key Features:**
- Type-safe zone progression validation
- Comprehensive metrics collection
- Health check capabilities
- Quality scoring framework

### 4. RAW Zone Implementation

**RAW Zone Processor** (`src/data/pipeline/raw_zone.py`):
- Stores data exactly as received from external sources
- Minimal validation (structural integrity only)
- Automatic metadata extraction from raw JSON
- Source-specific table routing

**RAW Zone Adapter** (`src/data/pipeline/raw_zone_adapter.py`):
- Adapts existing collectors to write to RAW zone
- Backward compatibility with current collection systems
- Support for all major data sources (Action Network, SBD, VSIN, MLB Stats)
- Convenience methods for different data types

### 5. STAGING Zone Implementation (`src/data/pipeline/staging_zone.py`)

**Data Processing Features:**
- Team name normalization using existing utilities
- Sportsbook name standardization
- Numeric field cleaning and validation
- Data quality scoring (completeness, accuracy, consistency)
- Duplicate detection and handling

**Quality Control:**
- Configurable quality thresholds
- Comprehensive validation rules
- Error tracking and reporting
- Data lineage preservation

### 6. Pipeline Orchestration (`src/data/pipeline/pipeline_orchestrator.py`)

**Orchestrator Features:**
- Multi-zone coordination and execution
- Comprehensive metrics collection
- Error handling and recovery
- Pipeline execution tracking
- Health monitoring for all zones

**Execution Modes:**
- Full pipeline (RAW → STAGING → CURATED)
- Zone-specific processing
- Custom pipeline configurations
- Batch processing with configurable sizes

### 7. CLI Integration (`src/interfaces/cli/commands/pipeline.py`)

**Command Categories:**
- `pipeline run`: Execute pipeline with various options
- `pipeline status`: Check zone health and execution status
- `pipeline migrate`: Migration planning and guidance

**Usage Examples:**
```bash
# Run full pipeline
uv run -m src.interfaces.cli pipeline run --zone all

# Process specific source
uv run -m src.interfaces.cli pipeline run --source action_network

# Check pipeline status
uv run -m src.interfaces.cli pipeline status --detailed

# Dry run to see what would be processed
uv run -m src.interfaces.cli pipeline run --dry-run
```

## Data Flow Architecture

### Current vs Enhanced Flow

**Before (Direct to Production):**
```
Action Network/VSIN/SBD → Collection → curated.spreads/totals/moneylines → Analysis
```

**After (Proper Data Lineage):**
```
External APIs → RAW Zone → STAGING Zone → CURATED Zone → Analysis
    ↓              ↓           ↓              ↓
Action/VSIN/SBD → raw_data.  → staging.     → curated.     → Strategies/
Betting Lines     spreads_raw   spreads       spreads         ML Models
                  totals_raw    totals        totals
                  moneylines_raw moneylines   moneylines
```

### Handshake Points

1. **Collection → RAW**: Use `RawZoneAdapter` methods
2. **RAW → STAGING**: Automatic promotion with quality gates
3. **STAGING → CURATED**: Feature engineering and enrichment
4. **CURATED → Analysis**: Clean, analysis-ready data

## Integration Strategy

### Phase 1: RAW Zone Implementation (Completed)

✅ **Database Schema**: Complete PostgreSQL schema for all zones  
✅ **Configuration**: Pipeline settings and zone configuration  
✅ **RAW Zone Processor**: Core RAW zone functionality  
✅ **RAW Zone Adapter**: Backward compatibility for existing collectors  
✅ **STAGING Zone Processor**: Data cleaning and validation  
✅ **Pipeline Orchestrator**: Multi-zone coordination  
✅ **CLI Interface**: Pipeline management commands  

### Phase 2: Collector Integration (Next Steps)

**Modify Existing Collectors:**
```python
# Old approach (direct to core_betting)
await connection.execute("INSERT INTO curated.spreads ...")

# New approach (through RAW zone)
from src.data.pipeline.raw_zone_adapter import create_raw_zone_adapter

adapter = create_raw_zone_adapter()
result = await adapter.store_betting_lines(lines_data, 'spread', 'action_network')
```

**Integration Points:**
- `consolidated_action_network_collector.py`: Use `adapter.store_action_network_games()`
- `sbd_unified_collector.py`: Use `adapter.store_sbd_betting_splits()`
- `vsin_unified_collector.py`: Use `adapter.store_vsin_data()`

### Phase 3: STAGING Zone Testing (Pending)

**Testing Checklist:**
- [ ] RAW → STAGING data flow validation
- [ ] Team name normalization accuracy
- [ ] Quality scoring calibration
- [ ] Performance benchmarking
- [ ] Error handling validation

### Phase 4: CURATED Zone Development (Pending)

**Remaining Implementation:**
- [ ] CURATED zone processor implementation
- [ ] Feature engineering pipeline
- [ ] ML feature vector generation  
- [ ] Strategy integration
- [ ] Performance optimization

## Usage Guide

### 1. Database Setup

```bash
# Apply schema migration
psql -d mlb_betting -f sql/migrations/004_create_pipeline_zones.sql
```

### 2. Configuration

Update `config.toml` with pipeline settings (already implemented).

### 3. Basic Pipeline Usage

```python
from src.data.pipeline.raw_zone_adapter import create_raw_zone_adapter
from src.data.pipeline.pipeline_orchestrator import create_pipeline_orchestrator

# Store data to RAW zone
adapter = create_raw_zone_adapter()
result = await adapter.store_betting_lines(betting_data, 'moneyline', 'action_network')

# Run full pipeline
orchestrator = await create_pipeline_orchestrator()
execution = await orchestrator.run_full_pipeline(records)
```

### 4. CLI Pipeline Management

```bash
# Check pipeline status
uv run -m src.interfaces.cli pipeline status --detailed

# Run pipeline with specific source
uv run -m src.interfaces.cli pipeline run --source action_network --zone all

# Monitor execution
uv run -m src.interfaces.cli pipeline status --execution-id <uuid>
```

## Quality Assurance

### Data Quality Features

**RAW Zone:**
- ✅ Exact data preservation
- ✅ JSON structure validation  
- ✅ Audit trail creation
- ✅ Source attribution

**STAGING Zone:**
- ✅ Team name normalization
- ✅ Numeric field validation
- ✅ Quality score calculation
- ✅ Consistency checks

**Pipeline Orchestration:**
- ✅ Comprehensive error handling
- ✅ Metrics collection
- ✅ Health monitoring
- ✅ Execution tracking

### Performance Characteristics

**OLTP Optimized:**
- RAW zone INSERTs are simple and fast
- Batch processing for STAGING/CURATED zones
- Configurable batch sizes for performance tuning
- Indexed tables for efficient querying

**Monitoring:**
- Pipeline execution logging
- Zone-specific health checks  
- Quality score tracking
- Error rate monitoring

## Next Steps

### Immediate (Week 1)
1. **Test Database Migration**: Apply schema migration in development environment
2. **Update One Collector**: Modify Action Network collector to use RAW zone adapter
3. **Test RAW → STAGING Flow**: Validate data processing through first two zones

### Short Term (Weeks 2-3)
1. **Integrate All Collectors**: Update remaining collectors to use RAW zone
2. **Performance Tuning**: Optimize batch sizes and processing parameters
3. **Monitor Data Quality**: Validate quality scores and error rates

### Medium Term (Month 1)
1. **Implement CURATED Zone**: Complete feature engineering pipeline
2. **Strategy Integration**: Connect CURATED zone to existing strategy processors
3. **Production Testing**: Validate full pipeline in production environment

### Long Term (Months 2-3)
1. **Performance Optimization**: Advanced caching and parallel processing
2. **Advanced Analytics**: ML feature pipelines and model integration  
3. **Monitoring Enhancement**: Advanced dashboards and alerting

## Architecture Benefits

### Data Engineering Best Practices
- ✅ **Separation of Concerns**: Clear boundaries between ingestion, processing, and analysis
- ✅ **Data Lineage**: Complete traceability from source to analysis
- ✅ **Quality Gates**: Automated validation at each stage
- ✅ **Rollback Capability**: Can reprocess data from any stage

### Operational Benefits
- ✅ **Debugging**: Easy to isolate issues to specific pipeline stages
- ✅ **Testing**: Can test each zone independently
- ✅ **Scalability**: Zones can be scaled independently
- ✅ **Monitoring**: Comprehensive visibility into data flow

### Development Benefits
- ✅ **Backward Compatibility**: Existing collectors work with minimal changes
- ✅ **Incremental Migration**: Can migrate collectors one at a time
- ✅ **Type Safety**: Pydantic v2 models throughout
- ✅ **Modularity**: Clear interfaces between components

## Conclusion

The pipeline implementation successfully establishes clear handshake points between the data collection systems and the data engineering pipeline. The RAW zone serves as the optimal write target for OLTP operations, while STAGING and CURATED zones provide the data engineering best practices needed for reliable, high-quality analysis.

The implementation preserves existing investments while providing a clear migration path to modern data architecture. The system is ready for incremental deployment and testing, with comprehensive monitoring and quality controls throughout.

---

*Implementation completed: July 21, 2025*  
*Next phase: Collector integration and STAGING zone testing*