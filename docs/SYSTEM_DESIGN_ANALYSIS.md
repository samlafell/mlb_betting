# 🏗️ MLB Betting System Design Analysis & Pipeline Architecture

## Executive Summary

Your current system already has a strong foundation for data collection separation and could be evolved into a proper RAW → STAGING → CURATED pipeline with strategic architectural improvements. The system demonstrates good practices but needs clearer boundary definition and pipeline orchestration.

## Current System Architecture Analysis

### 📊 **Strengths of Current Design**

1. **Clear Data Collection Layer Separation** ✅
   - Well-defined `src/data/collection/` directory with multiple collectors
   - Good abstraction with `BaseCollector` pattern
   - Rate limiting and monitoring already implemented
   - Multiple data sources (Action Network, SBD, VSIN, MLB Stats API)

2. **Unified Configuration** ✅
   - Centralized config.toml management
   - Pydantic v2 models for type safety
   - Environment-based configuration support

3. **Database Foundation** ✅
   - PostgreSQL as primary storage
   - Repository pattern implementation
   - Schema migration support

### ⚠️ **Areas Needing Pipeline Enhancement**

1. **Direct-to-Production Data Writes** ⚠️
   - Current: Programs write directly to `core_betting.spreads`, `core_betting.totals`, `core_betting.moneylines`
   - Issue: No raw data preservation, no processing lineage
   - Risk: Data loss, difficult debugging, no rollback capability

2. **Schema Organization** 
   - Current: Mixed schemas (`core_betting`, `splits`, `public`)
   - Opportunity: Clear RAW/STAGING/CURATED separation with betting line progression

3. **Betting Line Data Flow**
   - Current: Action Network/VSIN/SBD → Direct to core_betting tables
   - Missing: Raw storage → staging processing → curated analysis-ready tables

4. **Pipeline Orchestration**
   - Current: Individual collectors with orchestrator
   - Missing: Multi-stage pipeline coordination for betting lines processing

## Recommended Pipeline Architecture

### 🏗️ **Three-Tier Data Pipeline Design**

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   RAW ZONE      │───▶│  STAGING ZONE    │───▶│  CURATED ZONE   │
│                 │    │                  │    │                 │
│ • External APIs │    │ • Data Cleaning  │    │ • Feature Eng.  │
│ • Raw JSON/HTML │    │ • Normalization  │    │ • Aggregations  │
│ • Unprocessed   │    │ • Validation     │    │ • ML Features   │
│ • Historical    │    │ • Deduplication  │    │ • Analytics     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

### **Zone 1: RAW Data Zone**
**Purpose**: Ingest and store exactly what comes from external sources

**Schema**: `raw_data`
```sql
-- Keep your current collection system, but route to raw_data schema
CREATE SCHEMA raw_data;

-- Betting Lines Data (Core Product Data)
raw_data.betting_lines_raw         -- All raw betting lines (spreads, totals, MLs)
raw_data.moneylines_raw            -- Raw moneyline data from all sources
raw_data.spreads_raw               -- Raw spread data from all sources  
raw_data.totals_raw                -- Raw totals data from all sources
raw_data.line_movements_raw        -- Raw line movement history

-- Data by Source
raw_data.action_network_games      -- Raw Action Network JSON
raw_data.action_network_odds       -- Raw Action Network odds data
raw_data.sbd_betting_splits        -- Raw SBD API responses  
raw_data.vsin_data                 -- Raw VSIN feeds
raw_data.mlb_stats_api             -- Raw MLB official data
raw_data.odds_api_responses        -- Raw odds API data

-- Audit trail
raw_data.collection_log            -- When/what was collected
raw_data.source_health             -- Data source status monitoring
```

**Current System Integration**:
- Your existing collectors (`consolidated_action_network_collector.py`, etc.) work perfectly here
- Keep current orchestrator pattern
- Add zone routing configuration

### **Zone 2: STAGING Data Zone** 
**Purpose**: Clean, normalize, and prepare data for analysis

**Schema**: `staging`
```sql
CREATE SCHEMA staging;

-- Processed Betting Lines (Cleaned from Raw)
staging.moneylines                 -- Cleaned & normalized moneyline data
staging.spreads                    -- Cleaned & normalized spread data
staging.totals                     -- Cleaned & normalized totals data
staging.betting_lines              -- Unified cleaned betting lines
staging.line_movements             -- Processed movement history with patterns

-- Normalized game data  
staging.games                      -- Unified game records
staging.teams                      -- Standardized team data
staging.betting_splits             -- Normalized split data
staging.sharp_action_signals       -- Processed indicators

-- Quality control
staging.data_quality_metrics       -- Per-record quality scores
staging.validation_results         -- Cleaning audit trail
```

**Processing Logic**:
- Data validation and cleaning rules
- Team name standardization 
- Timezone normalization (your current EST/EDT handling)
- Duplicate detection and resolution
- Data quality scoring

### **Zone 3: CURATED Data Zone**
**Purpose**: Feature-enriched, analysis-ready datasets

**Schema**: `curated` 
```sql
CREATE SCHEMA curated;

-- Final Betting Lines (Analysis-Ready)
curated.moneylines                 -- Final moneyline data with analysis features
curated.spreads                    -- Final spread data with analysis features  
curated.totals                     -- Final totals data with analysis features
curated.betting_lines_enhanced     -- Complete betting lines with all features

-- Analysis-ready data
curated.enhanced_games             -- Games with all features
curated.betting_analysis           -- Sharp action, RLM, etc.
curated.movement_analysis          -- Line movement patterns
curated.strategy_results           -- Backtesting outcomes
curated.profitability_metrics      -- ROI, success rates

-- ML Features
curated.feature_vectors            -- Model-ready feature sets
curated.prediction_inputs          -- Real-time prediction data
```

## Implementation Strategy

### **Phase 1: Schema Reorganization** (Week 1-2)

**Current State Enhancement**:
```python
# Update your config.toml to support zones
[schemas]
raw = "raw_data"
staging = "staging" 
curated = "curated"

[pipeline]
enable_staging = true
enable_curated = true
auto_promotion = true  # Auto-move data through pipeline
```

**Database Migration**:
```sql
-- Extend your current consolidated_schema.sql
CREATE SCHEMA raw_data;
CREATE SCHEMA staging;  
CREATE SCHEMA curated;

-- Create raw betting line tables (NEW - captures source data)
CREATE TABLE raw_data.spreads_raw AS SELECT * FROM core_betting.spreads WHERE 1=0;
CREATE TABLE raw_data.totals_raw AS SELECT * FROM core_betting.totals WHERE 1=0;
CREATE TABLE raw_data.moneylines_raw AS SELECT * FROM core_betting.moneylines WHERE 1=0;

-- Create staging betting line tables (cleaned data)
CREATE TABLE staging.spreads AS SELECT * FROM core_betting.spreads WHERE 1=0;
CREATE TABLE staging.totals AS SELECT * FROM core_betting.totals WHERE 1=0;  
CREATE TABLE staging.moneylines AS SELECT * FROM core_betting.moneylines WHERE 1=0;

-- Migrate existing data to staging (current data becomes cleaned data)
INSERT INTO staging.spreads SELECT * FROM core_betting.spreads;
INSERT INTO staging.totals SELECT * FROM core_betting.totals;
INSERT INTO staging.moneylines SELECT * FROM core_betting.moneylines;

-- Keep core_betting as curated zone initially, rename later
-- RENAME core_betting TO curated; (after testing)
```

### **Phase 2: Pipeline Orchestration** (Week 3-4)

**Enhanced Orchestrator Pattern**:
```python
# src/services/pipeline/pipeline_orchestrator.py
class DataPipelineOrchestrator:
    """Multi-zone pipeline coordination"""
    
    def __init__(self):
        self.raw_collectors = YourCurrentCollectors()
        self.staging_processor = StagingProcessor()
        self.curated_builder = CuratedBuilder()
    
    async def run_full_pipeline(self, date_range):
        # Zone 1: Raw collection (your current system)
        raw_results = await self.raw_collectors.collect_all()
        
        # Zone 2: Staging processing (NEW)
        staging_results = await self.staging_processor.process(raw_results)
        
        # Zone 3: Curated enrichment (NEW)
        curated_results = await self.curated_builder.build_features(staging_results)
        
        return PipelineResults(raw, staging, curated)
```

### **Phase 3: Feature Engineering** (Week 5-6)

**Curated Zone Processing**:
```python
# src/analysis/feature_engineering/
class FeatureEngineer:
    """Transform staging data into analysis-ready features"""
    
    def build_sharp_action_features(self, staging_data):
        # Your existing sharp action detection logic
        # Enhanced with historical patterns
        
    def build_line_movement_features(self, staging_data):
        # RLM detection, steam moves, etc.
        # Your current movement analysis enhanced
        
    def build_consensus_features(self, staging_data):
        # Public vs sharp money patterns
        # Enhanced with trend analysis
```

## Integration with Current System

### **Minimal Disruption Approach**

1. **Keep Current Collection System** ✅
   - Your collectors are already well-designed
   - Just add zone routing configuration
   - No major code changes needed

2. **Enhance Database Schema** 📊
   - Add new schemas alongside existing
   - Migrate data gradually 
   - Maintain backward compatibility

3. **Extend CLI Interface** 🔧
   ```bash
   # Enhanced commands for pipeline management
   uv run -m src.interfaces.cli pipeline run --zone raw
   uv run -m src.interfaces.cli pipeline run --zone staging  
   uv run -m src.interfaces.cli pipeline run --zone curated
   uv run -m src.interfaces.cli pipeline run --full  # All zones
   
   # Zone-specific operations
   uv run -m src.interfaces.cli data status --zone staging
   uv run -m src.interfaces.cli data quality --zone all
   ```

### **Data Flow Enhancement**

**Current Flow** (Direct to core_betting):
```
Action Network/VSIN/SBD → Collection → core_betting.spreads/totals/moneylines → Analysis
```

**Enhanced Pipeline Flow** (Proper Data Lineage):
```
External APIs → RAW Zone → STAGING Zone → CURATED Zone → Analysis
    ↓              ↓           ↓              ↓
Action/VSIN/SBD → raw_data.  → staging.     → curated.     → Strategies/
Betting Lines     spreads_raw   spreads       spreads         ML Models
                  totals_raw    totals        totals
                  moneylines_raw moneylines   moneylines
                  ↓              ↓             ↓
              Raw Storage  →  Cleaning &  →  Feature Eng. → Analysis
                              Validation     & Enrichment
```

## Quality Benefits

### **Data Quality Improvements**

1. **Traceability** 📝
   - Complete audit trail from source to analysis
   - Data lineage tracking
   - Quality score propagation

2. **Reliability** 🔒
   - Isolation of raw data from processing errors
   - Rollback capability to any stage
   - Independent zone testing

3. **Performance** ⚡
   - Pre-computed features in curated zone
   - Reduced analysis query complexity
   - Caching opportunities at each stage

### **Development Benefits**

1. **Testability** 🧪
   - Test each zone independently
   - Mock data at any pipeline stage
   - Isolated feature development

2. **Scalability** 📈
   - Independent zone scaling
   - Parallel processing opportunities
   - Resource optimization per zone

3. **Maintainability** 🔧
   - Clear responsibility boundaries
   - Easier debugging and monitoring
   - Modular feature development

## Recommended Next Steps

### **Immediate Actions** (This Week)

1. **Create Schema Migration Plan**
   ```sql
   -- Add to your existing SQL migrations
   -- Start with schema creation, no data movement yet
   ```

2. **Update Configuration Structure**
   ```toml
   # Enhance config.toml with pipeline settings
   [pipeline.zones]
   raw_enabled = true
   staging_enabled = true
   curated_enabled = false  # Enable after testing
   ```

3. **Design Zone Interface**
   ```python
   # src/data/pipeline/zone_interface.py
   # Common interface for all zones
   ```

### **Progressive Implementation** (Next 2-4 Weeks)

1. **Week 1**: Schema setup + RAW zone routing
2. **Week 2**: STAGING zone processor development  
3. **Week 3**: CURATED zone feature engineering
4. **Week 4**: Full pipeline integration + testing

### **Success Metrics**

- **Data Quality**: 95%+ staging validation success rate
- **Performance**: <30s full pipeline execution for daily data
- **Reliability**: 99%+ pipeline completion rate
- **Maintainability**: Independent zone testing coverage >90%

## Conclusion

Your current system provides an excellent foundation for a modern data pipeline. The key insight is that you can evolve your existing architecture rather than rebuild it. Focus on adding the staging and curated layers while preserving your well-designed collection system.

The three-zone pipeline will provide the separation of concerns you need while maintaining the operational excellence you've already achieved in data collection.