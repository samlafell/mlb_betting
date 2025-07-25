# 🚀 Pipeline Implementation Roadmap
## RAW → STAGING → CURATED Data Pipeline Evolution

## Current State Assessment

Your system already has **excellent schema separation** that closely aligns with modern data pipeline patterns:

### ✅ **Existing Schema Structure** (Already Pipeline-Ready!)

```sql
1. raw_data      →  RAW ZONE ✅ 
2. core_betting  →  STAGING ZONE ✅ (needs minor enhancements)
3. analytics     →  CURATED ZONE ✅ 
4. operational   →  METADATA ZONE ✅
```

**Key Insight**: You don't need a complete redesign! Your current `consolidated_schema.sql` already implements the pipeline pattern with:
- **raw_data**: Perfect RAW zone with external API responses
- **core_betting**: Well-designed STAGING zone with cleaned data
- **analytics**: Strong CURATED zone for derived insights
- **operational**: Excellent metadata management

## 📋 Implementation Roadmap

### **Phase 1: Pipeline Configuration** (Week 1)
*Enhance existing system with pipeline orchestration*

#### **1.1 Update Configuration**
```toml
# config.toml - Add pipeline management
[pipeline]
enabled = true
mode = "full"  # Options: raw_only, staging_only, full

[pipeline.zones]
raw = "raw_data"           # ✅ Already exists
staging = "core_betting"   # ✅ Already exists  
curated = "analytics"      # ✅ Already exists
operational = "operational" # ✅ Already exists

[pipeline.processing]
auto_promotion = true      # Auto-move data through zones
parallel_processing = true # Process zones in parallel when possible
quality_gates = true       # Validate before zone promotion

[pipeline.retention]
raw_retention_days = 90
staging_retention_days = 365
curated_retention_days = -1  # Keep forever
```

#### **1.2 Enhance Core Configuration Class**
```python
# src/core/config.py - Add pipeline configuration
class PipelineSettings(BaseSettings):
    """Pipeline configuration for multi-zone processing"""
    
    enabled: bool = True
    mode: str = "full"  # raw_only, staging_only, full
    auto_promotion: bool = True
    parallel_processing: bool = True
    quality_gates: bool = True
    
    # Zone mappings
    raw_schema: str = "raw_data"
    staging_schema: str = "core_betting" 
    curated_schema: str = "analytics"
    operational_schema: str = "operational"
    
    # Processing settings
    batch_size: int = 1000
    max_parallel_jobs: int = 4
    quality_threshold: float = 0.8
```

### **Phase 2: Pipeline Orchestrator** (Week 2)
*Build multi-zone processing coordination*

#### **2.1 Create Pipeline Orchestrator**
```python
# src/services/pipeline/pipeline_orchestrator.py
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional

class PipelineZone(Enum):
    RAW = "raw_data"
    STAGING = "core_betting"
    CURATED = "analytics"
    OPERATIONAL = "operational"

@dataclass
class PipelineResult:
    zone: PipelineZone
    records_processed: int
    quality_score: float
    processing_time: float
    errors: List[str]

class DataPipelineOrchestrator:
    """Orchestrates data flow through RAW → STAGING → CURATED zones"""
    
    def __init__(self, settings: PipelineSettings):
        self.settings = settings
        # Use your existing collectors and repositories
        self.data_collectors = YourExistingOrchestrator()
        self.staging_processor = StagingProcessor()
        self.curated_builder = CuratedAnalyticsBuilder()
    
    async def run_full_pipeline(self, date_range: DateRange) -> Dict[PipelineZone, PipelineResult]:
        """Run complete RAW → STAGING → CURATED pipeline"""
        results = {}
        
        try:
            # Zone 1: RAW Collection (your existing system)
            if self.settings.mode in ["raw_only", "full"]:
                raw_result = await self._run_raw_collection(date_range)
                results[PipelineZone.RAW] = raw_result
                
                if not self._meets_quality_gate(raw_result):
                    raise PipelineQualityError("Raw data quality below threshold")
            
            # Zone 2: STAGING Processing  
            if self.settings.mode in ["staging_only", "full"]:
                staging_result = await self._run_staging_processing(date_range)
                results[PipelineZone.STAGING] = staging_result
                
                if not self._meets_quality_gate(staging_result):
                    raise PipelineQualityError("Staging data quality below threshold")
            
            # Zone 3: CURATED Analytics
            if self.settings.mode == "full":
                curated_result = await self._run_curated_building(date_range)
                results[PipelineZone.CURATED] = curated_result
            
            return results
            
        except Exception as e:
            await self._handle_pipeline_failure(e, results)
            raise
    
    async def _run_raw_collection(self, date_range) -> PipelineResult:
        """Zone 1: Use your existing collection orchestrator"""
        start_time = time.time()
        
        # Use your existing collectors - no changes needed!
        collection_results = await self.data_collectors.collect_all_sources(
            date_range=date_range,
            target_schema="raw_data"  # Route to raw zone
        )
        
        return PipelineResult(
            zone=PipelineZone.RAW,
            records_processed=sum(r.record_count for r in collection_results),
            quality_score=self._calculate_quality_score(collection_results),
            processing_time=time.time() - start_time,
            errors=[]
        )
    
    async def _run_staging_processing(self, date_range) -> PipelineResult:
        """Zone 2: Clean and normalize raw data into core_betting schema"""
        # This processes raw_data → core_betting (your existing target)
        # Add data cleaning, validation, team normalization
        pass
    
    async def _run_curated_building(self, date_range) -> PipelineResult:
        """Zone 3: Build analytics features from staging data"""
        # This processes core_betting → analytics 
        # Add feature engineering, strategy signals, ML features
        pass
```

#### **2.2 Enhanced CLI Commands**
```bash
# New pipeline commands
uv run -m src.interfaces.cli pipeline run --zone raw --date today
uv run -m src.interfaces.cli pipeline run --zone staging --date today  
uv run -m src.interfaces.cli pipeline run --zone curated --date today
uv run -m src.interfaces.cli pipeline run --full --date today

# Pipeline monitoring
uv run -m src.interfaces.cli pipeline status
uv run -m src.interfaces.cli pipeline quality --zone all
uv run -m src.interfaces.cli pipeline health

# Zone-specific operations  
uv run -m src.interfaces.cli data status --zone staging
uv run -m src.interfaces.cli data promote --from raw --to staging --date today
```

### **Phase 3: Staging Zone Enhancement** (Week 3)
*Enhance core_betting with proper staging capabilities*

#### **3.1 Add Staging Processing Logic**
```python
# src/services/pipeline/staging_processor.py
class StagingProcessor:
    """Process raw_data into clean core_betting records"""
    
    async def process_raw_to_staging(self, date_range: DateRange):
        """Main staging processing workflow"""
        
        # Step 1: Data validation and cleaning
        validated_data = await self._validate_raw_data(date_range)
        
        # Step 2: Team name standardization (your existing logic)
        normalized_data = await self._normalize_team_names(validated_data)
        
        # Step 3: Duplicate detection and resolution
        deduplicated_data = await self._deduplicate_records(normalized_data)
        
        # Step 4: Quality scoring
        scored_data = await self._assign_quality_scores(deduplicated_data)
        
        # Step 5: Insert into core_betting schema
        await self._insert_to_staging(scored_data)
        
        return StagingResult(...)
    
    async def _validate_raw_data(self, date_range):
        """Validate raw data quality and completeness"""
        # Check for required fields
        # Validate data formats
        # Flag anomalies
        
    async def _normalize_team_names(self, data):
        """Use your existing team normalization logic"""
        # Your current team_utils.py logic fits perfectly here
        
    async def _deduplicate_records(self, data):
        """Remove duplicates using business logic"""
        # Game ID matching
        # Timestamp-based deduplication
        # Source priority handling
```

#### **3.2 Quality Gates and Validation**
```python
# src/services/pipeline/quality_gates.py
class PipelineQualityGate:
    """Validate data quality before zone promotion"""
    
    def __init__(self, zone: PipelineZone, thresholds: QualityThresholds):
        self.zone = zone
        self.thresholds = thresholds
    
    async def validate(self, data_batch: DataBatch) -> QualityResult:
        """Run quality checks for zone promotion"""
        
        checks = [
            self._check_completeness(data_batch),
            self._check_accuracy(data_batch), 
            self._check_consistency(data_batch),
            self._check_timeliness(data_batch)
        ]
        
        results = await asyncio.gather(*checks)
        overall_score = self._calculate_overall_score(results)
        
        return QualityResult(
            passed=overall_score >= self.thresholds.minimum_score,
            score=overall_score,
            check_results=results
        )
```

### **Phase 4: Curated Zone Enhancement** (Week 4)
*Transform analytics schema into full feature engineering zone*

#### **4.1 Feature Engineering Pipeline**
```python
# src/services/pipeline/curated_builder.py
class CuratedAnalyticsBuilder:
    """Build analysis-ready features from staging data"""
    
    async def build_curated_features(self, date_range: DateRange):
        """Main feature engineering workflow"""
        
        # Step 1: Sharp action features (your existing logic enhanced)
        sharp_features = await self._build_sharp_action_features(date_range)
        
        # Step 2: Line movement features
        movement_features = await self._build_movement_features(date_range)
        
        # Step 3: Consensus features
        consensus_features = await self._build_consensus_features(date_range)
        
        # Step 4: Historical performance features
        historical_features = await self._build_historical_features(date_range)
        
        # Step 5: ML-ready feature vectors
        ml_features = await self._build_ml_feature_vectors([
            sharp_features, movement_features, 
            consensus_features, historical_features
        ])
        
        # Step 6: Insert into analytics schema
        await self._insert_curated_features(ml_features)
        
        return CuratedResult(...)
    
    async def _build_sharp_action_features(self, date_range):
        """Enhanced sharp action detection with historical patterns"""
        # Use your existing sharp action processors
        # Add historical pattern matching
        # Include confidence scoring
        
    async def _build_movement_features(self, date_range):
        """Line movement analysis with RLM detection"""
        # Use your existing movement analysis
        # Add steam move detection
        # Include velocity calculations
```

#### **4.2 ML Feature Store**
```sql
-- Add to analytics schema for ML features
CREATE TABLE analytics.feature_vectors (
    id SERIAL PRIMARY KEY,
    game_id INTEGER REFERENCES curated.games_complete(id),
    feature_set_version VARCHAR(10) NOT NULL,
    
    -- Sharp action features
    sharp_action_score FLOAT,
    reverse_line_movement_indicator BOOLEAN,
    steam_move_detected BOOLEAN,
    
    -- Movement features  
    line_velocity FLOAT,
    movement_consistency_score FLOAT,
    cross_book_consensus FLOAT,
    
    -- Historical features
    team_sharp_action_history FLOAT,
    pitcher_sharp_action_history FLOAT,
    
    -- Target variables (for ML training)
    actual_outcome VARCHAR(10),
    profit_loss FLOAT,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

### **Phase 5: Integration and Testing** (Week 5)
*Full pipeline integration with your existing system*

#### **5.1 Backward Compatibility**
```python
# Ensure existing CLI commands still work
# uv run -m src.interfaces.cli data collect --source action_network --real
# Routes to: pipeline run --zone raw --source action_network

# uv run -m src.interfaces.cli analysis run
# Routes to: pipeline run --zone curated --analysis-type all
```

#### **5.2 Performance Testing**
```python
# src/tests/pipeline/test_pipeline_performance.py
class TestPipelinePerformance:
    
    async def test_full_pipeline_performance(self):
        """Test complete pipeline execution time"""
        # Target: <5 minutes for daily data processing
        
    async def test_zone_parallel_processing(self):
        """Test parallel zone processing"""
        # Verify zones can process independently
        
    async def test_quality_gate_performance(self):
        """Test quality validation speed"""
        # Target: <30 seconds for quality checks
```

## 🎯 Success Metrics & Monitoring

### **Pipeline Health Dashboard**
```python
# Add to your existing monitoring
class PipelineHealthMonitor:
    
    def get_zone_health(self, zone: PipelineZone) -> ZoneHealth:
        """Monitor individual zone health"""
        
    def get_pipeline_latency(self) -> PipelineLatency:
        """Track end-to-end processing time"""
        
    def get_quality_trends(self) -> QualityTrends:
        """Monitor data quality over time"""
```

### **Key Performance Indicators**
- **Latency**: Raw→Staging: <2min, Staging→Curated: <3min
- **Quality**: >95% records pass quality gates
- **Reliability**: >99% pipeline completion rate
- **Throughput**: Process 50+ games/day with 1000+ betting records

## 🔄 Migration Strategy

### **Zero-Downtime Migration**
1. **Week 1**: Add pipeline configuration (no behavior change)
2. **Week 2**: Deploy orchestrator with current system fallback
3. **Week 3**: Enable staging processing (parallel to existing)
4. **Week 4**: Enable curated processing (parallel to existing)
5. **Week 5**: Full pipeline switchover with monitoring

### **Rollback Plan**
- Keep existing collectors and analysis working unchanged
- Pipeline can be disabled via configuration
- Individual zones can be bypassed if needed
- Full rollback capability to current system

## 📈 Business Value

### **Immediate Benefits**
- **Data Quality**: Systematic validation and cleaning
- **Reliability**: Error isolation and recovery
- **Traceability**: Complete data lineage tracking

### **Long-term Benefits**  
- **Scalability**: Independent zone scaling
- **ML Readiness**: Feature engineering pipeline
- **Analytics Speed**: Pre-computed feature store
- **Development Velocity**: Clear separation of concerns

## 🚀 Getting Started

### **This Week: Phase 1 Implementation**
```bash
# 1. Update config.toml with pipeline settings
# 2. Add PipelineSettings to core/config.py
# 3. Create basic pipeline orchestrator
# 4. Test with single zone (raw) first
```

Your system is already 80% of the way to a modern data pipeline! The key is evolving your excellent existing architecture rather than rebuilding it.