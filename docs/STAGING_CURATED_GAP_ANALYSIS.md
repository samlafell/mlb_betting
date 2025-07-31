# STAGING → CURATED Data Processing Gap Analysis

Generated: July 30, 2025

## Executive Summary

**Pipeline Status**: 🔴 **CRITICAL GAPS IDENTIFIED**

- **STAGING Zone**: Rich and current (88,975 recent odds records, 112 games)
- **CURATED Zone**: Stale and empty (0 recent records, 210+ hour lag)  
- **Game Coverage**: 0% (0/112 recent games processed)
- **Processing Pipeline**: BROKEN (STAGING → CURATED flow is non-functional)

## Key Findings

### ✅ **STAGING Zone - Working Well**

**📈 Rich Odds Data Available:**
- **Total**: 88,975 historical odds records
- **Recent (7d)**: 88,975 records (100% fresh)
- **Today (24h)**: 12,884 records (active collection)
- **Games**: 112 unique games tracked
- **Sportsbooks**: 6 major books (DraftKings, FanDuel, etc.)
- **Markets**: Complete coverage (moneyline, spread, total)

**🏟️ Staging Games Data:**
- **Total**: 15 games with complete metadata
- **Teams**: 15 unique home teams, 15 away teams
- **Status**: Games table has good historical data but no recent updates

**Market Coverage Breakdown (7d):**
```
moneyline.away:  12,359 records (112 games, 6 books)
moneyline.home:  14,371 records (112 games, 6 books)  
spread.away:     20,224 records (112 games, 6 books)
spread.home:     18,307 records (112 games, 6 books)
total.over:      11,799 records (112 games, 6 books)
total.under:     11,915 records (112 games, 6 books)
```

### 🔴 **CURATED Zone - Critical Gaps**

**🏟️ Enhanced Games:**
- **Total**: 1,391 historical games (from July 21st)
- **Recent (7d)**: **0 games** ❌
- **MLB Stats API IDs**: 0 linked
- **Action Network IDs**: 0 linked  
- **Feature Data**: Historical only, no recent processing

**📊 Unified Betting Splits:**
- **Total**: **0 records** ❌
- **Recent**: **0 records** ❌
- **Status**: Table exists but completely empty

**🤖 ML Temporal Features:**
- **Total**: **0 records** ❌  
- **Recent**: **0 records** ❌
- **ML Valid Features (≥60min)**: **0 records** ❌
- **Status**: Table exists but completely empty

## Critical Pipeline Breaks

### 1. **Game Processing Pipeline: BROKEN**
```
Status: 0% game coverage (0/112 recent games processed)
Impact: No recent games available for ML analysis
Root Cause: STAGING → CURATED game processing pipeline not running
```

**Evidence:**
- STAGING: 112 unique games with rich odds data (last 7 days)
- CURATED: 0 recent games processed
- Latest STAGING data: July 30, 2025 15:51:56 UTC
- Latest CURATED data: July 21, 2025 21:47:45 UTC
- **Processing lag: 210.1 hours (8.75 days)**

### 2. **Betting Splits Aggregation: MISSING**
```
Status: No unified betting splits pipeline
Impact: Cannot detect sharp action or public sentiment
Root Cause: Missing implementation of SBD + VSIN → unified_betting_splits
```

**Evidence:**
- Rich STAGING odds data from multiple sources
- Empty curated.unified_betting_splits table
- No cross-source betting percentage aggregation

### 3. **ML Feature Generation: MISSING**
```
Status: No ML temporal features generated
Impact: Cannot run ML models or backtesting
Root Cause: Missing ml_temporal_features pipeline implementation
```

**Evidence:**
- Empty curated.ml_temporal_features table
- No temporal feature extraction from STAGING odds
- Missing 60-minute ML data cutoff enforcement

## Root Cause Analysis

### **Missing Pipeline Components**

1. **Enhanced Games Population Service**
   - No service to process staging.action_network_games → curated.enhanced_games
   - Missing game metadata enrichment
   - No cross-system ID linking (MLB Stats API, Action Network)

2. **Betting Splits Aggregation Service**
   - No service to process multi-source betting splits
   - Missing SBD data integration
   - No VSIN sharp action detection

3. **ML Feature Generation Service**  
   - No temporal feature extraction from staging odds
   - Missing line movement velocity calculations
   - No sharp action intensity scoring

4. **STAGING → CURATED Orchestrator**
   - No automated pipeline orchestration
   - Missing scheduled processing
   - No real-time or batch processing coordination

## Implementation Requirements

### **Immediate (HIGH Priority)**

**1. Implement Enhanced Games Pipeline**
```bash
# Required: Service to process recent games
FROM: staging.action_network_games + staging.action_network_odds_historical  
TO: curated.enhanced_games
FEATURES: Cross-system ID linking, metadata enrichment, feature data population
```

**2. Create STAGING → CURATED Orchestrator**
```bash  
# Required: Automated pipeline coordination
FUNCTION: Coordinate all STAGING → CURATED processing
SCHEDULE: Real-time or 15-minute intervals
MONITORING: Processing lag alerts, failure detection
```

**3. Process Current Backlog**
```bash
# Required: Process 112 recent games immediately
SCOPE: Games from last 7 days with rich odds data
PRIORITY: Critical for system functionality
```

### **Short-term (MEDIUM Priority)**

**4. Implement Unified Betting Splits Service**
```bash
# Required: Multi-source betting splits aggregation
FROM: SBD data + VSIN data + Action Network betting percentages
TO: curated.unified_betting_splits  
FEATURES: Sharp action detection, public sentiment tracking
```

**5. Implement ML Temporal Features Service**
```bash
# Required: ML-ready feature generation
FROM: staging.action_network_odds_historical (temporal analysis)
TO: curated.ml_temporal_features
FEATURES: Line movement velocity, sharp action intensity, 60-min cutoff
```

**6. Add Pipeline Monitoring & Alerting**
```bash
# Required: Production monitoring
ALERTS: Processing lag > 1 hour, pipeline failures, data quality issues
DASHBOARD: Real-time STAGING → CURATED processing status
```

### **Long-term (LOW Priority)**

**7. Real-time Streaming Pipeline**
```bash
# Optional: Real-time feature updates
SCOPE: Live game feature updates during games
TECH: WebSocket or message queue integration
```

**8. Cross-Source Data Validation**
```bash
# Optional: Data quality and consistency checks
SCOPE: Validate data across Action Network, SBD, VSIN
FEATURES: Anomaly detection, data completeness scoring
```

## Expected Outcomes After Implementation

### **Data Volume Projections**
- **Enhanced Games**: 112 recent games (from 0)
- **Unified Betting Splits**: ~1,000+ split records (from 0)  
- **ML Temporal Features**: ~2,000+ feature records (from 0)
- **Processing Lag**: <1 hour (from 210+ hours)

### **Business Impact**
- **✅ Enable ML Model Training**: Real-time features available
- **✅ Enable Backtesting**: Historical feature completeness
- **✅ Enable Sharp Action Detection**: Multi-source betting splits
- **✅ Enable Real-time Analysis**: Fresh data pipeline

## Technical Architecture Recommendations

### **Service Layer Implementation**
```python
# New services needed:
src/services/curated_zone/
├── enhanced_games_service.py          # Game metadata enrichment
├── betting_splits_aggregator.py      # Multi-source splits processing  
├── ml_temporal_features_service.py   # ML feature generation
└── staging_curated_orchestrator.py   # Pipeline coordination
```

### **CLI Integration**
```bash
# New CLI commands needed:
uv run -m src.interfaces.cli pipeline run --zone curated
uv run -m src.interfaces.cli curated process-games --recent
uv run -m src.interfaces.cli curated generate-features --games recent
```

### **Database Optimization**
```sql
-- Indexes needed for performance:
CREATE INDEX idx_staging_odds_game_sportsbook_market ON staging.action_network_odds_historical(external_game_id, sportsbook_name, market_type);
CREATE INDEX idx_curated_games_an_id ON curated.enhanced_games(action_network_game_id);
CREATE INDEX idx_curated_features_game_cutoff ON curated.ml_temporal_features(game_id, feature_cutoff_time);
```

## Next Steps

### **Phase 1: Critical Gap Resolution (Days 1-3)**
1. **Implement enhanced_games_service.py** - Process 112 recent games
2. **Create staging_curated_orchestrator.py** - Automate pipeline
3. **Add CLI commands** - Enable manual processing and monitoring

### **Phase 2: Feature Completeness (Days 4-7)**  
4. **Implement ml_temporal_features_service.py** - Generate ML features
5. **Implement betting_splits_aggregator.py** - Process betting splits
6. **Add comprehensive testing** - Validate all STAGING → CURATED flows

### **Phase 3: Production Readiness (Days 8-14)**
7. **Add monitoring and alerting** - Pipeline health monitoring
8. **Performance optimization** - Indexing and query optimization  
9. **Documentation updates** - Update USER_GUIDE.md and README.md

---

**This analysis reveals that while the STAGING zone is rich with current data (88,975+ recent records), the CURATED zone is completely disconnected with 0% recent game coverage and 210+ hour processing lag. The core issue is missing STAGING → CURATED pipeline services, not data availability.**