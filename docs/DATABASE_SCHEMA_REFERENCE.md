# MLB Betting System - PostgreSQL Database Schema Reference

**Database**: `mlb_betting` (Port 5433)  
**Generated**: August 12, 2025  
**Total Tables**: 83 tables across 9 schemas  
**Architecture**: RAW → STAGING → CURATED data pipeline

---

## 📊 Database Overview

### Schema Summary
| Schema | Tables | Purpose | Key Tables |
|--------|--------|---------|------------|
| `raw_data` | 13 | Unprocessed data from external sources | action_network_odds (2,439 rows), vsin (265 rows) |
| `staging` | 9 | Cleaned, normalized data | spreads (1,057), moneylines (884), betting_odds_unified (8) |
| `curated` | 15 | Analysis-ready, enriched data | betting_lines_unified (7 rows) |
| `action_network` | 4 | Action Network specific tables | sportsbooks (6 rows) |
| `analytics` | 7 | Analysis results and recommendations | Empty (analytics pipeline) |
| `operational` | 10 | System operations and monitoring | Empty (monitoring pipeline) |
| `public` | 17 | MLflow tracking and pipeline logs | pipeline_execution_log (23 rows) |

### Total Data Volume
- **Largest Tables**: raw_data.action_network_odds (2.5MB), raw_data.vsin (936KB)
- **Most Active**: staging.spreads (1,057 rows), staging.moneylines (884 rows)
- **Storage**: ~8MB total across all schemas

---

## 🗄️ Core Data Tables (By Schema)

## RAW_DATA Schema
*Unprocessed data directly from external APIs and scrapers*

### `action_network_odds` ⭐ **PRIMARY DATA SOURCE**
**Rows**: 2,439 | **Size**: 2.5MB | **Purpose**: Action Network betting odds data

| Field | Type | Description |
|-------|------|-------------|
| `id` | bigint | Primary key |
| `external_game_id` | varchar(255) | Action Network game identifier |
| `sportsbook_key` | varchar(100) | Sportsbook identifier (15=FanDuel, 30=DraftKings, etc.) |
| `raw_odds` | jsonb | Complete odds data in JSON format |
| `collected_at` | timestamptz | When data was collected |
| `processed_at` | timestamptz | When data was processed (NULL = unprocessed) |

**Sample Data**: Contains moneyline, spread, and total betting odds from major sportsbooks including FanDuel, DraftKings, BetMGM, Caesars.

### `vsin` ⭐ **SHARP ACTION DATA**  
**Rows**: 265 | **Size**: 936KB | **Purpose**: VSIN sharp action analysis

| Field | Type | Description |
|-------|------|-------------|
| `id` | bigint | Primary key |
| `external_game_id` | varchar | Game identifier |
| `sport` | varchar | Sport type (MLB) |
| `sportsbook` | varchar | Sportsbook name |
| `betting_data` | jsonb | Complete betting analysis data |
| `sharp_action_detected` | boolean | Whether sharp action was detected |
| `data_quality_score` | numeric | Data quality assessment (0-1) |

### `action_network_history`
**Rows**: 26 | **Size**: 840KB | **Purpose**: Historical line movement data

| Field | Type | Description |
|-------|------|-------------|
| `id` | bigint | Primary key |
| `external_game_id` | varchar | Game identifier |
| `raw_history` | jsonb | Complete line movement history |
| `endpoint_url` | text | API endpoint used |

### `action_network_games`
**Rows**: 26 | **Size**: 288KB | **Purpose**: Game metadata and information

| Field | Type | Description |
|-------|------|-------------|
| `id` | bigint | Primary key |
| `external_game_id` | varchar | Game identifier |
| `raw_game_data` | jsonb | Complete game information |
| `game_date` | date | Game date |
| `home_team` | varchar | Home team name |
| `away_team` | varchar | Away team name |
| `game_status` | varchar | Game status (scheduled, in_progress, final) |

---

## STAGING Schema  
*Cleaned and normalized data ready for analysis*

### `betting_odds_unified` ⭐ **NEW UNIFIED MODEL**
**Rows**: 8 | **Size**: 304KB | **Purpose**: Unified staging table addressing all data model improvements

| Field | Type | Description |
|-------|------|-------------|
| `id` | bigint | Primary key |
| `data_source` | varchar | Source system (action_network, vsin, etc.) |
| `source_collector` | varchar | Specific collector used |
| `external_game_id` | varchar | Game identifier |
| `home_team` | varchar | Home team name (normalized) |
| `away_team` | varchar | Away team name (normalized) |
| `sportsbook_external_id` | varchar | External sportsbook identifier |
| `sportsbook_name` | varchar | Resolved sportsbook name |
| `market_type` | varchar | Primary market type (moneyline, spread, total) |
| `home_moneyline_odds` | integer | Home team moneyline odds |
| `away_moneyline_odds` | integer | Away team moneyline odds |
| `spread_line` | numeric | Spread line value |
| `home_spread_odds` | integer | Home team spread odds |
| `away_spread_odds` | integer | Away team spread odds |
| `total_line` | numeric | Total points line |
| `over_odds` | integer | Over bet odds |
| `under_odds` | integer | Under bet odds |
| `raw_data_table` | varchar | Source table reference |
| `raw_data_id` | bigint | Source record ID |
| `data_quality_score` | numeric | Quality assessment (0-1) |
| `validation_status` | varchar | Validation result (valid, invalid) |

**Key Features**: 
- **Source Attribution**: Tracks data_source and collector for complete lineage
- **Sportsbook Resolution**: Resolves external IDs to standardized names  
- **Team Information**: Normalized team names with validation
- **Bet Consolidation**: Single record contains all bet types for efficiency
- **Data Lineage**: Complete tracking from raw data to staging
- **Quality Metrics**: Comprehensive quality scoring and validation

### `spreads` ⭐ **LEGACY STAGING**
**Rows**: 1,057 | **Size**: 336KB | **Purpose**: Spread betting lines (legacy fragmented approach)

| Field | Type | Description |
|-------|------|-------------|
| `id` | bigint | Primary key |
| `raw_spreads_id` | bigint | Reference to raw data |
| `game_id` | varchar | Game identifier |
| `sportsbook_name` | varchar | Sportsbook name |
| `line_value` | numeric | Spread line value |
| `home_odds` | integer | Home team spread odds |
| `away_odds` | integer | Away team spread odds |
| `data_quality_score` | numeric | Quality score |

### `moneylines` ⭐ **LEGACY STAGING**
**Rows**: 884 | **Size**: 256KB | **Purpose**: Moneyline betting odds (legacy fragmented approach)

| Field | Type | Description |
|-------|------|-------------|
| `id` | bigint | Primary key |
| `raw_moneylines_id` | bigint | Reference to raw data |
| `game_id` | varchar | Game identifier |
| `sportsbook_name` | varchar | Sportsbook name |
| `home_odds` | integer | Home team moneyline odds |
| `away_odds` | integer | Away team moneyline odds |

### `totals` ⭐ **LEGACY STAGING**
**Rows**: 884 | **Size**: 312KB | **Purpose**: Total (over/under) betting lines (legacy fragmented approach)

| Field | Type | Description |
|-------|------|-------------|
| `id` | bigint | Primary key |
| `raw_totals_id` | bigint | Reference to raw data |
| `game_id` | varchar | Game identifier |
| `sportsbook_name` | varchar | Sportsbook name |
| `line_value` | numeric | Total points line |
| `over_odds` | integer | Over bet odds |
| `under_odds` | integer | Under bet odds |

---

## CURATED Schema
*Analysis-ready data with enrichments and validations*

### `betting_lines_unified`
**Rows**: 7 | **Size**: 112KB | **Purpose**: Final curated betting lines for analysis

| Field | Type | Description |
|-------|------|-------------|
| `id` | integer | Primary key |
| `game_id` | varchar | Game identifier |
| `sportsbook` | varchar | Sportsbook name |
| `market_type` | varchar | Bet type |
| `home_ml` | integer | Home moneyline |
| `away_ml` | integer | Away moneyline |
| `home_spread` | numeric | Home spread |
| `away_spread` | numeric | Away spread |
| `total_line` | numeric | Total line |
| `data_quality` | varchar | Quality rating (HIGH, MEDIUM, LOW) |
| `source_reliability_score` | numeric | Source reliability (0-1) |
| `odds_timestamp` | timestamptz | When odds were effective |

---

## ACTION_NETWORK Schema
*Action Network specific operational tables*

### `sportsbooks` ⭐ **REFERENCE DATA**
**Rows**: 6 | **Size**: 40KB | **Purpose**: Sportsbook master data

| Field | Type | Description |
|-------|------|-------------|
| `id` | integer | Primary key |
| `book_id` | integer | Action Network book ID |
| `name` | varchar | Sportsbook name |
| `display_name` | varchar | Display name |
| `is_active` | boolean | Whether sportsbook is active |

**Current Sportsbooks**: Includes major books like FanDuel (15), DraftKings (30), BetMGM (68), Caesars (69)

---

## ANALYTICS Schema  
*Analysis results and recommendations (Pipeline Ready)*

### Key Tables (Currently Empty - Analysis Pipeline)
- `betting_recommendations`: Generated betting recommendations with confidence scores
- `strategy_signals`: Strategy-specific signals and recommendations  
- `performance_metrics`: Strategy and system performance tracking
- `roi_calculations`: Return on investment calculations by strategy
- `confidence_scores`: Multi-factor confidence scoring
- `cross_market_analysis`: Cross-market opportunity analysis
- `timing_analysis_results`: Timing-based betting analysis

---

## OPERATIONAL Schema
*System operations and monitoring (Pipeline Ready)*

### Key Tables (Currently Empty - Monitoring Pipeline)
- `pipeline_execution_logs`: Pipeline execution tracking
- `data_quality_metrics`: Ongoing data quality monitoring
- `system_health_checks`: System health and performance monitoring
- `strategy_performance`: Strategy performance tracking
- `alert_configurations`: System alerting configuration
- `pre_game_recommendations`: Pre-game betting recommendations

---

## PUBLIC Schema
*MLflow tracking and system logs*

### `pipeline_execution_log` ⭐ **ACTIVE MONITORING**
**Rows**: 23 | **Size**: 112KB | **Purpose**: Pipeline execution tracking

| Field | Type | Description |
|-------|------|-------------|
| `execution_id` | uuid | Unique execution identifier |
| `pipeline_name` | varchar | Pipeline name |
| `zone` | varchar | Pipeline zone (raw, staging, curated) |
| `status` | varchar | Execution status (running, completed, failed) |
| `records_processed` | integer | Number of records processed |
| `records_successful` | integer | Successfully processed records |
| `records_failed` | integer | Failed records |
| `started_at` | timestamptz | Pipeline start time |
| `completed_at` | timestamptz | Pipeline completion time |

### MLflow Tracking Tables
- `experiments`: ML experiment tracking (1 experiment)
- `runs`: ML run tracking  
- `metrics`: ML metrics storage
- `params`: ML parameter storage
- Various MLflow metadata tables for model versioning and tracking

---

## 🔄 Data Flow Architecture

### Current Pipeline Flow
```
RAW DATA (External Sources)
├── action_network_odds (2,439 records) → API calls to Action Network
├── vsin (265 records) → VSIN sharp action data  
├── action_network_history (26 records) → Historical line movements
└── action_network_games (26 records) → Game metadata

↓ STAGING PROCESSING ↓

STAGING (Cleaned & Normalized)  
├── betting_odds_unified (8 records) ← NEW UNIFIED MODEL
├── spreads (1,057 records) ← Legacy fragmented approach
├── moneylines (884 records) ← Legacy fragmented approach
└── totals (884 records) ← Legacy fragmented approach

↓ CURATION PROCESSING ↓

CURATED (Analysis Ready)
└── betting_lines_unified (7 records) → Final analysis data

↓ ANALYTICS PROCESSING ↓

ANALYTICS (Insights & Recommendations)
└── [Pipeline Ready - Tables Created, Processing TBD]
```

### Data Quality Pipeline
1. **Collection**: Raw data collected from multiple sources with timestamps
2. **Validation**: Data quality scoring, validation status tracking
3. **Normalization**: Team names normalized, sportsbook IDs resolved
4. **Consolidation**: Multiple bet types consolidated into unified records
5. **Enrichment**: Enhanced with metadata, lineage tracking, quality metrics
6. **Analysis**: Ready for strategy processing and recommendation generation

---

## 🔧 Database Management

### Key Indexes
- Primary key indexes on all tables
- Foreign key indexes for referential integrity
- Processed_at indexes for pipeline efficiency
- Game_id indexes for game-based queries

### Constraints & Validation
- NOT NULL constraints on critical fields
- Foreign key relationships maintained between schemas
- Check constraints for data quality validation
- Unique constraints preventing duplicates

### Performance Characteristics
- **Read Performance**: Optimized for analytical queries
- **Write Performance**: Batch processing optimized
- **Storage Efficiency**: JSON compression for complex data
- **Query Patterns**: Time-series and game-based analysis optimized

---

## 📈 Current Status & Next Steps

### ✅ Implemented
- **Raw Data Collection**: Action Network, VSIN, historical data
- **Staging Pipeline**: Both legacy fragmented and new unified approaches
- **Data Quality**: Comprehensive quality scoring and validation
- **Monitoring**: Pipeline execution logging and tracking

### 🚧 In Progress  
- **Analytics Pipeline**: Tables created, processing logic in development
- **Curated Enrichment**: Enhanced with additional data sources
- **Strategy Integration**: Connecting analysis results to betting strategies

### 📋 Planned
- **Real-time Processing**: Live data ingestion and processing
- **Advanced Analytics**: ML model integration and prediction
- **Performance Optimization**: Query optimization and indexing improvements
- **Data Retention**: Archival and cleanup policies

---

**Last Updated**: August 12, 2025  
**Database Version**: PostgreSQL 13+  
**Total Tables**: 83 across 9 schemas  
**Active Data**: ~8MB with 6,000+ records across key tables