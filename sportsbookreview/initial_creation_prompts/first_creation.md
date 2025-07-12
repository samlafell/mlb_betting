# SportsbookReview.com Historical Odds Integration

## 🎯 **Objective**
Develop a robust data integration system to scrape, parse, and store historical odds data from SportsbookReview.com, seamlessly integrating with our existing sports betting platform architecture and MLB Stats API for comprehensive game data enrichment.

## 📋 **Project Overview**

**Context**: Based on the detailed analysis in `@adding_historical_odds.md`, we need to build a comprehensive system that:
- Scrapes historical MLB odds data (moneyline, spread, totals) from **Sunday, April 4th through current date**
- Supports multiple sportsbooks (DraftKings, FanDuel, Caesars, etc.)
- **Pairs each game with MLB Stats API data** for complete game context (Game ID, weather, venue, pitcher info, etc.)
- Integrates with our existing platform using established patterns
- **Leverages existing `game_updater.py` service** for game results and outcome calculations
- Maintains data isolation in the `sportsbookreview/` folder structure

---

## 🚀 **Phase 1: Research & Architecture Design**

### **Core Deliverables**
1. **Enhanced Data Models** (`sportsbookreview/models/`)
   - `Game` model with **MLB Stats API integration fields**:
     - `mlb_game_id` (gamePk from MLB API)
     - `venue_info` (stadium, weather conditions, attendance)
     - `pitcher_matchup` (starting pitchers, handedness)
     - `game_context` (series info, playoff status, etc.)
   - `OddsData` model for moneyline, spread, totals with **temporal tracking**
   - `SportsbookMapping` model for book identification and odds format normalization
   - `LineMovement` model for historical tracking with **MLB context correlation**

2. **Service Architecture Blueprint**
   - **MLB Stats API Integration Service** using existing `@mlb_stats_api_endpoints.mdc` patterns
   - **Game Correlation Service** to match SportsbookReview games with MLB Game IDs
   - **Existing Game Updater Integration** - leverage `@game_updater.py` for results
   - Define scraper service interfaces with rate limiting
   - Design parser service for HTML processing
   - Plan integration points with existing `models/` and `services/`
   - Document error handling and retry strategies

3. **Technical Specifications**
   - **Date Range Coverage**: **Sunday, April 4th, 2021** through **current date**
   - **MLB API Integration Contracts** with comprehensive game data fetching
   - **Game Results Sourcing Strategy**:
     - Primary: Existing `game_updater.py` service (already tested and reliable)
     - Secondary: SportsbookReview final scores (for validation/backup)
     - Tertiary: Direct MLB Stats API calls for missing data
   - PostgreSQL schema design with **MLB data enrichment tables**
   - Performance requirements and constraints

---

## 🔧 **Phase 2: Implementation**

### **Core Components**

#### **Models Layer** (`sportsbookreview/models/`)
- **Enhanced Game Model** with MLB Stats API fields:
  ```python
  class EnhancedGame(BaseModel):
      # SportsbookReview data
      sbr_game_id: str
      home_team: Team
      away_team: Team
      game_date: datetime
      
      # MLB Stats API enrichment
      mlb_game_id: Optional[str] = None
      venue_id: Optional[int] = None
      venue_name: Optional[str] = None
      weather_conditions: Optional[Dict[str, Any]] = None
      attendance: Optional[int] = None
      starting_pitchers: Optional[Dict[str, Any]] = None
      game_context: Optional[Dict[str, Any]] = None
  ```
- Comprehensive validation and type safety
- Integration with existing team mapping services

#### **Services Layer** (`sportsbookreview/services/`)
- **`MLBDataEnrichmentService`** - Core MLB Stats API integration
  - Game ID correlation and matching
  - Weather, venue, and context data fetching
  - Pitcher matchup information
  - Integration with existing `@mlb_stats_api_endpoints.mdc` patterns
- **`GameResultsOrchestrator`** - Coordinated results fetching
  - Primary: Leverage existing `game_updater.py` methods
  - Secondary: SportsbookReview scraping for validation
  - Conflict resolution and data quality checks
- `SportsbookReviewScraper` with session management
- `OddsParser` for HTML data extraction
- `DataTransformer` for odds normalization
- `HistoricalOddsService` for business logic

#### **Parser Layer** (`sportsbookreview/parsers/`)
- Page-specific parsers (moneyline, spread, totals)
- **MLB Game ID extraction and correlation**
- Team name resolution using existing mappers
- **Date range processing** (April 4th → current)
- Robust error handling for missing data

#### **Database Integration**
- **Enhanced PostgreSQL schema** with MLB enrichment tables:
  ```sql
  -- Enhanced games table with MLB data
  CREATE TABLE sportsbookreview_games (
      id SERIAL PRIMARY KEY,
      sbr_game_id VARCHAR(50) UNIQUE NOT NULL,
      mlb_game_id VARCHAR(20),
      home_team VARCHAR(3) NOT NULL,
      away_team VARCHAR(3) NOT NULL,
      game_date TIMESTAMP WITH TIME ZONE NOT NULL,
      venue_id INTEGER,
      venue_name VARCHAR(255),
      weather_conditions JSONB,
      attendance INTEGER,
      starting_pitchers JSONB,
      game_context JSONB,
      created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
  );
  ```
- Repository pattern implementation
- Migration scripts for seamless deployment

---

## 🧪 **Phase 3: Testing & Validation**

### **Testing Strategy**
1. **Unit Tests** - 90%+ coverage for all services
2. **Integration Tests** - End-to-end data flow validation
3. **MLB API Integration Tests** - Validate game correlation accuracy
4. **Historical Data Validation** - Spot-check April 4th → current date coverage
5. **Game Results Validation** - Compare SportsbookReview vs existing `game_updater.py` results
6. **Demo Scripts** - Iterative testing with real data
7. **Performance Tests** - Rate limiting and error scenarios

### **Validation Criteria**
- Successfully scrape data for **complete April 4th → current date range**
- **98%+ MLB Game ID correlation accuracy**
- Parse odds with 99%+ accuracy
- Handle rate limits gracefully
- Maintain sub-2 second API response times
- **Weather and venue data coverage >95%**

---

## 📊 **Technical Requirements**

### **Architecture Standards**
- ✅ Follow existing platform patterns (`models/`, `services/`, `parsers/`)
- ✅ Isolate in `sportsbookreview/` folder
- ✅ Use dependency injection for testability
- ✅ Implement comprehensive logging
- ✅ **Integrate with existing `game_updater.py` service patterns**

### **Data Processing**
- ✅ Support all bet types (moneyline, spread, totals)
- ✅ Handle multiple sportsbooks dynamically
- ✅ Convert all times to EST timezone
- ✅ Normalize odds format consistently (+/- signs)
- ✅ **Comprehensive MLB Stats API data enrichment**
- ✅ **Complete historical coverage: April 4th, 2021 → current**

### **MLB Stats API Integration**
- ✅ **Game ID correlation and matching**
- ✅ **Weather and venue data fetching**
- ✅ **Pitcher matchup information**
- ✅ **Game context (series, playoff status)**
- ✅ **Rate limiting compliance with MLB API**
- ✅ **Fallback handling for missing MLB data**

### **Integration Points**
- ✅ Extend existing team mapping service
- ✅ Use established database connection patterns
- ✅ Follow platform error handling conventions
- ✅ Integrate with existing logging system
- ✅ **Leverage existing `game_updater.py` for game results**

---

## ❓ **Key Questions to Address**

1. **Data Scope**: ✅ **Confirmed: April 4th, 2021 → current date**
2. **Update Frequency**: How often should we refresh historical data? - should be one backfill and then daily we should update
3. **Sportsbook Priority**: Which books are most critical for initial release? - no order of importance
4. **Performance Thresholds**: What are acceptable response times and error rates? - 
5. **Integration Depth**: How should this connect to existing betting analysis workflows? - currently it should be disconnected
6. **MLB Data Priority**: Which MLB Stats API endpoints are most critical for analysis? - the MLB Game Id is very important, another endpoint we want to look at game_contextMetrics (i'm not sure exactly what it has but docs said it would be interesting)
7. **Game Results Strategy**: Primary reliance on `game_updater.py` vs SportsbookReview results? - game_updater should be the preference but fallback to SportsbookReview

---

## 🎯 **Success Metrics**

- [ ] **Complete data scraping for April 4th, 2021 → current date**
- [ ] **98%+ MLB Game ID correlation accuracy**
- [ ] **95%+ weather and venue data coverage**
- [ ] 99%+ parsing accuracy across all bet types
- [ ] **Seamless integration with existing `game_updater.py` service**
- [ ] Graceful handling of rate limits and errors
- [ ] Seamless integration with existing platform services
- [ ] Comprehensive test coverage with demo scenarios
- [ ] **Validated game results consistency across data sources**

---

## 🛠️ **Getting Started**

1. **Review** the detailed specifications in `@adding_historical_odds.md`
2. **Analyze** existing platform architecture in `@/models` and `@/services`
3. **Study** `@game_updater.py` patterns for game results integration
4. **Examine** `@mlb_stats_api_endpoints.mdc` for API integration patterns
5. **Build** iterative demos to validate data extraction starting with April 4th, 2021
6. **Implement** following the phased approach above
7. **Test** thoroughly with real-world scenarios across the full date range

## 🔗 **Key Integration Points**

### **Existing Services to Leverage**
- **`game_updater.py`** - Primary game results source
  - `update_game_outcomes_for_date()` method
  - `update_recent_game_outcomes()` method
  - Game outcome calculation logic
- **MLB Stats API Client** - Game data enrichment
- **Team Mapping Service** - Consistent team identification
- **Database Connection Patterns** - Established PostgreSQL integration

### **New Services to Build**
- **`MLBDataEnrichmentService`** - MLB Stats API integration
- **`SportsbookReviewScraper`** - Historical odds scraping
- **`GameCorrelationService`** - Match SportsbookReview games with MLB Game IDs
- **`OddsDataProcessor`** - Normalize and store odds data

**Note**: All development should maintain separation in the `sportsbookreview/` folder while replicating the established service patterns from the main platform and leveraging existing game results infrastructure.

---

## 📅 **Historical Coverage Specification**

**Start Date**: Sunday, April 4th, 2021
**End Date**: Current date (continuously updated), first refresh is through July 6, 2025
**Estimated Games**: ~2,430 games per season × 4+ seasons = ~10,000+ games
**Data Points per Game**: 
- 3-5 sportsbooks × 3 bet types × historical line movements = ~45-75 data points per game
- MLB enrichment data (weather, venue, pitchers, etc.)
- **Total estimated data points**: ~500,000-750,000 odds records + enrichment data

**Implementation Priority**:
1. **April 2021** - Establish baseline and test all systems
2. **2021 Season** - Complete historical coverage
3. **2022-2025 Seasons** - Comprehensive data collection
4. **Current Season** - Real-time integration with existing systems

- General Balls