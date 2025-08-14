# TODO Comments Analysis & Cleanup Plan

**Date**: 2025-08-13  
**Issue**: #26 - TODO comments cleanup  
**Branch**: `cleanup/issue-26-todo-comments`

## Overview

Found 15 TODO comments across the codebase. Analysis categorizes them by priority and implementation complexity.

## TODO Comments Analysis

### Critical TODOs (Immediate Action Required)

#### 1. Database Foreign Key Issue
**File**: `src/services/curated_zone/enhanced_games_service.py:299`
```python
staging_game_id=None,  # TODO: Fix FK constraint issue - bypassing for now
```
**Issue**: FK constraint bypass in production code
**Priority**: HIGH - Security/Data Integrity Risk
**Action**: Fix FK constraint or implement proper handling

#### 2. Configuration Loading Issue  
**File**: `src/data/pipeline/staging_zone.py:780`
```python
# TODO: Fix config.toml loading issue for pipeline settings
CURATED_ZONE_DISABLED = True  # Temporary hardcode
```
**Issue**: Hardcoded configuration override
**Priority**: HIGH - Operational Risk  
**Action**: Fix config.toml loading mechanism

### ML Service TODOs (Feature Implementation)

#### 3-7. Prediction Service Enhancements
**File**: `src/ml/services/prediction_service.py`
- Line 572: `"confidence_threshold_met": True,  # TODO: Implement threshold logic`
- Line 573: `"risk_level": "medium",  # TODO: Implement risk calculation`  
- Line 612: `"expected_value": 0.0,  # TODO: Calculate EV`
- Line 613: `"kelly_fraction": 0.0,  # TODO: Implement Kelly Criterion`
- Line 692: `None,  # feature_vector_id (TODO: implement)`

**Priority**: MEDIUM - Feature Enhancement
**Action**: Implement comprehensive betting calculation logic

#### 8. Feature Drift Detection
**File**: `src/ml/services/feature_drift_detection_service.py:566`
```python
"latest",  # TODO: Get actual version
```
**Priority**: MEDIUM - ML Model Versioning
**Action**: Implement proper model version tracking

#### 9-11. Team Features Enhancements
**File**: `src/ml/features/team_features.py`
- Line 406: `# TODO: Integrate with MLB Stats API for detailed pitcher stats`
- Line 603: `# TODO: Calculate travel distance and timezone changes`
- Line 625: `# TODO: Add more sophisticated situational analysis`

**Priority**: LOW - Feature Enhancement
**Action**: Long-term ML feature improvements

### Infrastructure TODOs

#### 12-13. Migration System
**File**: `utilities/migration/run_migration.py`
- Line 246: `# TODO: Implement staging zone migration`
- Line 267: `# TODO: Implement curated zone migration`

**Priority**: MEDIUM - Infrastructure Development
**Action**: Complete migration system implementation

#### 14. Service Organization
**File**: `src/services/orchestration/__init__.py:19`
```python
# TODO: Implement these services in future phases
```
**Priority**: LOW - Future Development
**Action**: Document future service architecture

#### 15. Data Processing
**File**: `src/services/curated_zone/betting_splits_aggregator.py:296`
```python
# TODO: Implement actual VSIN data processing when available
```
**Priority**: MEDIUM - Data Integration
**Action**: Implement VSIN data processing

### Database Schema TODO

#### 16. Game Outcomes View
**File**: `sql/views/unified_game_outcomes_view.sql:202`
```sql
-- TODO: Add actual outcome source queries here, such as:
```
**Priority**: MEDIUM - Database Feature
**Action**: Complete game outcomes view implementation

## Action Plan

### Phase 1: Critical Fixes (Immediate)
1. ✅ **Fix FK constraint issue** in enhanced_games_service.py
2. ✅ **Fix config.toml loading issue** in staging_zone.py
3. ✅ **Update service imports** to remove future service references

### Phase 2: ML Service Implementation (Short-term)
4. **Implement prediction service logic**: threshold, risk, EV, Kelly criterion
5. **Add model versioning** in drift detection service
6. **Complete VSIN processing** in betting splits aggregator

### Phase 3: Infrastructure (Medium-term) 
7. **Complete migration system** implementation
8. **Enhance game outcomes view** with actual data sources
9. **Plan future service architecture** documentation

### Phase 4: Feature Enhancements (Long-term)
10. **MLB API integration** for detailed pitcher stats
11. **Travel distance calculations** for team features
12. **Sophisticated situational analysis** for team features

## Implementation Strategy

### Critical Issues First
- Fix operational risks (FK constraints, config loading)
- Address hardcoded values and temporary bypasses
- Ensure system stability

### Feature Implementation
- Group related TODOs for batch implementation
- Prioritize by business impact
- Maintain backward compatibility

### Documentation  
- Convert appropriate TODOs to GitHub issues for future work
- Document architectural decisions
- Update implementation status

## Risk Assessment

### High Risk TODOs
- FK constraint bypass: Data integrity risk
- Hardcoded config: Operational failure risk

### Medium Risk TODOs  
- Missing ML calculations: Business logic gaps
- Incomplete data processing: Data quality issues

### Low Risk TODOs
- Future features: Enhancement opportunities
- Documentation gaps: Maintainability issues

## Success Criteria

1. ✅ All critical operational TODOs resolved
2. ✅ System stability maintained
3. ✅ No functional regressions
4. ✅ Improved code maintainability
5. ✅ Clear roadmap for remaining enhancements