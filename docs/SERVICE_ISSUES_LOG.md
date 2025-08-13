# Service Issues Log

This document tracks known issues with pipeline services and their resolution status.

## Current Issues

### SBD Collector Async Error (Priority: Medium)
- **Service**: SportsBettingDime Collector (`sbd_unified_collector_api.py`)
- **Error**: `TypeError: '>' not supported between instances of 'coroutine' and 'int'`
- **Impact**: SBD data collection fails, but Action Network and VSIN collectors work correctly
- **First Observed**: 2025-08-13 during full pipeline execution
- **Status**: OPEN - requires async/await fix in collector logic
- **Workaround**: Use Action Network and VSIN as primary data sources
- **Resolution Required**: Fix async function call in SBD collector comparison logic

### Enhanced Games Service Table Mismatch (Priority: High)
- **Service**: Enhanced Games Service (`enhanced_games_service.py`)
- **Error**: Query looking for non-existent `staging.action_network_odds_historical` table
- **Impact**: 0% curated games processed despite 208 staging records available
- **First Observed**: 2025-08-13 during curated zone processing
- **Status**: FIXED - Updated queries to use `staging.betting_odds_unified`
- **Resolution**: Modified `_get_staging_games()` and `_add_market_features()` methods

## Resolved Issues

None yet - this is the first service issues log.

## Monitoring Recommendations

1. **Add service health checks** for each collector in monitoring dashboard
2. **Implement error rate tracking** per data source
3. **Create alerting** for service failures affecting critical pipeline flows
4. **Regular validation** of table dependencies in service queries
5. **Automated testing** of collector async patterns

## Next Steps

1. Fix SBD collector async error to restore full data source coverage
2. Add comprehensive service monitoring to prevent silent failures
3. Implement automated dependency validation for staging/curated services
4. Create service-specific health check endpoints

---
*Last Updated: 2025-08-13*
*Update Frequency: As issues are discovered/resolved*