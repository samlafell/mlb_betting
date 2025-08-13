# Archived Debug Scripts Summary

**Archive Date**: 2025-08-13  
**Reason**: Cleanup of old debug scripts (Issue #23)

## Scripts Archived

### Data Processing Debug Scripts
- `debug_backtesting_data.py` - Backtesting data analysis and validation
- `debug_data_flow.py` - Data flow analysis through pipeline stages
- `debug_integration_flow.py` - Integration workflow debugging
- `debug_staging_processing.py` - Staging pipeline processing issues

### Scraping & Storage Debug Scripts  
- `debug_scraper_storage.py` - Storage method debugging
- `debug_storage_methods.py` - Storage implementation testing
- `debug_odds_extraction.py` - Odds extraction debugging (16KB)
- `debug_script_content.py` - Script content analysis
- `debug_totals_structure.py` - Totals data structure debugging

### Strategy & Analysis Debug Scripts
- `debug_profitable_strategies.py` - Strategy profitability analysis

### Test Debug Scripts
- `test_actual_processing.py` - Processing pipeline testing
- `test_fresh_collection.py` - Fresh data collection testing  
- `test_full_processing.py` - End-to-end processing testing
- `test_historical_availability.py` - Historical data availability testing
- `test_season_start_detection.py` - Season start detection testing
- `test_sportsbookreview_pipeline.py` - SportsbookReview pipeline testing (17KB)
- `test_staging_only.py` - Staging-only pipeline testing

## Removed During Cleanup

### Resolved Issues (Deleted)
- `scripts/archive/resolved_issues/debug_bet_type_issue.py` - Resolved bet type parsing
- `scripts/archive/resolved_issues/debug_field_mapping.py` - Resolved field mapping issues
- `scripts/archive/resolved_issues/debug_total_line_issue.py` - Resolved total line parsing

### Date-Specific Debug (Deleted)  
- `scripts/archive/july_7th_debug/debug_july_7th_scraping.py` - July 7th specific scraping issues
- `scripts/archive/july_7th_debug/test_july_7th_scraper_debug.py` - July 7th scraper testing

## Historical Context

These scripts were primarily created around July 15, 2025, during a major pipeline debugging and improvement effort. They served their purpose in resolving various data collection, processing, and analysis issues.

## Recovery

If any of these scripts are needed again:
1. They can be recovered from git history
2. The logic can be recreated using the current CLI debug commands
3. Modern debugging should use the structured CLI system instead

## Active Debug Scripts

The following scripts remain active in `scripts/debug/`:
- `debug_conflict.py` - Recent database conflict debugging
- `debug_real_parsing_chain.py` - Parsing chain analysis  
- `debug_staging_processing.py` - Current staging issues