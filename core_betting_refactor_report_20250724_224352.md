# Core Betting Schema Refactoring Report

**Generated:** 2025-07-24T22:43:46.674720
**Files Scanned:** 7447
**Files with Changes:** 102
**Total Changes:** 2422

## Summary of Changes

### Schema Mapping Rules

- `core_betting.games` → `curated.games_complete`
- `core_betting.game_outcomes` → `curated.game_outcomes`
- `core_betting.betting_lines_moneyline` → `curated.betting_lines_unified`
- `core_betting.betting_lines_totals` → `curated.betting_lines_unified`
- `core_betting.betting_lines_spread` → `curated.betting_lines_unified`
- `core_betting.betting_lines_spreads` → `curated.betting_lines_unified`
- `core_betting.sportsbooks` → `curated.sportsbooks`
- `core_betting.teams` → `curated.teams_master`
- `core_betting.sportsbook_external_mappings` → `curated.sportsbook_mappings`
- `core_betting.data_source_metadata` → `curated.data_sources`
- `core_betting.data_migrations` → `operational.schema_migrations`
- `core_betting.supplementary_games` → `curated.games_complete`
- `core_betting.betting_splits` → `curated.betting_splits`
- `core_betting.` → `curated.`

### Files Requiring Changes

#### archive/migration/PHASE4_REMAINING_SCHEMA_CONSOLIDATION_PLAN.md

**Direct Mappings:**
- Line 39: core_betting.game_outcomes → curated.game_outcomes
- Line 39: core_betting. → curated.
- Line 63: core_betting.teams → curated.teams_master
- Line 63: core_betting. → curated.
- Line 64: core_betting.teams → curated.teams_master
- Line 64: core_betting. → curated.
- Line 65: core_betting.teams → curated.teams_master
- Line 65: core_betting. → curated.
- Line 68: core_betting. → curated.
- Line 69: core_betting. → curated.
- Line 72: core_betting.teams → curated.teams_master
- Line 72: core_betting. → curated.
- Line 78: core_betting.supplementary_games → curated.games_complete
- Line 78: core_betting. → curated.

#### archive/migration/phase3b_legacy_cleanup.py

**Direct Mappings:**
- Line 209: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 209: core_betting. → curated.
- Line 210: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 210: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 210: core_betting. → curated.
- Line 211: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 211: core_betting. → curated.
- Line 212: core_betting.games → curated.games_complete
- Line 212: core_betting. → curated.
- Line 389: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 389: core_betting. → curated.
- Line 394: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 394: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 394: core_betting. → curated.
- Line 397: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 397: core_betting. → curated.
- Line 541: core_betting.games → curated.games_complete
- Line 541: core_betting. → curated.
- Line 637: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 637: core_betting. → curated.
- Line 638: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 638: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 638: core_betting. → curated.
- Line 639: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 639: core_betting. → curated.
- Line 640: core_betting.games → curated.games_complete
- Line 640: core_betting. → curated.

#### archive/migration/SCHEMA_CONSOLIDATION_MIGRATION_GUIDE.md

**Direct Mappings:**
- Line 78: core_betting.games → curated.games_complete
- Line 78: core_betting. → curated.
- Line 79: core_betting.game_outcomes → curated.game_outcomes
- Line 79: core_betting. → curated.
- Line 80: core_betting.teams → curated.teams_master
- Line 80: core_betting. → curated.
- Line 81: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 81: core_betting. → curated.
- Line 82: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 82: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 82: core_betting. → curated.
- Line 83: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 83: core_betting. → curated.
- Line 84: core_betting.betting_splits → curated.betting_splits
- Line 84: core_betting. → curated.
- Line 85: core_betting. → curated.
- Line 86: core_betting. → curated.
- Line 87: core_betting. → curated.
- Line 254: core_betting.game_outcomes → curated.game_outcomes
- Line 254: core_betting. → curated.

#### archive/migration/PHASE3_COMPLETION_SUMMARY.md

**Direct Mappings:**
- Line 64: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 64: core_betting. → curated.
- Line 65: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 65: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 65: core_betting. → curated.
- Line 66: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 66: core_betting. → curated.
- Line 67: core_betting.games → curated.games_complete
- Line 67: core_betting. → curated.
- Line 147: core_betting.games → curated.games_complete
- Line 147: core_betting. → curated.

#### archive/migration/SCHEMA_MIGRATION_TEST_RESULTS.md

**Direct Mappings:**
- Line 21: core_betting.teams → curated.teams_master
- Line 21: core_betting. → curated.
- Line 57: core_betting.games → curated.games_complete
- Line 57: core_betting. → curated.
- Line 58: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 58: core_betting. → curated.
- Line 59: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 59: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 59: core_betting. → curated.
- Line 60: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 60: core_betting. → curated.

#### archive/migration/PHASE4_CONSOLIDATION_COMPLETION_REPORT.md

**Direct Mappings:**
- Line 52: core_betting.game_outcomes → curated.game_outcomes
- Line 52: core_betting. → curated.
- Line 57: core_betting.teams → curated.teams_master
- Line 57: core_betting. → curated.
- Line 67: core_betting.teams → curated.teams_master
- Line 67: core_betting. → curated.
- Line 68: core_betting.supplementary_games → curated.games_complete
- Line 68: core_betting. → curated.

#### tests/test_cli_pipeline.py

**Direct Mappings:**
- Line 360: core_betting. → curated.

#### .claude/settings.local.json

**Direct Mappings:**
- Line 52: core_betting.games → curated.games_complete
- Line 52: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 52: core_betting. → curated.
- Line 53: core_betting. → curated.
- Line 54: core_betting. → curated.

#### docs/SYSTEM_DESIGN_ANALYSIS.md

**Direct Mappings:**
- Line 30: core_betting. → curated.
- Line 177: core_betting. → curated.
- Line 178: core_betting. → curated.
- Line 179: core_betting. → curated.
- Line 182: core_betting. → curated.
- Line 183: core_betting. → curated.
- Line 184: core_betting. → curated.
- Line 187: core_betting. → curated.
- Line 188: core_betting. → curated.
- Line 189: core_betting. → curated.
- Line 273: core_betting. → curated.

#### docs/DATA_MODEL.md

**Direct Mappings:**
- Line 241: core_betting. → curated.
- Line 244: core_betting. → curated.
- Line 247: core_betting. → curated.
- Line 250: core_betting. → curated.
- Line 253: core_betting. → curated.
- Line 326: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 326: core_betting. → curated.
- Line 327: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 327: core_betting. → curated.
- Line 328: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 328: core_betting. → curated.

#### docs/DEPLOY_DATA_QUALITY.md

**Direct Mappings:**
- Line 37: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 37: core_betting. → curated.
- Line 43: core_betting. → curated.
- Line 68: core_betting. → curated.
- Line 71: core_betting. → curated.
- Line 75: core_betting. → curated.

#### docs/BETTING_LINES_DATA_QUALITY_IMPLEMENTATION_SUMMARY.md

**Direct Mappings:**
- Line 14: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 14: core_betting. → curated.
- Line 19: core_betting. → curated.
- Line 46: core_betting. → curated.
- Line 54: core_betting. → curated.
- Line 55: core_betting. → curated.
- Line 56: core_betting. → curated.
- Line 57: core_betting. → curated.
- Line 139: core_betting. → curated.
- Line 142: core_betting. → curated.
- Line 146: core_betting. → curated.
- Line 149: core_betting. → curated.
- Line 179: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 179: core_betting. → curated.
- Line 185: core_betting. → curated.
- Line 186: core_betting. → curated.
- Line 245: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 245: core_betting. → curated.
- Line 247: core_betting. → curated.
- Line 263: core_betting. → curated.
- Line 266: core_betting. → curated.
- Line 271: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 271: core_betting. → curated.

#### docs/BETTING_LINES_DATA_QUALITY_ASSESSMENT.md

**Direct Mappings:**
- Line 12: core_betting.games → curated.games_complete
- Line 12: core_betting.sportsbooks → curated.sportsbooks
- Line 12: core_betting. → curated.
- Line 20: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 20: core_betting. → curated.
- Line 28: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 28: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 28: core_betting. → curated.
- Line 36: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 36: core_betting. → curated.
- Line 59: core_betting.sportsbooks → curated.sportsbooks
- Line 59: core_betting. → curated.
- Line 86: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 86: core_betting. → curated.
- Line 88: core_betting.sportsbooks → curated.sportsbooks
- Line 88: core_betting. → curated.
- Line 97: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 97: core_betting. → curated.
- Line 99: core_betting.sportsbooks → curated.sportsbooks
- Line 99: core_betting. → curated.
- Line 100: core_betting.sportsbooks → curated.sportsbooks
- Line 100: core_betting. → curated.
- Line 101: core_betting.sportsbooks → curated.sportsbooks
- Line 101: core_betting. → curated.
- Line 113: core_betting.sportsbooks → curated.sportsbooks
- Line 113: core_betting. → curated.
- Line 114: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 114: core_betting. → curated.
- Line 135: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 135: core_betting. → curated.
- Line 144: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 144: core_betting. → curated.
- Line 147: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 147: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 147: core_betting. → curated.
- Line 150: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 150: core_betting. → curated.
- Line 164: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 164: core_betting. → curated.
- Line 173: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 173: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 173: core_betting. → curated.
- Line 189: core_betting.sportsbooks → curated.sportsbooks
- Line 189: core_betting. → curated.
- Line 190: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 190: core_betting. → curated.

#### docs/GAME_OUTCOME_INTEGRATION.md

**Direct Mappings:**
- Line 5: core_betting.game_outcomes → curated.game_outcomes
- Line 5: core_betting. → curated.
- Line 11: core_betting.game_outcomes → curated.game_outcomes
- Line 11: core_betting. → curated.
- Line 61: core_betting.game_outcomes → curated.game_outcomes
- Line 61: core_betting. → curated.
- Line 64: core_betting.game_outcomes → curated.game_outcomes
- Line 64: core_betting. → curated.
- Line 66: core_betting.games → curated.games_complete
- Line 66: core_betting. → curated.
- Line 254: core_betting.games → curated.games_complete
- Line 254: core_betting. → curated.

#### docs/UNIFIED_BETTING_LINES_SUMMARY.md

**Direct Mappings:**
- Line 12: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 12: core_betting. → curated.
- Line 13: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 13: core_betting. → curated.
- Line 14: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 14: core_betting. → curated.
- Line 183: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 183: core_betting. → curated.
- Line 195: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 195: core_betting. → curated.
- Line 207: core_betting.games → curated.games_complete
- Line 207: core_betting. → curated.
- Line 208: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 208: core_betting. → curated.

#### docs/PIPELINE_IMPLEMENTATION_GUIDE.md

**Direct Mappings:**
- Line 147: core_betting. → curated.
- Line 184: core_betting. → curated.

#### docs/CORE_BETTING_DECOMMISSION_DESIGN.md

**Direct Mappings:**
- Line 34: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 34: core_betting. → curated.
- Line 39: core_betting.data_source_metadata → curated.data_sources
- Line 39: core_betting. → curated.
- Line 44: core_betting.teams → curated.teams_master
- Line 44: core_betting. → curated.
- Line 49: core_betting.data_migrations → operational.schema_migrations
- Line 49: core_betting. → curated.
- Line 60: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 60: core_betting. → curated.
- Line 61: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 61: core_betting. → curated.
- Line 62: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 62: core_betting. → curated.
- Line 78: core_betting.games → curated.games_complete
- Line 78: core_betting. → curated.
- Line 79: core_betting.supplementary_games → curated.games_complete
- Line 79: core_betting. → curated.
- Line 80: core_betting.game_outcomes → curated.game_outcomes
- Line 80: core_betting. → curated.
- Line 102: core_betting.games → curated.games_complete
- Line 102: core_betting. → curated.
- Line 121: core_betting.games → curated.games_complete
- Line 121: core_betting. → curated.
- Line 130: core_betting.game_outcomes → curated.game_outcomes
- Line 130: core_betting. → curated.
- Line 139: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 139: core_betting. → curated.
- Line 163: core_betting.games → curated.games_complete
- Line 163: core_betting. → curated.
- Line 164: core_betting.game_outcomes → curated.game_outcomes
- Line 164: core_betting. → curated.
- Line 165: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 165: core_betting. → curated.
- Line 166: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 166: core_betting. → curated.
- Line 167: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 167: core_betting. → curated.
- Line 168: core_betting.sportsbooks → curated.sportsbooks
- Line 168: core_betting. → curated.
- Line 169: core_betting.teams → curated.teams_master
- Line 169: core_betting. → curated.
- Line 180: core_betting.games → curated.games_complete
- Line 180: core_betting. → curated.
- Line 189: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 189: core_betting. → curated.
- Line 191: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 191: core_betting. → curated.
- Line 202: core_betting.games → curated.games_complete
- Line 202: core_betting. → curated.
- Line 203: core_betting.game_outcomes → curated.game_outcomes
- Line 203: core_betting. → curated.
- Line 217: core_betting.games → curated.games_complete
- Line 217: core_betting. → curated.
- Line 305: core_betting.games → curated.games_complete
- Line 305: core_betting. → curated.

#### docs/PIPELINE_IMPLEMENTATION_ROADMAP.md

**Direct Mappings:**
- Line 320: core_betting.games → curated.games_complete
- Line 320: core_betting. → curated.

#### docs/migration/COMPREHENSIVE_STATUS_SUMMARY.md

**Direct Mappings:**
- Line 96: core_betting. → curated.

#### docs/migration/UNIFIED_CLI_INTEGRATION_SUMMARY.md

**Direct Mappings:**
- Line 27: core_betting.games → curated.games_complete
- Line 27: core_betting. → curated.

#### docs/migration/JULY10_2PM_PROGRESS.md

**Direct Mappings:**
- Line 295: core_betting. → curated.

#### docs/migration/DATA_COLLECTION_REALITY_CHECK.md

**Direct Mappings:**
- Line 32: core_betting.games → curated.games_complete
- Line 32: core_betting. → curated.
- Line 36: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 36: core_betting. → curated.
- Line 40: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 40: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 40: core_betting. → curated.
- Line 44: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 44: core_betting. → curated.
- Line 48: core_betting.betting_splits → curated.betting_splits
- Line 48: core_betting. → curated.
- Line 60: core_betting.games → curated.games_complete
- Line 60: core_betting. → curated.

#### docs/migration/DATA_COLLECTION_TEST_RESULTS.md

**Direct Mappings:**
- Line 46: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 46: core_betting. → curated.
- Line 47: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 47: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 47: core_betting. → curated.
- Line 48: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 48: core_betting. → curated.

#### utilities/execute_legacy_migration.py

**Direct Mappings:**
- Line 122: core_betting. → curated.
- Line 358: core_betting. → curated.
- Line 363: core_betting. → curated.
- Line 462: core_betting.games → curated.games_complete
- Line 462: core_betting. → curated.
- Line 463: core_betting.teams → curated.teams_master
- Line 463: core_betting. → curated.
- Line 464: core_betting.sportsbooks → curated.sportsbooks
- Line 464: core_betting. → curated.

#### utilities/deploy_data_quality_improvements.py

**Direct Mappings:**
- Line 151: core_betting. → curated.

#### utilities/load_action_network_complete_history.py

**Direct Mappings:**
- Line 225: core_betting.games → curated.games_complete
- Line 225: core_betting. → curated.
- Line 241: core_betting.games → curated.games_complete
- Line 241: core_betting. → curated.
- Line 492: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 492: core_betting. → curated.
- Line 506: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 506: core_betting. → curated.
- Line 636: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 636: core_betting. → curated.
- Line 650: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 650: core_betting. → curated.
- Line 781: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 781: core_betting. → curated.
- Line 795: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 795: core_betting. → curated.
- Line 844: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 844: core_betting. → curated.

#### utilities/core_betting_migration/validation_and_rollback.py

**Direct Mappings:**
- Line 399: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 399: core_betting. → curated.
- Line 401: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 401: core_betting. → curated.
- Line 403: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 403: core_betting. → curated.
- Line 409: core_betting.game_outcomes → curated.game_outcomes
- Line 409: core_betting. → curated.
- Line 410: core_betting.games → curated.games_complete
- Line 410: core_betting. → curated.
- Line 625: core_betting.games → curated.games_complete
- Line 625: core_betting. → curated.
- Line 627: core_betting.games → curated.games_complete
- Line 627: core_betting. → curated.
- Line 628: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 628: core_betting. → curated.
- Line 882: core_betting.games → curated.games_complete
- Line 882: core_betting. → curated.
- Line 883: core_betting.supplementary_games → curated.games_complete
- Line 883: core_betting. → curated.
- Line 1089: core_betting. → curated.

#### utilities/core_betting_migration/data_migration_scripts.sql

**Direct Mappings:**
- Line 45: core_betting.games → curated.games_complete
- Line 45: core_betting. → curated.
- Line 47: core_betting.game_outcomes → curated.game_outcomes
- Line 47: core_betting. → curated.
- Line 49: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 49: core_betting. → curated.
- Line 51: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 51: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 51: core_betting. → curated.
- Line 53: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 53: core_betting. → curated.
- Line 55: core_betting.sportsbooks → curated.sportsbooks
- Line 55: core_betting. → curated.
- Line 57: core_betting.teams → curated.teams_master
- Line 57: core_betting. → curated.
- Line 59: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 59: core_betting. → curated.
- Line 61: core_betting.data_source_metadata → curated.data_sources
- Line 61: core_betting. → curated.
- Line 63: core_betting.supplementary_games → curated.games_complete
- Line 63: core_betting. → curated.
- Line 298: core_betting.sportsbooks → curated.sportsbooks
- Line 298: core_betting. → curated.
- Line 321: core_betting.teams → curated.teams_master
- Line 321: core_betting. → curated.
- Line 344: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 344: core_betting. → curated.
- Line 366: core_betting.data_source_metadata → curated.data_sources
- Line 366: core_betting. → curated.
- Line 379: core_betting.sportsbooks → curated.sportsbooks
- Line 379: core_betting.teams → curated.teams_master
- Line 379: core_betting. → curated.
- Line 380: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 380: core_betting.data_source_metadata → curated.data_sources
- Line 380: core_betting. → curated.
- Line 395: core_betting.games → curated.games_complete
- Line 395: core_betting. → curated.
- Line 406: core_betting.supplementary_games → curated.games_complete
- Line 406: core_betting. → curated.
- Line 408: core_betting.games → curated.games_complete
- Line 408: core_betting. → curated.
- Line 409: core_betting.supplementary_games → curated.games_complete
- Line 409: core_betting. → curated.
- Line 471: core_betting.game_outcomes → curated.game_outcomes
- Line 471: core_betting. → curated.
- Line 502: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 502: core_betting. → curated.
- Line 529: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 529: core_betting. → curated.
- Line 555: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 555: core_betting. → curated.
- Line 661: core_betting.games → curated.games_complete
- Line 661: core_betting. → curated.
- Line 662: core_betting.supplementary_games → curated.games_complete
- Line 662: core_betting. → curated.
- Line 663: core_betting.game_outcomes → curated.game_outcomes
- Line 663: core_betting. → curated.
- Line 664: core_betting.sportsbooks → curated.sportsbooks
- Line 664: core_betting. → curated.
- Line 665: core_betting.teams → curated.teams_master
- Line 665: core_betting. → curated.
- Line 666: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 666: core_betting. → curated.
- Line 667: core_betting.data_source_metadata → curated.data_sources
- Line 667: core_betting. → curated.
- Line 671: core_betting.supplementary_games → curated.games_complete
- Line 671: core_betting. → curated.

#### utilities/core_betting_migration/complete_betting_lines_migration.sql

**Direct Mappings:**
- Line 21: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 21: core_betting. → curated.
- Line 42: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 42: core_betting. → curated.

#### utilities/core_betting_migration/automated_code_refactor.py

**Direct Mappings:**
- Line 62: core_betting.games → curated.games_complete
- Line 62: core_betting. → curated.
- Line 63: core_betting.game_outcomes → curated.game_outcomes
- Line 63: core_betting. → curated.
- Line 64: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 64: core_betting. → curated.
- Line 65: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 65: core_betting. → curated.
- Line 66: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 66: core_betting. → curated.
- Line 67: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 67: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 67: core_betting. → curated.
- Line 68: core_betting.sportsbooks → curated.sportsbooks
- Line 68: core_betting. → curated.
- Line 69: core_betting.teams → curated.teams_master
- Line 69: core_betting. → curated.
- Line 70: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 70: core_betting. → curated.
- Line 71: core_betting.data_source_metadata → curated.data_sources
- Line 71: core_betting. → curated.
- Line 72: core_betting.data_migrations → operational.schema_migrations
- Line 72: core_betting. → curated.
- Line 73: core_betting.supplementary_games → curated.games_complete
- Line 73: core_betting. → curated.
- Line 74: core_betting.betting_splits → curated.betting_splits
- Line 74: core_betting. → curated.
- Line 76: core_betting. → curated.
- Line 196: core_betting. → curated.
- Line 268: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 268: core_betting. → curated.
- Line 269: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 269: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 269: core_betting. → curated.
- Line 270: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 270: core_betting. → curated.

#### utilities/core_betting_migration/README.md

**Direct Mappings:**
- Line 227: core_betting.games → curated.games_complete
- Line 227: core_betting. → curated.
- Line 228: core_betting.supplementary_games → curated.games_complete
- Line 228: core_betting. → curated.
- Line 229: core_betting.game_outcomes → curated.game_outcomes
- Line 229: core_betting. → curated.
- Line 230: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 230: core_betting. → curated.
- Line 231: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 231: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 231: core_betting. → curated.
- Line 232: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 232: core_betting. → curated.
- Line 233: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 233: core_betting. → curated.
- Line 234: core_betting.teams → curated.teams_master
- Line 234: core_betting. → curated.
- Line 235: core_betting.data_source_metadata → curated.data_sources
- Line 235: core_betting. → curated.
- Line 236: core_betting.data_migrations → operational.schema_migrations
- Line 236: core_betting. → curated.
- Line 243: core_betting.games → curated.games_complete
- Line 243: core_betting. → curated.
- Line 252: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 252: core_betting. → curated.
- Line 253: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 253: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 253: core_betting. → curated.
- Line 254: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 254: core_betting. → curated.
- Line 265: core_betting.games → curated.games_complete
- Line 265: core_betting. → curated.
- Line 266: core_betting.game_outcomes → curated.game_outcomes
- Line 266: core_betting. → curated.

#### utilities/core_betting_migration/execution_checklist.md

**Direct Mappings:**
- Line 269: core_betting. → curated.

#### utilities/core_betting_migration/final_corrected_migration.sql

**Direct Mappings:**
- Line 24: core_betting.games → curated.games_complete
- Line 24: core_betting. → curated.
- Line 26: core_betting.game_outcomes → curated.game_outcomes
- Line 26: core_betting. → curated.
- Line 28: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 28: core_betting. → curated.
- Line 30: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 30: core_betting. → curated.
- Line 32: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 32: core_betting. → curated.
- Line 34: core_betting.sportsbooks → curated.sportsbooks
- Line 34: core_betting. → curated.
- Line 36: core_betting.teams → curated.teams_master
- Line 36: core_betting. → curated.
- Line 38: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 38: core_betting. → curated.
- Line 60: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 60: core_betting. → curated.
- Line 97: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 97: core_betting. → curated.
- Line 137: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 137: core_betting. → curated.
- Line 165: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 165: core_betting. → curated.
- Line 203: core_betting.games → curated.games_complete
- Line 203: core_betting. → curated.
- Line 204: core_betting.game_outcomes → curated.game_outcomes
- Line 204: core_betting. → curated.
- Line 205: core_betting.sportsbooks → curated.sportsbooks
- Line 205: core_betting. → curated.
- Line 206: core_betting.teams → curated.teams_master
- Line 206: core_betting. → curated.
- Line 207: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 207: core_betting. → curated.

#### utilities/core_betting_migration/corrected_data_migration_scripts.sql

**Direct Mappings:**
- Line 35: core_betting.games → curated.games_complete
- Line 35: core_betting. → curated.
- Line 37: core_betting.game_outcomes → curated.game_outcomes
- Line 37: core_betting. → curated.
- Line 39: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 39: core_betting. → curated.
- Line 41: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 41: core_betting. → curated.
- Line 43: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 43: core_betting. → curated.
- Line 45: core_betting.sportsbooks → curated.sportsbooks
- Line 45: core_betting. → curated.
- Line 47: core_betting.teams → curated.teams_master
- Line 47: core_betting. → curated.
- Line 49: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 49: core_betting. → curated.
- Line 301: core_betting.sportsbooks → curated.sportsbooks
- Line 301: core_betting. → curated.
- Line 327: core_betting.teams → curated.teams_master
- Line 327: core_betting. → curated.
- Line 362: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 362: core_betting. → curated.
- Line 404: core_betting.games → curated.games_complete
- Line 404: core_betting. → curated.
- Line 448: core_betting.game_outcomes → curated.game_outcomes
- Line 448: core_betting. → curated.
- Line 483: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 483: core_betting. → curated.
- Line 502: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 502: core_betting. → curated.
- Line 511: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 511: core_betting. → curated.
- Line 532: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 532: core_betting. → curated.
- Line 541: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 541: core_betting. → curated.
- Line 559: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 559: core_betting. → curated.
- Line 600: core_betting.games → curated.games_complete
- Line 600: core_betting. → curated.
- Line 601: core_betting.game_outcomes → curated.game_outcomes
- Line 601: core_betting. → curated.
- Line 602: core_betting.sportsbooks → curated.sportsbooks
- Line 602: core_betting. → curated.
- Line 603: core_betting.teams → curated.teams_master
- Line 603: core_betting. → curated.
- Line 604: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 604: core_betting. → curated.

#### utilities/migration/test_connection.py

**Direct Mappings:**
- Line 39: core_betting.games → curated.games_complete
- Line 39: core_betting. → curated.

#### utilities/migration/phase1_data_analysis.py

**Direct Mappings:**
- Line 103: core_betting.games → curated.games_complete
- Line 103: core_betting. → curated.
- Line 117: core_betting.games → curated.games_complete
- Line 117: core_betting. → curated.
- Line 128: core_betting.games → curated.games_complete
- Line 128: core_betting. → curated.
- Line 138: core_betting.games → curated.games_complete
- Line 138: core_betting. → curated.
- Line 142: core_betting.games → curated.games_complete
- Line 142: core_betting. → curated.
- Line 167: core_betting. → curated.
- Line 177: core_betting. → curated.
- Line 188: core_betting. → curated.
- Line 203: core_betting. → curated.
- Line 212: core_betting. → curated.
- Line 216: core_betting. → curated.
- Line 245: core_betting. → curated.
- Line 254: core_betting. → curated.
- Line 260: core_betting. → curated.
- Line 271: core_betting. → curated.

#### utilities/migration/README.md

**Direct Mappings:**
- Line 15: core_betting. → curated.
- Line 50: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 50: core_betting. → curated.
- Line 51: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 51: core_betting. → curated.
- Line 52: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 52: core_betting. → curated.

#### utilities/migration/migration_validator.py

**Direct Mappings:**
- Line 350: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 350: core_betting. → curated.
- Line 352: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 352: core_betting. → curated.
- Line 354: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 354: core_betting. → curated.
- Line 365: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 365: core_betting. → curated.
- Line 366: core_betting.games → curated.games_complete
- Line 366: core_betting. → curated.
- Line 370: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 370: core_betting. → curated.
- Line 371: core_betting.games → curated.games_complete
- Line 371: core_betting. → curated.
- Line 375: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 375: core_betting. → curated.
- Line 376: core_betting.games → curated.games_complete
- Line 376: core_betting. → curated.
- Line 416: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 416: core_betting. → curated.
- Line 417: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 417: core_betting. → curated.
- Line 418: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 418: core_betting. → curated.

#### utilities/migration/phase2_raw_zone_migration.py

**Direct Mappings:**
- Line 9: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 9: core_betting. → curated.
- Line 10: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 10: core_betting. → curated.
- Line 11: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 11: core_betting. → curated.
- Line 12: core_betting. → curated.
- Line 144: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 144: core_betting. → curated.
- Line 196: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 196: core_betting. → curated.
- Line 276: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 276: core_betting. → curated.
- Line 330: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 330: core_betting. → curated.
- Line 410: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 410: core_betting. → curated.
- Line 462: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 462: core_betting. → curated.

#### results/migration/phase2a_test_results_20250709_160225.json

**Direct Mappings:**
- Line 29: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 29: core_betting. → curated.
- Line 30: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 30: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 30: core_betting. → curated.
- Line 31: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 31: core_betting. → curated.
- Line 37: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 37: core_betting. → curated.

#### results/migration/phase2a_test_results_20250709_161136.json

**Direct Mappings:**
- Line 29: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 29: core_betting. → curated.
- Line 30: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 30: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 30: core_betting. → curated.
- Line 31: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 31: core_betting. → curated.
- Line 37: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 37: core_betting. → curated.

#### results/migration/phase3_test_results_20250709_170041.json

**Direct Mappings:**
- Line 256: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 256: core_betting. → curated.
- Line 260: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 260: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 260: core_betting. → curated.
- Line 264: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 264: core_betting. → curated.
- Line 268: core_betting.games → curated.games_complete
- Line 268: core_betting. → curated.

#### results/migration/phase3_test_results_20250709_165948.json

**Direct Mappings:**
- Line 227: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 227: core_betting. → curated.
- Line 231: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 231: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 231: core_betting. → curated.
- Line 235: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 235: core_betting. → curated.
- Line 239: core_betting.games → curated.games_complete
- Line 239: core_betting. → curated.

#### results/migration/phase2a_test_results_20250709_165347.json

**Direct Mappings:**
- Line 29: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 29: core_betting. → curated.
- Line 30: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 30: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 30: core_betting. → curated.
- Line 31: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 31: core_betting. → curated.
- Line 37: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 37: core_betting. → curated.

#### results/migration/phase2a_test_results_20250709_161046.json

**Direct Mappings:**
- Line 29: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 29: core_betting. → curated.
- Line 30: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 30: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 30: core_betting. → curated.
- Line 31: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 31: core_betting. → curated.
- Line 37: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 37: core_betting. → curated.

#### results/migration/phase2a_test_results_20250709_160155.json

**Direct Mappings:**
- Line 29: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 29: core_betting. → curated.
- Line 30: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 30: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 30: core_betting. → curated.
- Line 31: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 31: core_betting. → curated.
- Line 37: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 37: core_betting. → curated.

#### results/migration/migration_completion_report_20250709_171750.json

**Direct Mappings:**
- Line 105: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 105: core_betting. → curated.
- Line 106: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 106: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 106: core_betting. → curated.
- Line 107: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 107: core_betting. → curated.
- Line 108: core_betting.games → curated.games_complete
- Line 108: core_betting. → curated.

#### results/migration/phase3b_cleanup_results_20250709_171750.json

**Direct Mappings:**
- Line 74: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 74: core_betting. → curated.
- Line 75: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 75: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 75: core_betting. → curated.
- Line 76: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 76: core_betting. → curated.
- Line 77: core_betting.games → curated.games_complete
- Line 77: core_betting. → curated.

#### results/migration/phase2a_test_results_20250709_160050.json

**Direct Mappings:**
- Line 29: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 29: core_betting. → curated.
- Line 30: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 30: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 30: core_betting. → curated.
- Line 31: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 31: core_betting. → curated.
- Line 37: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 37: core_betting. → curated.

#### examples/test_betting_tables_with_teams.py

**Direct Mappings:**
- Line 40: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 40: core_betting. → curated.
- Line 67: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 67: core_betting. → curated.
- Line 95: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 95: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 95: core_betting. → curated.
- Line 123: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 123: core_betting. → curated.
- Line 149: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 149: core_betting. → curated.
- Line 159: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 159: core_betting. → curated.
- Line 169: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 169: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 169: core_betting. → curated.
- Line 191: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 191: core_betting. → curated.

#### scripts/archive/resolved_issues/test_phase3_legacy_cleanup.py

**Direct Mappings:**
- Line 318: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 318: core_betting. → curated.
- Line 322: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 322: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 322: core_betting. → curated.
- Line 326: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 326: core_betting. → curated.
- Line 328: core_betting.games → curated.games_complete
- Line 328: core_betting. → curated.
- Line 332: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 332: core_betting. → curated.
- Line 402: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 402: core_betting. → curated.
- Line 407: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 407: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 407: core_betting. → curated.
- Line 410: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 410: core_betting. → curated.

#### scripts/archive/resolved_issues/test_phase2a_migration.py

**Direct Mappings:**
- Line 116: core_betting. → curated.

#### reports/migration/migration_report_20250709_155824.json

**Direct Mappings:**
- Line 8: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 8: core_betting. → curated.
- Line 22: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 22: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 22: core_betting. → curated.
- Line 36: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 36: core_betting. → curated.
- Line 50: core_betting.games → curated.games_complete
- Line 50: core_betting. → curated.

#### src/core/sportsbook_utils.py

**Direct Mappings:**
- Line 31: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 31: core_betting. → curated.
- Line 32: core_betting.sportsbooks → curated.sportsbooks
- Line 32: core_betting. → curated.

#### src/core/config.py

**Direct Mappings:**
- Line 181: core_betting.games → curated.games_complete
- Line 181: core_betting. → curated.
- Line 182: core_betting. → curated.

#### src/data/database/action_network_repository.py

**Direct Mappings:**
- Line 6: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 6: core_betting. → curated.
- Line 7: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 7: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 7: core_betting. → curated.
- Line 8: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 8: core_betting. → curated.
- Line 9: core_betting.sportsbooks → curated.sportsbooks
- Line 9: core_betting. → curated.
- Line 144: core_betting.games → curated.games_complete
- Line 144: core_betting. → curated.
- Line 154: core_betting.games → curated.games_complete
- Line 154: core_betting. → curated.
- Line 164: core_betting.games → curated.games_complete
- Line 164: core_betting. → curated.
- Line 187: core_betting.games → curated.games_complete
- Line 187: core_betting. → curated.
- Line 218: core_betting.games → curated.games_complete
- Line 218: core_betting. → curated.
- Line 221: core_betting.games → curated.games_complete
- Line 221: core_betting. → curated.
- Line 324: core_betting.sportsbooks → curated.sportsbooks
- Line 324: core_betting. → curated.
- Line 336: core_betting.sportsbooks → curated.sportsbooks
- Line 336: core_betting. → curated.
- Line 406: core_betting. → curated.
- Line 434: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 434: core_betting. → curated.
- Line 525: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 525: core_betting. → curated.
- Line 577: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 577: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 577: core_betting. → curated.
- Line 666: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 666: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 666: core_betting. → curated.
- Line 716: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 716: core_betting. → curated.
- Line 794: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 794: core_betting. → curated.
- Line 913: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 913: core_betting. → curated.
- Line 929: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 929: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 929: core_betting. → curated.
- Line 945: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 945: core_betting. → curated.
- Line 969: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 969: core_betting. → curated.
- Line 972: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 972: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 972: core_betting. → curated.
- Line 975: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 975: core_betting. → curated.
- Line 978: core_betting.sportsbooks → curated.sportsbooks
- Line 978: core_betting. → curated.

#### src/data/pipeline/raw_zone_adapter.py

**Direct Mappings:**
- Line 4: core_betting. → curated.

#### src/data/collection/vsin_unified_collector.py

**Direct Mappings:**
- Line 2245: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 2245: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 2245: core_betting. → curated.
- Line 2293: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 2293: core_betting. → curated.
- Line 2299: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 2299: core_betting. → curated.

#### src/data/collection/monitoring.py

**Direct Mappings:**
- Line 102: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 102: core_betting. → curated.
- Line 131: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 131: core_betting. → curated.
- Line 240: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 240: core_betting. → curated.
- Line 250: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 250: core_betting. → curated.
- Line 424: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 424: core_betting. → curated.

#### src/data/collection/unified_betting_lines_collector.py

**Direct Mappings:**
- Line 154: core_betting.games → curated.games_complete
- Line 154: core_betting. → curated.
- Line 184: core_betting.games → curated.games_complete
- Line 184: core_betting. → curated.
- Line 190: core_betting.games → curated.games_complete
- Line 190: core_betting. → curated.
- Line 261: core_betting.games → curated.games_complete
- Line 261: core_betting. → curated.
- Line 353: core_betting.sportsbooks → curated.sportsbooks
- Line 353: core_betting. → curated.
- Line 361: core_betting.sportsbooks → curated.sportsbooks
- Line 361: core_betting. → curated.
- Line 369: core_betting.sportsbooks → curated.sportsbooks
- Line 369: core_betting. → curated.
- Line 1040: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 1040: core_betting. → curated.
- Line 1075: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 1075: core_betting. → curated.
- Line 1110: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 1110: core_betting. → curated.

#### src/data/collection/analytics.py

**Direct Mappings:**
- Line 146: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 146: core_betting. → curated.
- Line 269: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 269: core_betting. → curated.
- Line 360: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 360: core_betting. → curated.
- Line 361: core_betting.games → curated.games_complete
- Line 361: core_betting. → curated.
- Line 474: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 474: core_betting. → curated.
- Line 604: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 604: core_betting. → curated.
- Line 870: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 870: core_betting. → curated.

#### src/services/game_id_resolution_service.py

**Direct Mappings:**
- Line 11: core_betting.games → curated.games_complete
- Line 11: core_betting. → curated.
- Line 37: core_betting.games → curated.games_complete
- Line 37: core_betting. → curated.
- Line 220: core_betting.games → curated.games_complete
- Line 220: core_betting. → curated.
- Line 297: core_betting.games → curated.games_complete
- Line 297: core_betting. → curated.
- Line 301: core_betting.games → curated.games_complete
- Line 301: core_betting. → curated.
- Line 352: core_betting.games → curated.games_complete
- Line 352: core_betting. → curated.

#### src/services/sharp_action_detection_service.py

**Direct Mappings:**
- Line 116: core_betting.games → curated.games_complete
- Line 116: core_betting. → curated.
- Line 167: core_betting. → curated.
- Line 361: core_betting. → curated.
- Line 417: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 417: core_betting. → curated.
- Line 418: core_betting.games → curated.games_complete
- Line 418: core_betting. → curated.
- Line 428: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 428: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 428: core_betting. → curated.
- Line 429: core_betting.games → curated.games_complete
- Line 429: core_betting. → curated.
- Line 439: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 439: core_betting. → curated.
- Line 440: core_betting.games → curated.games_complete
- Line 440: core_betting. → curated.
- Line 474: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 474: core_betting. → curated.

#### src/services/cross_source_game_matching_service.py

**Direct Mappings:**
- Line 121: core_betting.games → curated.games_complete
- Line 121: core_betting. → curated.
- Line 244: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 244: core_betting. → curated.
- Line 249: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 249: core_betting. → curated.
- Line 254: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 254: core_betting. → curated.
- Line 259: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 259: core_betting. → curated.
- Line 506: core_betting.games → curated.games_complete
- Line 506: core_betting. → curated.
- Line 561: core_betting.games → curated.games_complete
- Line 561: core_betting. → curated.
- Line 621: core_betting.games → curated.games_complete
- Line 621: core_betting. → curated.
- Line 667: core_betting.games → curated.games_complete
- Line 667: core_betting. → curated.
- Line 747: core_betting.games → curated.games_complete
- Line 747: core_betting. → curated.

#### src/services/game_outcome_service.py

**Direct Mappings:**
- Line 6: core_betting.game_outcomes → curated.game_outcomes
- Line 6: core_betting. → curated.
- Line 13: core_betting.game_outcomes → curated.game_outcomes
- Line 13: core_betting. → curated.
- Line 41: core_betting.games → curated.games_complete
- Line 41: core_betting. → curated.
- Line 253: core_betting.game_outcomes → curated.game_outcomes
- Line 253: core_betting. → curated.
- Line 548: core_betting.games → curated.games_complete
- Line 548: core_betting. → curated.
- Line 574: core_betting.games → curated.games_complete
- Line 574: core_betting. → curated.
- Line 584: core_betting.games → curated.games_complete
- Line 584: core_betting. → curated.
- Line 585: core_betting.game_outcomes → curated.game_outcomes
- Line 585: core_betting. → curated.
- Line 881: core_betting.games → curated.games_complete
- Line 881: core_betting. → curated.
- Line 890: core_betting.games → curated.games_complete
- Line 890: core_betting. → curated.
- Line 893: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 893: core_betting. → curated.
- Line 900: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 900: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 900: core_betting. → curated.
- Line 934: core_betting.game_outcomes → curated.game_outcomes
- Line 934: core_betting. → curated.
- Line 1003: core_betting.game_outcomes → curated.game_outcomes
- Line 1003: core_betting. → curated.
- Line 1004: core_betting.games → curated.games_complete
- Line 1004: core_betting. → curated.
- Line 1050: core_betting.game_outcomes → curated.game_outcomes
- Line 1050: core_betting. → curated.
- Line 1051: core_betting.games → curated.games_complete
- Line 1051: core_betting. → curated.
- Line 1159: core_betting.games → curated.games_complete
- Line 1159: core_betting. → curated.
- Line 1168: core_betting.games → curated.games_complete
- Line 1168: core_betting. → curated.
- Line 1185: core_betting.games → curated.games_complete
- Line 1185: core_betting. → curated.
- Line 1211: core_betting.game_outcomes → curated.game_outcomes
- Line 1211: core_betting. → curated.

#### src/services/mlb_stats_api_game_resolution_service.py

**Direct Mappings:**
- Line 531: core_betting.games → curated.games_complete
- Line 531: core_betting. → curated.
- Line 786: core_betting.games → curated.games_complete
- Line 786: core_betting. → curated.
- Line 794: core_betting.games → curated.games_complete
- Line 794: core_betting. → curated.
- Line 802: core_betting.games → curated.games_complete
- Line 802: core_betting. → curated.
- Line 854: core_betting.games → curated.games_complete
- Line 854: core_betting. → curated.
- Line 878: core_betting.games → curated.games_complete
- Line 878: core_betting. → curated.

#### src/services/game/game_manager_service.py

**Direct Mappings:**
- Line 165: core_betting.games → curated.games_complete
- Line 165: core_betting. → curated.
- Line 168: core_betting.games → curated.games_complete
- Line 168: core_betting. → curated.
- Line 171: core_betting.games → curated.games_complete
- Line 171: core_betting. → curated.
- Line 175: core_betting.games → curated.games_complete
- Line 175: core_betting. → curated.
- Line 210: core_betting.games → curated.games_complete
- Line 210: core_betting. → curated.
- Line 250: core_betting.games → curated.games_complete
- Line 250: core_betting. → curated.
- Line 292: core_betting.games → curated.games_complete
- Line 292: core_betting. → curated.
- Line 297: core_betting.games → curated.games_complete
- Line 297: core_betting. → curated.
- Line 304: core_betting.games → curated.games_complete
- Line 304: core_betting. → curated.
- Line 311: core_betting.games → curated.games_complete
- Line 311: core_betting. → curated.
- Line 318: core_betting.games → curated.games_complete
- Line 318: core_betting. → curated.
- Line 325: core_betting.games → curated.games_complete
- Line 325: core_betting. → curated.
- Line 433: core_betting.games → curated.games_complete
- Line 433: core_betting. → curated.
- Line 444: core_betting.games → curated.games_complete
- Line 444: core_betting. → curated.
- Line 477: core_betting.games → curated.games_complete
- Line 477: core_betting. → curated.
- Line 502: core_betting.games → curated.games_complete
- Line 502: core_betting. → curated.

#### src/services/orchestration/pipeline_orchestration_service.py

**Direct Mappings:**
- Line 535: core_betting.games → curated.games_complete
- Line 535: core_betting. → curated.

#### src/interfaces/cli/commands/line_movement.py

**Direct Mappings:**
- Line 249: core_betting. → curated.
- Line 301: core_betting. → curated.
- Line 311: core_betting. → curated.
- Line 324: core_betting. → curated.
- Line 325: core_betting.sportsbooks → curated.sportsbooks
- Line 325: core_betting. → curated.

#### src/interfaces/cli/commands/data_quality_improvement.py

**Direct Mappings:**
- Line 239: core_betting. → curated.
- Line 276: core_betting. → curated.
- Line 304: core_betting. → curated.
- Line 378: core_betting. → curated.
- Line 388: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 388: core_betting. → curated.

#### src/interfaces/cli/commands/pipeline.py

**Direct Mappings:**
- Line 265: core_betting. → curated.
- Line 288: core_betting. → curated.
- Line 289: core_betting. → curated.
- Line 290: core_betting. → curated.

#### src/interfaces/cli/commands/game_outcomes.py

**Direct Mappings:**
- Line 6: core_betting.game_outcomes → curated.game_outcomes
- Line 6: core_betting. → curated.
- Line 220: core_betting.games → curated.games_complete
- Line 220: core_betting. → curated.
- Line 238: core_betting.games → curated.games_complete
- Line 238: core_betting. → curated.
- Line 270: core_betting.game_outcomes → curated.game_outcomes
- Line 270: core_betting. → curated.
- Line 611: core_betting.games → curated.games_complete
- Line 611: core_betting. → curated.
- Line 612: core_betting.game_outcomes → curated.game_outcomes
- Line 612: core_betting. → curated.
- Line 644: core_betting.games → curated.games_complete
- Line 644: core_betting. → curated.
- Line 645: core_betting.game_outcomes → curated.game_outcomes
- Line 645: core_betting. → curated.
- Line 679: core_betting.games → curated.games_complete
- Line 679: core_betting. → curated.
- Line 680: core_betting.game_outcomes → curated.game_outcomes
- Line 680: core_betting. → curated.
- Line 723: core_betting.games → curated.games_complete
- Line 723: core_betting. → curated.
- Line 724: core_betting.game_outcomes → curated.game_outcomes
- Line 724: core_betting. → curated.
- Line 754: core_betting.games → curated.games_complete
- Line 754: core_betting. → curated.

#### src/interfaces/cli/commands/data.py

**Direct Mappings:**
- Line 1198: core_betting. → curated.

#### sql/schema_migration_phase1_corrected.sql

**Direct Mappings:**
- Line 163: core_betting.teams → curated.teams_master
- Line 163: core_betting. → curated.
- Line 177: core_betting.teams → curated.teams_master
- Line 177: core_betting. → curated.
- Line 181: core_betting.teams → curated.teams_master
- Line 181: core_betting. → curated.
- Line 187: core_betting.games → curated.games_complete
- Line 187: core_betting. → curated.
- Line 214: core_betting.games → curated.games_complete
- Line 214: core_betting. → curated.
- Line 216: core_betting.games → curated.games_complete
- Line 216: core_betting. → curated.
- Line 221: core_betting.games → curated.games_complete
- Line 221: core_betting. → curated.
- Line 226: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 226: core_betting. → curated.
- Line 227: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 227: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 227: core_betting. → curated.
- Line 228: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 228: core_betting. → curated.

#### sql/phase2b_continuation.sql

**Direct Mappings:**
- Line 20: core_betting.games → curated.games_complete
- Line 20: core_betting. → curated.
- Line 23: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 23: core_betting. → curated.
- Line 85: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 85: core_betting. → curated.
- Line 95: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 95: core_betting. → curated.
- Line 105: core_betting.game_outcomes → curated.game_outcomes
- Line 105: core_betting. → curated.
- Line 108: core_betting.game_outcomes → curated.game_outcomes
- Line 108: core_betting. → curated.
- Line 140: core_betting.game_outcomes → curated.game_outcomes
- Line 140: core_betting. → curated.
- Line 148: core_betting.game_outcomes → curated.game_outcomes
- Line 148: core_betting. → curated.
- Line 335: core_betting.games → curated.games_complete
- Line 335: core_betting. → curated.
- Line 336: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 336: core_betting. → curated.
- Line 337: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 337: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 337: core_betting. → curated.
- Line 338: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 338: core_betting. → curated.
- Line 339: core_betting.game_outcomes → curated.game_outcomes
- Line 339: core_betting. → curated.
- Line 360: core_betting.games → curated.games_complete
- Line 360: core_betting. → curated.
- Line 361: core_betting.games → curated.games_complete
- Line 361: core_betting. → curated.
- Line 366: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 366: core_betting. → curated.
- Line 367: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 367: core_betting. → curated.
- Line 372: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 372: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 372: core_betting. → curated.
- Line 373: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 373: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 373: core_betting. → curated.
- Line 378: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 378: core_betting. → curated.
- Line 379: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 379: core_betting. → curated.
- Line 384: core_betting.game_outcomes → curated.game_outcomes
- Line 384: core_betting. → curated.
- Line 385: core_betting.game_outcomes → curated.game_outcomes
- Line 385: core_betting. → curated.

#### sql/schema_migration_phase1.sql

**Direct Mappings:**
- Line 158: core_betting.games → curated.games_complete
- Line 158: core_betting. → curated.
- Line 177: core_betting.games → curated.games_complete
- Line 177: core_betting. → curated.
- Line 178: core_betting.games → curated.games_complete
- Line 178: core_betting. → curated.
- Line 183: core_betting.games → curated.games_complete
- Line 183: core_betting. → curated.
- Line 192: core_betting.games → curated.games_complete
- Line 192: core_betting. → curated.
- Line 199: core_betting.games → curated.games_complete
- Line 199: core_betting. → curated.
- Line 200: core_betting.games → curated.games_complete
- Line 200: core_betting. → curated.
- Line 210: core_betting.game_outcomes → curated.game_outcomes
- Line 210: core_betting. → curated.
- Line 216: core_betting.games → curated.games_complete
- Line 216: core_betting. → curated.
- Line 222: core_betting.games → curated.games_complete
- Line 222: core_betting. → curated.
- Line 236: core_betting.game_outcomes → curated.game_outcomes
- Line 236: core_betting. → curated.
- Line 237: core_betting.game_outcomes → curated.game_outcomes
- Line 237: core_betting. → curated.
- Line 239: core_betting.game_outcomes → curated.game_outcomes
- Line 239: core_betting. → curated.
- Line 243: core_betting.teams → curated.teams_master
- Line 243: core_betting. → curated.
- Line 249: core_betting.teams → curated.teams_master
- Line 249: core_betting. → curated.
- Line 265: core_betting.teams → curated.teams_master
- Line 265: core_betting. → curated.
- Line 266: core_betting.teams → curated.teams_master
- Line 266: core_betting. → curated.
- Line 268: core_betting.teams → curated.teams_master
- Line 268: core_betting. → curated.
- Line 279: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 279: core_betting. → curated.
- Line 297: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 297: core_betting. → curated.
- Line 298: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 298: core_betting. → curated.
- Line 300: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 300: core_betting. → curated.
- Line 310: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 310: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 310: core_betting. → curated.
- Line 332: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 332: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 332: core_betting. → curated.
- Line 333: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 333: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 333: core_betting. → curated.
- Line 335: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 335: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 335: core_betting. → curated.
- Line 345: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 345: core_betting. → curated.
- Line 363: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 363: core_betting. → curated.
- Line 364: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 364: core_betting. → curated.
- Line 366: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 366: core_betting. → curated.
- Line 420: core_betting.games → curated.games_complete
- Line 420: core_betting. → curated.
- Line 432: core_betting.games → curated.games_complete
- Line 432: core_betting. → curated.

#### sql/final_validation_suite.sql

**Direct Mappings:**
- Line 73: core_betting.teams → curated.teams_master
- Line 73: core_betting. → curated.
- Line 77: core_betting.supplementary_games → curated.games_complete
- Line 77: core_betting. → curated.
- Line 128: core_betting.games → curated.games_complete
- Line 128: core_betting. → curated.
- Line 129: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 129: core_betting. → curated.
- Line 130: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 130: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 130: core_betting. → curated.
- Line 131: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 131: core_betting. → curated.
- Line 132: core_betting.teams → curated.teams_master
- Line 132: core_betting. → curated.
- Line 177: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 177: core_betting. → curated.
- Line 178: core_betting.games → curated.games_complete
- Line 178: core_betting. → curated.
- Line 182: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 182: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 182: core_betting. → curated.
- Line 183: core_betting.games → curated.games_complete
- Line 183: core_betting. → curated.
- Line 187: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 187: core_betting. → curated.
- Line 188: core_betting.games → curated.games_complete
- Line 188: core_betting. → curated.
- Line 294: core_betting.games → curated.games_complete
- Line 294: core_betting. → curated.
- Line 295: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 295: core_betting. → curated.

#### sql/phase2b_fix_game_outcomes.sql

**Direct Mappings:**
- Line 12: core_betting.games → curated.games_complete
- Line 12: core_betting. → curated.
- Line 15: core_betting.game_outcomes → curated.game_outcomes
- Line 15: core_betting. → curated.
- Line 47: core_betting.game_outcomes → curated.game_outcomes
- Line 47: core_betting. → curated.
- Line 55: core_betting.game_outcomes → curated.game_outcomes
- Line 55: core_betting. → curated.
- Line 232: core_betting.games → curated.games_complete
- Line 232: core_betting. → curated.
- Line 233: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 233: core_betting. → curated.
- Line 234: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 234: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 234: core_betting. → curated.
- Line 235: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 235: core_betting. → curated.
- Line 236: core_betting.game_outcomes → curated.game_outcomes
- Line 236: core_betting. → curated.
- Line 252: core_betting.games → curated.games_complete
- Line 252: core_betting. → curated.
- Line 257: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 257: core_betting. → curated.
- Line 262: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 262: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 262: core_betting. → curated.
- Line 267: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 267: core_betting. → curated.
- Line 272: core_betting.game_outcomes → curated.game_outcomes
- Line 272: core_betting. → curated.

#### sql/phase2b_final_fix.sql

**Direct Mappings:**
- Line 12: core_betting.games → curated.games_complete
- Line 12: core_betting. → curated.
- Line 15: core_betting.game_outcomes → curated.game_outcomes
- Line 15: core_betting. → curated.
- Line 48: core_betting.game_outcomes → curated.game_outcomes
- Line 48: core_betting. → curated.
- Line 56: core_betting.game_outcomes → curated.game_outcomes
- Line 56: core_betting. → curated.
- Line 248: core_betting.games → curated.games_complete
- Line 248: core_betting. → curated.
- Line 249: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 249: core_betting. → curated.
- Line 250: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 250: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 250: core_betting. → curated.
- Line 251: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 251: core_betting. → curated.
- Line 252: core_betting.game_outcomes → curated.game_outcomes
- Line 252: core_betting. → curated.
- Line 273: core_betting.games → curated.games_complete
- Line 273: core_betting. → curated.
- Line 278: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 278: core_betting. → curated.
- Line 283: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 283: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 283: core_betting. → curated.
- Line 288: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 288: core_betting. → curated.
- Line 293: core_betting.game_outcomes → curated.game_outcomes
- Line 293: core_betting. → curated.

#### sql/phase2b_completion.sql

**Direct Mappings:**
- Line 6: core_betting.games → curated.games_complete
- Line 6: core_betting. → curated.
- Line 7: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 7: core_betting. → curated.
- Line 8: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 8: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 8: core_betting. → curated.
- Line 9: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 9: core_betting. → curated.
- Line 10: core_betting.game_outcomes → curated.game_outcomes
- Line 10: core_betting. → curated.
- Line 33: core_betting.games → curated.games_complete
- Line 33: core_betting. → curated.
- Line 38: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 38: core_betting. → curated.
- Line 43: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 43: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 43: core_betting. → curated.
- Line 48: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 48: core_betting. → curated.
- Line 53: core_betting.game_outcomes → curated.game_outcomes
- Line 53: core_betting. → curated.

#### sql/consolidated_schema.sql

**Direct Mappings:**
- Line 137: core_betting.games → curated.games_complete
- Line 137: core_betting. → curated.
- Line 185: core_betting.game_outcomes → curated.game_outcomes
- Line 185: core_betting. → curated.
- Line 187: core_betting.games → curated.games_complete
- Line 187: core_betting. → curated.
- Line 211: core_betting.teams → curated.teams_master
- Line 211: core_betting. → curated.
- Line 230: core_betting.sportsbooks → curated.sportsbooks
- Line 230: core_betting. → curated.
- Line 241: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 241: core_betting. → curated.
- Line 243: core_betting.games → curated.games_complete
- Line 243: core_betting. → curated.
- Line 244: core_betting.sportsbooks → curated.sportsbooks
- Line 244: core_betting. → curated.
- Line 283: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 283: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 283: core_betting. → curated.
- Line 285: core_betting.games → curated.games_complete
- Line 285: core_betting. → curated.
- Line 286: core_betting.sportsbooks → curated.sportsbooks
- Line 286: core_betting. → curated.
- Line 333: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 333: core_betting. → curated.
- Line 335: core_betting.games → curated.games_complete
- Line 335: core_betting. → curated.
- Line 336: core_betting.sportsbooks → curated.sportsbooks
- Line 336: core_betting. → curated.
- Line 379: core_betting. → curated.
- Line 381: core_betting.games → curated.games_complete
- Line 381: core_betting. → curated.
- Line 393: core_betting. → curated.
- Line 395: core_betting.games → curated.games_complete
- Line 395: core_betting. → curated.
- Line 405: core_betting.betting_splits → curated.betting_splits
- Line 405: core_betting. → curated.
- Line 407: core_betting.games → curated.games_complete
- Line 407: core_betting. → curated.
- Line 432: core_betting. → curated.
- Line 434: core_betting.games → curated.games_complete
- Line 434: core_betting. → curated.
- Line 444: core_betting.games → curated.games_complete
- Line 444: core_betting. → curated.
- Line 445: core_betting.games → curated.games_complete
- Line 445: core_betting. → curated.
- Line 446: core_betting.games → curated.games_complete
- Line 446: core_betting. → curated.
- Line 447: core_betting.game_outcomes → curated.game_outcomes
- Line 447: core_betting. → curated.
- Line 448: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 448: core_betting. → curated.
- Line 449: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 449: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 449: core_betting. → curated.
- Line 450: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 450: core_betting. → curated.
- Line 451: core_betting.betting_splits → curated.betting_splits
- Line 451: core_betting. → curated.
- Line 452: core_betting. → curated.
- Line 463: core_betting.games → curated.games_complete
- Line 463: core_betting. → curated.
- Line 478: core_betting.games → curated.games_complete
- Line 478: core_betting. → curated.
- Line 531: core_betting.games → curated.games_complete
- Line 531: core_betting. → curated.
- Line 542: core_betting.games → curated.games_complete
- Line 542: core_betting. → curated.
- Line 845: core_betting.game_outcomes → curated.game_outcomes
- Line 845: core_betting. → curated.
- Line 846: core_betting.games → curated.games_complete
- Line 846: core_betting. → curated.

#### sql/migrate_action_network_data.sql

**Direct Mappings:**
- Line 6: core_betting.teams → curated.teams_master
- Line 6: core_betting. → curated.
- Line 7: core_betting. → curated.
- Line 8: core_betting. → curated.
- Line 65: core_betting.teams → curated.teams_master
- Line 65: core_betting. → curated.
- Line 69: core_betting.teams → curated.teams_master
- Line 69: core_betting. → curated.
- Line 70: core_betting.teams → curated.teams_master
- Line 70: core_betting. → curated.
- Line 79: core_betting.teams → curated.teams_master
- Line 79: core_betting. → curated.
- Line 88: core_betting.teams → curated.teams_master
- Line 88: core_betting. → curated.
- Line 89: core_betting.teams → curated.teams_master
- Line 89: core_betting. → curated.
- Line 92: core_betting.teams → curated.teams_master
- Line 92: core_betting. → curated.
- Line 104: core_betting.teams → curated.teams_master
- Line 104: core_betting. → curated.
- Line 105: core_betting.teams → curated.teams_master
- Line 105: core_betting. → curated.
- Line 109: core_betting.teams → curated.teams_master
- Line 109: core_betting. → curated.
- Line 110: core_betting.teams → curated.teams_master
- Line 110: core_betting. → curated.
- Line 112: core_betting.teams → curated.teams_master
- Line 112: core_betting. → curated.
- Line 113: core_betting.teams → curated.teams_master
- Line 113: core_betting. → curated.
- Line 121: core_betting. → curated.
- Line 129: core_betting. → curated.
- Line 156: core_betting. → curated.
- Line 157: core_betting. → curated.
- Line 159: core_betting. → curated.
- Line 164: core_betting. → curated.
- Line 172: core_betting. → curated.
- Line 193: core_betting. → curated.
- Line 194: core_betting. → curated.
- Line 196: core_betting. → curated.
- Line 213: core_betting.teams → curated.teams_master
- Line 213: core_betting. → curated.
- Line 218: core_betting. → curated.
- Line 221: core_betting. → curated.

#### sql/phase4b_data_migration_corrected.sql

**Direct Mappings:**
- Line 142: core_betting.game_outcomes → curated.game_outcomes
- Line 142: core_betting. → curated.
- Line 150: core_betting.game_outcomes → curated.game_outcomes
- Line 150: core_betting. → curated.
- Line 184: core_betting.game_outcomes → curated.game_outcomes
- Line 184: core_betting. → curated.
- Line 185: core_betting.game_outcomes → curated.game_outcomes
- Line 185: core_betting. → curated.
- Line 187: core_betting.game_outcomes → curated.game_outcomes
- Line 187: core_betting. → curated.
- Line 245: core_betting. → curated.
- Line 272: core_betting. → curated.
- Line 273: core_betting. → curated.
- Line 285: core_betting. → curated.
- Line 330: core_betting. → curated.
- Line 331: core_betting. → curated.
- Line 335: core_betting.teams → curated.teams_master
- Line 335: core_betting. → curated.
- Line 341: core_betting.teams → curated.teams_master
- Line 341: core_betting. → curated.
- Line 346: core_betting.teams → curated.teams_master
- Line 346: core_betting. → curated.
- Line 357: core_betting.teams → curated.teams_master
- Line 357: core_betting. → curated.
- Line 358: core_betting.teams → curated.teams_master
- Line 358: core_betting. → curated.
- Line 361: core_betting.teams → curated.teams_master
- Line 361: core_betting. → curated.
- Line 377: core_betting.supplementary_games → curated.games_complete
- Line 377: core_betting. → curated.
- Line 408: core_betting.supplementary_games → curated.games_complete
- Line 408: core_betting. → curated.
- Line 409: core_betting.supplementary_games → curated.games_complete
- Line 409: core_betting. → curated.

#### sql/migrate_splits_data.sql

**Direct Mappings:**
- Line 6: core_betting.supplementary_games → curated.games_complete
- Line 6: core_betting. → curated.
- Line 69: core_betting.supplementary_games → curated.games_complete
- Line 69: core_betting. → curated.
- Line 72: core_betting.supplementary_games → curated.games_complete
- Line 72: core_betting. → curated.
- Line 90: core_betting.supplementary_games → curated.games_complete
- Line 90: core_betting. → curated.
- Line 93: core_betting.supplementary_games → curated.games_complete
- Line 93: core_betting. → curated.
- Line 165: core_betting.supplementary_games → curated.games_complete
- Line 165: core_betting. → curated.
- Line 190: core_betting. → curated.

#### sql/phase4a_schema_extension.sql

**Direct Mappings:**
- Line 61: core_betting.game_outcomes → curated.game_outcomes
- Line 61: core_betting. → curated.
- Line 81: core_betting. → curated.
- Line 99: core_betting. → curated.
- Line 122: core_betting. → curated.
- Line 126: core_betting.supplementary_games → curated.games_complete
- Line 126: core_betting. → curated.
- Line 429: core_betting.game_outcomes → curated.game_outcomes
- Line 429: core_betting. → curated.
- Line 430: core_betting.game_outcomes → curated.game_outcomes
- Line 430: core_betting. → curated.
- Line 431: core_betting. → curated.
- Line 432: core_betting.supplementary_games → curated.games_complete
- Line 432: core_betting. → curated.
- Line 433: core_betting.supplementary_games → curated.games_complete
- Line 433: core_betting. → curated.

#### sql/phase2b_historical_data_migration.sql

**Direct Mappings:**
- Line 17: core_betting.games → curated.games_complete
- Line 17: core_betting. → curated.
- Line 18: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 18: core_betting. → curated.
- Line 19: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 19: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 19: core_betting. → curated.
- Line 20: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 20: core_betting. → curated.
- Line 52: core_betting.games → curated.games_complete
- Line 52: core_betting. → curated.
- Line 54: core_betting.games → curated.games_complete
- Line 54: core_betting. → curated.
- Line 59: core_betting.games → curated.games_complete
- Line 59: core_betting. → curated.
- Line 119: core_betting.games → curated.games_complete
- Line 119: core_betting. → curated.
- Line 127: core_betting.games → curated.games_complete
- Line 127: core_betting. → curated.
- Line 137: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 137: core_betting. → curated.
- Line 146: core_betting.games → curated.games_complete
- Line 146: core_betting. → curated.
- Line 149: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 149: core_betting. → curated.
- Line 203: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 203: core_betting. → curated.
- Line 213: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 213: core_betting. → curated.
- Line 223: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 223: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 223: core_betting. → curated.
- Line 226: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 226: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 226: core_betting. → curated.
- Line 296: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 296: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 296: core_betting. → curated.
- Line 306: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 306: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 306: core_betting. → curated.
- Line 316: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 316: core_betting. → curated.
- Line 319: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 319: core_betting. → curated.
- Line 381: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 381: core_betting. → curated.
- Line 391: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 391: core_betting. → curated.
- Line 401: core_betting.game_outcomes → curated.game_outcomes
- Line 401: core_betting. → curated.
- Line 404: core_betting.game_outcomes → curated.game_outcomes
- Line 404: core_betting. → curated.
- Line 436: core_betting.game_outcomes → curated.game_outcomes
- Line 436: core_betting. → curated.
- Line 444: core_betting.game_outcomes → curated.game_outcomes
- Line 444: core_betting. → curated.
- Line 615: core_betting.games → curated.games_complete
- Line 615: core_betting. → curated.
- Line 616: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 616: core_betting. → curated.
- Line 617: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 617: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 617: core_betting. → curated.
- Line 618: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 618: core_betting. → curated.
- Line 619: core_betting.game_outcomes → curated.game_outcomes
- Line 619: core_betting. → curated.
- Line 640: core_betting.games → curated.games_complete
- Line 640: core_betting. → curated.
- Line 641: core_betting.games → curated.games_complete
- Line 641: core_betting. → curated.
- Line 646: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 646: core_betting. → curated.
- Line 647: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 647: core_betting. → curated.
- Line 652: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 652: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 652: core_betting. → curated.
- Line 653: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 653: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 653: core_betting. → curated.
- Line 658: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 658: core_betting. → curated.
- Line 659: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 659: core_betting. → curated.
- Line 664: core_betting.game_outcomes → curated.game_outcomes
- Line 664: core_betting. → curated.
- Line 665: core_betting.game_outcomes → curated.game_outcomes
- Line 665: core_betting. → curated.

#### sql/phase4b_data_migration.sql

**Direct Mappings:**
- Line 136: core_betting.game_outcomes → curated.game_outcomes
- Line 136: core_betting. → curated.
- Line 144: core_betting.game_outcomes → curated.game_outcomes
- Line 144: core_betting. → curated.
- Line 184: core_betting.game_outcomes → curated.game_outcomes
- Line 184: core_betting. → curated.
- Line 185: core_betting.game_outcomes → curated.game_outcomes
- Line 185: core_betting. → curated.
- Line 187: core_betting.game_outcomes → curated.game_outcomes
- Line 187: core_betting. → curated.
- Line 245: core_betting. → curated.
- Line 272: core_betting. → curated.
- Line 273: core_betting. → curated.
- Line 285: core_betting. → curated.
- Line 330: core_betting. → curated.
- Line 331: core_betting. → curated.
- Line 335: core_betting.teams → curated.teams_master
- Line 335: core_betting. → curated.
- Line 341: core_betting.teams → curated.teams_master
- Line 341: core_betting. → curated.
- Line 346: core_betting.teams → curated.teams_master
- Line 346: core_betting. → curated.
- Line 357: core_betting.teams → curated.teams_master
- Line 357: core_betting. → curated.
- Line 358: core_betting.teams → curated.teams_master
- Line 358: core_betting. → curated.
- Line 361: core_betting.teams → curated.teams_master
- Line 361: core_betting. → curated.
- Line 377: core_betting.supplementary_games → curated.games_complete
- Line 377: core_betting. → curated.
- Line 408: core_betting.supplementary_games → curated.games_complete
- Line 408: core_betting. → curated.
- Line 409: core_betting.supplementary_games → curated.games_complete
- Line 409: core_betting. → curated.

#### sql/add_team_datetime_to_betting_tables.sql

**Direct Mappings:**
- Line 9: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 9: core_betting. → curated.
- Line 15: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 15: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 15: core_betting. → curated.
- Line 21: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 21: core_betting. → curated.
- Line 27: core_betting.betting_splits → curated.betting_splits
- Line 27: core_betting. → curated.
- Line 33: core_betting. → curated.
- Line 39: core_betting. → curated.
- Line 49: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 49: core_betting. → curated.
- Line 54: core_betting.games → curated.games_complete
- Line 54: core_betting. → curated.
- Line 55: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 55: core_betting. → curated.
- Line 56: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 56: core_betting. → curated.
- Line 59: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 59: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 59: core_betting. → curated.
- Line 64: core_betting.games → curated.games_complete
- Line 64: core_betting. → curated.
- Line 65: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 65: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 65: core_betting. → curated.
- Line 66: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 66: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 66: core_betting. → curated.
- Line 69: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 69: core_betting. → curated.
- Line 74: core_betting.games → curated.games_complete
- Line 74: core_betting. → curated.
- Line 75: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 75: core_betting. → curated.
- Line 76: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 76: core_betting. → curated.
- Line 79: core_betting.betting_splits → curated.betting_splits
- Line 79: core_betting. → curated.
- Line 84: core_betting.games → curated.games_complete
- Line 84: core_betting. → curated.
- Line 85: core_betting.betting_splits → curated.betting_splits
- Line 85: core_betting. → curated.
- Line 86: core_betting.betting_splits → curated.betting_splits
- Line 86: core_betting. → curated.
- Line 89: core_betting. → curated.
- Line 94: core_betting.games → curated.games_complete
- Line 94: core_betting. → curated.
- Line 95: core_betting. → curated.
- Line 96: core_betting. → curated.
- Line 99: core_betting. → curated.
- Line 104: core_betting.games → curated.games_complete
- Line 104: core_betting. → curated.
- Line 105: core_betting. → curated.
- Line 106: core_betting. → curated.
- Line 113: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 113: core_betting. → curated.
- Line 114: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 114: core_betting. → curated.
- Line 115: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 115: core_betting. → curated.
- Line 116: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 116: core_betting. → curated.
- Line 119: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 119: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 119: core_betting. → curated.
- Line 120: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 120: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 120: core_betting. → curated.
- Line 121: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 121: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 121: core_betting. → curated.
- Line 122: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 122: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 122: core_betting. → curated.
- Line 125: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 125: core_betting. → curated.
- Line 126: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 126: core_betting. → curated.
- Line 127: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 127: core_betting. → curated.
- Line 128: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 128: core_betting. → curated.
- Line 131: core_betting.betting_splits → curated.betting_splits
- Line 131: core_betting. → curated.
- Line 132: core_betting.betting_splits → curated.betting_splits
- Line 132: core_betting. → curated.
- Line 133: core_betting.betting_splits → curated.betting_splits
- Line 133: core_betting. → curated.
- Line 134: core_betting.betting_splits → curated.betting_splits
- Line 134: core_betting. → curated.
- Line 137: core_betting. → curated.
- Line 138: core_betting. → curated.
- Line 139: core_betting. → curated.
- Line 140: core_betting. → curated.
- Line 143: core_betting. → curated.
- Line 144: core_betting. → curated.
- Line 145: core_betting. → curated.
- Line 146: core_betting. → curated.
- Line 165: core_betting.games → curated.games_complete
- Line 165: core_betting. → curated.
- Line 174: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 174: core_betting. → curated.
- Line 180: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 180: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 180: core_betting. → curated.
- Line 186: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 186: core_betting. → curated.
- Line 192: core_betting.betting_splits → curated.betting_splits
- Line 192: core_betting. → curated.
- Line 198: core_betting. → curated.
- Line 204: core_betting. → curated.
- Line 220: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 220: core_betting. → curated.
- Line 230: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 230: core_betting. → curated.
- Line 240: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 240: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 240: core_betting. → curated.
- Line 252: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 252: core_betting. → curated.
- Line 261: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 261: core_betting. → curated.
- Line 262: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 262: core_betting. → curated.
- Line 263: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 263: core_betting. → curated.
- Line 265: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 265: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 265: core_betting. → curated.
- Line 266: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 266: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 266: core_betting. → curated.
- Line 267: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 267: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 267: core_betting. → curated.
- Line 269: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 269: core_betting. → curated.
- Line 270: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 270: core_betting. → curated.
- Line 271: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 271: core_betting. → curated.
- Line 273: core_betting.betting_splits → curated.betting_splits
- Line 273: core_betting. → curated.
- Line 274: core_betting.betting_splits → curated.betting_splits
- Line 274: core_betting. → curated.
- Line 275: core_betting.betting_splits → curated.betting_splits
- Line 275: core_betting. → curated.
- Line 277: core_betting. → curated.
- Line 278: core_betting. → curated.
- Line 279: core_betting. → curated.
- Line 281: core_betting. → curated.
- Line 282: core_betting. → curated.
- Line 283: core_betting. → curated.

#### sql/migrations/003_update_sports_book_review_references.sql

**Direct Mappings:**
- Line 43: core_betting. → curated.
- Line 53: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 53: core_betting. → curated.
- Line 57: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 57: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 57: core_betting. → curated.
- Line 61: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 61: core_betting. → curated.
- Line 70: core_betting.games → curated.games_complete
- Line 70: core_betting. → curated.
- Line 130: core_betting. → curated.
- Line 143: core_betting. → curated.
- Line 167: core_betting. → curated.
- Line 193: core_betting. → curated.
- Line 222: core_betting.data_migrations → operational.schema_migrations
- Line 222: core_betting. → curated.
- Line 234: core_betting. → curated.
- Line 250: core_betting. → curated.
- Line 258: core_betting. → curated.
- Line 279: core_betting. → curated.

#### sql/migrations/002_migrate_legacy_data.sql

**Direct Mappings:**
- Line 15: core_betting.data_migrations → operational.schema_migrations
- Line 15: core_betting. → curated.
- Line 33: core_betting. → curated.
- Line 41: core_betting.data_migrations → operational.schema_migrations
- Line 41: core_betting. → curated.
- Line 54: core_betting. → curated.
- Line 61: core_betting.data_migrations → operational.schema_migrations
- Line 61: core_betting. → curated.
- Line 80: core_betting. → curated.
- Line 83: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 83: core_betting. → curated.
- Line 92: core_betting. → curated.
- Line 111: core_betting. → curated.
- Line 126: core_betting. → curated.
- Line 129: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 129: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 129: core_betting. → curated.
- Line 138: core_betting. → curated.
- Line 152: core_betting. → curated.
- Line 167: core_betting. → curated.
- Line 170: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 170: core_betting. → curated.
- Line 179: core_betting. → curated.
- Line 193: core_betting. → curated.
- Line 203: core_betting. → curated.
- Line 228: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 228: core_betting. → curated.
- Line 236: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 236: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 236: core_betting. → curated.
- Line 244: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 244: core_betting. → curated.
- Line 319: core_betting.data_migrations → operational.schema_migrations
- Line 319: core_betting. → curated.
- Line 324: core_betting. → curated.
- Line 335: core_betting. → curated.

#### sql/migrations/001_remove_calculated_columns.sql

**Direct Mappings:**
- Line 12: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 12: core_betting. → curated.
- Line 13: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 13: core_betting. → curated.
- Line 15: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 15: core_betting. → curated.
- Line 16: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 16: core_betting. → curated.
- Line 18: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 18: core_betting. → curated.
- Line 19: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 19: core_betting. → curated.
- Line 25: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 25: core_betting. → curated.
- Line 40: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 40: core_betting. → curated.
- Line 57: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 57: core_betting. → curated.
- Line 75: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 75: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 75: core_betting. → curated.
- Line 115: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 115: core_betting. → curated.
- Line 118: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 118: core_betting. → curated.
- Line 121: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 121: core_betting. → curated.

#### sql/migrations/006_legacy_core_betting_migration_analysis.sql

**Direct Mappings:**
- Line 9: core_betting.supplementary_games → curated.games_complete
- Line 9: core_betting. → curated.
- Line 10: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 10: core_betting. → curated.
- Line 11: core_betting.data_migrations → operational.schema_migrations
- Line 11: core_betting. → curated.
- Line 12: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 12: core_betting. → curated.
- Line 13: core_betting.data_source_metadata → curated.data_sources
- Line 13: core_betting. → curated.
- Line 14: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 14: core_betting. → curated.
- Line 15: core_betting.games → curated.games_complete
- Line 15: core_betting. → curated.
- Line 16: core_betting.game_outcomes → curated.game_outcomes
- Line 16: core_betting. → curated.
- Line 17: core_betting.teams → curated.teams_master
- Line 17: core_betting. → curated.
- Line 18: core_betting.sportsbooks → curated.sportsbooks
- Line 18: core_betting. → curated.
- Line 19: core_betting. → curated.
- Line 20: core_betting. → curated.
- Line 21: core_betting. → curated.
- Line 22: core_betting.betting_splits → curated.betting_splits
- Line 22: core_betting. → curated.
- Line 23: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 23: core_betting. → curated.
- Line 24: core_betting. → curated.
- Line 94: core_betting.teams → curated.teams_master
- Line 94: core_betting. → curated.
- Line 95: core_betting.sportsbooks → curated.sportsbooks
- Line 95: core_betting. → curated.
- Line 96: core_betting.data_source_metadata → curated.data_sources
- Line 96: core_betting. → curated.
- Line 97: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 97: core_betting. → curated.
- Line 100: core_betting.games → curated.games_complete
- Line 100: core_betting. → curated.
- Line 101: core_betting.game_outcomes → curated.game_outcomes
- Line 101: core_betting. → curated.
- Line 102: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 102: core_betting. → curated.
- Line 103: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 103: core_betting. → curated.
- Line 104: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 104: core_betting. → curated.
- Line 105: core_betting. → curated.
- Line 106: core_betting.betting_splits → curated.betting_splits
- Line 106: core_betting. → curated.
- Line 109: core_betting. → curated.
- Line 110: core_betting. → curated.
- Line 111: core_betting. → curated.
- Line 114: core_betting.data_migrations → operational.schema_migrations
- Line 114: core_betting. → curated.
- Line 115: core_betting.supplementary_games → curated.games_complete
- Line 115: core_betting. → curated.
- Line 128: core_betting.games → curated.games_complete
- Line 128: core_betting. → curated.
- Line 135: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 135: core_betting. → curated.
- Line 143: core_betting.games → curated.games_complete
- Line 143: core_betting. → curated.
- Line 151: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 151: core_betting. → curated.

#### sql/migrations/007_comprehensive_legacy_migration_plan.sql

**Direct Mappings:**
- Line 33: core_betting.games → curated.games_complete
- Line 33: core_betting. → curated.
- Line 34: core_betting.teams → curated.teams_master
- Line 34: core_betting. → curated.
- Line 35: core_betting.sportsbooks → curated.sportsbooks
- Line 35: core_betting. → curated.
- Line 36: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 36: core_betting. → curated.
- Line 37: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 37: core_betting. → curated.
- Line 38: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 38: core_betting. → curated.
- Line 39: core_betting.supplementary_games → curated.games_complete
- Line 39: core_betting. → curated.
- Line 40: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 40: core_betting. → curated.
- Line 41: core_betting.data_source_metadata → curated.data_sources
- Line 41: core_betting. → curated.
- Line 42: core_betting.data_migrations → operational.schema_migrations
- Line 42: core_betting. → curated.
- Line 45: core_betting.games → curated.games_complete
- Line 45: core_betting. → curated.
- Line 46: core_betting.teams → curated.teams_master
- Line 46: core_betting. → curated.
- Line 47: core_betting.sportsbooks → curated.sportsbooks
- Line 47: core_betting. → curated.
- Line 90: core_betting.teams → curated.teams_master
- Line 90: core_betting. → curated.
- Line 128: core_betting.sportsbooks → curated.sportsbooks
- Line 128: core_betting. → curated.
- Line 132: core_betting.sportsbooks → curated.sportsbooks
- Line 132: core_betting. → curated.
- Line 165: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 165: core_betting. → curated.
- Line 169: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 169: core_betting. → curated.
- Line 199: core_betting.data_source_metadata → curated.data_sources
- Line 199: core_betting. → curated.
- Line 203: core_betting.data_source_metadata → curated.data_sources
- Line 203: core_betting. → curated.
- Line 304: core_betting.games → curated.games_complete
- Line 304: core_betting. → curated.
- Line 373: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 373: core_betting. → curated.
- Line 410: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 410: core_betting. → curated.
- Line 448: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 448: core_betting. → curated.
- Line 463: core_betting.data_migrations → operational.schema_migrations
- Line 463: core_betting. → curated.
- Line 474: core_betting.supplementary_games → curated.games_complete
- Line 474: core_betting. → curated.
- Line 498: core_betting.teams → curated.teams_master
- Line 498: core_betting. → curated.
- Line 499: core_betting.sportsbooks → curated.sportsbooks
- Line 499: core_betting. → curated.
- Line 500: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 500: core_betting. → curated.
- Line 501: core_betting.data_source_metadata → curated.data_sources
- Line 501: core_betting. → curated.
- Line 502: core_betting.games → curated.games_complete
- Line 502: core_betting. → curated.
- Line 503: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 503: core_betting. → curated.
- Line 504: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 504: core_betting. → curated.
- Line 505: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 505: core_betting. → curated.
- Line 506: core_betting.data_migrations → operational.schema_migrations
- Line 506: core_betting. → curated.
- Line 507: core_betting.supplementary_games → curated.games_complete
- Line 507: core_betting. → curated.
- Line 516: core_betting.teams → curated.teams_master
- Line 516: core_betting. → curated.
- Line 522: core_betting.games → curated.games_complete
- Line 522: core_betting. → curated.
- Line 523: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 523: core_betting. → curated.
- Line 524: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 524: core_betting. → curated.
- Line 525: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 525: core_betting. → curated.

#### sql/migrations/011_create_ml_curated_zone.sql

**Direct Mappings:**
- Line 107: core_betting.sportsbooks → curated.sportsbooks
- Line 107: core_betting. → curated.

#### sql/migrations/001_enhance_source_tracking.sql

**Direct Mappings:**
- Line 15: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 15: core_betting. → curated.
- Line 24: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 24: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 24: core_betting. → curated.
- Line 33: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 33: core_betting. → curated.
- Line 42: core_betting.data_source_metadata → curated.data_sources
- Line 42: core_betting. → curated.
- Line 60: core_betting.data_source_metadata → curated.data_sources
- Line 60: core_betting. → curated.
- Line 90: core_betting. → curated.
- Line 91: core_betting. → curated.
- Line 92: core_betting. → curated.
- Line 105: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 105: core_betting. → curated.
- Line 108: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 108: core_betting. → curated.
- Line 120: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 120: core_betting. → curated.
- Line 123: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 123: core_betting. → curated.
- Line 134: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 134: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 134: core_betting. → curated.
- Line 137: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 137: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 137: core_betting. → curated.
- Line 149: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 149: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 149: core_betting. → curated.
- Line 152: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 152: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 152: core_betting. → curated.
- Line 163: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 163: core_betting. → curated.
- Line 166: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 166: core_betting. → curated.
- Line 178: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 178: core_betting. → curated.
- Line 181: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 181: core_betting. → curated.
- Line 188: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 188: core_betting. → curated.
- Line 191: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 191: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 191: core_betting. → curated.
- Line 194: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 194: core_betting. → curated.
- Line 198: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 198: core_betting. → curated.
- Line 202: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 202: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 202: core_betting. → curated.
- Line 206: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 206: core_betting. → curated.
- Line 210: core_betting. → curated.
- Line 214: core_betting.data_source_metadata → curated.data_sources
- Line 214: core_betting. → curated.
- Line 220: core_betting.data_source_metadata → curated.data_sources
- Line 220: core_betting. → curated.
- Line 227: core_betting. → curated.
- Line 244: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 244: core_betting. → curated.
- Line 255: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 255: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 255: core_betting. → curated.
- Line 266: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 266: core_betting. → curated.
- Line 271: core_betting. → curated.
- Line 275: core_betting. → curated.
- Line 288: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 288: core_betting. → curated.
- Line 290: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 290: core_betting. → curated.
- Line 291: core_betting. → curated.
- Line 293: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 293: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 293: core_betting. → curated.
- Line 295: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 295: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 295: core_betting. → curated.
- Line 296: core_betting. → curated.
- Line 298: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 298: core_betting. → curated.
- Line 300: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 300: core_betting. → curated.
- Line 301: core_betting. → curated.
- Line 304: core_betting.data_source_metadata → curated.data_sources
- Line 304: core_betting. → curated.
- Line 305: core_betting.data_source_metadata → curated.data_sources
- Line 305: core_betting. → curated.
- Line 306: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 306: core_betting. → curated.
- Line 307: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 307: core_betting. → curated.
- Line 310: core_betting. → curated.
- Line 322: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 322: core_betting. → curated.
- Line 325: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 325: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 325: core_betting. → curated.
- Line 328: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 328: core_betting. → curated.
- Line 333: core_betting. → curated.
- Line 336: core_betting. → curated.
- Line 343: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 343: core_betting. → curated.
- Line 351: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 351: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 351: core_betting. → curated.
- Line 359: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 359: core_betting. → curated.
- Line 362: core_betting. → curated.
- Line 371: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 371: core_betting. → curated.
- Line 375: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 375: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 375: core_betting. → curated.
- Line 379: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 379: core_betting. → curated.
- Line 386: core_betting. → curated.
- Line 387: core_betting. → curated.
- Line 393: core_betting. → curated.

#### sql/migrations/008_execute_legacy_migration.sql

**Direct Mappings:**
- Line 26: core_betting.games → curated.games_complete
- Line 26: core_betting. → curated.
- Line 27: core_betting.teams → curated.teams_master
- Line 27: core_betting. → curated.
- Line 28: core_betting.sportsbooks → curated.sportsbooks
- Line 28: core_betting. → curated.
- Line 29: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 29: core_betting. → curated.
- Line 30: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 30: core_betting. → curated.
- Line 31: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 31: core_betting. → curated.
- Line 32: core_betting.supplementary_games → curated.games_complete
- Line 32: core_betting. → curated.
- Line 33: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 33: core_betting. → curated.
- Line 34: core_betting.data_source_metadata → curated.data_sources
- Line 34: core_betting. → curated.
- Line 35: core_betting.data_migrations → operational.schema_migrations
- Line 35: core_betting. → curated.
- Line 74: core_betting.games → curated.games_complete
- Line 74: core_betting. → curated.
- Line 75: core_betting.teams → curated.teams_master
- Line 75: core_betting. → curated.
- Line 76: core_betting.sportsbooks → curated.sportsbooks
- Line 76: core_betting. → curated.
- Line 77: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 77: core_betting. → curated.
- Line 78: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 78: core_betting. → curated.
- Line 79: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 79: core_betting. → curated.
- Line 80: core_betting.supplementary_games → curated.games_complete
- Line 80: core_betting. → curated.
- Line 81: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 81: core_betting. → curated.
- Line 82: core_betting.data_source_metadata → curated.data_sources
- Line 82: core_betting. → curated.
- Line 83: core_betting.data_migrations → operational.schema_migrations
- Line 83: core_betting. → curated.
- Line 86: core_betting.games → curated.games_complete
- Line 86: core_betting. → curated.
- Line 87: core_betting.teams → curated.teams_master
- Line 87: core_betting. → curated.
- Line 88: core_betting.sportsbooks → curated.sportsbooks
- Line 88: core_betting. → curated.
- Line 131: core_betting.sportsbooks → curated.sportsbooks
- Line 131: core_betting. → curated.
- Line 135: core_betting.sportsbooks → curated.sportsbooks
- Line 135: core_betting. → curated.
- Line 165: core_betting.teams → curated.teams_master
- Line 165: core_betting. → curated.
- Line 268: core_betting.games → curated.games_complete
- Line 268: core_betting. → curated.
- Line 335: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 335: core_betting. → curated.
- Line 361: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 361: core_betting. → curated.
- Line 387: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 387: core_betting. → curated.
- Line 416: core_betting.data_migrations → operational.schema_migrations
- Line 416: core_betting. → curated.
- Line 420: core_betting.supplementary_games → curated.games_complete
- Line 420: core_betting. → curated.
- Line 448: core_betting.sportsbooks → curated.sportsbooks
- Line 448: core_betting. → curated.
- Line 450: core_betting.games → curated.games_complete
- Line 450: core_betting. → curated.
- Line 452: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 452: core_betting. → curated.
- Line 454: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 454: core_betting. → curated.
- Line 456: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 456: core_betting. → curated.
- Line 458: core_betting.data_migrations → operational.schema_migrations
- Line 458: core_betting. → curated.
- Line 460: core_betting.supplementary_games → curated.games_complete
- Line 460: core_betting. → curated.

#### sql/migrations/001_remove_calculated_columns_safe.sql

**Direct Mappings:**
- Line 16: core_betting. → curated.
- Line 17: core_betting. → curated.
- Line 23: core_betting. → curated.
- Line 24: core_betting. → curated.
- Line 25: core_betting. → curated.
- Line 26: core_betting. → curated.
- Line 27: core_betting. → curated.
- Line 34: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 34: core_betting. → curated.
- Line 49: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 49: core_betting. → curated.
- Line 66: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 66: core_betting. → curated.
- Line 84: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 84: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 84: core_betting. → curated.
- Line 104: core_betting. → curated.
- Line 117: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 117: core_betting. → curated.
- Line 133: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 133: core_betting. → curated.
- Line 149: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 149: core_betting. → curated.
- Line 152: core_betting. → curated.
- Line 158: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 158: core_betting. → curated.
- Line 165: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 165: core_betting. → curated.
- Line 172: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 172: core_betting. → curated.

#### sql/improvements/02_data_validation_and_completeness.sql

**Direct Mappings:**
- Line 6: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 6: core_betting. → curated.
- Line 9: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 9: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 9: core_betting. → curated.
- Line 12: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 12: core_betting. → curated.
- Line 16: core_betting. → curated.
- Line 25: core_betting. → curated.
- Line 124: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 124: core_betting. → curated.
- Line 126: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 126: core_betting. → curated.
- Line 127: core_betting. → curated.
- Line 129: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 129: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 129: core_betting. → curated.
- Line 131: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 131: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 131: core_betting. → curated.
- Line 132: core_betting. → curated.
- Line 134: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 134: core_betting. → curated.
- Line 136: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 136: core_betting. → curated.
- Line 137: core_betting. → curated.
- Line 140: core_betting. → curated.
- Line 153: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 153: core_betting. → curated.
- Line 167: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 167: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 167: core_betting. → curated.
- Line 181: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 181: core_betting. → curated.
- Line 184: core_betting. → curated.
- Line 195: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 195: core_betting. → curated.
- Line 199: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 199: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 199: core_betting. → curated.
- Line 203: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 203: core_betting. → curated.
- Line 210: core_betting. → curated.
- Line 222: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 222: core_betting. → curated.
- Line 225: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 225: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 225: core_betting. → curated.
- Line 228: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 228: core_betting. → curated.
- Line 235: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 235: core_betting. → curated.
- Line 238: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 238: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 238: core_betting. → curated.
- Line 241: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 241: core_betting. → curated.
- Line 245: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 245: core_betting. → curated.
- Line 248: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 248: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 248: core_betting. → curated.
- Line 251: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 251: core_betting. → curated.
- Line 255: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 255: core_betting. → curated.
- Line 256: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 256: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 256: core_betting. → curated.
- Line 257: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 257: core_betting. → curated.

#### sql/improvements/01_sportsbook_mapping_system.sql

**Direct Mappings:**
- Line 6: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 6: core_betting. → curated.
- Line 8: core_betting.sportsbooks → curated.sportsbooks
- Line 8: core_betting. → curated.
- Line 19: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 19: core_betting. → curated.
- Line 22: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 22: core_betting. → curated.
- Line 29: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 29: core_betting. → curated.
- Line 47: core_betting.sportsbooks → curated.sportsbooks
- Line 47: core_betting. → curated.
- Line 53: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 53: core_betting. → curated.
- Line 60: core_betting.sportsbooks → curated.sportsbooks
- Line 60: core_betting. → curated.
- Line 64: core_betting. → curated.
- Line 74: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 74: core_betting. → curated.
- Line 81: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 81: core_betting. → curated.
- Line 89: core_betting.sportsbooks → curated.sportsbooks
- Line 89: core_betting. → curated.
- Line 98: core_betting. → curated.
- Line 107: core_betting. → curated.
- Line 116: core_betting. → curated.
- Line 130: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 130: core_betting. → curated.
- Line 132: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 132: core_betting. → curated.
- Line 133: core_betting. → curated.
- Line 135: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 135: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 135: core_betting. → curated.
- Line 137: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 137: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 137: core_betting. → curated.
- Line 138: core_betting. → curated.
- Line 140: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 140: core_betting. → curated.
- Line 142: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 142: core_betting. → curated.
- Line 143: core_betting. → curated.
- Line 146: core_betting. → curated.
- Line 154: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 154: core_betting. → curated.
- Line 163: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 163: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 163: core_betting. → curated.
- Line 172: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 172: core_betting. → curated.
- Line 175: core_betting. → curated.
- Line 183: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 183: core_betting. → curated.
- Line 185: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 185: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 185: core_betting. → curated.
- Line 187: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 187: core_betting. → curated.
- Line 194: core_betting. → curated.
- Line 203: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 203: core_betting. → curated.
- Line 204: core_betting. → curated.

#### sql/schemas/line_movement_history.sql

**Direct Mappings:**
- Line 5: core_betting. → curated.
- Line 9: core_betting.games → curated.games_complete
- Line 9: core_betting. → curated.
- Line 10: core_betting.sportsbooks → curated.sportsbooks
- Line 10: core_betting. → curated.
- Line 48: core_betting. → curated.
- Line 51: core_betting. → curated.
- Line 54: core_betting. → curated.
- Line 57: core_betting. → curated.
- Line 60: core_betting. → curated.
- Line 71: core_betting. → curated.
- Line 75: core_betting. → curated.
- Line 86: core_betting. → curated.
- Line 90: core_betting. → curated.
- Line 108: core_betting. → curated.
- Line 109: core_betting.sportsbooks → curated.sportsbooks
- Line 109: core_betting. → curated.
- Line 110: core_betting. → curated.
- Line 116: core_betting. → curated.

#### sql/schemas/analysis_reports.sql

**Direct Mappings:**
- Line 42: core_betting.sportsbooks → curated.sportsbooks
- Line 42: core_betting. → curated.
- Line 103: core_betting.sportsbooks → curated.sportsbooks
- Line 103: core_betting. → curated.
- Line 104: core_betting.sportsbooks → curated.sportsbooks
- Line 104: core_betting. → curated.

#### sql/views/unified_betting_lines.sql

**Direct Mappings:**
- Line 48: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 48: core_betting. → curated.
- Line 84: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 84: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 84: core_betting. → curated.
- Line 120: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 120: core_betting. → curated.


## Special Considerations

### Betting Lines Consolidation

The three betting lines tables will be consolidated into a single `curated.betting_lines_unified` table:

```sql
-- OLD: Separate tables
SELECT * FROM core_betting.betting_lines_moneyline WHERE game_id = 123;
SELECT * FROM core_betting.betting_lines_spreads WHERE game_id = 123;
SELECT * FROM core_betting.betting_lines_totals WHERE game_id = 123;

-- NEW: Unified table with market_type
SELECT * FROM curated.betting_lines_unified 
WHERE game_id = 123 AND market_type IN ('moneyline', 'spread', 'totals');
```

### Manual Review Required

The following patterns require manual review and adjustment:

1. **Complex JOIN queries** involving multiple betting lines tables
2. **Stored procedures** referencing core_betting schema
3. **Configuration files** with schema-specific settings
4. **SQL migrations** that reference old schema

## Next Steps

1. Review this report carefully
2. Execute backup creation
3. Run refactoring tool with --execute flag
4. Test all functionality thoroughly
5. Update any remaining manual patterns
