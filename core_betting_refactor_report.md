# Core Betting Schema Refactoring Report

**Generated:** 2025-07-24T22:24:13.558947
**Files to Process:** 84

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

**Betting Lines Consolidation (Manual Review):**
- Line 525: Betting lines consolidation needed
- Line 666: Betting lines consolidation needed
- Line 794: Betting lines consolidation needed
- Line 913: Betting lines consolidation needed
- Line 929: Betting lines consolidation needed
- Line 945: Betting lines consolidation needed
- Line 969: Betting lines consolidation needed
- Line 972: Betting lines consolidation needed
- Line 975: Betting lines consolidation needed

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

**Betting Lines Consolidation (Manual Review):**
- Line 2245: Betting lines consolidation needed
- Line 2299: Betting lines consolidation needed

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

**Betting Lines Consolidation (Manual Review):**
- Line 102: Betting lines consolidation needed
- Line 131: Betting lines consolidation needed
- Line 240: Betting lines consolidation needed
- Line 250: Betting lines consolidation needed
- Line 424: Betting lines consolidation needed

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

**Betting Lines Consolidation (Manual Review):**
- Line 146: Betting lines consolidation needed
- Line 269: Betting lines consolidation needed
- Line 360: Betting lines consolidation needed
- Line 474: Betting lines consolidation needed
- Line 604: Betting lines consolidation needed
- Line 870: Betting lines consolidation needed

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

**Betting Lines Consolidation (Manual Review):**
- Line 417: Betting lines consolidation needed
- Line 428: Betting lines consolidation needed
- Line 439: Betting lines consolidation needed
- Line 474: Betting lines consolidation needed

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

**Betting Lines Consolidation (Manual Review):**
- Line 893: Betting lines consolidation needed
- Line 900: Betting lines consolidation needed

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

**Betting Lines Consolidation (Manual Review):**
- Line 23: Betting lines consolidation needed
- Line 85: Betting lines consolidation needed
- Line 95: Betting lines consolidation needed
- Line 336: Betting lines consolidation needed
- Line 337: Betting lines consolidation needed
- Line 338: Betting lines consolidation needed
- Line 366: Betting lines consolidation needed
- Line 367: Betting lines consolidation needed
- Line 372: Betting lines consolidation needed
- Line 373: Betting lines consolidation needed
- Line 378: Betting lines consolidation needed
- Line 379: Betting lines consolidation needed

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

**Betting Lines Consolidation (Manual Review):**
- Line 279: Betting lines consolidation needed
- Line 310: Betting lines consolidation needed
- Line 345: Betting lines consolidation needed

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

**Betting Lines Consolidation (Manual Review):**
- Line 129: Betting lines consolidation needed
- Line 130: Betting lines consolidation needed
- Line 131: Betting lines consolidation needed
- Line 177: Betting lines consolidation needed
- Line 182: Betting lines consolidation needed
- Line 187: Betting lines consolidation needed

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

**Betting Lines Consolidation (Manual Review):**
- Line 233: Betting lines consolidation needed
- Line 234: Betting lines consolidation needed
- Line 235: Betting lines consolidation needed
- Line 257: Betting lines consolidation needed
- Line 262: Betting lines consolidation needed
- Line 267: Betting lines consolidation needed

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

**Betting Lines Consolidation (Manual Review):**
- Line 249: Betting lines consolidation needed
- Line 250: Betting lines consolidation needed
- Line 251: Betting lines consolidation needed
- Line 278: Betting lines consolidation needed
- Line 283: Betting lines consolidation needed
- Line 288: Betting lines consolidation needed

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

**Betting Lines Consolidation (Manual Review):**
- Line 7: Betting lines consolidation needed
- Line 8: Betting lines consolidation needed
- Line 9: Betting lines consolidation needed
- Line 38: Betting lines consolidation needed
- Line 43: Betting lines consolidation needed
- Line 48: Betting lines consolidation needed

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

**Manual Review Required:**
- Line 137: Complex SQL pattern requires manual review
- Line 185: Complex SQL pattern requires manual review
- Line 187: Complex SQL pattern requires manual review
- Line 211: Complex SQL pattern requires manual review
- Line 230: Complex SQL pattern requires manual review
- Line 241: Complex SQL pattern requires manual review
- Line 243: Complex SQL pattern requires manual review
- Line 244: Complex SQL pattern requires manual review
- Line 283: Complex SQL pattern requires manual review
- Line 285: Complex SQL pattern requires manual review
- Line 286: Complex SQL pattern requires manual review
- Line 333: Complex SQL pattern requires manual review
- Line 335: Complex SQL pattern requires manual review
- Line 336: Complex SQL pattern requires manual review
- Line 379: Complex SQL pattern requires manual review
- Line 381: Complex SQL pattern requires manual review
- Line 393: Complex SQL pattern requires manual review
- Line 395: Complex SQL pattern requires manual review
- Line 405: Complex SQL pattern requires manual review
- Line 407: Complex SQL pattern requires manual review
- Line 432: Complex SQL pattern requires manual review
- Line 434: Complex SQL pattern requires manual review
- Line 463: Complex SQL pattern requires manual review
- Line 478: Complex SQL pattern requires manual review
- Line 531: Complex SQL pattern requires manual review
- Line 542: Complex SQL pattern requires manual review

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

**Manual Review Required:**
- Line 65: Complex SQL pattern requires manual review

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

**Manual Review Required:**
- Line 341: Complex SQL pattern requires manual review

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

**Manual Review Required:**
- Line 61: Complex SQL pattern requires manual review
- Line 81: Complex SQL pattern requires manual review
- Line 99: Complex SQL pattern requires manual review
- Line 122: Complex SQL pattern requires manual review
- Line 122: Complex SQL pattern requires manual review
- Line 126: Complex SQL pattern requires manual review

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

**Betting Lines Consolidation (Manual Review):**
- Line 149: Betting lines consolidation needed
- Line 203: Betting lines consolidation needed
- Line 213: Betting lines consolidation needed
- Line 226: Betting lines consolidation needed
- Line 296: Betting lines consolidation needed
- Line 306: Betting lines consolidation needed
- Line 319: Betting lines consolidation needed
- Line 381: Betting lines consolidation needed
- Line 391: Betting lines consolidation needed
- Line 616: Betting lines consolidation needed
- Line 617: Betting lines consolidation needed
- Line 618: Betting lines consolidation needed
- Line 646: Betting lines consolidation needed
- Line 647: Betting lines consolidation needed
- Line 652: Betting lines consolidation needed
- Line 653: Betting lines consolidation needed
- Line 658: Betting lines consolidation needed
- Line 659: Betting lines consolidation needed

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

**Manual Review Required:**
- Line 341: Complex SQL pattern requires manual review

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

**Betting Lines Consolidation (Manual Review):**
- Line 49: Betting lines consolidation needed
- Line 59: Betting lines consolidation needed
- Line 69: Betting lines consolidation needed
- Line 220: Betting lines consolidation needed
- Line 230: Betting lines consolidation needed
- Line 240: Betting lines consolidation needed
- Line 252: Betting lines consolidation needed

**Manual Review Required:**
- Line 9: Complex SQL pattern requires manual review
- Line 15: Complex SQL pattern requires manual review
- Line 21: Complex SQL pattern requires manual review
- Line 27: Complex SQL pattern requires manual review
- Line 33: Complex SQL pattern requires manual review
- Line 39: Complex SQL pattern requires manual review

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

**Betting Lines Consolidation (Manual Review):**
- Line 53: Betting lines consolidation needed
- Line 57: Betting lines consolidation needed
- Line 61: Betting lines consolidation needed

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

**Betting Lines Consolidation (Manual Review):**
- Line 228: Betting lines consolidation needed
- Line 236: Betting lines consolidation needed
- Line 244: Betting lines consolidation needed

**Manual Review Required:**
- Line 15: Complex SQL pattern requires manual review

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

**Betting Lines Consolidation (Manual Review):**
- Line 13: Betting lines consolidation needed
- Line 16: Betting lines consolidation needed
- Line 19: Betting lines consolidation needed

**Manual Review Required:**
- Line 12: Complex SQL pattern requires manual review
- Line 15: Complex SQL pattern requires manual review
- Line 18: Complex SQL pattern requires manual review
- Line 25: Complex SQL pattern requires manual review
- Line 40: Complex SQL pattern requires manual review
- Line 57: Complex SQL pattern requires manual review
- Line 75: Complex SQL pattern requires manual review

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

**Betting Lines Consolidation (Manual Review):**
- Line 135: Betting lines consolidation needed
- Line 151: Betting lines consolidation needed

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

**Betting Lines Consolidation (Manual Review):**
- Line 36: Betting lines consolidation needed
- Line 37: Betting lines consolidation needed
- Line 38: Betting lines consolidation needed
- Line 373: Betting lines consolidation needed
- Line 410: Betting lines consolidation needed
- Line 448: Betting lines consolidation needed
- Line 523: Betting lines consolidation needed
- Line 524: Betting lines consolidation needed
- Line 525: Betting lines consolidation needed

**Manual Review Required:**
- Line 33: Complex SQL pattern requires manual review
- Line 34: Complex SQL pattern requires manual review
- Line 35: Complex SQL pattern requires manual review
- Line 36: Complex SQL pattern requires manual review
- Line 37: Complex SQL pattern requires manual review
- Line 38: Complex SQL pattern requires manual review
- Line 39: Complex SQL pattern requires manual review
- Line 40: Complex SQL pattern requires manual review
- Line 41: Complex SQL pattern requires manual review
- Line 42: Complex SQL pattern requires manual review
- Line 45: Complex SQL pattern requires manual review
- Line 46: Complex SQL pattern requires manual review
- Line 47: Complex SQL pattern requires manual review

#### sql/migrations/011_create_ml_curated_zone.sql

**Direct Mappings:**
- Line 107: core_betting.sportsbooks → curated.sportsbooks
- Line 107: core_betting. → curated.

**Manual Review Required:**
- Line 107: Complex SQL pattern requires manual review

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

**Betting Lines Consolidation (Manual Review):**
- Line 244: Betting lines consolidation needed
- Line 255: Betting lines consolidation needed
- Line 266: Betting lines consolidation needed
- Line 322: Betting lines consolidation needed
- Line 325: Betting lines consolidation needed
- Line 328: Betting lines consolidation needed
- Line 343: Betting lines consolidation needed
- Line 351: Betting lines consolidation needed
- Line 359: Betting lines consolidation needed
- Line 371: Betting lines consolidation needed
- Line 375: Betting lines consolidation needed
- Line 379: Betting lines consolidation needed

**Manual Review Required:**
- Line 15: Complex SQL pattern requires manual review
- Line 24: Complex SQL pattern requires manual review
- Line 33: Complex SQL pattern requires manual review
- Line 42: Complex SQL pattern requires manual review
- Line 105: Complex SQL pattern requires manual review
- Line 108: Complex SQL pattern requires manual review
- Line 120: Complex SQL pattern requires manual review
- Line 123: Complex SQL pattern requires manual review
- Line 134: Complex SQL pattern requires manual review
- Line 137: Complex SQL pattern requires manual review
- Line 149: Complex SQL pattern requires manual review
- Line 152: Complex SQL pattern requires manual review
- Line 163: Complex SQL pattern requires manual review
- Line 166: Complex SQL pattern requires manual review
- Line 178: Complex SQL pattern requires manual review
- Line 181: Complex SQL pattern requires manual review

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

**Betting Lines Consolidation (Manual Review):**
- Line 29: Betting lines consolidation needed
- Line 30: Betting lines consolidation needed
- Line 31: Betting lines consolidation needed
- Line 77: Betting lines consolidation needed
- Line 78: Betting lines consolidation needed
- Line 79: Betting lines consolidation needed
- Line 335: Betting lines consolidation needed
- Line 361: Betting lines consolidation needed
- Line 387: Betting lines consolidation needed

**Manual Review Required:**
- Line 74: Complex SQL pattern requires manual review
- Line 75: Complex SQL pattern requires manual review
- Line 76: Complex SQL pattern requires manual review
- Line 77: Complex SQL pattern requires manual review
- Line 78: Complex SQL pattern requires manual review
- Line 79: Complex SQL pattern requires manual review
- Line 80: Complex SQL pattern requires manual review
- Line 81: Complex SQL pattern requires manual review
- Line 82: Complex SQL pattern requires manual review
- Line 83: Complex SQL pattern requires manual review
- Line 86: Complex SQL pattern requires manual review
- Line 87: Complex SQL pattern requires manual review
- Line 88: Complex SQL pattern requires manual review

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

**Betting Lines Consolidation (Manual Review):**
- Line 117: Betting lines consolidation needed
- Line 133: Betting lines consolidation needed
- Line 149: Betting lines consolidation needed
- Line 158: Betting lines consolidation needed
- Line 165: Betting lines consolidation needed
- Line 172: Betting lines consolidation needed

**Manual Review Required:**
- Line 34: Complex SQL pattern requires manual review
- Line 49: Complex SQL pattern requires manual review
- Line 66: Complex SQL pattern requires manual review
- Line 84: Complex SQL pattern requires manual review

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

**Betting Lines Consolidation (Manual Review):**
- Line 153: Betting lines consolidation needed
- Line 167: Betting lines consolidation needed
- Line 181: Betting lines consolidation needed
- Line 195: Betting lines consolidation needed
- Line 199: Betting lines consolidation needed
- Line 203: Betting lines consolidation needed
- Line 222: Betting lines consolidation needed
- Line 225: Betting lines consolidation needed
- Line 228: Betting lines consolidation needed
- Line 255: Betting lines consolidation needed
- Line 256: Betting lines consolidation needed
- Line 257: Betting lines consolidation needed

**Manual Review Required:**
- Line 6: Complex SQL pattern requires manual review
- Line 9: Complex SQL pattern requires manual review
- Line 12: Complex SQL pattern requires manual review

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

**Betting Lines Consolidation (Manual Review):**
- Line 154: Betting lines consolidation needed
- Line 163: Betting lines consolidation needed
- Line 172: Betting lines consolidation needed
- Line 183: Betting lines consolidation needed
- Line 185: Betting lines consolidation needed
- Line 187: Betting lines consolidation needed

**Manual Review Required:**
- Line 6: Complex SQL pattern requires manual review
- Line 8: Complex SQL pattern requires manual review

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

**Manual Review Required:**
- Line 5: Complex SQL pattern requires manual review
- Line 9: Complex SQL pattern requires manual review
- Line 10: Complex SQL pattern requires manual review

#### sql/schemas/analysis_reports.sql

**Direct Mappings:**
- Line 42: core_betting.sportsbooks → curated.sportsbooks
- Line 42: core_betting. → curated.
- Line 103: core_betting.sportsbooks → curated.sportsbooks
- Line 103: core_betting. → curated.
- Line 104: core_betting.sportsbooks → curated.sportsbooks
- Line 104: core_betting. → curated.

**Manual Review Required:**
- Line 42: Complex SQL pattern requires manual review
- Line 103: Complex SQL pattern requires manual review
- Line 104: Complex SQL pattern requires manual review

#### sql/views/unified_betting_lines.sql

**Direct Mappings:**
- Line 48: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 48: core_betting. → curated.
- Line 84: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 84: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 84: core_betting. → curated.
- Line 120: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 120: core_betting. → curated.

**Betting Lines Consolidation (Manual Review):**
- Line 48: Betting lines consolidation needed
- Line 84: Betting lines consolidation needed
- Line 120: Betting lines consolidation needed

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

**Betting Lines Consolidation (Manual Review):**
- Line 492: Betting lines consolidation needed
- Line 506: Betting lines consolidation needed
- Line 636: Betting lines consolidation needed
- Line 650: Betting lines consolidation needed
- Line 781: Betting lines consolidation needed
- Line 795: Betting lines consolidation needed
- Line 844: Betting lines consolidation needed

#### utilities/test_game_outcomes.py

**Direct Mappings:**
- Line 72: core_betting.game_outcomes → curated.game_outcomes
- Line 72: core_betting. → curated.

#### utilities/core_betting_migration/validation_and_rollback.py

**Direct Mappings:**
- Line 197: core_betting. → curated.
- Line 215: core_betting. → curated.
- Line 251: core_betting. → curated.
- Line 337: core_betting. → curated.
- Line 344: core_betting. → curated.
- Line 345: core_betting. → curated.

**Manual Review Required:**
- Line 369: Complex SQL pattern requires manual review
- Line 371: Complex SQL pattern requires manual review
- Line 392: Complex SQL pattern requires manual review

#### utilities/core_betting_migration/data_migration_scripts.sql

**Direct Mappings:**
- Line 45: core_betting. → curated.
- Line 74: core_betting.games → curated.games_complete
- Line 74: core_betting. → curated.
- Line 154: core_betting.game_outcomes → curated.game_outcomes
- Line 154: core_betting. → curated.
- Line 201: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 201: core_betting. → curated.
- Line 223: core_betting.data_source_metadata → curated.data_sources
- Line 223: core_betting. → curated.
- Line 247: core_betting.teams → curated.teams_master
- Line 247: core_betting. → curated.
- Line 321: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 321: core_betting. → curated.
- Line 353: core_betting.data_source_metadata → curated.data_sources
- Line 353: core_betting. → curated.
- Line 388: core_betting.teams → curated.teams_master
- Line 388: core_betting. → curated.
- Line 421: core_betting.data_migrations → operational.schema_migrations
- Line 421: core_betting. → curated.
- Line 422: core_betting.data_migrations → operational.schema_migrations
- Line 422: core_betting. → curated.
- Line 437: core_betting.games → curated.games_complete
- Line 437: core_betting. → curated.
- Line 462: core_betting.games → curated.games_complete
- Line 462: core_betting. → curated.
- Line 478: core_betting.supplementary_games → curated.games_complete
- Line 478: core_betting. → curated.
- Line 516: core_betting.game_outcomes → curated.game_outcomes
- Line 516: core_betting. → curated.
- Line 517: core_betting.games → curated.games_complete
- Line 517: core_betting. → curated.
- Line 565: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 565: core_betting. → curated.
- Line 566: core_betting.games → curated.games_complete
- Line 566: core_betting. → curated.
- Line 595: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 595: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 595: core_betting. → curated.
- Line 596: core_betting.games → curated.games_complete
- Line 596: core_betting. → curated.
- Line 625: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 625: core_betting. → curated.
- Line 626: core_betting.games → curated.games_complete
- Line 626: core_betting. → curated.
- Line 647: core_betting.games → curated.games_complete
- Line 647: core_betting. → curated.
- Line 649: core_betting.game_outcomes → curated.game_outcomes
- Line 649: core_betting. → curated.
- Line 651: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 651: core_betting. → curated.
- Line 653: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 653: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 653: core_betting. → curated.
- Line 655: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 655: core_betting. → curated.
- Line 657: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 657: core_betting. → curated.
- Line 659: core_betting.teams → curated.teams_master
- Line 659: core_betting. → curated.
- Line 661: core_betting.data_source_metadata → curated.data_sources
- Line 661: core_betting. → curated.

**Betting Lines Consolidation (Manual Review):**
- Line 565: Betting lines consolidation needed
- Line 595: Betting lines consolidation needed
- Line 625: Betting lines consolidation needed
- Line 651: Betting lines consolidation needed
- Line 653: Betting lines consolidation needed
- Line 655: Betting lines consolidation needed

**Manual Review Required:**
- Line 18: Complex SQL pattern requires manual review
- Line 45: Complex SQL pattern requires manual review
- Line 56: Complex SQL pattern requires manual review
- Line 645: Complex SQL pattern requires manual review

#### utilities/core_betting_migration/automated_code_refactor.py

**Direct Mappings:**
- Line 36: core_betting.games → curated.games_complete
- Line 36: core_betting. → curated.
- Line 37: core_betting.game_outcomes → curated.game_outcomes
- Line 37: core_betting. → curated.
- Line 38: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 38: core_betting. → curated.
- Line 39: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 39: core_betting. → curated.
- Line 40: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 40: core_betting. → curated.
- Line 41: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 41: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 41: core_betting. → curated.
- Line 42: core_betting.sportsbooks → curated.sportsbooks
- Line 42: core_betting. → curated.
- Line 43: core_betting.teams → curated.teams_master
- Line 43: core_betting. → curated.
- Line 44: core_betting.sportsbook_external_mappings → curated.sportsbook_mappings
- Line 44: core_betting. → curated.
- Line 45: core_betting.data_source_metadata → curated.data_sources
- Line 45: core_betting. → curated.
- Line 46: core_betting.data_migrations → operational.schema_migrations
- Line 46: core_betting. → curated.
- Line 47: core_betting.supplementary_games → curated.games_complete
- Line 47: core_betting. → curated.
- Line 48: core_betting.betting_splits → curated.betting_splits
- Line 48: core_betting. → curated.
- Line 51: core_betting. → curated.

**Manual Review Required:**
- Line 177: Complex SQL pattern requires manual review
- Line 178: Complex SQL pattern requires manual review
- Line 179: Complex SQL pattern requires manual review
- Line 180: Complex SQL pattern requires manual review
- Line 181: Complex SQL pattern requires manual review

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

**Betting Lines Consolidation (Manual Review):**
- Line 252: Betting lines consolidation needed
- Line 253: Betting lines consolidation needed
- Line 254: Betting lines consolidation needed

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

**Betting Lines Consolidation (Manual Review):**
- Line 144: Betting lines consolidation needed
- Line 196: Betting lines consolidation needed
- Line 276: Betting lines consolidation needed
- Line 330: Betting lines consolidation needed
- Line 410: Betting lines consolidation needed
- Line 462: Betting lines consolidation needed

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

**Manual Review Required:**
- Line 177: Complex SQL pattern requires manual review
- Line 178: Complex SQL pattern requires manual review
- Line 179: Complex SQL pattern requires manual review
- Line 182: Complex SQL pattern requires manual review
- Line 183: Complex SQL pattern requires manual review
- Line 184: Complex SQL pattern requires manual review

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

**Betting Lines Consolidation (Manual Review):**
- Line 271: Betting lines consolidation needed

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

**Betting Lines Consolidation (Manual Review):**
- Line 164: Betting lines consolidation needed
- Line 173: Betting lines consolidation needed

**Manual Review Required:**
- Line 12: Complex SQL pattern requires manual review
- Line 59: Complex SQL pattern requires manual review
- Line 86: Complex SQL pattern requires manual review
- Line 88: Complex SQL pattern requires manual review
- Line 144: Complex SQL pattern requires manual review
- Line 147: Complex SQL pattern requires manual review
- Line 150: Complex SQL pattern requires manual review

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

**Manual Review Required:**
- Line 64: Complex SQL pattern requires manual review
- Line 66: Complex SQL pattern requires manual review

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

**Betting Lines Consolidation (Manual Review):**
- Line 183: Betting lines consolidation needed
- Line 195: Betting lines consolidation needed

#### docs/PIPELINE_IMPLEMENTATION_GUIDE.md

**Direct Mappings:**
- Line 147: core_betting. → curated.
- Line 184: core_betting. → curated.

#### docs/PIPELINE_IMPLEMENTATION_ROADMAP.md

**Direct Mappings:**
- Line 320: core_betting.games → curated.games_complete
- Line 320: core_betting. → curated.

**Manual Review Required:**
- Line 320: Complex SQL pattern requires manual review

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

**Betting Lines Consolidation (Manual Review):**
- Line 36: Betting lines consolidation needed
- Line 40: Betting lines consolidation needed
- Line 44: Betting lines consolidation needed

#### docs/migration/DATA_COLLECTION_TEST_RESULTS.md

**Direct Mappings:**
- Line 46: core_betting.betting_lines_moneyline → curated.betting_lines_unified
- Line 46: core_betting. → curated.
- Line 47: core_betting.betting_lines_spread → curated.betting_lines_unified
- Line 47: core_betting.betting_lines_spreads → curated.betting_lines_unified
- Line 47: core_betting. → curated.
- Line 48: core_betting.betting_lines_totals → curated.betting_lines_unified
- Line 48: core_betting. → curated.

#### tests/test_cli_pipeline.py

**Direct Mappings:**
- Line 360: core_betting. → curated.


## Summary Statistics

- **Total Files:** 76
- **Total Changes:** 2321
- **Estimated Time:** 380 minutes for automated changes
- **Manual Review Time:** 360 minutes
