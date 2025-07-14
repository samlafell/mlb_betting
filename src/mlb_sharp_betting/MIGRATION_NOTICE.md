# 🚨 MIGRATION NOTICE: Services Moved to Unified Architecture

## Critical Business Logic Migration Completed

The following services have been successfully migrated from this legacy folder to the unified architecture in `/src/services/`:

### Migrated Services:

1. **Game Manager Service**
   - **From:** `mlb_sharp_betting.services.game_manager`
   - **To:** `src.services.game.game_manager_service`
   - **Class:** `GameManagerService`

2. **Scheduler Engine Service**
   - **From:** `mlb_sharp_betting.services.scheduler_engine`
   - **To:** `src.services.scheduling.scheduler_engine_service`
   - **Class:** `SchedulerEngineService`

3. **Data Service**
   - **From:** `mlb_sharp_betting.services.data_service`
   - **To:** `src.services.data.enhanced_data_service`
   - **Class:** `EnhancedDataService`

4. **Pre-Game Workflow Service**
   - **From:** `mlb_sharp_betting.services.pre_game_workflow`
   - **To:** `src.services.workflow.pre_game_workflow_service`
   - **Class:** `PreGameWorkflowService`

5. **Strategy Manager Service**
   - **From:** `mlb_sharp_betting.services.strategy_manager`
   - **To:** `src.services.strategy.strategy_manager_service`
   - **Class:** `StrategyManagerService`

6. **Pipeline Orchestrator Service**
   - **From:** `mlb_sharp_betting.services.pipeline_orchestrator`
   - **To:** `src.services.orchestration.pipeline_orchestration_service`
   - **Class:** `PipelineOrchestrationService`

## Enhanced Features

All migrated services include:
- ✅ **Async-first design** with proper error handling
- ✅ **Comprehensive monitoring** and metrics tracking
- ✅ **Enhanced configuration** with dataclasses and validation
- ✅ **Unified architecture integration** with consistent patterns
- ✅ **Type safety** with extensive type hints and enums
- ✅ **Better error handling** with structured exceptions

## Migration Status

- **Phase 5D Complete:** All critical business logic has been migrated
- **Legacy Folder Status:** Scheduled for deprecation
- **New CLI Interface:** Available at `src.interfaces.cli.main`
- **Unified Configuration:** Centralized in `src.core.config`

## Action Required

**DO NOT** add new features to this legacy folder. All new development should use the unified architecture in `/src/`.

For any questions about the migration, refer to the comprehensive services in:
- `/src/services/game/`
- `/src/services/scheduling/`
- `/src/services/data/`
- `/src/services/workflow/`
- `/src/services/strategy/`
- `/src/services/orchestration/`

---
**Migration Completed:** Phase 5D - Critical Business Logic Migration
**Date:** January 2025
**Status:** ✅ Complete 