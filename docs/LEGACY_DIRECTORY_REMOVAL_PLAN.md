# Legacy Directory Removal Plan: `/src/mlb_sharp_betting/`

## Executive Summary

Comprehensive migration plan to safely remove the `/src/mlb_sharp_betting/` directory while preserving all functionality in the modern unified architecture.

## Analysis Results

### Reference Categories Identified

1. **External Module References (2 files)**
   - Strategy factory module paths
   - Action network utility imports

2. **Documentation/Comments (15+ files)**
   - Legacy source annotations
   - Migration documentation
   - Compatibility aliases

3. **Internal Self-References (70+ files)**
   - All files within `/src/mlb_sharp_betting/` importing from themselves
   - CLI entry points and module initialization

## Migration Strategy by Reference Type

### 1. External Module References

#### A. Strategy Factory Module Paths
**File**: `src/analysis/strategies/factory.py`
**Impact**: Critical - Strategy system won't load legacy processors
**Migration**: Update module paths to point to modern equivalents

```python
# BEFORE (Legacy modules):
"module": "src.mlb_sharp_betting.analysis.processors.sharpaction_processor"
"module": "src.mlb_sharp_betting.analysis.processors.timingbased_processor"
"module": "src.mlb_sharp_betting.analysis.processors.consensus_processor"
# ... 8 more legacy modules

# AFTER (Modern equivalents):
"module": "src.analysis.processors.sharp_action_processor"
"module": "src.analysis.processors.timing_based_processor"  
"module": "src.analysis.processors.consensus_processor"
# ... Use existing unified processors
```

#### B. Action Network Configuration Import
**File**: `src/analysis/processors/action/utils/actionnetwork_url_builder.py:76`
**Impact**: Medium - Configuration access for Action Network utilities
**Migration**: Update to use unified config system

```python
# BEFORE:
from src.mlb_sharp_betting.core.config import get_settings

# AFTER:
from src.core.config import get_settings
```

### 2. Documentation References

#### Strategy: Update to Modern Equivalents
- Update all "Legacy Source:" annotations
- Remove compatibility aliases in `src.core.exceptions.py:498`
- Update documentation strings referencing old architecture

### 3. Internal Self-References

#### Strategy: Complete Directory Removal
All files within `/src/mlb_sharp_betting/` will be removed, so internal imports become irrelevant.

## Implementation Plan

### Phase 1: Validate Modern Equivalents (CRITICAL)
**Duration**: 1-2 days
**Risk**: HIGH

1. **Verify Strategy Processor Equivalents**
   ```bash
   # Check each legacy processor has modern equivalent
   ls src/analysis/processors/*_processor.py
   ```

2. **Validate Configuration System**
   ```bash
   # Test unified config works for all use cases
   uv run python -c "from src.core.config import get_settings; print(get_settings())"
   ```

3. **Test Modern CLI Functionality**
   ```bash
   # Ensure modern CLI covers all legacy CLI functionality
   uv run -m src.interfaces.cli.main --help
   ```

### Phase 2: Update External References
**Duration**: 1 day
**Risk**: MEDIUM

1. **Update Strategy Factory Module Paths**
   - Update 10 module references in `factory.py`
   - Map legacy processors to modern equivalents
   - Test strategy loading: `uv run pytest tests/test_strategy_factory.py`

2. **Fix Action Network Config Import**
   - Update import in `actionnetwork_url_builder.py`
   - Test Action Network functionality
   - Verify database connectivity

3. **Update Documentation References**
   - Remove legacy source annotations
   - Update migration documentation
   - Clean up compatibility aliases

### Phase 3: Validation & Testing
**Duration**: 1 day
**Risk**: MEDIUM

1. **Run Comprehensive Tests**
   ```bash
   uv run pytest tests/ -v
   uv run pytest tests/integration/ -v
   uv run pytest tests/manual/ -v
   ```

2. **Test Core Workflows**
   ```bash
   # Test main service workflows
   uv run -m src.interfaces.cli.main data status
   uv run -m src.interfaces.cli.main action-network pipeline --dry-run
   uv run -m src.interfaces.cli.main movement analyze --date today
   ```

3. **Validate Database Operations**
   ```bash
   # Test database connectivity and operations
   uv run -m src.interfaces.cli.main database status
   ```

### Phase 4: Execute Removal
**Duration**: 0.5 days
**Risk**: LOW (after validation)

1. **Remove Legacy Directory**
   ```bash
   # After all validation passes
   rm -rf src/mlb_sharp_betting/
   ```

2. **Update pyproject.toml**
   ```toml
   # Remove legacy package reference
   # BEFORE:
   packages = ["src/mlb_sharp_betting"]
   # AFTER:
   packages = ["src"]
   ```

3. **Update .gitignore if needed**
   ```
   # Remove any mlb_sharp_betting specific ignores
   ```

### Phase 5: Final Validation
**Duration**: 0.5 days
**Risk**: LOW

1. **Clean Installation Test**
   ```bash
   # Test from fresh environment
   uv sync --dev
   uv run pytest tests/
   ```

2. **Integration Testing**
   ```bash
   # Test complete workflows
   uv run -m src.interfaces.cli.main data collect --dry-run
   uv run -m src.interfaces.cli.main outcomes update
   ```

## Risk Assessment & Mitigation

### High Risk Items

1. **Strategy Processor Mapping**
   - **Risk**: Legacy processors may have unique functionality
   - **Mitigation**: Map each legacy processor to modern equivalent before removal
   - **Validation**: Test all strategy categories load successfully

2. **Configuration System Dependencies**
   - **Risk**: Different configuration schemas between legacy/modern
   - **Mitigation**: Test configuration access in Action Network utilities
   - **Validation**: Verify database connections work with modern config

### Medium Risk Items

1. **CLI Command Coverage**
   - **Risk**: Legacy CLI commands not replicated in modern interface
   - **Mitigation**: Document CLI command mapping before removal
   - **Validation**: Test critical workflows with modern CLI

2. **Database Compatibility**
   - **Risk**: Legacy database access patterns may differ
   - **Mitigation**: Use unified database layer consistently
   - **Validation**: Test all CRUD operations

## Success Criteria

1. ✅ All tests pass after removal
2. ✅ Main service workflows function correctly
3. ✅ Strategy processors load and execute
4. ✅ Database operations work properly
5. ✅ Configuration system provides all needed settings
6. ✅ No broken imports or module references

## Rollback Plan

If issues arise during removal:

1. **Immediate Rollback**: Restore from git
   ```bash
   git checkout HEAD~1 src/mlb_sharp_betting/
   ```

2. **Partial Rollback**: Restore specific components
   ```bash
   git checkout HEAD~1 src/mlb_sharp_betting/services/specific_service.py
   ```

3. **Bridge Mode**: Temporarily update references to keep legacy alive
   - Update external references to keep legacy functional
   - Delay removal until issues resolved

## Dependencies & Prerequisites

- [ ] Modern strategy processors fully implement legacy functionality
- [ ] Unified configuration system supports all use cases
- [ ] Modern CLI interface covers all critical operations
- [ ] Database layer compatibility validated
- [ ] Integration tests pass consistently

## Estimated Timeline

- **Total Duration**: 3-4 days
- **Critical Path**: Strategy processor validation and mapping
- **Parallel Work**: Documentation updates can happen simultaneously

## Communication Plan

1. **Before Removal**: Announce planned removal timeline
2. **During Migration**: Status updates on validation progress
3. **After Removal**: Confirm successful migration and new architecture