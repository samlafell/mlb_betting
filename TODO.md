# TO_DO.md - Documentation Issues and Fixes

This file documents commands and workflows in the project documentation that are not working properly, along with hypotheses about their problems and where to look to solve them.

## Postgres
- Postgres uses Port 5433 with password postgres

## ❌ Non-Working Commands and Issues

### 1. Missing `action-network collect` Command
**Documentation**: README.md lines 72, 298  
**Failed Command**: `uv run -m src.interfaces.cli action-network collect --date today`
**Error**: `Error: No such command 'collect'.`

**Hypothesis**: The Action Network CLI module only has `pipeline`, `history`, and `opportunities` commands. The documentation references a non-existent `collect` command.

**Where to Fix**: 
- Update README.md line 72 to use `action-network pipeline --date today` instead
- Update README.md line 298 to use `action-network pipeline --date today` instead  
- Or implement the missing `collect` command in `src/interfaces/cli/commands/action_network.py`

### 2. Missing Action Network History File for Movement Analysis
**Documentation**: README.md lines 33, 76, 246, 249, 252, 308, 313, 317, 380  
**Failed Command**: `uv run -m src.interfaces.cli movement analyze --input-file output/action_network_history.json`
**Error**: `❌ File not found: output/action_network_history.json`

**Hypothesis**: The movement analysis commands assume a history file exists, but the `action-network history` command may not be generating the expected output file or placing it in the expected location.

**Where to Fix**:
- Check `src/interfaces/cli/commands/action_network.py` `history` command implementation
- Verify the output file path and format for the history command
- Update documentation to include the prerequisite step of generating the history file first
- Or modify movement analysis to work with database data directly

### 3. Database Setup Command Fails - Missing CURATED Table
**Documentation**: README.md lines 42, 65, 282, 363, 514; USER_GUIDE.md line 11  
**Failed Command**: `uv run -m src.interfaces.cli database setup-action-network --test-connection`
**Error**: `❌ Database setup validation failed: relation "curated.betting_lines_unified" does not exist`

**Hypothesis**: The database setup command expects CURATED zone tables that haven't been created or migrated properly. This suggests schema inconsistencies between expected and actual database structure.

**Where to Fix**:
- Check `src/interfaces/cli/commands/database.py` for the setup command validation logic
- Review `sql/migrations/` for missing CURATED zone table migrations
- Verify `curated.betting_lines_unified` table creation in database schema
- Update setup command to handle missing tables gracefully or create them

### 4. ML Database Setup Times Out
**Documentation**: ML_GETTING_STARTED.md line 43  
**Failed Command**: `uv run docker/scripts/setup_ml_database.py`
**Error**: `Command timed out after 2m 0.0s`

**Hypothesis**: The ML database setup script may be hanging on database connections, long-running queries, or missing dependencies.

**Where to Fix**:
- Check `docker/scripts/setup_ml_database.py` for infinite loops or blocking operations
- Verify database connection parameters in the script
- Add timeout handling and error reporting to the script
- Check if PostgreSQL is properly configured for the script's connection attempts

### 5. Schema Column Mismatch Errors in Pipeline Assessment
**Documentation**: USER_GUIDE.md line 157  
**Command Works But Has Errors**: `uv run assess_pipeline_data.py`
**Errors**: 
- `❌ ml_model_dashboard: Error - column "created_at" does not exist`
- Various `column "collected_at" does not exist` errors
- `relation "raw_data.vsin_data" does not exist`

**Hypothesis**: Database schema evolution has created mismatches between what the assessment script expects and what actually exists in the database.

**Where to Fix**:
- Update `assess_pipeline_data.py` to handle missing columns and tables gracefully
- Review database migrations to ensure all expected tables and columns exist
- Fix schema inconsistencies in `raw_data.vsin_data` table creation
- Add proper error handling for missing database objects

### 6. Pipeline Backlog Issue
**Documentation**: USER_GUIDE.md discusses pipeline health  
**Issue**: Pipeline assessment shows `⚠️ Pipeline backlog detected: 1,259 unprocessed records`
**Status**: System working but suboptimal

**Hypothesis**: The RAW → STAGING pipeline processing is not keeping up with data collection, leaving unprocessed records in RAW tables.

**Where to Fix**:
- Check `src/data/pipeline/pipeline_orchestrator.py` for processing bottlenecks
- Review staging zone processor performance in `src/data/pipeline/staging_zone.py`
- Consider implementing background processing or scheduling for pipeline execution
- Monitor processing rates and identify optimization opportunities

## ✅ Working Commands Confirmed

### Core CLI Commands
- `uv run -m src.interfaces.cli --help` ✅
- `uv run -m src.interfaces.cli data status` ✅  
- `uv run -m src.interfaces.cli monitoring dashboard --help` ✅
- `uv run -m src.interfaces.cli action-network pipeline --date today` ✅ (works but takes time)
- `uv run -m src.interfaces.cli pipeline run --zone staging --dry-run` ✅
- `uv run assess_pipeline_data.py` ✅ (works with warnings)

### External Services  
- `curl http://localhost/health` ✅ (ML system health check)

## 📋 Recommendations for Documentation Updates

### 1. README.md Updates Needed
- **Lines 72, 298**: Change `action-network collect` to `action-network pipeline`
- **Lines 33, 76, 246, 249, 252, 308, 313, 317, 380**: Add prerequisite step to generate history file before movement analysis
- **Lines 42, 65, 282, 363, 514**: Add troubleshooting note about database setup requirements

### 2. USER_GUIDE.md Updates Needed
- **Line 11**: Add troubleshooting section for database setup failures
- **Line 157**: Add note about expected warnings from pipeline assessment
- Add section about pipeline backlog monitoring and resolution

### 3. ML_GETTING_STARTED.md Updates Needed
- **Line 43**: Add timeout handling note and alternative setup methods
- Add troubleshooting section for ML database setup issues

## 🔧 Implementation Priority

### High Priority (Breaks user workflows)
1. Fix missing `action-network collect` command documentation
2. Resolve database setup command validation failures  
3. Fix movement analysis file dependency

### Medium Priority (Performance issues)
4. Address pipeline backlog processing
5. Improve ML database setup reliability

### Low Priority (Cosmetic warnings)
6. Clean up schema mismatch errors in assessment script

## 📊 Testing Methodology

Commands were tested systematically from the following documentation files:
- **README.md**: Primary user workflows and command examples
- **USER_GUIDE.md**: Operational procedures and troubleshooting
- **ML_GETTING_STARTED.md**: ML system setup and validation

Each command was executed with standard parameters as documented, and both successful execution and error cases were recorded for analysis.