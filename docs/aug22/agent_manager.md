# Git Workflow for Agents

1. Assign each agent a distinct set of GitHub issues and ensure that no two agents work on the same files or problems to avoid conflicts.
2. Require agents to regularly pull updates from the main branch (or a designated integration branch) into their own branch/worktree before starting new work, and after completing each issue, to stay up to date and minimize merge conflicts.
   - Example: `git fetch origin && git merge origin/main`
3. Instruct agents to commit and push their changes frequently so that others can see progress and avoid duplicated effort.
4. At the end of each issue or work session, agents should post a brief summary of what they changed to a shared log (Slack, Notion, GitHub comment thread, etc.) for transparency.
5. If an agent’s work depends on another agent’s changes, they should coordinate and pull those changes into their branch after they are merged to main.
6. Encourage agents to resolve merge conflicts immediately when they arise, and to communicate any blockers or overlapping work as soon as possible.
7. At regular intervals (e.g., every few hours), agents should re-sync their branches with the main branch to ensure all bugfixes and updates are incorporated.
8. Use pull requests for merging completed work back into main, and require review from at least one other agent to catch issues early.
9. After merging, agents should delete their feature branches to keep the repository clean.
10. If automation is available (e.g., GitHub Actions, bots like Mergify/Renovate), set up automatic notifications or syncing to help keep branches up to date.

## Logistics
The port for Postgres should always be 5433
The password is postgres
Do not add more tools to the current ecosystem.

## Work Distribution & Conflict Prevention

**11. Issue Categorization & Agent Specialization:**
- **Agent A**: Focus on data collection layer issues (`src/data/collection/`, database schemas, API integrations)
- **Agent B**: Handle analysis & strategy layer issues (`src/analysis/`, backtesting, processors)
- **Agent C**: Manage CLI, services, and core utilities (`src/interfaces/cli/`, `src/services/`, `src/core/`)

**12. File-Level Ownership Matrix:**
- Create a shared document mapping critical files to primary agents
- Mark shared files (like `config.py`, models) as requiring coordination
- Use file-level locks or comments when working on shared components

## Database & Schema Coordination

**13. Database Migration Safety:**
- Only one agent can create/modify database migrations at a time
- Migrations must be numbered sequentially - coordinate through shared counter
- Test migrations on local copy before pushing
- Document all schema changes in the shared log with table/column details

**14. Three-Tier Pipeline Awareness:**
- Changes to RAW schema require coordination with STAGING processing
- STAGING modifications may impact CURATED layer transformations
- Always test the full RAW→STAGING→CURATED pipeline after database changes

## Configuration & Environment Management

**15. Configuration Change Protocol:**
- Changes to `src/core/config.py` require immediate notification to other agents
- New environment variables must be documented in shared environment file
- API rate limits and credentials should not be modified without team approval

**16. Dependency Management:**
- Only one agent should modify `pyproject.toml` or add new dependencies
- New dependencies require justification and compatibility check
- Run `uv sync` after pulling any dependency changes

## Testing & Quality Assurance

**17. Continuous Testing Protocol:**
- Run `uv run pytest --cov=src` before each commit
- If tests fail on main branch, immediately notify other agents
- New features must include tests - no exceptions for overnight work
- Use `uv run ruff format && uv run ruff check && uv run mypy src/` before each push

**18. Integration Testing Checkpoints:**
- Every 3 completed issues, run full system integration test
- Test data collection pipeline: `uv run -m src.interfaces.cli data test --source action_network --real`
- Verify database integrity across all three zones (RAW/STAGING/CURATED)

## Communication & Monitoring

**19. Real-Time Status Updates:**
- Update shared status board every 30 minutes with current file/issue
- Use standardized format: `[Agent][Time] Working on: Issue #X - File: path/to/file.py - ETA: XX:XX`
- Immediately flag any blocking dependencies or unexpected complexities

**20. Emergency Escalation:**
- If agent encounters breaking changes that affect other agents, stop work and notify immediately
- Critical system components (database connection, core models) require team approval for changes
- If more than 2 tests fail after a change, revert and reassess

## Data & Output Management

**21. Output Directory Coordination:**
- Only modify files in your assigned subdirectories of `output/`
- Changes to shared analysis results require notification
- Backup important analysis results before modifying processing logic

**22. Log File Management:**
- Use agent-specific log prefixes: `[AGENT-A]`, `[AGENT-B]`, `[AGENT-C]`
- Rotate log files if they exceed 100MB during overnight session
- Monitor disk space - alert if approaching 80% capacity

## Performance & Resource Management

**23. Resource Usage Awareness:**
- Monitor database connection count - PostgreSQL has connection limits
- Coordinate intensive operations (large data imports, backtesting) to avoid resource conflicts
- Use `--dry-run` modes when testing new collection logic

**24. API Rate Limit Coordination:**
- Share API usage tracking - Action Network, SportsBettingDime have daily limits
- If one agent hits rate limit, others should avoid that API for cooldown period
- Use test data when possible instead of hitting external APIs repeatedly

## Recovery & Rollback Procedures

**25. Checkpoint Strategy:**
- Create git tags every 5 completed issues: `overnight-checkpoint-N`
- Maintain working database backup before schema changes
- Document rollback procedures for each major component modification

**26. Morning Handoff Preparation:**
- Prepare summary report of all changes with testing status
- Document any issues that need human review or couldn't be completed
- Ensure main branch is in deployable state with all tests passing
- Create deployment checklist for any new features or configuration changes

## Specific to MLB Betting System

**27. Market Hours Awareness:**
- MLB games typically run until late night/early morning EST
- Don't modify live data collection during active game windows
- Coordinate any changes to odds processing with actual betting market activity

**28. Strategy Validation:**
- Changes to betting strategies require backtesting validation
- Don't modify sharp action detection without historical performance verification
- New analysis must align with existing three-tier pipeline architecture

These guidelines should help prevent the most common overnight collaboration issues while respecting the complex, mission-critical nature of your MLB betting analysis system.



# AI Agent Documentation & Communication Standards

## Structured Logging Format for Agent Work Sessions

### 1. Issue Work Log Template

```markdown
## [AGENT-ID] Issue #[NUMBER] - [TIMESTAMP]

### Issue Summary
- **Title**: [GitHub issue title]
- **Priority**: [High/Medium/Low]
- **Component**: [Data Collection/Analysis/CLI/Services/Core]
- **Estimated Time**: [XX minutes]
- **Dependencies**: [List any blocking issues or agent dependencies]

### Files Modified
- `path/to/file1.py` - [Brief description of changes]
- `path/to/file2.sql` - [Brief description of changes]
- `tests/test_file.py` - [New tests added]

### Database Changes
- **Schema**: [RAW/STAGING/CURATED]
- **Tables Affected**: [table_name1, table_name2]
- **Migration Required**: [Yes/No - if yes, migration file name]

### Testing Results
- **Unit Tests**: [✅ Pass / ❌ Fail - details]
- **Integration Tests**: [✅ Pass / ❌ Fail - details]
- **Code Quality**: [✅ Pass / ❌ Fail - ruff/mypy results]

### Key Findings
- [Bullet point of important discoveries]
- [Any performance impacts noted]
- [Breaking changes or compatibility issues]

### Next Steps / Handoff Notes
- [What needs to be done next]
- [Any follow-up required by other agents]
- [Manual testing or validation needed]

### Git Status
- **Branch**: [feature-branch-name]
- **Commits**: [commit hash(es)]
- **PR Status**: [Ready for review/Needs work/Merged]
```

### 2. Real-Time Status Updates (Every 30 mins)

```markdown
[AGENT-A][23:30] 🔧 Issue #42 - Fixing Action Network rate limiter
├── File: src/data/collection/consolidated_action_network_collector.py
├── Progress: 60% - Implementing exponential backoff
├── ETA: 30 mins
├── Blockers: None
└── Next: Unit tests for retry logic
```

### 3. Critical Alert Format

```markdown
🚨 [AGENT-B][01:15] CRITICAL ALERT 🚨
Issue: Database migration #003 failed on STAGING schema
Impact: Blocks all agents working on curated data pipeline
Files: sql/migrations/003_add_arbitrage_table.sql
Action Required: Need Agent-A to review foreign key constraints
Workaround: Using previous schema version for now
ETA to Fix: 45 minutes
```

### 4. Handoff Summary Report Template

```markdown
# Overnight Session Summary - [DATE]
**Session Duration**: [XX:XX - XX:XX]
**Total Issues Completed**: [X/25]
**Agents Active**: [A, B, C]

## Agent A - Data Collection & Database
### Completed Issues
- Issue #12: ✅ Enhanced Action Network collector rate limiting
- Issue #34: ✅ Added new sportsbook integration (BetMGM)
- Issue #45: ✅ Fixed STAGING to CURATED pipeline bug

### In Progress
- Issue #67: 🔄 Database optimization for curated.analysis_reports (80% complete)

### Blocked/Needs Review
- Issue #89: ⚠️ PostgreSQL connection pooling - needs DBA review

### Database Changes
| Schema | Table | Change Type | Migration |
|--------|--------|-------------|-----------|
| RAW | raw_odds | Added columns | 004_add_odds_metadata.sql |
| CURATED | analysis_reports | Index optimization | 005_optimize_analysis_indexes.sql |

### Files Modified (12 total)
- `src/data/collection/consolidated_action_network_collector.py`
- `src/data/database/repositories/analysis_reports_repository.py`
- `sql/migrations/004_add_odds_metadata.sql`
- [Full list in detailed log]

## Agent B - Analysis & Strategy
### Completed Issues
- Issue #23: ✅ Improved sharp action detection accuracy
- Issue #56: ✅ Added new consensus tracking strategy
- Issue #78: ✅ Fixed backtesting performance regression

### Key Findings
- Sharp action detection improved by 15% with new threshold algorithm
- Backtesting now 3x faster with optimized data queries
- Found bug in line movement processor affecting weekend games

### Performance Metrics
- Strategy accuracy: 67.3% (up from 64.1%)
- Processing speed: 2.3s per game (down from 7.1s)
- Memory usage: Reduced by 40%

## Agent C - CLI & Services
### Completed Issues
- Issue #11: ✅ Added new CLI commands for manual data validation
- Issue #33: ✅ Enhanced error handling in orchestration service
- Issue #77: ✅ Improved logging format consistency

### CLI Enhancements
- New command: `data validate --zone [RAW/STAGING/CURATED]`
- Enhanced: `status --detailed` now shows pipeline health
- Fixed: Memory leak in long-running collection processes

## Cross-Agent Coordination
### Successful Collaborations
- Agents A & B: Coordinated database schema changes for new analysis features
- Agents B & C: Integrated new strategy results into CLI reporting
- All agents: Maintained consistent logging format

### Issues Resolved Together
- Issue #99: Database deadlock (Agent A + Agent B)
- Issue #101: CLI timeout with large datasets (Agent B + Agent C)

## System Health Check
- ✅ All tests passing (487/487)
- ✅ Code quality: 100% (ruff + mypy clean)
- ✅ Database integrity verified across all zones
- ✅ API integrations functional
- ⚠️ Disk usage at 78% (needs cleanup)

## Outstanding Items for Human Review
1. **Performance**: Query optimization in curated schema needs DBA review
2. **Security**: New API keys need to be rotated after testing
3. **Deployment**: Two new environment variables need production setup
4. **Documentation**: Updated architecture diagram needs review

## Rollback Information
- **Safe Rollback Point**: `overnight-checkpoint-5` (02:30 AM)
- **Database Backup**: `mlb_backup_2025_08_22_0230.sql`
- **Critical Changes**: None that would break production

## Next Session Priorities
1. Complete Issue #67 (database optimization)
2. Address disk space warning
3. Review and merge 3 pending PRs
4. Begin work on Issues #103-110 (next sprint)
```

### 5. Technical Discovery Log Format

```markdown
## Technical Discovery - [AGENT-ID] - [TIMESTAMP]

### Context
**Issue**: #[number]
**Component**: [specific system component]
**Investigation Trigger**: [what led to this discovery]

### Problem Description
[Detailed technical description]

### Root Cause Analysis
1. **Symptom**: [what was observed]
2. **Investigation Steps**: 
   - [Step 1 with results]
   - [Step 2 with results]
3. **Root Cause**: [underlying technical issue]

### Solution Implemented
```python
# Before (problematic code)
def old_function():
    # problematic implementation

# After (fixed code)  
def new_function():
    # improved implementation
```

### Testing & Validation
- **Test Cases**: [specific tests run]
- **Performance Impact**: [before/after metrics]
- **Edge Cases Considered**: [list of edge cases]

### Documentation Impact
- **README Updates**: [files that need updating]
- **API Changes**: [any breaking changes]
- **Configuration Changes**: [new settings required]

### Knowledge Transfer
**Key Learnings**:
- [Important technical insights]
- [Best practices discovered]
- [Pitfalls to avoid]

**Recommendations for Other Agents**:
- [Specific guidance for similar issues]
- [Code patterns to follow/avoid]
```

### 6. Shared Knowledge Base Entry Format

```markdown
# Knowledge Base Entry: [TOPIC]
**Created by**: [AGENT-ID] | **Date**: [DATE] | **Issue Context**: #[number]

## Quick Reference
**Problem**: [One-line problem description]
**Solution**: [One-line solution]
**Impact**: [System components affected]

## Detailed Guide

### When This Applies
- [Specific scenarios where this knowledge is relevant]
- [Warning signs that indicate this issue]

### Step-by-Step Solution
1. **Diagnosis**
   ```bash
   # Commands to identify the issue
   uv run -m src.interfaces.cli data status --detailed
   ```

2. **Fix Implementation**
   ```python
   # Code snippets or configuration changes
   ```

3. **Validation**
   ```bash
   # Commands to verify the fix
   ```

### Related Components
- **Files**: [List of files typically involved]
- **Database Tables**: [Tables that might be affected]
- **Dependencies**: [Other system components to check]

### Common Pitfalls
- ❌ [Thing not to do]
- ❌ [Another mistake to avoid]
- ✅ [Correct approach]

### See Also
- Issue #[related issue numbers]
- Knowledge Base: [related topics]
- Architecture docs: [relevant sections]
```

### 7. Daily Communication Board Format

```markdown
# AI Agent Coordination Board - [DATE]

## Current Status Overview
| Agent | Current Issue | File | Progress | ETA | Status |
|-------|---------------|------|----------|-----|--------|
| A | #42 | collector.py | 60% | 30m | 🔧 |
| B | #56 | strategy.py | 90% | 15m | 🔧 |
| C | #78 | cli/main.py | 100% | - | ✅ |

## Resource Locks
| Resource | Locked By | Until | Purpose |
|----------|-----------|--------|---------|
| config.py | Agent-C | 01:30 | CLI enhancements |
| Migration #005 | Agent-A | 02:00 | Schema updates |

## Upcoming Coordination Points
- **01:45**: Agent B needs Agent A's database changes
- **02:30**: All agents sync for checkpoint
- **03:00**: Integration testing window

## Shared Findings Queue
1. **Performance**: New query optimization pattern (Agent-A)
2. **Bug**: Edge case in weekend processing (Agent-B)  
3. **Enhancement**: Better error messages (Agent-C)

## Blockers & Dependencies
- Issue #67 blocked pending Issue #45 completion
- Agent-B waiting for Agent-A's schema migration
- Rate limit hit on Action Network (cooldown until 02:15)
```

This structured documentation system ensures:
- **Consistency** across all agents
- **Traceability** of all changes and decisions
- **Knowledge sharing** for complex technical discoveries
- **Coordination** to prevent conflicts
- **Handoff clarity** for human developers
- **Historical record** for future debugging and optimization