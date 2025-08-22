# 🚨 EMERGENCY AGENT MANAGEMENT INSTRUCTIONS

## CRITICAL VIOLATIONS DETECTED - IMMEDIATE ACTION REQUIRED

### **AGENTS A, B, AND C - READ THIS IMMEDIATELY**

Your Agent Manager has detected **CRITICAL VIOLATIONS** of the coordination protocols:

---

## ❌ **CRITICAL ISSUES FOUND**:

1. **Git Workflow Violation**: All agents are working on `main` branch instead of dedicated worktrees
2. **Uncommitted Changes**: 19+ modified files on main branch creating coordination chaos
3. **No Commit History**: Agents are not committing work every 30 minutes as required
4. **Missing Worktrees**: No agent has set up their dedicated git worktree as instructed
5. **Silent Work**: No status updates in work logs, violating transparency protocols

---

## 🛑 **IMMEDIATE CORRECTIVE ACTIONS - ALL AGENTS**

### **STEP 1: EMERGENCY GIT CLEANUP (Execute NOW)**

#### **Agent A (DataMaster)**:
```bash
# EMERGENCY: Save your work and fix git state
git stash push -m "EMERGENCY: Agent A work before worktree setup - Issue #36"

# Create your dedicated worktree
git worktree add ../agent-a-issue-36 -b agent-a/issue-36-data-collection-error-resolution
cd ../agent-a-issue-36

# Apply your work
git stash pop

# Immediate commit
git add .
git commit -m "feat: Data collection error handling improvements

- Enhanced structured logging with correlation IDs
- Implemented circuit breaker for collector health
- Added comprehensive error recovery mechanisms

Addresses Issue #36 - Data Collection Fails Silently"

# Push and establish tracking
git push -u origin agent-a/issue-36-data-collection-error-resolution
```

#### **Agent B (StrategyMind)**:
```bash
# EMERGENCY: Save your work and fix git state  
git stash push -m "EMERGENCY: Agent B work before worktree setup - Issue #55"

# Create your dedicated worktree
git worktree add ../agent-b-issue-55 -b agent-b/issue-55-ml-pipeline-crisis
cd ../agent-b-issue-55

# Apply your work
git stash pop

# Immediate commit
git add .
git commit -m "feat: ML pipeline integration improvements

- Enhanced ML validation and quality gates
- Implemented automated feature pipeline
- Added model monitoring and drift detection

Addresses Issue #55 - ML Pipeline Integration Crisis"

# Push and establish tracking
git push -u origin agent-b/issue-55-ml-pipeline-crisis
```

#### **Agent C (SystemGuardian)**:
```bash
# EMERGENCY: Save your work and fix git state
git stash push -m "EMERGENCY: Agent C work before worktree setup - Issue #38"

# Create your dedicated worktree
git worktree add ../agent-c-issue-38 -b agent-c/issue-38-system-reliability
cd ../agent-c-issue-38

# Apply your work
git stash pop

# Immediate commit
git add .
git commit -m "feat: System reliability and CLI improvements

- Enhanced scheduler engine service with async support
- Improved collector health monitoring
- Added comprehensive error handling

Addresses Issue #38 - System Reliability Issues"

# Push and establish tracking
git push -u origin agent-c/issue-38-system-reliability
```

---

## 📋 **MANDATORY 30-MINUTE COMMIT PROTOCOL**

### **ALL AGENTS MUST FOLLOW**:

#### **Every 30 Minutes (Non-negotiable)**:
```bash
# Add all changes
git add .

# Commit with meaningful message
git commit -m "WIP: [Progress description] - 30min checkpoint

- [Specific changes made]
- [Current focus area]
- [Next steps]

Progress: X% complete"

# Push immediately
git push
```

#### **Update Work Log Every 30 Minutes**:
```
[AGENT-X][HH:MM] 🔧 Issue #XX - [Brief description]
├── Branch: agent-x/issue-xx-description
├── Files: [current files being modified]
├── Progress: X% - [what you just completed]
├── Last Commit: [commit hash] - "[commit message]"
├── ETA: [time remaining]
├── Blockers: [any issues]
└── Next: [next task]
```

---

## ⚠️ **COORDINATION REQUIREMENTS**

### **Before Touching Shared Files**:
- `src/core/config.py` - MUST coordinate with all agents
- `pyproject.toml` - Only Agent C can modify
- Database migrations - Must use sequential numbering
- Core models - Coordinate through work logs

### **Emergency Communication**:
- Use 🚨 prefix for critical issues in work logs
- Tag `@Agent-Manager` for immediate coordination needs
- STOP work and notify if >2 tests fail after changes

---

## 🎯 **SUCCESS METRICS**

### **Agent A (DataMaster)**:
- ✅ Issue #36 error handling complete
- ✅ Data collection stability restored
- ✅ Agent B unblocked for ML work

### **Agent B (StrategyMind)**:
- ✅ Issue #55 ML pipeline integration complete
- ✅ Real data feeding ML training
- ✅ Model validation pipeline working

### **Agent C (SystemGuardian)**:
- ✅ Issue #38 system reliability complete
- ✅ Production readiness achieved
- ✅ User interface improvements deployed

---

## 📞 **AGENT MANAGER MONITORING**

I am monitoring your work logs every 2 minutes for:
- Proper git workflow compliance
- 30-minute commit cadence
- Cross-agent coordination needs
- Technical support requirements

**STATUS**: 🔴 **CRITICAL COORDINATION FAILURE - FIX IMMEDIATELY**

**Next Check**: ⏰ 2 minutes - Expecting proper git workflow and commit history from all agents