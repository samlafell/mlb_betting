# 🚀 MLB Sharp Betting - Reorganized Usage Guide

## Quick Reference for New File Locations

### 🔄 CLI Commands (Previously `run_*.py`)

```bash
# Automated Backtesting System
uv run -m mlb_sharp_betting.cli.commands.backtesting --mode scheduler
uv run -m mlb_sharp_betting.cli.commands.backtesting --mode single-run

# Comprehensive Analysis Runner  
uv run -m mlb_sharp_betting.cli.commands.analysis

# Daily Game Updates
uv run -m mlb_sharp_betting.cli.commands.daily_update

# MLB Betting Scheduler
uv run -m mlb_sharp_betting.cli.commands.scheduler
```

### 🔧 Development Utilities

```bash
# Database Inspection (was check_splits.py)
uv run -m mlb_sharp_betting.utils.database_inspector

# Quick Database Check (was quick_check.py)  
uv run -m mlb_sharp_betting.utils.quick_db_check
```

### 🧪 Testing

```bash
# Integration Tests
pytest tests/integration/test_game_updater.py
pytest tests/integration/test_mlb_api_integration.py  
pytest tests/integration/test_odds_api_integration.py

# Manual Testing Scripts
uv run -m tests.manual.test_json_parsing
uv run -m tests.manual.test_line_parsing
```

### 📚 Examples & Demos

```bash
# Pinnacle Scraper Demo
uv run -m mlb_sharp_betting.examples.pinnacle_scraper_demo
```

### 📊 Analysis Tools

```bash
# Sharp Action Analyzer (was sharp_action_analyzer_fixed.py)
uv run -m mlb_sharp_betting.analyzers.sharp_action_analyzer
```

## 📁 New Directory Structure

```
Root Directory (CLEAN! ✨)
├── pyproject.toml
├── README.md
├── uv.lock
└── [other config files]

src/mlb_sharp_betting/
├── cli/commands/          ← All CLI tools moved here
├── examples/              ← Demo scripts  
├── analyzers/             ← Analysis frameworks
└── utils/                 ← Database utilities

tests/
├── integration/           ← API & system integration tests
└── manual/                ← Manual testing scripts
```

## 🎯 Benefits of Reorganization

✅ **Clean root directory** - Only essential project files  
✅ **Logical organization** - Similar functionality grouped together  
✅ **Professional structure** - Follows Python packaging best practices  
✅ **Easy maintenance** - Clear separation of concerns  
✅ **Scalable** - Easy to add new functionality in right places

---
*General Balls ⚾* 