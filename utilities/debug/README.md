# Debug Utilities

This directory contains debugging tools and scripts for the MLB betting system.

## Directory Structure

```
utilities/debug/
├── README.md          # This file
└── archive/           # Archived debug scripts from completed investigations
```

## Active Debug Scripts

Current active debug scripts are located in `scripts/debug/`:

- `debug_conflict.py` - Debug database ON CONFLICT behavior
- `debug_real_parsing_chain.py` - Debug parsing chain issues
- `debug_staging_processing.py` - Debug staging pipeline processing

## Archived Debug Scripts

The `archive/` directory contains debug scripts from completed investigations:

- Backtesting data debugging tools
- Data flow analysis scripts  
- Integration flow debugging
- Odds extraction debugging
- Strategy profitability analysis
- Storage method testing
- And other historical debugging tools

## Usage Guidelines

### For Active Debugging
1. Create specific, focused debug scripts in `scripts/debug/`
2. Use descriptive names indicating the issue being investigated
3. Include clear comments about the debugging goal
4. Remove or archive scripts once issues are resolved

### For New Debug Scripts
1. Follow naming convention: `debug_<issue_description>.py`
2. Include docstring explaining the debugging objective
3. Use proper error handling and logging
4. Clean up after investigation is complete

### Archiving Debug Scripts
When debugging is complete:
1. Move resolved scripts to `utilities/debug/archive/`
2. Document findings in relevant issue/PR
3. Remove any hardcoded credentials or sensitive data

## Integration with Main CLI

For permanent debugging features, consider integrating into the main CLI:
- Add debug commands to `src/interfaces/cli/commands/`
- Use proper logging levels instead of print statements
- Follow the project's error handling patterns

## Cleanup Policy

- Archive debug scripts after 30 days of inactivity
- Remove archived scripts after 6 months unless historically significant
- Regular cleanup during project maintenance cycles