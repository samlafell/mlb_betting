#!/usr/bin/env python3
"""
Automated Code Refactor Tool for Core Betting Schema Decommission

This tool automates the replacement of core_betting schema references throughout
the codebase with their corresponding curated schema equivalents.

Key Features:
- Comprehensive file scanning and pattern matching
- Schema mapping configuration for all table replacements
- Special handling for betting lines consolidation
- Automatic backup creation
- Detailed refactoring reports

Usage:
    python automated_code_refactor.py --dry-run        # Preview changes
    python automated_code_refactor.py --report-only    # Generate detailed report
    python automated_code_refactor.py --execute        # Execute refactoring with backup
"""

import os
import re
import shutil
import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/core_betting_refactor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CoreBettingRefactor:
    """Automated code refactoring tool for core_betting schema decommission."""
    
    def __init__(self, project_root: str = "."):
        self.project_root = Path(project_root)
        self.backup_dir = None
        self.changes_made = []
        self.report_data = {
            'timestamp': datetime.now().isoformat(),
            'total_files_scanned': 0,
            'files_with_changes': 0,
            'total_changes': 0,
            'schema_mappings': {},
            'file_changes': {},
            'complex_patterns': []
        }
        
        # Schema mapping configuration based on Phase 1 analysis
        self.schema_mappings = {
            # Direct table mappings
            'curated.games_complete': 'curated.games_complete',
            'curated.game_outcomes': 'curated.game_outcomes',
            'curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline'': 'curated.betting_lines_unified',
            'curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals'': 'curated.betting_lines_unified',
            'curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread'': 'curated.betting_lines_unified',
            'curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread's': 'curated.betting_lines_unified',
            'curated.sportsbooks': 'curated.sportsbooks',
            'curated.teams_master': 'curated.teams_master',
            'curated.sportsbook_mappings': 'curated.sportsbook_mappings',
            'curated.data_sources': 'curated.data_sources',
            'operational.schema_migrations': 'operational.schema_migrations',
            'curated.games_complete': 'curated.games_complete',
            'curated.betting_splits': 'curated.betting_splits',
            # Schema-level mapping
            'curated.': 'curated.'
        }
        
        # Files to exclude from refactoring
        self.exclude_patterns = [
            '*.log',
            '*.pyc',
            '__pycache__/*',
            '.git/*',
            'venv/*',
            'env/*',
            'node_modules/*',
            'backups/*',
            'core_betting_refactor_report.md',
            'pre_migration_validation_report.md',
            'PHASE_1_COMPLETION_SUMMARY.md'
        ]
        
        # File extensions to process
        self.include_extensions = ['.py', '.sql', '.md', '.json', '.toml', '.yaml', '.yml']
        
    def create_backup(self) -> Path:
        """Create a complete backup of the project before refactoring."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.backup_dir = self.project_root / 'backups' / f'pre_refactor_backup_{timestamp}'
        
        logger.info(f"Creating backup at {self.backup_dir}")
        
        # Create backup directory
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        
        # Copy all relevant files
        for root, dirs, files in os.walk(self.project_root):
            # Skip excluded directories
            dirs[:] = [d for d in dirs if not any(
                d.startswith(exclude.rstrip('/*')) for exclude in self.exclude_patterns
                if '/*' in exclude
            )]
            
            for file in files:
                if any(file.endswith(ext) for ext in self.include_extensions):
                    source_path = Path(root) / file
                    relative_path = source_path.relative_to(self.project_root)
                    backup_path = self.backup_dir / relative_path
                    
                    # Create directory if it doesn't exist
                    backup_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    # Copy file
                    shutil.copy2(source_path, backup_path)
        
        logger.info(f"Backup created successfully at {self.backup_dir}")
        return self.backup_dir
    
    def scan_files(self) -> List[Path]:
        """Scan project for files that need refactoring."""
        files_to_process = []
        
        for root, dirs, files in os.walk(self.project_root):
            # Skip excluded directories
            dirs[:] = [d for d in dirs if not any(
                d.startswith(exclude.rstrip('/*')) for exclude in self.exclude_patterns
                if '/*' in exclude
            )]
            
            for file in files:
                if any(file.endswith(ext) for ext in self.include_extensions):
                    file_path = Path(root) / file
                    
                    # Skip excluded files
                    if any(file_path.match(pattern) for pattern in self.exclude_patterns):
                        continue
                    
                    files_to_process.append(file_path)
        
        self.report_data['total_files_scanned'] = len(files_to_process)
        logger.info(f"Found {len(files_to_process)} files to scan")
        return files_to_process
    
    def analyze_file(self, file_path: Path) -> List[Dict]:
        """Analyze a single file for core_betting references."""
        changes = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Track line numbers for changes
            lines = content.split('\n')
            
            for line_num, line in enumerate(lines, 1):
                for old_pattern, new_pattern in self.schema_mappings.items():
                    if old_pattern in line:
                        changes.append({
                            'line_number': line_num,
                            'old_pattern': old_pattern,
                            'new_pattern': new_pattern,
                            'original_line': line.strip(),
                            'file_path': str(file_path.relative_to(self.project_root))
                        })
        
        except Exception as e:
            logger.warning(f"Error analyzing {file_path}: {e}")
        
        return changes
    
    def apply_changes(self, file_path: Path, changes: List[Dict], dry_run: bool = False) -> bool:
        """Apply refactoring changes to a file."""
        if not changes:
            return False
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Apply all mappings
            modified_content = content
            for old_pattern, new_pattern in self.schema_mappings.items():
                if old_pattern in modified_content:
                    # Special handling for betting lines consolidation
                    if 'betting_lines_' in old_pattern and old_pattern != 'curated.betting_lines_unified':
                        # Add comment about market_type requirement
                        modified_content = modified_content.replace(
                            old_pattern,
                            f"{new_pattern} -- NOTE: Add WHERE market_type = '{self._get_market_type(old_pattern)}'"
                        )
                    else:
                        modified_content = modified_content.replace(old_pattern, new_pattern)
            
            if not dry_run and modified_content != content:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(modified_content)
                
                logger.info(f"Refactored {file_path}")
                return True
            
        except Exception as e:
            logger.error(f"Error applying changes to {file_path}: {e}")
        
        return False
    
    def _get_market_type(self, table_name: str) -> str:
        """Get market type for betting lines consolidation."""
        if 'moneyline' in table_name:
            return 'moneyline'
        elif 'spread' in table_name:
            return 'spread'
        elif 'total' in table_name:
            return 'totals'
        return 'unknown'
    
    def generate_report(self, output_file: str = None) -> str:
        """Generate detailed refactoring report."""
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"core_betting_refactor_report_{timestamp}.md"
        
        report_content = f"""# Core Betting Schema Refactoring Report

**Generated:** {self.report_data['timestamp']}
**Files Scanned:** {self.report_data['total_files_scanned']}
**Files with Changes:** {self.report_data['files_with_changes']}
**Total Changes:** {self.report_data['total_changes']}

## Summary of Changes

### Schema Mapping Rules

"""
        
        for old_pattern, new_pattern in self.schema_mappings.items():
            report_content += f"- `{old_pattern}` → `{new_pattern}`\n"
        
        report_content += "\n### Files Requiring Changes\n\n"
        
        for file_path, changes in self.report_data['file_changes'].items():
            if changes:
                report_content += f"#### {file_path}\n\n**Direct Mappings:**\n"
                for change in changes:
                    report_content += f"- Line {change['line_number']}: {change['old_pattern']} → {change['new_pattern']}\n"
                report_content += "\n"
        
        # Add special notes for complex patterns
        report_content += """
## Special Considerations

### Betting Lines Consolidation

The three betting lines tables will be consolidated into a single `curated.betting_lines_unified` table:

```sql
-- OLD: Separate tables
SELECT * FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline' WHERE game_id = 123;
SELECT * FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread's WHERE game_id = 123;
SELECT * FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals' WHERE game_id = 123;

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
"""
        
        # Write report to file
        report_path = self.project_root / output_file
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        logger.info(f"Report generated: {report_path}")
        return str(report_path)
    
    def execute_refactoring(self, dry_run: bool = False, create_backup: bool = True) -> Dict:
        """Execute the complete refactoring process."""
        logger.info(f"Starting core_betting schema refactoring (dry_run={dry_run})")
        
        # Create backup if not dry run
        if not dry_run and create_backup:
            self.create_backup()
        
        # Scan all files
        files_to_process = self.scan_files()
        
        # Process each file
        total_changes = 0
        files_modified = 0
        
        for file_path in files_to_process:
            changes = self.analyze_file(file_path)
            
            if changes:
                relative_path = str(file_path.relative_to(self.project_root))
                self.report_data['file_changes'][relative_path] = changes
                
                if self.apply_changes(file_path, changes, dry_run):
                    files_modified += 1
                
                total_changes += len(changes)
                self.changes_made.extend(changes)
        
        # Update report data
        self.report_data['files_with_changes'] = files_modified
        self.report_data['total_changes'] = total_changes
        
        # Generate report
        report_path = self.generate_report()
        
        result = {
            'success': True,
            'files_scanned': len(files_to_process),
            'files_modified': files_modified,
            'total_changes': total_changes,
            'backup_created': self.backup_dir if not dry_run and create_backup else None,
            'report_path': report_path,
            'dry_run': dry_run
        }
        
        logger.info(f"Refactoring completed: {result}")
        return result

def main():
    """Main entry point for the refactoring tool."""
    parser = argparse.ArgumentParser(
        description="Automated Core Betting Schema Refactoring Tool"
    )
    parser.add_argument(
        '--dry-run', 
        action='store_true',
        help='Preview changes without modifying files'
    )
    parser.add_argument(
        '--report-only', 
        action='store_true',
        help='Generate detailed report without making changes'
    )
    parser.add_argument(
        '--execute', 
        action='store_true',
        help='Execute refactoring with backup creation'
    )
    parser.add_argument(
        '--project-root',
        default='.',
        help='Project root directory (default: current directory)'
    )
    parser.add_argument(
        '--no-backup',
        action='store_true',
        help='Skip backup creation (not recommended)'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if not any([args.dry_run, args.report_only, args.execute]):
        print("Please specify one of: --dry-run, --report-only, or --execute")
        return 1
    
    try:
        refactor = CoreBettingRefactor(args.project_root)
        
        if args.report_only:
            # Generate report only
            refactor.scan_files()
            files_to_process = refactor.scan_files()
            for file_path in files_to_process:
                changes = refactor.analyze_file(file_path)
                if changes:
                    relative_path = str(file_path.relative_to(refactor.project_root))
                    refactor.report_data['file_changes'][relative_path] = changes
                    refactor.report_data['total_changes'] += len(changes)
                    refactor.report_data['files_with_changes'] += 1
            
            report_path = refactor.generate_report()
            print(f"Report generated: {report_path}")
            
        else:
            # Execute refactoring
            result = refactor.execute_refactoring(
                dry_run=args.dry_run,
                create_backup=not args.no_backup and not args.dry_run
            )
            
            print(f"Refactoring completed:")
            print(f"  Files scanned: {result['files_scanned']}")
            print(f"  Files modified: {result['files_modified']}")
            print(f"  Total changes: {result['total_changes']}")
            print(f"  Report: {result['report_path']}")
            
            if result['backup_created']:
                print(f"  Backup: {result['backup_created']}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Refactoring failed: {e}")
        return 1

if __name__ == "__main__":
    exit(main())