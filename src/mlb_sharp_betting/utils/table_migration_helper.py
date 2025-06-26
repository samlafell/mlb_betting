#!/usr/bin/env python3
"""
Table Name Migration Helper

This utility helps identify hardcoded table names in the codebase and provides
suggestions for updating them to use the centralized table registry.

Usage:
    from mlb_sharp_betting.utils.table_migration_helper import TableMigrationHelper
    
    helper = TableMigrationHelper()
    references = helper.scan_directory(Path("analysis_scripts"))
    report = helper.generate_migration_report(references)
    print(report)
"""

import re
import os
from pathlib import Path
from typing import Dict, List, Tuple, Set
from dataclasses import dataclass

from ..db.table_registry import get_table_registry, Tables, DatabaseType


@dataclass
class TableReference:
    """Represents a hardcoded table reference found in code."""
    file_path: str
    line_number: int
    line_content: str
    table_name: str
    suggested_replacement: str


class TableMigrationHelper:
    """Helper class for migrating hardcoded table names to use the table registry."""
    
    def __init__(self):
        self.registry = get_table_registry(DatabaseType.POSTGRESQL)
        
        # Create reverse mapping from actual table names to logical names
        self.reverse_mapping = {}
        for logical, actual in self.registry.list_tables().items():
            self.reverse_mapping[actual] = logical
            # Also map without schema for partial matches
            if '.' in actual:
                table_only = actual.split('.', 1)[1]
                if table_only not in self.reverse_mapping:
                    self.reverse_mapping[table_only] = logical
        
        # Common hardcoded table patterns to look for
        self.table_patterns = [
            # PostgreSQL patterns
            r'splits\.raw_mlb_betting_splits',
            r'splits\.betting_splits', 
            r'splits\.games',
            r'splits\.sharp_actions',
            r'public\.game_outcomes',
            r'backtesting\.strategy_performance',
            r'clean\.betting_recommendations',
            
            # Legacy database patterns
            r'mlb_betting\.splits\.raw_mlb_betting_splits',
            r'mlb_betting\.main\.game_outcomes',
            r'main\.game_outcomes',
            r'main\.raw_mlb_betting_splits',
            
            # Simple table names (be careful with these)
            r'\braw_mlb_betting_splits\b',
            r'\bgame_outcomes\b',
            r'\bstrategy_performance\b',
        ]
        
        # File extensions to scan
        self.file_extensions = {'.py', '.sql'}
        
        # Directories to exclude
        self.exclude_dirs = {'.git', '__pycache__', '.venv', 'venv', 'node_modules', '.pytest_cache'}
    
    def scan_directory(self, directory: Path) -> List[TableReference]:
        """Scan a directory for hardcoded table references."""
        references = []
        
        for file_path in self._get_files_to_scan(directory):
            file_references = self._scan_file(file_path)
            references.extend(file_references)
        
        return references
    
    def _get_files_to_scan(self, directory: Path) -> List[Path]:
        """Get list of files to scan for table references."""
        files = []
        
        for root, dirs, filenames in os.walk(directory):
            # Remove excluded directories
            dirs[:] = [d for d in dirs if d not in self.exclude_dirs]
            
            for filename in filenames:
                file_path = Path(root) / filename
                if file_path.suffix in self.file_extensions:
                    files.append(file_path)
        
        return files
    
    def _scan_file(self, file_path: Path) -> List[TableReference]:
        """Scan a single file for hardcoded table references."""
        references = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            for line_num, line in enumerate(lines, 1):
                line_references = self._scan_line(file_path, line_num, line)
                references.extend(line_references)
                
        except Exception as e:
            print(f"Warning: Could not scan {file_path}: {e}")
        
        return references
    
    def _scan_line(self, file_path: Path, line_num: int, line: str) -> List[TableReference]:
        """Scan a single line for hardcoded table references."""
        references = []
        
        for pattern in self.table_patterns:
            matches = re.finditer(pattern, line)
            for match in matches:
                table_name = match.group(0)
                suggested_replacement = self._get_suggested_replacement(table_name)
                
                if suggested_replacement:
                    references.append(TableReference(
                        file_path=str(file_path),
                        line_number=line_num,
                        line_content=line.strip(),
                        table_name=table_name,
                        suggested_replacement=suggested_replacement
                    ))
        
        return references
    
    def _get_suggested_replacement(self, table_name: str) -> str:
        """Get suggested replacement for a hardcoded table name."""
        # Try exact match first
        if table_name in self.reverse_mapping:
            logical_name = self.reverse_mapping[table_name]
            return f"registry.get_table(Tables.{logical_name.upper()})"
        
        # Try partial matches
        for actual, logical in self.reverse_mapping.items():
            if table_name in actual or actual.endswith(table_name):
                return f"registry.get_table(Tables.{logical_name.upper()})"
        
        return ""
    
    def generate_migration_report(self, references: List[TableReference]) -> str:
        """Generate a migration report."""
        if not references:
            return "✅ No hardcoded table names found!"
        
        report = []
        report.append("🔍 HARDCODED TABLE NAMES MIGRATION REPORT")
        report.append("=" * 60)
        report.append(f"Found {len(references)} hardcoded table references")
        report.append("")
        
        # Group by file
        by_file = {}
        for ref in references:
            if ref.file_path not in by_file:
                by_file[ref.file_path] = []
            by_file[ref.file_path].append(ref)
        
        for file_path, file_refs in by_file.items():
            report.append(f"📁 {file_path}")
            report.append("-" * 40)
            
            for ref in file_refs:
                report.append(f"  Line {ref.line_number:3d}: {ref.table_name}")
                report.append(f"           → {ref.suggested_replacement}")
                report.append(f"           Code: {ref.line_content[:80]}...")
                report.append("")
        
        # Add migration instructions
        report.append("🛠️  MIGRATION INSTRUCTIONS")
        report.append("=" * 60)
        report.append("1. Add table registry import to each file:")
        report.append("   from mlb_sharp_betting.db.table_registry import get_table_registry, Tables")
        report.append("")
        report.append("2. Initialize registry in your class/function:")
        report.append("   registry = get_table_registry()")
        report.append("")
        report.append("3. Replace hardcoded table names with registry calls:")
        report.append("   OLD: 'splits.raw_mlb_betting_splits'")
        report.append("   NEW: registry.get_table(Tables.RAW_BETTING_SPLITS)")
        report.append("")
        report.append("4. For SQL queries, use f-strings:")
        report.append("   query = f'SELECT * FROM {registry.get_table(Tables.RAW_BETTING_SPLITS)}'")
        
        return "\n".join(report)
    
    def show_available_tables(self) -> str:
        """Show all available table mappings."""
        report = []
        report.append("📋 AVAILABLE TABLE MAPPINGS")
        report.append("=" * 50)
        
        for logical, actual in self.registry.list_tables().items():
            constant_name = logical.upper()
            report.append(f"  Tables.{constant_name:<25} -> {actual}")
        
        return "\n".join(report)


if __name__ == "__main__":
    """Example usage."""
    helper = TableMigrationHelper()
    
    print(helper.show_available_tables())
    print("\n")
    
    # Scan analysis_scripts directory
    analysis_dir = Path(__file__).parent.parent.parent.parent / "analysis_scripts"
    if analysis_dir.exists():
        print(f"🔍 Scanning {analysis_dir} for hardcoded table names...")
        references = helper.scan_directory(analysis_dir)
        report = helper.generate_migration_report(references)
        print(report)
    else:
        print("analysis_scripts directory not found") 