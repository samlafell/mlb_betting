#!/usr/bin/env python3
"""
Fix SQL files for PostgreSQL compatibility.

This script processes all SQL files in the project to convert DuckDB-specific
syntax to PostgreSQL-compatible syntax using the enhanced SQL preprocessor.
"""

import os
import sys
from pathlib import Path
from typing import List

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / "src"))

from mlb_sharp_betting.services.sql_preprocessor import (
    SQLPreprocessor, 
    DatabaseType,
    process_sql_file
)

def find_sql_files() -> List[Path]:
    """Find all SQL files that need processing."""
    sql_files = []
    
    # Check main SQL directories
    directories = [
        project_root / "sql",
        project_root / "analysis_scripts",
        project_root / "manual_strategy_evaluation_queries.sql"
    ]
    
    for directory in directories:
        if directory.is_file() and directory.suffix == '.sql':
            sql_files.append(directory)
        elif directory.is_dir():
            sql_files.extend(directory.glob("*.sql"))
    
    return sql_files


def backup_file(file_path: Path) -> Path:
    """Create a backup of the original file."""
    backup_path = file_path.with_suffix(f"{file_path.suffix}.backup")
    if not backup_path.exists():
        backup_path.write_text(file_path.read_text())
        print(f"✅ Backup created: {backup_path}")
    return backup_path


def process_sql_files(dry_run: bool = False) -> None:
    """Process all SQL files for PostgreSQL compatibility."""
    sql_files = find_sql_files()
    preprocessor = SQLPreprocessor(DatabaseType.POSTGRESQL)
    
    print(f"🔍 Found {len(sql_files)} SQL files to process")
    print("=" * 60)
    
    total_files = 0
    processed_files = 0
    error_files = 0
    
    for sql_file in sql_files:
        total_files += 1
        print(f"\n📁 Processing: {sql_file.relative_to(project_root)}")
        
        try:
            # Read original content
            original_content = sql_file.read_text()
            
            # Process the SQL
            processed_content = preprocessor.process_sql_string(original_content)
            
            # Get transformation summary
            summary = preprocessor.get_transformation_summary(original_content, processed_content)
            
            # Check if changes were made
            if original_content == processed_content:
                print("   ℹ️  No changes needed")
                continue
            
            # Show what would be changed
            print(f"   📊 Original length: {summary['original_length']} chars")
            print(f"   📊 Processed length: {summary['processed_length']} chars")
            print(f"   📊 Character difference: {summary['characters_changed']}")
            
            if summary['transformations_applied']:
                print("   🔄 Transformations applied:")
                for transformation in summary['transformations_applied'][:5]:  # Show first 5
                    print(f"      • {transformation}")
                if len(summary['transformations_applied']) > 5:
                    print(f"      • ... and {len(summary['transformations_applied']) - 5} more")
            
            if summary['issues_found']:
                print("   ⚠️  Issues found in original:")
                for issue in summary['issues_found'][:3]:  # Show first 3
                    print(f"      • {issue}")
                if len(summary['issues_found']) > 3:
                    print(f"      • ... and {len(summary['issues_found']) - 3} more")
            
            if not dry_run:
                # Create backup before modifying
                backup_file(sql_file)
                
                # Write processed content
                sql_file.write_text(processed_content)
                print("   ✅ File updated successfully")
                processed_files += 1
            else:
                print("   🔍 DRY RUN: Would update this file")
                
        except Exception as e:
            print(f"   ❌ Error processing file: {e}")
            error_files += 1
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 PROCESSING SUMMARY")
    print("=" * 60)
    print(f"Total files examined: {total_files}")
    print(f"Files processed: {processed_files}")
    print(f"Files with errors: {error_files}")
    print(f"Files unchanged: {total_files - processed_files - error_files}")
    
    if dry_run:
        print("\n🔍 This was a DRY RUN - no files were actually modified")
        print("Run with --apply to make actual changes")
    else:
        print("\n✅ All files have been processed!")
        print("💾 Backups created with .backup extension")


def show_preview(file_path: Path) -> None:
    """Show a preview of changes for a specific file."""
    preprocessor = SQLPreprocessor(DatabaseType.POSTGRESQL)
    
    try:
        original_content = file_path.read_text()
        processed_content = preprocessor.process_sql_string(original_content)
        
        print(f"📁 File: {file_path.relative_to(project_root)}")
        print("=" * 80)
        
        if original_content == processed_content:
            print("ℹ️  No changes needed for this file")
            return
        
        # Show first few differences
        original_lines = original_content.split('\n')
        processed_lines = processed_content.split('\n')
        
        print("🔍 PREVIEW OF CHANGES:")
        print("-" * 40)
        
        changes_shown = 0
        for i, (orig, proc) in enumerate(zip(original_lines, processed_lines)):
            if orig != proc and changes_shown < 10:
                print(f"Line {i+1}:")
                print(f"  - {orig.strip()}")
                print(f"  + {proc.strip()}")
                print()
                changes_shown += 1
        
        if changes_shown == 10:
            print("... (showing first 10 changes)")
            
    except Exception as e:
        print(f"❌ Error previewing file: {e}")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Fix SQL files for PostgreSQL compatibility")
    parser.add_argument("--dry-run", action="store_true", 
                       help="Show what would be changed without modifying files")
    parser.add_argument("--apply", action="store_true",
                       help="Actually apply the changes")
    parser.add_argument("--preview", type=str, metavar="FILE",
                       help="Preview changes for a specific file")
    
    args = parser.parse_args()
    
    if args.preview:
        preview_file = Path(args.preview)
        if not preview_file.is_absolute():
            preview_file = project_root / preview_file
        if not preview_file.exists():
            print(f"❌ File not found: {preview_file}")
            sys.exit(1)
        show_preview(preview_file)
    elif args.apply:
        process_sql_files(dry_run=False)
    else:
        # Default to dry run
        process_sql_files(dry_run=True)


if __name__ == "__main__":
    main() 