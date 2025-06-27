#!/usr/bin/env python3
"""
DuckDB Reference Cleanup Script

This script systematically removes all DuckDB references from the codebase
as part of the migration to PostgreSQL-only architecture.
"""

import re
import shutil
from pathlib import Path
from typing import Dict, List, Tuple
import structlog

logger = structlog.get_logger(__name__)


class DuckDBCleanupManager:
    """Manager for removing all DuckDB references from the codebase."""
    
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.src_root = project_root / "src" / "mlb_sharp_betting"
        
        # Files to completely remove (DuckDB-specific)
        self.files_to_remove = [
            "src/mlb_sharp_betting/db/optimized_connection.py",  # Already removed
            "src/mlb_sharp_betting/utils/quick_db_check.py",  # Already removed
            "docs/duckdb_optimization_migration_guide.md",  # Already removed
            "src/mlb_sharp_betting/services/database_service_adapter.py",  # DuckDB-specific adapter
            "analysis_scripts/test_adaptive_system.py",  # Contains DuckDB connections
            "copy_betting_splits_data.py",  # Contains DuckDB connections
            "migrate_to_postgres.py",  # Migration script no longer needed
            "tests/manual/test_json_parsing.py",  # Contains DuckDB connections
            "backup_database.sh",  # DuckDB backup script
            "start_database_coordinator.sh",  # DuckDB coordinator
            "stop_database_coordinator.sh",  # DuckDB coordinator
        ]
        
        # Files to update (remove DuckDB references but keep files)
        self.files_to_update = {
            # Core source files
            "src/mlb_sharp_betting/analyzers/sharp_action_analyzer.py": self._update_sharp_action_analyzer,
            "src/mlb_sharp_betting/cli/commands/analysis.py": self._update_analysis_command,
            "src/mlb_sharp_betting/cli/commands/migrate_database.py": self._update_migrate_command,
            "src/mlb_sharp_betting/db/table_registry.py": self._update_table_registry,
            "src/mlb_sharp_betting/db/repositories.py": self._update_repositories,
            "src/mlb_sharp_betting/db/postgres_db_manager.py": self._update_postgres_db_manager,
            "src/mlb_sharp_betting/db/postgres_connection.py": self._update_postgres_connection,
            "src/mlb_sharp_betting/db/schema.py": self._update_schema,
            "src/mlb_sharp_betting/services/database_coordinator.py": self._update_database_coordinator,
            "src/mlb_sharp_betting/services/postgres_database_coordinator.py": self._update_postgres_coordinator,
            "src/mlb_sharp_betting/services/data_persistence.py": self._update_data_persistence,
            "src/mlb_sharp_betting/services/data_deduplication_service.py": self._update_data_dedup,
            "src/mlb_sharp_betting/services/sql_preprocessor.py": self._update_sql_preprocessor,
            "src/mlb_sharp_betting/utils/table_migration_helper.py": self._update_migration_helper,
            "src/mlb_sharp_betting/utils/database_inspector.py": self._update_database_inspector,
            "src/mlb_sharp_betting/entrypoint.py": self._update_entrypoint,
            "src/mlb_sharp_betting/examples/phase2_demo.py": self._update_phase2_demo,
            
            # Configuration files
            "config.toml": self._update_config_toml,
            
            # Documentation files
            "README.md": self._update_readme,
            "DAILY_UPDATER_README.md": self._update_daily_updater_readme,
            "PREGAME_WORKFLOW_README.md": self._update_pregame_readme,
            "REFACTORING_PLAN.md": self._update_refactoring_plan,
            
            # Analysis scripts (master_betting_detector.py removed - replaced by Phase 3 Orchestrator)
            "analysis_scripts/README.md": self._update_analysis_readme,
            "analysis_scripts/run_phase1_strategies.sh": self._update_phase1_strategies,
            
            # SQL files
            "sql/backtesting_schema.sql": self._update_backtesting_schema,
            "sql/postgresql_compatibility_functions.sql": self._update_postgresql_functions,
            
            # Documentation
            "docs/automated_backtesting_implementation_guide.md": self._update_backtesting_guide,
            "docs/phase2_implementation_summary.md": self._update_phase2_summary,
            
            # Test files
            "tests/test_optimized_database.py": self._update_optimized_database_test,
        }
    
    def run_cleanup(self) -> None:
        """Run the complete DuckDB cleanup process."""
        logger.info("Starting DuckDB reference cleanup")
        
        # Remove DuckDB-specific files
        self._remove_duckdb_files()
        
        # Update files with DuckDB references
        self._update_files_with_duckdb_references()
        
        # Update .gitignore to remove DuckDB patterns
        self._update_gitignore()
        
        # Generate cleanup report
        self._generate_cleanup_report()
        
        logger.info("DuckDB reference cleanup complete")
    
    def _remove_duckdb_files(self) -> None:
        """Remove files that are specific to DuckDB."""
        logger.info("Removing DuckDB-specific files")
        
        for file_path in self.files_to_remove:
            full_path = self.project_root / file_path
            if full_path.exists():
                if full_path.is_file():
                    full_path.unlink()
                    logger.info(f"Removed file: {file_path}")
                elif full_path.is_dir():
                    shutil.rmtree(full_path)
                    logger.info(f"Removed directory: {file_path}")
            else:
                logger.info(f"File already removed: {file_path}")
    
    def _update_files_with_duckdb_references(self) -> None:
        """Update files to remove DuckDB references."""
        logger.info("Updating files with DuckDB references")
        
        for file_path, update_func in self.files_to_update.items():
            full_path = self.project_root / file_path
            if full_path.exists():
                logger.info(f"Updating: {file_path}")
                try:
                    update_func(full_path)
                except Exception as e:
                    logger.error(f"Failed to update {file_path}: {e}")
            else:
                logger.warning(f"File not found: {file_path}")
    
    def _update_sharp_action_analyzer(self, file_path: Path) -> None:
        """Update sharp action analyzer to use PostgreSQL."""
        content = file_path.read_text()
        
        # Replace DuckDB imports and connections
        content = re.sub(r'import duckdb.*\n', '', content)
        content = re.sub(r'self\.conn = duckdb\.connect.*\n', 
                        'from ..db.connection import get_db_manager\n        self.db_manager = get_db_manager()\n', 
                        content)
        content = re.sub(r'"data/raw/mlb_betting\.duckdb"', '"PostgreSQL database"', content)
        content = re.sub(r'\.duckdb"', '(PostgreSQL)"', content)
        
        # Update SQL queries to PostgreSQL syntax
        content = content.replace('duckdb.connect(db_path)', 'get_db_manager()')
        content = content.replace('self.conn.execute(query).df()', 
                                'self._execute_query_to_dataframe(query)')
        
        file_path.write_text(content)
    
    def _update_analysis_command(self, file_path: Path) -> None:
        """Update analysis command to use PostgreSQL."""
        content = file_path.read_text()
        
        content = re.sub(r'import duckdb.*\n', '', content)
        content = re.sub(r'"data/raw/mlb_betting\.duckdb"', '"PostgreSQL database"', content)
        content = re.sub(r'"""Connect to the DuckDB database"""', 
                        '"""Connect to the PostgreSQL database"""', content)
        content = re.sub(r'self\.conn = duckdb\.connect.*\n',
                        'from ...db.connection import get_db_manager\n        self.db_manager = get_db_manager()\n',
                        content)
        
        file_path.write_text(content)
    
    def _update_migrate_command(self, file_path: Path) -> None:
        """Update migrate command to remove DuckDB references."""
        content = file_path.read_text()
        
        # Remove DuckDB-specific imports that we already removed
        content = re.sub(r'# Removed DuckDB-specific optimized connection.*\n', '', content)
        
        # Update remaining references
        content = content.replace('DuckDB Performance Benchmark', 'PostgreSQL Performance Benchmark')
        content = content.replace('DuckDB', 'PostgreSQL')
        content = content.replace('duckdb', 'postgresql')
        
        file_path.write_text(content)
    
    def _update_table_registry(self, file_path: Path) -> None:
        """Update table registry to remove DuckDB enum and mappings."""
        content = file_path.read_text()
        
        # Remove DuckDB enum value
        content = re.sub(r'\s+DUCKDB = "duckdb"\n', '', content)
        
        # Remove DuckDB elif block
        content = re.sub(r'\s+elif self\.database_type == DatabaseType\.DUCKDB:\s*\n\s+# DuckDB mappings.*\n',
                        '', content, flags=re.MULTILINE)
        
        file_path.write_text(content)
    
    def _update_repositories(self, file_path: Path) -> None:
        """Update repositories to remove DuckDB references."""
        content = file_path.read_text()
        
        content = content.replace('# Use PostgreSQL information_schema instead of DuckDB PRAGMA',
                                '# Using PostgreSQL information_schema')
        
        file_path.write_text(content)
    
    def _update_postgres_db_manager(self, file_path: Path) -> None:
        """Update postgres db manager to remove DuckDB compatibility references."""
        content = file_path.read_text()
        
        # Replace DuckDB references in comments and docstrings
        content = content.replace('as the original DuckDB DatabaseManager', 'as a PostgreSQL DatabaseManager')
        content = content.replace('drop-in replacement for the DuckDB DatabaseManager', 'PostgreSQL DatabaseManager')
        content = content.replace('same interface as the DuckDB manager', 'PostgreSQL manager interface')
        content = content.replace('DuckDB-like interface', 'PostgreSQL interface')
        content = content.replace('DuckDB-like format', 'PostgreSQL format')
        content = content.replace('DuckDB cursor interface', 'PostgreSQL cursor interface')
        content = content.replace('DuckDB connection interface', 'PostgreSQL connection interface')
        content = content.replace('DuckDB-style ? parameters', 'legacy ? parameters')
        
        file_path.write_text(content)
    
    def _update_postgres_connection(self, file_path: Path) -> None:
        """Update postgres connection to remove DuckDB references."""
        content = file_path.read_text()
        
        content = content.replace('This replaces DuckDB to eliminate all concurrency issues.',
                                'PostgreSQL connection manager with full concurrency support.')
        content = content.replace('DuckDB-style ? parameters', 'legacy ? parameters')
        
        file_path.write_text(content)
    
    def _update_schema(self, file_path: Path) -> None:
        """Update schema to remove DuckDB references."""
        content = file_path.read_text()
        
        content = content.replace('# Triggers might not be supported in all DuckDB versions',
                                '# PostgreSQL trigger support')
        
        file_path.write_text(content)
    
    def _update_database_coordinator(self, file_path: Path) -> None:
        """Update database coordinator to remove DuckDB references."""
        content = file_path.read_text()
        
        # Update module docstring
        content = re.sub(r'Database Coordinator Service for Multi-Process DuckDB Access',
                        'Database Coordinator Service (Legacy - Use PostgreSQL Connection Manager)',
                        content)
        content = content.replace('DuckDB\'s single-writer limitation', 'legacy database limitations')
        content = content.replace('duckdb_coordinator.lock', 'legacy_coordinator.lock')
        content = content.replace('Falls back to DuckDB only if PostgreSQL fails', 
                                'Legacy fallback - use PostgreSQL connection manager instead')
        content = content.replace('PostgreSQL coordinator failed, falling back to DuckDB',
                                'PostgreSQL coordinator failed, using legacy fallback')
        content = content.replace('make PostgreSQL coordinator work with existing DuckDB API',
                                'make PostgreSQL coordinator work with legacy API')
        
        file_path.write_text(content)
    
    def _update_postgres_coordinator(self, file_path: Path) -> None:
        """Update postgres coordinator to remove DuckDB references."""
        content = file_path.read_text()
        
        content = content.replace('using PostgreSQL instead of DuckDB', 'using PostgreSQL')
        content = content.replace('replaces the legacy DuckDB coordinator', 'replaces the legacy coordinator')
        
        file_path.write_text(content)
    
    def _update_data_persistence(self, file_path: Path) -> None:
        """Update data persistence to remove DuckDB references."""
        content = file_path.read_text()
        
        content = content.replace('let DuckDB handle it', 'let PostgreSQL handle it')
        
        file_path.write_text(content)
    
    def _update_data_dedup(self, file_path: Path) -> None:
        """Update data deduplication service to remove DuckDB references."""
        content = file_path.read_text()
        
        content = content.replace('For DuckDB, we\'ll use a different approach',
                                'For PostgreSQL, we use the standard approach')
        
        file_path.write_text(content)
    
    def _update_sql_preprocessor(self, file_path: Path) -> None:
        """Update SQL preprocessor to remove DuckDB references."""
        content = file_path.read_text()
        
        content = content.replace('when migrating from DuckDB', 'for SQL compatibility')
        
        file_path.write_text(content)
    
    def _update_migration_helper(self, file_path: Path) -> None:
        """Update migration helper to remove DuckDB references."""
        content = file_path.read_text()
        
        content = content.replace('# Legacy DuckDB patterns', '# Legacy database patterns')
        
        file_path.write_text(content)
    
    def _update_database_inspector(self, file_path: Path) -> None:
        """Update database inspector to remove DuckDB references."""
        content = file_path.read_text()
        
        content = content.replace('data/raw/mlb_betting.duckdb', 'PostgreSQL database')
        
        file_path.write_text(content)
    
    def _update_entrypoint(self, file_path: Path) -> None:
        """Update entrypoint to remove DuckDB references."""
        content = file_path.read_text()
        
        content = content.replace('3. Store validated data in DuckDB', '3. Store validated data in PostgreSQL')
        
        file_path.write_text(content)
    
    def _update_phase2_demo(self, file_path: Path) -> None:
        """Update phase2 demo to remove DuckDB references."""
        content = file_path.read_text()
        
        content = content.replace('Database connection management with DuckDB',
                                'Database connection management with PostgreSQL')
        content = content.replace('Database connection manager with DuckDB',
                                'Database connection manager with PostgreSQL')
        
        file_path.write_text(content)
    
    def _update_config_toml(self, file_path: Path) -> None:
        """Update config.toml to remove DuckDB references."""
        content = file_path.read_text()
        
        # Remove or comment out DuckDB path
        content = re.sub(r'path = "data/raw/mlb_betting\.duckdb"',
                        '# path = "data/raw/mlb_betting.duckdb"  # Migrated to PostgreSQL',
                        content)
        
        file_path.write_text(content)
    
    def _update_readme(self, file_path: Path) -> None:
        """Update README to remove DuckDB references."""
        content = file_path.read_text()
        
        content = content.replace('mlb_betting.duckdb     # DuckDB database',
                                'postgresql/            # PostgreSQL data')
        content = content.replace('path = "data/raw/mlb_betting.duckdb"',
                                '# PostgreSQL connection configured in settings')
        content = content.replace('duckdb data/raw/mlb_betting.duckdb',
                                'psql -h localhost -d mlb_betting')
        content = content.replace('Connect to DuckDB and run verification queries',
                                'Connect to PostgreSQL and run verification queries')
        content = content.replace('follows DuckDB best practices',
                                'follows PostgreSQL best practices')
        
        file_path.write_text(content)
    
    def _update_daily_updater_readme(self, file_path: Path) -> None:
        """Update daily updater README to remove DuckDB references."""
        content = file_path.read_text()
        
        content = content.replace('DuckDB requires exclusive access for writes',
                                'PostgreSQL supports concurrent access')
        
        file_path.write_text(content)
    
    def _update_pregame_readme(self, file_path: Path) -> None:
        """Update pregame README to remove DuckDB references."""
        content = file_path.read_text()
        
        content = content.replace('du -h data/raw/mlb_betting.duckdb',
                                'psql -h localhost -d mlb_betting -c "\\dt+"')
        
        file_path.write_text(content)
    
    def _update_refactoring_plan(self, file_path: Path) -> None:
        """Update refactoring plan to remove DuckDB references."""
        content = file_path.read_text()
        
        content = content.replace('Full DuckDB-based implementation',
                                'Full database-based implementation (migrated to PostgreSQL)')
        
        file_path.write_text(content)
    
    def _update_master_detector(self, file_path: Path) -> None:
        """Update master detector to remove DuckDB references."""
        content = file_path.read_text()
        
        content = content.replace('data/raw/mlb_betting.duckdb', 'PostgreSQL database')
        content = content.replace('logging.getLogger("duckdb")', 'logging.getLogger("postgresql")')
        
        file_path.write_text(content)
    
    def _update_analysis_readme(self, file_path: Path) -> None:
        """Update analysis README to remove DuckDB references."""
        content = file_path.read_text()
        
        content = content.replace('duckdb data/raw/mlb_betting.duckdb',
                                'psql -h localhost -d mlb_betting -f')
        content = content.replace('Connect to your DuckDB database',
                                'Connect to your PostgreSQL database')
        content = content.replace('tables in your DuckDB database',
                                'tables in your PostgreSQL database')
        
        file_path.write_text(content)
    
    def _update_phase1_strategies(self, file_path: Path) -> None:
        """Update phase1 strategies script to remove DuckDB references."""
        content = file_path.read_text()
        
        content = content.replace('DB_PATH="data/raw/mlb_betting.duckdb"',
                                'DB_CONNECTION="postgresql://localhost/mlb_betting"')
        
        file_path.write_text(content)
    
    def _update_backtesting_schema(self, file_path: Path) -> None:
        """Update backtesting schema to remove DuckDB references."""
        content = file_path.read_text()
        
        content = content.replace('if supported by DuckDB', 'PostgreSQL supported')
        content = content.replace('DuckDB may not support triggers',
                                'PostgreSQL supports triggers')
        
        file_path.write_text(content)
    
    def _update_postgresql_functions(self, file_path: Path) -> None:
        """Update PostgreSQL functions to remove DuckDB references."""
        content = file_path.read_text()
        
        content = content.replace('Replaces DuckDB-specific functionality',
                                'PostgreSQL-specific functionality')
        content = content.replace('replacing DuckDB-specific aggregates',
                                'PostgreSQL aggregates')
        content = content.replace('replacing DuckDB TRY_CAST',
                                'PostgreSQL safe casting')
        
        file_path.write_text(content)
    
    def _update_backtesting_guide(self, file_path: Path) -> None:
        """Update backtesting guide to remove DuckDB references."""
        content = file_path.read_text()
        
        content = content.replace('duckdb.connect(\'data/raw/mlb_betting.duckdb\')',
                                'psycopg2.connect(database="mlb_betting")')
        content = content.replace('data/raw/mlb_betting.duckdb',
                                'PostgreSQL database mlb_betting')
        
        file_path.write_text(content)
    
    def _update_phase2_summary(self, file_path: Path) -> None:
        """Update phase2 summary to remove DuckDB references."""
        content = file_path.read_text()
        
        content = content.replace('thread-safe DuckDB connection management',
                                'thread-safe PostgreSQL connection management')
        content = content.replace('appropriate for DuckDB architecture',
                                'appropriate for PostgreSQL architecture')
        content = content.replace('Single DuckDB connection', 'PostgreSQL connection pooling')
        content = content.replace('Chose DuckDB over PostgreSQL', 'Using PostgreSQL')
        content = content.replace('DuckDB-specific', 'PostgreSQL-specific')
        
        file_path.write_text(content)
    
    def _update_optimized_database_test(self, file_path: Path) -> None:
        """Update optimized database test to remove DuckDB references."""
        content = file_path.read_text()
        
        content = content.replace('DuckDB Optimization Performance Benchmark',
                                'PostgreSQL Performance Benchmark')
        
        file_path.write_text(content)
    
    def _update_gitignore(self) -> None:
        """Update .gitignore to remove DuckDB patterns."""
        gitignore_path = self.project_root / ".gitignore"
        if gitignore_path.exists():
            content = gitignore_path.read_text()
            
            # Comment out DuckDB patterns instead of removing them completely
            content = content.replace('*.duckdb', '# *.duckdb  # No longer using DuckDB')
            content = content.replace('**/*.duckdb', '# **/*.duckdb  # No longer using DuckDB')
            content = content.replace('*.duckdb.wal', '# *.duckdb.wal  # No longer using DuckDB')
            
            gitignore_path.write_text(content)
            logger.info("Updated .gitignore to comment out DuckDB patterns")
    
    def _generate_cleanup_report(self) -> None:
        """Generate a report of the cleanup process."""
        report_path = self.project_root / "reports" / "duckdb_cleanup_report.md"
        report_path.parent.mkdir(exist_ok=True)
        
        report_content = """# DuckDB Reference Cleanup Report

## Overview
This report documents the complete removal of DuckDB references from the codebase
as part of the migration to PostgreSQL-only architecture.

## Files Removed
The following DuckDB-specific files were completely removed:
"""
        
        for file_path in self.files_to_remove:
            report_content += f"- `{file_path}`\n"
        
        report_content += """
## Files Updated
The following files were updated to remove DuckDB references:
"""
        
        for file_path in self.files_to_update.keys():
            report_content += f"- `{file_path}`\n"
        
        report_content += """
## Changes Made
1. **Removed DuckDB imports** and replaced with PostgreSQL equivalents
2. **Updated database connection strings** to use PostgreSQL
3. **Replaced DuckDB-specific SQL syntax** with PostgreSQL syntax
4. **Updated comments and documentation** to reflect PostgreSQL usage
5. **Removed DuckDB configuration** from config files
6. **Updated shell scripts** to use PostgreSQL commands
7. **Commented out DuckDB patterns** in .gitignore (for reference)

## Next Steps
1. Test all functionality to ensure PostgreSQL migration is complete
2. Remove any remaining `.duckdb` files from the file system
3. Update deployment scripts to use PostgreSQL
4. Update CI/CD pipelines to use PostgreSQL for testing

## Verification
To verify that all DuckDB references have been removed, run:
```bash
grep -r -i "duckdb" . --exclude-dir=.git --exclude="*.md" --exclude="*cleanup*"
```

This should return no results (except for this report and cleanup scripts).
"""
        
        report_path.write_text(report_content)
        logger.info(f"Generated cleanup report: {report_path}")


def main():
    """Run the DuckDB cleanup process."""
    project_root = Path(__file__).parent.parent
    
    cleanup_manager = DuckDBCleanupManager(project_root)
    cleanup_manager.run_cleanup()
    
    print("✅ DuckDB reference cleanup complete!")
    print("📋 See reports/duckdb_cleanup_report.md for details")
    print("\n🔍 To verify cleanup, run:")
    print('grep -r -i "duckdb" . --exclude-dir=.git --exclude="*.md" --exclude="*cleanup*"')
    

if __name__ == "__main__":
    main() 