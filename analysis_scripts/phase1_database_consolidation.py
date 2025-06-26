#!/usr/bin/env python3
"""
Phase 1: Database Layer Consolidation Script

This script helps consolidate the multiple database connection implementations
into a single, unified PostgreSQL connection manager.

Steps:
1. Audit all database connection files
2. Identify import dependencies
3. Create migration mapping
4. Generate consolidation report
"""

import ast
import re
from pathlib import Path
from typing import Dict, List, Set, Tuple
import structlog

logger = structlog.get_logger(__name__)


class DatabaseConsolidationAnalyzer:
    """Analyzer for database consolidation planning."""
    
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.src_root = project_root / "src" / "mlb_sharp_betting"
        
        # Database files to analyze
        self.db_files = {
            "connection.py": self.src_root / "db" / "connection.py",
            "postgres_connection.py": self.src_root / "db" / "postgres_connection.py", 
            "optimized_connection.py": self.src_root / "db" / "optimized_connection.py",
            "postgres_db_manager.py": self.src_root / "db" / "postgres_db_manager.py"
        }
        
        # Service files that use database connections
        self.service_files = {
            "database_coordinator.py": self.src_root / "services" / "database_coordinator.py",
            "postgres_database_coordinator.py": self.src_root / "services" / "postgres_database_coordinator.py",
            "database_service_adapter.py": self.src_root / "services" / "database_service_adapter.py",
            "data_persistence.py": self.src_root / "services" / "data_persistence.py"
        }
        
        self.analysis_results = {}
    
    def analyze_file(self, file_path: Path) -> Dict:
        """Analyze a Python file for database-related patterns."""
        if not file_path.exists():
            return {"error": "File does not exist", "exists": False}
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Parse AST
            tree = ast.parse(content)
            
            analysis = {
                "exists": True,
                "line_count": len(content.splitlines()),
                "classes": [],
                "functions": [],
                "imports": [],
                "database_patterns": {
                    "postgresql": [],
                    "duckdb": [],
                    "connection_pool": [],
                    "threading": [],
                    "async": [],
                    "transaction": []
                }
            }
            
            # Extract classes and functions
            for node in ast.walk(tree):
                if isinstance(node, ast.ClassDef):
                    analysis["classes"].append({
                        "name": node.name,
                        "line": node.lineno,
                        "methods": [n.name for n in node.body if isinstance(n, ast.FunctionDef)]
                    })
                elif isinstance(node, ast.FunctionDef) and not any(
                    node.lineno >= cls_node.lineno and node.lineno <= getattr(cls_node, 'end_lineno', float('inf'))
                    for cls_node in ast.walk(tree) if isinstance(cls_node, ast.ClassDef)
                ):
                    analysis["functions"].append({
                        "name": node.name,
                        "line": node.lineno,
                        "is_async": isinstance(node, ast.AsyncFunctionDef)
                    })
                elif isinstance(node, (ast.Import, ast.ImportFrom)):
                    if isinstance(node, ast.Import):
                        for alias in node.names:
                            analysis["imports"].append(alias.name)
                    else:
                        module = node.module or ""
                        for alias in node.names:
                            analysis["imports"].append(f"{module}.{alias.name}")
            
            # Search for specific patterns in content
            patterns = {
                "postgresql": [
                    r"psycopg2", r"PostgreSQL", r"postgres", r"pg_", 
                    r"ThreadedConnectionPool", r"RealDictCursor"
                ],
                "duckdb": [
                    r"duckdb", r"DuckDB", r"\.duckdb", r"PRAGMA"
                ],
                "connection_pool": [
                    r"connection.*pool", r"ThreadedConnectionPool", r"QueuePool",
                    r"getconn", r"putconn", r"pool_size"
                ],
                "threading": [
                    r"threading\.", r"_lock", r"RLock", r"Lock\(\)", r"thread.*safe"
                ],
                "async": [
                    r"async def", r"await ", r"asyncio", r"@asynccontextmanager"
                ],
                "transaction": [
                    r"\.commit\(\)", r"\.rollback\(\)", r"transaction", r"BEGIN", r"COMMIT"
                ]
            }
            
            for category, pattern_list in patterns.items():
                for pattern in pattern_list:
                    matches = re.finditer(pattern, content, re.IGNORECASE)
                    analysis["database_patterns"][category].extend([
                        {"pattern": pattern, "line": content[:m.start()].count('\n') + 1}
                        for m in matches
                    ])
            
            return analysis
            
        except Exception as e:
            return {"error": str(e), "exists": True}
    
    def find_import_dependencies(self) -> Dict[str, List[str]]:
        """Find which files import database modules."""
        dependencies = {}
        
        # Search all Python files in the project
        for py_file in self.src_root.rglob("*.py"):
            if py_file.name.startswith("__"):
                continue
                
            try:
                with open(py_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Look for imports of database modules
                db_imports = []
                
                patterns = [
                    r"from.*\.db\.(\w+)",
                    r"from.*\.services\.(database_\w+|postgres_database_\w+|data_persistence)",
                    r"import.*\.db\.(\w+)",
                ]
                
                for pattern in patterns:
                    matches = re.finditer(pattern, content)
                    for match in matches:
                        db_imports.append(match.group(0))
                
                if db_imports:
                    rel_path = py_file.relative_to(self.src_root)
                    dependencies[str(rel_path)] = db_imports
                    
            except Exception as e:
                logger.warning(f"Error analyzing {py_file}: {e}")
        
        return dependencies
    
    def run_analysis(self) -> Dict:
        """Run complete database consolidation analysis."""
        logger.info("Starting database consolidation analysis")
        
        results = {
            "db_files": {},
            "service_files": {},
            "dependencies": {},
            "consolidation_plan": {},
            "summary": {}
        }
        
        # Analyze database files
        logger.info("Analyzing database files")
        for name, path in self.db_files.items():
            logger.info(f"Analyzing {name}")
            results["db_files"][name] = self.analyze_file(path)
        
        # Analyze service files
        logger.info("Analyzing service files")
        for name, path in self.service_files.items():
            logger.info(f"Analyzing {name}")
            results["service_files"][name] = self.analyze_file(path)
        
        # Find dependencies
        logger.info("Finding import dependencies")
        results["dependencies"] = self.find_import_dependencies()
        
        # Generate consolidation plan
        results["consolidation_plan"] = self._generate_consolidation_plan(results)
        
        # Generate summary
        results["summary"] = self._generate_summary(results)
        
        return results
    
    def _generate_consolidation_plan(self, results: Dict) -> Dict:
        """Generate specific consolidation recommendations."""
        plan = {
            "primary_implementation": "connection.py",
            "rationale": "Most comprehensive PostgreSQL implementation with connection pooling",
            "files_to_merge": [],
            "files_to_eliminate": [],
            "migration_steps": [],
            "risk_assessment": {}
        }
        
        # Identify files to merge vs eliminate
        for name, analysis in results["db_files"].items():
            if not analysis.get("exists", False):
                continue
                
            if name == "connection.py":
                continue  # This is our primary implementation
            
            if analysis.get("line_count", 0) > 100:
                plan["files_to_merge"].append({
                    "file": name,
                    "reason": f"Substantial implementation ({analysis['line_count']} lines)",
                    "classes": [cls["name"] for cls in analysis.get("classes", [])],
                    "key_features": self._identify_key_features(analysis)
                })
            else:
                plan["files_to_eliminate"].append({
                    "file": name,
                    "reason": f"Small implementation ({analysis.get('line_count', 0)} lines)",
                    "classes": [cls["name"] for cls in analysis.get("classes", [])]
                })
        
        # Generate migration steps
        plan["migration_steps"] = [
            "1. Create backup branch for rollback safety",
            "2. Audit connection.py as primary implementation",
            "3. Extract useful features from postgres_connection.py (SQLAlchemy integration)",
            "4. Merge PostgreSQL compatibility wrappers from postgres_db_manager.py",
            "5. Update all service imports to use unified connection manager",
            "6. Remove optimized_connection.py (DuckDB-specific, no longer needed)",
            "7. Consolidate database coordinators into single service",
            "8. Update CLI commands and tests",
            "9. Performance testing and validation"
        ]
        
        # Risk assessment
        plan["risk_assessment"] = {
            "high_risk": [
                "All database operations depend on connection layer",
                "Multiple services use different connection managers",
                "Potential for connection pool conflicts during migration"
            ],
            "medium_risk": [
                "Import path changes require careful update",
                "Different connection interfaces may cause compatibility issues"
            ],
            "low_risk": [
                "Most implementations are PostgreSQL-based",
                "Connection pooling patterns are similar"
            ],
            "mitigation": [
                "Phase migration with feature flags",
                "Comprehensive testing at each step", 
                "Database backup before major changes",
                "Keep old implementations until validation complete"
            ]
        }
        
        return plan
    
    def _identify_key_features(self, analysis: Dict) -> List[str]:
        """Identify key features from file analysis."""
        features = []
        
        patterns = analysis.get("database_patterns", {})
        
        if patterns.get("connection_pool"):
            features.append("Connection pooling")
        if patterns.get("async"):
            features.append("Async operations")  
        if patterns.get("threading"):
            features.append("Thread safety")
        if patterns.get("transaction"):
            features.append("Transaction management")
        
        # Check for SQLAlchemy
        imports = analysis.get("imports", [])
        if any("sqlalchemy" in imp.lower() for imp in imports):
            features.append("SQLAlchemy integration")
        
        return features
    
    def _generate_summary(self, results: Dict) -> Dict:
        """Generate analysis summary."""
        existing_files = [
            name for name, analysis in results["db_files"].items() 
            if analysis.get("exists", False)
        ]
        
        total_lines = sum(
            analysis.get("line_count", 0) 
            for analysis in results["db_files"].values() 
            if analysis.get("exists", False)
        )
        
        service_files = [
            name for name, analysis in results["service_files"].items()
            if analysis.get("exists", False)
        ]
        
        dependent_files = len(results["dependencies"])
        
        return {
            "database_files_found": len(existing_files),
            "database_files": existing_files,
            "total_database_lines": total_lines,
            "service_files_found": len(service_files),
            "service_files": service_files,
            "dependent_files": dependent_files,
            "estimated_effort": "2-3 days for Phase 1 database consolidation",
            "estimated_reduction": f"~{total_lines * 0.4:.0f} lines (40% reduction expected)"
        }
    
    def generate_report(self) -> str:
        """Generate human-readable consolidation report."""
        results = self.run_analysis()
        
        report = []
        report.append("# Database Layer Consolidation Analysis Report")
        report.append("")
        
        # Summary
        summary = results["summary"]
        report.append("## Summary")
        report.append(f"- **Database files found**: {summary['database_files_found']}")
        report.append(f"- **Total lines of database code**: {summary['total_database_lines']}")
        report.append(f"- **Service files using database**: {summary['service_files_found']}")
        report.append(f"- **Files with database dependencies**: {summary['dependent_files']}")
        report.append(f"- **Estimated effort**: {summary['estimated_effort']}")
        report.append(f"- **Estimated code reduction**: {summary['estimated_reduction']}")
        report.append("")
        
        # Database files analysis
        report.append("## Database Files Analysis")
        for name, analysis in results["db_files"].items():
            if not analysis.get("exists", False):
                report.append(f"### {name} (NOT FOUND)")
                continue
                
            report.append(f"### {name}")
            report.append(f"- **Lines**: {analysis.get('line_count', 0)}")
            report.append(f"- **Classes**: {', '.join([cls['name'] for cls in analysis.get('classes', [])])}")
            
            patterns = analysis.get("database_patterns", {})
            if patterns.get("postgresql"):
                report.append("- **PostgreSQL**: ✅")
            if patterns.get("duckdb"):
                report.append("- **DuckDB**: ⚠️ (needs migration)")
            if patterns.get("connection_pool"):
                report.append("- **Connection pooling**: ✅")
            if patterns.get("async"):
                report.append("- **Async support**: ✅")
            if patterns.get("threading"):
                report.append("- **Thread safety**: ✅")
            report.append("")
        
        # Consolidation plan
        plan = results["consolidation_plan"]
        report.append("## Consolidation Plan")
        report.append(f"**Primary implementation**: {plan['primary_implementation']}")
        report.append(f"**Rationale**: {plan['rationale']}")
        report.append("")
        
        if plan["files_to_merge"]:
            report.append("### Files to merge features from:")
            for file_info in plan["files_to_merge"]:
                report.append(f"- **{file_info['file']}**: {file_info['reason']}")
                if file_info["key_features"]:
                    report.append(f"  - Features: {', '.join(file_info['key_features'])}")
            report.append("")
        
        if plan["files_to_eliminate"]:
            report.append("### Files to eliminate:")
            for file_info in plan["files_to_eliminate"]:
                report.append(f"- **{file_info['file']}**: {file_info['reason']}")
            report.append("")
        
        # Migration steps
        report.append("### Migration Steps")
        for step in plan["migration_steps"]:
            report.append(f"- {step}")
        report.append("")
        
        # Risk assessment
        risk = plan["risk_assessment"]
        report.append("## Risk Assessment")
        
        if risk["high_risk"]:
            report.append("### High Risk ⚠️")
            for item in risk["high_risk"]:
                report.append(f"- {item}")
            report.append("")
        
        if risk["mitigation"]:
            report.append("### Mitigation Strategies")
            for item in risk["mitigation"]:
                report.append(f"- {item}")
            report.append("")
        
        # Dependencies
        deps = results["dependencies"]
        if deps:
            report.append("## Import Dependencies")
            report.append("Files that import database modules (need update):")
            for file_path, imports in deps.items():
                report.append(f"- **{file_path}**:")
                for imp in imports:
                    report.append(f"  - `{imp}`")
            report.append("")
        
        return "\n".join(report)


def main():
    """Run database consolidation analysis."""
    project_root = Path(__file__).parent.parent
    analyzer = DatabaseConsolidationAnalyzer(project_root)
    
    try:
        report = analyzer.generate_report()
        
        # Save report
        report_path = project_root / "reports" / "database_consolidation_analysis.md"
        report_path.parent.mkdir(exist_ok=True)
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report)
        
        print(f"Database consolidation analysis complete!")
        print(f"Report saved to: {report_path}")
        print("\nNext steps:")
        print("1. Review the analysis report")
        print("2. Create backup branch: git checkout -b database-consolidation-backup")
        print("3. Begin Phase 1 implementation following the migration steps")
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        raise


if __name__ == "__main__":
    main() 