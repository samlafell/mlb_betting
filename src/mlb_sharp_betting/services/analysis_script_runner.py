"""
Analysis Script Runner

This service runs all analysis scripts from the analysis_scripts directory
using the centralized table registry and PostgreSQL compatibility.

Usage:
    from mlb_sharp_betting.services.analysis_script_runner import AnalysisScriptRunner
    
    runner = AnalysisScriptRunner()
    results = await runner.run_all_scripts()
"""

import asyncio
import json
from pathlib import Path
from typing import Dict, List, Optional, Any
import structlog
import time

from ..db.connection import get_db_manager
from ..db.table_registry import get_table_registry, DatabaseType
from .sql_preprocessor import SQLPreprocessor

logger = structlog.get_logger(__name__)


class AnalysisScriptRunner:
    """
    Runs analysis scripts with centralized table naming and PostgreSQL compatibility.
    """
    
    def __init__(self, database_type: DatabaseType = DatabaseType.POSTGRESQL):
        """Initialize the analysis script runner."""
        self.database_type = database_type
        self.db_manager = get_db_manager()
        self.table_registry = get_table_registry(database_type)
        self.sql_preprocessor = SQLPreprocessor(database_type)
        self.logger = logger.bind(component="analysis_script_runner")
        
        # Define the analysis scripts to run
        self.analysis_scripts = {
            "consensus_moneyline_strategy": "analysis_scripts/consensus_moneyline_strategy.sql",
            "strategy_comparison_roi": "analysis_scripts/strategy_comparison_roi.sql",
            "sharp_action_detector": "analysis_scripts/sharp_action_detector.sql", 
            "timing_based_strategy": "analysis_scripts/timing_based_strategy.sql",
            "hybrid_line_sharp_strategy": "analysis_scripts/hybrid_line_sharp_strategy.sql",
            "line_movement_strategy": "analysis_scripts/line_movement_strategy.sql",
            "signal_combinations": "analysis_scripts/signal_combinations.sql",
            "opposing_markets_strategy": "analysis_scripts/opposing_markets_strategy.sql",
            "public_money_fade_strategy": "analysis_scripts/public_money_fade_strategy.sql",
            "book_conflicts_strategy": "analysis_scripts/book_conflicts_strategy.sql",
            "executive_summary_report": "analysis_scripts/executive_summary_report.sql",
            "total_line_sweet_spots_strategy": "analysis_scripts/total_line_sweet_spots_strategy.sql",
            "underdog_ml_value_strategy": "analysis_scripts/underdog_ml_value_strategy.sql",
            "team_specific_bias_strategy": "analysis_scripts/team_specific_bias_strategy.sql"
        }
    
    def run_single_script(self, script_name: str) -> Dict[str, Any]:
        """Run a single analysis script."""
        if script_name not in self.analysis_scripts:
            raise ValueError(f"Script '{script_name}' not found")
        
        script_path = self.analysis_scripts[script_name]
        
        result = {
            'success': False,
            'data': [],
            'error': None,
            'preprocessing_stats': {},
            'original_sql_length': 0,
            'processed_sql_length': 0,
            'execution_time_seconds': 0.0
        }
        
        try:
            # Read the original SQL file
            full_path = Path(script_path)
            if not full_path.exists():
                raise FileNotFoundError(f"Script not found: {script_path}")
            
            original_sql = full_path.read_text()
            result['original_sql_length'] = len(original_sql)
            
            # Preprocess the SQL
            processed_sql = self.sql_preprocessor.process_sql_string(original_sql)
            result['processed_sql_length'] = len(processed_sql)
            
            # Get preprocessing statistics
            preprocessing_summary = self.sql_preprocessor.get_transformation_summary(
                original_sql, processed_sql
            )
            result['preprocessing_stats'] = preprocessing_summary
            
            # Execute the processed SQL
            start_time = time.time()
            
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(processed_sql)
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                
                # Convert to list of dictionaries
                data = [dict(zip(columns, row)) for row in rows]
                result['data'] = data
            
            result['execution_time_seconds'] = time.time() - start_time
            result['success'] = True
            
        except Exception as e:
            result['error'] = str(e)
            result['success'] = False
            
            # Add debugging info for failures
            if 'preprocessing_stats' in result:
                full_path = Path(script_path)
                if full_path.exists():
                    original_sql = full_path.read_text()
                    processed_sql = self.sql_preprocessor.process_sql_string(original_sql)
                    
                    result['debug_info'] = {
                        'original_sql_preview': original_sql[:1000] + "..." if len(original_sql) > 1000 else original_sql,
                        'processed_sql_preview': processed_sql[:1000] + "..." if len(processed_sql) > 1000 else processed_sql,
                        'validation_issues': preprocessing_summary.get('validation_issues', [])
                    }
        
        return result
    
    def run_all_scripts(self) -> Dict[str, Any]:
        """Run all analysis scripts and return comprehensive results."""
        results = {
            'script_results': {},
            'successful_scripts': [],
            'failed_scripts': [],
            'preprocessing_stats': {},
            'total_scripts': len(self.analysis_scripts),
            'execution_summary': {}
        }
        
        self.logger.info("Starting analysis script execution", 
                        total_scripts=len(self.analysis_scripts))
        
        for script_name, script_path in self.analysis_scripts.items():
            try:
                self.logger.info("Processing script", script=script_name)
                
                # Process the script
                script_result = self.run_single_script(script_name)
                
                results['script_results'][script_name] = script_result
                
                if script_result['success']:
                    results['successful_scripts'].append(script_name)
                    self.logger.info("Script executed successfully", 
                                   script=script_name,
                                   rows_returned=len(script_result['data']))
                else:
                    results['failed_scripts'].append(script_name)
                    self.logger.error("Script execution failed", 
                                    script=script_name,
                                    error=script_result['error'])
                
                # Store preprocessing stats
                results['preprocessing_stats'][script_name] = script_result['preprocessing_stats']
                
            except Exception as e:
                self.logger.error("Script processing failed", script=script_name, error=str(e))
                results['failed_scripts'].append(script_name)
                results['script_results'][script_name] = {
                    'success': False,
                    'error': str(e),
                    'data': [],
                    'preprocessing_stats': {}
                }
        
        # Generate execution summary
        results['execution_summary'] = {
            'successful_count': len(results['successful_scripts']),
            'failed_count': len(results['failed_scripts']),
            'success_rate': len(results['successful_scripts']) / len(self.analysis_scripts) * 100 if self.analysis_scripts else 0,
            'total_rows_returned': sum(
                len(result.get('data', [])) 
                for result in results['script_results'].values() 
                if result.get('success', False)
            )
        }
        
        self.logger.info("Analysis script execution completed",
                        successful=len(results['successful_scripts']),
                        failed=len(results['failed_scripts']),
                        success_rate=f"{results['execution_summary']['success_rate']:.1f}%")
        
        return results
    
    def get_script_status_report(self, results: Dict[str, Any]) -> str:
        """
        Generate a detailed status report for script execution.
        
        Args:
            results: Results from run_all_scripts()
            
        Returns:
            Formatted status report
        """
        report_lines = [
            "# 📊 ANALYSIS SCRIPT EXECUTION REPORT",
            "",
            "## 📈 Executive Summary",
            f"- **Total Scripts:** {results['total_scripts']}",
            f"- **Successful:** {results['execution_summary']['successful_count']}",
            f"- **Failed:** {results['execution_summary']['failed_count']}",
            f"- **Success Rate:** {results['execution_summary']['success_rate']:.1f}%",
            f"- **Total Rows Returned:** {results['execution_summary']['total_rows_returned']:,}",
            "",
            "## ✅ Successful Scripts"
        ]
        
        if results['successful_scripts']:
            for script_name in results['successful_scripts']:
                script_result = results['script_results'][script_name]
                preprocessing = script_result['preprocessing_stats']
                
                report_lines.extend([
                    f"### {script_name}",
                    f"- **Rows Returned:** {len(script_result['data']):,}",
                    f"- **Execution Time:** {script_result['execution_time_seconds']:.2f}s",
                    f"- **Table Replacements:** {preprocessing['table_replacements_applied']}",
                    f"- **Syntax Transformations:** {preprocessing['syntax_transformations_applied']}",
                    f"- **Validation Issues:** {len(preprocessing['validation_issues'])}",
                    ""
                ])
        else:
            report_lines.append("*No scripts executed successfully.*")
        
        report_lines.extend([
            "",
            "## ❌ Failed Scripts"
        ])
        
        if results['failed_scripts']:
            for script_name in results['failed_scripts']:
                script_result = results['script_results'][script_name]
                
                report_lines.extend([
                    f"### {script_name}",
                    f"- **Error:** {script_result['error']}",
                    ""
                ])
                
                # Show preprocessing issues if available
                if 'preprocessing_stats' in script_result and script_result['preprocessing_stats']:
                    preprocessing = script_result['preprocessing_stats']
                    if preprocessing.get('validation_issues'):
                        report_lines.append("- **Validation Issues:**")
                        for issue in preprocessing['validation_issues']:
                            report_lines.append(f"  - {issue}")
                        report_lines.append("")
        else:
            report_lines.append("*No script failures.*")
        
        report_lines.extend([
            "",
            "## 🔧 Preprocessing Statistics",
            "",
            "| Script | Table Replacements | Syntax Transformations | Validation Issues |",
            "|--------|-------------------|------------------------|-------------------|"
        ])
        
        for script_name, stats in results['preprocessing_stats'].items():
            table_reps = stats.get('table_replacements_applied', 0)
            syntax_trans = stats.get('syntax_transformations_applied', 0)
            validation_issues = len(stats.get('validation_issues', []))
            
            report_lines.append(
                f"| {script_name} | {table_reps} | {syntax_trans} | {validation_issues} |"
            )
        
        report_lines.extend([
            "",
            "---",
            "*Report generated by MLB Sharp Betting Analytics Platform*",
            "*General Balls*"
        ])
        
        return "\n".join(report_lines)
    
    async def test_single_script(self, script_name: str) -> Dict[str, Any]:
        """
        Test a single script for debugging purposes.
        
        Args:
            script_name: Name of the script to test
            
        Returns:
            Detailed test results
        """
        if script_name not in self.analysis_scripts:
            raise ValueError(f"Script '{script_name}' not found in available scripts")
        
        script_path = self.analysis_scripts[script_name]
        
        self.logger.info("Testing single script", script=script_name)
        
        result = self.run_single_script(script_name)
        
        # Add additional debugging information
        if not result['success'] and 'preprocessing_stats' in result:
            preprocessing_stats = result['preprocessing_stats']
            
            # Read the original and processed SQL for debugging
            full_path = Path(script_path)
            original_sql = full_path.read_text()
            processed_sql = self.sql_preprocessor.process_sql_string(original_sql)
            
            result['debug_info'] = {
                'original_sql_preview': original_sql[:500] + "..." if len(original_sql) > 500 else original_sql,
                'processed_sql_preview': processed_sql[:500] + "..." if len(processed_sql) > 500 else processed_sql,
                'validation_issues_detail': preprocessing_stats.get('validation_issues', [])
            }
        
        return result


def main():
    """Test the analysis script runner."""
    runner = AnalysisScriptRunner()
    
    print("🚀 TESTING ANALYSIS SCRIPTS WITH CENTRALIZED TABLE REGISTRY")
    print("=" * 70)
    
    # Test one script first
    print("\n📋 TESTING SINGLE SCRIPT: consensus_moneyline_strategy")
    print("-" * 50)
    
    single_result = runner.run_single_script("consensus_moneyline_strategy")
    
    if single_result['success']:
        print(f"✅ SUCCESS: {len(single_result['data'])} rows returned")
        print(f"⏱️  Execution time: {single_result['execution_time_seconds']:.2f}s")
        print(f"🔄 Table replacements: {single_result['preprocessing_stats']['table_replacements_applied']}")
        print(f"🔄 Syntax transformations: {single_result['preprocessing_stats']['syntax_transformations_applied']}")
        
        if single_result['preprocessing_stats']['validation_issues']:
            print(f"⚠️  Validation issues:")
            for issue in single_result['preprocessing_stats']['validation_issues']:
                print(f"   • {issue}")
    else:
        print(f"❌ FAILED: {single_result['error']}")
        if 'debug_info' in single_result:
            print(f"\n🔍 DEBUG INFO:")
            print(f"Original SQL (first 500 chars):")
            print(single_result['debug_info']['original_sql_preview'][:500])
            print(f"\nProcessed SQL (first 500 chars):")
            print(single_result['debug_info']['processed_sql_preview'][:500])
    
    # Run all scripts
    print(f"\n📊 RUNNING ALL {len(runner.analysis_scripts)} SCRIPTS")
    print("=" * 70)
    
    all_results = runner.run_all_scripts()
    
    print(f"\n📈 EXECUTION SUMMARY:")
    print(f"✅ Successful: {all_results['execution_summary']['successful_count']}")
    print(f"❌ Failed: {all_results['execution_summary']['failed_count']}")
    print(f"📊 Success Rate: {all_results['execution_summary']['success_rate']:.1f}%")
    print(f"📋 Total Rows: {all_results['execution_summary']['total_rows_returned']:,}")
    
    if all_results['successful_scripts']:
        print(f"\n✅ SUCCESSFUL SCRIPTS:")
        for script in all_results['successful_scripts']:
            result = all_results['script_results'][script]
            print(f"   • {script}: {len(result['data'])} rows in {result['execution_time_seconds']:.2f}s")
    
    if all_results['failed_scripts']:
        print(f"\n❌ FAILED SCRIPTS:")
        for script in all_results['failed_scripts']:
            result = all_results['script_results'][script]
            print(f"   • {script}: {result['error'][:100]}...")
    
    print(f"\n🎯 ANALYSIS COMPLETE")
    print("General Balls")


if __name__ == "__main__":
    main() 