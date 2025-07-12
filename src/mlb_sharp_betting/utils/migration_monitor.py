"""
Migration Monitor - Phase 2A Schema Migration Monitoring

This utility monitors data flow between legacy and new consolidated schema tables
during the Phase 2A migration, providing real-time insights into:

1. Data volume comparisons between legacy and new tables
2. Migration progress tracking
3. Data integrity validation
4. Performance impact assessment
5. Rollback readiness verification

Usage:
    from mlb_sharp_betting.utils.migration_monitor import MigrationMonitor
    
    monitor = MigrationMonitor()
    report = await monitor.generate_migration_report()
    print(report.summary)
"""

import asyncio
import json
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path
import structlog

from ..db.connection import get_db_manager
from ..db.table_registry import get_table_registry
from ..core.exceptions import DatabaseError

logger = structlog.get_logger(__name__)


@dataclass
class TableComparisonResult:
    """Results of comparing legacy vs new table data."""
    legacy_table: str
    new_table: str
    legacy_count: int
    new_count: int
    difference: int
    percentage_migrated: float
    last_legacy_record: Optional[datetime]
    last_new_record: Optional[datetime]
    data_integrity_status: str  # 'GOOD', 'WARNING', 'ERROR'
    issues: List[str]


@dataclass
class MigrationReport:
    """Complete migration status report."""
    report_timestamp: datetime
    migration_phase: str
    overall_status: str  # 'ON_TRACK', 'NEEDS_ATTENTION', 'CRITICAL'
    
    # Table comparisons
    table_comparisons: List[TableComparisonResult]
    
    # Summary statistics
    total_legacy_records: int
    total_new_records: int
    overall_migration_percentage: float
    
    # Performance metrics
    data_freshness_status: str
    migration_lag_minutes: float
    
    # Recommendations
    recommendations: List[str]
    rollback_readiness: bool
    
    @property
    def summary(self) -> str:
        """Generate a human-readable summary."""
        return f"""
🚀 PHASE 2A MIGRATION MONITOR REPORT
Generated: {self.report_timestamp.strftime('%Y-%m-%d %H:%M:%S')}
Overall Status: {self.overall_status}

📊 MIGRATION PROGRESS:
- Total Legacy Records: {self.total_legacy_records:,}
- Total New Schema Records: {self.total_new_records:,}
- Migration Percentage: {self.overall_migration_percentage:.1f}%
- Data Freshness: {self.data_freshness_status}
- Migration Lag: {self.migration_lag_minutes:.1f} minutes

📋 TABLE COMPARISONS:
{self._format_table_comparisons()}

💡 RECOMMENDATIONS:
{chr(10).join(f"- {rec}" for rec in self.recommendations)}

🔄 ROLLBACK READINESS: {'✅ READY' if self.rollback_readiness else '❌ NOT READY'}
"""
    
    def _format_table_comparisons(self) -> str:
        """Format table comparison results."""
        lines = []
        for comp in self.table_comparisons:
            status_emoji = "✅" if comp.data_integrity_status == "GOOD" else "⚠️" if comp.data_integrity_status == "WARNING" else "❌"
            lines.append(f"  {status_emoji} {comp.legacy_table} → {comp.new_table}: {comp.percentage_migrated:.1f}% ({comp.new_count:,}/{comp.legacy_count:,})")
            if comp.issues:
                for issue in comp.issues:
                    lines.append(f"    ⚠️ {issue}")
        return "\n".join(lines)


class MigrationMonitor:
    """
    Monitors Phase 2A schema migration progress and data integrity.
    
    Provides comprehensive monitoring of the migration from legacy tables
    to the new consolidated schema structure.
    """
    
    def __init__(self):
        """Initialize the migration monitor."""
        self.db_manager = get_db_manager()
        self.table_registry = get_table_registry()
        self.logger = logger.bind(component="migration_monitor")
        
        # Define table mappings for monitoring
        self.table_mappings = {
            # Betting lines tables
            'moneyline': {
                'legacy': 'mlb_betting.moneyline',
                'new': self.table_registry.get_table('moneyline'),
                'timestamp_column': 'odds_timestamp'
            },
            'spreads': {
                'legacy': 'mlb_betting.spreads', 
                'new': self.table_registry.get_table('spreads'),
                'timestamp_column': 'odds_timestamp'
            },
            'totals': {
                'legacy': 'mlb_betting.totals',
                'new': self.table_registry.get_table('totals'),
                'timestamp_column': 'odds_timestamp'
            },
            
            # Games table
            'games': {
                'legacy': 'public.games',
                'new': self.table_registry.get_table('games'),
                'timestamp_column': 'created_at'
            },
            
            # Betting recommendations
            'betting_recommendations': {
                'legacy': 'clean.betting_recommendations',
                'new': self.table_registry.get_table('betting_recommendations'),
                'timestamp_column': 'created_at'
            },
            
            # Raw data
            'raw_betting_splits': {
                'legacy': 'splits.raw_mlb_betting_splits',
                'new': self.table_registry.get_table('raw_betting_splits'),
                'timestamp_column': 'last_updated'
            }
        }
    
    async def generate_migration_report(self, 
                                      lookback_hours: int = 24,
                                      include_historical: bool = True) -> MigrationReport:
        """
        Generate a comprehensive migration status report.
        
        Args:
            lookback_hours: Hours to look back for recent data analysis
            include_historical: Whether to include full historical data comparison
            
        Returns:
            Complete migration report
        """
        self.logger.info("Generating migration report", 
                        lookback_hours=lookback_hours,
                        include_historical=include_historical)
        
        try:
            # Compare all table pairs
            table_comparisons = []
            total_legacy = 0
            total_new = 0
            
            for table_name, mapping in self.table_mappings.items():
                comparison = await self._compare_tables(
                    table_name, mapping, lookback_hours, include_historical
                )
                table_comparisons.append(comparison)
                total_legacy += comparison.legacy_count
                total_new += comparison.new_count
            
            # Calculate overall metrics
            overall_percentage = (total_new / max(total_legacy, 1)) * 100
            
            # Assess data freshness and migration lag
            freshness_status, migration_lag = await self._assess_data_freshness(lookback_hours)
            
            # Determine overall status
            overall_status = self._determine_overall_status(table_comparisons, overall_percentage)
            
            # Generate recommendations
            recommendations = self._generate_recommendations(table_comparisons, overall_percentage, migration_lag)
            
            # Check rollback readiness
            rollback_readiness = await self._check_rollback_readiness()
            
            report = MigrationReport(
                report_timestamp=datetime.now(),
                migration_phase="Phase 2A - Application Migration",
                overall_status=overall_status,
                table_comparisons=table_comparisons,
                total_legacy_records=total_legacy,
                total_new_records=total_new,
                overall_migration_percentage=overall_percentage,
                data_freshness_status=freshness_status,
                migration_lag_minutes=migration_lag,
                recommendations=recommendations,
                rollback_readiness=rollback_readiness
            )
            
            self.logger.info("Migration report generated successfully",
                           overall_status=overall_status,
                           migration_percentage=f"{overall_percentage:.1f}%")
            
            return report
            
        except Exception as e:
            self.logger.error("Failed to generate migration report", error=str(e))
            raise DatabaseError(f"Migration report generation failed: {e}")
    
    async def _compare_tables(self, table_name: str, mapping: Dict[str, str], 
                            lookback_hours: int, include_historical: bool) -> TableComparisonResult:
        """Compare legacy and new table data."""
        try:
            legacy_table = mapping['legacy']
            new_table = mapping['new']
            timestamp_col = mapping['timestamp_column']
            
            # Get record counts
            legacy_count = await self._get_table_count(legacy_table, timestamp_col, 
                                                     lookback_hours if not include_historical else None)
            new_count = await self._get_table_count(new_table, timestamp_col,
                                                   lookback_hours if not include_historical else None)
            
            # Get latest timestamps
            last_legacy = await self._get_latest_timestamp(legacy_table, timestamp_col)
            last_new = await self._get_latest_timestamp(new_table, timestamp_col)
            
            # Calculate metrics
            difference = new_count - legacy_count
            percentage_migrated = (new_count / max(legacy_count, 1)) * 100
            
            # Assess data integrity
            integrity_status, issues = await self._assess_table_integrity(
                legacy_table, new_table, timestamp_col, lookback_hours
            )
            
            return TableComparisonResult(
                legacy_table=legacy_table,
                new_table=new_table,
                legacy_count=legacy_count,
                new_count=new_count,
                difference=difference,
                percentage_migrated=percentage_migrated,
                last_legacy_record=last_legacy,
                last_new_record=last_new,
                data_integrity_status=integrity_status,
                issues=issues
            )
            
        except Exception as e:
            self.logger.error("Failed to compare tables", 
                            table_name=table_name, error=str(e))
            return TableComparisonResult(
                legacy_table=mapping['legacy'],
                new_table=mapping['new'],
                legacy_count=0,
                new_count=0,
                difference=0,
                percentage_migrated=0.0,
                last_legacy_record=None,
                last_new_record=None,
                data_integrity_status='ERROR',
                issues=[f"Comparison failed: {str(e)}"]
            )
    
    async def _get_table_count(self, table_name: str, timestamp_col: str, 
                             lookback_hours: Optional[int] = None) -> int:
        """Get record count for a table, optionally filtered by time."""
        try:
            # Check if table exists first
            schema, table = table_name.split('.')
            exists_query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = %s AND table_name = %s
                )
            """
            
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(exists_query, (schema, table))
                table_exists = cursor.fetchone()[0]
                
                if not table_exists:
                    return 0
                
                # Build count query
                if lookback_hours:
                    cutoff_time = datetime.now() - timedelta(hours=lookback_hours)
                    query = f"SELECT COUNT(*) FROM {table_name} WHERE {timestamp_col} >= %s"
                    cursor.execute(query, (cutoff_time,))
                else:
                    query = f"SELECT COUNT(*) FROM {table_name}"
                    cursor.execute(query)
                
                return cursor.fetchone()[0]
                
        except Exception as e:
            self.logger.warning("Failed to get table count", 
                              table=table_name, error=str(e))
            return 0
    
    async def _get_latest_timestamp(self, table_name: str, timestamp_col: str) -> Optional[datetime]:
        """Get the latest timestamp from a table."""
        try:
            # Check if table exists first
            schema, table = table_name.split('.')
            exists_query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = %s AND table_name = %s
                )
            """
            
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(exists_query, (schema, table))
                table_exists = cursor.fetchone()[0]
                
                if not table_exists:
                    return None
                
                query = f"SELECT MAX({timestamp_col}) FROM {table_name}"
                cursor.execute(query)
                result = cursor.fetchone()
                return result[0] if result and result[0] else None
                
        except Exception as e:
            self.logger.warning("Failed to get latest timestamp", 
                              table=table_name, error=str(e))
            return None
    
    async def _assess_table_integrity(self, legacy_table: str, new_table: str,
                                    timestamp_col: str, lookback_hours: int) -> Tuple[str, List[str]]:
        """Assess data integrity between legacy and new tables."""
        issues = []
        
        try:
            # Check if new table exists
            schema, table = new_table.split('.')
            exists_query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = %s AND table_name = %s
                )
            """
            
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(exists_query, (schema, table))
                new_table_exists = cursor.fetchone()[0]
                
                if not new_table_exists:
                    issues.append("New table does not exist")
                    return 'ERROR', issues
                
                # Check recent data flow
                cutoff_time = datetime.now() - timedelta(hours=lookback_hours)
                
                # Check if new table has recent data
                recent_new_query = f"SELECT COUNT(*) FROM {new_table} WHERE {timestamp_col} >= %s"
                cursor.execute(recent_new_query, (cutoff_time,))
                recent_new_count = cursor.fetchone()[0]
                
                if recent_new_count == 0:
                    issues.append("No recent data in new table")
                
                # Check if legacy table has more recent data than new table
                legacy_latest_query = f"SELECT MAX({timestamp_col}) FROM {legacy_table}"
                new_latest_query = f"SELECT MAX({timestamp_col}) FROM {new_table}"
                
                cursor.execute(legacy_latest_query)
                legacy_latest = cursor.fetchone()[0]
                
                cursor.execute(new_latest_query)
                new_latest = cursor.fetchone()[0]
                
                if legacy_latest and new_latest:
                    # Ensure both datetimes are timezone-naive for comparison
                    if legacy_latest.tzinfo is not None:
                        legacy_latest = legacy_latest.replace(tzinfo=None)
                    if new_latest.tzinfo is not None:
                        new_latest = new_latest.replace(tzinfo=None)
                    
                    lag_minutes = (legacy_latest - new_latest).total_seconds() / 60
                    if lag_minutes > 60:  # More than 1 hour lag
                        issues.append(f"Data lag of {lag_minutes:.1f} minutes")
                
                # Determine overall status
                if not issues:
                    return 'GOOD', issues
                elif len(issues) == 1 and 'lag' in issues[0]:
                    return 'WARNING', issues
                else:
                    return 'ERROR', issues
                    
        except Exception as e:
            issues.append(f"Integrity check failed: {str(e)}")
            return 'ERROR', issues
    
    async def _assess_data_freshness(self, lookback_hours: int) -> Tuple[str, float]:
        """Assess overall data freshness and migration lag."""
        try:
            max_lag = 0.0
            fresh_tables = 0
            total_tables = 0
            
            for table_name, mapping in self.table_mappings.items():
                legacy_latest = await self._get_latest_timestamp(
                    mapping['legacy'], mapping['timestamp_column']
                )
                new_latest = await self._get_latest_timestamp(
                    mapping['new'], mapping['timestamp_column']
                )
                
                total_tables += 1
                
                if legacy_latest and new_latest:
                    # Ensure both datetimes are timezone-naive for comparison
                    if legacy_latest.tzinfo is not None:
                        legacy_latest = legacy_latest.replace(tzinfo=None)
                    if new_latest.tzinfo is not None:
                        new_latest = new_latest.replace(tzinfo=None)
                    
                    lag_minutes = (legacy_latest - new_latest).total_seconds() / 60
                    max_lag = max(max_lag, lag_minutes)
                    
                    if lag_minutes <= 30:  # Fresh if within 30 minutes
                        fresh_tables += 1
            
            freshness_percentage = (fresh_tables / max(total_tables, 1)) * 100
            
            if freshness_percentage >= 80:
                status = "EXCELLENT"
            elif freshness_percentage >= 60:
                status = "GOOD" 
            elif freshness_percentage >= 40:
                status = "FAIR"
            else:
                status = "POOR"
            
            return status, max_lag
            
        except Exception as e:
            self.logger.error("Failed to assess data freshness", error=str(e))
            return "UNKNOWN", 0.0
    
    def _determine_overall_status(self, comparisons: List[TableComparisonResult], 
                                overall_percentage: float) -> str:
        """Determine overall migration status."""
        error_count = sum(1 for comp in comparisons if comp.data_integrity_status == 'ERROR')
        warning_count = sum(1 for comp in comparisons if comp.data_integrity_status == 'WARNING')
        
        if error_count > 0:
            return "CRITICAL"
        elif warning_count > 2 or overall_percentage < 50:
            return "NEEDS_ATTENTION"
        else:
            return "ON_TRACK"
    
    def _generate_recommendations(self, comparisons: List[TableComparisonResult],
                                overall_percentage: float, migration_lag: float) -> List[str]:
        """Generate actionable recommendations."""
        recommendations = []
        
        # Check for tables with issues
        error_tables = [comp for comp in comparisons if comp.data_integrity_status == 'ERROR']
        if error_tables:
            recommendations.append(f"URGENT: Fix {len(error_tables)} tables with errors")
        
        # Check migration progress
        if overall_percentage < 25:
            recommendations.append("Migration just started - monitor closely for next few hours")
        elif overall_percentage < 75:
            recommendations.append("Migration in progress - verify application services are updated")
        elif overall_percentage < 95:
            recommendations.append("Migration nearly complete - prepare for legacy table deprecation")
        
        # Check data lag
        if migration_lag > 120:  # More than 2 hours
            recommendations.append("High data lag detected - check application service configuration")
        elif migration_lag > 60:  # More than 1 hour
            recommendations.append("Moderate data lag - monitor application performance")
        
        # Check for zero new records
        zero_new = [comp for comp in comparisons if comp.new_count == 0 and comp.legacy_count > 0]
        if zero_new:
            recommendations.append("Some tables not receiving new data - verify service updates")
        
        if not recommendations:
            recommendations.append("Migration proceeding smoothly - continue monitoring")
        
        return recommendations
    
    async def _check_rollback_readiness(self) -> bool:
        """Check if system is ready for rollback if needed."""
        try:
            # Check that legacy tables still exist and have recent data
            legacy_tables_active = 0
            
            for mapping in self.table_mappings.values():
                legacy_table = mapping['legacy']
                timestamp_col = mapping['timestamp_column']
                
                # Check if legacy table has data from last 24 hours
                recent_count = await self._get_table_count(legacy_table, timestamp_col, 24)
                if recent_count > 0:
                    legacy_tables_active += 1
            
            # Rollback ready if at least half of legacy tables are still active
            return legacy_tables_active >= len(self.table_mappings) // 2
            
        except Exception as e:
            self.logger.error("Failed to check rollback readiness", error=str(e))
            return False
    
    async def save_report(self, report: MigrationReport, output_dir: Optional[Path] = None) -> Path:
        """Save migration report to file."""
        if output_dir is None:
            output_dir = Path("reports/migration")
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = report.report_timestamp.strftime("%Y%m%d_%H%M%S")
        report_file = output_dir / f"migration_report_{timestamp}.json"
        
        try:
            with open(report_file, 'w') as f:
                json.dump(asdict(report), f, indent=2, default=str)
            
            self.logger.info("Migration report saved", file_path=str(report_file))
            return report_file
            
        except Exception as e:
            self.logger.error("Failed to save migration report", error=str(e))
            raise


async def monitor_migration(lookback_hours: int = 24, 
                          save_report: bool = True,
                          output_dir: Optional[Path] = None) -> MigrationReport:
    """
    Convenience function to monitor migration and optionally save report.
    
    Args:
        lookback_hours: Hours to look back for analysis
        save_report: Whether to save the report to file
        output_dir: Directory to save report
        
    Returns:
        Migration report
    """
    monitor = MigrationMonitor()
    report = await monitor.generate_migration_report(lookback_hours=lookback_hours)
    
    if save_report:
        await monitor.save_report(report, output_dir)
    
    return report


if __name__ == "__main__":
    async def main():
        """Run migration monitoring when executed directly."""
        print("🔍 PHASE 2A MIGRATION MONITOR")
        print("=" * 50)
        
        try:
            report = await monitor_migration(lookback_hours=24, save_report=True)
            print(report.summary)
            
        except Exception as e:
            print(f"❌ Monitoring failed: {e}")
            import traceback
            traceback.print_exc()
    
    asyncio.run(main()) 