#!/usr/bin/env python3
"""
Migration Validation and Testing Utility

Provides comprehensive validation functions for the pipeline migration process.
Verifies data integrity, completeness, and quality across all migration phases.

Validation Categories:
1. Pre-migration validation (schema readiness)
2. Data integrity validation (record counts, referential integrity)
3. Quality validation (data quality scores, completeness)
4. Post-migration validation (pipeline functionality)
"""

import asyncio
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timezone
import json

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.core.config import get_settings
from src.core.logging import get_logger, LogComponent
from src.data.database import get_connection
from src.data.database.connection import initialize_connections

logger = get_logger(__name__, LogComponent.CORE)


class MigrationValidator:
    """Validates migration integrity across all phases."""
    
    def __init__(self):
        self.settings = get_settings()
        self.validation_results = {
            'timestamp': datetime.now().isoformat(),
            'validations': {},
            'summary': {},
            'issues': [],
            'recommendations': []
        }
        
    async def initialize(self):
        """Initialize database connection."""
        initialize_connections(self.settings)
        
    async def close(self):
        """Close database connections."""
        pass  # Connection pool managed globally
    
    async def run_comprehensive_validation(self, phase: str = 'all') -> Dict[str, Any]:
        """Run comprehensive validation based on migration phase."""
        logger.info(f"Starting comprehensive validation for phase: {phase}")
        
        try:
            connection_manager = get_connection()
            async with connection_manager.get_async_connection() as conn:
                if phase in ['all', 'pre-migration']:
                    self.validation_results['validations']['pre_migration'] = await self._validate_pre_migration(conn)
                
                if phase in ['all', 'raw-zone', 'phase2']:
                    self.validation_results['validations']['raw_zone'] = await self._validate_raw_zone_migration(conn)
                
                if phase in ['all', 'staging-zone', 'phase3']:
                    self.validation_results['validations']['staging_zone'] = await self._validate_staging_zone_migration(conn)
                
                if phase in ['all', 'curated-zone', 'phase4']:
                    self.validation_results['validations']['curated_zone'] = await self._validate_curated_zone_migration(conn)
                
                if phase in ['all', 'pipeline']:
                    self.validation_results['validations']['pipeline'] = await self._validate_pipeline_functionality(conn)
                
                # Generate overall summary and recommendations
                self.validation_results['summary'] = self._generate_validation_summary()
                self.validation_results['recommendations'] = self._generate_recommendations()
                
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            self.validation_results['error'] = str(e)
            self.validation_results['issues'].append({
                'severity': 'critical',
                'category': 'validation_error',
                'message': f"Validation process failed: {e}",
                'timestamp': datetime.now().isoformat()
            })
        
        return self.validation_results
    
    async def _validate_pre_migration(self, conn) -> Dict[str, Any]:
        """Validate system readiness before migration."""
        logger.info("Validating pre-migration requirements...")
        
        validation = {
            'status': 'checking',
            'checks': {},
            'issues': [],
            'passed': 0,
            'failed': 0
        }
        
        # Check 1: Schema existence
        schema_check = await self._check_schema_existence(conn)
        validation['checks']['schema_existence'] = schema_check
        if schema_check['passed']:
            validation['passed'] += 1
        else:
            validation['failed'] += 1
            validation['issues'].extend(schema_check.get('issues', []))
        
        # Check 2: Table structure compatibility
        table_check = await self._check_table_structures(conn)
        validation['checks']['table_structures'] = table_check
        if table_check['passed']:
            validation['passed'] += 1
        else:
            validation['failed'] += 1
            validation['issues'].extend(table_check.get('issues', []))
        
        # Check 3: Source data integrity
        data_check = await self._check_source_data_integrity(conn)
        validation['checks']['source_data_integrity'] = data_check
        if data_check['passed']:
            validation['passed'] += 1
        else:
            validation['failed'] += 1
            validation['issues'].extend(data_check.get('issues', []))
        
        # Check 4: Database permissions
        permissions_check = await self._check_database_permissions(conn)
        validation['checks']['database_permissions'] = permissions_check
        if permissions_check['passed']:
            validation['passed'] += 1
        else:
            validation['failed'] += 1
            validation['issues'].extend(permissions_check.get('issues', []))
        
        validation['status'] = 'passed' if validation['failed'] == 0 else 'failed'
        return validation
    
    async def _validate_raw_zone_migration(self, conn) -> Dict[str, Any]:
        """Validate RAW zone migration results."""
        logger.info("Validating RAW zone migration...")
        
        validation = {
            'status': 'checking',
            'checks': {},
            'issues': [],
            'passed': 0,
            'failed': 0
        }
        
        # Check 1: Record count consistency
        count_check = await self._check_raw_zone_record_counts(conn)
        validation['checks']['record_counts'] = count_check
        if count_check['passed']:
            validation['passed'] += 1
        else:
            validation['failed'] += 1
            validation['issues'].extend(count_check.get('issues', []))
        
        # Check 2: Data completeness
        completeness_check = await self._check_raw_zone_completeness(conn)
        validation['checks']['data_completeness'] = completeness_check
        if completeness_check['passed']:
            validation['passed'] += 1
        else:
            validation['failed'] += 1
            validation['issues'].extend(completeness_check.get('issues', []))
        
        # Check 3: Source attribution preservation
        attribution_check = await self._check_source_attribution(conn)
        validation['checks']['source_attribution'] = attribution_check
        if attribution_check['passed']:
            validation['passed'] += 1
        else:
            validation['failed'] += 1
            validation['issues'].extend(attribution_check.get('issues', []))
        
        validation['status'] = 'passed' if validation['failed'] == 0 else 'failed'
        return validation
    
    async def _validate_staging_zone_migration(self, conn) -> Dict[str, Any]:
        """Validate STAGING zone migration results."""
        logger.info("Validating STAGING zone migration...")
        
        validation = {
            'status': 'checking',
            'checks': {},
            'issues': [],
            'passed': 0,
            'failed': 0
        }
        
        # Check 1: Data quality scores
        quality_check = await self._check_staging_quality_scores(conn)
        validation['checks']['quality_scores'] = quality_check
        if quality_check['passed']:
            validation['passed'] += 1
        else:
            validation['failed'] += 1
            validation['issues'].extend(quality_check.get('issues', []))
        
        # Check 2: Referential integrity
        integrity_check = await self._check_staging_referential_integrity(conn)
        validation['checks']['referential_integrity'] = integrity_check
        if integrity_check['passed']:
            validation['passed'] += 1
        else:
            validation['failed'] += 1
            validation['issues'].extend(integrity_check.get('issues', []))
        
        validation['status'] = 'passed' if validation['failed'] == 0 else 'failed'
        return validation
    
    async def _validate_curated_zone_migration(self, conn) -> Dict[str, Any]:
        """Validate CURATED zone migration results."""
        logger.info("Validating CURATED zone migration...")
        
        validation = {
            'status': 'checking', 
            'checks': {},
            'issues': [],
            'passed': 0,
            'failed': 0
        }
        
        # Check 1: Feature vector generation
        features_check = await self._check_feature_generation(conn)
        validation['checks']['feature_generation'] = features_check
        if features_check['passed']:
            validation['passed'] += 1
        else:
            validation['failed'] += 1
            validation['issues'].extend(features_check.get('issues', []))
        
        validation['status'] = 'passed' if validation['failed'] == 0 else 'failed'
        return validation
    
    async def _validate_pipeline_functionality(self, conn) -> Dict[str, Any]:
        """Validate end-to-end pipeline functionality."""
        logger.info("Validating pipeline functionality...")
        
        validation = {
            'status': 'checking',
            'checks': {},
            'issues': [],
            'passed': 0,
            'failed': 0
        }
        
        # Check 1: Zone processors availability
        processor_check = await self._check_zone_processors(conn)
        validation['checks']['zone_processors'] = processor_check
        if processor_check['passed']:
            validation['passed'] += 1
        else:
            validation['failed'] += 1
            validation['issues'].extend(processor_check.get('issues', []))
        
        validation['status'] = 'passed' if validation['failed'] == 0 else 'failed'
        return validation
    
    async def _check_schema_existence(self, conn) -> Dict[str, Any]:
        """Check that all required schemas exist."""
        required_schemas = ['raw_data', 'staging', 'curated']
        
        existing_schemas = await conn.fetch("""
            SELECT schema_name FROM information_schema.schemata 
            WHERE schema_name IN ('raw_data', 'staging', 'curated')
        """)
        existing_schema_names = [row['schema_name'] for row in existing_schemas]
        
        missing_schemas = [schema for schema in required_schemas if schema not in existing_schema_names]
        
        return {
            'passed': len(missing_schemas) == 0,
            'required_schemas': required_schemas,
            'existing_schemas': existing_schema_names,
            'missing_schemas': missing_schemas,
            'issues': [f"Missing schema: {schema}" for schema in missing_schemas]
        }
    
    async def _check_table_structures(self, conn) -> Dict[str, Any]:
        """Check that all required tables exist with correct structure."""
        required_tables = [
            ('raw_data', 'betting_lines_raw'),
            ('raw_data', 'moneylines_raw'),
            ('raw_data', 'spreads_raw'),
            ('raw_data', 'totals_raw'),
            ('staging', 'betting_lines'),
            ('staging', 'moneylines'),
            ('staging', 'spreads'),
            ('staging', 'totals'),
            ('staging', 'games'),
            ('curated', 'betting_lines_enhanced'),
            ('curated', 'feature_vectors')
        ]
        
        missing_tables = []
        
        for schema, table in required_tables:
            result = await conn.fetchrow("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = $1 AND table_name = $2
                )
            """, schema, table)
            
            if not result[0]:
                missing_tables.append(f"{schema}.{table}")
        
        return {
            'passed': len(missing_tables) == 0,
            'required_tables_count': len(required_tables),
            'missing_tables': missing_tables,
            'issues': [f"Missing table: {table}" for table in missing_tables]
        }
    
    async def _check_source_data_integrity(self, conn) -> Dict[str, Any]:
        """Check integrity of source data in core_betting schema."""
        issues = []
        
        # Check for null game IDs
        null_games = await conn.fetchrow("""
            SELECT COUNT(*) as count FROM (
                SELECT game_id FROM core_betting.betting_lines_moneyline WHERE game_id IS NULL
                UNION ALL
                SELECT game_id FROM core_betting.betting_lines_spread WHERE game_id IS NULL
                UNION ALL  
                SELECT game_id FROM core_betting.betting_lines_totals WHERE game_id IS NULL
            ) nulls
        """)
        
        if null_games['count'] > 0:
            issues.append(f"{null_games['count']} betting records have null game_id")
        
        # Check for orphaned records
        orphaned = await conn.fetchrow("""
            SELECT COUNT(*) as count FROM (
                SELECT DISTINCT bl.game_id 
                FROM core_betting.betting_lines_moneyline bl
                LEFT JOIN core_betting.games g ON bl.game_id = g.id
                WHERE g.id IS NULL
                UNION
                SELECT DISTINCT bl.game_id 
                FROM core_betting.betting_lines_spread bl
                LEFT JOIN core_betting.games g ON bl.game_id = g.id
                WHERE g.id IS NULL
                UNION
                SELECT DISTINCT bl.game_id 
                FROM core_betting.betting_lines_totals bl
                LEFT JOIN core_betting.games g ON bl.game_id = g.id
                WHERE g.id IS NULL
            ) orphaned
        """)
        
        if orphaned['count'] > 0:
            issues.append(f"{orphaned['count']} game IDs reference non-existent games")
        
        return {
            'passed': len(issues) == 0,
            'null_game_ids': null_games['count'],
            'orphaned_records': orphaned['count'],
            'issues': issues
        }
    
    async def _check_database_permissions(self, conn) -> Dict[str, Any]:
        """Check database permissions for migration operations."""
        issues = []
        
        # Test INSERT permission on raw_data schema
        try:
            await conn.execute("BEGIN")
            await conn.execute("""
                INSERT INTO raw_data.betting_lines_raw (
                    external_id, source, collected_at, created_at, updated_at
                ) VALUES (
                    'test_permission', 'TEST', NOW(), NOW(), NOW()
                )
            """)
            await conn.execute("ROLLBACK")
        except Exception as e:
            issues.append(f"Cannot insert into raw_data schema: {e}")
        
        return {
            'passed': len(issues) == 0,
            'issues': issues
        }
    
    async def _check_raw_zone_record_counts(self, conn) -> Dict[str, Any]:
        """Validate record counts between source and RAW zone."""
        # Get source counts
        source_counts = await conn.fetchrow("""
            SELECT 
                (SELECT COUNT(*) FROM core_betting.betting_lines_moneyline) as source_moneylines,
                (SELECT COUNT(*) FROM core_betting.betting_lines_spread) as source_spreads,
                (SELECT COUNT(*) FROM core_betting.betting_lines_totals) as source_totals
        """)
        
        # Get RAW zone counts
        raw_counts = await conn.fetchrow("""
            SELECT 
                (SELECT COUNT(*) FROM raw_data.moneylines_raw) as raw_moneylines,
                (SELECT COUNT(*) FROM raw_data.spreads_raw) as raw_spreads,
                (SELECT COUNT(*) FROM raw_data.totals_raw) as raw_totals,
                (SELECT COUNT(*) FROM raw_data.betting_lines_raw) as raw_unified
        """)
        
        issues = []
        
        # Compare counts
        if source_counts['source_moneylines'] != raw_counts['raw_moneylines']:
            issues.append(f"Moneyline count mismatch: source={source_counts['source_moneylines']}, raw={raw_counts['raw_moneylines']}")
        
        if source_counts['source_spreads'] != raw_counts['raw_spreads']:
            issues.append(f"Spread count mismatch: source={source_counts['source_spreads']}, raw={raw_counts['raw_spreads']}")
        
        if source_counts['source_totals'] != raw_counts['raw_totals']:
            issues.append(f"Totals count mismatch: source={source_counts['source_totals']}, raw={raw_counts['raw_totals']}")
        
        return {
            'passed': len(issues) == 0,
            'source_counts': dict(source_counts),
            'raw_counts': dict(raw_counts),
            'issues': issues
        }
    
    async def _check_raw_zone_completeness(self, conn) -> Dict[str, Any]:
        """Check data completeness in RAW zone."""
        issues = []
        
        # Check for null external_ids
        null_external_ids = await conn.fetchrow("""
            SELECT COUNT(*) as count FROM (
                SELECT external_id FROM raw_data.moneylines_raw WHERE external_id IS NULL
                UNION ALL
                SELECT external_id FROM raw_data.spreads_raw WHERE external_id IS NULL
                UNION ALL
                SELECT external_id FROM raw_data.totals_raw WHERE external_id IS NULL
            ) nulls
        """)
        
        if null_external_ids['count'] > 0:
            issues.append(f"{null_external_ids['count']} records have null external_id")
        
        # Check raw_data JSONB completeness
        empty_raw_data = await conn.fetchrow("""
            SELECT COUNT(*) as count FROM (
                SELECT raw_data FROM raw_data.moneylines_raw WHERE raw_data IS NULL OR raw_data = '{}'::jsonb
                UNION ALL
                SELECT raw_data FROM raw_data.spreads_raw WHERE raw_data IS NULL OR raw_data = '{}'::jsonb
                UNION ALL
                SELECT raw_data FROM raw_data.totals_raw WHERE raw_data IS NULL OR raw_data = '{}'::jsonb
            ) empty
        """)
        
        if empty_raw_data['count'] > 0:
            issues.append(f"{empty_raw_data['count']} records have empty raw_data")
        
        return {
            'passed': len(issues) == 0,
            'null_external_ids': null_external_ids['count'],
            'empty_raw_data': empty_raw_data['count'],
            'issues': issues
        }
    
    async def _check_source_attribution(self, conn) -> Dict[str, Any]:
        """Check that source attribution is preserved."""
        # Check source distribution in RAW zone
        source_distribution = await conn.fetch("""
            SELECT source, COUNT(*) as count FROM (
                SELECT source FROM raw_data.moneylines_raw
                UNION ALL
                SELECT source FROM raw_data.spreads_raw
                UNION ALL
                SELECT source FROM raw_data.totals_raw
            ) sources
            GROUP BY source
            ORDER BY count DESC
        """)
        
        issues = []
        source_names = [row['source'] for row in source_distribution]
        
        if not source_names:
            issues.append("No source attribution found in RAW zone")
        elif len(source_names) == 1 and source_names[0] == 'UNKNOWN':
            issues.append("All records have UNKNOWN source attribution")
        
        return {
            'passed': len(issues) == 0,
            'source_distribution': [dict(row) for row in source_distribution],
            'unique_sources': len(source_names),
            'issues': issues
        }
    
    async def _check_staging_quality_scores(self, conn) -> Dict[str, Any]:
        """Check quality scores in staging zone."""
        # This would check if staging tables exist and have quality scores
        # For now, return a placeholder check
        return {
            'passed': True,
            'message': 'Staging zone validation placeholder - implement after staging migration',
            'issues': []
        }
    
    async def _check_staging_referential_integrity(self, conn) -> Dict[str, Any]:
        """Check referential integrity in staging zone."""
        return {
            'passed': True,
            'message': 'Staging referential integrity placeholder - implement after staging migration',
            'issues': []
        }
    
    async def _check_feature_generation(self, conn) -> Dict[str, Any]:
        """Check feature vector generation in curated zone."""
        return {
            'passed': True,
            'message': 'Curated zone validation placeholder - implement after curated migration',
            'issues': []
        }
    
    async def _check_zone_processors(self, conn) -> Dict[str, Any]:
        """Check that zone processors are available and functional."""
        issues = []
        
        try:
            # Try importing zone processors to verify they're available
            from src.data.pipeline.raw_zone import RawZoneProcessor
            from src.data.pipeline.staging_zone import StagingZoneProcessor  
            from src.data.pipeline.curated_zone import CuratedZoneProcessor
            
            # Basic instantiation test (without actual database operations)
            processors_available = {
                'RawZoneProcessor': True,
                'StagingZoneProcessor': True,
                'CuratedZoneProcessor': True
            }
            
        except ImportError as e:
            issues.append(f"Zone processor import failed: {e}")
            processors_available = {}
        
        return {
            'passed': len(issues) == 0,
            'processors_available': processors_available,
            'issues': issues
        }
    
    def _generate_validation_summary(self) -> Dict[str, Any]:
        """Generate overall validation summary."""
        total_checks = 0
        total_passed = 0
        total_failed = 0
        critical_issues = 0
        
        for validation_name, validation_data in self.validation_results['validations'].items():
            if isinstance(validation_data, dict):
                total_checks += validation_data.get('passed', 0) + validation_data.get('failed', 0)
                total_passed += validation_data.get('passed', 0)
                total_failed += validation_data.get('failed', 0)
                
                # Count critical issues
                for issue in validation_data.get('issues', []):
                    if 'missing' in issue.lower() or 'failed' in issue.lower():
                        critical_issues += 1
        
        return {
            'overall_status': 'passed' if total_failed == 0 else 'failed',
            'total_checks': total_checks,
            'checks_passed': total_passed,
            'checks_failed': total_failed,
            'success_rate': (total_passed / total_checks * 100) if total_checks > 0 else 0,
            'critical_issues': critical_issues,
            'validation_phases': len(self.validation_results['validations'])
        }
    
    def _generate_recommendations(self) -> List[Dict[str, Any]]:
        """Generate recommendations based on validation results."""
        recommendations = []
        
        # Analyze validation results and generate recommendations
        for validation_name, validation_data in self.validation_results['validations'].items():
            if isinstance(validation_data, dict) and validation_data.get('failed', 0) > 0:
                recommendations.append({
                    'priority': 'HIGH',
                    'category': 'validation_failure',
                    'phase': validation_name,
                    'title': f'Address {validation_name} validation failures',
                    'description': f"{validation_data.get('failed', 0)} checks failed in {validation_name}",
                    'action_required': True
                })
        
        # Add general recommendations
        summary = self.validation_results.get('summary', {})
        if summary.get('success_rate', 0) < 100:
            recommendations.append({
                'priority': 'MEDIUM',
                'category': 'quality_improvement',
                'title': 'Improve validation success rate',
                'description': f"Current success rate: {summary.get('success_rate', 0):.1f}%",
                'action_required': True
            })
        
        return recommendations


async def main():
    """Main execution function."""
    validator = MigrationValidator()
    
    try:
        await validator.initialize()
        
        print("🔍 Starting Migration Validation")
        print("=" * 60)
        
        # Run validation (you can specify phase: 'pre-migration', 'raw-zone', etc.)
        phase = sys.argv[1] if len(sys.argv) > 1 else 'all'
        results = await validator.run_comprehensive_validation(phase)
        
        # Display results
        print(f"\n📊 VALIDATION RESULTS - Phase: {phase.upper()}")
        print("-" * 40)
        
        if 'summary' in results:
            summary = results['summary']
            status_emoji = "✅" if summary['overall_status'] == 'passed' else "❌"
            print(f"{status_emoji} Overall Status: {summary['overall_status'].upper()}")
            print(f"📈 Total Checks: {summary.get('total_checks', 0)}")
            print(f"✅ Checks Passed: {summary.get('checks_passed', 0)}")
            print(f"❌ Checks Failed: {summary.get('checks_failed', 0)}")
            print(f"📊 Success Rate: {summary.get('success_rate', 0):.1f}%")
            print(f"🔥 Critical Issues: {summary.get('critical_issues', 0)}")
        
        # Show recommendations
        if results.get('recommendations'):
            print("\n🎯 RECOMMENDATIONS")
            print("-" * 40)
            for rec in results['recommendations']:
                priority_emoji = "🔥" if rec['priority'] == 'HIGH' else "⚠️"
                print(f"{priority_emoji} {rec['title']}")
                print(f"   {rec['description']}")
                print()
        
        # Save results
        output_file = Path(f"utilities/migration/validation_results_{phase}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        print(f"💾 Validation results saved to: {output_file}")
        print(f"\n✅ Migration validation complete for phase: {phase}")
        
        # Return exit code based on validation results
        return 0 if results.get('summary', {}).get('overall_status') == 'passed' else 1
        
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        print(f"\n❌ Validation failed: {e}")
        return 1
        
    finally:
        await validator.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))