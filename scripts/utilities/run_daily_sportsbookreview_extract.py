#!/usr/bin/env python3
"""
Daily SportsbookReview Extract and Verification Script

This script runs a complete day of SportsbookReview data extraction and then
verifies that the data was saved correctly in the PostgreSQL database.

Usage:
    python run_daily_sportsbookreview_extract.py --date 2025-07-05
    python run_daily_sportsbookreview_extract.py --date 2025-07-05 --verify-only
    python run_daily_sportsbookreview_extract.py --help
"""

import asyncio
import argparse
import logging
import sys
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
import asyncpg

# Add project paths
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from sportsbookreview.services.collection_orchestrator import CollectionOrchestrator
from sportsbookreview.services.data_storage_service import DataStorageService
from src.mlb_sharp_betting.db.connection import get_db_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sportsbookreview_extract.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DailyExtractRunner:
    """
    Handles running daily SportsbookReview extracts and verification.
    """
    
    def __init__(self, target_date: date, verify_only: bool = False):
        """
        Initialize the extract runner.
        
        Args:
            target_date: Date to extract data for
            verify_only: If True, only run verification (skip extraction)
        """
        self.target_date = target_date
        self.verify_only = verify_only
        
        # Database connection
        self.db_pool: Optional[asyncpg.Pool] = None
        
        # Results tracking
        self.extraction_results: Optional[Dict[str, Any]] = None
        self.verification_results: Optional[Dict[str, Any]] = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.initialize_db_connection()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close_db_connection()
    
    async def initialize_db_connection(self):
        """Initialize database connection pool."""
        try:
            self.db_pool = await asyncpg.create_pool(
                host='localhost',
                port=5432,
                database='mlb_betting',
                user='samlafell',
                min_size=2,
                max_size=10,
                command_timeout=60
            )
            logger.info("Database connection pool initialized")
        except Exception as e:
            logger.error(f"Failed to initialize database connection: {e}")
            raise
    
    async def close_db_connection(self):
        """Close database connection pool."""
        if self.db_pool:
            await self.db_pool.close()
            self.db_pool = None
    
    async def run_complete_process(self) -> Dict[str, Any]:
        """
        Run the complete extract and verification process.
        
        Returns:
            Dictionary with results from both extraction and verification
        """
        logger.info(f"Starting daily SportsbookReview process for {self.target_date}")
        
        results = {
            'date': self.target_date.strftime('%Y-%m-%d'),
            'started_at': datetime.now().isoformat(),
            'extraction_results': None,
            'verification_results': None,
            'overall_status': 'UNKNOWN'
        }
        
        try:
            # Step 1: Run extraction (unless verify-only)
            if not self.verify_only:
                logger.info("=" * 60)
                logger.info("STEP 1: RUNNING DATA EXTRACTION")
                logger.info("=" * 60)
                
                self.extraction_results = await self.run_extraction()
                results['extraction_results'] = self.extraction_results
                
                if self.extraction_results.get('status') != 'SUCCESS':
                    results['overall_status'] = 'EXTRACTION_FAILED'
                    return results
                
                logger.info("✅ Data extraction completed successfully")
                
                # Wait a moment for data to be fully committed
                await asyncio.sleep(2)
            
            # Step 2: Run verification
            logger.info("=" * 60)
            logger.info("STEP 2: RUNNING DATA VERIFICATION")
            logger.info("=" * 60)
            
            self.verification_results = await self.run_verification()
            results['verification_results'] = self.verification_results
            
            # Determine overall status
            if self.verification_results.get('status') == 'SUCCESS':
                results['overall_status'] = 'SUCCESS'
                logger.info("✅ Data verification completed successfully")
            else:
                results['overall_status'] = 'VERIFICATION_FAILED'
                logger.error("❌ Data verification failed")
            
            results['completed_at'] = datetime.now().isoformat()
            
        except Exception as e:
            logger.error(f"Process failed: {e}")
            results['overall_status'] = 'PROCESS_FAILED'
            results['error'] = str(e)
        
        return results
    
    async def run_extraction(self) -> Dict[str, Any]:
        """
        Run the data extraction for the target date.
        
        Returns:
            Dictionary with extraction results
        """
        try:
            # Progress tracking
            def progress_callback(progress: float, message: str):
                logger.info(f"Progress: {progress:.1f}% - {message}")
            
            # Run collection using orchestrator
            async with CollectionOrchestrator(
                output_dir=Path("./output"),
                enable_checkpoints=True
            ) as orchestrator:
                
                results = await orchestrator.collect_date_range(
                    start_date=self.target_date,
                    end_date=self.target_date,
                    progress_callback=progress_callback
                )
                
                # Extract key metrics
                stats = results.get('stats', {})
                
                extraction_summary = {
                    'status': 'SUCCESS',
                    'date': self.target_date.strftime('%Y-%m-%d'),
                    'games_collected': stats.get('games_processed', 0),
                    'games_stored': stats.get('games_stored', 0),
                    'betting_records_stored': stats.get('betting_records_stored', 0),
                    'pages_scraped': stats.get('pages_scraped', 0),
                    'pages_failed': stats.get('pages_failed', 0),
                    'scraping_success_rate': stats.get('scraping_success_rate', 0.0),
                    'storage_success_rate': stats.get('storage_success_rate', 0.0),
                    'duration_seconds': stats.get('total_duration', 0.0),
                    'errors': stats.get('errors_encountered', [])
                }
                
                logger.info("Extraction Summary:")
                logger.info(f"  • Games collected: {extraction_summary['games_collected']}")
                logger.info(f"  • Games stored: {extraction_summary['games_stored']}")
                logger.info(f"  • Betting records: {extraction_summary['betting_records_stored']}")
                logger.info(f"  • Success rate: {extraction_summary['scraping_success_rate']:.1f}%")
                logger.info(f"  • Duration: {extraction_summary['duration_seconds']:.1f} seconds")
                
                return extraction_summary
                
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            return {
                'status': 'FAILED',
                'error': str(e),
                'date': self.target_date.strftime('%Y-%m-%d')
            }
    
    async def run_verification(self) -> Dict[str, Any]:
        """
        Run comprehensive verification of the stored data.
        
        Returns:
            Dictionary with verification results
        """
        try:
            verification_results = {
                'status': 'SUCCESS',
                'date': self.target_date.strftime('%Y-%m-%d'),
                'checks_passed': 0,
                'checks_failed': 0,
                'total_checks': 0,
                'detailed_results': {},
                'summary': {},
                'issues': []
            }
            
            # Database verification checks
            checks = [
                ('games_count', self.verify_games_count),
                ('games_data_quality', self.verify_games_data_quality),
                ('betting_data_count', self.verify_betting_data_count),
                ('betting_data_quality', self.verify_betting_data_quality),
                ('data_consistency', self.verify_data_consistency),
                ('sportsbook_coverage', self.verify_sportsbook_coverage),
                ('team_normalization', self.verify_team_normalization),
                ('date_accuracy', self.verify_date_accuracy)
            ]
            
            verification_results['total_checks'] = len(checks)
            
            # Run each verification check
            for check_name, check_function in checks:
                logger.info(f"Running check: {check_name}")
                
                try:
                    check_result = await check_function()
                    verification_results['detailed_results'][check_name] = check_result
                    
                    if check_result.get('passed', False):
                        verification_results['checks_passed'] += 1
                        logger.info(f"  ✅ {check_name}: PASSED")
                    else:
                        verification_results['checks_failed'] += 1
                        logger.error(f"  ❌ {check_name}: FAILED")
                        verification_results['issues'].append({
                            'check': check_name,
                            'issue': check_result.get('message', 'Unknown issue')
                        })
                    
                except Exception as e:
                    verification_results['checks_failed'] += 1
                    logger.error(f"  ❌ {check_name}: ERROR - {e}")
                    verification_results['issues'].append({
                        'check': check_name,
                        'issue': f"Check failed with error: {e}"
                    })
            
            # Generate summary
            success_rate = (verification_results['checks_passed'] / verification_results['total_checks']) * 100
            verification_results['summary'] = {
                'success_rate': success_rate,
                'total_games': verification_results['detailed_results'].get('games_count', {}).get('count', 0),
                'total_betting_records': verification_results['detailed_results'].get('betting_data_count', {}).get('total_records', 0),
                'sportsbooks_found': verification_results['detailed_results'].get('sportsbook_coverage', {}).get('unique_sportsbooks', 0)
            }
            
            # Overall status
            if verification_results['checks_failed'] == 0:
                verification_results['status'] = 'SUCCESS'
            elif verification_results['checks_passed'] > verification_results['checks_failed']:
                verification_results['status'] = 'PARTIAL_SUCCESS'
            else:
                verification_results['status'] = 'FAILED'
            
            # Print summary
            logger.info("Verification Summary:")
            logger.info(f"  • Checks passed: {verification_results['checks_passed']}/{verification_results['total_checks']}")
            logger.info(f"  • Success rate: {success_rate:.1f}%")
            logger.info(f"  • Total games: {verification_results['summary']['total_games']}")
            logger.info(f"  • Total betting records: {verification_results['summary']['total_betting_records']}")
            logger.info(f"  • Sportsbooks found: {verification_results['summary']['sportsbooks_found']}")
            
            return verification_results
            
        except Exception as e:
            logger.error(f"Verification failed: {e}")
            return {
                'status': 'FAILED',
                'error': str(e),
                'date': self.target_date.strftime('%Y-%m-%d')
            }
    
    async def verify_games_count(self) -> Dict[str, Any]:
        """Verify that games were stored for the target date."""
        async with self.db_pool.acquire() as conn:
            query = """
                SELECT COUNT(*) as game_count
                FROM public.games 
                WHERE game_date = $1
            """
            result = await conn.fetchrow(query, self.target_date)
            
            count = result['game_count']
            
            return {
                'passed': count > 0,
                'count': count,
                'message': f"Found {count} games for {self.target_date}" if count > 0 else f"No games found for {self.target_date}"
            }
    
    async def verify_games_data_quality(self) -> Dict[str, Any]:
        """Verify the quality of stored game data."""
        async with self.db_pool.acquire() as conn:
            query = """
                SELECT 
                    COUNT(*) as total_games,
                    COUNT(CASE WHEN home_team IS NOT NULL AND away_team IS NOT NULL THEN 1 END) as games_with_teams,
                    COUNT(CASE WHEN game_datetime IS NOT NULL THEN 1 END) as games_with_datetime,
                    COUNT(CASE WHEN sportsbookreview_game_id IS NOT NULL THEN 1 END) as games_with_sbr_id,
                    COUNT(CASE WHEN home_team IS NULL OR away_team IS NULL THEN 1 END) as games_missing_teams
                FROM public.games 
                WHERE game_date = $1
            """
            result = await conn.fetchrow(query, self.target_date)
            
            total = result['total_games']
            complete_games = result['games_with_teams']
            missing_teams = result['games_missing_teams']
            
            return {
                'passed': missing_teams == 0 and complete_games == total,
                'total_games': total,
                'complete_games': complete_games,
                'missing_teams': missing_teams,
                'message': f"Data quality: {complete_games}/{total} games complete, {missing_teams} missing teams"
            }
    
    async def verify_betting_data_count(self) -> Dict[str, Any]:
        """Verify that betting data was stored."""
        async with self.db_pool.acquire() as conn:
            # Count betting records across all tables
            queries = {
                'moneyline': """
                    SELECT COUNT(*) as count FROM mlb_betting.moneyline m
                    JOIN public.games g ON m.game_id = g.id
                    WHERE g.game_date = $1
                """,
                'spreads': """
                    SELECT COUNT(*) as count FROM mlb_betting.spreads s
                    JOIN public.games g ON s.game_id = g.id
                    WHERE g.game_date = $1
                """,
                'totals': """
                    SELECT COUNT(*) as count FROM mlb_betting.totals t
                    JOIN public.games g ON t.game_id = g.id
                    WHERE g.game_date = $1
                """
            }
            
            counts = {}
            total_records = 0
            
            for bet_type, query in queries.items():
                result = await conn.fetchrow(query, self.target_date)
                counts[bet_type] = result['count']
                total_records += result['count']
            
            return {
                'passed': total_records > 0,
                'total_records': total_records,
                'by_bet_type': counts,
                'message': f"Found {total_records} betting records ({counts['moneyline']} ML, {counts['spreads']} spreads, {counts['totals']} totals)"
            }
    
    async def verify_betting_data_quality(self) -> Dict[str, Any]:
        """Verify the quality of betting data."""
        async with self.db_pool.acquire() as conn:
            query = """
                SELECT 
                    'moneyline' as bet_type,
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN home_ml IS NOT NULL AND away_ml IS NOT NULL THEN 1 END) as complete_records,
                    COUNT(CASE WHEN sportsbook IS NOT NULL THEN 1 END) as records_with_sportsbook
                FROM mlb_betting.moneyline m
                JOIN public.games g ON m.game_id = g.id
                WHERE g.game_date = $1
                
                UNION ALL
                
                SELECT 
                    'spreads' as bet_type,
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN home_spread IS NOT NULL AND away_spread IS NOT NULL THEN 1 END) as complete_records,
                    COUNT(CASE WHEN sportsbook IS NOT NULL THEN 1 END) as records_with_sportsbook
                FROM mlb_betting.spreads s
                JOIN public.games g ON s.game_id = g.id
                WHERE g.game_date = $1
                
                UNION ALL
                
                SELECT 
                    'totals' as bet_type,
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN total_line IS NOT NULL THEN 1 END) as complete_records,
                    COUNT(CASE WHEN sportsbook IS NOT NULL THEN 1 END) as records_with_sportsbook
                FROM mlb_betting.totals t
                JOIN public.games g ON t.game_id = g.id
                WHERE g.game_date = $1
            """
            
            results = await conn.fetch(query, self.target_date)
            
            quality_data = {}
            total_complete = 0
            total_records = 0
            
            for row in results:
                bet_type = row['bet_type']
                quality_data[bet_type] = {
                    'total': row['total_records'],
                    'complete': row['complete_records'],
                    'with_sportsbook': row['records_with_sportsbook']
                }
                total_complete += row['complete_records']
                total_records += row['total_records']
            
            quality_rate = (total_complete / max(total_records, 1)) * 100
            
            return {
                'passed': quality_rate >= 80,  # 80% threshold
                'quality_rate': quality_rate,
                'total_records': total_records,
                'complete_records': total_complete,
                'by_bet_type': quality_data,
                'message': f"Betting data quality: {quality_rate:.1f}% complete ({total_complete}/{total_records})"
            }
    
    async def verify_data_consistency(self) -> Dict[str, Any]:
        """Verify data consistency across tables."""
        async with self.db_pool.acquire() as conn:
            # Check for orphaned betting records
            query = """
                SELECT 
                    (SELECT COUNT(*) FROM mlb_betting.moneyline WHERE game_id NOT IN (SELECT id FROM public.games)) as orphaned_moneyline,
                    (SELECT COUNT(*) FROM mlb_betting.spreads WHERE game_id NOT IN (SELECT id FROM public.games)) as orphaned_spreads,
                    (SELECT COUNT(*) FROM mlb_betting.totals WHERE game_id NOT IN (SELECT id FROM public.games)) as orphaned_totals
            """
            
            result = await conn.fetchrow(query)
            
            total_orphaned = result['orphaned_moneyline'] + result['orphaned_spreads'] + result['orphaned_totals']
            
            return {
                'passed': total_orphaned == 0,
                'orphaned_records': total_orphaned,
                'orphaned_by_type': {
                    'moneyline': result['orphaned_moneyline'],
                    'spreads': result['orphaned_spreads'],
                    'totals': result['orphaned_totals']
                },
                'message': f"Data consistency: {total_orphaned} orphaned records found" if total_orphaned > 0 else "No orphaned records found"
            }
    
    async def verify_sportsbook_coverage(self) -> Dict[str, Any]:
        """Verify sportsbook coverage in the data."""
        async with self.db_pool.acquire() as conn:
            query = """
                SELECT sportsbook, COUNT(*) as record_count
                FROM (
                    SELECT sportsbook FROM mlb_betting.moneyline m
                    JOIN public.games g ON m.game_id = g.id
                    WHERE g.game_date = $1
                    
                    UNION ALL
                    
                    SELECT sportsbook FROM mlb_betting.spreads s
                    JOIN public.games g ON s.game_id = g.id
                    WHERE g.game_date = $1
                    
                    UNION ALL
                    
                    SELECT sportsbook FROM mlb_betting.totals t
                    JOIN public.games g ON t.game_id = g.id
                    WHERE g.game_date = $1
                ) combined
                GROUP BY sportsbook
                ORDER BY record_count DESC
            """
            
            results = await conn.fetch(query, self.target_date)
            
            sportsbook_data = {}
            for row in results:
                sportsbook_data[row['sportsbook']] = row['record_count']
            
            unique_sportsbooks = len(sportsbook_data)
            expected_min_sportsbooks = 3  # Minimum expected sportsbooks
            
            return {
                'passed': unique_sportsbooks >= expected_min_sportsbooks,
                'unique_sportsbooks': unique_sportsbooks,
                'sportsbook_breakdown': sportsbook_data,
                'message': f"Sportsbook coverage: {unique_sportsbooks} unique sportsbooks found"
            }
    
    async def verify_team_normalization(self) -> Dict[str, Any]:
        """Verify that team names are properly normalized."""
        async with self.db_pool.acquire() as conn:
            query = """
                SELECT DISTINCT home_team, away_team
                FROM public.games
                WHERE game_date = $1
            """
            
            results = await conn.fetch(query, self.target_date)
            
            teams_found = set()
            for row in results:
                teams_found.add(row['home_team'])
                teams_found.add(row['away_team'])
            
            # Check for invalid team codes (should be 2-3 characters)
            invalid_teams = [team for team in teams_found if not team or len(team) < 2 or len(team) > 3]
            
            return {
                'passed': len(invalid_teams) == 0,
                'total_teams': len(teams_found),
                'invalid_teams': invalid_teams,
                'all_teams': sorted(list(teams_found)),
                'message': f"Team normalization: {len(teams_found)} teams found, {len(invalid_teams)} invalid"
            }
    
    async def verify_date_accuracy(self) -> Dict[str, Any]:
        """Verify that all stored games have the correct date."""
        async with self.db_pool.acquire() as conn:
            query = """
                SELECT 
                    COUNT(*) as total_games,
                    COUNT(CASE WHEN DATE(game_date) = $1 THEN 1 END) as correct_date_games,
                    COUNT(CASE WHEN DATE(game_date) != $1 THEN 1 END) as incorrect_date_games
                FROM public.games
                WHERE ABS(DATE(game_date) - $1) <= 1
            """
            
            result = await conn.fetchrow(query, self.target_date)
            
            total = result['total_games']
            correct = result['correct_date_games']
            incorrect = result['incorrect_date_games']
            
            return {
                'passed': incorrect == 0,
                'total_games': total,
                'correct_date_games': correct,
                'incorrect_date_games': incorrect,
                'message': f"Date accuracy: {correct}/{total} games with correct date, {incorrect} incorrect"
            }
    
    def print_summary_report(self, results: Dict[str, Any]):
        """Print a summary report of the process."""
        print("\n" + "=" * 80)
        print("SPORTSBOOKREVIEW DAILY EXTRACT SUMMARY REPORT")
        print("=" * 80)
        print(f"Date: {results['date']}")
        print(f"Overall Status: {results['overall_status']}")
        print(f"Started: {results['started_at']}")
        print(f"Completed: {results.get('completed_at', 'N/A')}")
        
        # Extraction summary
        if results.get('extraction_results'):
            ext = results['extraction_results']
            print(f"\nEXTRACTION RESULTS:")
            print(f"  Status: {ext.get('status', 'N/A')}")
            print(f"  Games Collected: {ext.get('games_collected', 0)}")
            print(f"  Games Stored: {ext.get('games_stored', 0)}")
            print(f"  Betting Records: {ext.get('betting_records_stored', 0)}")
            print(f"  Success Rate: {ext.get('scraping_success_rate', 0):.1f}%")
            print(f"  Duration: {ext.get('duration_seconds', 0):.1f} seconds")
        
        # Verification summary
        if results.get('verification_results'):
            ver = results['verification_results']
            print(f"\nVERIFICATION RESULTS:")
            print(f"  Status: {ver.get('status', 'N/A')}")
            print(f"  Checks Passed: {ver.get('checks_passed', 0)}/{ver.get('total_checks', 0)}")
            print(f"  Success Rate: {ver.get('summary', {}).get('success_rate', 0):.1f}%")
            print(f"  Total Games: {ver.get('summary', {}).get('total_games', 0)}")
            print(f"  Total Betting Records: {ver.get('summary', {}).get('total_betting_records', 0)}")
            print(f"  Sportsbooks Found: {ver.get('summary', {}).get('sportsbooks_found', 0)}")
            
            # Issues
            if ver.get('issues'):
                print(f"\nISSUES FOUND:")
                for issue in ver['issues']:
                    print(f"  • {issue['check']}: {issue['issue']}")
        
        print("=" * 80)


def parse_date(date_str: str) -> date:
    """Parse date string in various formats."""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        try:
            return datetime.strptime(date_str, '%m/%d/%Y').date()
        except ValueError:
            try:
                return datetime.strptime(date_str, '%Y%m%d').date()
            except ValueError:
                raise ValueError(f"Unable to parse date: {date_str}. Use format: YYYY-MM-DD")


async def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description='Run SportsbookReview daily extract and verification',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract and verify data for July 5, 2025
  python run_daily_sportsbookreview_extract.py --date 2025-07-05
  
  # Only verify existing data (skip extraction)
  python run_daily_sportsbookreview_extract.py --date 2025-07-05 --verify-only
  
  # Extract yesterday's data
  python run_daily_sportsbookreview_extract.py --date yesterday
        """
    )
    
    parser.add_argument(
        '--date',
        type=str,
        required=True,
        help='Date to extract/verify (YYYY-MM-DD format, or "yesterday", "today")'
    )
    
    parser.add_argument(
        '--verify-only',
        action='store_true',
        help='Only run verification, skip data extraction'
    )
    
    parser.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    # Configure logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Parse date
    try:
        if args.date.lower() == 'yesterday':
            target_date = date.today() - timedelta(days=1)
        elif args.date.lower() == 'today':
            target_date = date.today()
        else:
            target_date = parse_date(args.date)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    # Run the process
    try:
        async with DailyExtractRunner(target_date, args.verify_only) as runner:
            results = await runner.run_complete_process()
            
            # Print summary report
            runner.print_summary_report(results)
            
            # Exit with appropriate code
            if results['overall_status'] == 'SUCCESS':
                print("\n✅ Process completed successfully!")
                sys.exit(0)
            else:
                print("\n❌ Process completed with issues!")
                sys.exit(1)
                
    except Exception as e:
        logger.error(f"Process failed: {e}")
        print(f"\n❌ Process failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main()) 