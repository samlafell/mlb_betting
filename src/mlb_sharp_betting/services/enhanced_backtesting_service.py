"""
Enhanced Backtesting Service

This service extends the existing BacktestingService with integrated data collection.
Provides a complete pipeline that ensures fresh data before running backtesting.
"""

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional
import structlog

from mlb_sharp_betting.utils.timezone_utils import hours_since

from .backtesting_service import BacktestingService, BacktestingResults
from ..entrypoint import DataPipeline
from ..db.connection import DatabaseManager, get_db_manager

logger = structlog.get_logger(__name__)


class EnhancedBacktestingService(BacktestingService):
    """BacktestingService with integrated data collection."""
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None, auto_collect_data: bool = True):
        """
        Initialize enhanced backtesting service.
        
        Args:
            db_manager: Database manager instance
            auto_collect_data: Whether to automatically collect fresh data before backtesting
        """
        super().__init__(db_manager)
        self.auto_collect_data = auto_collect_data
        self.logger = logger.bind(service="enhanced_backtesting")
        
        # Initialize data pipeline for fresh data collection
        self.data_pipeline = DataPipeline(
            sport='mlb',
            sportsbook='circa',
            dry_run=False
        )
    
    async def run_daily_backtesting_pipeline_with_fresh_data(self) -> Dict[str, Any]:
        """
        Enhanced pipeline that ensures fresh data before backtesting.
        
        Returns:
            Dict containing both data collection metrics and backtesting results
        """
        start_time = datetime.now()
        self.logger.info("🚀 Starting enhanced backtesting pipeline with fresh data")
        
        pipeline_results = {
            'data_collection_metrics': None,
            'backtesting_results': None,
            'execution_time_seconds': 0,
            'steps_executed': [],
            'errors': []
        }
        
        try:
            # Step 1: Collect fresh data if enabled
            if self.auto_collect_data:
                self.logger.info("📡 Collecting fresh betting data...")
                
                try:
                    data_metrics = await self.data_pipeline.run()
                    pipeline_results['data_collection_metrics'] = data_metrics
                    pipeline_results['steps_executed'].append('data_collection')
                    
                    self.logger.info(
                        "✅ Data collection completed",
                        records_processed=data_metrics.get('parsed_records', 0),
                        sharp_indicators=data_metrics.get('sharp_indicators', 0)
                    )
                    
                except Exception as e:
                    error_msg = f"Data collection failed: {str(e)}"
                    self.logger.error(error_msg)
                    pipeline_results['errors'].append(error_msg)
                    # Continue with backtesting using existing data
            
            # Step 2: Run comprehensive backtesting
            self.logger.info("🔬 Running comprehensive strategy backtesting...")
            
            try:
                backtest_results = await self.run_daily_backtesting_pipeline()
                pipeline_results['backtesting_results'] = backtest_results
                pipeline_results['steps_executed'].append('backtesting')
                
                self.logger.info(
                    "✅ Backtesting completed",
                    strategies_analyzed=backtest_results.total_strategies_analyzed,
                    profitable_strategies=backtest_results.profitable_strategies
                )
                
            except Exception as e:
                error_msg = f"Backtesting failed: {str(e)}"
                self.logger.error(error_msg)
                pipeline_results['errors'].append(error_msg)
                raise
            
            # Calculate total execution time
            execution_time = (datetime.now() - start_time).total_seconds()
            pipeline_results['execution_time_seconds'] = execution_time
            
            self.logger.info(
                "🎉 Enhanced backtesting pipeline completed",
                execution_time=f"{execution_time:.2f}s",
                steps_executed=pipeline_results['steps_executed']
            )
            
            return pipeline_results
            
        except Exception as e:
            self.logger.error("❌ Enhanced backtesting pipeline failed", error=str(e))
            pipeline_results['errors'].append(str(e))
            raise
    
    async def check_data_freshness(self, max_age_hours: int = 6) -> Dict[str, Any]:
        """
        Check if existing data is fresh enough for backtesting.
        
        Args:
            max_age_hours: Maximum age in hours for data to be considered fresh
            
        Returns:
            Dict with freshness status and metrics
        """
        try:
            with self.db_manager.get_cursor() as cursor:
                # Check latest betting splits data
                cursor.execute("""
                    SELECT 
                        MAX(last_updated) as latest_update,
                        COUNT(*) as total_records,
                        COUNT(DISTINCT game_id) as unique_games
                    FROM splits.raw_mlb_betting_splits
                """)
                
                splits_result = cursor.fetchone()
                
                # Check game outcomes data
                cursor.execute("""
                    SELECT 
                        MAX(updated_at) as latest_outcome_update,
                        COUNT(*) as total_outcomes
                    FROM public.game_outcomes
                """)
                
                outcomes_result = cursor.fetchone()
                
                # Calculate data age
                data_age_hours = None
                is_fresh = False
                
                if splits_result['latest_update']:
                    data_age_hours = hours_since(splits_result['latest_update'])
                    is_fresh = data_age_hours <= max_age_hours
                
                return {
                    'is_fresh': is_fresh,
                    'data_age_hours': data_age_hours,
                    'max_age_hours': max_age_hours,
                    'total_splits': splits_result['total_records'] or 0,
                    'unique_games': splits_result['unique_games'] or 0,
                    'total_outcomes': outcomes_result['total_outcomes'] or 0,
                    'latest_splits_update': splits_result['latest_update'],
                    'latest_outcomes_update': outcomes_result['latest_outcome_update'],
                    'needs_collection': not is_fresh
                }
                
        except Exception as e:
            self.logger.error("Failed to check data freshness", error=str(e))
            return {
                'is_fresh': False,
                'data_age_hours': None,
                'needs_collection': True,
                'error': str(e)
            }
    
    async def run_conditional_pipeline(self, max_data_age_hours: int = 6) -> Dict[str, Any]:
        """
        Run pipeline with conditional data collection based on data freshness.
        
        Args:
            max_data_age_hours: Maximum age for data to be considered fresh
            
        Returns:
            Pipeline results with freshness analysis
        """
        self.logger.info("🔍 Checking data freshness before pipeline execution")
        
        # Check data freshness
        freshness_check = await self.check_data_freshness(max_data_age_hours)
        
        if freshness_check['is_fresh']:
            self.logger.info(
                "✅ Data is fresh, running backtesting only",
                data_age_hours=freshness_check['data_age_hours']
            )
            
            # Run backtesting only
            backtest_results = await self.run_daily_backtesting_pipeline()
            
            return {
                'data_collection_metrics': None,
                'backtesting_results': backtest_results,
                'freshness_check': freshness_check,
                'steps_executed': ['backtesting'],
                'reason': 'Data was fresh enough, skipped collection'
            }
        else:
            self.logger.info(
                "📡 Data is stale, running full pipeline with collection",
                data_age_hours=freshness_check.get('data_age_hours', 'unknown')
            )
            
            # Run full pipeline with data collection
            results = await self.run_daily_backtesting_pipeline_with_fresh_data()
            results['freshness_check'] = freshness_check
            results['reason'] = 'Data was stale, ran full collection + backtesting'
            
            return results
    
    async def validate_pipeline_requirements(self) -> Dict[str, bool]:
        """
        Validate that all pipeline requirements are met.
        
        Returns:
            Dict with validation results for each requirement
        """
        validations = {
            'database_connection': False,
            'schema_exists': False,
            'data_pipeline_ready': False,
            'backtesting_service_ready': False
        }
        
        try:
            # Test database connection
            with self.db_manager.get_cursor() as cursor:
                cursor.execute("SELECT 1")
                validations['database_connection'] = True
            
            # Check schema existence
            with self.db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT schema_name 
                    FROM information_schema.schemata 
                    WHERE schema_name IN ('splits', 'main', 'backtesting')
                """)
                schemas = [row['schema_name'] for row in cursor.fetchall()]
                validations['schema_exists'] = all(s in schemas for s in ['splits', 'main'])
            
            # Test data pipeline
            try:
                # Create a test pipeline instance
                test_pipeline = DataPipeline(sport='mlb', sportsbook='circa', dry_run=True)
                validations['data_pipeline_ready'] = True
            except Exception:
                validations['data_pipeline_ready'] = False
            
            # Test backtesting service
            try:
                await self._validate_data_quality()
                validations['backtesting_service_ready'] = True
            except Exception:
                validations['backtesting_service_ready'] = False
                
        except Exception as e:
            self.logger.error("Pipeline validation failed", error=str(e))
        
        return validations 