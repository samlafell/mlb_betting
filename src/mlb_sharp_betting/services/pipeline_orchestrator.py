"""
Pipeline Orchestration Service

Service to orchestrate the complete pipeline with intelligent decisions.
Analyzes system state and executes only the necessary steps.
"""

from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import structlog

from mlb_sharp_betting.utils.timezone_utils import hours_since

from ..db.connection import DatabaseManager, get_db_manager
from .backtesting_engine import get_backtesting_engine
from ..entrypoint import DataPipeline

logger = structlog.get_logger(__name__)


class PipelineOrchestrator:
    """
    Orchestrates the complete data -> backtesting -> detection pipeline
    with intelligent decision making about what steps are needed.
    """
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        Initialize the pipeline orchestrator.
        
        Args:
            db_manager: Database manager instance
        """
        self.db_manager = db_manager or get_db_manager()
        self.logger = logger.bind(service="pipeline_orchestrator")
        
        # Initialize backtesting engine
        self.backtesting_engine = get_backtesting_engine()
        self.backtesting_engine.db_manager = self.db_manager
    
    async def analyze_system_state(self) -> Dict[str, Any]:
        """
        Analyze current system state to determine what needs to be run.
        
        Returns:
            Dict with system state analysis and recommendations
        """
        
        system_state = {
            'needs_data_collection': False,
            'needs_backtesting': False,
            'data_age_hours': None,
            'backtesting_age_hours': None,
            'recommendations': [],
            'data_quality_issues': [],
            'system_health': 'unknown'
        }
        
        try:
            with self.db_manager.get_cursor() as cursor:
                # Check data freshness
                self.logger.info("🔍 Analyzing data freshness...")
                
                cursor.execute("""
                    SELECT 
                        MAX(last_updated) as latest_update,
                        COUNT(*) as total_splits,
                        COUNT(DISTINCT game_id) as unique_games,
                        COUNT(DISTINCT DATE(game_datetime)) as game_dates
                    FROM splits.raw_mlb_betting_splits
                """)
                data_result = cursor.fetchone()
                
                if data_result and data_result['latest_update']:
                    data_age = hours_since(data_result['latest_update'])
                    system_state['data_age_hours'] = data_age
                    system_state['needs_data_collection'] = data_age > 6
                    
                    # Data quality checks
                    if data_result['total_splits'] < 10:
                        system_state['data_quality_issues'].append("Low split count")
                    if data_result['unique_games'] < 5:
                        system_state['data_quality_issues'].append("Insufficient game coverage")
                    
                    self.logger.info(
                        "📊 Data analysis complete",
                        age_hours=data_age,
                        total_splits=data_result['total_splits'],
                        unique_games=data_result['unique_games']
                    )
                else:
                    system_state['needs_data_collection'] = True
                    system_state['recommendations'].append("No betting data found - collection required")
                    system_state['data_quality_issues'].append("No data available")
                
                # Check backtesting freshness
                self.logger.info("🔬 Analyzing backtesting status...")
                
                cursor.execute("""
                    SELECT 
                        MAX(created_at) as latest_backtest,
                        COUNT(*) as total_results,
                        COUNT(DISTINCT strategy_name) as unique_strategies
                    FROM backtesting.strategy_performance
                    WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
                """)
                backtest_result = cursor.fetchone()
                
                if backtest_result and backtest_result['latest_backtest']:
                    backtest_age = hours_since(backtest_result['latest_backtest'])
                    system_state['backtesting_age_hours'] = backtest_age
                    system_state['needs_backtesting'] = (
                        backtest_age > 24 or 
                        system_state['needs_data_collection'] or  # Always re-backtest after fresh data
                        backtest_result['total_results'] < 5  # Insufficient backtest results
                    )
                    
                    if backtest_result['unique_strategies'] < 3:
                        system_state['data_quality_issues'].append("Limited strategy coverage in backtesting")
                    
                    self.logger.info(
                        "🔬 Backtesting analysis complete",
                        age_hours=backtest_age,
                        total_results=backtest_result['total_results'],
                        unique_strategies=backtest_result['unique_strategies']
                    )
                else:
                    system_state['needs_backtesting'] = True
                    system_state['recommendations'].append("No backtesting results found - analysis required")
                
                # Check game outcomes availability
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_outcomes,
                                                  MAX(updated_at) as latest_outcome_update
                    FROM public.game_outcomes
                                          WHERE game_date >= CURRENT_DATE - INTERVAL '30 days'
                """)
                outcomes_result = cursor.fetchone()
                
                if not outcomes_result or outcomes_result['total_outcomes'] < 10:
                    system_state['data_quality_issues'].append("Insufficient game outcomes for backtesting")
                
                # Determine overall system health
                if not system_state['data_quality_issues']:
                    system_state['system_health'] = 'excellent'
                elif len(system_state['data_quality_issues']) <= 2:
                    system_state['system_health'] = 'good'
                elif len(system_state['data_quality_issues']) <= 4:
                    system_state['system_health'] = 'fair'
                else:
                    system_state['system_health'] = 'poor'
                
                # Generate recommendations
                if system_state['needs_data_collection']:
                    system_state['recommendations'].append("Fresh data collection recommended")
                if system_state['needs_backtesting']:
                    system_state['recommendations'].append("Strategy backtesting recommended")
                if not system_state['needs_data_collection'] and not system_state['needs_backtesting']:
                    system_state['recommendations'].append("System is up-to-date, ready for detection")
        
        except Exception as e:
            self.logger.error("Failed to analyze system state", error=str(e))
            # Default to running everything if we can't determine state
            system_state.update({
                'needs_data_collection': True,
                'needs_backtesting': True,
                'system_health': 'unknown',
                'recommendations': [f"System analysis failed: {e}"],
                'data_quality_issues': ['Analysis failure - defaulting to full pipeline']
            })
        
        return system_state
    
    async def execute_smart_pipeline(self, detection_minutes: int = 60, 
                                   force_fresh_data: bool = False,
                                   force_backtesting: bool = False) -> Dict[str, Any]:
        """
        Execute the complete pipeline with intelligent step selection.
        
        Args:
            detection_minutes: Minutes ahead to look for opportunities
            force_fresh_data: Force data collection regardless of freshness
            force_backtesting: Force backtesting regardless of age
            
        Returns:
            Dict with execution results and metrics
        """
        start_time = datetime.now()
        self.logger.info("🚀 Starting smart pipeline execution")
        
        # Analyze what needs to be done
        system_state = await self.analyze_system_state()
        
        # Apply force flags
        if force_fresh_data:
            system_state['needs_data_collection'] = True
            system_state['needs_backtesting'] = True  # Always backtest after forced data collection
            self.logger.info("🔄 Forced fresh data collection enabled")
        
        if force_backtesting:
            system_state['needs_backtesting'] = True
            self.logger.info("🔄 Forced backtesting enabled")
        
        execution_results = {
            'system_state': system_state,
            'steps_executed': [],
            'data_collection_metrics': None,
            'backtesting_results': None,
            'detection_results': None,
            'cross_market_flips': None,
            'total_execution_time': None,
            'errors': [],
            'warnings': []
        }
        
        try:
            # Step 1: Data collection if needed
            if system_state['needs_data_collection']:
                self.logger.info("📡 Executing data collection step")
                
                try:
                    data_pipeline = DataPipeline(sport='mlb', sportsbook='circa', dry_run=False)
                    data_metrics = await data_pipeline.run()
                    
                    execution_results['data_collection_metrics'] = data_metrics
                    execution_results['steps_executed'].append('data_collection')
                    
                    self.logger.info(
                        "✅ Data collection completed",
                        records_processed=data_metrics.get('parsed_records', 0),
                        sharp_indicators=data_metrics.get('sharp_indicators', 0)
                    )
                    
                except Exception as e:
                    error_msg = f"Data collection failed: {str(e)}"
                    self.logger.error(error_msg)
                    execution_results['errors'].append(error_msg)
                    execution_results['warnings'].append("Continuing with existing data")
            
            # Step 2: Backtesting if needed  
            if system_state['needs_backtesting']:
                self.logger.info("🔬 Executing backtesting step")
                
                try:
                    await self.backtesting_engine.initialize()
                    backtest_results = await self.backtesting_engine.run_daily_pipeline()
                    execution_results['backtesting_results'] = backtest_results
                    execution_results['steps_executed'].append('backtesting')
                    
                    self.logger.info(
                        "✅ Backtesting completed",
                        backtest_results=str(backtest_results)
                    )
                    
                except Exception as e:
                    error_msg = f"Backtesting failed: {str(e)}"
                    self.logger.error(error_msg)
                    execution_results['errors'].append(error_msg)
                    execution_results['warnings'].append("Detection will proceed without fresh backtesting")
            
            # Step 3: Opportunity Detection (always run)
            self.logger.info("🎯 Executing opportunity detection step")
            
            try:
                # Import the new orchestrator-based detector
                from mlb_sharp_betting.services.adaptive_detector import AdaptiveBettingDetector
                from mlb_sharp_betting.services.cross_market_flip_detector import CrossMarketFlipDetector
                
                # Run main detection using new orchestrator
                detector = AdaptiveBettingDetector()
                detection_results = await detector.analyze_opportunities(detection_minutes)
                execution_results['detection_results'] = detection_results
                
                # Run cross-market flip detection
                flip_detector = CrossMarketFlipDetector(self.db_manager)
                cross_market_flips, flip_summary = await flip_detector.detect_todays_flips_with_summary(
                    min_confidence=60.0
                )
                execution_results['cross_market_flips'] = {
                    'flips': cross_market_flips,
                    'summary': flip_summary
                }
                
                execution_results['steps_executed'].append('detection')
                
                self.logger.info(
                    "✅ Detection completed",
                    games_analyzed=len(detection_results.games),
                    cross_market_flips=len(cross_market_flips)
                )
                
            except Exception as e:
                error_msg = f"Detection failed: {str(e)}"
                self.logger.error(error_msg)
                execution_results['errors'].append(error_msg)
                # Don't re-raise for detection errors - return partial results
            
            # Calculate total execution time
            execution_results['total_execution_time'] = (datetime.now() - start_time).total_seconds()
            
            self.logger.info(
                "🎉 Smart pipeline execution completed",
                execution_time=f"{execution_results['total_execution_time']:.2f}s",
                steps_executed=execution_results['steps_executed'],
                errors=len(execution_results['errors']),
                warnings=len(execution_results['warnings'])
            )
            
            return execution_results
            
        except Exception as e:
            self.logger.error("❌ Smart pipeline execution failed", error=str(e))
            execution_results['errors'].append(str(e))
            execution_results['total_execution_time'] = (datetime.now() - start_time).total_seconds()
            raise
    
    async def get_pipeline_recommendations(self) -> Dict[str, Any]:
        """
        Get intelligent recommendations for pipeline execution.
        
        Returns:
            Dict with specific recommendations and reasoning
        """
        system_state = await self.analyze_system_state()
        
        recommendations = {
            'immediate_actions': [],
            'optional_actions': [],
            'system_health': system_state['system_health'],
            'priority_level': 'low',
            'estimated_runtime_minutes': 0,
            'reasoning': []
        }
        
        runtime_estimate = 0
        
        # Analyze what needs to be done
        if system_state['needs_data_collection']:
            recommendations['immediate_actions'].append({
                'action': 'data_collection',
                'reason': f"Data is {system_state.get('data_age_hours') or 0:.1f} hours old",
                'estimated_minutes': 3
            })
            runtime_estimate += 3
            recommendations['priority_level'] = 'high'
        
        if system_state['needs_backtesting']:
            recommendations['immediate_actions'].append({
                'action': 'backtesting',
                'reason': f"Backtesting is {system_state.get('backtesting_age_hours') or 0:.1f} hours old",
                'estimated_minutes': 5
            })
            runtime_estimate += 5
            recommendations['priority_level'] = 'high'
        
        # Always recommend detection
        recommendations['immediate_actions'].append({
            'action': 'detection',
            'reason': "Find current betting opportunities",
            'estimated_minutes': 2
        })
        runtime_estimate += 2
        
        # Optional actions based on data quality
        if system_state['data_quality_issues']:
            recommendations['optional_actions'].append({
                'action': 'data_quality_review',
                'reason': f"Found {len(system_state['data_quality_issues'])} data quality issues",
                'issues': system_state['data_quality_issues']
            })
        
        recommendations['estimated_runtime_minutes'] = runtime_estimate
        recommendations['reasoning'] = system_state['recommendations']
        
        return recommendations
    
    def close(self):
        """Clean up resources."""
        try:
            if hasattr(self, 'db_manager') and self.db_manager:
                self.db_manager.close()
        except Exception as e:
            self.logger.warning("Error during cleanup", error=str(e)) 