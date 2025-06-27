#!/usr/bin/env python3
"""
Test Script for Existing Strategy Processors

Tests all implemented processors to validate they're working correctly:
- ConsensusProcessor (new)
- UnderdogValueProcessor (existing) 
- LineMovementProcessor (new)
- RealTimeProcessor (sharp action)
- BettingRecommendationFormatter (new)

Focus on signal generation, confidence scoring, and actionable output.
"""

import asyncio
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent / "src"
sys.path.insert(0, str(project_root))

from mlb_sharp_betting.core.logging import get_logger
from mlb_sharp_betting.core.config import get_settings
from mlb_sharp_betting.db.connection import get_db_connection
from mlb_sharp_betting.models.betting_analysis import SignalProcessorConfig, ProfitableStrategy
from mlb_sharp_betting.services.betting_signal_repository import BettingSignalRepository
from mlb_sharp_betting.services.strategy_validator import StrategyValidator, StrategyThresholds
from mlb_sharp_betting.analysis.processors.strategy_processor_factory import StrategyProcessorFactory
from mlb_sharp_betting.services.betting_recommendation_formatter import BettingRecommendationFormatter


class ProcessorTestRunner:
    """Comprehensive test runner for strategy processors"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.config = get_settings()
        self.test_results = {}
        
    async def run_all_tests(self):
        """Run comprehensive tests on all implemented processors"""
        self.logger.info("🧪 Starting comprehensive processor testing...")
        
        try:
            # Initialize components
            await self._initialize_components()
            
            # Test individual processors
            await self._test_processor_creation()
            await self._test_data_access()
            await self._test_processor_execution()
            await self._test_recommendation_formatting()
            
            # Summary
            self._print_test_summary()
            
        except Exception as e:
            self.logger.error(f"❌ Test suite failed: {e}")
            self.logger.error(traceback.format_exc())
    
    async def _initialize_components(self):
        """Initialize all required components for testing"""
        self.logger.info("🔧 Initializing test components...")
        
        # Database manager
        from mlb_sharp_betting.db.connection import get_db_manager
        self.db = get_db_manager()
        
        # Processor config
        self.processor_config = SignalProcessorConfig(
            minimum_differential=5.0,
            maximum_differential=80.0,
            data_freshness_hours=4
        )
        
        # Repository
        self.repository = BettingSignalRepository(self.processor_config)
        
        # Strategy validator with mock strategies
        mock_strategies = self._create_mock_strategies()
        strategy_thresholds = StrategyThresholds()
        self.validator = StrategyValidator(mock_strategies, strategy_thresholds)
        
        # Processor factory
        self.factory = StrategyProcessorFactory(
            self.repository, self.validator, self.processor_config
        )
        
        # Recommendation formatter
        self.formatter = BettingRecommendationFormatter()
        
        self.logger.info("✅ Components initialized successfully")
    
    def _create_mock_strategies(self) -> list:
        """Create mock profitable strategies for testing"""
        return [
            ProfitableStrategy(
                strategy_name="consensus_moneyline_heavy",
                source_book="VSIN-draftkings",
                split_type="moneyline",
                win_rate=67.5,
                roi=12.3,
                total_bets=45,
                confidence=0.82,
                ci_lower=9.1,
                ci_upper=15.5
            ),
            ProfitableStrategy(
                strategy_name="underdog_ml_value_away",
                source_book="VSIN-circa",
                split_type="moneyline", 
                win_rate=58.2,
                roi=8.7,
                total_bets=67,
                confidence=0.73,
                ci_lower=5.2,
                ci_upper=12.2
            ),
            ProfitableStrategy(
                strategy_name="sharp_action_high_diff",
                source_book="VSIN-draftkings",
                split_type="moneyline",
                win_rate=62.1,
                roi=11.8,
                total_bets=89,
                confidence=0.78,
                ci_lower=8.5,
                ci_upper=15.1
            ),
            ProfitableStrategy(
                strategy_name="line_movement_reverse",
                source_book="multiple",
                split_type="spread",
                win_rate=64.3,
                roi=13.2,
                total_bets=52,
                confidence=0.80,
                ci_lower=9.8,
                ci_upper=16.6
            )
        ]
    
    async def _test_processor_creation(self):
        """Test that all processors can be created successfully"""
        self.logger.info("🏭 Testing processor creation...")
        
        test_processors = [
            'consensus_moneyline',
            'underdog_ml_value', 
            'line_movement',
            'sharp_action'
        ]
        
        creation_results = {}
        
        for processor_name in test_processors:
            try:
                processor = self.factory.create_processor(processor_name)
                if processor:
                    creation_results[processor_name] = {
                        'status': 'SUCCESS',
                        'class': processor.__class__.__name__,
                        'signal_type': processor.get_signal_type().value,
                        'category': processor.get_strategy_category(),
                        'description': processor.get_strategy_description()
                    }
                    self.logger.info(f"✅ {processor_name}: {processor.__class__.__name__}")
                else:
                    creation_results[processor_name] = {'status': 'FAILED - NOT_IMPLEMENTED'}
                    self.logger.warning(f"❌ {processor_name}: Not implemented")
            except Exception as e:
                creation_results[processor_name] = {'status': f'ERROR - {str(e)}'}
                self.logger.error(f"💥 {processor_name}: {e}")
        
        self.test_results['processor_creation'] = creation_results
        
        success_count = sum(1 for r in creation_results.values() if r['status'] == 'SUCCESS')
        self.logger.info(f"📊 Processor Creation: {success_count}/{len(test_processors)} successful")
    
    async def _test_data_access(self):
        """Test that processors can access required data"""
        self.logger.info("📊 Testing data access...")
        
        try:
            # Test current game data access
            current_time = datetime.now()
            end_time = current_time + timedelta(hours=6)
            
            # Check raw betting splits data
            query = """
            SELECT COUNT(*) as total_records,
                   COUNT(DISTINCT game_id) as unique_games,
                   COUNT(DISTINCT source) as data_sources,
                   COUNT(DISTINCT book) as books,
                   MIN(last_updated) as oldest_data,
                   MAX(last_updated) as newest_data
            FROM splits.raw_mlb_betting_splits 
            WHERE game_datetime BETWEEN %s AND %s;
            """
            
            result = self.db.execute_query(query, (current_time, end_time), fetch=True)
            if result:
                result = result[0]  # Get first row as dict
                
                data_summary = {
                    'total_records': result[0] if result else 0,  # total_records
                    'unique_games': result[1] if result else 0,   # unique_games
                    'data_sources': result[2] if result else 0,   # data_sources
                    'books': result[3] if result else 0,          # books
                    'data_range': f"{result[4]} to {result[5]}" if result and len(result) > 5 else "No data"
                }
            else:
                data_summary = {
                    'total_records': 0,
                    'unique_games': 0,
                    'data_sources': 0,
                    'books': 0,
                    'data_range': "No data"
                }
            
            self.test_results['data_access'] = data_summary
            
            if data_summary['total_records'] > 0:
                self.logger.info(f"✅ Data Access: {data_summary['total_records']} records, {data_summary['unique_games']} games")
            else:
                self.logger.warning("⚠️ Data Access: No current data found")
                
        except Exception as e:
            self.logger.error(f"❌ Data Access Test Failed: {e}")
            self.test_results['data_access'] = {'error': str(e)}
    
    async def _test_processor_execution(self):
        """Test actual processor execution with signal generation"""
        self.logger.info("⚡ Testing processor execution...")
        
        execution_results = {}
        minutes_ahead = 360  # 6 hours ahead
        mock_strategies = self._create_mock_strategies()
        
        processors_to_test = [
            'consensus_moneyline',
            'underdog_ml_value',
            'sharp_action'
        ]
        
        for processor_name in processors_to_test:
            try:
                processor = self.factory.create_processor(processor_name)
                if not processor:
                    execution_results[processor_name] = {'status': 'SKIPPED - Not implemented'}
                    continue
                
                # Execute processor
                start_time = datetime.now()
                signals = await processor.process(minutes_ahead, mock_strategies)
                execution_time = (datetime.now() - start_time).total_seconds()
                
                # Analyze results
                signal_analysis = self._analyze_signals(signals)
                
                execution_results[processor_name] = {
                    'status': 'SUCCESS',
                    'execution_time_seconds': execution_time,
                    'signals_generated': len(signals),
                    'signal_analysis': signal_analysis
                }
                
                self.logger.info(
                    f"✅ {processor_name}: {len(signals)} signals in {execution_time:.2f}s"
                )
                
                # Log sample signals
                if signals:
                    sample_signal = signals[0]
                    self.logger.info(
                        f"   📋 Sample: {sample_signal.home_team} vs {sample_signal.away_team}, "
                        f"Confidence: {sample_signal.confidence:.1%}, "
                        f"Strategy: {sample_signal.strategy_name}"
                    )
                
            except Exception as e:
                execution_results[processor_name] = {
                    'status': 'ERROR',
                    'error': str(e),
                    'traceback': traceback.format_exc()
                }
                self.logger.error(f"💥 {processor_name}: {e}")
        
        self.test_results['processor_execution'] = execution_results
    
    def _analyze_signals(self, signals):
        """Analyze signal quality and characteristics"""
        if not signals:
            return {'count': 0}
        
        confidence_scores = [s.confidence for s in signals]
        signal_types = [s.signal_type.value for s in signals]
        
        return {
            'count': len(signals),
            'avg_confidence': sum(confidence_scores) / len(confidence_scores),
            'min_confidence': min(confidence_scores),
            'max_confidence': max(confidence_scores),
            'signal_types': list(set(signal_types)),
            'high_confidence_signals': sum(1 for c in confidence_scores if c >= 0.75)
        }
    
    async def _test_recommendation_formatting(self):
        """Test the new betting recommendation formatter"""
        self.logger.info("💬 Testing recommendation formatting...")
        
        try:
            # Create mock signals for testing
            mock_signals = await self._create_mock_signals()
            
            if not mock_signals:
                self.test_results['recommendation_formatting'] = {
                    'status': 'SKIPPED - No signals to format'
                }
                return
            
            # Test formatting
            formatted_recommendations = []
            for signal in mock_signals:
                try:
                    recommendation = self.formatter.format_recommendation(signal)
                    formatted_recommendations.append(recommendation)
                except Exception as e:
                    self.logger.error(f"Formatting error for signal: {e}")
            
            # Analyze formatted output
            formatting_analysis = {
                'signals_processed': len(mock_signals),
                'recommendations_generated': len(formatted_recommendations),
                'sample_recommendation': formatted_recommendations[0] if formatted_recommendations else None
            }
            
            self.test_results['recommendation_formatting'] = {
                'status': 'SUCCESS',
                'analysis': formatting_analysis
            }
            
            self.logger.info(f"✅ Recommendation Formatting: {len(formatted_recommendations)} recommendations generated")
            
            # Log sample recommendation
            if formatted_recommendations:
                sample = formatted_recommendations[0]
                self.logger.info("📋 Sample Recommendation:")
                for line in sample.split('\n')[:5]:  # First 5 lines
                    self.logger.info(f"   {line}")
                
        except Exception as e:
            self.logger.error(f"❌ Recommendation Formatting Test Failed: {e}")
            self.test_results['recommendation_formatting'] = {
                'status': 'ERROR',
                'error': str(e)
            }
    
    async def _create_mock_signals(self):
        """Create mock signals for testing formatting"""
        # Try to get real signals first
        try:
            processor = self.factory.create_processor('consensus_moneyline')
            if processor:
                signals = await processor.process(360, self._create_mock_strategies())
                if signals:
                    return signals[:3]  # Return up to 3 real signals
        except:
            pass
        
        # Fall back to completely mock signals
        from mlb_sharp_betting.models.betting_analysis import BettingSignal, SignalType
        
        mock_signal = BettingSignal(
            game_id=12345,
            home_team="Yankees",
            away_team="Red Sox",
            signal_type=SignalType.CONSENSUS_MONEYLINE,
            confidence=0.78,
            recommendation="AWAY_ML",
            strategy_name="consensus_moneyline_heavy",
            created_at=datetime.now(),
            game_datetime=datetime.now() + timedelta(hours=3),
            metadata={
                'consensus_type': 'CONSENSUS_HEAVY',
                'public_bet_pct': 85.2,
                'sharp_money_pct': 88.7,
                'consensus_strength': 92.5
            }
        )
        
        return [mock_signal]
    
    def _print_test_summary(self):
        """Print comprehensive test summary"""
        self.logger.info("📋 TEST SUMMARY")
        self.logger.info("=" * 50)
        
        for test_name, results in self.test_results.items():
            self.logger.info(f"\n🧪 {test_name.upper()}:")
            
            if test_name == 'processor_creation':
                successful = sum(1 for r in results.values() if r['status'] == 'SUCCESS')
                total = len(results)
                self.logger.info(f"   ✅ Success Rate: {successful}/{total} ({successful/total*100:.1f}%)")
                
            elif test_name == 'data_access':
                if 'error' not in results:
                    self.logger.info(f"   📊 Records: {results.get('total_records', 0)}")
                    self.logger.info(f"   🎮 Games: {results.get('unique_games', 0)}")
                else:
                    self.logger.info(f"   ❌ Error: {results['error']}")
                    
            elif test_name == 'processor_execution':
                successful_executions = sum(1 for r in results.values() if r['status'] == 'SUCCESS')
                total_signals = sum(r.get('signals_generated', 0) for r in results.values() if r['status'] == 'SUCCESS')
                self.logger.info(f"   ⚡ Successful Executions: {successful_executions}")
                self.logger.info(f"   📊 Total Signals Generated: {total_signals}")
                
            elif test_name == 'recommendation_formatting':
                if results['status'] == 'SUCCESS':
                    analysis = results.get('analysis', {})
                    self.logger.info(f"   💬 Recommendations: {analysis.get('recommendations_generated', 0)}")
                else:
                    self.logger.info(f"   ❌ Status: {results['status']}")


async def main():
    """Main test execution"""
    print("🧪 MLB Sharp Betting - Processor Test Suite")
    print("=" * 60)
    
    runner = ProcessorTestRunner()
    await runner.run_all_tests()
    
    print("\n✅ Test suite completed!")


if __name__ == "__main__":
    asyncio.run(main()) 