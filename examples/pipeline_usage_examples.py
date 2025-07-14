"""
Examples showing how to use the new integrated pipeline.

These examples demonstrate the enhanced CLI commands and pipeline orchestration using Phase 3/4 engines.
"""

import asyncio
from src.services.orchestration.pipeline_orchestration_service import PipelineOrchestrationService
from src.analysis.backtesting.engine import create_recommendation_backtesting_engine
from src.data.database.connection import get_db_manager


async def example_smart_pipeline():
    """Example: Run smart pipeline that decides what's needed."""
    
    print("🧠 SMART PIPELINE EXAMPLE")
    print("=" * 50)
    
    db_manager = get_db_manager()
    orchestrator = PipelineOrchestrationService(db_manager)
    
    try:
        # First, get recommendations
        recommendations = await orchestrator.get_pipeline_recommendations()
        
        print(f"📊 System Health: {recommendations['system_health']}")
        print(f"🚨 Priority: {recommendations['priority_level']}")
        print(f"⏱️  Estimated Runtime: {recommendations['estimated_runtime_minutes']} min")
        
        if recommendations['immediate_actions']:
            print(f"\n🚀 Recommended Actions:")
            for action in recommendations['immediate_actions']:
                print(f"   • {action['action']}: {action['reason']}")
        
        # Analyze system and execute only what's needed
        results = await orchestrator.execute_smart_pipeline(
            detection_minutes=60,
            force_fresh_data=False
        )
        
        print(f"\n✅ Pipeline executed steps: {results['steps_executed']}")
        print(f"⏱️  Total execution time: {results['total_execution_time']:.1f}s")
        
        if results['detection_results']:
            total_opportunities = sum(
                len(game_analysis.sharp_signals) + 
                len(game_analysis.opposing_markets) + 
                len(game_analysis.steam_moves) + 
                len(game_analysis.book_conflicts)
                for game_analysis in results['detection_results'].games.values()
            )
            print(f"🎯 Found {len(results['detection_results'].games)} games with {total_opportunities} opportunities")
        
        if results['cross_market_flips']:
            flips = results['cross_market_flips']['flips']
            print(f"🔀 Found {len(flips)} cross-market flips")
    
    finally:
        orchestrator.close()


async def example_forced_fresh_pipeline():
    """Example: Force fresh data collection and full pipeline."""
    
    print("🔄 FORCED FRESH PIPELINE EXAMPLE")
    print("=" * 50)
    
    db_manager = get_db_manager()
    orchestrator = PipelineOrchestrator(db_manager)
    
    try:
        print("🚀 Forcing fresh data collection and complete analysis...")
        
        results = await orchestrator.execute_smart_pipeline(
            detection_minutes=120,  # Look further ahead
            force_fresh_data=True,   # Force fresh data
            force_backtesting=True   # Force backtesting
        )
        
        print(f"\n✅ Forced fresh pipeline completed")
        print(f"🔧 Steps executed: {', '.join(results['steps_executed'])}")
        print(f"⏱️  Total time: {results['total_execution_time']:.1f}s")
        
        if results['data_collection_metrics']:
            metrics = results['data_collection_metrics']
            print(f"📡 Data collected: {metrics.get('parsed_records', 0)} records")
        
        if results['backtesting_results']:
            backtest = results['backtesting_results']
            print(f"🔬 Strategies analyzed: {backtest.total_strategies_analyzed}")
            print(f"💰 Profitable strategies: {backtest.profitable_strategies}")
        
    finally:
        orchestrator.close()


async def example_data_freshness_check():
    """Example: Check data freshness and conditional pipeline using BacktestingEngine."""
    
    print("📡 DATA FRESHNESS CHECK EXAMPLE (Phase 3 Engine)")
    print("=" * 50)
    
    backtesting_engine = get_backtesting_engine()
    
    try:
        await backtesting_engine.initialize()
        
        # Check data freshness
        from src.services.data.enhanced_data_service import EnhancedDataService
        from src.data.database.connection import get_db_manager
        data_service = EnhancedDataService(get_db_manager())
        freshness_check = await data_service.check_data_freshness()
        
        print(f"📅 Data age: {freshness_check.get('data_age_hours', 0):.1f} hours")
        print(f"✅ Is fresh: {freshness_check['is_fresh']}")
        print(f"📊 Total splits: {freshness_check.get('total_splits', 0):,}")
        print(f"🎮 Unique games: {freshness_check.get('unique_games', 0)}")
        
        # Run conditional pipeline
        if freshness_check['needs_collection']:
            print(f"\n🔄 Data is stale, running full pipeline...")
            results = await backtesting_engine.run_daily_pipeline()
        else:
            print(f"\n✅ Data is fresh, running backtesting only...")
            results = await backtesting_engine.core_engine.run_strategies_only()
        
        print(f"🔧 Pipeline execution completed")
        print(f"📝 Results: {results}")
        
    except Exception as e:
        print(f"❌ Example failed: {e}")


async def example_pipeline_validation():
    """Example: Validate pipeline requirements using BacktestingEngine."""
    
    print("✅ PIPELINE VALIDATION EXAMPLE (Phase 3 Engine)")
    print("=" * 50)
    
    backtesting_engine = get_backtesting_engine()
    
    try:
        await backtesting_engine.initialize()
        
        # Validate all requirements
        validations = await backtesting_engine.validate_pipeline_requirements()
        
        print("🔍 Pipeline Requirements:")
        for requirement, is_valid in validations.items():
            status = "✅" if is_valid else "❌"
            req_name = requirement.replace('_', ' ').title()
            print(f"   {status} {req_name}")
        
        if all(validations.values()):
            print(f"\n🎉 All requirements met - pipeline ready!")
        else:
            failed_reqs = [k for k, v in validations.items() if not v]
            print(f"\n⚠️  Failed requirements: {', '.join(failed_reqs)}")
            print(f"💡 Fix these issues before running the pipeline")
        
    except Exception as e:
        print(f"❌ Validation failed: {e}")


async def example_backtesting_engine_features():
    """Example: Demonstrate BacktestingEngine specific features."""
    
    print("🔬 BACKTESTING ENGINE FEATURES EXAMPLE")
    print("=" * 50)
    
    repository = get_db_manager()
    backtesting_engine = create_recommendation_backtesting_engine(repository)
    
    try:
        await backtesting_engine.initialize()
        
        print("🏥 Running comprehensive diagnostics...")
        diagnostics = await backtesting_engine.diagnostics.run_full_diagnostic()
        print(f"   Overall health: {diagnostics.get('overall_health', 'unknown')}")
        print(f"   Issues found: {len(diagnostics.get('issues', []))}")
        
        print("\n📊 Getting comprehensive status...")
        status = backtesting_engine.get_comprehensive_status()
        print(f"   Engine status: {status}")
        
        print("\n🎯 Running quick strategy test...")
        test_results = await backtesting_engine.core_engine.run_quick_test()
        print(f"   Test results: {test_results}")
        
        print("\n⚡ Running performance benchmark...")
        import time
        start_time = time.time()
        await backtesting_engine.run_daily_pipeline()
        benchmark_time = time.time() - start_time
        print(f"   Benchmark time: {benchmark_time:.2f} seconds")
        
    except Exception as e:
        print(f"❌ BacktestingEngine features example failed: {e}")


def show_cli_examples():
    """Show examples of using the new CLI commands with Phase 3/4 engines."""
    
    print("🖥️  CLI USAGE EXAMPLES (Phase 3/4 Engines)")
    print("=" * 50)
    
    print("""
🧠 SMART PIPELINE (Recommended):
   # Automatically decides what needs to be run
   uv run -m mlb_sharp_betting.cli detect smart-pipeline
   
   # Force fresh data collection
   uv run -m mlb_sharp_betting.cli detect smart-pipeline --force-fresh
   
   # Get recommendations without running
   uv run -m mlb_sharp_betting.cli detect recommendations

📡 DATA MANAGEMENT (Phase 3 Engine):
   # Collect fresh data
   uv run -m mlb_sharp_betting.cli data collect
   
   # Check data status
   uv run -m mlb_sharp_betting.cli data status --detailed
   
   # Validate data freshness only
   uv run -m mlb_sharp_betting.cli data collect --validate-only
   
   # Force collection even if data is fresh
   uv run -m mlb_sharp_betting.cli data collect --force

🎯 ENHANCED DETECTION (Phase 3 Engine):
   # Full pipeline with fresh data and backtesting
   uv run -m mlb_sharp_betting.cli detect opportunities
   
   # Use existing data only
   uv run -m mlb_sharp_betting.cli detect opportunities --use-existing --skip-backtesting
   
   # Save results to JSON
   uv run -m mlb_sharp_betting.cli detect opportunities --format json --output results.json

🔬 ENHANCED BACKTESTING (Phase 3 Engine):
   # Full backtesting with fresh data
   uv run -m mlb_sharp_betting.cli backtesting run
   
   # Run specific mode (scheduler, single-run, status, test)
   uv run -m mlb_sharp_betting.cli backtesting run --mode scheduler
   uv run -m mlb_sharp_betting.cli backtesting run --mode single-run
   uv run -m mlb_sharp_betting.cli backtesting run --mode status
   uv run -m mlb_sharp_betting.cli backtesting run --mode test
   
   # Enhanced backtesting with diagnostics
   uv run -m mlb_sharp_betting.cli enhanced-backtesting run --mode full --diagnostics
   uv run -m mlb_sharp_betting.cli enhanced-backtesting diagnostics --save-report /tmp/report.json

🗓️  PRE-GAME SCHEDULER (Phase 4 Engine):
   # Start pre-game scheduler
   uv run -m mlb_sharp_betting.cli pre-game start
   
   # Start full scheduler (all modes)
   uv run -m mlb_sharp_betting.cli pre-game start-full
   
   # Check scheduler status
   uv run -m mlb_sharp_betting.cli pre-game status
   
   # Stop scheduler
   uv run -m mlb_sharp_betting.cli pre-game stop

🏥 SYSTEM HEALTH (Phase 3/4 Engines):
   # Check overall system health
   uv run -m mlb_sharp_betting.cli system-status health --detailed
   
   # Check data pipeline health
   uv run -m mlb_sharp_betting.cli system-status pipeline --detailed
   
   # Run comprehensive diagnostics
   uv run -m mlb_sharp_betting.cli system-status diagnostics --save-report /tmp/diagnostics.json
""")


async def main():
    """Run all examples."""
    await example_smart_pipeline()
    await example_forced_fresh_pipeline()
    await example_data_freshness_check()
    await example_pipeline_validation()
    await example_backtesting_engine_features()
    show_cli_examples()


if __name__ == "__main__":
    asyncio.run(main()) 