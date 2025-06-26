"""
Examples showing how to use the new integrated pipeline.

These examples demonstrate the enhanced CLI commands and pipeline orchestration.
"""

import asyncio
from mlb_sharp_betting.services.pipeline_orchestrator import PipelineOrchestrator
from mlb_sharp_betting.services.enhanced_backtesting_service import EnhancedBacktestingService
from mlb_sharp_betting.db.connection import get_db_manager


async def example_smart_pipeline():
    """Example: Run smart pipeline that decides what's needed."""
    
    print("🧠 SMART PIPELINE EXAMPLE")
    print("=" * 50)
    
    db_manager = get_db_manager()
    orchestrator = PipelineOrchestrator(db_manager)
    
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
    """Example: Check data freshness and conditional pipeline."""
    
    print("📡 DATA FRESHNESS CHECK EXAMPLE")
    print("=" * 50)
    
    enhanced_service = EnhancedBacktestingService()
    
    try:
        # Check data freshness
        freshness_check = await enhanced_service.check_data_freshness()
        
        print(f"📅 Data age: {freshness_check.get('data_age_hours', 0):.1f} hours")
        print(f"✅ Is fresh: {freshness_check['is_fresh']}")
        print(f"📊 Total splits: {freshness_check.get('total_splits', 0):,}")
        print(f"🎮 Unique games: {freshness_check.get('unique_games', 0)}")
        
        # Run conditional pipeline
        if freshness_check['needs_collection']:
            print(f"\n🔄 Data is stale, running full pipeline...")
            results = await enhanced_service.run_daily_backtesting_pipeline_with_fresh_data()
        else:
            print(f"\n✅ Data is fresh, running backtesting only...")
            results = await enhanced_service.run_conditional_pipeline()
        
        print(f"🔧 Steps executed: {', '.join(results['steps_executed'])}")
        print(f"📝 Reason: {results.get('reason', 'Standard pipeline')}")
        
    except Exception as e:
        print(f"❌ Example failed: {e}")


async def example_pipeline_validation():
    """Example: Validate pipeline requirements."""
    
    print("✅ PIPELINE VALIDATION EXAMPLE")
    print("=" * 50)
    
    enhanced_service = EnhancedBacktestingService()
    
    try:
        # Validate all requirements
        validations = await enhanced_service.validate_pipeline_requirements()
        
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


def show_cli_examples():
    """Show examples of using the new CLI commands."""
    
    print("🖥️  CLI USAGE EXAMPLES")
    print("=" * 50)
    
    print("""
🧠 SMART PIPELINE (Recommended):
   # Automatically decides what needs to be run
   mlb-cli detect smart-pipeline
   
   # Force fresh data collection
   mlb-cli detect smart-pipeline --force-fresh
   
   # Get recommendations without running
   mlb-cli detect recommendations

📡 DATA MANAGEMENT:
   # Collect fresh data
   mlb-cli data collect
   
   # Check data status
   mlb-cli data status --detailed
   
   # Validate data freshness only
   mlb-cli data collect --validate-only
   
   # Force collection even if data is fresh
   mlb-cli data collect --force

🎯 ENHANCED DETECTION:
   # Full pipeline with fresh data and backtesting
   mlb-cli detect opportunities
   
   # Use existing data only
   mlb-cli detect opportunities --use-existing --skip-backtesting
   
   # Save results to JSON
   mlb-cli detect opportunities --format json --output results.json

🔬 ENHANCED BACKTESTING:
   # Full backtesting with fresh data
   mlb-cli backtest run
   
   # Quick analysis of top strategies
   mlb-cli backtest quick --strategy-count 10
   
   # Validate strategies against criteria
   mlb-cli backtest validate --min-sample-size 20 --min-roi 10.0
   
   # Compare two strategies
   mlb-cli backtest compare --strategy1 "sharp_action" --strategy2 "public_fade"

🔧 SYSTEM STATUS:
   # Complete system overview
   mlb-cli status overview
   
   # Detailed health check
   mlb-cli status health --detailed
   
   # Performance metrics
   mlb-cli status performance
   
   # Auto-fix common issues
   mlb-cli status fix --auto-approve

🔀 CROSS-MARKET ANALYSIS:
   # Still available as before
   mlb-cli cross-market-flips --min-confidence 70 --hours-back 24

📊 LEGACY COMMANDS (Still Available):
   # Original commands remain unchanged
   mlb-cli run --mock
   mlb-cli query --table splits.raw_mlb_betting_splits
   mlb-cli analyze
   mlb-cli demo
""")


async def main():
    """Run all examples."""
    
    print("🚀 ENHANCED PIPELINE EXAMPLES")
    print("=" * 60)
    
    try:
        # Show CLI examples first
        show_cli_examples()
        
        print("\n" + "="*60)
        print("🔧 PROGRAMMATIC EXAMPLES")
        print("="*60)
        
        # Run programmatic examples
        await example_pipeline_validation()
        print("\n" + "-"*50)
        
        await example_data_freshness_check()
        print("\n" + "-"*50)
        
        await example_smart_pipeline()
        print("\n" + "-"*50)
        
        # Note: Skipping forced fresh pipeline to avoid unnecessary data collection
        print("📝 Skipping forced fresh pipeline example to avoid unnecessary data collection")
        print("💡 Run example_forced_fresh_pipeline() manually if needed")
        
        print(f"\n🎉 Examples completed successfully!")
        print(f"💡 Try the CLI commands shown above to use the enhanced pipeline")
        
    except Exception as e:
        print(f"❌ Examples failed: {e}")


if __name__ == "__main__":
    # Run examples
    asyncio.run(main()) 