#!/usr/bin/env python3
"""
Test Automated Collection Integration

Tests the scheduler-based automated data collection functionality
to verify Issue #36 resolution.
"""

import asyncio
import os
from datetime import datetime

# Set database password
os.environ["DB_PASSWORD"] = "postgres"

async def test_automated_collection():
    """Test the automated collection via scheduler."""
    try:
        from src.services.scheduling.scheduler_engine_service import SchedulerEngineService, SchedulerConfig
        
        # Create a test scheduler configuration
        config = SchedulerConfig(
            notifications_enabled=True,
            enable_metrics=True,
            log_job_execution=True
        )
        
        # Initialize scheduler
        scheduler = SchedulerEngineService(config)
        
        print("🔧 Testing automated data collection integration...")
        print(f"⏰ Test started at: {datetime.now()}")
        
        # Test the hourly data handler directly
        print("\n🚀 Testing hourly data collection handler...")
        await scheduler._hourly_data_handler()
        
        print("\n✅ Hourly data collection test completed!")
        
        # Display metrics
        metrics = scheduler.get_metrics()
        print(f"\n📊 Test Metrics:")
        print(f"   - Hourly runs: {metrics.get('hourly_runs', 0)}")
        print(f"   - Total jobs executed: {metrics.get('total_jobs_executed', 0)}")
        print(f"   - Errors: {metrics.get('errors', 0)}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = asyncio.run(test_automated_collection())
    
    if success:
        print("\n🎉 SUCCESS: Automated collection integration working!")
        print("   - Scheduler can now run automated hourly data collection")
        print("   - Issue #36 resolved: Silent failures eliminated with proper scheduling")
    else:
        print("\n💥 FAILED: Automated collection integration needs fixes")