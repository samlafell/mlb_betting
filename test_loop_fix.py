#!/usr/bin/env python3
"""
Test script to validate the infinite loop and clean logging fixes.

This script tests:
1. Strategy factory circuit breaker
2. Database connection initialization
3. Clean logging configuration
4. Backtest execution without loops
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent / "src"
sys.path.insert(0, str(project_root))

from mlb_sharp_betting.services.backtesting_service import SimplifiedBacktestingService


async def test_loop_fixes():
    """Test that our fixes prevent infinite loops and clean up logging."""
    
    print("🧪 Testing Loop Prevention and Clean Logging Fixes")
    print("=" * 60)
    
    try:
        # Test 1: Initialize service with clean logging
        print("\n1️⃣ Testing service initialization...")
        service = SimplifiedBacktestingService()
        
        # Test 2: Initialize components (this used to cause infinite loops)
        print("2️⃣ Testing component initialization...")
        await service.initialize()
        
        # Test 3: Run a quick backtest (with loop prevention)
        print("3️⃣ Testing backtest execution...")
        results = await service.run_backtest("2024-01-01", "2024-01-31")
        
        # Test 4: Verify results structure
        print("4️⃣ Verifying results...")
        assert "results" in results
        assert "stats" in results
        assert "summary" in results
        
        print("\n✅ ALL TESTS PASSED!")
        print(f"   - Executed {results['stats']['total_strategies_attempted']} strategies")
        print(f"   - {results['stats']['successful_strategies']} successful")
        print(f"   - {results['stats']['profitable_strategies']} profitable")
        print(f"   - Log file: {service.log_file}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        return False


if __name__ == "__main__":
    success = asyncio.run(test_loop_fixes())
    sys.exit(0 if success else 1) 