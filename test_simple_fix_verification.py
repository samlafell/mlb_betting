#!/usr/bin/env python3
"""Simple test to verify the betting recommendation model fixes"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "src"))

def test_imports():
    """Test that all imports work correctly"""
    print("🔍 Testing imports...")
    
    try:
        from mlb_sharp_betting.models.betting_analysis import BettingSignal, SignalType
        print("✅ BettingSignal model imports OK")
        
        from mlb_sharp_betting.services.betting_signal_repository import BettingSignalRepository
        print("✅ BettingSignalRepository imports OK")
        
        from mlb_sharp_betting.analysis.processors.consensus_processor import ConsensusProcessor
        print("✅ ConsensusProcessor imports OK")
        
        from mlb_sharp_betting.analysis.processors.underdogvalue_processor import UnderdogValueProcessor
        print("✅ UnderdogValueProcessor imports OK")
        
        from mlb_sharp_betting.analysis.processors.linemovement_processor import LineMovementProcessor
        print("✅ LineMovementProcessor imports OK")
        
        return True
        
    except Exception as e:
        print(f"❌ Import failed: {e}")
        return False

def test_repository_methods():
    """Test that repository methods exist"""
    print("\n🔍 Testing repository methods...")
    
    try:
        from mlb_sharp_betting.models.betting_analysis import SignalProcessorConfig
        from mlb_sharp_betting.services.betting_signal_repository import BettingSignalRepository
        
        config = SignalProcessorConfig()
        repo = BettingSignalRepository(config)
        
        # Check if methods exist
        methods = ['get_consensus_signal_data', 'get_underdog_value_data']
        for method in methods:
            if hasattr(repo, method):
                print(f"✅ {method} method exists")
            else:
                print(f"❌ {method} method missing")
                return False
        
        return True
        
    except Exception as e:
        print(f"❌ Repository test failed: {e}")
        return False

def test_betting_signal_model():
    """Test BettingSignal model structure"""
    print("\n🔍 Testing BettingSignal model...")
    
    try:
        from mlb_sharp_betting.models.betting_analysis import BettingSignal, SignalType, ConfidenceLevel
        from datetime import datetime
        
        # Test signal creation
        signal = BettingSignal(
            signal_type=SignalType.CONSENSUS_MONEYLINE,
            home_team="Test Home",
            away_team="Test Away",
            game_time=datetime.now(),
            minutes_to_game=60,
            split_type="moneyline",
            split_value=None,
            source="TEST",
            book=None,
            differential=15.0,
            signal_strength=15.0,
            confidence_score=0.75,  # Test confidence_score field
            confidence_level=ConfidenceLevel.HIGH,
            confidence_explanation="Test explanation",
            recommendation="BET Test Home",
            recommendation_strength="STRONG",
            last_updated=datetime.now(),
            strategy_name="Test Strategy",
            win_rate=65.0,
            roi=20.0,
            total_bets=10
        )
        
        print("✅ BettingSignal created successfully")
        print(f"✅ confidence_score: {signal.confidence_score}")
        print(f"✅ confidence_level: {signal.confidence_level}")
        
        return True
        
    except Exception as e:
        print(f"❌ BettingSignal test failed: {e}")
        return False

def main():
    """Run all tests"""
    print("🚀 Testing Betting Recommendation Model Fixes\n")
    
    success_count = 0
    total_tests = 3
    
    if test_imports():
        success_count += 1
    
    if test_repository_methods():
        success_count += 1
    
    if test_betting_signal_model():
        success_count += 1
    
    print(f"\n📊 RESULTS: {success_count}/{total_tests} tests passed")
    
    if success_count == total_tests:
        print("🎉 ALL TESTS PASSED - Core fixes are working!")
    else:
        print("⚠️  Some tests failed - need more work")
    
    return success_count == total_tests

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 