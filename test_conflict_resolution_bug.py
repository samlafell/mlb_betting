#!/usr/bin/env python3
"""
Test Script: Conflict Resolution Bug Analysis
===========================================

This script demonstrates the bug where PHI ML (+2% differential) 
is being selected over HOU -1.5 (-22% differential) despite the
latter being clearly stronger.

The issue appears to be in the conflict resolution logic or display.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List
import pytz

# Import the components we need to test
from mlb_sharp_betting.services.confidence_scorer import ConfidenceScorer
from mlb_sharp_betting.services.betting_recommendation_formatter import (
    BettingRecommendationFormatter, 
    RecommendationConflictDetector
)
from mlb_sharp_betting.models.betting_analysis import BettingSignal, SignalType

@dataclass
class MockBettingSignal:
    """Mock betting signal for testing"""
    away_team: str
    home_team: str
    signal_type: SignalType
    recommendation: str
    differential: float
    confidence_score: float
    confidence_explanation: str
    stake_size: str
    strategy_name: str
    last_updated: datetime

def create_houston_spread_signal():
    """Create Houston -1.5 spread signal with 22% differential"""
    return MockBettingSignal(
        away_team="PHI",
        home_team="HOU", 
        signal_type=SignalType.SHARP_ACTION,
        recommendation="BET HOU -1.5",
        differential=-22.0,  # Strong signal favoring Houston
        confidence_score=85.0,  # Should be high with 22% differential
        confidence_explanation="Very strong signal (-22.0% differential) • Sharp money heavily on Houston",
        stake_size="3-4 units (HIGH CONVICTION)",
        strategy_name="Sharp Action Spread",
        last_updated=datetime.now(pytz.timezone('US/Eastern'))
    )

def create_philadelphia_ml_signal():
    """Create Philadelphia ML signal with 2% differential"""
    return MockBettingSignal(
        away_team="PHI",
        home_team="HOU",
        signal_type=SignalType.UNDERDOG_VALUE,
        recommendation="BET PHI +131",
        differential=2.0,  # Very weak signal
        confidence_score=25.0,  # Should be low with 2% differential
        confidence_explanation="Weak signal (+2.0% differential) • Minimal edge detected",
        stake_size="1 unit (EXPLORATORY)",
        strategy_name="Underdog ML Value",
        last_updated=datetime.now(pytz.timezone('US/Eastern'))
    )

def test_confidence_scoring():
    """Test that confidence scoring works correctly"""
    print("🔬 TESTING CONFIDENCE SCORING")
    print("=" * 50)
    
    scorer = ConfidenceScorer()
    est = pytz.timezone('US/Eastern')
    now = datetime.now(est)
    game_time = now.replace(hour=19, minute=0)  # 7 PM game
    
    # Test 22% differential (should be high confidence)
    result_22pct = scorer.calculate_confidence(
        signal_differential=-22.0,
        source="VSIN",
        book="Circa",
        split_type="spread",
        strategy_name="sharp_action",
        last_updated=now,
        game_datetime=game_time
    )
    
    # Test 2% differential (should be low confidence)
    result_2pct = scorer.calculate_confidence(
        signal_differential=2.0,
        source="VSIN", 
        book="DraftKings",
        split_type="moneyline",
        strategy_name="underdog_value",
        last_updated=now,
        game_datetime=game_time
    )
    
    print(f"📊 22% Differential Results:")
    print(f"   Overall Confidence: {result_22pct.overall_confidence:.1f}%")
    print(f"   Confidence Level: {result_22pct.confidence_level}")
    print(f"   Signal Strength Score: {result_22pct.components.signal_strength_score:.1f}")
    print(f"   Recommendation: {result_22pct.recommendation_strength}")
    print()
    
    print(f"📊 2% Differential Results:")
    print(f"   Overall Confidence: {result_2pct.overall_confidence:.1f}%")
    print(f"   Confidence Level: {result_2pct.confidence_level}")
    print(f"   Signal Strength Score: {result_2pct.components.signal_strength_score:.1f}")
    print(f"   Recommendation: {result_2pct.recommendation_strength}")
    print()
    
    # Verify the fix is working
    assert result_22pct.overall_confidence > result_2pct.overall_confidence, \
        "22% differential should have higher confidence than 2%!"
    
    assert result_22pct.components.signal_strength_score > 70, \
        "22% differential should get high signal strength score!"
    
    assert result_2pct.components.signal_strength_score < 30, \
        "2% differential should get low signal strength score!"
    
    print("✅ Confidence scoring is working correctly!")
    return result_22pct, result_2pct

def test_conflict_resolution():
    """Test conflict resolution logic"""
    print("\n🚨 TESTING CONFLICT RESOLUTION")
    print("=" * 50)
    
    # Create the two conflicting signals
    hou_spread = create_houston_spread_signal()
    phi_ml = create_philadelphia_ml_signal()
    
    print(f"🏈 Houston Spread Signal:")
    print(f"   Recommendation: {hou_spread.recommendation}")
    print(f"   Differential: {hou_spread.differential}%")
    print(f"   Confidence: {hou_spread.confidence_score}%")
    print(f"   Strategy: {hou_spread.strategy_name}")
    print()
    
    print(f"💰 Philadelphia ML Signal:")
    print(f"   Recommendation: {phi_ml.recommendation}")
    print(f"   Differential: {phi_ml.differential}%")
    print(f"   Confidence: {phi_ml.confidence_score}%")
    print(f"   Strategy: {phi_ml.strategy_name}")
    print()
    
    # Test conflict detection
    detector = RecommendationConflictDetector()
    conflicts = detector.detect_conflicts([hou_spread, phi_ml])
    
    print(f"🔍 Conflicts Detected: {len(conflicts)} games")
    
    if conflicts:
        game_key = list(conflicts.keys())[0]
        conflicting_signals = conflicts[game_key]
        
        print(f"   Game: {game_key}")
        print(f"   Conflicting Signals: {len(conflicting_signals)}")
        
        # Test conflict resolution
        resolved = detector.resolve_conflicts(conflicts)
        winner = resolved[0]
        
        print(f"\n🏆 Conflict Resolution Result:")
        print(f"   Selected: {winner.recommendation}")
        print(f"   Confidence: {winner.confidence_score}%")
        print(f"   Differential: {winner.differential}%")
        print(f"   Strategy: {winner.strategy_name}")
        
        # CRITICAL CHECK: The stronger signal should win
        if abs(hou_spread.differential) > abs(phi_ml.differential):
            expected_winner = hou_spread
            print(f"\n📋 Expected Winner: Houston Spread (stronger {abs(hou_spread.differential)}% vs {abs(phi_ml.differential)}%)")
        else:
            expected_winner = phi_ml
            print(f"\n📋 Expected Winner: Philadelphia ML (stronger {abs(phi_ml.differential)}% vs {abs(hou_spread.differential)}%)")
        
        # Verify the correct signal won
        if winner.recommendation == expected_winner.recommendation:
            print("✅ Conflict resolution selected the correct stronger signal!")
        else:
            print("❌ BUG DETECTED: Conflict resolution selected the weaker signal!")
            print(f"   Selected: {winner.recommendation} ({abs(winner.differential)}% diff)")
            print(f"   Should have selected: {expected_winner.recommendation} ({abs(expected_winner.differential)}% diff)")
            
            # This is the bug we need to fix!
            return False
    
    return True

def demonstrate_real_issue():
    """Demonstrate the real issue from the user's output"""
    print("\n🎯 REAL ISSUE ANALYSIS")
    print("=" * 50)
    
    print("From the user's output, we see:")
    print("🔥 🎯 PHI Moneyline (+131)")
    print("   📊 Confidence: 95%")
    print("   💰 Sharp Edge: 2.0% money/bet differential")
    print("   💵 Suggested Stake: 4-5 units (MAX BET - ELITE EDGE)")
    print("   🧠 Reasoning: Selected highest confidence signal (1% vs 1%)")
    print()
    
    print("❌ PROBLEMS IDENTIFIED:")
    print("1. 2% differential getting 95% confidence (should be ~25%)")
    print("2. 2% differential getting 4-5 units stake (should be 1 unit)")
    print("3. Conflict resolution showing '1% vs 1%' (makes no sense)")
    print("4. Houston -1.5 with 22% differential not shown in output")
    print()
    
    print("🔧 ROOT CAUSE:")
    print("Either:")
    print("A) The confidence scoring fixes haven't been applied properly")
    print("B) There's a bug in how signals are being created before they reach the formatter")
    print("C) Multiple systems are generating different confidence scores for the same data")
    print("D) The conflict resolution is using stale/incorrect confidence scores")

def main():
    """Run all tests"""
    print("🧪 CONFLICT RESOLUTION BUG ANALYSIS")
    print("=" * 60)
    
    try:
        # Test 1: Verify confidence scoring is working
        result_22, result_2 = test_confidence_scoring()
        
        # Test 2: Test conflict resolution with mock data
        resolution_works = test_conflict_resolution()
        
        # Test 3: Analyze the real issue
        demonstrate_real_issue()
        
        print("\n📋 SUMMARY:")
        print("=" * 50)
        
        if result_22.overall_confidence > result_2.overall_confidence:
            print("✅ Confidence scoring: WORKING")
        else:
            print("❌ Confidence scoring: BROKEN")
        
        if resolution_works:
            print("✅ Conflict resolution: WORKING (with mock data)")
        else:
            print("❌ Conflict resolution: BROKEN")
        
        print("\n🎯 RECOMMENDED ACTION:")
        print("The issue is likely that the actual signals being generated")
        print("in the live system have incorrect confidence scores before")
        print("they reach the conflict resolver. We need to trace where")
        print("the 95% confidence for 2% differential is coming from.")
        
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 