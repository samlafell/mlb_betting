"""
Simple Pinnacle Integration Demo

This demo showcases the Pinnacle models and basic functionality without complex imports.
"""

import json
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any
import structlog

from mlb_sharp_betting.models.pinnacle import (
    PinnacleMatchup,
    PinnacleLeague,
    PinnacleSport,
    PinnacleParticipant,
    PinnacleSpecial,
    PinnacleParentInfo,
    PinnacleAlignment,
    PinnacleMatchupStatus,
)

logger = structlog.get_logger()


def create_mock_matchup_data() -> Dict[str, Any]:
    """Create mock matchup data based on Pinnacle's API structure"""
    return {
        "id": 1610704108,
        "participants": [
            {
                "id": 1610704109,
                "name": "Houston Astros",
                "alignment": "away",
                "order": 0,
                "rotation": 101
            },
            {
                "id": 1610704110,
                "name": "Oakland Athletics", 
                "alignment": "home",
                "order": 1,
                "rotation": 102
            }
        ],
        "startTime": "2025-01-15T19:10:00Z",
        "league": {
            "id": 246,
            "name": "MLB",
            "group": "USA",
            "sport": {
                "id": 3,
                "name": "Baseball",
                "primaryMarketType": "moneyline"
            }
        },
        "isLive": False,
        "status": "pending",
        "version": 426970028
    }


def create_mock_special_data() -> Dict[str, Any]:
    """Create mock special/prop bet data"""
    return {
        "id": 1610909574,
        "participants": [
            {
                "id": 1610909575,
                "name": "Over",
                "alignment": "neutral",
                "order": 0,
                "rotation": 964
            },
            {
                "id": 1610909576,
                "name": "Under",
                "alignment": "neutral",
                "order": 0,
                "rotation": 965
            }
        ],
        "startTime": "2025-01-15T19:10:00Z",
        "league": {
            "id": 246,
            "name": "MLB",
            "group": "USA",
            "sport": {
                "id": 3,
                "name": "Baseball",
                "primaryMarketType": "moneyline"
            }
        },
        "special": {
            "category": "Player Props",
            "description": "José Altuve (Total Bases)(must start)"
        },
        "parent": {
            "id": 1610704108,
            "participants": [
                {
                    "name": "Houston Astros",
                    "alignment": "away"
                },
                {
                    "name": "Oakland Athletics",
                    "alignment": "home"
                }
            ],
            "startTime": "2025-01-15T19:10:00+00:00"
        },
        "type": "special",
        "units": "TotalBases",
        "status": "pending",
        "version": 426970028
    }


def demo_model_validation():
    """Demonstrate Pydantic model validation with mock data"""
    print("🔍 Pinnacle Model Validation Demo")
    print("=" * 50)
    
    try:
        # Test matchup model
        matchup_data = create_mock_matchup_data()
        matchup = PinnacleMatchup(**matchup_data)
        print(f"✅ Matchup Model: {matchup.participants[0].name} @ {matchup.participants[1].name}")
        print(f"   Start Time: {matchup.start_time}")
        print(f"   League: {matchup.league.name}")
        print(f"   Status: {matchup.status}")
        print()
        
        # Test special/prop model
        special_data = create_mock_special_data()
        special = PinnacleMatchup(**special_data)
        print(f"✅ Special Model: {special.special.description}")
        print(f"   Category: {special.special.category}")
        print(f"   Units: {special.units}")
        print()
        
        return True
        
    except Exception as e:
        logger.error("Model validation failed", error=str(e))
        print(f"❌ Error: {e}")
        return False


def demo_data_structures():
    """Demonstrate the data structures and their properties"""
    print("🏗️  Data Structure Demo")
    print("=" * 50)
    
    try:
        matchup_data = create_mock_matchup_data()
        matchup = PinnacleMatchup(**matchup_data)
        
        print("✅ Matchup Structure:")
        print(f"   ID: {matchup.id}")
        print(f"   Game: {matchup.participants[0].name} @ {matchup.participants[1].name}")
        print(f"   Time: {matchup.start_time}")
        print(f"   League: {matchup.league.name} ({matchup.league.sport.name})")
        print(f"   Status: {matchup.status}")
        print(f"   Live: {matchup.is_live}")
        print(f"   Has Markets: {matchup.has_markets}")
        print()
        
        print("✅ Participants:")
        for participant in matchup.participants:
            print(f"   - {participant.name} ({participant.alignment})")
            print(f"     Rotation: {participant.rotation}")
            print(f"     Order: {participant.order}")
        print()
        
        print("✅ League Details:")
        print(f"   Sport: {matchup.league.sport.name}")
        print(f"   Primary Market: {matchup.league.sport.primary_market_type}")
        print(f"   Group: {matchup.league.group}")
        print()
        
        return True
        
    except Exception as e:
        logger.error("Data structure demo failed", error=str(e))
        print(f"❌ Error: {e}")
        return False


def demo_special_bets():
    """Demonstrate special bets (props) handling"""
    print("🎲 Special Bets Demo")
    print("=" * 50)
    
    try:
        special_data = create_mock_special_data()
        special = PinnacleMatchup(**special_data)
        
        print("✅ Special Bet Structure:")
        print(f"   Description: {special.special.description}")
        print(f"   Category: {special.special.category}")
        print(f"   Type: {special.type}")
        print(f"   Units: {special.units}")
        print()
        
        print("✅ Parent Game Info:")
        if special.parent:
            parent_teams = " @ ".join([p.name for p in special.parent.participants])
            print(f"   Game: {parent_teams}")
            print(f"   Start: {special.parent.start_time}")
        print()
        
        print("✅ Prop Participants:")
        for participant in special.participants:
            print(f"   - {participant.name}")
            print(f"     Alignment: {participant.alignment}")
            print(f"     Rotation: {participant.rotation}")
        print()
        
        return True
        
    except Exception as e:
        logger.error("Special bets demo failed", error=str(e))
        print(f"❌ Error: {e}")
        return False


def demo_json_serialization():
    """Demonstrate JSON serialization/deserialization"""
    print("🔄 JSON Serialization Demo")
    print("=" * 50)
    
    try:
        # Create matchup
        matchup_data = create_mock_matchup_data()
        matchup = PinnacleMatchup(**matchup_data)
        
        # Convert to JSON
        json_str = matchup.model_dump_json(indent=2)
        print("✅ Model → JSON:")
        print(json_str[:300] + "..." if len(json_str) > 300 else json_str)
        print()
        
        # Parse back from JSON
        parsed_data = json.loads(json_str)
        recreated_matchup = PinnacleMatchup(**parsed_data)
        
        print("✅ JSON → Model:")
        print(f"   Recreation successful: {recreated_matchup.id == matchup.id}")
        print(f"   Teams match: {recreated_matchup.participants[0].name == matchup.participants[0].name}")
        print(f"   Time matches: {recreated_matchup.start_time == matchup.start_time}")
        print()
        
        return True
        
    except Exception as e:
        logger.error("JSON serialization demo failed", error=str(e))
        print(f"❌ Error: {e}")
        return False


async def main():
    """Main demo function"""
    print("🎯 Simple Pinnacle Integration Demo")
    print("=" * 50)
    print("This demo showcases Pinnacle data models using mock data")
    print("(No API calls or complex imports required)")
    print("=" * 50)
    print()
    
    # Run all demos
    demos = [
        ("Model Validation", demo_model_validation),
        ("Data Structures", demo_data_structures),
        ("Special Bets", demo_special_bets),
        ("JSON Serialization", demo_json_serialization),
    ]
    
    results = []
    for name, demo_func in demos:
        print(f"Running {name}...")
        result = demo_func()
        results.append((name, result))
        print()
    
    # Summary
    print("📊 Demo Results Summary")
    print("=" * 50)
    for name, result in results:
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{name}: {status}")
    
    all_passed = all(result for _, result in results)
    print()
    print(f"Overall Status: {'🎉 ALL DEMOS PASSED!' if all_passed else '⚠️  SOME DEMOS FAILED'}")
    
    if all_passed:
        print("\n🚀 Pinnacle Models are working perfectly!")
        print("   Key Features Demonstrated:")
        print("   ✅ Pydantic model validation")
        print("   ✅ Complex nested data structures")
        print("   ✅ Enum handling for status/alignment")
        print("   ✅ JSON serialization/deserialization")
        print("   ✅ Special bets (props) support")
        print("   ✅ Parent/child matchup relationships")


if __name__ == "__main__":
    asyncio.run(main()) 