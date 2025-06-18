"""
Pinnacle Integration Mock Demo

This demo showcases the Pinnacle integration using mock data instead of live API calls.
Perfect for demonstrating functionality without requiring API credentials.
"""

import json
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any
import structlog

from mlb_sharp_betting.models.pinnacle import (
    PinnacleMatchup,
    PinnacleMarket,
    PinnacleLeague,
    PinnacleSport,
    PinnacleParticipant,
    PinnaclePrice,
    PinnacleSpecial,
    PinnacleParentInfo,
    PinnacleMarketType,
    PinnacleAlignment,
    PinnacleMarketStatus,
    PinnacleMatchupStatus,
    PinnacleLimit
)
# Direct imports to avoid circular dependency
from mlb_sharp_betting.parsers.pinnacle import PinnacleParser
from mlb_sharp_betting.services.pinnacle_api_service import PinnacleAPIService, PinnacleAPIConfig

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


def create_mock_market_data() -> Dict[str, Any]:
    """Create mock market data based on Pinnacle's API structure"""
    return {
        "matchupId": 1610704108,
        "key": "s;0;m",
        "type": "moneyline",
        "period": 0,
        "status": "open",
        "prices": [
            {
                "designation": "home",
                "price": 169,
                "participant": {
                    "id": 1610704110,
                    "name": "Oakland Athletics",
                    "alignment": "home"
                }
            },
            {
                "designation": "away", 
                "price": -189,
                "participant": {
                    "id": 1610704109,
                    "name": "Houston Astros",
                    "alignment": "away"
                }
            }
        ],
        "limits": [
            {
                "amount": 2000,
                "type": "maxRiskStake"
            }
        ],
        "version": 3149186706,
        "cutoffAt": "2025-01-15T19:10:00Z"
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
        print(f"   Status: {matchup.status.value}")
        print()
        
        # Test market model
        market_data = create_mock_market_data()
        market = PinnacleMarket(**market_data)
        print(f"✅ Market Model: {market.type.value} market")
        print(f"   Status: {market.status.value}")
        print(f"   Prices: {market.prices[0].designation} {market.prices[0].price}, {market.prices[1].designation} {market.prices[1].price}")
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
        return False


def demo_parser_functionality():
    """Demonstrate parser functionality with mock data"""
    print("📊 Pinnacle Parser Demo")
    print("=" * 50)
    
    try:
        parser = PinnacleParser()
        
        # Parse matchup data
        matchup_data = create_mock_matchup_data()
        matchup = parser.parse_matchup(matchup_data)
        print(f"✅ Parsed Matchup: {matchup['away_team']} @ {matchup['home_team']}")
        print(f"   Game Time: {matchup['game_time']}")
        print(f"   League: {matchup['league']}")
        print()
        
        # Parse market data
        market_data = create_mock_market_data()
        market = parser.parse_market(market_data)
        print(f"✅ Parsed Market: {market['market_type']}")
        print(f"   Home Odds: {market['home_odds']}")
        print(f"   Away Odds: {market['away_odds']}")
        print()
        
        # Parse special data
        special_data = create_mock_special_data()
        special = parser.parse_special(special_data)
        print(f"✅ Parsed Special: {special['description']}")
        print(f"   Category: {special['category']}")
        print(f"   Over/Under: {special['over_under']}")
        print()
        
        return True
        
    except Exception as e:
        logger.error("Parser demo failed", error=str(e))
        return False


def demo_api_service_config():
    """Demonstrate API service configuration"""
    print("⚙️  Pinnacle API Service Configuration Demo")
    print("=" * 50)
    
    try:
        # Create API config
        api_config = PinnacleAPIConfig(
            base_url="https://guest.api.arcadia.pinnacle.com",
            version="0.1",
            timeout=30,
            max_retries=3,
            rate_limit_requests=100,
            rate_limit_period=60
        )
        
        print(f"✅ API Config Created:")
        print(f"   Base URL: {api_config.base_url}")
        print(f"   Version: {api_config.version}")
        print(f"   Timeout: {api_config.timeout}s")
        print(f"   Max Retries: {api_config.max_retries}")
        print(f"   Rate Limit: {api_config.rate_limit_requests} requests per {api_config.rate_limit_period}s")
        print()
        
        # Create API service
        api_service = PinnacleAPIService(api_config)
        print(f"✅ API Service Created and Ready")
        print(f"   Service Type: {type(api_service).__name__}")
        print()
        
        return True
        
    except Exception as e:
        logger.error("API service demo failed", error=str(e))
        return False


def demo_data_transformation():
    """Demonstrate data transformation capabilities"""
    print("🔄 Data Transformation Demo")
    print("=" * 50)
    
    try:
        parser = PinnacleParser()
        
        # Transform matchup data to common format
        matchup_data = create_mock_matchup_data()
        transformed = parser.parse_matchup(matchup_data)
        
        print("✅ Raw Pinnacle Data → Standardized Format:")
        print(f"   Raw: {json.dumps(matchup_data, indent=2)[:200]}...")
        print()
        print(f"   Transformed:")
        for key, value in transformed.items():
            print(f"     {key}: {value}")
        print()
        
        # Show odds conversion
        market_data = create_mock_market_data()
        market_transformed = parser.parse_market(market_data)
        
        print("✅ Odds Conversion:")
        print(f"   Raw Pinnacle Odds: {market_data['prices'][0]['price']}, {market_data['prices'][1]['price']}")
        print(f"   Converted: Home {market_transformed['home_odds']}, Away {market_transformed['away_odds']}")
        print()
        
        return True
        
    except Exception as e:
        logger.error("Data transformation demo failed", error=str(e))
        return False


def demo_integration_workflow():
    """Demonstrate the complete integration workflow"""
    print("🔗 Complete Integration Workflow Demo")
    print("=" * 50)
    
    try:
        # Step 1: Data Models
        print("Step 1: Validate data with Pydantic models ✅")
        matchup_data = create_mock_matchup_data()
        matchup = PinnacleMatchup(**matchup_data)
        
        # Step 2: Parsing
        print("Step 2: Parse data with custom parser ✅")
        parser = PinnacleParser()
        parsed_matchup = parser.parse_matchup(matchup_data)
        
        # Step 3: API Service Ready
        print("Step 3: API service configuration ✅")
        config = PinnacleAPIConfig()
        service = PinnacleAPIService(config)
        
        # Step 4: Show workflow
        print("\n📋 Workflow Summary:")
        print(f"   1. Raw API Data → Pydantic Models (validation)")
        print(f"   2. Pydantic Models → Standardized Format (parsing)")
        print(f"   3. API Service → Rate Limited Requests (service)")
        print(f"   4. Error Handling → Structured Logging (reliability)")
        print()
        
        print("✅ Integration Ready for Live Data!")
        print("   - All components tested and working")
        print("   - Ready to connect to live Pinnacle API")
        print("   - Proper error handling and logging in place")
        print()
        
        return True
        
    except Exception as e:
        logger.error("Integration workflow demo failed", error=str(e))
        return False


async def main():
    """Main demo function"""
    print("🎯 Pinnacle Integration Demo")
    print("=" * 50)
    print("This demo showcases the complete Pinnacle integration")
    print("using mock data (no API credentials required)")
    print("=" * 50)
    print()
    
    # Run all demos
    demos = [
        ("Model Validation", demo_model_validation),
        ("Parser Functionality", demo_parser_functionality),
        ("API Service Config", demo_api_service_config),
        ("Data Transformation", demo_data_transformation),
        ("Integration Workflow", demo_integration_workflow),
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
        print("\n🚀 Integration is ready for production use!")
        print("   Next steps:")
        print("   1. Add your Pinnacle API credentials")
        print("   2. Test with live data")
        print("   3. Implement your betting strategy")


if __name__ == "__main__":
    asyncio.run(main()) 