"""
Pinnacle Scraper Demo

This demo shows how to use the new Pinnacle scraper to extract 
essential MLB betting data from Pinnacle's JSON endpoints.
"""

import asyncio
import json
from datetime import datetime
import structlog

from mlb_sharp_betting.scrapers.pinnacle import PinnacleScraper
from mlb_sharp_betting.models.game import Team

# Configure logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


async def demo_pinnacle_scraper():
    """Demonstrate the Pinnacle scraper functionality."""
    print("🏟️  Pinnacle Scraper Demo")
    print("=" * 50)
    
    # Initialize the scraper
    scraper = PinnacleScraper()
    
    try:
        async with scraper:
            print("\n📊 Scraping all current MLB betting data from Pinnacle...")
            
            # Scrape all current MLB data
            result = await scraper.scrape()
            
            print(f"✅ Scraping completed!")
            print(f"   Success: {result.success}")
            print(f"   Markets found: {result.data_count}")
            print(f"   Errors: {result.error_count}")
            print(f"   Response time: {result.response_time_ms:.2f}ms")
            print(f"   Requests made: {result.request_count}")
            
            if result.has_data:
                print("\n📈 Sample Market Data:")
                # Show first few markets
                for i, market in enumerate(result.data[:3]):
                    print(f"\n   Market {i+1}:")
                    print(f"     Type: {market.get('market_type', 'N/A')}")
                    print(f"     Teams: {market.get('teams', {})}")
                    print(f"     Status: {market.get('status', 'N/A')}")
                    
                    # Show prices
                    prices = market.get('prices', [])
                    if prices:
                        print(f"     Prices:")
                        for price in prices:
                            print(f"       {price.get('designation', 'N/A')}: {price.get('price', 'N/A')}")
                    
                    # Show line value if available
                    if 'line_value' in market:
                        print(f"     Line: {market['line_value']}")
                    
                    # Show limits if available
                    limits = market.get('limits', [])
                    if limits:
                        print(f"     Limits:")
                        for limit in limits:
                            print(f"       {limit.get('type', 'N/A')}: ${limit.get('amount', 'N/A')}")
            
            if result.error_count > 0:
                print(f"\n⚠️  Errors encountered:")
                for error in result.errors:
                    print(f"   - {error}")
            
            # Demonstrate team-specific scraping
            print(f"\n🎯 Scraping specific team matchup...")
            print(f"   Looking for: Houston Astros @ Oakland Athletics")
            
            team_result = await scraper.scrape_team_matchup(Team.HOU, Team.OAK)
            
            print(f"✅ Team-specific scraping completed!")
            print(f"   Success: {team_result.success}")
            print(f"   Markets found: {team_result.data_count}")
            
            if team_result.has_data:
                print(f"\n📊 Team Matchup Markets:")
                for market in team_result.data:
                    print(f"   - {market.get('market_type', 'N/A').title()}")
                    prices = market.get('prices', [])
                    for price in prices:
                        print(f"     {price.get('designation', 'N/A')}: {price.get('price', 'N/A')}")
            else:
                print(f"   No markets found for this specific matchup")
            
            # Show scraper metrics
            print(f"\n📈 Scraper Performance Metrics:")
            metrics = scraper.get_metrics()
            print(f"   Total requests: {metrics['total_requests']}")
            print(f"   Failed requests: {metrics['failed_requests']}")
            print(f"   Success rate: {metrics['success_rate']:.2%}")
            print(f"   Avg response time: {metrics['average_response_time_ms']:.2f}ms")
            
    except Exception as e:
        print(f"❌ Demo failed: {str(e)}")
    
    print(f"\n🎉 Pinnacle scraper demo completed!")


def demo_data_extraction():
    """Show how the scraper extracts only essential data."""
    print(f"\n🔍 Data Extraction Focus")
    print("=" * 30)
    print("The Pinnacle scraper is designed to extract ONLY essential information:")
    print("   ✅ Team names and matchup IDs")
    print("   ✅ Market types (moneyline, spread, total)")
    print("   ✅ Current odds/prices")
    print("   ✅ Market status and timing")
    print("   ✅ Betting limits (crucial for sharp betting)")
    print("   ❌ Unnecessary UI elements")
    print("   ❌ Marketing content")
    print("   ❌ Historical data")
    print("   ❌ Player statistics")
    print("\nThis focused approach:")
    print("   • Reduces bandwidth usage")
    print("   • Improves scraping speed")
    print("   • Minimizes storage requirements")
    print("   • Focuses on actionable betting data")


if __name__ == "__main__":
    demo_data_extraction()
    asyncio.run(demo_pinnacle_scraper()) 