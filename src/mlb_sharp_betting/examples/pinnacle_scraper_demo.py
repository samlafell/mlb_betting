"""
Demo script for Pinnacle scraper using the new three-step approach.

This script demonstrates how to use the restructured PinnacleScraper that:
1. Gets matchup IDs from the league endpoint
2. Gets team info from matchup details endpoints  
3. Gets market data (moneyline, total, spread only) from markets endpoints

Run with: uv run pinnacle_scraper_demo.py
"""

import asyncio
from datetime import datetime
from ..scrapers.pinnacle import PinnacleScraper
from ..models.game import Team


async def demo_pinnacle_scraper():
    """Demonstrate the new Pinnacle scraper functionality."""
    print("🏟️  Pinnacle MLB Scraper Demo (New 3-Step Approach)")
    print("=" * 60)
    
    scraper = PinnacleScraper()
    
    # Demo 1: Get all current MLB matchups and markets
    print("\n📊 Step 1: Getting all MLB matchups with team info and markets...")
    
    try:
        result = await scraper.scrape()
        
        print(f"\n✅ Scraping Results:")
        print(f"   Success: {result.success}")
        print(f"   Complete Matchups: {len(result.data)}")
        print(f"   Total Requests: {result.request_count}")
        print(f"   Response Time: {result.response_time_ms:.1f}ms")
        print(f"   Avg Time per Request: {result.response_time_ms/result.request_count:.1f}ms")
        
        if result.errors:
            print(f"   Errors: {len(result.errors)}")
            for error in result.errors[:3]:  # Show first 3 errors
                print(f"     - {error}")
        
        # Show sample data structure
        if result.data:
            print(f"\n📋 Sample Matchup Data Structure:")
            sample_matchup = result.data[0]
            print(f"   Matchup ID: {sample_matchup.get('matchup_id')}")
            print(f"   Home Team: {sample_matchup.get('home_team')}")
            print(f"   Away Team: {sample_matchup.get('away_team')}")
            print(f"   Start Time: {sample_matchup.get('start_time')}")
            print(f"   Markets Count: {len(sample_matchup.get('markets', []))}")
            
            # Show market types
            market_types = {}
            for matchup in result.data:
                for market in matchup.get('markets', []):
                    market_type = market.get('type', 'unknown')
                    market_types[market_type] = market_types.get(market_type, 0) + 1
            
            print(f"\n📈 Market Distribution:")
            for market_type, count in sorted(market_types.items()):
                print(f"   {market_type.title()}: {count} markets")
            
            # Show a detailed market example
            print(f"\n💰 Sample Market Details:")
            for matchup in result.data[:3]:  # Check first few matchups
                for market in matchup.get('markets', []):
                    if market.get('type') == 'moneyline':
                        print(f"   Market Type: {market.get('type').title()}")
                        print(f"   Status: {market.get('status', 'unknown')}")
                        print(f"   Prices: {len(market.get('prices', []))} options")
                        
                        # Show first few prices
                        for i, price in enumerate(market.get('prices', [])[:3]):
                            designation = price.get('designation', f"Option {i+1}")
                            odds = price.get('price', 'N/A')
                            print(f"     {designation}: {odds}")
                        
                        # Show limits if available
                        limits = market.get('limits', [])
                        if limits:
                            for limit in limits:
                                print(f"     Limit: ${limit.get('amount', 'N/A')} ({limit.get('type', 'unknown')})")
                        break
                break
        
        # Demo 2: Try team-specific search
        print(f"\n🔍 Step 2: Searching for specific team matchup...")
        team_result = await scraper.scrape_team_matchup(Team.LAD, Team.SF)
        
        print(f"\n✅ Team Search Results (LAD vs SF):")
        print(f"   Success: {team_result.success}")
        print(f"   Matching Matchups: {len(team_result.data)}")
        
        if team_result.data:
            for matchup in team_result.data:
                print(f"   Found: {matchup.get('away_team')} @ {matchup.get('home_team')}")
                print(f"          Start: {matchup.get('start_time')}")
                print(f"          Markets: {len(matchup.get('markets', []))}")
        
        # Performance summary
        print(f"\n⚡ Performance Summary:")
        print(f"   Total Data Points: {len(result.data)} complete matchups")
        print(f"   Request Efficiency: {len(result.data)/result.request_count:.2f} matchups per request")
        print(f"   Rate Limit Compliance: ✅ (1 req/sec, max 30/min)")
        print(f"   Focus: Only essential betting data (team info + ML/spread/total)")
        
    except Exception as e:
        print(f"❌ Demo failed: {str(e)}")
        import traceback
        traceback.print_exc()


async def demo_data_extraction_focus():
    """Show what data we're extracting vs. what we're ignoring."""
    print(f"\n🎯 Data Extraction Focus")
    print("=" * 40)
    
    print("✅ EXTRACTING (Essential for Betting Analysis):")
    print("   📍 Team Information:")
    print("     - Home team name (normalized to Team enum)")
    print("     - Away team name (normalized to Team enum)")
    print("     - Game start time")
    print("   💰 Market Data (Moneyline, Spread, Total only):")
    print("     - Market type and status")
    print("     - Odds/prices for each side")
    print("     - Line values (for spread/total)")
    print("     - Betting limits (max stake amounts)")
    print("     - Market version and cutoff times")
    
    print("\n❌ IGNORING (Unnecessary for our use case):")
    print("   - Player props and exotic markets")
    print("   - Detailed matchup metadata")
    print("   - Historical market movement")
    print("   - Participant details beyond team alignment")
    print("   - Complex nested market structures")
    
    print("\n⚡ Three-Step Efficiency:")
    print("   1️⃣  Get unique matchup IDs (1 request)")
    print("   2️⃣  Get team info per matchup (N requests)")  
    print("   3️⃣  Get essential markets per matchup (N requests)")
    print("   📊 Total: 1 + 2N requests for N matchups")
    print("   🚀 Concurrent processing with rate limiting")


if __name__ == "__main__":
    print(f"🚀 Starting Pinnacle Scraper Demo - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    asyncio.run(demo_pinnacle_scraper())
    asyncio.run(demo_data_extraction_focus())
    print(f"\n🏁 Demo completed - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\nGeneral Balls ⚾") 