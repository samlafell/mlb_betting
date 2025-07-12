#!/usr/bin/env python3
"""
Debug why total_line is not being set correctly.
"""

import asyncio
import re
import json
import sys
from datetime import date
from bs4 import BeautifulSoup

sys.path.append('.')

from sportsbookreview.parsers.sportsbookreview_parser import SportsbookReviewParser

async def debug_total_line_issue():
    """Debug the total_line parsing issue."""
    print("🔍 Debugging total_line parsing issue...")
    
    # Create a mock odds_source that matches what we see in the JSON
    mock_odds_source = {
        'odds': None,
        'homeOdds': None,
        'awayOdds': None,
        'overOdds': -105,
        'underOdds': -115,
        'drawOdds': None,
        'homeSpread': None,
        'awaySpread': None,
        'total': 10
    }
    
    print(f"📊 Mock odds source: {mock_odds_source}")
    
    # Test the _format_odds_line method directly
    parser = SportsbookReviewParser()
    
    # Create a mock line_data structure
    mock_line_data = {
        'currentLine': mock_odds_source,
        'openingLine': mock_odds_source,
        'sportsbook': 'fanduel'
    }
    
    print(f"\n🧪 Testing _format_odds_line with totals bet_type:")
    result = parser._format_odds_line(mock_line_data, 'totals')
    print(f"Result: {result}")
    
    print(f"\n🧪 Testing _format_odds_line with moneyline bet_type:")
    result = parser._format_odds_line(mock_line_data, 'moneyline')
    print(f"Result: {result}")
    
    # Test the condition logic step by step
    print(f"\n🔍 Debugging the condition logic:")
    
    odds_source = mock_odds_source
    over_odds = odds_source.get("overOdds")
    under_odds = odds_source.get("underOdds")
    total_line = odds_source.get("total") or odds_source.get("totalLine")
    
    print(f"over_odds: {over_odds}")
    print(f"under_odds: {under_odds}")
    print(f"total_line: {total_line}")
    print(f"total_line is not None: {total_line is not None}")
    print(f"over_odds is not None: {over_odds is not None}")
    print(f"under_odds is not None: {under_odds is not None}")
    print(f"(over_odds is not None or under_odds is not None): {(over_odds is not None or under_odds is not None)}")
    print(f"Full condition: {total_line is not None and (over_odds is not None or under_odds is not None)}")

if __name__ == "__main__":
    asyncio.run(debug_total_line_issue()) 