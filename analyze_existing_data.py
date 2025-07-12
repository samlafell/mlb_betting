#!/usr/bin/env python3
"""
Analyze Existing Action Network Historical Data

This script analyzes the existing historical data file and detects
betting opportunities including RLM, steam moves, and high movement games.

Usage:
    uv run python analyze_existing_data.py
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any
from collections import defaultdict


def analyze_historical_data(data_file: str = "output/historical_line_movement_full_20250711_165111.json"):
    """Analyze the existing historical data file."""
    
    print("🔍 Action Network Historical Data Analysis")
    print("=" * 60)
    
    # Load the data
    try:
        with open(data_file, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"❌ File not found: {data_file}")
        return
    except json.JSONDecodeError:
        print(f"❌ Invalid JSON file: {data_file}")
        return
    
    historical_data = data.get('historical_data', [])
    print(f"📊 Total games: {len(historical_data)}")
    
    # Analysis results
    analysis_results = {
        'total_games': len(historical_data),
        'games_with_movements': 0,
        'total_movements': 0,
        'market_breakdown': defaultdict(int),
        'book_breakdown': defaultdict(int),
        'games_by_movement': [],
        'rlm_opportunities': [],
        'steam_moves': [],
        'high_movement_games': []
    }
    
    # Book mapping
    book_mapping = {
        15: 'DraftKings',
        30: 'FanDuel',
        2194: 'Caesars',
        2292: 'BetMGM',
        2888: 'PointsBet',
        2889: 'Barstool'
    }
    
    # Analyze each game
    for game in historical_data:
        game_id = game.get('game_id')
        home_team = game.get('home_team', 'Unknown')
        away_team = game.get('away_team', 'Unknown')
        teams = f"{away_team} @ {home_team}"
        
        raw_data = game.get('raw_data', {})
        game_movements = 0
        game_markets = set()
        game_books = set()
        
        # Process each book's data
        for book_id_str, book_data in raw_data.items():
            try:
                book_id = int(book_id_str)
                book_name = book_mapping.get(book_id, f"Book {book_id}")
                game_books.add(book_name)
                
                event_data = book_data.get('event', {})
                
                # Process each market type
                for market_type, market_data in event_data.items():
                    if not isinstance(market_data, list):
                        continue
                    
                    game_markets.add(market_type)
                    
                    # Count movements in history
                    for outcome in market_data:
                        history = outcome.get('history', [])
                        movements = len(history)
                        game_movements += movements
                        
                        analysis_results['market_breakdown'][market_type] += movements
                        analysis_results['book_breakdown'][book_name] += movements
                        
                        # Check for RLM opportunities
                        rlm_detected = detect_rlm_in_outcome(outcome, game_id, teams, market_type, book_name)
                        if rlm_detected:
                            analysis_results['rlm_opportunities'].extend(rlm_detected)
                
            except (ValueError, TypeError) as e:
                continue
        
        if game_movements > 0:
            analysis_results['games_with_movements'] += 1
            analysis_results['total_movements'] += game_movements
            
            game_info = {
                'game_id': game_id,
                'teams': teams,
                'movements': game_movements,
                'markets': len(game_markets),
                'books': len(game_books)
            }
            analysis_results['games_by_movement'].append(game_info)
            
            # High movement detection
            if game_movements > 200:
                analysis_results['high_movement_games'].append(game_info)
    
    # Sort games by movement count
    analysis_results['games_by_movement'].sort(key=lambda x: x['movements'], reverse=True)
    
    # Detect steam moves (games with movements across multiple books)
    for game_info in analysis_results['games_by_movement']:
        if game_info['books'] >= 3 and game_info['movements'] > 100:
            analysis_results['steam_moves'].append({
                'game_id': game_info['game_id'],
                'teams': game_info['teams'],
                'movements': game_info['movements'],
                'books': game_info['books'],
                'markets': game_info['markets'],
                'strength': 'Strong' if game_info['books'] >= 5 else 'Moderate'
            })
    
    # Display results
    display_analysis_results(analysis_results)
    
    # Save results
    save_analysis_results(analysis_results)
    
    return analysis_results


def detect_rlm_in_outcome(outcome: Dict, game_id: int, teams: str, market_type: str, book_name: str) -> List[Dict]:
    """Detect RLM opportunities in a single outcome."""
    rlm_opportunities = []
    
    bet_info = outcome.get('bet_info', {})
    tickets = bet_info.get('tickets', {})
    money = bet_info.get('money', {})
    
    ticket_percent = tickets.get('percent', 0)
    money_percent = money.get('percent', 0)
    
    # RLM detection: line moves opposite to heavy public betting
    if ticket_percent > 0 and money_percent > 0:
        # Check for significant disparity (sharp money vs public)
        disparity = abs(ticket_percent - money_percent)
        
        if disparity > 20:  # Significant disparity indicates sharp action
            strength = 'Strong' if disparity > 30 else 'Moderate'
            
            rlm_opportunities.append({
                'game_id': game_id,
                'teams': teams,
                'market_type': market_type,
                'book': book_name,
                'ticket_percent': ticket_percent,
                'money_percent': money_percent,
                'disparity': disparity,
                'strength': strength,
                'side': outcome.get('side', 'unknown'),
                'current_odds': outcome.get('odds', 0),
                'current_value': outcome.get('value', 0)
            })
    
    return rlm_opportunities


def display_analysis_results(results: Dict):
    """Display comprehensive analysis results."""
    print(f"\n📊 ANALYSIS RESULTS")
    print("=" * 60)
    
    # Summary statistics
    print(f"📈 Games with movements: {results['games_with_movements']}")
    print(f"🔄 Total movements: {results['total_movements']:,}")
    print(f"🎯 RLM opportunities: {len(results['rlm_opportunities'])}")
    print(f"🚂 Steam moves: {len(results['steam_moves'])}")
    print(f"📊 High movement games: {len(results['high_movement_games'])}")
    
    # Top games by movement
    print(f"\n🏆 TOP 10 GAMES BY MOVEMENT COUNT:")
    for i, game in enumerate(results['games_by_movement'][:10], 1):
        print(f"  {i:2d}. {game['teams']:<40} {game['movements']:,} movements ({game['markets']} markets, {game['books']} books)")
    
    # Market breakdown
    print(f"\n📊 MARKET BREAKDOWN:")
    for market, count in sorted(results['market_breakdown'].items(), key=lambda x: x[1], reverse=True):
        print(f"  • {market:<15}: {count:,} movements")
    
    # Book breakdown
    print(f"\n📚 SPORTSBOOK BREAKDOWN:")
    for book, count in sorted(results['book_breakdown'].items(), key=lambda x: x[1], reverse=True):
        print(f"  • {book:<15}: {count:,} movements")
    
    # RLM opportunities
    if results['rlm_opportunities']:
        print(f"\n🔄 RLM OPPORTUNITIES:")
        for i, rlm in enumerate(results['rlm_opportunities'][:10], 1):
            print(f"  {i:2d}. {rlm['teams']:<35} {rlm['market_type']:<10} {rlm['book']:<12} ({rlm['strength']})")
            print(f"      Tickets: {rlm['ticket_percent']}% | Money: {rlm['money_percent']}% | Disparity: {rlm['disparity']}%")
    
    # Steam moves
    if results['steam_moves']:
        print(f"\n🚂 STEAM MOVES:")
        for i, steam in enumerate(results['steam_moves'][:10], 1):
            print(f"  {i:2d}. {steam['teams']:<35} {steam['movements']:,} movements ({steam['books']} books, {steam['strength']})")
    
    # High movement games
    if results['high_movement_games']:
        print(f"\n📈 HIGH MOVEMENT GAMES:")
        for i, game in enumerate(results['high_movement_games'][:10], 1):
            print(f"  {i:2d}. {game['teams']:<35} {game['movements']:,} movements")


def save_analysis_results(results: Dict):
    """Save analysis results to file."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f"output/comprehensive_analysis_{timestamp}.json"
    
    # Convert defaultdict to regular dict for JSON serialization
    results_copy = dict(results)
    results_copy['market_breakdown'] = dict(results['market_breakdown'])
    results_copy['book_breakdown'] = dict(results['book_breakdown'])
    results_copy['analyzed_at'] = datetime.now().isoformat()
    
    with open(output_file, 'w') as f:
        json.dump(results_copy, f, indent=2)
    
    print(f"\n💾 Results saved to: {output_file}")


def main():
    """Main entry point."""
    # Check if the data file exists
    data_file = "output/historical_line_movement_full_20250711_165111.json"
    
    if not Path(data_file).exists():
        print(f"❌ Data file not found: {data_file}")
        print("Please run the Action Network pipeline first to collect historical data.")
        return
    
    # Run the analysis
    results = analyze_historical_data(data_file)
    
    if results:
        print(f"\n✅ Analysis completed successfully!")
        print(f"📊 Found {results['total_movements']:,} total movements across {results['games_with_movements']} games")
        print(f"🎯 Detected {len(results['rlm_opportunities'])} RLM opportunities")
        print(f"🚂 Identified {len(results['steam_moves'])} steam moves")
        
        print(f"\n🎯 KEY INSIGHTS:")
        if results['games_by_movement']:
            top_game = results['games_by_movement'][0]
            print(f"  • Most active game: {top_game['teams']} ({top_game['movements']:,} movements)")
        
        if results['rlm_opportunities']:
            strong_rlm = [r for r in results['rlm_opportunities'] if r['strength'] == 'Strong']
            print(f"  • Strong RLM opportunities: {len(strong_rlm)}")
        
        if results['steam_moves']:
            strong_steam = [s for s in results['steam_moves'] if s['strength'] == 'Strong']
            print(f"  • Strong steam moves: {len(strong_steam)}")


if __name__ == "__main__":
    main() 