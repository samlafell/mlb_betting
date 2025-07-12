#!/usr/bin/env python3
"""
Standalone Action Network Pipeline

This script runs the complete Action Network pipeline without dealing with
complex configuration systems. It extracts URLs, collects historical data,
and analyzes opportunities.

Usage:
    uv run python run_action_network_pipeline.py
"""

import asyncio
import json
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import aiohttp
import structlog

# Configure logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.dev.ConsoleRenderer()
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)


class ActionNetworkStandalonePipeline:
    """Standalone Action Network pipeline that bypasses configuration issues."""
    
    def __init__(self, output_dir: str = "output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Headers for API requests
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': 'https://www.actionnetwork.com/mlb/sharp-report',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-GPC': '1',
        }
    
    async def run_complete_pipeline(self, date: str = "today", max_games: Optional[int] = None):
        """Run the complete Action Network pipeline."""
        logger.info("🚀 Starting Action Network Standalone Pipeline", date=date)
        
        pipeline_results = {
            'pipeline_start': datetime.now().isoformat(),
            'configuration': {
                'date': date,
                'max_games': max_games
            },
            'phases': {}
        }
        
        try:
            # Phase 1: Extract Game URLs
            logger.info("📡 Phase 1: Extracting game URLs")
            urls_result = await self._extract_game_urls(date)
            pipeline_results['phases']['url_extraction'] = urls_result
            
            if not urls_result['success']:
                logger.error("❌ Failed to extract game URLs", error=urls_result['error'])
                return False
            
            logger.info("✅ Game URLs extracted", total_games=urls_result['total_games'])
            
            # Phase 2: Collect Historical Data
            logger.info("📊 Phase 2: Collecting historical data")
            history_result = await self._collect_historical_data(urls_result['games'], max_games)
            pipeline_results['phases']['history_collection'] = history_result
            
            if not history_result['success']:
                logger.error("❌ Failed to collect historical data", error=history_result['error'])
                return False
            
            logger.info("✅ Historical data collected", 
                       games=history_result['games_processed'],
                       movements=history_result['total_movements'])
            
            # Phase 3: Analyze Opportunities
            logger.info("🔍 Phase 3: Analyzing opportunities")
            analysis_result = await self._analyze_opportunities(history_result['historical_data'])
            pipeline_results['phases']['opportunity_analysis'] = analysis_result
            
            if not analysis_result['success']:
                logger.error("❌ Failed to analyze opportunities", error=analysis_result['error'])
                return False
            
            logger.info("✅ Analysis completed",
                       games_analyzed=analysis_result['games_analyzed'],
                       rlm_opportunities=analysis_result['rlm_opportunities'],
                       steam_moves=analysis_result['steam_moves'])
            
            # Phase 4: Generate Reports
            logger.info("📈 Phase 4: Generating reports")
            pipeline_results['pipeline_end'] = datetime.now().isoformat()
            
            # Save results
            await self._save_results(pipeline_results, history_result['historical_data'], analysis_result)
            
            # Display summary
            self._display_summary(pipeline_results, analysis_result)
            
            logger.info("✅ Pipeline completed successfully!")
            return True
            
        except Exception as e:
            logger.error("💥 Pipeline failed", error=str(e))
            pipeline_results['error'] = str(e)
            pipeline_results['pipeline_end'] = datetime.now().isoformat()
            return False
    
    async def _extract_game_urls(self, date: str) -> Dict:
        """Extract game URLs using the existing Action Network extractor."""
        try:
            cmd = [
                'uv', 'run', 'python', '-m', 'action.extract_todays_game_urls',
                '--date', date,
                '--no-test'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode != 0:
                return {
                    'success': False,
                    'error': f"URL extraction failed: {result.stderr}",
                    'total_games': 0,
                    'games': []
                }
            
            # Find the generated file
            import glob
            pattern = f"output/action_network_game_urls_{date}_*.json"
            files = glob.glob(pattern)
            
            if not files:
                return {
                    'success': False,
                    'error': "No URLs file generated",
                    'total_games': 0,
                    'games': []
                }
            
            # Use the most recent file
            latest_file = max(files, key=lambda x: Path(x).stat().st_mtime)
            
            # Load the data
            with open(latest_file, 'r') as f:
                data = json.load(f)
            
            games = data.get('games', [])
            
            return {
                'success': True,
                'total_games': len(games),
                'games': games,
                'source_file': latest_file
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'total_games': 0,
                'games': []
            }
    
    async def _collect_historical_data(self, games: List[Dict], max_games: Optional[int]) -> Dict:
        """Collect historical data from game URLs."""
        try:
            if max_games:
                games = games[:max_games]
            
            logger.info("🔄 Collecting historical data from games", total_games=len(games))
            
            historical_data = []
            
            async with aiohttp.ClientSession(headers=self.headers) as session:
                for i, game in enumerate(games, 1):
                    try:
                        game_id = game.get('game_id')
                        history_url = game.get('history_url')
                        
                        if not history_url:
                            logger.warning("No history URL for game", game_id=game_id)
                            continue
                        
                        logger.info(f"📊 Collecting data for game {i}/{len(games)}", 
                                   game_id=game_id,
                                   teams=f"{game.get('away_team', 'Unknown')} @ {game.get('home_team', 'Unknown')}")
                        
                        # Collect historical data
                        game_history = await self._collect_game_history(session, game_id, history_url, game)
                        if game_history:
                            historical_data.append(game_history)
                        
                        # Be respectful to the API
                        await asyncio.sleep(1)
                        
                    except Exception as e:
                        logger.warning("Error collecting game history", game_id=game.get('game_id'), error=str(e))
                        continue
            
            total_movements = sum(len(game.get('historical_entries', [])) for game in historical_data)
            
            return {
                'success': True,
                'games_processed': len(historical_data),
                'total_movements': total_movements,
                'historical_data': historical_data
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'games_processed': 0,
                'total_movements': 0,
                'historical_data': []
            }
    
    async def _collect_game_history(self, session: aiohttp.ClientSession, game_id: int, history_url: str, game_info: Dict) -> Optional[Dict]:
        """Collect history for a single game."""
        try:
            async with session.get(history_url) as response:
                if response.status != 200:
                    logger.warning("Failed to fetch history", game_id=game_id, status=response.status)
                    return None
                
                data = await response.json()
            
            # Parse the response
            markets = data.get('markets', [])
            historical_entries = []
            
            for market in markets:
                market_type = market.get('market_type')
                books = market.get('books', [])
                
                for book in books:
                    book_id = book.get('book_id')
                    outcomes = book.get('outcomes', [])
                    
                    for outcome in outcomes:
                        # Extract betting percentages if available
                        bet_percent = outcome.get('bet_percent')
                        money_percent = outcome.get('money_percent')
                        
                        historical_entries.append({
                            'market_type': market_type,
                            'book_id': book_id,
                            'outcome_type': outcome.get('type'),
                            'price': outcome.get('price'),
                            'line': outcome.get('line'),
                            'timestamp': outcome.get('timestamp'),
                            'bet_percent': bet_percent,  # Point-in-time snapshot
                            'money_percent': money_percent  # Point-in-time snapshot
                        })
            
            return {
                'game_id': game_id,
                'away_team': game_info.get('away_team', 'Unknown'),
                'home_team': game_info.get('home_team', 'Unknown'),
                'start_time': game_info.get('start_time'),
                'status': game_info.get('status'),
                'collected_at': datetime.now().isoformat(),
                'total_entries': len(historical_entries),
                'historical_entries': historical_entries
            }
            
        except Exception as e:
            logger.error("Error collecting game history", game_id=game_id, error=str(e))
            return None
    
    async def _analyze_opportunities(self, historical_data: List[Dict]) -> Dict:
        """Analyze historical data for opportunities."""
        try:
            logger.info("🔍 Analyzing opportunities", total_games=len(historical_data))
            
            analysis_results = {
                'analyzed_at': datetime.now().isoformat(),
                'total_games': len(historical_data),
                'games_with_movements': 0,
                'total_movements': 0,
                'rlm_opportunities': [],
                'steam_moves': [],
                'high_movement_games': [],
                'market_breakdown': {},
                'book_breakdown': {}
            }
            
            # Book name mapping
            book_mapping = {
                15: 'DraftKings',
                30: 'FanDuel',
                2194: 'Caesars',
                2292: 'BetMGM',
                2888: 'PointsBet',
                2889: 'Barstool'
            }
            
            for game in historical_data:
                entries = game.get('historical_entries', [])
                if not entries:
                    continue
                
                analysis_results['games_with_movements'] += 1
                analysis_results['total_movements'] += len(entries)
                
                # Market and book breakdown
                for entry in entries:
                    market_type = entry.get('market_type', 'unknown')
                    analysis_results['market_breakdown'][market_type] = analysis_results['market_breakdown'].get(market_type, 0) + 1
                    
                    book_id = entry.get('book_id')
                    book_name = book_mapping.get(book_id, f"Book {book_id}")
                    analysis_results['book_breakdown'][book_name] = analysis_results['book_breakdown'].get(book_name, 0) + 1
                
                # High movement detection
                if len(entries) > 200:  # Threshold for high movement
                    analysis_results['high_movement_games'].append({
                        'game_id': game.get('game_id'),
                        'teams': f"{game.get('away_team', 'Unknown')} @ {game.get('home_team', 'Unknown')}",
                        'movement_count': len(entries),
                        'start_time': game.get('start_time')
                    })
                
                # Simple RLM detection (look for betting percentage conflicts)
                rlm_detected = self._detect_rlm_opportunities(game, entries)
                if rlm_detected:
                    analysis_results['rlm_opportunities'].extend(rlm_detected)
                
                # Steam move detection (multiple books moving together)
                steam_detected = self._detect_steam_moves(game, entries)
                if steam_detected:
                    analysis_results['steam_moves'].extend(steam_detected)
            
            return {
                'success': True,
                'games_analyzed': len(historical_data),
                'rlm_opportunities': len(analysis_results['rlm_opportunities']),
                'steam_moves': len(analysis_results['steam_moves']),
                'high_movement_games': len(analysis_results['high_movement_games']),
                'analysis_data': analysis_results
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'games_analyzed': 0,
                'rlm_opportunities': 0,
                'steam_moves': 0,
                'high_movement_games': 0,
                'analysis_data': {}
            }
    
    def _detect_rlm_opportunities(self, game: Dict, entries: List[Dict]) -> List[Dict]:
        """Detect Reverse Line Movement opportunities."""
        rlm_opportunities = []
        
        # Group entries by market type and look for line movements against betting percentages
        market_groups = {}
        for entry in entries:
            market_type = entry.get('market_type')
            if market_type not in market_groups:
                market_groups[market_type] = []
            market_groups[market_type].append(entry)
        
        for market_type, market_entries in market_groups.items():
            # Look for entries with betting percentages
            entries_with_bet_percent = [e for e in market_entries if e.get('bet_percent') is not None]
            
            if len(entries_with_bet_percent) > 1:
                # Simple RLM detection: if public is heavily on one side but line moves the other way
                for entry in entries_with_bet_percent:
                    bet_percent = entry.get('bet_percent', 0)
                    if bet_percent > 70:  # Heavy public betting
                        rlm_opportunities.append({
                            'game_id': game.get('game_id'),
                            'teams': f"{game.get('away_team', 'Unknown')} @ {game.get('home_team', 'Unknown')}",
                            'market_type': market_type,
                            'book_id': entry.get('book_id'),
                            'bet_percent': bet_percent,
                            'money_percent': entry.get('money_percent'),
                            'strength': 'Strong' if bet_percent > 75 else 'Moderate',
                            'timestamp': entry.get('timestamp')
                        })
        
        return rlm_opportunities
    
    def _detect_steam_moves(self, game: Dict, entries: List[Dict]) -> List[Dict]:
        """Detect steam moves across multiple books."""
        steam_moves = []
        
        # Group entries by market type and timestamp window
        market_groups = {}
        for entry in entries:
            market_type = entry.get('market_type')
            if market_type not in market_groups:
                market_groups[market_type] = []
            market_groups[market_type].append(entry)
        
        for market_type, market_entries in market_groups.items():
            # Look for coordinated movements across multiple books
            book_movements = {}
            for entry in market_entries:
                book_id = entry.get('book_id')
                if book_id not in book_movements:
                    book_movements[book_id] = []
                book_movements[book_id].append(entry)
            
            # If 3+ books have movements, consider it a steam move
            if len(book_movements) >= 3:
                steam_moves.append({
                    'game_id': game.get('game_id'),
                    'teams': f"{game.get('away_team', 'Unknown')} @ {game.get('home_team', 'Unknown')}",
                    'market_type': market_type,
                    'participating_books': list(book_movements.keys()),
                    'book_count': len(book_movements),
                    'total_movements': len(market_entries),
                    'strength': 'Strong' if len(book_movements) >= 5 else 'Moderate'
                })
        
        return steam_moves
    
    async def _save_results(self, pipeline_results: Dict, historical_data: List[Dict], analysis_result: Dict):
        """Save all results to files."""
        try:
            # Save historical data
            history_file = self.output_dir / f"historical_line_movement_full_{self.timestamp}.json"
            history_output = {
                'collected_at': datetime.now().isoformat(),
                'total_games': len(historical_data),
                'historical_data': historical_data
            }
            
            with open(history_file, 'w') as f:
                json.dump(history_output, f, indent=2)
            
            # Save analysis results
            analysis_file = self.output_dir / f"analysis_report_{self.timestamp}.json"
            with open(analysis_file, 'w') as f:
                json.dump(analysis_result['analysis_data'], f, indent=2)
            
            # Save opportunities
            opportunities_file = self.output_dir / f"betting_opportunities_{self.timestamp}.json"
            opportunities_data = {
                'generated_at': datetime.now().isoformat(),
                'total_games': analysis_result['games_analyzed'],
                'rlm_opportunities': analysis_result['analysis_data']['rlm_opportunities'],
                'steam_moves': analysis_result['analysis_data']['steam_moves'],
                'high_movement_games': analysis_result['analysis_data']['high_movement_games'],
                'summary': {
                    'total_rlm_opportunities': len(analysis_result['analysis_data']['rlm_opportunities']),
                    'total_steam_moves': len(analysis_result['analysis_data']['steam_moves']),
                    'total_high_movement_games': len(analysis_result['analysis_data']['high_movement_games'])
                }
            }
            
            with open(opportunities_file, 'w') as f:
                json.dump(opportunities_data, f, indent=2)
            
            # Save pipeline results
            pipeline_file = self.output_dir / f"pipeline_results_{self.timestamp}.json"
            with open(pipeline_file, 'w') as f:
                json.dump(pipeline_results, f, indent=2)
            
            logger.info("✅ Results saved",
                       history_file=history_file.name,
                       analysis_file=analysis_file.name,
                       opportunities_file=opportunities_file.name,
                       pipeline_file=pipeline_file.name)
            
        except Exception as e:
            logger.error("Error saving results", error=str(e))
    
    def _display_summary(self, pipeline_results: Dict, analysis_result: Dict):
        """Display pipeline summary."""
        analysis_data = analysis_result.get('analysis_data', {})
        
        print("\n" + "="*80)
        print("🎯 ACTION NETWORK PIPELINE RESULTS")
        print("="*80)
        
        # Execution summary
        start_time = pipeline_results.get('pipeline_start')
        end_time = pipeline_results.get('pipeline_end')
        if start_time and end_time:
            start_dt = datetime.fromisoformat(start_time)
            end_dt = datetime.fromisoformat(end_time)
            duration = end_dt - start_dt
            print(f"⏱️  Execution Time: {duration.total_seconds():.1f}s")
        
        # Phase results
        phases = pipeline_results.get('phases', {})
        if 'url_extraction' in phases:
            print(f"📡 URLs Extracted: {phases['url_extraction'].get('total_games', 0)} games")
        if 'history_collection' in phases:
            print(f"📊 Historical Data: {phases['history_collection'].get('total_movements', 0)} movements")
        if 'opportunity_analysis' in phases:
            print(f"🔍 Games Analyzed: {phases['opportunity_analysis'].get('games_analyzed', 0)}")
        
        # Opportunities
        print(f"\n🎯 OPPORTUNITIES DETECTED:")
        print(f"  • RLM Opportunities: {analysis_result.get('rlm_opportunities', 0)}")
        print(f"  • Steam Moves: {analysis_result.get('steam_moves', 0)}")
        print(f"  • High Movement Games: {analysis_result.get('high_movement_games', 0)}")
        
        # Market breakdown
        market_breakdown = analysis_data.get('market_breakdown', {})
        if market_breakdown:
            print(f"\n📊 MARKET BREAKDOWN:")
            for market, count in market_breakdown.items():
                print(f"  • {market}: {count} movements")
        
        # Book breakdown
        book_breakdown = analysis_data.get('book_breakdown', {})
        if book_breakdown:
            print(f"\n📚 SPORTSBOOK BREAKDOWN:")
            for book, count in book_breakdown.items():
                print(f"  • {book}: {count} movements")
        
        # Top opportunities
        rlm_opportunities = analysis_data.get('rlm_opportunities', [])
        if rlm_opportunities:
            print(f"\n🔄 TOP RLM OPPORTUNITIES:")
            for i, opp in enumerate(rlm_opportunities[:5], 1):
                print(f"  {i}. {opp['teams']} - {opp['market_type']} ({opp['strength']})")
        
        steam_moves = analysis_data.get('steam_moves', [])
        if steam_moves:
            print(f"\n🚂 TOP STEAM MOVES:")
            for i, move in enumerate(steam_moves[:5], 1):
                print(f"  {i}. {move['teams']} - {move['market_type']} ({move['book_count']} books)")
        
        high_movement_games = analysis_data.get('high_movement_games', [])
        if high_movement_games:
            print(f"\n📈 HIGH MOVEMENT GAMES:")
            for i, game in enumerate(high_movement_games[:5], 1):
                print(f"  {i}. {game['teams']} - {game['movement_count']} movements")
        
        print("\n" + "="*80)
        print("📁 Results saved to 'output' directory")
        print("="*80)


async def main():
    """Main entry point."""
    print("🚀 Action Network Standalone Pipeline")
    print("=" * 60)
    
    # Create pipeline instance
    pipeline = ActionNetworkStandalonePipeline()
    
    # Run the complete pipeline
    success = await pipeline.run_complete_pipeline(
        date="today",
        max_games=5  # Limit to 5 games for testing
    )
    
    if success:
        print("\n✅ Pipeline completed successfully!")
        print("📁 Check the 'output' directory for detailed results")
    else:
        print("\n❌ Pipeline failed")
        print("📋 Check the logs above for details")


if __name__ == "__main__":
    asyncio.run(main()) 