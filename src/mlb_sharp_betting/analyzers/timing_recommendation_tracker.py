"""
Timing recommendation tracker for automatic timing analysis data collection.

This analyzer integrates with the master betting detector to automatically track
betting recommendations with timing data for comprehensive timing analysis.
"""

import logging
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

import structlog

from mlb_sharp_betting.services.timing_analysis_service import TimingAnalysisService
from mlb_sharp_betting.models.splits import SplitType, DataSource, BookType
from mlb_sharp_betting.db.connection import DatabaseManager

logger = structlog.get_logger(__name__)


class TimingRecommendationTracker:
    """Tracks betting recommendations with timing data for analysis."""
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize timing recommendation tracker.
        
        Args:
            db_manager: Database connection manager
        """
        self.db_manager = db_manager
        self.timing_service = TimingAnalysisService(db_manager)
        self.logger = logger.bind(service="timing_tracker")
    
    async def track_recommendation_from_master_detector(
        self,
        recommendation: Dict[str, Any]
    ) -> bool:
        """
        Track a recommendation from the master betting detector.
        
        Args:
            recommendation: Recommendation data from master detector
            
        Returns:
            True if successfully tracked
        """
        try:
            # Extract required fields
            game_id = recommendation.get('game_id')
            home_team = recommendation.get('home_team')
            away_team = recommendation.get('away_team')
            game_datetime = recommendation.get('game_datetime')
            source = recommendation.get('source')
            book = recommendation.get('book')
            split_type = recommendation.get('split_type')
            strategy = recommendation.get('strategy')
            recommended_side = recommendation.get('recommended_side')
            odds = recommendation.get('odds')
            confidence = recommendation.get('confidence', 1.0)
            
            # Validate required fields
            if not all([game_id, home_team, away_team, game_datetime, source, split_type, strategy, recommended_side]):
                self.logger.warning(
                    "Missing required fields for timing tracking", 
                    recommendation=recommendation
                )
                return False
            
            # Convert enum types
            source_enum = DataSource(source) if isinstance(source, str) else source
            book_enum = BookType(book) if book and isinstance(book, str) else book
            split_type_enum = SplitType(split_type) if isinstance(split_type, str) else split_type
            
            # Ensure datetime has timezone
            if isinstance(game_datetime, str):
                game_datetime = datetime.fromisoformat(game_datetime.replace('Z', '+00:00'))
            elif game_datetime.tzinfo is None:
                game_datetime = game_datetime.replace(tzinfo=timezone.utc)
            
            # Track the recommendation
            success = await self.timing_service.track_recommendation(
                game_id=str(game_id),
                home_team=str(home_team),
                away_team=str(away_team),
                game_datetime=game_datetime,
                source=source_enum,
                book=book_enum,
                split_type=split_type_enum,
                strategy_name=str(strategy),
                recommended_side=str(recommended_side),
                odds_at_recommendation=float(odds) if odds else None,
                units_wagered=float(confidence)  # Use confidence as units weighting
            )
            
            if success:
                self.logger.info(
                    "Tracked recommendation for timing analysis",
                    game_id=game_id,
                    strategy=strategy,
                    split_type=split_type,
                    source=source
                )
            
            return success
            
        except Exception as e:
            self.logger.error(
                "Failed to track recommendation for timing analysis",
                error=str(e),
                recommendation=recommendation
            )
            return False
    
    async def track_multiple_recommendations(
        self,
        recommendations: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        """
        Track multiple recommendations in batch.
        
        Args:
            recommendations: List of recommendation data
            
        Returns:
            Statistics about tracking results
        """
        stats = {
            'total': len(recommendations),
            'successful': 0,
            'failed': 0
        }
        
        for recommendation in recommendations:
            success = await self.track_recommendation_from_master_detector(recommendation)
            if success:
                stats['successful'] += 1
            else:
                stats['failed'] += 1
        
        self.logger.info(
            "Batch tracking completed",
            **stats
        )
        
        return stats
    
    async def track_recommendation_from_signal(
        self,
        game_id: str,
        home_team: str,
        away_team: str,
        game_datetime: datetime,
        signal_type: str,  # e.g., "sharp_action", "opposing_markets", "steam_move"
        signal_source: str,  # e.g., "VSIN", "SBD"
        signal_book: Optional[str],
        bet_type: str,  # e.g., "moneyline", "spread", "total"
        recommended_side: str,  # e.g., "home", "away", "over", "under"
        signal_strength: float = 1.0,
        odds: Optional[float] = None
    ) -> bool:
        """
        Track a recommendation generated from a specific signal.
        
        Args:
            game_id: Unique game identifier
            home_team: Home team abbreviation
            away_team: Away team abbreviation
            game_datetime: Game start time
            signal_type: Type of signal (strategy)
            signal_source: Source of the signal
            signal_book: Sportsbook (optional)
            bet_type: Type of bet
            recommended_side: Recommended side
            signal_strength: Strength of signal (used as units)
            odds: Odds at recommendation time
            
        Returns:
            True if successfully tracked
        """
        try:
            # Convert to proper types
            source_enum = DataSource(signal_source.upper())
            book_enum = BookType(signal_book.lower()) if signal_book else None
            split_type_enum = SplitType(bet_type.lower())
            
            # Ensure timezone awareness
            if game_datetime.tzinfo is None:
                game_datetime = game_datetime.replace(tzinfo=timezone.utc)
            
            # Track the recommendation
            success = await self.timing_service.track_recommendation(
                game_id=game_id,
                home_team=home_team,
                away_team=away_team,
                game_datetime=game_datetime,
                source=source_enum,
                book=book_enum,
                split_type=split_type_enum,
                strategy_name=f"{signal_type}_strategy",
                recommended_side=recommended_side,
                odds_at_recommendation=odds,
                units_wagered=signal_strength
            )
            
            if success:
                self.logger.info(
                    "Tracked signal-based recommendation",
                    game_id=game_id,
                    signal_type=signal_type,
                    source=signal_source,
                    bet_type=bet_type
                )
            
            return success
            
        except Exception as e:
            self.logger.error(
                "Failed to track signal-based recommendation",
                error=str(e),
                game_id=game_id,
                signal_type=signal_type
            )
            return False
    
    async def update_outcomes_and_run_analysis(
        self,
        days_back: int = 30,
        min_sample_size: int = 10
    ) -> Optional[Dict[str, Any]]:
        """
        Update recommendation outcomes and run fresh timing analysis.
        
        Args:
            days_back: Days to look back for analysis
            min_sample_size: Minimum sample size for analysis
            
        Returns:
            Analysis summary or None if failed
        """
        try:
            # Update outcomes first
            updated_count = await self.timing_service.update_recommendation_outcomes()
            self.logger.info("Updated recommendation outcomes", count=updated_count)
            
            # Run fresh analysis
            end_date = datetime.now(timezone.utc)
            start_date = end_date - timedelta(days=days_back)
            
            analysis = await self.timing_service.analyze_timing_performance(
                start_date=start_date,
                end_date=end_date,
                minimum_sample_size=min_sample_size
            )
            
            # Return summary
            return {
                'analysis_name': analysis.analysis_name,
                'total_games': analysis.total_games_analyzed,
                'total_recommendations': analysis.total_recommendations,
                'best_timing': analysis.best_bucket.value if analysis.best_bucket else None,
                'overall_win_rate': analysis.overall_metrics.win_rate,
                'overall_roi': analysis.overall_metrics.roi_percentage,
                'bucket_count': len(analysis.bucket_analyses),
                'optimal_recommendation': analysis.optimal_timing_recommendation
            }
            
        except Exception as e:
            self.logger.error("Failed to update outcomes and run analysis", error=str(e))
            return None


class MasterDetectorIntegration:
    """Integration class for master betting detector timing tracking."""
    
    def __init__(self, timing_tracker: TimingRecommendationTracker):
        """
        Initialize integration.
        
        Args:
            timing_tracker: Timing recommendation tracker instance
        """
        self.timing_tracker = timing_tracker
        self.logger = logger.bind(service="master_detector_integration")
    
    async def process_master_detector_results(
        self,
        games_data: Dict[Any, Dict[str, List]]
    ) -> Dict[str, int]:
        """
        Process results from master betting detector for timing tracking.
        
        Args:
            games_data: Games data from master detector format:
                {(away, home, game_time): {
                    'sharp_signals': [...],
                    'opposing_markets': [...], 
                    'steam_moves': [...]
                }}
                
        Returns:
            Statistics about tracking results
        """
        recommendations = []
        
        for (away_team, home_team, game_time), signals in games_data.items():
            # Process sharp signals
            for signal in signals.get('sharp_signals', []):
                rec = self._convert_signal_to_recommendation(
                    away_team, home_team, game_time, signal, 'sharp_action'
                )
                if rec:
                    recommendations.append(rec)
            
            # Process opposing markets
            for signal in signals.get('opposing_markets', []):
                rec = self._convert_signal_to_recommendation(
                    away_team, home_team, game_time, signal, 'opposing_markets'
                )
                if rec:
                    recommendations.append(rec)
            
            # Process steam moves
            for signal in signals.get('steam_moves', []):
                rec = self._convert_signal_to_recommendation(
                    away_team, home_team, game_time, signal, 'steam_move'
                )
                if rec:
                    recommendations.append(rec)
        
        # Track all recommendations
        if recommendations:
            return await self.timing_tracker.track_multiple_recommendations(recommendations)
        else:
            return {'total': 0, 'successful': 0, 'failed': 0}
    
    def _convert_signal_to_recommendation(
        self,
        away_team: str,
        home_team: str,
        game_time: datetime,
        signal: Dict[str, Any],
        signal_type: str
    ) -> Optional[Dict[str, Any]]:
        """
        Convert a signal from master detector to recommendation format.
        
        Args:
            away_team: Away team
            home_team: Home team
            game_time: Game start time
            signal: Signal data
            signal_type: Type of signal
            
        Returns:
            Recommendation dictionary or None if conversion fails
        """
        try:
            # Extract common fields
            game_id = f"{away_team}_{home_team}_{game_time.strftime('%Y%m%d')}"
            
            # Map signal fields to recommendation format
            return {
                'game_id': game_id,
                'home_team': home_team,
                'away_team': away_team,
                'game_datetime': game_time,
                'source': signal.get('source', 'VSIN'),
                'book': signal.get('book'),
                'split_type': signal.get('bet_type', 'moneyline'),
                'strategy': f"{signal_type}_strategy",
                'recommended_side': signal.get('recommended_side', 'home'),
                'odds': signal.get('odds'),
                'confidence': signal.get('strength', 1.0)
            }
            
        except Exception as e:
            self.logger.warning(
                "Failed to convert signal to recommendation",
                error=str(e),
                signal=signal,
                signal_type=signal_type
            )
            return None


# Example usage function
async def demo_timing_tracking():
    """Demonstrate timing tracking functionality."""
    from mlb_sharp_betting.db.connection import get_db_manager
    
    db_manager = get_db_manager()
    timing_tracker = TimingRecommendationTracker(db_manager)
    
    # Example recommendation tracking
    example_recommendation = {
        'game_id': 'demo_game_123',
        'home_team': 'LAD',
        'away_team': 'SF',
        'game_datetime': datetime.now(timezone.utc) + timedelta(hours=3),
        'source': 'VSIN',
        'book': 'circa',
        'split_type': 'moneyline',
        'strategy': 'sharp_action_strategy',
        'recommended_side': 'home',
        'odds': -150,
        'confidence': 1.5
    }
    
    success = await timing_tracker.track_recommendation_from_master_detector(example_recommendation)
    print(f"Tracking success: {success}")
    
    # Update outcomes and run analysis
    analysis_summary = await timing_tracker.update_outcomes_and_run_analysis()
    if analysis_summary:
        print(f"Analysis: {analysis_summary}")
    
    db_manager.close()


if __name__ == "__main__":
    import asyncio
    asyncio.run(demo_timing_tracking()) 