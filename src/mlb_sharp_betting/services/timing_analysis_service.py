"""
Timing analysis service for evaluating betting recommendation accuracy by timing.

This service analyzes the performance of betting recommendations based on their timing
relative to game start, providing insights for optimal bet placement timing.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Union
from decimal import Decimal
import json

import structlog
from psycopg2.extras import RealDictCursor

from mlb_sharp_betting.models.timing_analysis import (
    TimingBucket, 
    ConfidenceLevel,
    TimingPerformanceMetrics, 
    TimingBucketAnalysis,
    ComprehensiveTimingAnalysis,
    RealtimeTimingLookup,
    TimingRecommendation
)
from mlb_sharp_betting.models.splits import SplitType, DataSource, BookType
from mlb_sharp_betting.db.connection import DatabaseManager

logger = structlog.get_logger(__name__)


class TimingAnalysisService:
    """Service for analyzing betting recommendation timing accuracy."""
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize timing analysis service.
        
        Args:
            db_manager: Database connection manager
        """
        self.db_manager = db_manager
        self.logger = logger.bind(service="timing_analysis")
    
    async def analyze_timing_performance(
        self,
        start_date: datetime,
        end_date: datetime,
        source: Optional[DataSource] = None,
        book: Optional[BookType] = None,
        split_type: Optional[SplitType] = None,
        strategy_name: Optional[str] = None,
        minimum_sample_size: int = 10
    ) -> ComprehensiveTimingAnalysis:
        """
        Perform comprehensive timing analysis for the specified period and filters.
        
        Args:
            start_date: Start of analysis period
            end_date: End of analysis period  
            source: Optional data source filter
            book: Optional sportsbook filter
            split_type: Optional bet type filter
            strategy_name: Optional strategy filter
            minimum_sample_size: Minimum bets required for bucket analysis
            
        Returns:
            Complete timing analysis results
        """
        self.logger.info(
            "Starting timing analysis",
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            source=source,
            book=book,
            split_type=split_type,
            strategy_name=strategy_name
        )
        
        # Get data from recommendation history
        bucket_analyses = []
        total_games = 0
        total_recommendations = 0
        overall_metrics = None
        
        with self.db_manager.get_cursor() as cursor:
            # First, get overall statistics
            overall_query, overall_params = self._build_analysis_query(
                start_date, end_date, source, book, split_type, strategy_name, 
                bucket_filter=None
            )
            
            cursor.execute(overall_query, overall_params)
            overall_result = cursor.fetchone()
            
            if overall_result and overall_result['total_bets'] > 0:
                overall_metrics = TimingPerformanceMetrics(
                    total_bets=overall_result['total_bets'],
                    wins=overall_result['wins'],
                    losses=overall_result['losses'], 
                    pushes=overall_result['pushes'],
                    total_units_wagered=Decimal(str(overall_result['total_units_wagered'])),
                    total_profit_loss=Decimal(str(overall_result['total_profit_loss'])),
                    avg_odds_at_recommendation=Decimal(str(overall_result['avg_odds_at_recommendation'])) if overall_result['avg_odds_at_recommendation'] else None,
                    avg_closing_odds=Decimal(str(overall_result['avg_closing_odds'])) if overall_result['avg_closing_odds'] else None
                )
                total_recommendations = overall_result['total_bets']
            
            # Get count of unique games
            cursor.execute("""
                SELECT COUNT(DISTINCT game_id) as game_count
                FROM timing_analysis.recommendation_history
                WHERE recommendation_datetime >= %s 
                  AND recommendation_datetime <= %s
                  AND (%s IS NULL OR source = %s)
                  AND (%s IS NULL OR book = %s)
                  AND (%s IS NULL OR split_type = %s)
                  AND (%s IS NULL OR strategy_name = %s)
            """, (
                start_date, end_date, 
                source.value if source else None, source.value if source else None,
                book.value if book else None, book.value if book else None,
                split_type.value if split_type else None, split_type.value if split_type else None,
                strategy_name, strategy_name
            ))
            
            game_result = cursor.fetchone()
            total_games = game_result['game_count'] if game_result else 0
            
            # Analyze each timing bucket
            for bucket in TimingBucket:
                bucket_query, bucket_params = self._build_analysis_query(
                    start_date, end_date, source, book, split_type, strategy_name,
                    bucket_filter=bucket
                )
                
                cursor.execute(bucket_query, bucket_params)
                bucket_result = cursor.fetchone()
                
                if bucket_result and bucket_result['total_bets'] >= minimum_sample_size:
                    bucket_metrics = TimingPerformanceMetrics(
                        total_bets=bucket_result['total_bets'],
                        wins=bucket_result['wins'],
                        losses=bucket_result['losses'],
                        pushes=bucket_result['pushes'],
                        total_units_wagered=Decimal(str(bucket_result['total_units_wagered'])),
                        total_profit_loss=Decimal(str(bucket_result['total_profit_loss'])),
                        avg_odds_at_recommendation=Decimal(str(bucket_result['avg_odds_at_recommendation'])) if bucket_result['avg_odds_at_recommendation'] else None,
                        avg_closing_odds=Decimal(str(bucket_result['avg_closing_odds'])) if bucket_result['avg_closing_odds'] else None
                    )
                    
                    bucket_analysis = TimingBucketAnalysis(
                        timing_bucket=bucket,
                        source=source,
                        book=book,
                        split_type=split_type,
                        strategy_name=strategy_name,
                        metrics=bucket_metrics,
                        analysis_start_date=start_date,
                        analysis_end_date=end_date
                    )
                    
                    bucket_analyses.append(bucket_analysis)
        
        # Default overall metrics if no data
        if overall_metrics is None:
            overall_metrics = TimingPerformanceMetrics(
                total_bets=0,
                wins=0,
                losses=0,
                pushes=0,
                total_units_wagered=Decimal('0'),
                total_profit_loss=Decimal('0')
            )
        
        # Find best performers
        best_bucket = None
        best_source = None
        best_strategy = None
        
        if bucket_analyses:
            # Find best bucket by ROI
            best_bucket_analysis = max(bucket_analyses, key=lambda x: x.metrics.roi_percentage)
            best_bucket = best_bucket_analysis.timing_bucket
            
            # If not filtered, find best source and strategy
            if source is None:
                best_source = await self._find_best_source(start_date, end_date, book, split_type, strategy_name)
            if strategy_name is None:
                best_strategy = await self._find_best_strategy(start_date, end_date, source, book, split_type)
        
        # Generate trend analysis
        trends = await self._analyze_trends(start_date, end_date, bucket_analyses)
        
        # Create comprehensive analysis
        analysis = ComprehensiveTimingAnalysis(
            analysis_name=f"Timing Analysis {start_date.date()} to {end_date.date()}",
            total_games_analyzed=total_games,
            total_recommendations=total_recommendations,
            bucket_analyses=bucket_analyses,
            overall_metrics=overall_metrics,
            best_bucket=best_bucket,
            best_source=best_source,
            best_strategy=best_strategy,
            trends=trends
        )
        
        # Store analysis results
        await self._store_analysis_results(analysis)
        
        self.logger.info(
            "Timing analysis completed",
            total_games=total_games,
            total_recommendations=total_recommendations,
            buckets_analyzed=len(bucket_analyses),
            best_bucket=best_bucket.value if hasattr(best_bucket, 'value') and best_bucket else (best_bucket if best_bucket else None)
        )
        
        return analysis
    
    async def get_realtime_timing_recommendation(
        self,
        lookup: RealtimeTimingLookup,
        use_cache: bool = True
    ) -> TimingRecommendation:
        """
        Get real-time timing recommendation for a betting opportunity.
        
        Args:
            lookup: Lookup parameters for the recommendation
            use_cache: Whether to use cached recommendations
            
        Returns:
            Timing-based recommendation
        """
        # Check cache first if enabled
        if use_cache:
            cached_rec = await self._get_cached_recommendation(lookup)
            if cached_rec:
                return cached_rec
        
        # Get historical performance for this timing/context
        historical_metrics = await self._get_historical_performance(lookup)
        
        # Generate recommendation
        recommendation = self._generate_recommendation(lookup, historical_metrics)
        
        # Cache the recommendation
        if use_cache:
            await self._cache_recommendation(lookup, recommendation)
        
        return recommendation
    
    async def track_recommendation(
        self,
        game_id: str,
        home_team: str,
        away_team: str,
        game_datetime: datetime,
        source: DataSource,
        book: Optional[BookType],
        split_type: SplitType,
        strategy_name: str,
        recommended_side: str,
        odds_at_recommendation: Optional[float] = None,
        units_wagered: float = 1.0
    ) -> bool:
        """
        Track a betting recommendation for timing analysis.
        
        Args:
            game_id: Unique game identifier
            home_team: Home team abbreviation
            away_team: Away team abbreviation  
            game_datetime: Game start time
            source: Data source
            book: Sportsbook (optional)
            split_type: Type of bet
            strategy_name: Strategy that generated the recommendation
            recommended_side: Recommended betting side
            odds_at_recommendation: Odds when recommendation was made
            units_wagered: Units to wager (default 1.0)
            
        Returns:
            True if successfully tracked
        """
        try:
            # Calculate hours until game
            now = datetime.now(timezone.utc)
            if game_datetime.tzinfo is None:
                game_datetime = game_datetime.replace(tzinfo=timezone.utc)
                
            hours_until_game = (game_datetime - now).total_seconds() / 3600
            
            with self.db_manager.get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO timing_analysis.recommendation_history
                    (game_id, home_team, away_team, game_datetime, recommendation_datetime,
                     hours_until_game, source, book, split_type, strategy_name,
                     recommended_side, odds_at_recommendation, units_wagered)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (game_id, source, COALESCE(book, ''), split_type, strategy_name, recommendation_datetime)
                    DO UPDATE SET
                        odds_at_recommendation = EXCLUDED.odds_at_recommendation,
                        units_wagered = EXCLUDED.units_wagered,
                        updated_at = NOW()
                """, (
                    game_id, home_team, away_team, game_datetime, now,
                    hours_until_game, source.value, book.value if book else None, 
                    split_type.value, strategy_name, recommended_side,
                    odds_at_recommendation, units_wagered
                ))
                
            self.logger.info(
                "Tracked recommendation",
                game_id=game_id,
                hours_until_game=round(hours_until_game, 2),
                strategy=strategy_name,
                source=source.value
            )
            
            return True
            
        except Exception as e:
            self.logger.error(
                "Failed to track recommendation",
                error=str(e),
                game_id=game_id,
                strategy=strategy_name
            )
            return False
    
    async def update_recommendation_outcomes(self) -> int:
        """
        Update outcomes for pending recommendations using game results.
        
        Returns:
            Number of recommendations updated
        """
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute("SELECT timing_analysis.update_recommendation_outcomes()")
                
                # Get count of updated records
                cursor.execute("""
                    SELECT COUNT(*) as updated_count
                    FROM timing_analysis.recommendation_history 
                    WHERE outcome IS NOT NULL 
                      AND updated_at >= NOW() - INTERVAL '1 hour'
                """)
                
                result = cursor.fetchone()
                updated_count = result['updated_count'] if result else 0
                
                self.logger.info("Updated recommendation outcomes", count=updated_count)
                return updated_count
                
        except Exception as e:
            self.logger.error("Failed to update recommendation outcomes", error=str(e))
            return 0
    
    async def get_timing_performance_summary(
        self,
        days_back: int = 30,
        minimum_sample_size: int = 10
    ) -> List[Dict]:
        """
        Get timing performance summary for recent period.
        
        Args:
            days_back: Number of days to look back
            minimum_sample_size: Minimum bets for inclusion
            
        Returns:
            List of performance summaries by timing bucket
        """
        with self.db_manager.get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    timing_bucket,
                    source,
                    split_type,
                    strategy_name,
                    total_bets,
                    win_rate,
                    roi_percentage,
                    confidence_level,
                    performance_grade,
                    recommendation_confidence,
                    avg_odds_movement
                FROM timing_analysis.current_timing_performance
                WHERE total_bets >= %s
                ORDER BY roi_percentage DESC, win_rate DESC
            """, (minimum_sample_size,))
            
            results = cursor.fetchall()
            return [dict(row) for row in results]
    
    def _build_analysis_query(
        self,
        start_date: datetime,
        end_date: datetime,
        source: Optional[DataSource],
        book: Optional[BookType],
        split_type: Optional[SplitType],
        strategy_name: Optional[str],
        bucket_filter: Optional[TimingBucket]
    ) -> Tuple[str, tuple]:
        """Build SQL query for timing analysis."""
        
        base_query = """
            SELECT 
                COUNT(*) as total_bets,
                SUM(CASE WHEN outcome = 'win' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN outcome = 'loss' THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN outcome = 'push' THEN 1 ELSE 0 END) as pushes,
                SUM(units_wagered) as total_units_wagered,
                COALESCE(SUM(actual_profit_loss), 0) as total_profit_loss,
                AVG(odds_at_recommendation) as avg_odds_at_recommendation,
                AVG(closing_odds) as avg_closing_odds
            FROM timing_analysis.recommendation_history
            WHERE recommendation_datetime >= %s 
              AND recommendation_datetime <= %s
              AND outcome IS NOT NULL
        """
        
        params = [start_date, end_date]
        
        if source:
            base_query += " AND source = %s"
            params.append(source.value if hasattr(source, 'value') else source)
        
        if book:
            base_query += " AND book = %s"
            params.append(book.value if hasattr(book, 'value') else book)
            
        if split_type:
            base_query += " AND split_type = %s"
            params.append(split_type.value if hasattr(split_type, 'value') else split_type)
            
        if strategy_name:
            base_query += " AND strategy_name = %s"
            params.append(strategy_name)
            
        if bucket_filter:
            base_query += " AND timing_bucket = %s"
            params.append(bucket_filter.value if hasattr(bucket_filter, 'value') else bucket_filter)
        
        return base_query, tuple(params)
    
    async def _find_best_source(
        self,
        start_date: datetime,
        end_date: datetime,
        book: Optional[BookType],
        split_type: Optional[SplitType],
        strategy_name: Optional[str]
    ) -> Optional[DataSource]:
        """Find best performing data source."""
        with self.db_manager.get_cursor() as cursor:
            query = """
                SELECT source, 
                       AVG(roi_percentage) as avg_roi
                FROM timing_analysis.timing_bucket_performance
                WHERE analysis_start_date >= %s 
                  AND analysis_end_date <= %s
                  AND total_bets >= 10
            """
            params = [start_date.date(), end_date.date()]
            
            if book:
                query += " AND book = %s"
                params.append(book.value if hasattr(book, 'value') else book)
            if split_type:
                query += " AND split_type = %s"
                params.append(split_type.value if hasattr(split_type, 'value') else split_type)
            if strategy_name:
                query += " AND strategy_name = %s"
                params.append(strategy_name)
                
            query += " GROUP BY source ORDER BY avg_roi DESC LIMIT 1"
            
            cursor.execute(query, params)
            result = cursor.fetchone()
            
            if result and result['source']:
                return DataSource(result['source'])
            return None
    
    async def _find_best_strategy(
        self,
        start_date: datetime,
        end_date: datetime,
        source: Optional[DataSource],
        book: Optional[BookType],
        split_type: Optional[SplitType]
    ) -> Optional[str]:
        """Find best performing strategy."""
        with self.db_manager.get_cursor() as cursor:
            query = """
                SELECT strategy_name,
                       AVG(roi_percentage) as avg_roi
                FROM timing_analysis.timing_bucket_performance
                WHERE analysis_start_date >= %s 
                  AND analysis_end_date <= %s
                  AND total_bets >= 10
                  AND strategy_name IS NOT NULL
            """
            params = [start_date.date(), end_date.date()]
            
            if source:
                query += " AND source = %s"
                params.append(source.value if hasattr(source, 'value') else source)
            if book:
                query += " AND book = %s"
                params.append(book.value if hasattr(book, 'value') else book)
            if split_type:
                query += " AND split_type = %s"
                params.append(split_type.value if hasattr(split_type, 'value') else split_type)
                
            query += " GROUP BY strategy_name ORDER BY avg_roi DESC LIMIT 1"
            
            cursor.execute(query, params)
            result = cursor.fetchone()
            
            return result['strategy_name'] if result else None
    
    async def _analyze_trends(
        self,
        start_date: datetime,
        end_date: datetime,
        bucket_analyses: List[TimingBucketAnalysis]
    ) -> Dict:
        """Analyze trends in timing performance."""
        trends = {}
        
        if not bucket_analyses:
            return trends
        
        # Find optimal timing bucket
        best_bucket = max(bucket_analyses, key=lambda x: x.metrics.roi_percentage)
        trends['optimal_timing'] = best_bucket.timing_bucket.value if hasattr(best_bucket.timing_bucket, 'value') else best_bucket.timing_bucket
        trends['optimal_timing_roi'] = float(best_bucket.metrics.roi_percentage)
        trends['optimal_timing_win_rate'] = float(best_bucket.metrics.win_rate)
        
        # Calculate timing degradation/improvement pattern
        bucket_performance = {
            (ba.timing_bucket.value if hasattr(ba.timing_bucket, 'value') else ba.timing_bucket): ba.metrics.roi_percentage 
            for ba in bucket_analyses
        }
        
        # Check if performance decreases with time to game
        bucket_order = ['0-2h', '2-6h', '6-24h', '24h+']
        performance_by_order = [bucket_performance.get(bucket, 0) for bucket in bucket_order if bucket in bucket_performance]
        
        if len(performance_by_order) >= 2:
            is_decreasing = all(performance_by_order[i] >= performance_by_order[i+1] for i in range(len(performance_by_order)-1))
            is_increasing = all(performance_by_order[i] <= performance_by_order[i+1] for i in range(len(performance_by_order)-1))
            
            if is_decreasing:
                trends['timing_pattern'] = 'performance_decreases_with_time'
            elif is_increasing:
                trends['timing_pattern'] = 'performance_increases_with_time'
            else:
                trends['timing_pattern'] = 'mixed_performance'
        
        # Average odds movement
        total_movement = sum(ba.metrics.odds_movement or 0 for ba in bucket_analyses)
        trends['avg_odds_movement'] = total_movement / len(bucket_analyses) if bucket_analyses else 0
        
        return trends
    
    async def _store_analysis_results(self, analysis: ComprehensiveTimingAnalysis) -> None:
        """Store comprehensive analysis results in database."""
        try:
            with self.db_manager.get_cursor() as cursor:
                # Store comprehensive analysis
                cursor.execute("""
                    INSERT INTO timing_analysis.comprehensive_analyses
                    (analysis_name, total_games_analyzed, total_recommendations,
                     analysis_start_date, analysis_end_date,
                     overall_total_bets, overall_wins, overall_losses, overall_pushes,
                     overall_total_units_wagered, overall_total_profit_loss,
                     overall_avg_odds_at_recommendation, overall_avg_closing_odds,
                     best_bucket, best_source, best_strategy, trends)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    analysis.analysis_name,
                    analysis.total_games_analyzed,
                    analysis.total_recommendations,
                    analysis.created_at.date(),
                    analysis.created_at.date(),
                    analysis.overall_metrics.total_bets,
                    analysis.overall_metrics.wins,
                    analysis.overall_metrics.losses,
                    analysis.overall_metrics.pushes,
                    float(analysis.overall_metrics.total_units_wagered),
                    float(analysis.overall_metrics.total_profit_loss),
                    float(analysis.overall_metrics.avg_odds_at_recommendation) if analysis.overall_metrics.avg_odds_at_recommendation else None,
                    float(analysis.overall_metrics.avg_closing_odds) if analysis.overall_metrics.avg_closing_odds else None,
                    analysis.best_bucket.value if hasattr(analysis.best_bucket, 'value') and analysis.best_bucket else (analysis.best_bucket if analysis.best_bucket else None),
                    analysis.best_source.value if hasattr(analysis.best_source, 'value') and analysis.best_source else (analysis.best_source if analysis.best_source else None),
                    analysis.best_strategy,
                    json.dumps(analysis.trends)
                ))
                
                # Store bucket analyses
                for bucket_analysis in analysis.bucket_analyses:
                    cursor.execute("""
                        INSERT INTO timing_analysis.timing_bucket_performance
                        (timing_bucket, source, book, split_type, strategy_name,
                         analysis_start_date, analysis_end_date,
                         total_bets, wins, losses, pushes,
                         total_units_wagered, total_profit_loss,
                         avg_odds_at_recommendation, avg_closing_odds)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (timing_bucket, COALESCE(source, ''), COALESCE(book, ''), 
                                   COALESCE(split_type, ''), COALESCE(strategy_name, ''),
                                   analysis_start_date, analysis_end_date)
                        DO UPDATE SET
                            total_bets = EXCLUDED.total_bets,
                            wins = EXCLUDED.wins,
                            losses = EXCLUDED.losses,
                            pushes = EXCLUDED.pushes,
                            total_units_wagered = EXCLUDED.total_units_wagered,
                            total_profit_loss = EXCLUDED.total_profit_loss,
                            avg_odds_at_recommendation = EXCLUDED.avg_odds_at_recommendation,
                            avg_closing_odds = EXCLUDED.avg_closing_odds,
                            updated_at = NOW()
                    """, (
                        bucket_analysis.timing_bucket.value if hasattr(bucket_analysis.timing_bucket, 'value') else bucket_analysis.timing_bucket,
                        bucket_analysis.source.value if hasattr(bucket_analysis.source, 'value') and bucket_analysis.source else (bucket_analysis.source if bucket_analysis.source else None),
                        bucket_analysis.book.value if hasattr(bucket_analysis.book, 'value') and bucket_analysis.book else (bucket_analysis.book if bucket_analysis.book else None),
                        bucket_analysis.split_type.value if hasattr(bucket_analysis.split_type, 'value') and bucket_analysis.split_type else (bucket_analysis.split_type if bucket_analysis.split_type else None),
                        bucket_analysis.strategy_name,
                        bucket_analysis.analysis_start_date.date(),
                        bucket_analysis.analysis_end_date.date(),
                        bucket_analysis.metrics.total_bets,
                        bucket_analysis.metrics.wins,
                        bucket_analysis.metrics.losses,
                        bucket_analysis.metrics.pushes,
                        float(bucket_analysis.metrics.total_units_wagered),
                        float(bucket_analysis.metrics.total_profit_loss),
                        float(bucket_analysis.metrics.avg_odds_at_recommendation) if bucket_analysis.metrics.avg_odds_at_recommendation else None,
                        float(bucket_analysis.metrics.avg_closing_odds) if bucket_analysis.metrics.avg_closing_odds else None
                    ))
                
        except Exception as e:
            self.logger.error("Failed to store analysis results", error=str(e))
            raise
    
    async def _get_cached_recommendation(self, lookup: RealtimeTimingLookup) -> Optional[TimingRecommendation]:
        """Get cached timing recommendation if available and not expired."""
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM timing_analysis.timing_recommendations_cache
                    WHERE timing_bucket = %s
                      AND COALESCE(source, '') = COALESCE(%s, '')
                      AND COALESCE(book, '') = COALESCE(%s, '')
                      AND split_type = %s
                      AND COALESCE(strategy_name, '') = COALESCE(%s, '')
                      AND expires_at > NOW()
                """, (
                    lookup.timing_bucket.value,
                    lookup.source.value if hasattr(lookup.source, 'value') and lookup.source else (lookup.source if lookup.source else None),
                    lookup.book.value if hasattr(lookup.book, 'value') and lookup.book else (lookup.book if lookup.book else None),
                    lookup.split_type.value if hasattr(lookup.split_type, 'value') else lookup.split_type,
                    lookup.strategy_name
                ))
                
                result = cursor.fetchone()
                if result:
                    historical_metrics = None
                    if result['historical_total_bets']:
                        historical_metrics = TimingPerformanceMetrics(
                            total_bets=result['historical_total_bets'],
                            wins=int(result['historical_win_rate'] * result['historical_total_bets'] / 100) if result['historical_win_rate'] else 0,
                            losses=result['historical_total_bets'] - int(result['historical_win_rate'] * result['historical_total_bets'] / 100) if result['historical_win_rate'] else result['historical_total_bets'],
                            pushes=0,
                            total_units_wagered=Decimal(str(result['historical_total_bets'])),
                            total_profit_loss=Decimal(str(result['historical_roi'] * result['historical_total_bets'] / 100)) if result['historical_roi'] else Decimal('0')
                        )
                    
                    return TimingRecommendation(
                        lookup=lookup,
                        historical_metrics=historical_metrics,
                        recommendation=result['recommendation'],
                        confidence=result['confidence'],
                        expected_win_rate=float(result['expected_win_rate']) if result['expected_win_rate'] else None,
                        expected_roi=float(result['expected_roi']) if result['expected_roi'] else None,
                        risk_factors=result['risk_factors'] or [],
                        sample_size_warning=result['sample_size_warning']
                    )
                    
        except Exception as e:
            self.logger.warning("Failed to get cached recommendation", error=str(e))
        
        return None
    
    async def _get_historical_performance(self, lookup: RealtimeTimingLookup) -> Optional[TimingPerformanceMetrics]:
        """Get historical performance metrics for the lookup parameters."""
        with self.db_manager.get_cursor() as cursor:
            query = """
                SELECT 
                    COUNT(*) as total_bets,
                    SUM(CASE WHEN outcome = 'win' THEN 1 ELSE 0 END) as wins,
                    SUM(CASE WHEN outcome = 'loss' THEN 1 ELSE 0 END) as losses,
                    SUM(CASE WHEN outcome = 'push' THEN 1 ELSE 0 END) as pushes,
                    SUM(units_wagered) as total_units_wagered,
                    COALESCE(SUM(actual_profit_loss), 0) as total_profit_loss,
                    AVG(odds_at_recommendation) as avg_odds_at_recommendation,
                    AVG(closing_odds) as avg_closing_odds
                FROM timing_analysis.recommendation_history
                WHERE timing_bucket = %s
                  AND split_type = %s
                  AND outcome IS NOT NULL
                  AND recommendation_datetime >= NOW() - INTERVAL '90 days'
            """
            
            params = [
                lookup.timing_bucket.value,
                lookup.split_type.value if hasattr(lookup.split_type, 'value') else lookup.split_type
            ]
            
            if lookup.source:
                query += " AND source = %s"
                params.append(lookup.source.value if hasattr(lookup.source, 'value') else lookup.source)
            
            if lookup.book:
                query += " AND book = %s"
                params.append(lookup.book.value if hasattr(lookup.book, 'value') else lookup.book)
                
            if lookup.strategy_name:
                query += " AND strategy_name = %s"
                params.append(lookup.strategy_name)
            
            cursor.execute(query, params)
            result = cursor.fetchone()
            
            if result and result['total_bets'] > 0:
                return TimingPerformanceMetrics(
                    total_bets=result['total_bets'],
                    wins=result['wins'],
                    losses=result['losses'],
                    pushes=result['pushes'],
                    total_units_wagered=Decimal(str(result['total_units_wagered'])),
                    total_profit_loss=Decimal(str(result['total_profit_loss'])),
                    avg_odds_at_recommendation=Decimal(str(result['avg_odds_at_recommendation'])) if result['avg_odds_at_recommendation'] else None,
                    avg_closing_odds=Decimal(str(result['avg_closing_odds'])) if result['avg_closing_odds'] else None
                )
        
        return None
    
    def _generate_recommendation(
        self, 
        lookup: RealtimeTimingLookup, 
        historical_metrics: Optional[TimingPerformanceMetrics]
    ) -> TimingRecommendation:
        """Generate timing recommendation based on historical performance."""
        
        if not historical_metrics or historical_metrics.total_bets < 10:
            return TimingRecommendation(
                lookup=lookup,
                historical_metrics=historical_metrics,
                recommendation="Insufficient historical data for this timing and context. Wait for more data or use general timing guidelines.",
                confidence="INSUFFICIENT_DATA",
                sample_size_warning=True,
                risk_factors=["Limited sample size", "No historical precedent"]
            )
        
        # Analyze performance
        win_rate = historical_metrics.win_rate
        roi = historical_metrics.roi_percentage
        confidence_level = historical_metrics.confidence_level
        
        risk_factors = []
        
        # Check sample size adequacy
        sample_size_warning = confidence_level == ConfidenceLevel.LOW
        if sample_size_warning:
            risk_factors.append(f"Small sample size ({historical_metrics.total_bets} bets)")
        
        # Check for negative trends
        if roi < 0:
            risk_factors.append("Negative historical ROI")
        if win_rate < 50:
            risk_factors.append("Below 50% historical win rate")
        
        # Check odds movement
        if historical_metrics.odds_movement:
            if historical_metrics.odds_movement < -5:
                risk_factors.append("Historically unfavorable odds movement")
            elif historical_metrics.odds_movement > 5:
                risk_factors.append("Odds typically move in your favor")
        
        # Generate recommendation text and confidence
        if win_rate >= 60 and roi >= 10 and confidence_level in [ConfidenceLevel.HIGH, ConfidenceLevel.VERY_HIGH]:
            recommendation = f"STRONG RECOMMENDATION: This timing has excellent historical performance ({win_rate:.1f}% WR, {roi:.1f}% ROI). Place bet immediately."
            confidence = "HIGH_CONFIDENCE"
        elif win_rate >= 55 and roi >= 5 and confidence_level != ConfidenceLevel.LOW:
            recommendation = f"GOOD TIMING: Historical performance is solid ({win_rate:.1f}% WR, {roi:.1f}% ROI). Consider placing bet."
            confidence = "MODERATE_CONFIDENCE"
        elif win_rate >= 52 and roi >= 0:
            recommendation = f"ACCEPTABLE TIMING: Marginally profitable historically ({win_rate:.1f}% WR, {roi:.1f}% ROI). Proceed with caution."
            confidence = "MODERATE_CONFIDENCE"
        elif win_rate >= 50:
            recommendation = f"NEUTRAL TIMING: Break-even performance historically ({win_rate:.1f}% WR, {roi:.1f}% ROI). Not recommended."
            confidence = "LOW_CONFIDENCE"
        else:
            recommendation = f"POOR TIMING: Historically unprofitable ({win_rate:.1f}% WR, {roi:.1f}% ROI). Avoid betting at this timing."
            confidence = "LOW_CONFIDENCE"
        
        # Add timing-specific insights
        timing_insights = {
            TimingBucket.ZERO_TO_TWO_HOURS: "Last-minute betting - odds are likely close to efficient",
            TimingBucket.TWO_TO_SIX_HOURS: "Short-term betting - some line movement possible",
            TimingBucket.SIX_TO_TWENTY_FOUR_HOURS: "Medium-term betting - moderate line movement risk",
            TimingBucket.TWENTY_FOUR_PLUS_HOURS: "Early betting - higher line movement risk but potential for better odds"
        }
        
        timing_note = timing_insights.get(lookup.timing_bucket, "")
        if timing_note:
            recommendation += f" {timing_note}."
        
        return TimingRecommendation(
            lookup=lookup,
            historical_metrics=historical_metrics,
            recommendation=recommendation,
            confidence=confidence,
            expected_win_rate=win_rate,
            expected_roi=roi,
            risk_factors=risk_factors,
            sample_size_warning=sample_size_warning
        )
    
    async def _cache_recommendation(self, lookup: RealtimeTimingLookup, recommendation: TimingRecommendation) -> None:
        """Cache timing recommendation for future use."""
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO timing_analysis.timing_recommendations_cache
                    (timing_bucket, source, book, split_type, strategy_name,
                     recommendation, confidence, expected_win_rate, expected_roi,
                     risk_factors, sample_size_warning,
                     historical_total_bets, historical_win_rate, historical_roi)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (timing_bucket, COALESCE(source, ''), COALESCE(book, ''), 
                               split_type, COALESCE(strategy_name, ''))
                    DO UPDATE SET
                        recommendation = EXCLUDED.recommendation,
                        confidence = EXCLUDED.confidence,
                        expected_win_rate = EXCLUDED.expected_win_rate,
                        expected_roi = EXCLUDED.expected_roi,
                        risk_factors = EXCLUDED.risk_factors,
                        sample_size_warning = EXCLUDED.sample_size_warning,
                        historical_total_bets = EXCLUDED.historical_total_bets,
                        historical_win_rate = EXCLUDED.historical_win_rate,
                        historical_roi = EXCLUDED.historical_roi,
                        expires_at = NOW() + INTERVAL '1 hour',
                        created_at = NOW()
                """, (
                    lookup.timing_bucket.value,
                    lookup.source.value if hasattr(lookup.source, 'value') and lookup.source else (lookup.source if lookup.source else None),
                    lookup.book.value if hasattr(lookup.book, 'value') and lookup.book else (lookup.book if lookup.book else None),
                    lookup.split_type.value if hasattr(lookup.split_type, 'value') else lookup.split_type,
                    lookup.strategy_name,
                    recommendation.recommendation,
                    recommendation.confidence,
                    recommendation.expected_win_rate,
                    recommendation.expected_roi,
                    json.dumps(recommendation.risk_factors),
                    recommendation.sample_size_warning,
                    recommendation.historical_metrics.total_bets if recommendation.historical_metrics else None,
                    recommendation.expected_win_rate,
                    recommendation.expected_roi
                                    ))
                
        except Exception as e:
            self.logger.warning("Failed to cache recommendation", error=str(e)) 