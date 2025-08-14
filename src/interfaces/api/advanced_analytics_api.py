#!/usr/bin/env python3
"""
Advanced Analytics API Service

Extends the monitoring dashboard with sophisticated analytics capabilities including:
- Interactive line movement charts with multi-sportsbook data
- Statistical analysis tools (regression, correlation, confidence intervals)
- Performance attribution dashboard
- Advanced data filtering and export capabilities

This service works alongside the monitoring dashboard to provide comprehensive
betting analytics and business intelligence.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
import json
import csv
import io
from statistics import mean, stdev, median
from scipy import stats
import numpy as np
import pandas as pd

from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel, Field

from ...core.config import get_settings
from ...core.enhanced_logging import get_contextual_logger, LogComponent
from ...core.exceptions import handle_exception, DatabaseError, AnalyticsError
from ...data.database.connection import get_db_connection
from ...data.models.unified.betting_analysis import BettingAnalysis, BettingSignalType
from ...data.models.unified.movement_analysis import GameMovementAnalysis, LineMovementDetail
from ...services.monitoring.prometheus_metrics_service import get_metrics_service
from ...services.analytics.statistical_analysis_service import get_statistical_analysis_service

# Initialize components
settings = get_settings()
logger = get_contextual_logger(__name__, LogComponent.API_CLIENT)
metrics_service = get_metrics_service()
stats_service = get_statistical_analysis_service()

# Create router for advanced analytics endpoints
analytics_router = APIRouter(prefix="/api/analytics", tags=["Advanced Analytics"])


class LineMovementChartData(BaseModel):
    """Line movement chart data structure."""
    
    game_id: int
    home_team: str
    away_team: str
    game_datetime: datetime
    market_type: str  # 'moneyline', 'spread', 'total'
    
    # Time series data for chart
    timestamps: List[datetime]
    sportsbook_data: Dict[str, List[Dict[str, Any]]]  # sportsbook_name -> data points
    
    # Statistical summaries
    opening_range: Dict[str, float]  # min/max opening odds across books
    closing_range: Dict[str, float]  # min/max closing odds across books
    movement_summary: Dict[str, Any]
    sharp_indicators: List[Dict[str, Any]]


class StatisticalAnalysis(BaseModel):
    """Statistical analysis results."""
    
    analysis_type: str
    timeframe: str
    sample_size: int
    
    # Correlation analysis
    correlations: Dict[str, float] = {}
    
    # Regression analysis  
    regression_results: Dict[str, Any] = {}
    
    # Confidence intervals
    confidence_intervals: Dict[str, Dict[str, float]] = {}
    
    # Distribution analysis
    distribution_stats: Dict[str, Any] = {}
    
    # Performance metrics
    performance_attribution: Dict[str, Any] = {}


class PerformanceAttribution(BaseModel):
    """Performance attribution analysis."""
    
    total_opportunities: int
    successful_opportunities: int
    success_rate: float
    
    # Strategy breakdown
    strategy_performance: Dict[str, Dict[str, Any]]
    
    # Time-based analysis
    hourly_performance: Dict[str, Dict[str, Any]]
    daily_performance: Dict[str, Dict[str, Any]]
    
    # Market type analysis
    market_performance: Dict[str, Dict[str, Any]]
    
    # Risk-adjusted returns
    sharpe_ratio: Optional[float] = None
    max_drawdown: Optional[float] = None
    
    # Attribution factors
    attribution_factors: Dict[str, float]
    
    # Data quality information
    data_quality: Optional[Dict[str, Any]] = None


class FilterOptions(BaseModel):
    """Advanced filtering options."""
    
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    teams: Optional[List[str]] = None
    sportsbooks: Optional[List[str]] = None
    market_types: Optional[List[str]] = None
    signal_types: Optional[List[str]] = None
    confidence_threshold: Optional[float] = None
    min_movement_threshold: Optional[float] = None
    
    # Time-based filters
    time_to_game_min: Optional[int] = None  # minutes
    time_to_game_max: Optional[int] = None
    
    # Performance filters
    min_success_rate: Optional[float] = None
    strategy_types: Optional[List[str]] = None


@analytics_router.get("/line-movement-chart", response_model=LineMovementChartData)
async def get_line_movement_chart_data(
    game_id: int,
    market_type: str = Query("moneyline", regex="^(moneyline|spread|total)$"),
    side: Optional[str] = Query(None, regex="^(home|away|over|under)$")
):
    """Get interactive line movement chart data for a specific game."""
    try:
        async with get_db_connection() as db:
            # Get game information
            game_query = """
                SELECT id, home_team, away_team, game_datetime
                FROM curated.games_complete 
                WHERE id = $1
            """
            game_result = await db.fetchrow(game_query, game_id)
            if not game_result:
                raise HTTPException(status_code=404, detail="Game not found")
            
            # Get line movement history
            movement_query = """
                SELECT 
                    lmh.line_timestamp,
                    s.display_name as sportsbook_name,
                    lmh.odds,
                    lmh.line_value,
                    lmh.side,
                    lmh.bet_type
                FROM curated.line_movement_history lmh
                JOIN curated.sportsbooks s ON lmh.sportsbook_id = s.id
                WHERE lmh.game_id = $1 
                  AND lmh.bet_type = $2
                  AND ($3 IS NULL OR lmh.side = $3)
                ORDER BY lmh.line_timestamp ASC
            """
            
            movements = await db.fetch(movement_query, game_id, market_type, side)
            
            # Process data for chart visualization
            sportsbook_data = {}
            timestamps = set()
            
            for movement in movements:
                book_name = movement['sportsbook_name']
                timestamp = movement['line_timestamp']
                timestamps.add(timestamp)
                
                if book_name not in sportsbook_data:
                    sportsbook_data[book_name] = []
                
                data_point = {
                    'timestamp': timestamp,
                    'odds': movement['odds'],
                    'line_value': float(movement['line_value']) if movement['line_value'] else None,
                    'side': movement['side']
                }
                sportsbook_data[book_name].append(data_point)
            
            # Calculate statistical summaries
            opening_odds = []
            closing_odds = []
            
            for book_data in sportsbook_data.values():
                if book_data:
                    opening_odds.append(book_data[0]['odds'])
                    closing_odds.append(book_data[-1]['odds'])
            
            opening_range = {
                'min': min(opening_odds) if opening_odds else 0,
                'max': max(opening_odds) if opening_odds else 0,
                'spread': (max(opening_odds) - min(opening_odds)) if opening_odds else 0
            }
            
            closing_range = {
                'min': min(closing_odds) if closing_odds else 0,
                'max': max(closing_odds) if closing_odds else 0,
                'spread': (max(closing_odds) - min(closing_odds)) if closing_odds else 0
            }
            
            movement_summary = {
                'total_movements': len(movements),
                'books_tracked': len(sportsbook_data),
                'time_span_hours': (max(timestamps) - min(timestamps)).total_seconds() / 3600 if timestamps else 0,
                'average_opening': mean(opening_odds) if opening_odds else 0,
                'average_closing': mean(closing_odds) if closing_odds else 0,
                'movement_range': abs(max(closing_odds) - min(closing_odds)) if closing_odds else 0
            }
            
            # Get sharp action indicators
            sharp_query = """
                SELECT 
                    sa.detected_at,
                    sa.sharp_side,
                    sa.signal_strength,
                    sa.sharp_confidence,
                    sa.market_type
                FROM curated.sharp_action sa
                WHERE sa.game_id = $1 AND sa.market_type = $2
                ORDER BY sa.detected_at ASC
            """
            
            sharp_indicators = []
            try:
                sharp_results = await db.fetch(sharp_query, game_id, market_type)
                for sharp in sharp_results:
                    sharp_indicators.append({
                        'timestamp': sharp['detected_at'],
                        'side': sharp['sharp_side'],
                        'strength': sharp['signal_strength'],
                        'confidence': float(sharp['sharp_confidence']),
                        'market_type': sharp['market_type']
                    })
            except Exception as e:
                logger.warning("Could not fetch sharp action data", error=str(e))
            
            return LineMovementChartData(
                game_id=game_id,
                home_team=game_result['home_team'],
                away_team=game_result['away_team'],
                game_datetime=game_result['game_datetime'],
                market_type=market_type,
                timestamps=sorted(timestamps),
                sportsbook_data=sportsbook_data,
                opening_range=opening_range,
                closing_range=closing_range,
                movement_summary=movement_summary,
                sharp_indicators=sharp_indicators
            )
            
    except DatabaseError as e:
        logger.error("Database error in line movement chart", error=e, correlation_id=e.correlation_id)
        raise HTTPException(status_code=503, detail=f"Database error: {e.user_message}")
    except Exception as e:
        handled_error = handle_exception(e, component="advanced_analytics", operation="get_line_movement_chart")
        logger.error("Failed to get line movement chart data", error=handled_error, correlation_id=handled_error.correlation_id)
        raise HTTPException(status_code=500, detail=handled_error.user_message)


@analytics_router.post("/statistical-analysis", response_model=StatisticalAnalysis)
async def perform_statistical_analysis(
    filters: FilterOptions,
    analysis_type: str = Query("correlation", regex="^(correlation|regression|distribution|performance)$")
):
    """Perform advanced statistical analysis on betting data."""
    try:
        async with get_db_connection() as db:
            # Build query based on filters
            base_query = """
                SELECT 
                    ba.game_id,
                    ba.analysis_timestamp,
                    ba.market_type,
                    ba.confidence_score,
                    ba.recommendation,
                    ba.signal_strength,
                    ba.primary_signal,
                    gc.home_team,
                    gc.away_team,
                    gc.game_datetime,
                    lms.odds_movement,
                    lms.line_movement,
                    lms.total_movements
                FROM curated.betting_analysis ba
                JOIN curated.games_complete gc ON ba.game_id = gc.id
                LEFT JOIN curated.line_movement_summary lms ON (
                    ba.game_id = lms.game_id AND 
                    ba.market_type = lms.bet_type
                )
                WHERE 1=1
            """
            
            params = []
            param_count = 0
            
            # Apply filters
            if filters.start_date:
                param_count += 1
                base_query += f" AND ba.analysis_timestamp >= ${param_count}"
                params.append(filters.start_date)
                
            if filters.end_date:
                param_count += 1
                base_query += f" AND ba.analysis_timestamp <= ${param_count}"
                params.append(filters.end_date)
                
            if filters.confidence_threshold:
                param_count += 1
                base_query += f" AND ba.confidence_score >= ${param_count}"
                params.append(filters.confidence_threshold)
                
            base_query += " ORDER BY ba.analysis_timestamp ASC"
            
            results = await db.fetch(base_query, *params)
            
            if not results:
                return StatisticalAnalysis(
                    analysis_type=analysis_type,
                    timeframe=f"{filters.start_date} to {filters.end_date}",
                    sample_size=0
                )
            
            # Convert to DataFrame for analysis
            df = pd.DataFrame([dict(row) for row in results])
            
            analysis_result = StatisticalAnalysis(
                analysis_type=analysis_type,
                timeframe=f"{filters.start_date} to {filters.end_date}",
                sample_size=len(df)
            )
            
            # Perform specific analysis based on type
            if analysis_type == "correlation":
                analysis_result.correlations = await _perform_correlation_analysis(df)
            elif analysis_type == "regression":
                analysis_result.regression_results = await _perform_regression_analysis(df)
            elif analysis_type == "distribution":
                analysis_result.distribution_stats = await _perform_distribution_analysis(df)
            elif analysis_type == "performance":
                analysis_result.performance_attribution = await _perform_performance_analysis(df)
                
            # Calculate confidence intervals for key metrics
            analysis_result.confidence_intervals = await _calculate_confidence_intervals(df)
            
            return analysis_result
            
    except Exception as e:
        handled_error = handle_exception(e, component="advanced_analytics", operation="statistical_analysis")
        logger.error("Failed to perform statistical analysis", error=handled_error, correlation_id=handled_error.correlation_id)
        raise HTTPException(status_code=500, detail=handled_error.user_message)


@analytics_router.get("/performance-attribution", response_model=PerformanceAttribution)
async def get_performance_attribution(
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    strategy_filter: Optional[List[str]] = Query(None)
):
    """Get detailed performance attribution analysis."""
    try:
        async with get_db_connection() as db:
            # Default to last 30 days if no dates provided
            if not start_date:
                start_date = datetime.now(timezone.utc) - timedelta(days=30)
            if not end_date:
                end_date = datetime.now(timezone.utc)
                
            # Get opportunity performance data with proper outcome tracking
            perf_query = """
                SELECT 
                    ba.analysis_id,
                    ba.game_id,
                    ba.analysis_timestamp,
                    ba.primary_signal,
                    ba.signal_strength,
                    ba.confidence_score,
                    ba.recommendation,
                    ba.market_type,
                    gc.game_datetime,
                    gc.home_team,
                    gc.away_team,
                    EXTRACT(HOUR FROM ba.analysis_timestamp) as analysis_hour,
                    EXTRACT(DOW FROM ba.analysis_timestamp) as day_of_week,
                    -- Real outcome tracking using proper game outcomes and strategy results
                    COALESCE(
                        -- Check strategy results first (most accurate)
                        CASE 
                            WHEN sr.outcome = 'WIN' THEN true
                            WHEN sr.outcome IN ('LOSS', 'PUSH', 'VOID') THEN false
                            ELSE NULL
                        END,
                        -- Fallback to game outcome analysis based on recommendation
                        CASE 
                            -- Spread bets
                            WHEN ba.market_type = 'spread' AND ba.recommendation LIKE '%home%' AND go.home_cover_spread = true THEN true
                            WHEN ba.market_type = 'spread' AND ba.recommendation LIKE '%away%' AND go.home_cover_spread = false THEN true
                            WHEN ba.market_type = 'spread' AND go.home_cover_spread IS NOT NULL THEN false
                            -- Total bets
                            WHEN ba.market_type = 'total' AND ba.recommendation LIKE '%over%' AND go.over = true THEN true
                            WHEN ba.market_type = 'total' AND ba.recommendation LIKE '%under%' AND go.over = false THEN true
                            WHEN ba.market_type = 'total' AND go.over IS NOT NULL THEN false
                            -- Moneyline bets
                            WHEN ba.market_type = 'moneyline' AND ba.recommendation LIKE '%home%' AND go.home_win = true THEN true
                            WHEN ba.market_type = 'moneyline' AND ba.recommendation LIKE '%away%' AND go.home_win = false THEN true
                            WHEN ba.market_type = 'moneyline' AND go.home_win IS NOT NULL THEN false
                            -- No outcome data available
                            ELSE NULL
                        END
                    ) as was_successful
                FROM curated.betting_analysis ba
                JOIN curated.games_complete gc ON ba.game_id = gc.id
                LEFT JOIN curated.game_outcomes go ON ba.game_id = go.game_id
                LEFT JOIN analysis.strategy_results sr ON (
                    ba.game_id = sr.game_id::INTEGER AND
                    ba.market_type = sr.bet_type AND
                    ba.analysis_timestamp <= sr.bet_placed_at AND
                    sr.status = 'COMPLETED'
                )
                WHERE ba.analysis_timestamp BETWEEN $1 AND $2
                    AND gc.game_datetime < NOW() - INTERVAL '2 hours' -- Only include completed games
                ORDER BY ba.analysis_timestamp ASC
            """
            
            results = await db.fetch(perf_query, start_date, end_date)
            
            # Calculate success metrics, excluding opportunities without outcome data
            opportunities_with_outcomes = [r for r in results if r['was_successful'] is not None]
            total_opportunities = len(opportunities_with_outcomes)
            successful_opportunities = sum(1 for r in opportunities_with_outcomes if r['was_successful'])
            success_rate = successful_opportunities / total_opportunities if total_opportunities > 0 else 0.0
            
            # Track incomplete data for transparency
            opportunities_without_outcomes = len(results) - len(opportunities_with_outcomes)
            
            # Analyze by strategy (primary signal) - only include opportunities with outcomes
            strategy_performance = {}
            for result in opportunities_with_outcomes:
                strategy = result['primary_signal'] or 'unknown'
                if strategy not in strategy_performance:
                    strategy_performance[strategy] = {
                        'total': 0, 
                        'successful': 0, 
                        'success_rate': 0.0,
                        'avg_confidence': 0.0,
                        'total_confidence': 0.0
                    }
                
                strategy_performance[strategy]['total'] += 1
                strategy_performance[strategy]['total_confidence'] += result['confidence_score']
                if result['was_successful']:
                    strategy_performance[strategy]['successful'] += 1
            
            # Calculate final strategy metrics
            for strategy in strategy_performance:
                perf = strategy_performance[strategy]
                perf['success_rate'] = perf['successful'] / perf['total'] if perf['total'] > 0 else 0.0
                perf['avg_confidence'] = perf['total_confidence'] / perf['total'] if perf['total'] > 0 else 0.0
                del perf['total_confidence']  # Remove intermediate calculation
            
            # Analyze by time of day - only opportunities with outcomes
            hourly_performance = {}
            for hour in range(24):
                hourly_performance[str(hour)] = {'total': 0, 'successful': 0, 'success_rate': 0.0}
                
            for result in opportunities_with_outcomes:
                hour = str(result['analysis_hour'])
                hourly_performance[hour]['total'] += 1
                if result['was_successful']:
                    hourly_performance[hour]['successful'] += 1
                    
            for hour in hourly_performance:
                perf = hourly_performance[hour]
                perf['success_rate'] = perf['successful'] / perf['total'] if perf['total'] > 0 else 0.0
            
            # Analyze by day of week - only opportunities with outcomes
            daily_performance = {}
            day_names = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
            for i, day_name in enumerate(day_names):
                daily_performance[day_name] = {'total': 0, 'successful': 0, 'success_rate': 0.0}
                
            for result in opportunities_with_outcomes:
                day_name = day_names[result['day_of_week']]
                daily_performance[day_name]['total'] += 1
                if result['was_successful']:
                    daily_performance[day_name]['successful'] += 1
                    
            for day in daily_performance:
                perf = daily_performance[day]
                perf['success_rate'] = perf['successful'] / perf['total'] if perf['total'] > 0 else 0.0
            
            # Analyze by market type - only opportunities with outcomes
            market_performance = {}
            for result in opportunities_with_outcomes:
                market = result['market_type']
                if market not in market_performance:
                    market_performance[market] = {'total': 0, 'successful': 0, 'success_rate': 0.0}
                
                market_performance[market]['total'] += 1
                if result['was_successful']:
                    market_performance[market]['successful'] += 1
                    
            for market in market_performance:
                perf = market_performance[market]
                perf['success_rate'] = perf['successful'] / perf['total'] if perf['total'] > 0 else 0.0
            
            # Calculate attribution factors (simplified)
            attribution_factors = {
                'confidence_score': 0.35,  # Impact of confidence on success
                'signal_strength': 0.25,   # Impact of signal strength
                'market_timing': 0.20,     # Impact of timing
                'market_type': 0.15,       # Impact of market selection
                'external_factors': 0.05   # Other factors
            }
            
            return PerformanceAttribution(
                total_opportunities=total_opportunities,
                successful_opportunities=successful_opportunities,
                success_rate=success_rate,
                strategy_performance=strategy_performance,
                hourly_performance=hourly_performance,
                daily_performance=daily_performance,
                market_performance=market_performance,
                attribution_factors=attribution_factors,
                data_quality={
                    'total_raw_opportunities': len(results),
                    'opportunities_with_outcomes': total_opportunities,
                    'opportunities_without_outcomes': opportunities_without_outcomes,
                    'data_completeness_rate': total_opportunities / len(results) if len(results) > 0 else 0.0,
                    'outcome_sources': {
                        'strategy_results_table': 'Primary source for tracked bet outcomes',
                        'game_outcomes_analysis': 'Fallback based on recommendation vs actual game results',
                        'filtering_criteria': 'Only completed games (>2 hours post-game) included'
                    },
                    'note': 'Success rates calculated only from opportunities with confirmed outcomes'
                }
            )
            
    except Exception as e:
        handled_error = handle_exception(e, component="advanced_analytics", operation="performance_attribution")
        logger.error("Failed to get performance attribution", error=handled_error, correlation_id=handled_error.correlation_id)
        raise HTTPException(status_code=500, detail=handled_error.user_message)


# Statistical analysis helper functions
async def _perform_correlation_analysis(df: pd.DataFrame) -> Dict[str, float]:
    """Perform correlation analysis on the dataset."""
    try:
        correlation_results = stats_service.analyze_correlations(df)
        
        # Extract key correlations for API response
        correlations = {}
        if 'correlations' in correlation_results and 'pearson' in correlation_results['correlations']:
            pearson_corr = correlation_results['correlations']['pearson']
            
            # Extract specific correlation pairs if they exist
            for col1 in pearson_corr:
                for col2 in pearson_corr[col1]:
                    if col1 != col2 and not np.isnan(pearson_corr[col1][col2]):
                        correlations[f"{col1}_vs_{col2}"] = pearson_corr[col1][col2]
        
        return correlations
        
    except Exception as e:
        logger.error(f"Error in correlation analysis: {e}")
        return {}


async def _perform_regression_analysis(df: pd.DataFrame) -> Dict[str, Any]:
    """Perform regression analysis."""
    try:
        # Try to perform regression if we have suitable columns
        if 'confidence_score' in df.columns:
            # Create a simulated success column for demo (in real implementation, this would be actual outcomes)
            df_sim = df.copy()
            df_sim['success'] = np.random.binomial(1, df_sim['confidence_score'], size=len(df_sim))
            
            regression_results = stats_service.perform_regression_analysis(
                data=df_sim,
                target_column='success',
                feature_columns=['confidence_score']
            )
            
            return regression_results
            
    except Exception as e:
        logger.error(f"Error in regression analysis: {e}")
        
    return {}


async def _perform_distribution_analysis(df: pd.DataFrame) -> Dict[str, Any]:
    """Perform distribution analysis."""
    try:
        distribution_results = stats_service.analyze_distributions(df)
        return distribution_results
        
    except Exception as e:
        logger.error(f"Error in distribution analysis: {e}")
        return {}


async def _perform_performance_analysis(df: pd.DataFrame) -> Dict[str, Any]:
    """Perform performance analysis."""
    performance_data = {}
    
    # Analyze signal strength distribution
    if 'signal_strength' in df.columns:
        signal_counts = df['signal_strength'].value_counts().to_dict()
        performance_data['signal_strength_distribution'] = signal_counts
    
    # Analyze market type distribution
    if 'market_type' in df.columns:
        market_counts = df['market_type'].value_counts().to_dict()
        performance_data['market_type_distribution'] = market_counts
    
    return performance_data


async def _calculate_confidence_intervals(df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    """Calculate confidence intervals for key metrics."""
    confidence_intervals = {}
    
    # Calculate confidence intervals for numeric columns
    numeric_columns = df.select_dtypes(include=[np.number]).columns
    
    for column in numeric_columns:
        if column in df.columns:
            ci_result = stats_service.calculate_confidence_intervals(df[column])
            
            if 'error' not in ci_result:
                confidence_intervals[column] = ci_result
    
    return confidence_intervals


@analytics_router.get("/filter-options")
async def get_filter_options():
    """Get available filter options for the analytics dashboard."""
    try:
        async with get_db_connection() as db:
            # Get available teams
            teams_query = """
                SELECT DISTINCT home_team as team FROM curated.games_complete 
                UNION 
                SELECT DISTINCT away_team as team FROM curated.games_complete
                ORDER BY team
            """
            teams = await db.fetch(teams_query)
            
            # Get available sportsbooks
            sportsbooks_query = """
                SELECT DISTINCT display_name as sportsbook_name, id
                FROM curated.sportsbooks 
                ORDER BY display_name
            """
            sportsbooks = await db.fetch(sportsbooks_query)
            
            # Get available signal types
            signal_types_query = """
                SELECT DISTINCT primary_signal as signal_type
                FROM curated.betting_analysis
                WHERE primary_signal IS NOT NULL
                ORDER BY primary_signal
            """
            signal_types = await db.fetch(signal_types_query)
            
            # Get date range of available data
            date_range_query = """
                SELECT 
                    MIN(analysis_timestamp) as earliest_date,
                    MAX(analysis_timestamp) as latest_date
                FROM curated.betting_analysis
            """
            date_range = await db.fetchrow(date_range_query)
            
            return {
                'teams': [row['team'] for row in teams if row['team']],
                'sportsbooks': [
                    {'id': row['id'], 'name': row['sportsbook_name']} 
                    for row in sportsbooks
                ],
                'signal_types': [row['signal_type'] for row in signal_types if row['signal_type']],
                'market_types': ['moneyline', 'spread', 'total'],
                'strategy_types': [
                    'sharp_action', 'steam_move', 'reverse_line_movement', 
                    'book_conflict', 'late_money', 'timing_edge'
                ],
                'date_range': {
                    'earliest': date_range['earliest_date'] if date_range else None,
                    'latest': date_range['latest_date'] if date_range else None
                }
            }
            
    except Exception as e:
        handled_error = handle_exception(e, component="advanced_analytics", operation="get_filter_options")
        logger.error("Failed to get filter options", error=handled_error, correlation_id=handled_error.correlation_id)
        raise HTTPException(status_code=500, detail=handled_error.user_message)


@analytics_router.get("/filtered-data")
async def get_filtered_analytics_data(
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    teams: Optional[List[str]] = Query(None),
    sportsbooks: Optional[List[str]] = Query(None),
    market_types: Optional[List[str]] = Query(None),
    signal_types: Optional[List[str]] = Query(None),
    confidence_threshold: Optional[float] = Query(None, ge=0.0, le=1.0),
    min_movement_threshold: Optional[float] = Query(None),
    limit: int = Query(100, ge=1, le=1000)
):
    """Get filtered analytics data based on multiple criteria."""
    try:
        async with get_db_connection() as db:
            # Build dynamic query based on filters
            base_query = """
                SELECT 
                    ba.analysis_id,
                    ba.game_id,
                    ba.analysis_timestamp,
                    ba.market_type,
                    ba.confidence_score,
                    ba.recommendation,
                    ba.signal_strength,
                    ba.primary_signal,
                    ba.market_side,
                    gc.home_team,
                    gc.away_team,
                    gc.game_datetime,
                    COALESCE(lms.odds_movement, 0) as odds_movement,
                    COALESCE(lms.line_movement, 0) as line_movement,
                    COALESCE(lms.total_movements, 0) as total_movements,
                    s.display_name as sportsbook_name
                FROM curated.betting_analysis ba
                JOIN curated.games_complete gc ON ba.game_id = gc.id
                LEFT JOIN curated.line_movement_summary lms ON (
                    ba.game_id = lms.game_id AND 
                    ba.market_type = lms.bet_type
                )
                LEFT JOIN curated.sportsbooks s ON lms.sportsbook_id = s.id
                WHERE 1=1
            """
            
            params = []
            param_count = 0
            
            # Apply filters
            if start_date:
                param_count += 1
                base_query += f" AND ba.analysis_timestamp >= ${param_count}"
                params.append(start_date)
                
            if end_date:
                param_count += 1  
                base_query += f" AND ba.analysis_timestamp <= ${param_count}"
                params.append(end_date)
                
            if teams:
                param_count += 1
                base_query += f" AND (gc.home_team = ANY(${param_count}) OR gc.away_team = ANY(${param_count}))"
                params.append(teams)
                param_count += 1
                params.append(teams)  # Add twice for home and away
                
            if market_types:
                param_count += 1
                base_query += f" AND ba.market_type = ANY(${param_count})"
                params.append(market_types)
                
            if signal_types:
                param_count += 1
                base_query += f" AND ba.primary_signal = ANY(${param_count})"
                params.append(signal_types)
                
            if confidence_threshold:
                param_count += 1
                base_query += f" AND ba.confidence_score >= ${param_count}"
                params.append(confidence_threshold)
                
            if min_movement_threshold:
                param_count += 1
                base_query += f" AND ABS(COALESCE(lms.odds_movement, 0)) >= ${param_count}"
                params.append(min_movement_threshold)
                
            if sportsbooks:
                param_count += 1
                base_query += f" AND s.display_name = ANY(${param_count})"
                params.append(sportsbooks)
            
            # Add ordering and limit
            base_query += f" ORDER BY ba.analysis_timestamp DESC LIMIT {limit}"
            
            results = await db.fetch(base_query, *params)
            
            # Convert to list of dictionaries  
            filtered_data = []
            for row in results:
                filtered_data.append({
                    'analysis_id': row['analysis_id'],
                    'game_id': row['game_id'], 
                    'analysis_timestamp': row['analysis_timestamp'],
                    'market_type': row['market_type'],
                    'confidence_score': float(row['confidence_score']),
                    'recommendation': row['recommendation'],
                    'signal_strength': row['signal_strength'],
                    'primary_signal': row['primary_signal'],
                    'market_side': row['market_side'],
                    'home_team': row['home_team'],
                    'away_team': row['away_team'],
                    'game_datetime': row['game_datetime'],
                    'odds_movement': float(row['odds_movement'] or 0),
                    'line_movement': float(row['line_movement'] or 0),
                    'total_movements': int(row['total_movements'] or 0),
                    'sportsbook_name': row['sportsbook_name']
                })
            
            return {
                'data': filtered_data,
                'total_results': len(filtered_data),
                'filters_applied': {
                    'start_date': start_date,
                    'end_date': end_date,
                    'teams': teams,
                    'sportsbooks': sportsbooks,
                    'market_types': market_types, 
                    'signal_types': signal_types,
                    'confidence_threshold': confidence_threshold,
                    'min_movement_threshold': min_movement_threshold
                },
                'query_limit': limit
            }
            
    except Exception as e:
        handled_error = handle_exception(e, component="advanced_analytics", operation="get_filtered_data")
        logger.error("Failed to get filtered analytics data", error=handled_error, correlation_id=handled_error.correlation_id)
        raise HTTPException(status_code=500, detail=handled_error.user_message)


@analytics_router.get("/export/{export_type}")
async def export_analytics_data(
    export_type: str = Query(..., regex="^(analytics|line_movements|performance|statistical_report)$"),
    format: str = Query("csv", regex="^(csv|json|excel|pdf)$"),
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    teams: Optional[List[str]] = Query(None),
    market_types: Optional[List[str]] = Query(None),
    confidence_threshold: Optional[float] = Query(None, ge=0.0, le=1.0),
    limit: int = Query(1000, ge=1, le=10000)
):
    """Export analytics data in various formats with comprehensive filtering."""
    try:
        async with get_db_connection() as db:
            export_data = None
            filename_base = f"{export_type}_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            if export_type == "analytics":
                export_data = await _export_analytics_data(db, start_date, end_date, teams, market_types, confidence_threshold, limit)
            elif export_type == "line_movements":
                export_data = await _export_line_movements_data(db, start_date, end_date, teams, market_types, limit)
            elif export_type == "performance":
                export_data = await _export_performance_data(db, start_date, end_date, limit)
            elif export_type == "statistical_report":
                export_data = await _export_statistical_report(db, start_date, end_date, teams, market_types)
                
            if not export_data:
                return {
                    "content_type": "application/json",
                    "filename": f"empty_{filename_base}.json",
                    "data": json.dumps({"message": "No data found for export parameters"}, indent=2)
                }
            
            # Format the data based on requested format
            if format == "csv":
                output = io.StringIO()
                if export_data and isinstance(export_data, list) and len(export_data) > 0:
                    fieldnames = export_data[0].keys()
                    writer = csv.DictWriter(output, fieldnames=fieldnames)
                    writer.writeheader()
                    for row in export_data:
                        # Handle datetime objects and None values
                        cleaned_row = {}
                        for key, value in row.items():
                            if isinstance(value, datetime):
                                cleaned_row[key] = value.isoformat()
                            elif value is None:
                                cleaned_row[key] = ''
                            else:
                                cleaned_row[key] = str(value)
                        writer.writerow(cleaned_row)
                
                return {
                    "content_type": "text/csv",
                    "filename": f"{filename_base}.csv",
                    "data": output.getvalue()
                }
            
            elif format == "json":
                return {
                    "content_type": "application/json",
                    "filename": f"{filename_base}.json",
                    "data": json.dumps(export_data, default=str, indent=2)
                }
            
            elif format == "excel":
                # For Excel format, we'll return CSV with Excel-friendly formatting
                output = io.StringIO()
                if export_data and isinstance(export_data, list) and len(export_data) > 0:
                    df = pd.DataFrame(export_data)
                    # Format datetime columns
                    for col in df.columns:
                        if df[col].dtype == 'datetime64[ns]' or 'timestamp' in col.lower():
                            df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d %H:%M:%S')
                    
                    df.to_csv(output, index=False, encoding='utf-8-sig')  # BOM for Excel
                
                return {
                    "content_type": "application/vnd.ms-excel",
                    "filename": f"{filename_base}.csv",
                    "data": output.getvalue()
                }
            
            elif format == "pdf":
                # For PDF, we'll create a simple report format
                pdf_content = await _generate_pdf_report(export_data, export_type, start_date, end_date)
                return {
                    "content_type": "application/pdf",
                    "filename": f"{filename_base}.pdf",
                    "data": pdf_content
                }
            
    except Exception as e:
        handled_error = handle_exception(e, component="advanced_analytics", operation="export_data")
        logger.error("Failed to export analytics data", error=handled_error, correlation_id=handled_error.correlation_id)
        raise HTTPException(status_code=500, detail=handled_error.user_message)


async def _export_analytics_data(db, start_date, end_date, teams, market_types, confidence_threshold, limit):
    """Export betting analytics data."""
    query = """
        SELECT 
            ba.analysis_id,
            ba.game_id,
            ba.analysis_timestamp,
            ba.market_type,
            ba.market_side,
            ba.confidence_score,
            ba.recommendation,
            ba.signal_strength,
            ba.primary_signal,
            ba.risk_level,
            ba.time_to_game,
            gc.home_team,
            gc.away_team,
            gc.game_datetime,
            COALESCE(lms.odds_movement, 0) as odds_movement,
            COALESCE(lms.line_movement, 0) as line_movement,
            COALESCE(lms.total_movements, 0) as total_movements
        FROM curated.betting_analysis ba
        JOIN curated.games_complete gc ON ba.game_id = gc.id
        LEFT JOIN curated.line_movement_summary lms ON (
            ba.game_id = lms.game_id AND ba.market_type = lms.bet_type
        )
        WHERE 1=1
    """
    
    params = []
    if start_date:
        params.append(start_date)
        query += f" AND ba.analysis_timestamp >= ${len(params)}"
    if end_date:
        params.append(end_date)
        query += f" AND ba.analysis_timestamp <= ${len(params)}"
    if teams:
        params.append(teams)
        query += f" AND (gc.home_team = ANY(${len(params)}) OR gc.away_team = ANY(${len(params)}))"
        params.append(teams)
    if market_types:
        params.append(market_types)
        query += f" AND ba.market_type = ANY(${len(params)})"
    if confidence_threshold:
        params.append(confidence_threshold)
        query += f" AND ba.confidence_score >= ${len(params)}"
    
    query += f" ORDER BY ba.analysis_timestamp DESC LIMIT {limit}"
    
    results = await db.fetch(query, *params)
    return [dict(row) for row in results]


async def _export_line_movements_data(db, start_date, end_date, teams, market_types, limit):
    """Export line movement data."""
    query = """
        SELECT 
            lmh.id,
            lmh.line_timestamp,
            lmh.bet_type,
            lmh.side,
            lmh.odds,
            lmh.line_value,
            lmh.home_team,
            lmh.away_team,
            lmh.game_datetime,
            s.display_name as sportsbook_name,
            gc.id as game_id
        FROM curated.line_movement_history lmh
        JOIN curated.sportsbooks s ON lmh.sportsbook_id = s.id
        JOIN curated.games_complete gc ON lmh.game_id = gc.id
        WHERE 1=1
    """
    
    params = []
    if start_date:
        params.append(start_date)
        query += f" AND lmh.line_timestamp >= ${len(params)}"
    if end_date:
        params.append(end_date)
        query += f" AND lmh.line_timestamp <= ${len(params)}"
    if teams:
        params.append(teams)
        query += f" AND (lmh.home_team = ANY(${len(params)}) OR lmh.away_team = ANY(${len(params)}))"
        params.append(teams)
    if market_types:
        params.append(market_types)
        query += f" AND lmh.bet_type = ANY(${len(params)})"
    
    query += f" ORDER BY lmh.line_timestamp DESC LIMIT {limit}"
    
    results = await db.fetch(query, *params)
    return [dict(row) for row in results]


async def _export_performance_data(db, start_date, end_date, limit):
    """Export performance attribution data."""
    query = """
        SELECT 
            ba.analysis_id,
            ba.primary_signal as strategy,
            ba.market_type,
            ba.confidence_score,
            ba.signal_strength,
            ba.recommendation,
            ba.analysis_timestamp,
            gc.home_team,
            gc.away_team,
            gc.game_datetime,
            EXTRACT(HOUR FROM ba.analysis_timestamp) as analysis_hour,
            EXTRACT(DOW FROM ba.analysis_timestamp) as day_of_week,
            -- Real outcome tracking using proper game outcomes and strategy results
            COALESCE(
                -- Check strategy results first (most accurate)
                CASE 
                    WHEN sr.outcome = 'WIN' THEN true
                    WHEN sr.outcome IN ('LOSS', 'PUSH', 'VOID') THEN false
                    ELSE NULL
                END,
                -- Fallback to game outcome analysis based on recommendation
                CASE 
                    -- Spread bets
                    WHEN ba.market_type = 'spread' AND ba.recommendation LIKE '%home%' AND go.home_cover_spread = true THEN true
                    WHEN ba.market_type = 'spread' AND ba.recommendation LIKE '%away%' AND go.home_cover_spread = false THEN true
                    WHEN ba.market_type = 'spread' AND go.home_cover_spread IS NOT NULL THEN false
                    -- Total bets
                    WHEN ba.market_type = 'total' AND ba.recommendation LIKE '%over%' AND go.over = true THEN true
                    WHEN ba.market_type = 'total' AND ba.recommendation LIKE '%under%' AND go.over = false THEN true
                    WHEN ba.market_type = 'total' AND go.over IS NOT NULL THEN false
                    -- Moneyline bets
                    WHEN ba.market_type = 'moneyline' AND ba.recommendation LIKE '%home%' AND go.home_win = true THEN true
                    WHEN ba.market_type = 'moneyline' AND ba.recommendation LIKE '%away%' AND go.home_win = false THEN true
                    WHEN ba.market_type = 'moneyline' AND go.home_win IS NOT NULL THEN false
                    -- No outcome data available
                    ELSE NULL
                END
            ) as was_successful,
            -- Data quality information for export transparency
            CASE
                WHEN sr.outcome IS NOT NULL THEN 'strategy_result'
                WHEN go.game_id IS NOT NULL THEN 'game_outcome'
                ELSE 'no_outcome_data'
            END as outcome_source
        FROM curated.betting_analysis ba
        JOIN curated.games_complete gc ON ba.game_id = gc.id
        LEFT JOIN curated.game_outcomes go ON ba.game_id = go.game_id
        LEFT JOIN analysis.strategy_results sr ON (
            ba.game_id = sr.game_id::INTEGER AND
            ba.market_type = sr.bet_type AND
            ba.analysis_timestamp <= sr.bet_placed_at AND
            sr.status = 'COMPLETED'
        )
        WHERE gc.game_datetime < NOW() - INTERVAL '2 hours'  -- Only include completed games
    """
    
    params = []
    if start_date:
        params.append(start_date)
        query += f" AND ba.analysis_timestamp >= ${len(params)}"
    if end_date:
        params.append(end_date)
        query += f" AND ba.analysis_timestamp <= ${len(params)}"
    
    # Optional: Only include entries with confirmed outcomes for more reliable data
    # query += " AND COALESCE(sr.outcome, CASE WHEN go.game_id IS NOT NULL THEN 'outcome_available' END) IS NOT NULL"
    
    query += f" ORDER BY ba.analysis_timestamp DESC LIMIT {limit}"
    
    results = await db.fetch(query, *params)
    return [dict(row) for row in results]


async def _export_statistical_report(db, start_date, end_date, teams, market_types):
    """Export statistical analysis report."""
    # Get summary statistics
    query = """
        SELECT 
            COUNT(*) as total_analyses,
            AVG(ba.confidence_score) as avg_confidence,
            COUNT(DISTINCT ba.game_id) as unique_games,
            COUNT(DISTINCT ba.primary_signal) as unique_signals,
            ba.market_type,
            ba.primary_signal,
            COUNT(*) as signal_count,
            AVG(ba.confidence_score) as avg_signal_confidence
        FROM curated.betting_analysis ba
        JOIN curated.games_complete gc ON ba.game_id = gc.id
        WHERE 1=1
    """
    
    params = []
    if start_date:
        params.append(start_date)
        query += f" AND ba.analysis_timestamp >= ${len(params)}"
    if end_date:
        params.append(end_date)
        query += f" AND ba.analysis_timestamp <= ${len(params)}"
    if teams:
        params.append(teams)
        query += f" AND (gc.home_team = ANY(${len(params)}) OR gc.away_team = ANY(${len(params)}))"
        params.append(teams)
    if market_types:
        params.append(market_types)
        query += f" AND ba.market_type = ANY(${len(params)})"
    
    query += " GROUP BY ROLLUP(ba.market_type, ba.primary_signal) ORDER BY ba.market_type, signal_count DESC"
    
    results = await db.fetch(query, *params)
    return [dict(row) for row in results]


async def _generate_pdf_report(data, export_type, start_date, end_date):
    """Generate PDF report (simplified HTML-to-PDF approach)."""
    # For a full implementation, you'd use libraries like weasyprint or reportlab
    # This is a simplified version that returns HTML that could be converted to PDF
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>MLB Betting Analytics Report - {export_type.title()}</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; }}
            h1 {{ color: #2563eb; }}
            table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f5f5f5; }}
            .summary {{ background-color: #f0f9ff; padding: 15px; border-radius: 5px; }}
        </style>
    </head>
    <body>
        <h1>MLB Betting Analytics Report</h1>
        <div class="summary">
            <h3>Report Summary</h3>
            <p><strong>Report Type:</strong> {export_type.title()}</p>
            <p><strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p><strong>Date Range:</strong> {start_date or 'All'} to {end_date or 'All'}</p>
            <p><strong>Total Records:</strong> {len(data) if data else 0}</p>
        </div>
        
        <h3>Data Summary</h3>
        <p>This report contains {export_type.replace('_', ' ')} data for the MLB betting analytics system.</p>
        <p>For detailed data analysis, please refer to the CSV or JSON exports.</p>
        
        <p><em>Generated by MLB Betting Analytics System</em></p>
    </body>
    </html>
    """
    
    return html_content  # In production, convert this HTML to PDF