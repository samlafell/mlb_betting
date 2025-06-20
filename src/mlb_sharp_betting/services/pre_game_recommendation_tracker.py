#!/usr/bin/env python3
"""
Pre-Game Recommendation Tracker Service

This service tracks the performance of specific betting recommendations that were 
sent via email through the pre-game workflow system. This is different from general
backtesting - it only analyzes bets that were actually recommended to the user.

Features:
1. Logs betting recommendations from pre-game workflow
2. Tracks performance once game outcomes are available  
3. Generates daily/weekly performance reports
4. Validates that the pre-game alert system is profitable
"""

import asyncio
import json
import structlog
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
import re
import pytz

try:
    from ..db.connection import DatabaseManager, get_db_manager
    from ..core.exceptions import DatabaseError, ValidationError
    from .mlb_api_service import MLBStatsAPIService
except ImportError:
    # Handle direct execution
    import sys
    from pathlib import Path
    
    # Add the src directory to the path
    src_path = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(src_path))
    
    from mlb_sharp_betting.db.connection import DatabaseManager, get_db_manager
    from mlb_sharp_betting.core.exceptions import DatabaseError, ValidationError
    from mlb_sharp_betting.services.mlb_api_service import MLBStatsAPIService


logger = structlog.get_logger(__name__)


@dataclass
class PreGameRecommendation:
    """A specific betting recommendation that was sent via email."""
    recommendation_id: str
    game_pk: int
    home_team: str
    away_team: str
    game_datetime: datetime
    recommendation: str  # e.g., "BET YANKEES", "BET OVER"
    bet_type: str  # moneyline, spread, total
    confidence_level: str  # HIGH, MODERATE, LOW
    signal_source: str  # SHARP_ACTION, OPPOSING_MARKETS, STEAM_MOVE
    signal_strength: float
    recommended_at: datetime
    email_sent: bool = True
    
    # Outcome tracking (filled in after game)
    game_completed: bool = False
    bet_won: Optional[bool] = None
    actual_outcome: Optional[str] = None
    profit_loss: Optional[float] = None  # Assuming $100 unit bets at -110
    
    # Metadata
    created_at: datetime = None
    updated_at: datetime = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)
        if self.updated_at is None:
            self.updated_at = self.created_at


@dataclass
class PerformanceReport:
    """Performance report for pre-game recommendations."""
    report_date: datetime
    period_start: datetime
    period_end: datetime
    
    # Basic metrics
    total_recommendations: int
    completed_games: int
    wins: int
    losses: int
    pending_games: int
    
    # Performance metrics
    win_rate: float
    total_profit_loss: float
    roi_per_100_units: float
    average_bet_profit: float
    
    # Breakdown by type
    by_bet_type: Dict[str, Dict[str, Any]]
    by_signal_source: Dict[str, Dict[str, Any]]
    by_confidence: Dict[str, Dict[str, Any]]
    
    # Recent trends
    last_7_days: Dict[str, Any] = None
    last_30_days: Dict[str, Any] = None
    
    created_at: datetime = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)


class PreGameRecommendationTracker:
    """Service for tracking pre-game betting recommendation performance."""
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """Initialize the recommendation tracker."""
        self.db_manager = db_manager or get_db_manager()
        self.mlb_api = MLBStatsAPIService()
        self.logger = logger.bind(service="recommendation_tracker")
        
        # Timezone setup
        self.est = pytz.timezone('US/Eastern')
        
        # Initialize database tables
        self._initialize_tables()
    
    def parse_pre_game_email_content(self, email_content: str, game_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Parse betting recommendations from pre-game email content.
        
        Args:
            email_content: The text content of the pre-game email
            game_info: Dictionary with game_pk, home_team, away_team, game_datetime
            
        Returns:
            List of recommendation dictionaries
        """
        recommendations = []
        
        try:
            # Look for betting recommendations in the email content
            recommendation_patterns = [
                r'💰\s+(BET\s+[A-Z\s]+)',  # Pattern: 💰 BET YANKEES
                r'💰\s+(BET\s+OVER)',      # Pattern: 💰 BET OVER  
                r'💰\s+(BET\s+UNDER)',     # Pattern: 💰 BET UNDER
                r'Recommendation:\s*([^\\n]+)',  # Pattern: Recommendation: BET TEAM
                r'💰\s*([^\\n]+?(?:BET|bet)[^\\n]*)',  # General betting pattern
            ]
            
            # Extract confidence levels
            confidence_patterns = [
                (r'HIGH CONFIDENCE', 'HIGH'),
                (r'MODERATE CONFIDENCE', 'MODERATE'), 
                (r'LOW CONFIDENCE', 'LOW'),
                (r'STEAM_MOVE', 'HIGH'),  # Steam moves are high confidence
                (r'OPPOSING_MARKETS', 'MODERATE'),  # Opposing markets are moderate
            ]
            
            # Extract signal sources
            source_patterns = [
                (r'STEAM_MOVE|⚡.*STEAM|STEAM.*MOVES', 'STEAM_MOVE'),
                (r'OPPOSING_MARKETS|🔄.*OPPOSING|OPPOSING.*MARKETS', 'OPPOSING_MARKETS'),
                (r'SHARP_ACTION|🔥.*SHARP|SHARP.*ACTION', 'SHARP_ACTION'),
                (r'VALIDATED.*SHARP', 'SHARP_ACTION'),
            ]
            
            # Parse each line looking for recommendations
            lines = email_content.split('\n')
            current_confidence = 'MODERATE'  # Default
            current_source = 'SHARP_ACTION'  # Default
            current_signal_strength = 0.0
            
            for line in lines:
                # Update context based on section headers
                for pattern, confidence in confidence_patterns:
                    if re.search(pattern, line, re.IGNORECASE):
                        current_confidence = confidence
                        break
                
                for pattern, source in source_patterns:
                    if re.search(pattern, line, re.IGNORECASE):
                        current_source = source
                        break
                
                # Extract signal strength if present
                strength_match = re.search(r'(\d+\.?\d*)%.*differential', line)
                if strength_match:
                    current_signal_strength = float(strength_match.group(1))
                
                # Look for betting recommendations
                for pattern in recommendation_patterns:
                    matches = re.findall(pattern, line, re.IGNORECASE)
                    for match in matches:
                        recommendation_text = match.strip().upper()
                        
                        # Determine bet type
                        if 'OVER' in recommendation_text or 'UNDER' in recommendation_text:
                            bet_type = 'total'
                        elif 'SPREAD' in recommendation_text:
                            bet_type = 'spread'
                        else:
                            bet_type = 'moneyline'  # Default
                        
                        # Create recommendation
                        rec_id = f"{game_info['game_pk']}_{current_source}_{bet_type}_{len(recommendations)}"
                        
                        recommendation = {
                            'recommendation_id': rec_id,
                            'game_pk': game_info['game_pk'],
                            'home_team': game_info['home_team'],
                            'away_team': game_info['away_team'],
                            'game_datetime': game_info['game_datetime'],
                            'recommendation': recommendation_text,
                            'bet_type': bet_type,
                            'confidence_level': current_confidence,
                            'signal_source': current_source,
                            'signal_strength': current_signal_strength,
                            'recommended_at': datetime.now(timezone.utc)
                        }
                        
                        recommendations.append(recommendation)
            
            self.logger.info("Parsed pre-game email", 
                           game=f"{game_info['away_team']} @ {game_info['home_team']}",
                           recommendations_found=len(recommendations))
            
        except Exception as e:
            self.logger.error("Failed to parse pre-game email", error=str(e))
        
        return recommendations
    
    async def log_pre_game_recommendations(self, recommendations: List[Dict[str, Any]]) -> None:
        """Log pre-game recommendations to database."""
        if not recommendations:
            return
        
        try:
            with self.db_manager.get_cursor() as cursor:
                for rec in recommendations:
                    cursor.execute("""
                        INSERT OR REPLACE INTO mlb_betting.tracking.pre_game_recommendations (
                            recommendation_id, game_pk, home_team, away_team, game_datetime,
                            recommendation, bet_type, confidence_level, signal_source, signal_strength,
                            recommended_at, email_sent, game_completed, bet_won, actual_outcome, 
                            profit_loss, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        rec['recommendation_id'], rec['game_pk'], rec['home_team'], rec['away_team'], rec['game_datetime'],
                        rec['recommendation'], rec['bet_type'], rec['confidence_level'], rec['signal_source'], 
                        rec['signal_strength'], rec['recommended_at'], True, False,
                        None, None, None, datetime.now(timezone.utc), datetime.now(timezone.utc)
                    ))
                
                self.logger.info("Logged pre-game recommendations", count=len(recommendations))
                
        except Exception as e:
            self.logger.error("Failed to log recommendations", error=str(e))
            raise
    
    def _initialize_tables(self):
        """Create tables for tracking recommendations."""
        try:
            with self.db_manager.get_cursor() as cursor:
                # Create tracking schema first
                cursor.execute("CREATE SCHEMA IF NOT EXISTS mlb_betting.tracking")
                
                # Recommendations table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS mlb_betting.tracking.pre_game_recommendations (
                        recommendation_id VARCHAR PRIMARY KEY,
                        game_pk INTEGER NOT NULL,
                        home_team VARCHAR NOT NULL,
                        away_team VARCHAR NOT NULL,
                        game_datetime TIMESTAMP NOT NULL,
                        recommendation TEXT NOT NULL,
                        bet_type VARCHAR NOT NULL,
                        confidence_level VARCHAR NOT NULL,
                        signal_source VARCHAR NOT NULL,
                        signal_strength DOUBLE NOT NULL,
                        recommended_at TIMESTAMP NOT NULL,
                        email_sent BOOLEAN DEFAULT TRUE,
                        
                        -- Outcome tracking
                        game_completed BOOLEAN DEFAULT FALSE,
                        bet_won BOOLEAN,
                        actual_outcome TEXT,
                        profit_loss DOUBLE,
                        
                        -- Metadata
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                self.logger.info("Recommendation tracking tables initialized")
                
        except Exception as e:
            self.logger.error("Failed to initialize tracking tables", error=str(e))
            raise
    
    def parse_pre_game_email_content(self, email_content: str, game_info: Dict[str, Any]) -> List[PreGameRecommendation]:
        """
        Parse betting recommendations from pre-game email content.
        
        Args:
            email_content: The text content of the pre-game email
            game_info: Dictionary with game_pk, home_team, away_team, game_datetime
            
        Returns:
            List of PreGameRecommendation objects
        """
        recommendations = []
        
        try:
            # Look for betting recommendations in the email content
            recommendation_patterns = [
                r'💰\s+(BET\s+[A-Z\s]+)',  # Pattern: 💰 BET YANKEES
                r'💰\s+(BET\s+OVER)',      # Pattern: 💰 BET OVER  
                r'💰\s+(BET\s+UNDER)',     # Pattern: 💰 BET UNDER
                r'Recommendation:\s*([^\\n]+)',  # Pattern: Recommendation: BET TEAM
                r'💰\s*([^\\n]+?(?:BET|bet)[^\\n]*)',  # General betting pattern
            ]
            
            # Extract confidence levels
            confidence_patterns = [
                (r'HIGH CONFIDENCE', 'HIGH'),
                (r'MODERATE CONFIDENCE', 'MODERATE'), 
                (r'LOW CONFIDENCE', 'LOW'),
                (r'STEAM_MOVE', 'HIGH'),  # Steam moves are high confidence
                (r'OPPOSING_MARKETS', 'MODERATE'),  # Opposing markets are moderate
            ]
            
            # Extract signal sources
            source_patterns = [
                (r'STEAM_MOVE|⚡.*STEAM|STEAM.*MOVES', 'STEAM_MOVE'),
                (r'OPPOSING_MARKETS|🔄.*OPPOSING|OPPOSING.*MARKETS', 'OPPOSING_MARKETS'),
                (r'SHARP_ACTION|🔥.*SHARP|SHARP.*ACTION', 'SHARP_ACTION'),
                (r'VALIDATED.*SHARP', 'SHARP_ACTION'),
            ]
            
            # Parse each line looking for recommendations
            lines = email_content.split('\n')
            current_confidence = 'MODERATE'  # Default
            current_source = 'SHARP_ACTION'  # Default
            current_signal_strength = 0.0
            
            for line in lines:
                # Update context based on section headers
                for pattern, confidence in confidence_patterns:
                    if re.search(pattern, line, re.IGNORECASE):
                        current_confidence = confidence
                        break
                
                for pattern, source in source_patterns:
                    if re.search(pattern, line, re.IGNORECASE):
                        current_source = source
                        break
                
                # Extract signal strength if present
                strength_match = re.search(r'(\d+\.?\d*)%.*differential', line)
                if strength_match:
                    current_signal_strength = float(strength_match.group(1))
                
                # Look for betting recommendations
                for pattern in recommendation_patterns:
                    matches = re.findall(pattern, line, re.IGNORECASE)
                    for match in matches:
                        recommendation_text = match.strip().upper()
                        
                        # Determine bet type
                        if 'OVER' in recommendation_text or 'UNDER' in recommendation_text:
                            bet_type = 'total'
                        elif 'SPREAD' in recommendation_text:
                            bet_type = 'spread'
                        else:
                            bet_type = 'moneyline'  # Default
                        
                        # Create recommendation
                        rec_id = f"{game_info['game_pk']}_{current_source}_{bet_type}_{len(recommendations)}"
                        
                        recommendation = PreGameRecommendation(
                            recommendation_id=rec_id,
                            game_pk=game_info['game_pk'],
                            home_team=game_info['home_team'],
                            away_team=game_info['away_team'],
                            game_datetime=game_info['game_datetime'],
                            recommendation=recommendation_text,
                            bet_type=bet_type,
                            confidence_level=current_confidence,
                            signal_source=current_source,
                            signal_strength=current_signal_strength,
                            recommended_at=datetime.now(timezone.utc)
                        )
                        
                        recommendations.append(recommendation)
            
            self.logger.info("Parsed pre-game email", 
                           game=f"{game_info['away_team']} @ {game_info['home_team']}",
                           recommendations_found=len(recommendations))
            
        except Exception as e:
            self.logger.error("Failed to parse pre-game email", error=str(e))
        
        return recommendations
    
    async def log_pre_game_recommendations(self, recommendations: List[PreGameRecommendation]) -> None:
        """Log pre-game recommendations to database."""
        if not recommendations:
            return
        
        try:
            with self.db_manager.get_cursor() as cursor:
                for rec in recommendations:
                    cursor.execute("""
                        INSERT OR REPLACE INTO mlb_betting.tracking.pre_game_recommendations (
                            recommendation_id, game_pk, home_team, away_team, game_datetime,
                            recommendation, bet_type, confidence_level, signal_source, signal_strength,
                            recommended_at, email_sent, game_completed, bet_won, actual_outcome, 
                            profit_loss, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        rec.recommendation_id, rec.game_pk, rec.home_team, rec.away_team, rec.game_datetime,
                        rec.recommendation, rec.bet_type, rec.confidence_level, rec.signal_source, 
                        rec.signal_strength, rec.recommended_at, rec.email_sent, rec.game_completed,
                        rec.bet_won, rec.actual_outcome, rec.profit_loss, rec.created_at, rec.updated_at
                    ))
                
                self.logger.info("Logged pre-game recommendations", count=len(recommendations))
                
        except Exception as e:
            self.logger.error("Failed to log recommendations", error=str(e))
            raise
    
    async def update_recommendation_outcomes(self, lookback_days: int = 7) -> None:
        """Update outcomes for recommendations from completed games."""
        try:
            with self.db_manager.get_cursor() as cursor:
                # Get recommendations from completed games that haven't been updated
                cursor.execute("""
                    SELECT r.recommendation_id, r.game_pk, r.home_team, r.away_team, 
                           r.recommendation, r.bet_type, r.game_datetime
                    FROM mlb_betting.tracking.pre_game_recommendations r
                    JOIN mlb_betting.main.game_outcomes go ON r.game_pk = go.game_id
                    WHERE r.game_completed = FALSE 
                      AND r.game_datetime >= ?
                    ORDER BY r.game_datetime DESC
                """, (datetime.now(timezone.utc) - timedelta(days=lookback_days),))
                
                recommendations = cursor.fetchall()
                updated_count = 0
                
                for rec_data in recommendations:
                    rec_id, game_pk, home_team, away_team, recommendation, bet_type, game_datetime = rec_data
                    
                    # Get game outcome
                    cursor.execute("""
                        SELECT home_score, away_score, winning_team, total_runs
                        FROM mlb_betting.main.game_outcomes 
                        WHERE game_id = ?
                    """, (game_pk,))
                    
                    outcome_data = cursor.fetchone()
                    if not outcome_data:
                        continue
                    
                    home_score, away_score, winning_team, total_runs = outcome_data
                    
                    # Determine if bet won
                    bet_won = self._evaluate_bet_outcome(
                        recommendation, bet_type, home_team, away_team,
                        home_score, away_score, winning_team, total_runs
                    )
                    
                    # Calculate profit/loss (assuming $100 units at -110 odds)
                    if bet_won is not None:
                        profit_loss = 90.91 if bet_won else -100.0  # -110 odds
                        actual_outcome = f"{winning_team} won {home_score}-{away_score}, total: {total_runs}"
                        
                        # Update recommendation
                        cursor.execute("""
                            UPDATE mlb_betting.tracking.pre_game_recommendations 
                            SET game_completed = TRUE, bet_won = ?, actual_outcome = ?, 
                                profit_loss = ?, updated_at = CURRENT_TIMESTAMP
                            WHERE recommendation_id = ?
                        """, (bet_won, actual_outcome, profit_loss, rec_id))
                        
                        updated_count += 1
                
                self.logger.info("Updated recommendation outcomes", updated_count=updated_count)
                
        except Exception as e:
            self.logger.error("Failed to update recommendation outcomes", error=str(e))
            raise
    
    def _evaluate_bet_outcome(self, recommendation: str, bet_type: str, home_team: str, 
                            away_team: str, home_score: int, away_score: int, 
                            winning_team: str, total_runs: int) -> Optional[bool]:
        """Evaluate whether a betting recommendation won or lost."""
        
        try:
            rec_upper = recommendation.upper()
            
            if bet_type == 'moneyline':
                if f'BET {home_team.upper()}' in rec_upper:
                    return winning_team == home_team
                elif f'BET {away_team.upper()}' in rec_upper:
                    return winning_team == away_team
            
            elif bet_type == 'total':
                if 'BET OVER' in rec_upper:
                    # For this example, assume standard MLB total is 8.5
                    return total_runs > 8.5
                elif 'BET UNDER' in rec_upper:
                    return total_runs < 8.5
            
            elif bet_type == 'spread':
                # This would require knowing the actual spread, which isn't stored
                # For now, return None (unknown)
                return None
            
            return None
            
        except Exception as e:
            self.logger.error("Failed to evaluate bet outcome", 
                            recommendation=recommendation, error=str(e))
            return None
    
    async def generate_performance_report(self, days_back: int = 30) -> PerformanceReport:
        """Generate performance report for pre-game recommendations."""
        
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days_back)
        
        try:
            with self.db_manager.get_cursor() as cursor:
                # Get all recommendations in period
                cursor.execute("""
                    SELECT * FROM mlb_betting.tracking.pre_game_recommendations
                    WHERE recommended_at BETWEEN ? AND ?
                    ORDER BY recommended_at DESC
                """, (start_date, end_date))
                
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                
                if not rows:
                    return PerformanceReport(
                        report_date=end_date,
                        period_start=start_date,
                        period_end=end_date,
                        total_recommendations=0,
                        completed_games=0,
                        wins=0,
                        losses=0,
                        pending_games=0,
                        win_rate=0.0,
                        total_profit_loss=0.0,
                        roi_per_100_units=0.0,
                        average_bet_profit=0.0,
                        by_bet_type={},
                        by_signal_source={},
                        by_confidence={}
                    )
                
                # Convert to list of dicts
                recommendations = [dict(zip(columns, row)) for row in rows]
                
                # Calculate basic metrics
                total_recs = len(recommendations)
                completed = [r for r in recommendations if r['game_completed']]
                completed_count = len(completed)
                wins = [r for r in completed if r['bet_won']]
                losses = [r for r in completed if r['bet_won'] == False]
                pending = total_recs - completed_count
                
                win_rate = len(wins) / completed_count if completed_count > 0 else 0.0
                total_pnl = sum(r['profit_loss'] or 0 for r in completed)
                roi_per_100 = (total_pnl / completed_count) if completed_count > 0 else 0.0
                avg_bet_profit = total_pnl / completed_count if completed_count > 0 else 0.0
                
                # Breakdowns
                by_bet_type = self._calculate_breakdown(completed, 'bet_type')
                by_signal_source = self._calculate_breakdown(completed, 'signal_source')
                by_confidence = self._calculate_breakdown(completed, 'confidence_level')
                
                # Recent trends
                last_7_days = self._calculate_recent_performance(recommendations, 7)
                last_30_days = self._calculate_recent_performance(recommendations, 30)
                
                return PerformanceReport(
                    report_date=end_date,
                    period_start=start_date,
                    period_end=end_date,
                    total_recommendations=total_recs,
                    completed_games=completed_count,
                    wins=len(wins),
                    losses=len(losses),
                    pending_games=pending,
                    win_rate=win_rate,
                    total_profit_loss=total_pnl,
                    roi_per_100_units=roi_per_100,
                    average_bet_profit=avg_bet_profit,
                    by_bet_type=by_bet_type,
                    by_signal_source=by_signal_source,
                    by_confidence=by_confidence,
                    last_7_days=last_7_days,
                    last_30_days=last_30_days
                )
                
        except Exception as e:
            self.logger.error("Failed to generate performance report", error=str(e))
            raise
    
    def _calculate_breakdown(self, recommendations: List[Dict], breakdown_field: str) -> Dict[str, Dict[str, Any]]:
        """Calculate performance breakdown by a specific field."""
        breakdown = {}
        
        for rec in recommendations:
            key = rec[breakdown_field]
            if key not in breakdown:
                breakdown[key] = {
                    'total': 0, 'wins': 0, 'losses': 0, 'win_rate': 0.0, 
                    'total_pnl': 0.0, 'avg_profit': 0.0
                }
            
            breakdown[key]['total'] += 1
            if rec['bet_won'] == True:
                breakdown[key]['wins'] += 1
            elif rec['bet_won'] == False:
                breakdown[key]['losses'] += 1
            
            if rec['profit_loss']:
                breakdown[key]['total_pnl'] += rec['profit_loss']
        
        # Calculate derived metrics
        for key, data in breakdown.items():
            completed = data['wins'] + data['losses']
            if completed > 0:
                data['win_rate'] = data['wins'] / completed
                data['avg_profit'] = data['total_pnl'] / completed
        
        return breakdown
    
    def _calculate_recent_performance(self, recommendations: List[Dict], days: int) -> Dict[str, Any]:
        """Calculate performance for recent period."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        recent = [r for r in recommendations if r['recommended_at'] >= cutoff and r['game_completed']]
        
        if not recent:
            return {'total': 0, 'wins': 0, 'win_rate': 0.0, 'total_pnl': 0.0}
        
        wins = len([r for r in recent if r['bet_won']])
        total_pnl = sum(r['profit_loss'] or 0 for r in recent)
        
        return {
            'total': len(recent),
            'wins': wins,
            'win_rate': wins / len(recent),
            'total_pnl': total_pnl,
            'avg_profit': total_pnl / len(recent)
        }
    
    async def generate_daily_report_text(self, days_back: int = 7) -> str:
        """Generate a human-readable daily performance report."""
        
        # Update outcomes first
        await self.update_recommendation_outcomes(lookback_days=days_back)
        
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days_back)
        
        try:
            with self.db_manager.get_cursor() as cursor:
                # Get all recommendations in period
                cursor.execute("""
                    SELECT * FROM mlb_betting.tracking.pre_game_recommendations
                    WHERE recommended_at BETWEEN ? AND ?
                    ORDER BY recommended_at DESC
                """, (start_date, end_date))
                
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                
                if not rows:
                    return f"""# 📊 PRE-GAME BETTING RECOMMENDATION PERFORMANCE

**Period:** {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}
**Generated:** {end_date.strftime('%Y-%m-%d %H:%M UTC')}

## ❌ No Recommendations Found

No pre-game betting recommendations were tracked in this period.
This typically means:
- No pre-game workflows were triggered
- No betting opportunities were found by the master detector
- The recommendation tracking system needs to be integrated

*To start tracking: The pre-game workflow needs to log recommendations when they're sent via email.*

---
*General Balls*"""
                
                # Convert to list of dicts
                recommendations = [dict(zip(columns, row)) for row in rows]
                
                # Calculate basic metrics
                total_recs = len(recommendations)
                completed = [r for r in recommendations if r['game_completed']]
                completed_count = len(completed)
                wins = [r for r in completed if r['bet_won']]
                losses = [r for r in completed if r['bet_won'] == False]
                pending = total_recs - completed_count
                
                win_rate = len(wins) / completed_count if completed_count > 0 else 0.0
                total_pnl = sum(r['profit_loss'] or 0 for r in completed)
                roi_per_100 = (total_pnl / completed_count) if completed_count > 0 else 0.0
                avg_bet_profit = total_pnl / completed_count if completed_count > 0 else 0.0
                
                # Breakdowns
                by_bet_type = self._calculate_breakdown(completed, 'bet_type')
                by_signal_source = self._calculate_breakdown(completed, 'signal_source')
                by_confidence = self._calculate_breakdown(completed, 'confidence_level')
                
                # Format report
                report_lines = [
                    "# 📊 PRE-GAME BETTING RECOMMENDATION PERFORMANCE",
                    f"**Period:** {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
                    f"**Generated:** {end_date.strftime('%Y-%m-%d %H:%M UTC')}",
                    "",
                    "## 📈 Overall Performance",
                    f"- **Total Recommendations:** {total_recs}",
                    f"- **Completed Games:** {completed_count}",
                    f"- **Pending Games:** {pending}",
                    f"- **Win Rate:** {win_rate:.1%} ({len(wins)}W-{len(losses)}L)",
                    f"- **Total Profit/Loss:** ${total_pnl:+.2f}",
                    f"- **ROI per $100:** {roi_per_100:+.2f}%",
                    f"- **Average Bet Profit:** ${avg_bet_profit:+.2f}",
                    "",
                ]
                
                # Profitability assessment
                if completed_count > 0:
                    break_even_rate = 52.38  # At -110 odds
                    if win_rate * 100 >= break_even_rate:
                        status = "✅ PROFITABLE"
                        emoji = "🎯"
                    else:
                        status = "❌ UNPROFITABLE" 
                        emoji = "⚠️"
                    
                    report_lines.extend([
                        f"## {emoji} Status Assessment",
                        f"**{status}** (Break-even: {break_even_rate:.1%})",
                        f"**Performance vs Break-even:** {(win_rate - break_even_rate/100)*100:+.1f} percentage points",
                        ""
                    ])
                
                # Breakdown by signal source
                if by_signal_source:
                    report_lines.extend([
                        "## 🔍 Performance by Signal Source",
                    ])
                    for source, data in by_signal_source.items():
                        if data['total'] > 0:
                            report_lines.append(f"**{source}:** {data['win_rate']:.1%} ({data['wins']}W-{data['losses']}L) | ${data['total_pnl']:+.2f}")
                    report_lines.append("")
                
                # Breakdown by confidence level
                if by_confidence:
                    report_lines.extend([
                        "## 🎯 Performance by Confidence Level",
                    ])
                    for confidence, data in by_confidence.items():
                        if data['total'] > 0:
                            report_lines.append(f"**{confidence}:** {data['win_rate']:.1%} ({data['wins']}W-{data['losses']}L) | ${data['total_pnl']:+.2f}")
                    report_lines.append("")
                
                # Breakdown by bet type
                if by_bet_type:
                    report_lines.extend([
                        "## 🎲 Performance by Bet Type",
                    ])
                    for bet_type, data in by_bet_type.items():
                        if data['total'] > 0:
                            report_lines.append(f"**{bet_type.title()}:** {data['win_rate']:.1%} ({data['wins']}W-{data['losses']}L) | ${data['total_pnl']:+.2f}")
                    report_lines.append("")
                
                report_lines.extend([
                    "---",
                    "*Report tracks only betting recommendations sent via pre-game email alerts*",
                    "*General Balls*"
                ])
                
                return "\n".join(report_lines)
                
        except Exception as e:
            self.logger.error("Failed to generate performance report", error=str(e))
            raise


async def main():
    """Test the recommendation tracker."""
    tracker = PreGameRecommendationTracker()
    
    # Generate and print report
    report_text = await tracker.generate_daily_report_text(days_back=7)
    print(report_text)


if __name__ == "__main__":
    asyncio.run(main()) 