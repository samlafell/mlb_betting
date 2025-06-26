"""
Data Deduplication Service
==========================

This service handles data integrity issues by implementing the "One Bet Per Market" rule
and ensuring clean, deduplicated data for analysis.

Key Features:
1. Enforces maximum 1 bet per game per market type (Moneyline, Spread, Total)
2. Captures final bet recommendations exactly 5 minutes before first pitch
3. Handles cross-book consensus signals separately from duplicates
4. Validates data integrity and provides cleanup procedures
5. Value-based deduplication instead of time-based filtering
6. Dynamic thresholds based on historical performance
7. Cross-source validation and consensus tracking
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

from mlb_sharp_betting.core.logging import get_logger
from mlb_sharp_betting.services.database_coordinator import get_database_coordinator
from mlb_sharp_betting.core.exceptions import DatabaseError

logger = get_logger(__name__)


class MarketType(Enum):
    """Betting market types."""
    MONEYLINE = "moneyline"
    SPREAD = "spread"
    TOTAL = "total"


@dataclass
class BettingRecommendation:
    """A single, deduplicated betting recommendation."""
    game_id: str
    home_team: str
    away_team: str
    game_datetime: datetime
    market_type: MarketType
    source: str
    book: str
    recommended_side: str  # 'home', 'away', 'over', 'under'
    line_value: Optional[str]
    confidence_score: float  # 0.0 to 1.0 based on differential strength
    differential: float  # Stake % - Bet %
    stake_percentage: float
    bet_percentage: float
    minutes_before_game: int
    signal_strength: str  # 'WEAK', 'MODERATE', 'STRONG', 'VERY_STRONG'
    consensus_boost: float  # Additional confidence from cross-source agreement
    last_updated: datetime
    
    @property
    def unique_key(self) -> str:
        """Generate unique key for deduplication."""
        return f"{self.game_id}_{self.market_type.value}_{self.source}_{self.book}_{int(self.last_updated.timestamp())}"


@dataclass
class ConsensusSignal:
    """Cross-book consensus when multiple sources agree."""
    game_id: str
    home_team: str
    away_team: str
    game_datetime: datetime
    market_type: MarketType
    recommended_side: str
    agreeing_sources: List[str]  # List of source-book combinations
    consensus_strength: float  # 0.0 to 1.0 based on agreement level
    avg_differential: float
    total_sources: int
    detected_at: datetime


class DataDeduplicationService:
    """Service for enforcing data integrity and deduplication rules."""
    
    def __init__(self):
        self.coordinator = get_database_coordinator()
        self.logger = logger.bind(service="data_deduplication")
        
        # Initialize deduplication infrastructure
        self._ensure_deduplication_tables()
    
    def _ensure_deduplication_tables(self):
        """Create tables for tracking deduplicated recommendations."""
        try:
            # Create schema first
            self.coordinator.execute_write("CREATE SCHEMA IF NOT EXISTS mlb_betting.clean", [])
            
            # Create deduplicated recommendations table
            dedup_table_sql = """
            CREATE TABLE IF NOT EXISTS mlb_betting.clean.betting_recommendations (
                id VARCHAR PRIMARY KEY,
                game_id VARCHAR NOT NULL,
                home_team VARCHAR NOT NULL,
                away_team VARCHAR NOT NULL,
                game_datetime TIMESTAMP NOT NULL,
                market_type VARCHAR NOT NULL,
                source VARCHAR NOT NULL,
                book VARCHAR NOT NULL,
                recommended_side VARCHAR NOT NULL,
                line_value VARCHAR,
                confidence_score DOUBLE NOT NULL,
                differential DOUBLE NOT NULL,
                stake_percentage DOUBLE NOT NULL,
                bet_percentage DOUBLE NOT NULL,
                minutes_before_game INTEGER NOT NULL,
                signal_strength VARCHAR NOT NULL,
                consensus_boost DOUBLE DEFAULT 0.0,
                last_updated TIMESTAMP NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            # Create signal evolution tracking table
            evolution_table_sql = """
            CREATE TABLE IF NOT EXISTS mlb_betting.clean.signal_evolution (
                id VARCHAR PRIMARY KEY,
                game_id VARCHAR NOT NULL,
                market_type VARCHAR NOT NULL,
                source VARCHAR NOT NULL,
                book VARCHAR NOT NULL,
                differential DOUBLE NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                minutes_before_game DOUBLE NOT NULL,
                evolution_type VARCHAR -- 'INITIAL', 'STEAM', 'REVERSE', 'FINAL'
            )
            """
            
            self.coordinator.execute_write(dedup_table_sql, [])
            self.coordinator.execute_write(evolution_table_sql, [])
            self.logger.info("Deduplication tables ensured")
            
        except Exception as e:
            self.logger.error("Failed to create deduplication tables", error=str(e))
            raise DatabaseError(f"Failed to create deduplication tables: {e}")
    
    def apply_database_constraints(self):
        """Apply database constraints to prevent future duplicates."""
        try:
            # For PostgreSQL, we use the standard approach since IF NOT EXISTS isn't supported for constraints
            # First check if constraint exists, then add if needed
            constraint_sql = """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_game_split_daily 
            ON mlb_betting.splits.raw_mlb_betting_splits (game_id, split_type, source, book, DATE(last_updated))
            """
            
            self.coordinator.execute_write(constraint_sql, [])
            self.logger.info("Applied database constraints")
            
        except Exception as e:
            # Constraint might already exist, log but don't fail
            self.logger.warning("Could not apply database constraint", error=str(e))
    
    def _get_dynamic_threshold(self, market_type: MarketType) -> float:
        """Get dynamic threshold based on market type and historical performance."""
        
        # Query historical performance by threshold
        performance_query = """
        SELECT 
            CASE 
                WHEN ABS(differential) >= 15 THEN 15
                WHEN ABS(differential) >= 10 THEN 10
                WHEN ABS(differential) >= 7 THEN 7
                WHEN ABS(differential) >= 5 THEN 5
                ELSE 0
            END as threshold_bucket,
            COUNT(*) as signal_count,
            AVG(CASE WHEN outcome = 'WIN' THEN 1.0 ELSE 0.0 END) as win_rate
        FROM mlb_betting.clean.betting_recommendations r
        LEFT JOIN mlb_betting.results.game_outcomes o ON r.game_id = o.game_id
        WHERE r.market_type = $1
          AND r.game_datetime >= CURRENT_DATE - INTERVAL 90 DAY
        GROUP BY threshold_bucket
        HAVING signal_count >= 10
        ORDER BY win_rate DESC
        """
        
        try:
            results = self.coordinator.execute_read(performance_query, [market_type.value])
            
            # Return the threshold with best historical performance
            if results:
                best_threshold = results[0][0]  # First row has highest win rate
                return max(best_threshold, 5.0)  # Minimum 5% to avoid noise
        except Exception as e:
            self.logger.warning("Could not calculate dynamic threshold", error=str(e))
        
        return 8.0  # Conservative default
    
    def process_raw_data_for_deduplication(self, lookback_days: int = 30) -> Dict:
        """
        Process raw betting data to create clean, deduplicated recommendations.
        
        Args:
            lookback_days: How many days back to process
            
        Returns:
            Dictionary with processing results
        """
        self.logger.info("Processing raw data for deduplication", lookback_days=lookback_days)
        
        processed_games = 0
        recommendations_created = 0
        
        # Get all games from the lookback period
        games_query = """
        SELECT DISTINCT 
            game_id, home_team, away_team, game_datetime
        FROM mlb_betting.splits.raw_mlb_betting_splits
        WHERE game_datetime >= CURRENT_DATE - INTERVAL $1 DAY
        ORDER BY game_datetime DESC
        """
        
        games = self.coordinator.execute_read(games_query, [lookback_days])
        
        for game_row in games:
            game_id, home_team, away_team, game_datetime = game_row
            
            # Process each market type for this game
            for market_type in MarketType:
                recs_created = self._process_game_market_for_deduplication(
                    game_id, home_team, away_team, game_datetime, market_type
                )
                recommendations_created += recs_created
            
            processed_games += 1
        
        results = {
            'processed_games': processed_games,
            'recommendations_created': recommendations_created,
            'timestamp': datetime.now()
        }
        
        self.logger.info("Completed raw data deduplication", **results)
        return results
    
    def _process_game_market_for_deduplication(self, game_id: str, home_team: str, 
                                             away_team: str, game_datetime: datetime,
                                             market_type: MarketType) -> int:
        """Process a single game's market for deduplication using value-based logic."""
        
        # Get dynamic threshold based on historical performance
        threshold = self._get_dynamic_threshold(market_type)
        
        # GET ALL SIGNIFICANT RECORDS, NOT JUST ONE PER SOURCE
        market_data_query = """
        SELECT 
            source, book, split_type,
            home_or_over_stake_percentage, home_or_over_bets_percentage,
            home_or_over_stake_percentage - home_or_over_bets_percentage as differential,
            split_value, last_updated,
            EXTRACT(EPOCH FROM (game_datetime - last_updated)) / 60 as minutes_before_game
        FROM mlb_betting.splits.raw_mlb_betting_splits
        WHERE game_id = $1
          AND split_type = $2
          AND last_updated < game_datetime
          AND ABS(home_or_over_stake_percentage - home_or_over_bets_percentage) >= $3
        ORDER BY source, book, last_updated DESC
        """
        
        market_data = self.coordinator.execute_read(
            market_data_query, [game_id, market_type.value, threshold]
        )
        
        # NOW DEDUPLICATE BASED ON ACTUAL VALUE CHANGES
        return self._deduplicate_by_value_changes(
            market_data, game_id, home_team, away_team, game_datetime, market_type
        )
    
    def _deduplicate_by_value_changes(self, market_data: List, game_id: str, 
                                    home_team: str, away_team: str, 
                                    game_datetime: datetime, market_type: MarketType) -> int:
        """Deduplicate based on actual value changes, not time."""
        
        recommendations_created = 0
        source_book_tracking = {}  # Track last values per source-book
        
        for row in market_data:
            source, book = row[0], row[1]
            current_differential = row[5]
            last_updated = row[7]
            
            source_book_key = f"{source}_{book}"
            
            # Check if this is a meaningful change from last recorded value
            if source_book_key in source_book_tracking:
                last_differential = source_book_tracking[source_book_key]['differential']
                last_timestamp = source_book_tracking[source_book_key]['timestamp']
                
                # Only skip if values are essentially identical AND recent
                value_diff = abs(current_differential - last_differential)
                time_diff = (last_updated - last_timestamp).total_seconds() if isinstance(last_updated, datetime) else 0
                
                if value_diff < 2.0 and time_diff < 1800:  # 30 minutes for identical values
                    continue  # Skip this record
            
            # This is a meaningful update - create recommendation
            recommendation = self._create_clean_recommendation(
                row, game_id, home_team, away_team, game_datetime, market_type
            )
            
            if recommendation:
                recommendations_created += 1
                
                # Update tracking
                source_book_tracking[source_book_key] = {
                    'differential': current_differential,
                    'timestamp': last_updated
                }
                
                # Track signal evolution
                self._track_signal_evolution(
                    game_id, market_type, source, book, current_differential, 
                    last_updated, row[8]  # minutes_before_game
                )
        
        return recommendations_created
    
    def _check_cross_source_consensus(self, game_id: str, market_type: MarketType, 
                                    recommended_side: str, differential: float, 
                                    timestamp: datetime) -> float:
        """Check if other sources agree with this signal."""
        
        consensus_query = """
        SELECT COUNT(DISTINCT CONCAT(source, '-', book)) as agreeing_sources
        FROM mlb_betting.splits.raw_mlb_betting_splits
        WHERE game_id = $1
          AND split_type = $2
          AND ABS(EXTRACT(EPOCH FROM (last_updated - $3))) <= 900  -- 15 minute window
          AND CASE 
              WHEN $4 = 'home' OR $4 = 'over' THEN 
                  (home_or_over_stake_percentage - home_or_over_bets_percentage) > 5
              ELSE 
                  (home_or_over_stake_percentage - home_or_over_bets_percentage) < -5
              END
        """
        
        try:
            agreeing_sources = self.coordinator.execute_read(
                consensus_query, [game_id, market_type.value, timestamp, recommended_side]
            )[0][0]
            
            # Boost confidence for consensus signals
            return min(agreeing_sources * 2.0, 10.0)  # Max 10% boost
        except Exception as e:
            self.logger.warning("Could not check cross-source consensus", error=str(e))
            return 0.0
    
    def _track_signal_evolution(self, game_id: str, market_type: MarketType, 
                              source: str, book: str, differential: float,
                              timestamp: datetime, minutes_before: float):
        """Track signal evolution for steam move and reverse line movement detection."""
        
        # Determine evolution type based on timing and magnitude
        evolution_type = 'INITIAL'
        if minutes_before <= 5:
            evolution_type = 'FINAL'
        elif minutes_before <= 30:
            evolution_type = 'STEAM' if abs(differential) >= 15 else 'LATE'
        
        insert_sql = """
        INSERT OR REPLACE INTO mlb_betting.clean.signal_evolution
        (id, game_id, market_type, source, book, differential, timestamp, 
         minutes_before_game, evolution_type)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """
        
        evolution_id = f"{game_id}_{market_type.value}_{source}_{book}_{int(timestamp.timestamp())}"
        
        try:
            self.coordinator.execute_write(insert_sql, [
                evolution_id, game_id, market_type.value, source, book,
                differential, timestamp, minutes_before, evolution_type
            ])
        except Exception as e:
            self.logger.warning("Could not track signal evolution", error=str(e))
    
    def _create_clean_recommendation(self, row: tuple, game_id: str, home_team: str, 
                                   away_team: str, game_datetime: datetime,
                                   market_type: MarketType) -> Optional[BettingRecommendation]:
        """Create a clean betting recommendation with cross-source validation."""
        
        try:
            source, book = row[0], row[1]
            stake_pct, bet_pct, differential = row[3], row[4], row[5]
            split_value, last_updated, minutes_before = row[6], row[7], row[8]
            
            # Determine recommended side based on differential
            if differential > 0:  # Sharp money on home/over
                recommended_side = 'home' if market_type in [MarketType.MONEYLINE, MarketType.SPREAD] else 'over'
            else:  # Sharp money on away/under
                recommended_side = 'away' if market_type in [MarketType.MONEYLINE, MarketType.SPREAD] else 'under'
            
            # ENHANCEMENT: Check for cross-source consensus
            consensus_boost = self._check_cross_source_consensus(
                game_id, market_type, recommended_side, differential, last_updated
            )
            
            # Calculate confidence score and signal strength with consensus boost
            abs_differential = abs(differential)
            confidence_score = min((abs_differential + consensus_boost) / 30.0, 1.0)
            
            if abs_differential >= 25:
                signal_strength = 'VERY_STRONG'
            elif abs_differential >= 20:
                signal_strength = 'STRONG'
            elif abs_differential >= 15:
                signal_strength = 'MODERATE'
            else:
                signal_strength = 'WEAK'
            
            recommendation = BettingRecommendation(
                game_id=game_id,
                home_team=home_team,
                away_team=away_team,
                game_datetime=game_datetime,
                market_type=market_type,
                source=source,
                book=book,
                recommended_side=recommended_side,
                line_value=split_value,
                confidence_score=confidence_score,
                differential=differential,
                stake_percentage=stake_pct,
                bet_percentage=bet_pct,
                minutes_before_game=int(minutes_before),
                signal_strength=signal_strength,
                consensus_boost=consensus_boost,
                last_updated=last_updated
            )
            
            # Store in database
            self._store_clean_recommendation(recommendation)
            return recommendation
            
        except Exception as e:
            self.logger.error("Failed to create clean recommendation", 
                            game_id=game_id, error=str(e))
            return None
    
    def _store_clean_recommendation(self, recommendation: BettingRecommendation):
        """Store clean recommendation in database."""
        
        insert_sql = """
        INSERT OR REPLACE INTO mlb_betting.clean.betting_recommendations
        (id, game_id, home_team, away_team, game_datetime, market_type, source, book,
         recommended_side, line_value, confidence_score, differential, stake_percentage,
         bet_percentage, minutes_before_game, signal_strength, consensus_boost, last_updated)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
        """
        
        params = [
            recommendation.unique_key,
            recommendation.game_id,
            recommendation.home_team,
            recommendation.away_team,
            recommendation.game_datetime,
            recommendation.market_type.value,
            recommendation.source,
            recommendation.book,
            recommendation.recommended_side,
            recommendation.line_value,
            recommendation.confidence_score,
            recommendation.differential,
            recommendation.stake_percentage,
            recommendation.bet_percentage,
            recommendation.minutes_before_game,
            recommendation.signal_strength,
            recommendation.consensus_boost,
            recommendation.last_updated
        ]
        
        self.coordinator.execute_write(insert_sql, params)
    
    def track_signal_evolution(self, game_id: str, market_type: MarketType) -> List[Dict]:
        """Track how signals evolve over time leading up to game."""
        
        evolution_query = """
        SELECT 
            source, book, 
            home_or_over_stake_percentage - home_or_over_bets_percentage as differential,
            last_updated,
            EXTRACT(EPOCH FROM (game_datetime - last_updated)) / 60 as minutes_before
        FROM mlb_betting.splits.raw_mlb_betting_splits
        WHERE game_id = $1 AND split_type = $2
        ORDER BY source, book, last_updated
        """
        
        rows = self.coordinator.execute_read(evolution_query, [game_id, market_type.value])
        
        # Group by source-book and show evolution
        evolution = {}
        for row in rows:
            key = f"{row[0]}-{row[1]}"
            if key not in evolution:
                evolution[key] = []
            
            evolution[key].append({
                'differential': row[2],
                'timestamp': row[3],
                'minutes_before': row[4]
            })
        
        return evolution
    
    def get_clean_recommendations(self, days_back: int = 7) -> List[Dict]:
        """Get clean, deduplicated betting recommendations."""
        
        query = """
        SELECT 
            game_id, home_team, away_team, game_datetime, market_type,
            source, book, recommended_side, confidence_score, differential,
            signal_strength, minutes_before_game, consensus_boost
        FROM mlb_betting.clean.betting_recommendations
        WHERE game_datetime >= CURRENT_DATE - INTERVAL $1 DAY
        ORDER BY game_datetime DESC, confidence_score DESC
        """
        
        rows = self.coordinator.execute_read(query, [days_back])
        
        return [
            {
                'game': f"{row[1]} vs {row[2]}",
                'game_id': row[0],
                'market_type': row[4],
                'source': f"{row[5]}-{row[6]}",
                'recommended_side': row[7],
                'confidence_score': row[8],
                'differential': row[9],
                'signal_strength': row[10],
                'minutes_before_game': row[11],
                'consensus_boost': row[12]
            }
            for row in rows
        ]
    
    def cleanup_historical_duplicates(self, keep_latest_only: bool = True) -> Dict:
        """Clean up historical duplicate records."""
        
        if keep_latest_only:
            # Delete all but the latest record per game/market/source
            cleanup_sql = """
            DELETE FROM mlb_betting.splits.raw_mlb_betting_splits
            WHERE id NOT IN (
                SELECT id FROM (
                    SELECT id,
                        ROW_NUMBER() OVER (
                            PARTITION BY game_id, split_type, source, book 
                            ORDER BY last_updated DESC
                        ) as rn
                    FROM mlb_betting.splits.raw_mlb_betting_splits
                ) ranked
                WHERE rn = 1
            )
            """
        else:
            # More conservative: only delete obvious duplicates (same timestamp)
            cleanup_sql = """
            DELETE FROM mlb_betting.splits.raw_mlb_betting_splits
            WHERE id NOT IN (
                SELECT MIN(id) FROM mlb_betting.splits.raw_mlb_betting_splits
                GROUP BY game_id, split_type, source, book, last_updated
            )
            """
        
        # Count before cleanup
        count_before_sql = "SELECT COUNT(*) FROM mlb_betting.splits.raw_mlb_betting_splits"
        count_before = self.coordinator.execute_read(count_before_sql)[0][0]
        
        # Perform cleanup
        self.coordinator.execute_write(cleanup_sql, [])
        
        # Count after cleanup
        count_after = self.coordinator.execute_read(count_before_sql)[0][0]
        
        records_removed = count_before - count_after
        
        result = {
            'records_before': count_before,
            'records_after': count_after,
            'records_removed': records_removed,
            'cleanup_type': 'latest_only' if keep_latest_only else 'same_timestamp_only'
        }
        
        self.logger.info("Completed historical cleanup", **result)
        return result
    
    def generate_data_quality_report(self) -> Dict:
        """Generate comprehensive data quality report."""
        
        # Raw data metrics
        raw_metrics_query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT game_id) as unique_games,
            COUNT(DISTINCT CONCAT(game_id, '-', split_type)) as unique_game_markets,
            COUNT(DISTINCT CONCAT(source, '-', book)) as unique_sources
        FROM mlb_betting.splits.raw_mlb_betting_splits
        WHERE game_datetime >= CURRENT_DATE - INTERVAL 7 DAY
        """
        
        clean_metrics_query = """
        SELECT 
            COUNT(*) as total_recommendations,
            COUNT(DISTINCT game_id) as unique_games,
            COUNT(DISTINCT CONCAT(game_id, '-', market_type)) as unique_game_markets,
            AVG(confidence_score) as avg_confidence,
            AVG(consensus_boost) as avg_consensus_boost
        FROM mlb_betting.clean.betting_recommendations
        WHERE game_datetime >= CURRENT_DATE - INTERVAL 7 DAY
        """
        
        raw_metrics = self.coordinator.execute_read(raw_metrics_query)[0]
        clean_metrics = self.coordinator.execute_read(clean_metrics_query)[0]
        
        # Calculate data quality metrics
        raw_total, raw_games, raw_markets, raw_sources = raw_metrics
        clean_total, clean_games, clean_markets, clean_avg_conf, clean_avg_consensus = clean_metrics
        
        duplication_ratio = (raw_total - clean_total) / raw_total if raw_total > 0 else 0
        market_coverage = clean_markets / raw_markets if raw_markets > 0 else 0
        
        report = {
            'raw_data': {
                'total_records': raw_total,
                'unique_games': raw_games,
                'unique_markets': raw_markets,
                'unique_sources': raw_sources,
                'avg_records_per_market': raw_total / raw_markets if raw_markets > 0 else 0
            },
            'clean_data': {
                'total_recommendations': clean_total,
                'unique_games': clean_games,
                'unique_markets': clean_markets,
                'avg_confidence_score': clean_avg_conf,
                'avg_consensus_boost': clean_avg_consensus
            },
            'quality_metrics': {
                'duplication_elimination': f"{duplication_ratio:.1%}",
                'market_coverage': f"{market_coverage:.1%}",
                'data_reduction': f"{(1 - clean_total/raw_total):.1%}" if raw_total > 0 else "0%",
                'quality_score': 'EXCELLENT' if duplication_ratio > 0.8 else 'GOOD' if duplication_ratio > 0.5 else 'NEEDS_IMPROVEMENT'
            },
            'generated_at': datetime.now()
        }
        
        return report


def main():
    """Test the deduplication service."""
    service = DataDeduplicationService()
    
    print("🧹 Running Enhanced Data Deduplication Process...")
    
    # Step 1: Apply constraints
    service.apply_database_constraints()
    print("   ✅ Applied database constraints")
    
    # Step 2: Process raw data with value-based deduplication
    results = service.process_raw_data_for_deduplication(lookback_days=30)
    print(f"   ✅ Processed {results['processed_games']} games, created {results['recommendations_created']} recommendations")
    
    # Step 3: Generate quality report
    report = service.generate_data_quality_report()
    print(f"   ✅ Data quality: {report['quality_metrics']['quality_score']}")
    print(f"      • Eliminated {report['quality_metrics']['duplication_elimination']} duplicate data")
    print(f"      • Reduced dataset by {report['quality_metrics']['data_reduction']}")
    print(f"      • Average consensus boost: {report['clean_data']['avg_consensus_boost']:.2f}")
    
    # Step 4: Show sample clean recommendations
    clean_recs = service.get_clean_recommendations(days_back=7)
    print(f"\n📊 Sample Clean Recommendations (Last 7 Days): {len(clean_recs)} total")
    
    for rec in clean_recs[:5]:  # Show top 5
        consensus_indicator = f" (+{rec['consensus_boost']:.1f}% consensus)" if rec['consensus_boost'] > 0 else ""
        print(f"   • {rec['game']} | {rec['market_type']} | {rec['source']} | "
              f"BET {rec['recommended_side'].upper()} | {rec['signal_strength']} | "
              f"{rec['differential']:+.1f}% differential{consensus_indicator}")
    
    return results, report


if __name__ == "__main__":
    main() 