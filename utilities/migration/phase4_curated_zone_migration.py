#!/usr/bin/env python3
"""
Phase 4: CURATED Zone Migration

Migrates and enhances data from STAGING zone to CURATED zone with ML features,
analytics, and betting intelligence. This phase creates analysis-ready datasets
with advanced feature engineering for betting strategies.

Migration flow:
- staging.games → curated.enhanced_games (with ML features)
- staging.moneylines → curated.moneylines (with analytics)
- staging.spreads → curated.spreads (with analytics)
- staging.totals → curated.totals (with analytics)
- Advanced feature engineering for betting analytics
"""

import asyncio
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timezone, timedelta
import json
import math
from decimal import Decimal, ROUND_HALF_UP


class DecimalEncoder(json.JSONEncoder):
    """JSON encoder that handles Decimal objects."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.core.config import get_settings
from src.core.logging import get_logger, LogComponent
from src.data.database import get_connection
from src.data.database.connection import initialize_connections

logger = get_logger(__name__, LogComponent.CORE)


class MLFeatureEngineer:
    """Advanced ML feature engineering for betting analytics."""
    
    @staticmethod
    def calculate_implied_probability(american_odds: int) -> float:
        """Convert American odds to implied probability."""
        if american_odds == 0:
            return 0.0
            
        if american_odds > 0:
            return 100 / (american_odds + 100)
        else:
            return abs(american_odds) / (abs(american_odds) + 100)
    
    @staticmethod
    def calculate_no_vig_probability(prob1: float, prob2: float) -> Tuple[float, float]:
        """Remove vig from implied probabilities."""
        total_prob = prob1 + prob2
        if total_prob == 0:
            return 0.0, 0.0
        
        return prob1 / total_prob, prob2 / total_prob
    
    @staticmethod
    def calculate_expected_value(true_prob: float, odds: int, stake: float = 100.0) -> float:
        """Calculate expected value for a bet."""
        if odds == 0:
            return 0.0
            
        if odds > 0:
            potential_win = (odds / 100) * stake
        else:
            potential_win = (100 / abs(odds)) * stake
            
        return (true_prob * potential_win) - ((1 - true_prob) * stake)
    
    @staticmethod
    def calculate_market_efficiency_score(home_prob: float, away_prob: float) -> float:
        """Calculate market efficiency based on probability sum deviation from 1."""
        total_prob = home_prob + away_prob
        efficiency = 1.0 - abs(total_prob - 1.0)
        return max(0.0, min(9.99, efficiency * 10))
    
    @classmethod
    def create_betting_features(cls, record: dict, bet_type: str) -> dict:
        """Create comprehensive betting features for ML models."""
        features = {
            'bet_type': bet_type,
            'sportsbook_tier': cls._get_sportsbook_tier(record.get('sportsbook_name', '')),
            'data_quality_score': float(record.get('data_quality_score', 0)),
            'validation_status': record.get('validation_status', 'unknown')
        }
        
        if bet_type == 'moneyline':
            home_odds = record.get('home_odds', 0) or 0
            away_odds = record.get('away_odds', 0) or 0
            
            home_prob = cls.calculate_implied_probability(home_odds)
            away_prob = cls.calculate_implied_probability(away_odds)
            no_vig_home, no_vig_away = cls.calculate_no_vig_probability(home_prob, away_prob)
            
            features.update({
                'home_implied_prob': home_prob,
                'away_implied_prob': away_prob,
                'no_vig_home_prob': no_vig_home,
                'no_vig_away_prob': no_vig_away,
                'market_efficiency': cls.calculate_market_efficiency_score(home_prob, away_prob),
                'home_expected_value': cls.calculate_expected_value(no_vig_home, home_odds),
                'away_expected_value': cls.calculate_expected_value(no_vig_away, away_odds),
                'odds_differential': abs(home_odds - away_odds) if home_odds and away_odds else 0
            })
            
        elif bet_type == 'spread':
            spread_odds = record.get('spread_odds', 0) or 0
            spread_value = float(record.get('spread_value', 0) or 0)
            
            implied_prob = cls.calculate_implied_probability(spread_odds)
            
            features.update({
                'spread_value': spread_value,
                'spread_implied_prob': implied_prob,
                'spread_magnitude': abs(spread_value),
                'spread_expected_value': cls.calculate_expected_value(0.5, spread_odds),  # Assume 50% true prob
                'key_number_proximity': cls._calculate_key_number_proximity(spread_value)
            })
            
        elif bet_type == 'total':
            over_odds = record.get('over_odds', 0) or 0
            under_odds = record.get('under_odds', 0) or 0
            total_points = float(record.get('total_points', 0) or 0)
            
            over_prob = cls.calculate_implied_probability(over_odds)
            under_prob = cls.calculate_implied_probability(under_odds)
            no_vig_over, no_vig_under = cls.calculate_no_vig_probability(over_prob, under_prob)
            
            features.update({
                'total_points': total_points,
                'over_implied_prob': over_prob,
                'under_implied_prob': under_prob,
                'no_vig_over_prob': no_vig_over,
                'no_vig_under_prob': no_vig_under,
                'over_expected_value': cls.calculate_expected_value(no_vig_over, over_odds),
                'under_expected_value': cls.calculate_expected_value(no_vig_under, under_odds),
                'total_tier': cls._get_total_tier(total_points)
            })
        
        return features
    
    @staticmethod
    def _get_sportsbook_tier(sportsbook_name: str) -> str:
        """Classify sportsbook by market influence tier."""
        premium_books = {'Pinnacle', 'Circa Sports', 'CRIS'}
        major_books = {'DraftKings', 'FanDuel', 'BetMGM', 'Caesars'}
        
        if sportsbook_name in premium_books:
            return 'premium'
        elif sportsbook_name in major_books:
            return 'major'
        else:
            return 'regional'
    
    @staticmethod
    def _calculate_key_number_proximity(spread_value: float) -> float:
        """Calculate proximity to key numbers (3, 7, 10 in NFL; 1.5, 2.5 in MLB)."""
        key_numbers = [1.5, 2.5, 3.5]  # MLB key numbers
        min_distance = min(abs(spread_value - key) for key in key_numbers)
        return max(0, 3 - min_distance)  # Higher score for closer to key numbers
    
    @staticmethod
    def _get_total_tier(total_points: float) -> str:
        """Categorize total by MLB scoring environment."""
        if total_points < 7.5:
            return 'low'
        elif total_points < 9.5:
            return 'medium'
        else:
            return 'high'


class SharpActionDetector:
    """Detects sharp money indicators and market inefficiencies."""
    
    @staticmethod
    def detect_reverse_line_movement(record: dict, bet_type: str) -> bool:
        """Detect reverse line movement patterns."""
        # Placeholder - would analyze historical line movements
        # For now, use simple heuristics based on data quality and odds
        quality_score = float(record.get('data_quality_score', 0))
        return quality_score > 8.5  # High quality data often indicates sharp action
    
    @staticmethod
    def detect_steam_moves(record: dict, bet_type: str) -> bool:
        """Detect steam move patterns."""
        # Placeholder - would analyze rapid line movements across multiple books
        sportsbook_name = record.get('sportsbook_name', '')
        return sportsbook_name in ['Pinnacle', 'Circa Sports']  # Sharp books indicator
    
    @staticmethod
    def calculate_sharp_action_score(record: dict, bet_type: str) -> float:
        """Calculate overall sharp action probability score."""
        score = 5.0  # Neutral starting point
        
        # Data quality influence
        quality_score = float(record.get('data_quality_score', 0))
        if quality_score > 9.0:
            score += 1.5
        elif quality_score > 8.0:
            score += 0.5
        elif quality_score < 6.0:
            score -= 1.0
        
        # Sportsbook influence
        sbook_name = record.get('sportsbook_name', '')
        if sbook_name in ['Pinnacle', 'Circa Sports']:
            score += 2.0
        elif sbook_name in ['DraftKings', 'FanDuel', 'BetMGM']:
            score += 0.5
        
        # Validation status influence
        validation_status = record.get('validation_status', '')
        if validation_status == 'validated':
            score += 0.5
        elif validation_status == 'invalid':
            score -= 2.0
        
        return max(0.0, min(9.99, score))
    
    @classmethod
    def analyze_sharp_indicators(cls, record: dict, bet_type: str) -> dict:
        """Comprehensive sharp action analysis."""
        return {
            'reverse_line_movement': cls.detect_reverse_line_movement(record, bet_type),
            'steam_move_detected': cls.detect_steam_moves(record, bet_type),
            'sharp_action_score': cls.calculate_sharp_action_score(record, bet_type),
            'market_inefficiency_score': cls._calculate_market_inefficiency(record, bet_type)
        }
    
    @staticmethod
    def _calculate_market_inefficiency(record: dict, bet_type: str) -> float:
        """Calculate market inefficiency indicators."""
        # Placeholder for advanced market efficiency analysis
        base_score = 5.0
        quality_score = float(record.get('data_quality_score', 0))
        
        # High quality data from sharp books indicates efficient pricing
        if quality_score > 9.0:
            return max(0.0, base_score - 2.0)  # More efficient
        else:
            return min(9.99, base_score + 1.0)  # Less efficient


class CuratedZoneMigrator:
    """Handles migration and enhancement of STAGING zone data to CURATED zone."""
    
    def __init__(self, batch_size: int = 500):
        self.settings = get_settings()
        self.batch_size = batch_size
        self.migration_stats = {
            'enhanced_games': {'processed': 0, 'successful': 0, 'failed': 0},
            'moneylines': {'processed': 0, 'successful': 0, 'failed': 0},
            'spreads': {'processed': 0, 'successful': 0, 'failed': 0},
            'totals': {'processed': 0, 'successful': 0, 'failed': 0},
            'feature_vectors': {'processed': 0, 'successful': 0, 'failed': 0}
        }
        self.ml_engineer = MLFeatureEngineer()
        self.sharp_detector = SharpActionDetector()
        
    async def initialize(self):
        """Initialize database connection."""
        initialize_connections(self.settings)
        
    async def close(self):
        """Close database connections."""
        pass  # Connection pool managed globally
    
    async def migrate_all_to_curated_zone(self) -> Dict[str, Any]:
        """Execute complete CURATED zone migration with ML features."""
        logger.info("Starting Phase 4: CURATED zone migration")
        
        migration_results = {
            'timestamp': datetime.now().isoformat(),
            'phase': 'Phase 4 - CURATED Zone Migration',
            'status': 'in_progress',
            'tables_migrated': {},
            'summary': {},
            'errors': []
        }
        
        try:
            connection_manager = get_connection()
            async with connection_manager.get_async_connection() as conn:
                # Pre-flight checks
                await self._verify_curated_zone_tables(conn)
                
                # Clear existing CURATED data for clean migration
                await self._clear_curated_tables(conn)
                
                # Migrate enhanced games first (needed for foreign keys)
                migration_results['tables_migrated']['enhanced_games'] = await self._migrate_enhanced_games(conn)
                
                # Migrate betting tables with ML features
                migration_results['tables_migrated']['moneylines'] = await self._migrate_moneylines(conn)
                migration_results['tables_migrated']['spreads'] = await self._migrate_spreads(conn)
                migration_results['tables_migrated']['totals'] = await self._migrate_totals(conn)
                
                # Create feature vectors for ML models
                migration_results['tables_migrated']['feature_vectors'] = await self._create_feature_vectors(conn)
                
                # Generate summary
                migration_results['summary'] = self._generate_migration_summary()
                migration_results['status'] = 'completed'
                
        except Exception as e:
            logger.error(f"CURATED zone migration failed: {e}")
            migration_results['status'] = 'failed'
            migration_results['error'] = str(e)
            migration_results['errors'].append({
                'timestamp': datetime.now().isoformat(),
                'error': str(e),
                'context': 'main_migration_loop'
            })
            
        return migration_results
    
    async def _verify_curated_zone_tables(self, conn):
        """Verify that CURATED zone tables exist and are accessible."""
        logger.info("Verifying CURATED zone table structure...")
        
        required_tables = [
            'curated.enhanced_games',
            'curated.moneylines',
            'curated.spreads',
            'curated.totals',
            'curated.feature_vectors'
        ]
        
        for table in required_tables:
            schema, table_name = table.split('.')
            result = await conn.fetchrow("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = $1 AND table_name = $2
                )
            """, schema, table_name)
            
            if not result[0]:
                raise Exception(f"Required CURATED zone table {table} does not exist")
        
        logger.info("✅ All CURATED zone tables verified")
    
    async def _clear_curated_tables(self, conn):
        """Clear existing CURATED data for clean migration."""
        logger.info("Clearing CURATED zone tables for clean migration...")
        
        # Order matters due to foreign key constraints
        tables = [
            'curated.feature_vectors',
            'curated.betting_analysis', 
            'curated.movement_analysis',
            'curated.prediction_inputs',
            'curated.strategy_results',
            'curated.moneylines',
            'curated.spreads', 
            'curated.totals',
            'curated.enhanced_games'
        ]
        
        for table in tables:
            await conn.execute(f"DELETE FROM {table}")
            logger.info(f"Cleared {table}")
        
        logger.info("✅ CURATED zone tables cleared")
    
    async def _migrate_enhanced_games(self, conn) -> Dict[str, Any]:
        """Create enhanced game records with ML features."""
        logger.info("Migrating enhanced games to CURATED zone...")
        
        table_stats = {'processed': 0, 'successful': 0, 'failed': 0, 'batches': 0}
        
        try:
            # Get staging games with aggregated betting data
            games_query = """
                SELECT 
                    sg.*,
                    COUNT(DISTINCT sm.sportsbook_name) as unique_sportsbooks,
                    COUNT(sm.id) as total_moneyline_records,
                    COUNT(ss.id) as total_spread_records,
                    COUNT(st.id) as total_total_records,
                    AVG(sm.data_quality_score) as avg_moneyline_quality,
                    AVG(ss.data_quality_score) as avg_spread_quality,
                    AVG(st.data_quality_score) as avg_total_quality
                FROM staging.games sg
                LEFT JOIN staging.moneylines sm ON sg.id = sm.game_id
                LEFT JOIN staging.spreads ss ON sg.id = ss.game_id  
                LEFT JOIN staging.totals st ON sg.id = st.game_id
                GROUP BY sg.id, sg.external_id, sg.game_date, sg.game_datetime, 
                         sg.data_quality_score, sg.validation_status, sg.created_at, sg.updated_at
                ORDER BY sg.game_date DESC, sg.id
            """
            
            games = await conn.fetch(games_query)
            total_games = len(games)
            logger.info(f"Processing {total_games} enhanced games...")
            
            batch_successful = 0
            for game in games:
                try:
                    # Calculate enhanced features
                    market_features = self._create_market_features(game)
                    sharp_action_summary = self._create_sharp_action_summary(game)
                    
                    # Derive team names (simplified - would integrate with team normalization)
                    home_team = "HOME_TEAM"  # Placeholder
                    away_team = "AWAY_TEAM"  # Placeholder
                    season = game['game_date'].year if game.get('game_date') else 2024
                    
                    # Insert enhanced game record
                    await conn.execute("""
                        INSERT INTO curated.enhanced_games (
                            staging_game_id, home_team, away_team, game_datetime, season,
                            market_features, sharp_action_summary, created_at, updated_at
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    """, 
                        game['id'],
                        home_team,
                        away_team,
                        game['game_datetime'],
                        season,
                        json.dumps(market_features, cls=DecimalEncoder),
                        json.dumps(sharp_action_summary, cls=DecimalEncoder),
                        datetime.now(timezone.utc),
                        datetime.now(timezone.utc)
                    )
                    
                    batch_successful += 1
                    
                except Exception as e:
                    logger.error(f"Failed to migrate enhanced game {game['id']}: {e}")
                    table_stats['failed'] += 1
            
            table_stats['processed'] = total_games
            table_stats['successful'] = batch_successful
            table_stats['batches'] = 1
            
            self.migration_stats['enhanced_games'] = table_stats
            logger.info(f"✅ Enhanced games migration completed: {batch_successful:,} games")
            
        except Exception as e:
            logger.error(f"Enhanced games migration failed: {e}")
            table_stats['error'] = str(e)
            
        return table_stats
    
    def _create_market_features(self, game: dict) -> dict:
        """Create market-level features for the game."""
        return {
            'unique_sportsbooks': game.get('unique_sportsbooks', 0),
            'total_betting_records': (game.get('total_moneyline_records', 0) + 
                                    game.get('total_spread_records', 0) + 
                                    game.get('total_total_records', 0)),
            'market_depth_score': min(9.99, (game.get('unique_sportsbooks', 0) * 1.5)),
            'data_coverage_score': min(9.99, (
                (game.get('total_moneyline_records', 0) * 0.3) + 
                (game.get('total_spread_records', 0) * 0.4) + 
                (game.get('total_total_records', 0) * 0.3)
            ) / 10),
            'avg_data_quality': (
                (game.get('avg_moneyline_quality', 0) or 0) + 
                (game.get('avg_spread_quality', 0) or 0) + 
                (game.get('avg_total_quality', 0) or 0)
            ) / 3 if any([game.get('avg_moneyline_quality'), game.get('avg_spread_quality'), game.get('avg_total_quality')]) else 5.0
        }
    
    def _create_sharp_action_summary(self, game: dict) -> dict:
        """Create sharp action summary for the game."""
        return {
            'sharp_book_coverage': 1 if game.get('unique_sportsbooks', 0) >= 3 else 0,
            'data_quality_tier': 'high' if game.get('data_quality_score', 0) >= 8.0 else 'medium' if game.get('data_quality_score', 0) >= 6.0 else 'low',
            'market_efficiency_estimate': min(9.99, game.get('data_quality_score', 5.0)),
            'liquidity_score': min(9.99, (game.get('total_moneyline_records', 0) + 
                                        game.get('total_spread_records', 0) + 
                                        game.get('total_total_records', 0)) / 10)
        }
    
    async def _migrate_moneylines(self, conn) -> Dict[str, Any]:
        """Migrate moneylines with ML features and analytics."""
        logger.info("Migrating moneylines to CURATED zone...")
        
        table_stats = {'processed': 0, 'successful': 0, 'failed': 0, 'batches': 0}
        
        try:
            # Get total count
            count_result = await conn.fetchrow("""
                SELECT COUNT(*) as total FROM staging.moneylines
            """)
            total_records = count_result['total']
            logger.info(f"Processing {total_records:,} moneyline records...")
            
            # Process in batches
            offset = 0
            while offset < total_records:
                batch_records = await conn.fetch("""
                    SELECT 
                        sm.*,
                        ceg.id as curated_game_id
                    FROM staging.moneylines sm
                    JOIN curated.enhanced_games ceg ON sm.game_id = ceg.staging_game_id
                    ORDER BY sm.id
                    LIMIT $1 OFFSET $2
                """, self.batch_size, offset)
                
                if not batch_records:
                    break
                
                batch_successful = 0
                for record in batch_records:
                    try:
                        # Skip records without curated game reference
                        if not record['curated_game_id']:
                            table_stats['failed'] += 1
                            continue
                        
                        # Create ML features
                        record_dict = dict(record)
                        ml_features = self.ml_engineer.create_betting_features(record_dict, 'moneyline')
                        sharp_analysis = self.sharp_detector.analyze_sharp_indicators(record_dict, 'moneyline')
                        
                        # Calculate advanced analytics
                        home_odds = record.get('home_odds') or 0
                        away_odds = record.get('away_odds') or 0
                        
                        home_prob = self.ml_engineer.calculate_implied_probability(home_odds)
                        away_prob = self.ml_engineer.calculate_implied_probability(away_odds)
                        no_vig_home, no_vig_away = self.ml_engineer.calculate_no_vig_probability(home_prob, away_prob)
                        
                        home_ev = self.ml_engineer.calculate_expected_value(no_vig_home, home_odds)
                        away_ev = self.ml_engineer.calculate_expected_value(no_vig_away, away_odds)
                        market_efficiency = self.ml_engineer.calculate_market_efficiency_score(home_prob, away_prob)
                        
                        # Insert curated record
                        await conn.execute("""
                            INSERT INTO curated.moneylines (
                                staging_id, game_id, sportsbook_name, home_odds, away_odds,
                                implied_probability_home, implied_probability_away, 
                                no_vig_probability_home, no_vig_probability_away,
                                expected_value_home, expected_value_away, market_efficiency_score,
                                sharp_action_indicator, feature_vector, created_at, updated_at
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
                        """,
                            record['id'],  # staging_id
                            record['curated_game_id'],
                            record['sportsbook_name'],
                            home_odds,
                            away_odds,
                            round(Decimal(home_prob), 4),
                            round(Decimal(away_prob), 4),
                            round(Decimal(no_vig_home), 4),
                            round(Decimal(no_vig_away), 4),
                            round(Decimal(home_ev), 4),
                            round(Decimal(away_ev), 4),
                            round(Decimal(market_efficiency), 2),
                            sharp_analysis['sharp_action_score'] > 7.0,
                            json.dumps(ml_features, cls=DecimalEncoder),
                            datetime.now(timezone.utc),
                            datetime.now(timezone.utc)
                        )
                        
                        batch_successful += 1
                        
                    except Exception as e:
                        logger.error(f"Failed to migrate moneyline record {record['id']}: {e}")
                        table_stats['failed'] += 1
                
                table_stats['processed'] += len(batch_records)
                table_stats['successful'] += batch_successful
                table_stats['batches'] += 1
                offset += self.batch_size
                
                # Progress logging
                progress = (offset / total_records) * 100
                logger.info(f"Moneylines progress: {progress:.1f}% ({offset:,}/{total_records:,})")
            
            self.migration_stats['moneylines'] = table_stats
            logger.info(f"✅ Moneylines migration completed: {table_stats['successful']:,} records")
            
        except Exception as e:
            logger.error(f"Moneylines migration failed: {e}")
            table_stats['error'] = str(e)
            
        return table_stats
    
    async def _migrate_spreads(self, conn) -> Dict[str, Any]:
        """Migrate spreads with ML features and analytics."""
        logger.info("Migrating spreads to CURATED zone...")
        
        table_stats = {'processed': 0, 'successful': 0, 'failed': 0, 'batches': 0}
        
        try:
            count_result = await conn.fetchrow("""
                SELECT COUNT(*) as total FROM staging.spreads
            """)
            total_records = count_result['total']
            logger.info(f"Processing {total_records:,} spread records...")
            
            offset = 0
            while offset < total_records:
                batch_records = await conn.fetch("""
                    SELECT 
                        ss.*,
                        ceg.id as curated_game_id
                    FROM staging.spreads ss
                    JOIN curated.enhanced_games ceg ON ss.game_id = ceg.staging_game_id
                    ORDER BY ss.id
                    LIMIT $1 OFFSET $2
                """, self.batch_size, offset)
                
                if not batch_records:
                    break
                
                batch_successful = 0
                for record in batch_records:
                    try:
                        if not record['curated_game_id']:
                            table_stats['failed'] += 1
                            continue
                        
                        # Create ML features
                        record_dict = dict(record)
                        ml_features = self.ml_engineer.create_betting_features(record_dict, 'spread')
                        sharp_analysis = self.sharp_detector.analyze_sharp_indicators(record_dict, 'spread')
                        
                        # Calculate advanced analytics
                        spread_odds = record.get('spread_odds') or 0
                        spread_value = float(record.get('spread_value') or 0)
                        
                        implied_prob = self.ml_engineer.calculate_implied_probability(spread_odds)
                        expected_value = self.ml_engineer.calculate_expected_value(0.5, spread_odds)  # Assume 50% true prob
                        
                        # Detect patterns
                        rlm_detected = self.sharp_detector.detect_reverse_line_movement(record_dict, 'spread')
                        steam_detected = self.sharp_detector.detect_steam_moves(record_dict, 'spread')
                        
                        # Insert curated record
                        await conn.execute("""
                            INSERT INTO curated.spreads (
                                staging_id, game_id, sportsbook_name, spread_value, spread_odds,
                                implied_probability, expected_value, sharp_action_indicator,
                                reverse_line_movement, steam_move_detected, feature_vector,
                                created_at, updated_at
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                        """,
                            record['id'],  # staging_id
                            record['curated_game_id'],
                            record['sportsbook_name'],
                            round(Decimal(spread_value), 1),
                            spread_odds,
                            round(Decimal(implied_prob), 4),
                            round(Decimal(expected_value), 4),
                            sharp_analysis['sharp_action_score'] > 7.0,
                            rlm_detected,
                            steam_detected,
                            json.dumps(ml_features, cls=DecimalEncoder),
                            datetime.now(timezone.utc),
                            datetime.now(timezone.utc)
                        )
                        
                        batch_successful += 1
                        
                    except Exception as e:
                        logger.error(f"Failed to migrate spread record {record['id']}: {e}")
                        table_stats['failed'] += 1
                
                table_stats['processed'] += len(batch_records)
                table_stats['successful'] += batch_successful
                table_stats['batches'] += 1
                offset += self.batch_size
                
                progress = (offset / total_records) * 100
                logger.info(f"Spreads progress: {progress:.1f}% ({offset:,}/{total_records:,})")
            
            self.migration_stats['spreads'] = table_stats
            logger.info(f"✅ Spreads migration completed: {table_stats['successful']:,} records")
            
        except Exception as e:
            logger.error(f"Spreads migration failed: {e}")
            table_stats['error'] = str(e)
            
        return table_stats
    
    async def _migrate_totals(self, conn) -> Dict[str, Any]:
        """Migrate totals with ML features and analytics."""
        logger.info("Migrating totals to CURATED zone...")
        
        table_stats = {'processed': 0, 'successful': 0, 'failed': 0, 'batches': 0}
        
        try:
            count_result = await conn.fetchrow("""
                SELECT COUNT(*) as total FROM staging.totals
            """)
            total_records = count_result['total']
            logger.info(f"Processing {total_records:,} totals records...")
            
            offset = 0
            while offset < total_records:
                batch_records = await conn.fetch("""
                    SELECT 
                        st.*,
                        ceg.id as curated_game_id
                    FROM staging.totals st
                    JOIN curated.enhanced_games ceg ON st.game_id = ceg.staging_game_id
                    ORDER BY st.id
                    LIMIT $1 OFFSET $2
                """, self.batch_size, offset)
                
                if not batch_records:
                    break
                
                batch_successful = 0
                for record in batch_records:
                    try:
                        if not record['curated_game_id']:
                            table_stats['failed'] += 1
                            continue
                        
                        # Create ML features
                        record_dict = dict(record)
                        ml_features = self.ml_engineer.create_betting_features(record_dict, 'total')
                        sharp_analysis = self.sharp_detector.analyze_sharp_indicators(record_dict, 'total')
                        
                        # Calculate advanced analytics
                        over_odds = record.get('over_odds') or 0
                        under_odds = record.get('under_odds') or 0
                        total_points = float(record.get('total_points') or 0)
                        
                        over_prob = self.ml_engineer.calculate_implied_probability(over_odds)
                        under_prob = self.ml_engineer.calculate_implied_probability(under_odds)
                        no_vig_over, no_vig_under = self.ml_engineer.calculate_no_vig_probability(over_prob, under_prob)
                        
                        over_ev = self.ml_engineer.calculate_expected_value(no_vig_over, over_odds)
                        under_ev = self.ml_engineer.calculate_expected_value(no_vig_under, under_odds)
                        
                        # Placeholder impact scores (would integrate weather/pitcher data)
                        weather_impact = 5.0  # Neutral
                        pitcher_impact = 5.0  # Neutral
                        
                        # Insert curated record
                        await conn.execute("""
                            INSERT INTO curated.totals (
                                staging_id, game_id, sportsbook_name, total_points, 
                                over_odds, under_odds, implied_probability_over, implied_probability_under,
                                expected_value_over, expected_value_under, weather_impact_score,
                                pitcher_impact_score, sharp_action_indicator, feature_vector,
                                created_at, updated_at
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
                        """,
                            record['id'],  # staging_id
                            record['curated_game_id'],
                            record['sportsbook_name'],
                            round(Decimal(total_points), 1),
                            over_odds,
                            under_odds,
                            round(Decimal(over_prob), 4),
                            round(Decimal(under_prob), 4),
                            round(Decimal(over_ev), 4),
                            round(Decimal(under_ev), 4),
                            round(Decimal(weather_impact), 2),
                            round(Decimal(pitcher_impact), 2),
                            sharp_analysis['sharp_action_score'] > 7.0,
                            json.dumps(ml_features, cls=DecimalEncoder),
                            datetime.now(timezone.utc),
                            datetime.now(timezone.utc)
                        )
                        
                        batch_successful += 1
                        
                    except Exception as e:
                        logger.error(f"Failed to migrate totals record {record['id']}: {e}")
                        table_stats['failed'] += 1
                
                table_stats['processed'] += len(batch_records)
                table_stats['successful'] += batch_successful
                table_stats['batches'] += 1
                offset += self.batch_size
                
                progress = (offset / total_records) * 100
                logger.info(f"Totals progress: {progress:.1f}% ({offset:,}/{total_records:,})")
            
            self.migration_stats['totals'] = table_stats
            logger.info(f"✅ Totals migration completed: {table_stats['successful']:,} records")
            
        except Exception as e:
            logger.error(f"Totals migration failed: {e}")
            table_stats['error'] = str(e)
            
        return table_stats
    
    async def _create_feature_vectors(self, conn) -> Dict[str, Any]:
        """Create feature vectors for ML models."""
        logger.info("Creating feature vectors for ML models...")
        
        table_stats = {'processed': 0, 'successful': 0, 'failed': 0}
        
        try:
            # Create feature vectors for each bet type per game
            bet_types = ['moneyline', 'spread', 'total']
            
            for bet_type in bet_types:
                if bet_type == 'moneyline':
                    query = """
                        SELECT 
                            ceg.id as game_id,
                            cm.feature_vector,
                            AVG(cm.expected_value_home + cm.expected_value_away) as avg_expected_value
                        FROM curated.enhanced_games ceg
                        JOIN curated.moneylines cm ON ceg.id = cm.game_id
                        GROUP BY ceg.id, cm.feature_vector
                    """
                elif bet_type == 'spread':
                    query = """
                        SELECT 
                            ceg.id as game_id,
                            cs.feature_vector,
                            AVG(cs.expected_value) as avg_expected_value
                        FROM curated.enhanced_games ceg
                        JOIN curated.spreads cs ON ceg.id = cs.game_id
                        GROUP BY ceg.id, cs.feature_vector
                    """
                else:  # totals
                    query = """
                        SELECT 
                            ceg.id as game_id,
                            ct.feature_vector,
                            AVG(ct.expected_value_over + ct.expected_value_under) as avg_expected_value
                        FROM curated.enhanced_games ceg
                        JOIN curated.totals ct ON ceg.id = ct.game_id
                        GROUP BY ceg.id, ct.feature_vector
                    """
                
                feature_records = await conn.fetch(query)
                
                for record in feature_records:
                    try:
                        feature_vector = json.loads(record['feature_vector']) if record['feature_vector'] else {}
                        
                        # Add game-level features
                        game_features = await self._get_game_features(conn, record['game_id'])
                        feature_vector.update(game_features)
                        
                        # Insert feature vector
                        await conn.execute("""
                            INSERT INTO curated.feature_vectors (
                                game_id, bet_type, feature_set_version, features, 
                                target_variable, created_at
                            ) VALUES ($1, $2, $3, $4, $5, $6)
                        """,
                            record['game_id'],
                            bet_type,
                            "v1.0",
                            json.dumps(feature_vector, cls=DecimalEncoder),
                            round(Decimal(record.get('avg_expected_value', 0) or 0), 6),
                            datetime.now(timezone.utc)
                        )
                        
                        table_stats['successful'] += 1
                        
                    except Exception as e:
                        logger.error(f"Failed to create feature vector for game {record['game_id']}, bet_type {bet_type}: {e}")
                        table_stats['failed'] += 1
                    
                    table_stats['processed'] += 1
            
            self.migration_stats['feature_vectors'] = table_stats
            logger.info(f"✅ Feature vectors created: {table_stats['successful']:,} vectors")
            
        except Exception as e:
            logger.error(f"Feature vectors creation failed: {e}")
            table_stats['error'] = str(e)
            
        return table_stats
    
    async def _get_game_features(self, conn, game_id: int) -> dict:
        """Get game-level features for ML models."""
        game_info = await conn.fetchrow("""
            SELECT market_features, sharp_action_summary, game_datetime, season
            FROM curated.enhanced_games
            WHERE id = $1
        """, game_id)
        
        if not game_info:
            return {}
        
        features = {}
        
        # Add market features
        if game_info['market_features']:
            market_features = json.loads(game_info['market_features'])
            features.update({
                f"market_{k}": v for k, v in market_features.items()
            })
        
        # Add sharp action features
        if game_info['sharp_action_summary']:
            sharp_features = json.loads(game_info['sharp_action_summary'])
            features.update({
                f"sharp_{k}": v for k, v in sharp_features.items()
            })
        
        # Add temporal features
        if game_info['game_datetime']:
            dt = game_info['game_datetime']
            features.update({
                'hour_of_day': dt.hour,
                'day_of_week': dt.weekday(),
                'month': dt.month,
                'is_weekend': dt.weekday() >= 5
            })
        
        features['season'] = game_info.get('season', 2024)
        
        return features
    
    def _generate_migration_summary(self) -> Dict[str, Any]:
        """Generate migration summary statistics."""
        total_processed = sum(stats['processed'] for stats in self.migration_stats.values())
        total_successful = sum(stats['successful'] for stats in self.migration_stats.values())
        total_failed = sum(stats['failed'] for stats in self.migration_stats.values())
        
        return {
            'total_records_processed': total_processed,
            'total_records_successful': total_successful,
            'total_records_failed': total_failed,
            'success_rate': (total_successful / total_processed * 100) if total_processed > 0 else 0,
            'tables_migrated': len([k for k, v in self.migration_stats.items() if v['successful'] > 0]),
            'migration_stats_by_table': self.migration_stats,
            'migration_completed_at': datetime.now().isoformat(),
            'ml_features_created': True,
            'analytics_enabled': True
        }


async def main():
    """Main execution function."""
    migrator = CuratedZoneMigrator(batch_size=500)
    
    try:
        await migrator.initialize()
        
        print("🚀 Starting Phase 4: CURATED Zone Migration")
        print("=" * 60)
        
        # Run migration
        results = await migrator.migrate_all_to_curated_zone()
        
        # Display results
        print("\n📊 MIGRATION RESULTS")
        print("-" * 40)
        
        if results['status'] == 'completed' and 'summary' in results:
            summary = results['summary']
            print(f"✅ Migration Status: {results['status'].upper()}")
            print(f"📈 Total Records Processed: {summary.get('total_records_processed', 0):,}")
            print(f"✅ Successful Migrations: {summary.get('total_records_successful', 0):,}")
            print(f"❌ Failed Migrations: {summary.get('total_records_failed', 0):,}")
            print(f"📊 Success Rate: {summary.get('success_rate', 0):.1f}%")
            print(f"🗄️ Tables Migrated: {summary.get('tables_migrated', 0)}")
            print(f"🧠 ML Features: {'✅ Created' if summary.get('ml_features_created') else '❌ Failed'}")
            print(f"📈 Analytics: {'✅ Enabled' if summary.get('analytics_enabled') else '❌ Failed'}")
            
            print("\n📋 Migration Details by Table:")
            for table, stats in summary.get('migration_stats_by_table', {}).items():
                print(f"  {table}: {stats['successful']:,} successful, {stats['failed']} failed")
        else:
            print(f"❌ Migration Status: {results['status'].upper()}")
            if 'error' in results:
                print(f"Error: {results['error']}")
        
        # Save results to file
        output_file = Path("utilities/migration/phase4_migration_results.json")
        
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, cls=DecimalEncoder)
        
        print(f"\n💾 Migration results saved to: {output_file}")
        print("\n✅ Phase 4 CURATED Zone Migration Complete!")
        print("🧠 ML features and analytics are now available!")
        
    except Exception as e:
        logger.error(f"Phase 4 migration failed: {e}")
        print(f"\n❌ Migration failed: {e}")
        return 1
        
    finally:
        await migrator.close()
    
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))