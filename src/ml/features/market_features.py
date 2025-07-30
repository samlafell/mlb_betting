"""
Market Feature Extractor
Extracts market structure, efficiency, arbitrage opportunities, and consensus features
Uses Polars for high-performance cross-sportsbook analysis
"""

import logging
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timedelta
from decimal import Decimal

import polars as pl
import numpy as np

from .models import MarketFeatures, BaseFeatureExtractor

# Proper package imports

logger = logging.getLogger(__name__)


class MarketFeatureExtractor(BaseFeatureExtractor):
    """
    Market structure and efficiency feature extraction
    Analyzes arbitrage opportunities, steam moves, and market consensus
    """
    
    def __init__(self, feature_version: str = "v2.1"):
        super().__init__(feature_version)
        self.min_arbitrage_threshold = 0.01  # 1% minimum arbitrage opportunity
        self.steam_move_threshold = 10  # Minimum odds change for steam move detection
        self.min_sportsbooks = 3  # Minimum books required for consensus analysis
    
    def get_required_columns(self) -> List[str]:
        """Required columns for market feature extraction"""
        return [
            'game_id',
            'timestamp',
            'sportsbook_name',
            'market_type',
            'home_ml_odds',
            'away_ml_odds',
            'home_spread_line',
            'home_spread_odds',
            'away_spread_odds',
            'total_line',
            'over_odds',
            'under_odds',
            'sharp_action_direction',
            'sharp_action_strength',
            'bet_percentage_home',
            'money_percentage_home',
            'bet_percentage_over',
            'money_percentage_over'
        ]
    
    async def extract_features(
        self, 
        df: pl.DataFrame, 
        game_id: int, 
        cutoff_time: datetime
    ) -> MarketFeatures:
        """
        Extract market structure and efficiency features
        
        Args:
            df: Cross-sportsbook odds and betting data
            game_id: Game ID for feature extraction
            cutoff_time: Feature cutoff time
        
        Returns:
            MarketFeatures instance
        """
        try:
            logger.info(f"Extracting market features for game {game_id}")
            
            # Validate data quality
            data_quality = self.validate_data_quality(df, self.get_required_columns())
            if not data_quality['is_valid']:
                logger.warning(f"Market data quality issues for game {game_id}: {data_quality['missing_columns']}")
            
            # Filter data for this game up to cutoff time
            game_data = df.filter(
                (pl.col('game_id') == game_id) &
                (pl.col('timestamp') <= cutoff_time)
            ).sort('timestamp')
            
            if game_data.is_empty():
                logger.warning(f"No market data available for game {game_id}")
                return self._create_empty_features()
            
            # Extract market efficiency features
            efficiency_features = self._extract_efficiency_features(game_data)
            
            # Extract steam move features
            steam_features = self._extract_steam_move_features(game_data)
            
            # Extract arbitrage features
            arbitrage_features = self._extract_arbitrage_features(game_data)
            
            # Extract sportsbook consensus features
            consensus_features = self._extract_consensus_features(game_data)
            
            # Extract market depth features
            depth_features = self._extract_market_depth_features(game_data)
            
            # Extract line movement patterns
            movement_features = self._extract_line_movement_patterns(game_data)
            
            # Extract sharp vs public indicators
            sharp_public_features = self._extract_sharp_public_indicators(game_data)
            
            # Extract market microstructure features
            microstructure_features = self._extract_microstructure_features(game_data)
            
            # Combine all features
            features = MarketFeatures(
                feature_version=self.feature_version,
                calculation_timestamp=cutoff_time,
                **efficiency_features,
                **steam_features,
                **arbitrage_features,
                **consensus_features,
                **depth_features,
                **movement_features,
                **sharp_public_features,
                **microstructure_features
            )
            
            logger.info(f"Successfully extracted market features for game {game_id}")
            return features
            
        except Exception as e:
            logger.error(f"Error extracting market features for game {game_id}: {e}")
            raise
    
    def _extract_efficiency_features(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Extract market efficiency metrics"""
        try:
            # Calculate line stability (inverse of movement frequency)
            movements = df.group_by(['sportsbook_name', 'market_type']).agg([
                (pl.col('home_ml_odds').diff().abs() > self.steam_move_threshold).sum().alias('ml_movements'),
                (pl.col('home_spread_odds').diff().abs() > self.steam_move_threshold).sum().alias('spread_movements'),
                (pl.col('over_odds').diff().abs() > self.steam_move_threshold).sum().alias('total_movements'),
                pl.col('timestamp').count().alias('data_points')
            ])
            
            # Calculate stability score (fewer movements = more stable)
            stability_score = 1.0
            if not movements.is_empty():
                total_movements = movements.select([
                    (pl.col('ml_movements') + pl.col('spread_movements') + pl.col('total_movements')).sum().alias('total')
                ]).item()
                total_points = movements.select('data_points').sum().item()
                movement_rate = total_movements / max(total_points, 1)
                stability_score = 1.0 / (1.0 + movement_rate)
            
            # Market liquidity score (based on number of sportsbooks and data frequency)
            unique_books = df.select('sportsbook_name').unique().height
            data_frequency = df.height / max((df.select('timestamp').unique().height), 1)
            liquidity_score = min(1.0, (unique_books / 10) * (data_frequency / 60))  # Normalize to 0-1
            
            return {
                'line_stability_score': Decimal(str(stability_score)),
                'market_liquidity_score': Decimal(str(liquidity_score))
            }
            
        except Exception as e:
            logger.error(f"Error extracting efficiency features: {e}")
            return {}
    
    def _extract_steam_move_features(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Extract steam move detection features"""
        try:
            # Detect steam moves (simultaneous large movements across books)
            steam_data = df.sort('timestamp').group_by(['timestamp', 'market_type']).agg([
                (pl.col('home_ml_odds').diff().abs() > self.steam_move_threshold).sum().alias('ml_steam'),
                (pl.col('home_spread_odds').diff().abs() > self.steam_move_threshold).sum().alias('spread_steam'),
                (pl.col('over_odds').diff().abs() > self.steam_move_threshold).sum().alias('total_steam'),
                pl.col('sportsbook_name').unique().alias('books_involved'),
                pl.col('home_ml_odds').diff().abs().max().alias('max_ml_move'),
                pl.col('home_spread_odds').diff().abs().max().alias('max_spread_move'),
                pl.col('over_odds').diff().abs().max().alias('max_total_move')
            ])
            
            # Count steam moves (require at least 3 books moving simultaneously)
            steam_moves = steam_data.filter(
                (pl.col('ml_steam') >= 3) | 
                (pl.col('spread_steam') >= 3) | 
                (pl.col('total_steam') >= 3)
            )
            
            steam_count = steam_moves.height
            
            # Calculate average and largest steam move magnitudes
            avg_magnitude = 0.0
            largest_move = 0.0
            involved_books = []
            
            if not steam_moves.is_empty():
                magnitude_stats = steam_moves.select([
                    ((pl.col('max_ml_move') + pl.col('max_spread_move') + pl.col('max_total_move')) / 3).mean().alias('avg_mag'),
                    (pl.col('max_ml_move').max()).alias('max_ml'),
                    (pl.col('max_spread_move').max()).alias('max_spread'), 
                    (pl.col('max_total_move').max()).alias('max_total')
                ])
                
                if not magnitude_stats.is_empty():
                    stats = magnitude_stats.row(0, named=True)
                    avg_magnitude = stats.get('avg_mag', 0) or 0
                    largest_move = max(
                        stats.get('max_ml', 0) or 0,
                        stats.get('max_spread', 0) or 0,
                        stats.get('max_total', 0) or 0
                    )
                
                # Get unique books involved in steam moves
                all_books = steam_moves.select('books_involved').to_series().explode().unique().to_list()
                involved_books = [book for book in all_books if book is not None]
            
            return {
                'steam_move_indicators': steam_count,
                'steam_move_magnitude': Decimal(str(avg_magnitude)) if avg_magnitude > 0 else None,
                'largest_steam_move': Decimal(str(largest_move)) if largest_move > 0 else None,
                'steam_move_sportsbooks': involved_books
            }
            
        except Exception as e:
            logger.error(f"Error extracting steam move features: {e}")
            return {}
    
    def _extract_arbitrage_features(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Extract arbitrage opportunity features"""
        try:
            # Group by timestamp to find arbitrage opportunities at each point in time
            arbitrage_data = df.group_by('timestamp').agg([
                pl.col('home_ml_odds').min().alias('best_home_ml'),
                pl.col('away_ml_odds').min().alias('best_away_ml'),
                pl.col('home_spread_odds').min().alias('best_home_spread'),
                pl.col('away_spread_odds').min().alias('best_away_spread'),
                pl.col('over_odds').min().alias('best_over'),
                pl.col('under_odds').min().alias('best_under'),
                pl.col('sportsbook_name').count().alias('book_count')
            ]).filter(pl.col('book_count') >= self.min_sportsbooks)
            
            max_ml_arb = 0.0
            max_spread_arb = 0.0
            max_total_arb = 0.0
            arb_duration = 0
            
            if not arbitrage_data.is_empty():
                # Calculate arbitrage opportunities
                arb_calcs = arbitrage_data.with_columns([
                    # ML arbitrage: 1/best_home + 1/best_away < 1
                    (1.0 - (1.0 / pl.col('best_home_ml') + 1.0 / pl.col('best_away_ml'))).alias('ml_arb'),
                    # Spread arbitrage
                    (1.0 - (1.0 / pl.col('best_home_spread') + 1.0 / pl.col('best_away_spread'))).alias('spread_arb'),
                    # Total arbitrage
                    (1.0 - (1.0 / pl.col('best_over') + 1.0 / pl.col('best_under'))).alias('total_arb')
                ]).filter(
                    (pl.col('ml_arb') > self.min_arbitrage_threshold) |
                    (pl.col('spread_arb') > self.min_arbitrage_threshold) |
                    (pl.col('total_arb') > self.min_arbitrage_threshold)
                )
                
                if not arb_calcs.is_empty():
                    # Get maximum arbitrage opportunities
                    max_arbs = arb_calcs.select([
                        pl.col('ml_arb').max().alias('max_ml'),
                        pl.col('spread_arb').max().alias('max_spread'),
                        pl.col('total_arb').max().alias('max_total'),
                        pl.col('timestamp').count().alias('arb_count')
                    ])
                    
                    if not max_arbs.is_empty():
                        arb_row = max_arbs.row(0, named=True)
                        max_ml_arb = arb_row.get('max_ml', 0) or 0
                        max_spread_arb = arb_row.get('max_spread', 0) or 0
                        max_total_arb = arb_row.get('max_total', 0) or 0
                        arb_duration = arb_row.get('arb_count', 0) or 0
            
            return {
                'max_ml_arbitrage_opportunity': Decimal(str(max_ml_arb)) if max_ml_arb > 0 else None,
                'max_spread_arbitrage_opportunity': Decimal(str(max_spread_arb)) if max_spread_arb > 0 else None,
                'max_total_arbitrage_opportunity': Decimal(str(max_total_arb)) if max_total_arb > 0 else None,
                'arbitrage_duration_minutes': arb_duration if arb_duration > 0 else None
            }
            
        except Exception as e:
            logger.error(f"Error extracting arbitrage features: {e}")
            return {}
    
    def _extract_consensus_features(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Extract sportsbook consensus and coverage features"""
        try:
            # Get participating sportsbooks
            participating_books = df.select('sportsbook_name').unique().to_series().to_list()
            participating_books = [book for book in participating_books if book is not None]
            
            sportsbook_count = len(participating_books)
            
            # Calculate consensus strength (inverse of variance across books at each timestamp)
            consensus_data = df.group_by('timestamp').agg([
                pl.col('home_ml_odds').var().alias('ml_var'),
                pl.col('home_spread_line').var().alias('spread_var'),
                pl.col('total_line').var().alias('total_var'),
                pl.col('sportsbook_name').count().alias('book_count')
            ]).filter(pl.col('book_count') >= self.min_sportsbooks)
            
            consensus_strength = 0.0
            if not consensus_data.is_empty():
                avg_variances = consensus_data.select([
                    pl.col('ml_var').mean().alias('avg_ml_var'),
                    pl.col('spread_var').mean().alias('avg_spread_var'),
                    pl.col('total_var').mean().alias('avg_total_var')
                ])
                
                if not avg_variances.is_empty():
                    var_row = avg_variances.row(0, named=True)
                    avg_variance = np.mean([
                        var_row.get('avg_ml_var', 0) or 0,
                        var_row.get('avg_spread_var', 0) or 0,
                        var_row.get('avg_total_var', 0) or 0
                    ])
                    consensus_strength = 1.0 / (1.0 + avg_variance)
            
            return {
                'participating_sportsbooks': participating_books,
                'sportsbook_count': sportsbook_count,
                'sportsbook_consensus_strength': Decimal(str(consensus_strength)) if consensus_strength > 0 else None
            }
            
        except Exception as e:
            logger.error(f"Error extracting consensus features: {e}")
            return {}
    
    def _extract_market_depth_features(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Extract market depth and spread features"""
        try:
            # Calculate best spreads at each timestamp
            depth_data = df.group_by('timestamp').agg([
                (pl.col('home_ml_odds').max() - pl.col('home_ml_odds').min()).alias('ml_spread'),
                (pl.col('over_odds').max() - pl.col('under_odds').min()).alias('total_spread')
            ])
            
            best_ml_spread = None
            best_total_spread = None
            
            if not depth_data.is_empty():
                spreads = depth_data.select([
                    pl.col('ml_spread').min().alias('best_ml'),
                    pl.col('total_spread').min().alias('best_total')
                ])
                
                if not spreads.is_empty():
                    spread_row = spreads.row(0, named=True)
                    best_ml_spread = int(spread_row.get('best_ml', 0) or 0)
                    best_total_spread = int(spread_row.get('best_total', 0) or 0)
            
            # Calculate odds efficiency score (based on spread tightness)
            efficiency_score = 0.0
            if best_ml_spread is not None and best_total_spread is not None:
                # Lower spreads = higher efficiency
                avg_spread = (best_ml_spread + best_total_spread) / 2
                efficiency_score = 1.0 / (1.0 + avg_spread / 100)  # Normalize spread impact
            
            return {
                'best_ml_spread': best_ml_spread,
                'best_total_spread': best_total_spread,
                'odds_efficiency_score': Decimal(str(efficiency_score)) if efficiency_score > 0 else None
            }
            
        except Exception as e:
            logger.error(f"Error extracting market depth features: {e}")
            return {}
    
    def _extract_line_movement_patterns(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Extract line movement pattern features"""
        try:
            # Count significant line movements
            movements = df.sort('timestamp').group_by(['sportsbook_name', 'market_type']).agg([
                (pl.col('home_ml_odds').diff().abs() > self.steam_move_threshold).sum().alias('ml_moves'),
                (pl.col('home_spread_line').diff().abs() > 0.5).sum().alias('spread_moves'),
                (pl.col('total_line').diff().abs() > 0.5).sum().alias('total_moves'),
                pl.col('home_ml_odds').diff().abs().mean().alias('avg_ml_move'),
                pl.col('timestamp').count().alias('data_points')
            ])
            
            total_movements = 0
            avg_magnitude = 0.0
            movement_frequency = 0.0
            
            if not movements.is_empty():
                totals = movements.select([
                    (pl.col('ml_moves') + pl.col('spread_moves') + pl.col('total_moves')).sum().alias('total'),
                    pl.col('avg_ml_move').mean().alias('avg_mag'),
                    pl.col('data_points').sum().alias('total_points')
                ])
                
                if not totals.is_empty():
                    total_row = totals.row(0, named=True)
                    total_movements = total_row.get('total', 0) or 0
                    avg_magnitude = total_row.get('avg_mag', 0) or 0
                    total_points = total_row.get('total_points', 0) or 1
                    
                    # Calculate movement frequency (movements per hour)
                    time_span = df.select((pl.col('timestamp').max() - pl.col('timestamp').min()).dt.total_seconds() / 3600).item()
                    movement_frequency = total_movements / max(time_span, 1) if time_span > 0 else 0
            
            # Check for late movement (movements in final hour)
            final_hour = df.select('timestamp').max().item() - timedelta(hours=1)
            late_movements = df.filter(pl.col('timestamp') >= final_hour)
            late_movement_indicator = not late_movements.is_empty() and late_movements.height > df.height * 0.2
            
            return {
                'total_line_movements': total_movements,
                'average_movement_magnitude': Decimal(str(avg_magnitude)) if avg_magnitude > 0 else None,
                'movement_frequency': Decimal(str(movement_frequency)) if movement_frequency > 0 else None,
                'late_movement_indicator': late_movement_indicator
            }
            
        except Exception as e:
            logger.error(f"Error extracting line movement patterns: {e}")
            return {}
    
    def _extract_sharp_public_indicators(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Extract sharp vs public divergence indicators"""
        try:
            # Calculate divergence in preferences
            sharp_data = df.filter(
                pl.col('sharp_action_direction').is_not_null() &
                (pl.col('sharp_action_direction') != 'none')
            )
            
            # Count sharp signals by direction
            sharp_home = sharp_data.filter(pl.col('sharp_action_direction') == 'home').height
            sharp_away = sharp_data.filter(pl.col('sharp_action_direction') == 'away').height
            sharp_over = sharp_data.filter(pl.col('sharp_action_direction') == 'over').height
            sharp_under = sharp_data.filter(pl.col('sharp_action_direction') == 'under').height
            
            # Calculate public preferences (based on bet percentages)
            public_data = df.filter(
                pl.col('bet_percentage_home').is_not_null()
            )
            
            ml_divergence = None
            spread_divergence = None  
            total_divergence = None
            
            if not public_data.is_empty():
                public_stats = public_data.select([
                    pl.col('bet_percentage_home').mean().alias('avg_home_bet'),
                    pl.col('bet_percentage_over').mean().alias('avg_over_bet')
                ])
                
                if not public_stats.is_empty():
                    stats = public_stats.row(0, named=True)
                    avg_home_bet = stats.get('avg_home_bet', 50) or 50
                    avg_over_bet = stats.get('avg_over_bet', 50) or 50
                    
                    # Calculate divergence (sharp preference - public preference)
                    sharp_home_pct = (sharp_home / max(sharp_home + sharp_away, 1)) * 100
                    sharp_over_pct = (sharp_over / max(sharp_over + sharp_under, 1)) * 100
                    
                    ml_divergence = sharp_home_pct - avg_home_bet
                    total_divergence = sharp_over_pct - avg_over_bet
                    spread_divergence = ml_divergence  # Assume similar to ML for now
            
            return {
                'sharp_public_divergence_ml': Decimal(str(ml_divergence)) if ml_divergence is not None else None,
                'sharp_public_divergence_spread': Decimal(str(spread_divergence)) if spread_divergence is not None else None,
                'sharp_public_divergence_total': Decimal(str(total_divergence)) if total_divergence is not None else None
            }
            
        except Exception as e:
            logger.error(f"Error extracting sharp vs public indicators: {e}")
            return {}
    
    def _extract_microstructure_features(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Extract market microstructure features"""
        try:
            # Estimate bid-ask spread based on odds variance
            spread_data = df.group_by('timestamp').agg([
                (pl.col('home_ml_odds').max() - pl.col('home_ml_odds').min()).alias('ml_spread'),
                (pl.col('over_odds').max() - pl.col('under_odds').min()).alias('total_spread')
            ])
            
            bid_ask_estimate = None
            if not spread_data.is_empty():
                avg_spread = spread_data.select([
                    ((pl.col('ml_spread') + pl.col('total_spread')) / 2).mean().alias('avg_spread')
                ]).item()
                bid_ask_estimate = avg_spread or 0
            
            # Market maker vs flow indicator (stability vs movement)
            movements = df.sort('timestamp').select([
                pl.col('home_ml_odds').diff().abs().sum().alias('total_movement')
            ]).item()
            
            data_points = df.height
            stability_ratio = 1.0 / (1.0 + (movements or 0) / max(data_points, 1))
            
            return {
                'bid_ask_spread_estimate': Decimal(str(bid_ask_estimate)) if bid_ask_estimate is not None else None,
                'market_maker_vs_flow': Decimal(str(stability_ratio))
            }
            
        except Exception as e:
            logger.error(f"Error extracting microstructure features: {e}")
            return {}
    
    def _create_empty_features(self) -> MarketFeatures:
        """Create empty market features when no data is available"""
        return MarketFeatures(
            feature_version=self.feature_version,
            calculation_timestamp=datetime.utcnow()
        )