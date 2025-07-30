"""
Betting Splits Feature Extractor
Extracts unified betting splits features from multi-source data (VSIN, SBD, Action Network)
Sharp action indicators, public sentiment, cross-sportsbook consensus
"""

import logging
from typing import List, Optional, Dict, Any, Set
from datetime import datetime, timedelta
from decimal import Decimal

import polars as pl
import numpy as np

from .models import BettingSplitsFeatures, BaseFeatureExtractor

# Add src to path for imports
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

logger = logging.getLogger(__name__)


class BettingSplitsFeatureExtractor(BaseFeatureExtractor):
    """
    Unified betting splits feature extraction from multiple sources
    Aggregates VSIN sharp action, SBD consensus, and Action Network data
    """
    
    def __init__(self, feature_version: str = "v2.1"):
        super().__init__(feature_version)
        self.min_split_threshold = 1.0  # Minimum percentage difference to consider significant
        self.sharp_action_sources = {'vsin', 'action_network'}  # Sources with reliable sharp indicators
        self.consensus_sources = {'sbd', 'action_network'}  # Sources for consensus analysis
    
    def get_required_columns(self) -> List[str]:
        """Required columns for betting splits feature extraction"""
        return [
            'game_id',
            'data_source',
            'sportsbook_name',
            'sportsbook_id',
            'market_type',
            'bet_percentage_home',
            'bet_percentage_away',
            'money_percentage_home',
            'money_percentage_away',
            'bet_percentage_over',
            'bet_percentage_under',
            'money_percentage_over',
            'money_percentage_under',
            'sharp_action_direction',
            'sharp_action_strength',
            'reverse_line_movement',
            'collected_at',
            'minutes_before_game'
        ]
    
    async def extract_features(
        self, 
        df: pl.DataFrame, 
        game_id: int, 
        cutoff_time: datetime
    ) -> BettingSplitsFeatures:
        """
        Extract unified betting splits features from multi-source data
        
        Args:
            df: Unified betting splits data from multiple sources
            game_id: Game ID for feature extraction
            cutoff_time: Feature cutoff time (60min before game)
        
        Returns:
            BettingSplitsFeatures instance
        """
        try:
            logger.info(f"Extracting betting splits features for game {game_id}")
            
            # Validate data quality
            data_quality = self.validate_data_quality(df, self.get_required_columns())
            if not data_quality['is_valid']:
                logger.warning(f"Betting splits data quality issues for game {game_id}: {data_quality['missing_columns']}")
            
            # Filter data for this game with 60-minute cutoff enforcement
            game_data = df.filter(
                (pl.col('game_id') == game_id) &
                (pl.col('collected_at') <= cutoff_time) &
                (pl.col('minutes_before_game') >= 60)  # Enforce ML cutoff
            )
            
            if game_data.is_empty():
                logger.warning(f"No betting splits data available for game {game_id} before cutoff")
                return self._create_empty_features()
            
            # Extract data source attribution
            source_features = self._extract_source_attribution(game_data)
            
            # Extract aggregated betting splits
            split_aggregates = self._extract_split_aggregates(game_data)
            
            # Extract sharp action indicators
            sharp_features = self._extract_sharp_action_features(game_data)
            
            # Extract public vs sharp divergence
            divergence_features = self._extract_divergence_features(game_data)
            
            # Extract cross-sportsbook consensus
            consensus_features = self._extract_consensus_features(game_data)
            
            # Extract variance metrics
            variance_features = self._extract_variance_features(game_data)
            
            # Extract weighted features (by sportsbook importance)
            weighted_features = self._extract_weighted_features(game_data)
            
            # Extract source-specific highlights
            source_specific_features = self._extract_source_specific_features(game_data)
            
            # Combine all features
            features = BettingSplitsFeatures(
                feature_version=self.feature_version,
                last_updated=cutoff_time,
                **source_features,
                **split_aggregates,
                **sharp_features,
                **divergence_features,
                **consensus_features,
                **variance_features,
                **weighted_features,
                **source_specific_features
            )
            
            logger.info(f"Successfully extracted betting splits features for game {game_id}")
            return features
            
        except Exception as e:
            logger.error(f"Error extracting betting splits features for game {game_id}: {e}")
            raise
    
    def _extract_source_attribution(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Extract data source attribution and coverage information"""
        try:
            # Get unique data sources
            data_sources = df.select('data_source').unique().to_series().to_list()
            data_sources = [source for source in data_sources if source is not None]
            
            # Get unique sportsbooks covered
            sportsbooks = df.select('sportsbook_name').unique().to_series().to_list()
            sportsbooks = [book for book in sportsbooks if book is not None]
            
            return {
                'data_sources': data_sources,
                'sportsbook_coverage': sportsbooks
            }
            
        except Exception as e:
            logger.error(f"Error extracting source attribution: {e}")
            return {'data_sources': [], 'sportsbook_coverage': []}
    
    def _extract_split_aggregates(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Extract aggregated betting splits across all sources"""
        try:
            # Calculate weighted averages of betting splits
            # Weight by recency and data source reliability
            
            # Add time weights (more recent = higher weight)
            max_timestamp = df.select('collected_at').max().item()
            if isinstance(max_timestamp, str):
                max_timestamp = datetime.fromisoformat(max_timestamp.replace('Z', '+00:00'))
            
            weighted_data = df.with_columns([
                ((max_timestamp.timestamp() - pl.col('collected_at').dt.timestamp()) / 3600).alias('hours_ago')
            ]).with_columns([
                (1.0 / (1.0 + pl.col('hours_ago') * 0.1)).alias('time_weight')
            ])
            
            # Calculate weighted averages
            aggregates = weighted_data.select([
                (pl.col('bet_percentage_home') * pl.col('time_weight')).sum() / pl.col('time_weight').sum().alias('avg_bet_home'),
                (pl.col('bet_percentage_away') * pl.col('time_weight')).sum() / pl.col('time_weight').sum().alias('avg_bet_away'),
                (pl.col('money_percentage_home') * pl.col('time_weight')).sum() / pl.col('time_weight').sum().alias('avg_money_home'),
                (pl.col('money_percentage_away') * pl.col('time_weight')).sum() / pl.col('time_weight').sum().alias('avg_money_away'),
                (pl.col('bet_percentage_over') * pl.col('time_weight')).sum() / pl.col('time_weight').sum().alias('avg_bet_over'),
                (pl.col('bet_percentage_under') * pl.col('time_weight')).sum() / pl.col('time_weight').sum().alias('avg_bet_under'),
                (pl.col('money_percentage_over') * pl.col('time_weight')).sum() / pl.col('time_weight').sum().alias('avg_money_over'),
                (pl.col('money_percentage_under') * pl.col('time_weight')).sum() / pl.col('time_weight').sum().alias('avg_money_under')
            ])
            
            if aggregates.is_empty():
                return {}
            
            agg_row = aggregates.row(0, named=True)
            
            return {
                'avg_bet_percentage_home': Decimal(str(agg_row.get('avg_bet_home', 0) or 0)),
                'avg_bet_percentage_away': Decimal(str(agg_row.get('avg_bet_away', 0) or 0)),
                'avg_money_percentage_home': Decimal(str(agg_row.get('avg_money_home', 0) or 0)),
                'avg_money_percentage_away': Decimal(str(agg_row.get('avg_money_away', 0) or 0)),
                'avg_bet_percentage_over': Decimal(str(agg_row.get('avg_bet_over', 0) or 0)),
                'avg_bet_percentage_under': Decimal(str(agg_row.get('avg_bet_under', 0) or 0)),
                'avg_money_percentage_over': Decimal(str(agg_row.get('avg_money_over', 0) or 0)),
                'avg_money_percentage_under': Decimal(str(agg_row.get('avg_money_under', 0) or 0))
            }
            
        except Exception as e:
            logger.error(f"Error extracting split aggregates: {e}")
            return {}
    
    def _extract_sharp_action_features(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Extract sharp action indicators and signals"""
        try:
            # Count sharp action signals from reliable sources
            sharp_data = df.filter(
                pl.col('data_source').is_in(list(self.sharp_action_sources)) &
                pl.col('sharp_action_direction').is_not_null() &
                (pl.col('sharp_action_direction') != 'none')
            )
            
            sharp_signals = sharp_data.height
            
            # Determine overall sharp action strength
            strength_counts = sharp_data.group_by('sharp_action_strength').agg(pl.len().alias('count'))
            dominant_strength = 'weak'
            
            if not strength_counts.is_empty():
                max_strength = strength_counts.sort('count', descending=True).head(1)
                if not max_strength.is_empty():
                    dominant_strength = max_strength.select('sharp_action_strength').item() or 'weak'
            
            # Count reverse line movement instances
            rlm_count = df.filter(pl.col('reverse_line_movement') == True).height
            
            return {
                'sharp_action_signals': sharp_signals,
                'sharp_action_strength': dominant_strength if sharp_signals > 0 else None,
                'reverse_line_movement_count': rlm_count
            }
            
        except Exception as e:
            logger.error(f"Error extracting sharp action features: {e}")
            return {}
    
    def _extract_divergence_features(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Extract public vs sharp money divergence calculations"""
        try:
            # Calculate money % - bet % divergence for each market
            divergence_data = df.filter(
                pl.col('bet_percentage_home').is_not_null() &
                pl.col('money_percentage_home').is_not_null()
            ).select([
                (pl.col('money_percentage_home') - pl.col('bet_percentage_home')).mean().alias('home_divergence'),
                (pl.col('money_percentage_away') - pl.col('bet_percentage_away')).mean().alias('away_divergence'),
                (pl.col('money_percentage_over') - pl.col('bet_percentage_over')).mean().alias('over_divergence'),
                (pl.col('money_percentage_under') - pl.col('bet_percentage_under')).mean().alias('under_divergence')
            ])
            
            if divergence_data.is_empty():
                return {}
            
            div_row = divergence_data.row(0, named=True)
            
            return {
                'home_money_bet_divergence': Decimal(str(div_row.get('home_divergence', 0) or 0)),
                'away_money_bet_divergence': Decimal(str(div_row.get('away_divergence', 0) or 0)),
                'over_money_bet_divergence': Decimal(str(div_row.get('over_divergence', 0) or 0)),
                'under_money_bet_divergence': Decimal(str(div_row.get('under_divergence', 0) or 0))
            }
            
        except Exception as e:
            logger.error(f"Error extracting divergence features: {e}")
            return {}
    
    def _extract_consensus_features(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Extract cross-sportsbook consensus metrics"""
        try:
            # Calculate consensus strength across sportsbooks
            consensus_data = df.group_by(['sportsbook_name', 'market_type']).agg([
                pl.col('bet_percentage_home').mean().alias('book_bet_home'),
                pl.col('money_percentage_home').mean().alias('book_money_home'),
                pl.col('bet_percentage_over').mean().alias('book_bet_over'),
                pl.col('money_percentage_over').mean().alias('book_money_over')
            ])
            
            if consensus_data.is_empty():
                return {}
            
            # Calculate variance across sportsbooks (lower variance = higher consensus)
            consensus_stats = consensus_data.select([
                pl.col('book_bet_home').var().alias('ml_bet_var'),
                pl.col('book_money_home').var().alias('ml_money_var'),
                pl.col('book_bet_over').var().alias('total_bet_var'),
                pl.col('book_money_over').var().alias('total_money_var')
            ])
            
            if consensus_stats.is_empty():
                return {}
            
            stats_row = consensus_stats.row(0, named=True)
            
            # Convert variance to consensus strength (inverse relationship)
            ml_consensus = 1.0 / (1.0 + (stats_row.get('ml_bet_var', 0) or 0))
            spread_consensus = ml_consensus  # Assume similar for spread
            total_consensus = 1.0 / (1.0 + (stats_row.get('total_bet_var', 0) or 0))
            
            return {
                'sportsbook_consensus_ml': Decimal(str(ml_consensus)),
                'sportsbook_consensus_spread': Decimal(str(spread_consensus)),
                'sportsbook_consensus_total': Decimal(str(total_consensus))
            }
            
        except Exception as e:
            logger.error(f"Error extracting consensus features: {e}")
            return {}
    
    def _extract_variance_features(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Extract variance metrics across sportsbooks"""
        try:
            # Calculate variance in betting splits across books
            variance_data = df.group_by('sportsbook_name').agg([
                pl.col('bet_percentage_home').mean().alias('book_bet_home'),
                pl.col('money_percentage_home').mean().alias('book_money_home'),
                pl.col('bet_percentage_over').mean().alias('book_bet_over'),
                pl.col('money_percentage_over').mean().alias('book_money_over')
            ])
            
            if variance_data.is_empty():
                return {}
            
            variances = variance_data.select([
                pl.col('book_bet_home').var().alias('bet_home_var'),
                pl.col('book_money_home').var().alias('money_home_var'),
                pl.col('book_bet_over').var().alias('bet_over_var'),
                pl.col('book_money_over').var().alias('money_over_var')
            ])
            
            if variances.is_empty():
                return {}
            
            var_row = variances.row(0, named=True)
            
            return {
                'bet_percentage_variance_home': Decimal(str(var_row.get('bet_home_var', 0) or 0)),
                'money_percentage_variance_home': Decimal(str(var_row.get('money_home_var', 0) or 0)),
                'bet_percentage_variance_over': Decimal(str(var_row.get('bet_over_var', 0) or 0)),
                'money_percentage_variance_over': Decimal(str(var_row.get('money_over_var', 0) or 0))
            }
            
        except Exception as e:
            logger.error(f"Error extracting variance features: {e}")
            return {}
    
    def _extract_weighted_features(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Extract weighted averages by sportsbook importance/volume"""
        try:
            # Define sportsbook importance weights (based on market share/reliability)
            sportsbook_weights = {
                'draftkings': 1.0,
                'fanduel': 1.0,
                'betmgm': 0.9,
                'caesars': 0.8,
                'circa': 0.7,  # Lower volume but sharp action
                'pinnacle': 0.9,  # Sharp book
                'bet365': 0.8,
                'pointsbet': 0.6,
                'wynnbet': 0.5
            }
            
            # Add weights to data
            weighted_data = df.with_columns([
                pl.col('sportsbook_name').str.to_lowercase().map_elements(
                    lambda x: sportsbook_weights.get(x, 0.3),  # Default weight for unknown books
                    return_dtype=pl.Float64
                ).alias('book_weight')
            ])
            
            # Calculate weighted averages for sharp action and sentiment
            weighted_stats = weighted_data.select([
                # Weighted sharp action score
                (pl.when(pl.col('sharp_action_direction').is_not_null())
                 .then(pl.col('book_weight'))
                 .otherwise(0)).sum() / pl.col('book_weight').sum().alias('weighted_sharp_score'),
                
                # Weighted public sentiment (based on bet percentages)
                (pl.col('bet_percentage_home') * pl.col('book_weight')).sum() / 
                pl.col('book_weight').sum().alias('weighted_public_sentiment')
            ])
            
            if weighted_stats.is_empty():
                return {}
            
            stats_row = weighted_stats.row(0, named=True)
            
            return {
                'weighted_sharp_action_score': Decimal(str(stats_row.get('weighted_sharp_score', 0) or 0)),
                'weighted_public_sentiment': Decimal(str(stats_row.get('weighted_public_sentiment', 50) or 50))
            }
            
        except Exception as e:
            logger.error(f"Error extracting weighted features: {e}")
            return {}
    
    def _extract_source_specific_features(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Extract source-specific highlights and signals"""
        try:
            # VSIN-specific sharp signals
            vsin_data = df.filter(pl.col('data_source') == 'vsin')
            vsin_sharp_signals = vsin_data.filter(
                pl.col('sharp_action_direction').is_not_null() &
                (pl.col('sharp_action_direction') != 'none')
            ).height
            
            # SBD consensus strength (based on number of books)
            sbd_data = df.filter(pl.col('data_source') == 'sbd')
            sbd_book_count = sbd_data.select('sportsbook_name').unique().height
            sbd_consensus_strength = min(1.0, sbd_book_count / 9.0)  # SBD covers 9+ books
            
            # Action Network steam signals (rapid line movements)
            action_network_data = df.filter(pl.col('data_source') == 'action_network')
            action_network_steam = action_network_data.filter(
                pl.col('sharp_action_strength') == 'strong'
            ).height
            
            return {
                'vsin_sharp_signals': vsin_sharp_signals,
                'sbd_consensus_strength': Decimal(str(sbd_consensus_strength)) if sbd_consensus_strength > 0 else None,
                'action_network_steam_signals': action_network_steam
            }
            
        except Exception as e:
            logger.error(f"Error extracting source-specific features: {e}")
            return {}
    
    def _create_empty_features(self) -> BettingSplitsFeatures:
        """Create empty betting splits features when no data is available"""
        return BettingSplitsFeatures(
            feature_version=self.feature_version,
            last_updated=datetime.utcnow()
        )