"""
Betting Signal Repository

Repository for fetching and analyzing betting signal data from the database.
This service provides data for various betting signal processors and strategies.

🚀 PHASE 2B: Updated to use table registry for dynamic table resolution
"""

from datetime import datetime
from typing import Any

import structlog

from ..db.connection import get_db_manager
from ..db.table_registry import get_table_registry

logger = structlog.get_logger(__name__)


class BettingSignalRepository:
    """Repository for betting signal data operations."""

    def __init__(self, config: dict | None = None):
        """Initialize the repository with optional configuration."""
        self.coordinator = get_db_manager()
        self.config = config or {}
        self.logger = logger.bind(service="betting_signal_repository")

        # 🚀 PHASE 2B: Initialize table registry for dynamic table resolution
        self.table_registry = get_table_registry()

        # Performance tracking
        self._call_stats = {"database_calls": 0, "cache_hits": 0, "total_calls": 0}

        # Simple in-memory cache
        self._cache = {}
        self._cache_ttl = 300  # 5 minutes

        self.logger.info(
            "🚀 BettingSignalRepository initialized with table registry support"
        )

    def _get_cache_key(self, method_name: str, *args) -> str:
        """Generate cache key for method and arguments."""
        return f"{method_name}_{hash(str(args))}"

    def _get_from_cache(self, cache_key: str) -> Any | None:
        """Get data from cache if not expired."""
        if cache_key in self._cache:
            data, timestamp = self._cache[cache_key]
            if datetime.now().timestamp() - timestamp < self._cache_ttl:
                self._call_stats["cache_hits"] += 1
                return data
            else:
                del self._cache[cache_key]
        return None

    def _set_cache(self, cache_key: str, data: Any) -> None:
        """Set data in cache with current timestamp."""
        self._cache[cache_key] = (data, datetime.now().timestamp())

    async def get_sharp_signal_data(
        self, start_time: datetime, end_time: datetime
    ) -> list[dict]:
        """
        Get raw sharp signal data for analysis

        🚀 ENHANCED: Added intelligent caching to reduce database calls
        🚀 PHASE 2B: Updated to use table registry for table resolution
        """
        # Check cache first
        cache_key = self._get_cache_key("get_sharp_signal_data", start_time, end_time)
        cached_data = self._get_from_cache(cache_key)
        if cached_data is not None:
            return cached_data

        # 🚀 PERFORMANCE LOG: Track database calls
        self._call_stats["database_calls"] += 1
        self.logger.info(
            f"🗄️  Fetching sharp signal data from database (time window: {start_time.strftime('%H:%M')} to {end_time.strftime('%H:%M')})"
        )

        # 🚀 PHASE 2B: Get table name from registry
        raw_splits_table = self.table_registry.get_table("raw_betting_splits")

        query = f"""
        WITH valid_splits AS (
            SELECT 
                home_team, away_team, split_type, split_value,
                home_or_over_stake_percentage, home_or_over_bets_percentage,
                (home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
                source, book, game_datetime, last_updated
            FROM {raw_splits_table}
            WHERE game_datetime BETWEEN %s AND %s
              AND home_or_over_stake_percentage IS NOT NULL 
              AND home_or_over_bets_percentage IS NOT NULL
              AND game_datetime IS NOT NULL
              AND NOT (home_or_over_stake_percentage = 0 AND home_or_over_bets_percentage = 0)
              AND ABS(home_or_over_stake_percentage - home_or_over_bets_percentage) >= %s
              AND last_updated >= NOW() - INTERVAL '24 hours'
        ),
        latest_splits AS (
            SELECT 
                home_team, away_team, split_type, split_value,
                home_or_over_stake_percentage, home_or_over_bets_percentage,
                differential, source, book, game_datetime, last_updated,
                ROW_NUMBER() OVER (
                    PARTITION BY home_team, away_team, game_datetime, split_type, source, COALESCE(book, 'UNKNOWN')
                    ORDER BY last_updated DESC
                ) as rn
            FROM valid_splits
        )
        SELECT home_team, away_team, split_type, split_value, 
               home_or_over_stake_percentage, home_or_over_bets_percentage,
               differential, source, book, game_datetime, last_updated
        FROM latest_splits
        WHERE rn = 1
        ORDER BY ABS(differential) DESC
        """

        min_differential = self.config.get("minimum_differential", 10.0)

        results = self.coordinator.execute_query(
            query, (start_time, end_time, min_differential)
        )

        # Cache results
        self._set_cache(cache_key, results)

        return results or []

    async def get_betting_signal_data(
        self, start_time: datetime, end_time: datetime
    ) -> list[dict]:
        """
        Get comprehensive betting signal data for all signal types

        🚀 PHASE 2B: Updated to use table registry for table resolution
        """
        min_differential = self.config.get("minimum_differential", 10.0)

        # 🚀 PHASE 2B: Get table name from registry
        raw_splits_table = self.table_registry.get_table("raw_betting_splits")

        # Single optimized query for all signal types
        query = f"""
        WITH valid_splits AS (
            SELECT 
                home_team, away_team, split_type, split_value,
                home_or_over_stake_percentage, home_or_over_bets_percentage,
                (home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
                source, book, game_datetime, last_updated
            FROM {raw_splits_table}
            WHERE game_datetime BETWEEN %s AND %s
              AND home_or_over_stake_percentage IS NOT NULL 
              AND home_or_over_bets_percentage IS NOT NULL
              AND game_datetime IS NOT NULL
              AND NOT (home_or_over_stake_percentage = 0 AND home_or_over_bets_percentage = 0)
              AND last_updated >= NOW() - INTERVAL '24 hours'
        ),
        latest_splits AS (
            SELECT 
                home_team, away_team, split_type, split_value,
                home_or_over_stake_percentage, home_or_over_bets_percentage,
                differential, source, book, game_datetime, last_updated,
                ROW_NUMBER() OVER (
                    PARTITION BY home_team, away_team, game_datetime, split_type, source, COALESCE(book, 'UNKNOWN')
                    ORDER BY last_updated DESC
                ) as rn
            FROM valid_splits
        ),
        clean_data AS (
            SELECT * FROM latest_splits WHERE rn = 1
        )
        SELECT 
            home_team, away_team, split_type, split_value,
            home_or_over_stake_percentage, home_or_over_bets_percentage,
            differential, source, book, game_datetime, last_updated,
            ABS(differential) as abs_differential,
            CASE WHEN differential > 0 THEN home_team ELSE away_team END as recommended_team
        FROM clean_data
        WHERE ABS(differential) >= %s
        ORDER BY split_type, ABS(differential) DESC
        """

        return (
            self.coordinator.execute_query(
                query, (start_time, end_time, min_differential)
            )
            or []
        )

    async def get_line_movement_data(
        self, start_time: datetime, end_time: datetime
    ) -> list[dict]:
        """Get line movement data for line movement processor"""
        # 🚀 PHASE 2B: Get table name from registry
        raw_splits_table = self.table_registry.get_table("raw_betting_splits")

        query = f"""
        WITH time_ordered_splits AS (
            SELECT 
                home_team, away_team, split_type, split_value,
                home_or_over_stake_percentage, home_or_over_bets_percentage,
                (home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
                source, book, game_datetime, last_updated,
                LAG(split_value) OVER (
                    PARTITION BY home_team, away_team, game_datetime, split_type, source, book
                    ORDER BY last_updated
                ) as prev_split_value,
                LAG(home_or_over_stake_percentage) OVER (
                    PARTITION BY home_team, away_team, game_datetime, split_type, source, book
                    ORDER BY last_updated
                ) as prev_stake_pct
            FROM {raw_splits_table}
            WHERE game_datetime BETWEEN %s AND %s
              AND home_or_over_stake_percentage IS NOT NULL 
              AND home_or_over_bets_percentage IS NOT NULL
              AND split_value IS NOT NULL
              AND game_datetime IS NOT NULL
              AND last_updated >= NOW() - INTERVAL '24 hours'
        )
        SELECT home_team, away_team, split_type, split_value, prev_split_value,
               home_or_over_stake_percentage, prev_stake_pct, differential, 
               source, book, game_datetime, last_updated
        FROM time_ordered_splits
        WHERE prev_split_value IS NOT NULL
          AND split_value != prev_split_value
        ORDER BY ABS(differential) DESC
        """

        return self.coordinator.execute_query(query, (start_time, end_time)) or []

    async def get_opposing_markets_data(
        self, start_time: datetime, end_time: datetime
    ) -> list[dict]:
        """Get opposing markets data where ML and spread signals conflict"""
        # 🚀 PHASE 2B: Get table name from registry
        raw_splits_table = self.table_registry.get_table("raw_betting_splits")

        query = f"""
        WITH latest_splits AS (
            SELECT 
                home_team, away_team, split_type, split_value,
                home_or_over_stake_percentage, home_or_over_bets_percentage,
                (home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
                source, book, game_datetime, last_updated,
                ROW_NUMBER() OVER (
                    PARTITION BY home_team, away_team, game_datetime, split_type, source, COALESCE(book, 'UNKNOWN')
                    ORDER BY last_updated DESC
                ) as rn
            FROM {raw_splits_table}
            WHERE game_datetime BETWEEN %s AND %s
              AND home_or_over_stake_percentage IS NOT NULL 
              AND home_or_over_bets_percentage IS NOT NULL
              AND split_type IN ('moneyline', 'spread')
              AND NOT (home_or_over_stake_percentage = 0 AND home_or_over_bets_percentage = 0)
              AND last_updated >= NOW() - INTERVAL '24 hours'
        ),
        clean_splits AS (
            SELECT * FROM latest_splits WHERE rn = 1
        ),
        ml_signals AS (
            SELECT 
                home_team, away_team, game_datetime, source, book,
                differential as ml_diff, 
                home_or_over_stake_percentage as ml_stake_pct, 
                home_or_over_bets_percentage as ml_bet_pct,
                CASE WHEN differential > 0 THEN home_team ELSE away_team END as ml_rec_team,
                ABS(differential) as ml_strength, last_updated
            FROM clean_splits 
            WHERE split_type = 'moneyline'
              AND ABS(differential) >= 5
              AND ABS(differential) <= %s
        ),
        spread_signals AS (
            SELECT 
                home_team, away_team, game_datetime, source, book,
                differential as spread_diff, 
                home_or_over_stake_percentage as spread_stake_pct, 
                home_or_over_bets_percentage as spread_bet_pct,
                CASE WHEN differential > 0 THEN home_team ELSE away_team END as spread_rec_team,
                ABS(differential) as spread_strength, last_updated
            FROM clean_splits 
            WHERE split_type = 'spread'
              AND ABS(differential) >= 8
              AND ABS(differential) <= %s
        )
        SELECT 
            ml.home_team, ml.away_team, ml.game_datetime, ml.source, ml.book,
            ml.ml_rec_team, ml.ml_diff, ml.ml_strength, ml.ml_stake_pct, ml.ml_bet_pct,
            sp.spread_rec_team, sp.spread_diff, sp.spread_strength, sp.spread_stake_pct, sp.spread_bet_pct,
            (ml.ml_strength + sp.spread_strength) / 2 as combined_strength,
            ABS(ml.ml_diff - sp.spread_diff) as opposition_strength,
            CASE 
                WHEN ml.ml_strength > sp.spread_strength THEN 'ML_STRONGER'
                WHEN sp.spread_strength > ml.ml_strength THEN 'SPREAD_STRONGER'
                ELSE 'EQUAL'
            END as dominant_market,
            ml.last_updated,
            (100 - ml.ml_stake_pct) as ml_opposing_stake_pct,
            (100 - ml.ml_bet_pct) as ml_opposing_bet_pct,
            (100 - sp.spread_stake_pct) as spread_opposing_stake_pct,
            (100 - sp.spread_bet_pct) as spread_opposing_bet_pct
        FROM ml_signals ml
        INNER JOIN spread_signals sp ON ml.home_team = sp.home_team AND ml.away_team = sp.away_team 
            AND ml.game_datetime = sp.game_datetime AND ml.source = sp.source AND ml.book = sp.book
        WHERE ml.ml_rec_team != sp.spread_rec_team
        ORDER BY combined_strength DESC
        """

        max_differential = self.config.get("max_opposing_differential", 40.0)

        return (
            self.coordinator.execute_query(
                query, (start_time, end_time, max_differential, max_differential)
            )
            or []
        )

    async def get_book_conflicts_data(
        self, start_time: datetime, end_time: datetime
    ) -> list[dict]:
        """Get book conflicts data where different books have opposing signals"""
        # 🚀 PHASE 2B: Get table name from registry
        raw_splits_table = self.table_registry.get_table("raw_betting_splits")

        query = f"""
        WITH latest_splits AS (
            SELECT 
                home_team, away_team, split_type, split_value,
                home_or_over_stake_percentage, home_or_over_bets_percentage,
                (home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
                source, book, game_datetime, last_updated,
                ROW_NUMBER() OVER (
                    PARTITION BY home_team, away_team, game_datetime, split_type, source, book
                    ORDER BY last_updated DESC
                ) as rn
            FROM {raw_splits_table}
            WHERE game_datetime BETWEEN %s AND %s
              AND home_or_over_stake_percentage IS NOT NULL 
              AND home_or_over_bets_percentage IS NOT NULL
              AND book IS NOT NULL
              AND book != 'UNKNOWN'
              AND NOT (home_or_over_stake_percentage = 0 AND home_or_over_bets_percentage = 0)
              AND last_updated >= NOW() - INTERVAL '24 hours'
        )
        SELECT 
            ls1.home_team, ls1.away_team, ls1.game_datetime, ls1.split_type, ls1.source,
            ls1.book as book1, ls1.differential as diff1, ls1.split_value as value1,
            ls2.book as book2, ls2.differential as diff2, ls2.split_value as value2,
            ABS(ls1.differential - ls2.differential) as conflict_strength,
            CASE 
                WHEN ABS(ls1.differential) > ABS(ls2.differential) THEN ls1.book
                ELSE ls2.book
            END as stronger_book,
            CASE 
                WHEN ls1.differential > 0 THEN ls1.home_team ELSE ls1.away_team
            END as book1_rec_team,
            CASE 
                WHEN ls2.differential > 0 THEN ls2.home_team ELSE ls2.away_team
            END as book2_rec_team,
            ls1.last_updated
        FROM latest_splits ls1
        INNER JOIN latest_splits ls2 ON ls1.home_team = ls2.home_team 
            AND ls1.away_team = ls2.away_team
            AND ls1.game_datetime = ls2.game_datetime
            AND ls1.split_type = ls2.split_type
            AND ls1.source = ls2.source
            AND ls1.book < ls2.book  -- Avoid duplicates
        WHERE ls1.rn = 1 AND ls2.rn = 1
          AND ABS(ls1.differential) >= %s
          AND ABS(ls2.differential) >= %s
          AND SIGN(ls1.differential) != SIGN(ls2.differential)  -- Opposing signals
        ORDER BY conflict_strength DESC
        """

        min_differential = self.config.get("minimum_differential", 10.0)

        return (
            self.coordinator.execute_query(
                query, (start_time, end_time, min_differential, min_differential)
            )
            or []
        )

    async def get_strategy_performance_data(self) -> list[dict]:
        """Get latest strategy performance data for validation"""
        # 🚀 PHASE 2B: Get table name from registry
        strategy_performance_table = self.table_registry.get_table(
            "strategy_performance"
        )

        query = f"""
        SELECT strategy_name, win_rate, roi, total_bets, confidence_score, last_updated
        FROM {strategy_performance_table}
        WHERE backtest_date = (SELECT MAX(backtest_date) FROM {strategy_performance_table})
        ORDER BY roi DESC
        """

        return self.coordinator.execute_query(query, []) or []

    async def get_data_quality_metrics(self) -> dict[str, Any]:
        """Get data quality metrics for monitoring"""
        # 🚀 PHASE 2B: Get table name from registry
        raw_splits_table = self.table_registry.get_table("raw_betting_splits")

        # Get basic counts and quality metrics
        queries = {
            "total_records": f"SELECT COUNT(*) FROM {raw_splits_table}",
            "recent_records": f"""
                SELECT COUNT(*) FROM {raw_splits_table}
                WHERE last_updated >= NOW() - INTERVAL '24 hours'
            """,
            "null_percentages": f"""
                SELECT COUNT(*) FROM {raw_splits_table}
                WHERE home_or_over_stake_percentage IS NULL 
                   OR home_or_over_bets_percentage IS NULL
            """,
            "zero_values": f"""
                SELECT COUNT(*) FROM {raw_splits_table}
                WHERE home_or_over_stake_percentage = 0 
                  AND home_or_over_bets_percentage = 0
            """,
            "sources": f"""
                SELECT source, COUNT(*) as count 
                FROM {raw_splits_table}
                WHERE last_updated >= NOW() - INTERVAL '24 hours'
                GROUP BY source
            """,
            "split_types": f"""
                SELECT split_type, COUNT(*) as count 
                FROM {raw_splits_table}
                WHERE last_updated >= NOW() - INTERVAL '24 hours'
                GROUP BY split_type
            """,
        }

        metrics = {}
        for metric_name, query in queries.items():
            try:
                result = self.coordinator.execute_query(query, [])
                if metric_name in ["sources", "split_types"]:
                    metrics[metric_name] = (
                        {row[0]: row[1] for row in result} if result else {}
                    )
                else:
                    metrics[metric_name] = result[0][0] if result and result[0] else 0
            except Exception as e:
                self.logger.error(f"Failed to get metric {metric_name}", error=str(e))
                metrics[metric_name] = 0

        return metrics

    async def get_public_betting_data(
        self, start_time: datetime, end_time: datetime
    ) -> list[dict]:
        """Get public betting data for fade opportunities"""
        # 🚀 PHASE 2B: Get table name from registry
        raw_splits_table = self.table_registry.get_table("raw_betting_splits")

        query = f"""
        WITH latest_splits AS (
            SELECT 
                game_id, home_team, away_team, split_type, split_value,
                home_or_over_stake_percentage, home_or_over_bets_percentage,
                (home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
                source, COALESCE(book, 'UNKNOWN') as book, game_datetime, last_updated,
                ROW_NUMBER() OVER (
                    PARTITION BY game_id, split_type, source, COALESCE(book, 'UNKNOWN')
                    ORDER BY last_updated DESC
                ) as rn
            FROM {raw_splits_table}
            WHERE game_datetime BETWEEN %s AND %s
              AND home_or_over_stake_percentage IS NOT NULL 
              AND home_or_over_bets_percentage IS NOT NULL
              AND EXTRACT('epoch' FROM (game_datetime - last_updated)) / 60 BETWEEN -5 AND 45
        )
        SELECT game_id, home_team, away_team, split_type, split_value,
               home_or_over_stake_percentage, home_or_over_bets_percentage,
               differential, source, book, game_datetime, last_updated,
               -- Public betting indicators (calculated but not filtered)
               CASE 
                   WHEN home_or_over_bets_percentage > 70 THEN 'HEAVY_PUBLIC_HOME_OVER'
                   WHEN home_or_over_bets_percentage < 30 THEN 'HEAVY_PUBLIC_AWAY_UNDER'
                   ELSE 'BALANCED'
               END as public_tendency,
               -- Fade opportunity strength (calculated but not filtered)
               CASE 
                   WHEN home_or_over_bets_percentage > 75 OR home_or_over_bets_percentage < 25 THEN 'HIGH_FADE'
                   WHEN home_or_over_bets_percentage > 70 OR home_or_over_bets_percentage < 30 THEN 'MODERATE_FADE'
                   ELSE 'LOW_FADE'
               END as fade_strength
        FROM latest_splits
        WHERE rn = 1
        ORDER BY ABS(differential) DESC
        """

        return self.coordinator.execute_query(query, (start_time, end_time)) or []

    async def get_underdog_value_data(
        self, start_time: datetime, end_time: datetime
    ) -> list[dict]:
        """Get underdog value data for underdog value processor"""
        # 🚀 PHASE 2B: Get table name from registry
        raw_splits_table = self.table_registry.get_table("raw_betting_splits")

        query = f"""
        WITH latest_moneyline AS (
            SELECT 
                home_team, away_team, split_type, split_value,
                home_or_over_stake_percentage as home_stake_pct,
                home_or_over_bets_percentage as home_bet_pct,
                (home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
                source, book, game_datetime, last_updated,
                ROW_NUMBER() OVER (
                    PARTITION BY home_team, away_team, game_datetime, source, COALESCE(book, 'UNKNOWN')
                    ORDER BY last_updated DESC
                ) as rn
            FROM {raw_splits_table}
            WHERE game_datetime BETWEEN %s AND %s
              AND split_type = 'moneyline' 
              AND home_or_over_stake_percentage IS NOT NULL 
              AND home_or_over_bets_percentage IS NOT NULL
              AND game_datetime IS NOT NULL
              AND split_value IS NOT NULL
              AND split_value != '{{}}'
              AND NOT (home_or_over_stake_percentage = 0 AND home_or_over_bets_percentage = 0)
              AND last_updated >= NOW() - INTERVAL '24 hours'
        )
        SELECT home_team, away_team, split_type, split_value,
               home_stake_pct, home_bet_pct, differential, source, book,
               game_datetime, last_updated
        FROM latest_moneyline
        WHERE rn = 1
        ORDER BY ABS(differential) DESC
        """

        return self.coordinator.execute_query(query, (start_time, end_time)) or []

    async def get_hybrid_sharp_data(
        self, start_time: datetime, end_time: datetime
    ) -> list[dict]:
        """Get hybrid sharp data with line movement calculations"""
        # Ensure minimum_differential has a default value
        min_diff = getattr(self.config, "minimum_differential", 10.0)

        # 🚀 PHASE 2B: Get table names from registry
        raw_splits_table = self.table_registry.get_table("raw_betting_splits")
        game_outcomes_table = self.table_registry.get_table("game_outcomes")

        query = rf"""
        WITH comprehensive_data AS (
            SELECT 
                rmbs.game_id,
                rmbs.source,
                COALESCE(rmbs.book, 'UNKNOWN') as book,
                rmbs.split_type,
                rmbs.home_team,
                rmbs.away_team,
                rmbs.game_datetime,
                rmbs.last_updated,
                rmbs.split_value,
                rmbs.home_or_over_stake_percentage as stake_pct,
                rmbs.home_or_over_bets_percentage as bet_pct,
                rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage as differential,
                
                -- Extract line values from JSON using PostgreSQL JSONB operators with safe casting
                CASE 
                    WHEN rmbs.split_type = 'moneyline' AND rmbs.split_value LIKE '{{%}}' THEN
                        CASE WHEN (rmbs.split_value::JSONB->>'home') ~ '^-?[0-9]+\.?[0-9]*$' 
                             THEN (rmbs.split_value::JSONB->>'home')::DOUBLE PRECISION 
                             ELSE NULL END
                    WHEN rmbs.split_type IN ('spread', 'total') AND rmbs.split_value ~ '^-?[0-9]+\.?[0-9]*$' THEN
                        rmbs.split_value::DOUBLE PRECISION
                    ELSE NULL
                END as current_line,
                
                -- Get outcomes for completed games
                go.home_win,
                go.over,
                go.home_cover_spread
                
            FROM {raw_splits_table} rmbs
            LEFT JOIN {game_outcomes_table} go ON rmbs.game_id = go.game_id
            WHERE rmbs.game_datetime BETWEEN %s AND %s
              AND rmbs.home_or_over_stake_percentage IS NOT NULL 
              AND rmbs.home_or_over_bets_percentage IS NOT NULL
              AND rmbs.game_datetime IS NOT NULL
              AND rmbs.last_updated >= NOW() - INTERVAL '48 hours'
        ),
        
        latest_data AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY game_id, split_type, source, book
                    ORDER BY last_updated DESC
                ) as rn
            FROM comprehensive_data
        ),
        
        line_movement_data AS (
            SELECT *,
                LAG(current_line) OVER (
                    PARTITION BY game_id, split_type, source, book
                    ORDER BY last_updated
                ) as previous_line,
                LAG(stake_pct) OVER (
                    PARTITION BY game_id, split_type, source, book
                    ORDER BY last_updated
                ) as previous_stake_pct
            FROM latest_data
            WHERE rn <= 5  -- Keep last 5 updates for line movement analysis
        )
        
        SELECT 
            game_id, source, book, split_type, home_team, away_team,
            game_datetime, last_updated, split_value, stake_pct, bet_pct,
            differential, current_line, previous_line, previous_stake_pct,
            COALESCE(current_line - previous_line, 0) as line_movement,
            COALESCE(stake_pct - previous_stake_pct, 0) as stake_movement,
            home_win, over, home_cover_spread,
            ABS(differential) as signal_strength,
            CASE WHEN differential > 0 THEN home_team ELSE away_team END as recommended_side
        FROM line_movement_data
        WHERE ABS(differential) >= %s
        ORDER BY ABS(differential) DESC, ABS(COALESCE(current_line - previous_line, 0)) DESC
        """

        return (
            self.coordinator.execute_query(query, (start_time, end_time, min_diff))
            or []
        )

    async def get_profitable_strategies(self) -> list[dict]:
        """Get profitable betting strategies from the database."""
        try:
            # Get strategy performance data
            strategy_data = await self.get_strategy_performance_data()

            # Filter for profitable strategies (win rate > 52.4% and sufficient bets)
            profitable_strategies = [
                strategy
                for strategy in strategy_data
                if strategy.get("win_rate", 0) > 52.4
                and strategy.get("total_bets", 0) >= 5
            ]

            self.logger.info(
                f"Retrieved {len(profitable_strategies)} profitable strategies"
            )
            return profitable_strategies

        except Exception as e:
            self.logger.error(f"Error retrieving profitable strategies: {e}")
            return []

    def get_performance_stats(self) -> dict[str, Any]:
        """Get performance statistics for the repository."""
        self._call_stats["total_calls"] = (
            self._call_stats["database_calls"] + self._call_stats["cache_hits"]
        )
        cache_hit_rate = (
            self._call_stats["cache_hits"] / max(self._call_stats["total_calls"], 1)
        ) * 100

        return {
            **self._call_stats,
            "cache_hit_rate_percent": round(cache_hit_rate, 2),
            "cache_size": len(self._cache),
        }
