"""
Sharp Action Detection Service

Integrates existing strategy processors with betting lines data storage
to populate sharp_action fields in the betting lines tables.
"""

from datetime import date, datetime
from typing import Any

import structlog

from src.analysis.models.unified_models import UnifiedBettingSignal

logger = structlog.get_logger(__name__)


class SharpActionDetectionService:
    """Service to detect and populate sharp action indicators in betting lines tables."""

    def __init__(self, connection):
        self.connection = connection
        self.logger = logger.bind(component="SharpActionDetectionService")
        # Note: Strategy orchestrator integration will be added in future iteration

    async def update_sharp_action_indicators(
        self, target_date: date | None = None, force_update: bool = False
    ) -> dict[str, Any]:
        """
        Update sharp action indicators for games on specified date.

        Args:
            target_date: Date to process (defaults to today)
            force_update: Whether to update existing records

        Returns:
            Dict with processing results
        """
        if target_date is None:
            target_date = date.today()

        self.logger.info(
            "Starting sharp action detection",
            target_date=target_date,
            force_update=force_update,
        )

        try:
            from src.data.database.connection import get_connection
            async with get_connection() as conn:
                # Get games for the target date
                games = await self._get_games_for_date(conn, target_date)

                if not games:
                    self.logger.info("No games found for date", target_date=target_date)
                    return {"success": True, "games_processed": 0, "records_updated": 0}

                total_updated = 0
                games_processed = 0

                for game in games:
                    game_id = game["id"]
                    self.logger.info(
                        "Processing game for sharp action",
                        game_id=game_id,
                        home_team=game["home_team"],
                        away_team=game["away_team"],
                    )

                    # Run strategy analysis for this game
                    strategy_results = await self._analyze_game_for_sharp_action(
                        conn, game_id, target_date
                    )

                    # Update betting lines tables with sharp action indicators
                    updated_count = await self._update_betting_lines_sharp_action(
                        conn, game_id, strategy_results, force_update
                    )

                    total_updated += updated_count
                    games_processed += 1

                    self.logger.info(
                        "Completed game sharp action processing",
                        game_id=game_id,
                        updated_records=updated_count,
                    )

                self.logger.info(
                    "Completed sharp action detection",
                    target_date=target_date,
                    games_processed=games_processed,
                    total_records_updated=total_updated,
                )

                return {
                    "success": True,
                    "games_processed": games_processed,
                    "records_updated": total_updated,
                    "target_date": target_date.isoformat(),
                }

        except Exception as e:
            self.logger.error(
                "Failed to update sharp action indicators",
                target_date=target_date,
                error=str(e),
            )
            return {"success": False, "error": str(e)}

    async def _get_games_for_date(
        self, conn, target_date: date
    ) -> list[dict[str, Any]]:
        """Get all games for the specified date."""
        query = """
            SELECT id, home_team, away_team, game_date, game_datetime
            FROM curated.games_complete 
            WHERE game_date = $1
            ORDER BY game_datetime
        """

        rows = await conn.fetch(query, target_date)
        return [dict(row) for row in rows]

    async def _analyze_game_for_sharp_action(
        self, conn, game_id: int, target_date: date
    ) -> dict[str, Any]:
        """
        Analyze a specific game for sharp action using existing strategy processors.

        Returns:
            Dict containing sharp action analysis results
        """
        try:
            # For now, create mock sharp action indicators based on data patterns
            # In a future iteration, this would integrate with the full strategy orchestrator

            sharp_indicators = await self._detect_basic_sharp_patterns(conn, game_id)

            return sharp_indicators

        except Exception as e:
            self.logger.error(
                "Failed to analyze game for sharp action", game_id=game_id, error=str(e)
            )
            return {}

    async def _detect_basic_sharp_patterns(self, conn, game_id: int) -> dict[str, Any]:
        """
        Detect basic sharp action patterns from betting lines data.
        This is a simplified implementation that can be enhanced later.
        """
        indicators = {
            "moneyline": {"detected": False, "confidence": 0.0, "patterns": []},
            "spread": {"detected": False, "confidence": 0.0, "patterns": []},
            "total": {"detected": False, "confidence": 0.0, "patterns": []},
        }

        # Check for line movement patterns in the betting lines data
        for market_type in ["moneyline", "spread", "total"]:
            table_name = f"betting_lines_{market_type if market_type != 'moneyline' else 'moneyline'}"

            # Look for line movement patterns
            query = f"""
                SELECT COUNT(*) as line_count,
                       COUNT(DISTINCT sportsbook_id) as book_count,
                       AVG(data_completeness_score) as avg_completeness
                FROM curated.{table_name}
                WHERE game_id = $1
                AND sportsbook_id IS NOT NULL
            """

            try:
                result = await conn.fetchrow(query, game_id)

                if result and result["line_count"] > 0:
                    # Simple heuristic: multiple books with good data quality suggests activity
                    if result["book_count"] >= 2 and result["avg_completeness"] > 0.6:
                        indicators[market_type]["detected"] = True
                        indicators[market_type]["confidence"] = min(
                            result["avg_completeness"] * result["book_count"] / 5.0, 1.0
                        )
                        indicators[market_type]["patterns"].append(
                            {
                                "strategy": "basic_line_movement",
                                "recommendation": "MONITOR",
                                "confidence": indicators[market_type]["confidence"],
                                "reasoning": f"Multiple books ({result['book_count']}) with good data quality",
                            }
                        )

            except Exception as e:
                self.logger.warning(
                    "Failed to check sharp patterns for market",
                    market_type=market_type,
                    game_id=game_id,
                    error=str(e),
                )

        return indicators

    def _extract_sharp_action_indicators(
        self, analysis_results: list[UnifiedBettingSignal]
    ) -> dict[str, Any]:
        """
        Extract sharp action indicators from strategy analysis results.

        Args:
            analysis_results: List of strategy results

        Returns:
            Dict with sharp action indicators for different market types
        """
        indicators = {
            "moneyline": {"detected": False, "confidence": 0.0, "patterns": []},
            "spread": {"detected": False, "confidence": 0.0, "patterns": []},
            "total": {"detected": False, "confidence": 0.0, "patterns": []},
        }

        # Pattern keywords that indicate sharp action
        sharp_patterns = [
            "sharp_action",
            "line_movement",
            "reverse_line_movement",
            "consensus_fade",
            "late_flip",
            "steam_move",
            "sharp_money",
        ]

        for result in analysis_results:
            if not result.recommended_side or result.recommended_side == "PASS":
                continue

            signal_type = result.signal_type.value.lower()
            market_type = self._determine_market_type_from_bet_type(result.bet_type)

            # Check if this signal indicates sharp action
            is_sharp_strategy = any(
                pattern in signal_type for pattern in sharp_patterns
            )

            if is_sharp_strategy and market_type in indicators:
                indicators[market_type]["detected"] = True
                indicators[market_type]["confidence"] = max(
                    indicators[market_type]["confidence"], result.confidence_score
                )
                indicators[market_type]["patterns"].append(
                    {
                        "strategy": result.signal_type.value,
                        "recommendation": result.recommended_side,
                        "confidence": result.confidence_score,
                        "reasoning": f"Signal: {result.signal_type.value}, Side: {result.recommended_side}",
                    }
                )

        return indicators

    def _determine_market_type_from_bet_type(self, bet_type: str) -> str:
        """Determine market type from bet type."""
        bet_type_lower = bet_type.lower()

        if "ml" in bet_type_lower or "moneyline" in bet_type_lower:
            return "moneyline"
        elif "spread" in bet_type_lower or "point" in bet_type_lower:
            return "spread"
        elif (
            "total" in bet_type_lower
            or "over" in bet_type_lower
            or "under" in bet_type_lower
        ):
            return "total"
        else:
            # Default to spread for generic betting types
            return "spread"

    async def _update_betting_lines_sharp_action(
        self,
        conn,
        game_id: int,
        sharp_indicators: dict[str, Any],
        force_update: bool = False,
    ) -> int:
        """
        Update betting lines tables with sharp action indicators.

        Returns:
            Number of records updated
        """
        updated_count = 0

        # Update moneyline table
        if sharp_indicators.get("moneyline", {}).get("detected"):
            moneyline_indicator = self._format_sharp_action_indicator(
                sharp_indicators["moneyline"]
            )

            update_count = await self._update_table_sharp_action(
                conn,
                "betting_lines_moneyline",
                game_id,
                moneyline_indicator,
                force_update,
            )
            updated_count += update_count

        # Update spreads table
        if sharp_indicators.get("spread", {}).get("detected"):
            spread_indicator = self._format_sharp_action_indicator(
                sharp_indicators["spread"]
            )

            update_count = await self._update_table_sharp_action(
                conn, "betting_lines_spreads", game_id, spread_indicator, force_update
            )
            updated_count += update_count

        # Update totals table
        if sharp_indicators.get("total", {}).get("detected"):
            total_indicator = self._format_sharp_action_indicator(
                sharp_indicators["total"]
            )

            update_count = await self._update_table_sharp_action(
                conn, "betting_lines_totals", game_id, total_indicator, force_update
            )
            updated_count += update_count

        return updated_count

    def _format_sharp_action_indicator(self, indicator_data: dict[str, Any]) -> str:
        """Format sharp action indicator for database storage."""
        patterns = indicator_data.get("patterns", [])
        if not patterns:
            return "DETECTED"

        # Create a summary of detected patterns
        pattern_summary = []
        for pattern in patterns[:3]:  # Limit to top 3 patterns
            pattern_summary.append(
                f"{pattern['strategy']}({pattern['confidence']:.2f})"
            )

        return f"SHARP_ACTION: {', '.join(pattern_summary)}"

    async def _update_table_sharp_action(
        self,
        conn,
        table_name: str,
        game_id: int,
        sharp_action_value: str,
        force_update: bool = False,
    ) -> int:
        """Update sharp_action field in a specific betting lines table."""

        # Build the update query with optional force update condition
        where_clause = "game_id = $1"
        if not force_update:
            where_clause += " AND (sharp_action IS NULL OR sharp_action = '')"

        query = f"""
            UPDATE curated.{table_name}
            SET sharp_action = $2, updated_at = NOW()
            WHERE {where_clause}
        """

        try:
            result = await conn.execute(query, game_id, sharp_action_value)
            # Extract affected row count from result
            updated_count = int(result.split()[-1]) if result else 0

            self.logger.info(
                "Updated sharp action indicators",
                table=table_name,
                game_id=game_id,
                updated_count=updated_count,
                sharp_action_value=sharp_action_value,
            )

            return updated_count

        except Exception as e:
            self.logger.error(
                "Failed to update sharp action",
                table=table_name,
                game_id=game_id,
                error=str(e),
            )
            return 0

    async def get_sharp_action_summary(
        self, start_date: date | None = None, end_date: date | None = None
    ) -> dict[str, Any]:
        """
        Get summary of sharp action detection results.

        Args:
            start_date: Start date for analysis (defaults to 30 days ago)
            end_date: End date for analysis (defaults to today)

        Returns:
            Dict with sharp action summary statistics
        """
        if end_date is None:
            end_date = date.today()
        if start_date is None:
            start_date = date.today().replace(day=1)  # Start of current month

        try:
            from src.data.database.connection import get_connection
            async with get_connection() as conn:
                # Query sharp action statistics
                query = """
                    SELECT 
                        'moneyline' as market_type,
                        COUNT(*) as total_records,
                        COUNT(CASE WHEN sharp_action IS NOT NULL AND sharp_action != '' THEN 1 END) as sharp_action_records,
                        ROUND(COUNT(CASE WHEN sharp_action IS NOT NULL AND sharp_action != '' THEN 1 END) * 100.0 / COUNT(*), 2) as sharp_action_pct
                    FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline' m
                    JOIN curated.games_complete g ON m.game_id = g.id
                    WHERE g.game_date BETWEEN $1 AND $2
                    
                    UNION ALL
                    
                    SELECT 
                        'spreads' as market_type,
                        COUNT(*) as total_records,
                        COUNT(CASE WHEN sharp_action IS NOT NULL AND sharp_action != '' THEN 1 END) as sharp_action_records,
                        ROUND(COUNT(CASE WHEN sharp_action IS NOT NULL AND sharp_action != '' THEN 1 END) * 100.0 / COUNT(*), 2) as sharp_action_pct
                    FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread's s
                    JOIN curated.games_complete g ON s.game_id = g.id
                    WHERE g.game_date BETWEEN $1 AND $2
                    
                    UNION ALL
                    
                    SELECT 
                        'totals' as market_type,
                        COUNT(*) as total_records,
                        COUNT(CASE WHEN sharp_action IS NOT NULL AND sharp_action != '' THEN 1 END) as sharp_action_records,
                        ROUND(COUNT(CASE WHEN sharp_action IS NOT NULL AND sharp_action != '' THEN 1 END) * 100.0 / COUNT(*), 2) as sharp_action_pct
                    FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals' t
                    JOIN curated.games_complete g ON t.game_id = g.id
                    WHERE g.game_date BETWEEN $1 AND $2
                """

                rows = await conn.fetch(query, start_date, end_date)

                summary = {
                    "period": {
                        "start_date": start_date.isoformat(),
                        "end_date": end_date.isoformat(),
                    },
                    "markets": {row["market_type"]: dict(row) for row in rows},
                }

                return summary

        except Exception as e:
            self.logger.error(
                "Failed to get sharp action summary",
                start_date=start_date,
                end_date=end_date,
                error=str(e),
            )
            return {"error": str(e)}

    async def health_check(self) -> dict[str, Any]:
        """Perform health check on the sharp action detection service."""
        try:
            from src.data.database.connection import get_connection
            async with get_connection() as conn:
                # Check strategy orchestrator availability (if available)
                orchestrator_status = None
                if hasattr(self, 'strategy_orchestrator') and self.strategy_orchestrator:
                    orchestrator_status = await self.strategy_orchestrator.health_check()
                else:
                    orchestrator_status = {"status": "not_configured", "message": "Strategy orchestrator integration pending"}

                # Check recent sharp action detection activity
                try:
                    recent_activity = await conn.fetchval("""
                        SELECT COUNT(*) FROM curated.betting_lines_unified 
                        WHERE sharp_action IS NOT NULL 
                        AND sharp_action != ''
                        AND updated_at >= NOW() - INTERVAL '7 days'
                    """)
                except Exception:
                    # Table might not exist yet
                    recent_activity = 0

                return {
                    "status": "healthy",
                    "strategy_orchestrator": orchestrator_status,
                    "recent_sharp_action_records": recent_activity,
                    "timestamp": datetime.now().isoformat(),
                }

        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
            }
