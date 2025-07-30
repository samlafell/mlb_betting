"""
Repository for analysis reports and betting opportunities data.
Handles persistence of pipeline analysis results to PostgreSQL instead of JSON files.
"""

from datetime import datetime

from src.core.config import get_settings
from src.core.logging import LogComponent, get_logger
from src.core.team_utils import normalize_team_name
from src.data.database.connection import get_connection
from src.data.models.unified.movement_analysis import (
    CrossBookMovement,
    MovementAnalysisReport,
    RLMIndicator,
)

logger = get_logger(__name__, LogComponent.CORE)


class AnalysisReportsRepository:
    """Repository for managing analysis reports and opportunities in PostgreSQL."""

    def __init__(self):
        self.settings = get_settings()

    def _safe_normalize_team_name(self, team_name, game_id: str = "unknown") -> str:
        """
        Safely normalize team name with comprehensive validation and error handling.

        Args:
            team_name: Team name to normalize (can be None, empty, or string)
            game_id: Game ID for logging context

        Returns:
            str: Normalized team name, "UNK" if normalization fails
        """
        try:
            # Handle None or non-string values explicitly
            if team_name is None:
                logger.warning(f"None team name for game {game_id}")
                return "UNK"

            # Convert to string if not already
            if not isinstance(team_name, str):
                team_name = str(team_name)

            # Use the enhanced normalize_team_name function
            normalized = normalize_team_name(team_name)

            # Final validation - ensure result fits database constraints
            if not normalized or len(normalized) > 10:
                logger.warning(
                    f"Team normalization produced invalid result '{normalized}' for game {game_id}"
                )
                return "UNK"

            return normalized

        except Exception as e:
            logger.error(
                f"Team normalization failed for '{team_name}' in game {game_id}: {e}"
            )
            return "UNK"

    def _validate_game_data(self, game_data: dict) -> tuple[str, str] | None:
        """
        Validate game data and extract normalized team names.

        Args:
            game_data: Game data dictionary

        Returns:
            tuple[str, str] | None: (home_team, away_team) if valid, None if invalid
        """
        game_id = game_data.get("game_id", "unknown")
        home_team = game_data.get("home_team")
        away_team = game_data.get("away_team")

        # Check for missing team data
        if not home_team or not away_team:
            logger.warning(
                f"Missing team data for game {game_id}: home='{home_team}', away='{away_team}'"
            )
            return None

        # Normalize team names safely
        normalized_home = self._safe_normalize_team_name(home_team, game_id)
        normalized_away = self._safe_normalize_team_name(away_team, game_id)

        # Validate normalization results
        if normalized_home == "UNK" or normalized_away == "UNK":
            logger.warning(f"Team normalization failed for game {game_id}")
            return None

        return normalized_home, normalized_away

    async def create_analysis_report(
        self,
        report_type: str,
        analysis_timestamp: datetime,
        pipeline_run_id: str,
        total_games_analyzed: int = 0,
        games_with_rlm: int = 0,
        games_with_steam_moves: int = 0,
        games_with_arbitrage: int = 0,
        total_movements: int = 0,
        execution_time_seconds: float | None = None,
        data_source: str = "action_network",
    ) -> int:
        """Create a new analysis report and return its ID."""

        query = """
        INSERT INTO curated.analysis_reports (
            report_type, analysis_timestamp, pipeline_run_id,
            total_games_analyzed, games_with_rlm, games_with_steam_moves,
            games_with_arbitrage, total_movements, execution_time_seconds,
            data_source
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        RETURNING id
        """

        async with get_connection() as conn:
            result = await conn.fetchrow(
                query,
                report_type,
                analysis_timestamp,
                pipeline_run_id,
                total_games_analyzed,
                games_with_rlm,
                games_with_steam_moves,
                games_with_arbitrage,
                total_movements,
                execution_time_seconds,
                data_source,
            )

        return result["id"] if result else None

    async def save_rlm_opportunities(
        self,
        analysis_report_id: int,
        rlm_indicators: list[RLMIndicator],
        game_data: dict,
    ) -> list[int]:
        """Save RLM opportunities to the database."""

        # Validate game data and normalize team names
        team_names = self._validate_game_data(game_data)
        if team_names is None:
            logger.warning(
                f"Skipping RLM opportunities for invalid game data: {game_data.get('game_id')}"
            )
            return []

        normalized_home, normalized_away = team_names

        opportunities = []
        for rlm in rlm_indicators:
            opportunities.append(
                (
                    analysis_report_id,
                    game_data.get("game_id"),
                    game_data.get("action_network_game_id"),
                    normalized_home,
                    normalized_away,
                    game_data.get("game_datetime"),
                    None,  # sportsbook_id - need to resolve from action_network_book_id
                    rlm.sportsbook_id,
                    rlm.market_type.value,
                    "home"
                    if rlm.market_type.value == "moneyline"
                    else "over",  # Simplified
                    rlm.line_direction.value,
                    rlm.public_betting_direction.value,
                    float(rlm.public_percentage) if rlm.public_percentage else None,
                    float(rlm.line_movement_amount)
                    if rlm.line_movement_amount
                    else None,
                    rlm.rlm_strength,
                    datetime.now(),
                )
            )

        if not opportunities:
            return []

        query = """
        INSERT INTO curated.rlm_opportunities (
            analysis_report_id, game_id, action_network_game_id,
            home_team, away_team, game_datetime, sportsbook_id,
            action_network_book_id, market_type, side,
            line_direction, public_betting_direction, public_percentage,
            line_movement_amount, rlm_strength, detected_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
        RETURNING id
        """

        results = []
        async with get_connection() as conn:
            for opp in opportunities:
                result = await conn.fetchrow(query, *opp)
                results.append(result["id"] if result else None)

        return results

    async def save_steam_moves(
        self,
        analysis_report_id: int,
        cross_book_movements: list[CrossBookMovement],
        game_data: dict,
    ) -> list[int]:
        """Save steam moves to the database."""

        # Validate game data and normalize team names
        team_names = self._validate_game_data(game_data)
        if team_names is None:
            logger.warning(
                f"Skipping steam moves for invalid game data: {game_data.get('game_id')}"
            )
            return []

        normalized_home, normalized_away = team_names

        steam_moves = []
        for movement in cross_book_movements:
            if movement.steam_move_detected:
                steam_moves.append(
                    (
                        analysis_report_id,
                        game_data.get("game_id"),
                        game_data.get("action_network_game_id"),
                        normalized_home,
                        normalized_away,
                        game_data.get("game_datetime"),
                        movement.market_type.value,
                        movement.consensus_direction.value,
                        movement.consensus_strength,
                        movement.participating_books,
                        movement.divergent_books,
                        float(movement.average_movement)
                        if movement.average_movement
                        else None,
                        movement.timestamp,
                        datetime.now(),
                    )
                )

        if not steam_moves:
            return []

        query = """
        INSERT INTO curated.steam_moves (
            analysis_report_id, game_id, action_network_game_id,
            home_team, away_team, game_datetime, market_type,
            consensus_direction, consensus_strength, participating_books,
            divergent_books, average_movement, movement_timestamp, detected_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        RETURNING id
        """

        results = []
        async with get_connection() as conn:
            for steam in steam_moves:
                result = await conn.fetchrow(query, *steam)
                results.append(result["id"] if result else None)

        return results

    async def save_arbitrage_opportunities(
        self,
        analysis_report_id: int,
        arbitrage_opportunities: list[dict],
        game_data: dict,
    ) -> list[int]:
        """Save arbitrage opportunities to the database."""
        if not arbitrage_opportunities:
            return []

        # Validate game data and normalize team names
        team_names = self._validate_game_data(game_data)
        if team_names is None:
            logger.warning(
                f"Skipping arbitrage opportunities for invalid game data: {game_data.get('game_id')}"
            )
            return []

        normalized_home, normalized_away = team_names

        opportunities = []
        for arb in arbitrage_opportunities:
            # Enhanced validation for arbitrage profit handling
            potential_profit = arb.get("potential_profit")
            validated_profit = None

            if potential_profit is not None:
                try:
                    # Ensure we have a valid number
                    validated_profit = (
                        float(potential_profit)
                        if potential_profit not in ("TBD", "", "N/A")
                        else None
                    )
                except (ValueError, TypeError):
                    logger.warning(
                        f"Invalid profit value '{potential_profit}' for game {game_data.get('game_id')}"
                    )
                    validated_profit = None

            opportunities.append(
                (
                    analysis_report_id,
                    game_data.get("game_id"),
                    game_data.get("action_network_game_id"),
                    normalized_home,
                    normalized_away,
                    game_data.get("game_datetime"),
                    arb["market_type"],
                    None,  # book_a_id - need to resolve
                    None,  # book_b_id - need to resolve
                    arb.get("max_odds", 0),
                    arb.get("min_odds", 0),
                    arb["discrepancy"],
                    validated_profit,
                    datetime.now(),
                )
            )

        query = """
        INSERT INTO curated.arbitrage_opportunities (
            analysis_report_id, game_id, action_network_game_id,
            home_team, away_team, game_datetime, market_type,
            book_a_id, book_b_id, book_a_odds, book_b_odds,
            odds_discrepancy, potential_profit_percentage, detected_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        RETURNING id
        """

        results = []
        async with get_connection() as conn:
            for opp in opportunities:
                result = await conn.fetchrow(query, *opp)
                results.append(result["id"] if result else None)

        return results

    async def create_pipeline_run(
        self,
        run_id: str,
        command_type: str,
        date_target: str,
        max_games: int | None = None,
        skip_history: bool = False,
        analyze_only: bool = False,
    ) -> int:
        """Create a pipeline run record."""
        query = """
        INSERT INTO curated.pipeline_runs (
            run_id, command_type, status, date_target,
            max_games, skip_history, analyze_only, start_time
        ) VALUES ($1, $2, 'running', $3, $4, $5, $6, $7)
        RETURNING id
        """

        async with get_connection() as conn:
            result = await conn.fetchrow(
                query,
                run_id,
                command_type,
                date_target,
                max_games,
                skip_history,
                analyze_only,
                datetime.now(),
            )

        return result["id"] if result else None

    async def update_pipeline_run(
        self,
        run_id: str,
        status: str,
        games_extracted: int = 0,
        games_analyzed: int = 0,
        total_movements: int = 0,
        total_opportunities: int = 0,
        execution_time_seconds: float | None = None,
        error_message: str | None = None,
        warnings: list[str] | None = None,
    ) -> bool:
        """Update pipeline run with results."""
        query = """
        UPDATE curated.pipeline_runs SET
            status = $2,
            games_extracted = $3,
            games_analyzed = $4,
            total_movements = $5,
            total_opportunities = $6,
            execution_time_seconds = $7,
            error_message = $8,
            warnings = $9,
            end_time = $10
        WHERE run_id = $1
        """

        async with get_connection() as conn:
            result = await conn.execute(
                query,
                run_id,
                status,
                games_extracted,
                games_analyzed,
                total_movements,
                total_opportunities,
                execution_time_seconds,
                error_message,
                warnings,
                datetime.now(),
            )

        return "UPDATE" in str(result) if result else False

    async def get_latest_opportunities(self, hours: int = 24) -> list[dict]:
        """Get latest opportunities from the database view."""
        query = f"""
        SELECT 
            opportunity_type,
            home_team,
            away_team,
            game_datetime,
            market_type,
            strength,
            detected_at,
            analysis_timestamp,
            profit_potential
        FROM curated.latest_opportunities
        WHERE analysis_timestamp > NOW() - INTERVAL '{hours} hours'
        ORDER BY detected_at DESC
        LIMIT 50
        """

        async with get_connection() as conn:
            rows = await conn.fetch(query)

        return [dict(row) for row in rows] if rows else []

    async def get_daily_summary(self, days: int = 7) -> list[dict]:
        """Get daily opportunities summary."""
        query = f"""
        SELECT *
        FROM curated.daily_opportunities_summary
        WHERE analysis_date > CURRENT_DATE - INTERVAL '{days} days'
        ORDER BY analysis_date DESC
        """

        async with get_connection() as conn:
            rows = await conn.fetch(query)

        return [dict(row) for row in rows] if rows else []

    async def save_complete_analysis(
        self,
        pipeline_run_id: str,
        analysis_report: MovementAnalysisReport,
        execution_time_seconds: float | None = None,
    ) -> int:
        """Save complete analysis results to database instead of JSON files."""

        # Create main analysis report
        analysis_report_id = await self.create_analysis_report(
            report_type="movement_analysis",
            analysis_timestamp=analysis_report.analysis_timestamp,
            pipeline_run_id=pipeline_run_id,
            total_games_analyzed=analysis_report.total_games_analyzed,
            games_with_rlm=analysis_report.games_with_rlm,
            games_with_steam_moves=analysis_report.games_with_steam_moves,
            games_with_arbitrage=analysis_report.games_with_arbitrage,
            execution_time_seconds=execution_time_seconds,
        )

        # Save opportunities from each game analysis
        total_opportunities = 0

        for game_analysis in analysis_report.game_analyses:
            game_data = {
                "game_id": None,  # Would need to resolve from database
                "action_network_game_id": game_analysis.game_id,
                "home_team": game_analysis.home_team,
                "away_team": game_analysis.away_team,
                "game_datetime": game_analysis.game_datetime,
            }

            # Save RLM opportunities
            if game_analysis.rlm_indicators:
                await self.save_rlm_opportunities(
                    analysis_report_id, game_analysis.rlm_indicators, game_data
                )
                total_opportunities += len(game_analysis.rlm_indicators)

            # Save steam moves
            if game_analysis.cross_book_movements:
                steam_count = await self.save_steam_moves(
                    analysis_report_id, game_analysis.cross_book_movements, game_data
                )
                total_opportunities += len(steam_count)

            # Save arbitrage opportunities
            if game_analysis.arbitrage_opportunities:
                arb_count = await self.save_arbitrage_opportunities(
                    analysis_report_id, game_analysis.arbitrage_opportunities, game_data
                )
                total_opportunities += len(arb_count)

        return analysis_report_id

    async def cleanup_old_reports(self, days_to_keep: int = 30) -> int:
        """Clean up old analysis reports to prevent database bloat."""
        query = f"""
        DELETE FROM curated.analysis_reports
        WHERE analysis_timestamp < NOW() - INTERVAL '{days_to_keep} days'
        """

        async with get_connection() as conn:
            result = await conn.execute(query)

        # Extract number of deleted rows
        result_str = str(result) if result else ""
        if result_str.startswith("DELETE "):
            return int(result_str.split(" ")[1])
        return 0
