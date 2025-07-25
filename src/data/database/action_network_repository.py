"""
Action Network Database Repository

Specialized repository for Action Network betting data operations using
the curated schema tables:
- curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline'
- curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread's
- curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals'
- curated.sportsbooks
- operational.extraction_log (for tracking)
"""

from datetime import datetime, timedelta
from typing import Any

import structlog

from ..models.unified.actionnetwork import (
    ActionNetworkHistoricalData,
    ActionNetworkHistoricalEntry,
)
from .connection import DatabaseConnection

logger = structlog.get_logger(__name__)


class ActionNetworkRepository:
    """Repository for Action Network data using curated schema."""

    def __init__(self, connection: DatabaseConnection):
        self.connection = connection
        self.logger = logger.bind(component="ActionNetworkRepository")

        # Action Network sportsbook ID mapping
        self.action_network_books = {
            "15": "DraftKings",
            "30": "FanDuel",
            "68": "BetMGM",
            "69": "Caesars",
            "71": "BetRivers",
            "75": "ESPN BET",
        }

        # Team name to abbreviation mapping
        self.team_abbreviations = {
            "Arizona Diamondbacks": "ARI",
            "Atlanta Braves": "ATL",
            "Baltimore Orioles": "BAL",
            "Boston Red Sox": "BOS",
            "Chicago Cubs": "CHC",
            "Chicago White Sox": "CWS",
            "Cincinnati Reds": "CIN",
            "Cleveland Guardians": "CLE",
            "Colorado Rockies": "COL",
            "Detroit Tigers": "DET",
            "Houston Astros": "HOU",
            "Kansas City Royals": "KC",
            "Los Angeles Angels": "LAA",
            "Los Angeles Dodgers": "LAD",
            "Miami Marlins": "MIA",
            "Milwaukee Brewers": "MIL",
            "Minnesota Twins": "MIN",
            "New York Mets": "NYM",
            "New York Yankees": "NYY",
            "Oakland Athletics": "OAK",
            "Philadelphia Phillies": "PHI",
            "Pittsburgh Pirates": "PIT",
            "San Diego Padres": "SD",
            "San Francisco Giants": "SF",
            "Seattle Mariners": "SEA",
            "St. Louis Cardinals": "STL",
            "Tampa Bay Rays": "TB",
            "Texas Rangers": "TEX",
            "Toronto Blue Jays": "TOR",
            "Washington Nationals": "WAS",
        }

    def _get_team_abbreviation(self, team_name: str) -> str:
        """Convert full team name to abbreviation."""
        return self.team_abbreviations.get(team_name, team_name[:5].upper())

    async def _get_mlb_game_id(
        self, home_team: str, away_team: str, game_datetime: datetime
    ) -> str | None:
        """
        Get official MLB game ID using MLB-StatsAPI.

        Args:
            home_team: Home team name
            away_team: Away team name
            game_datetime: Game datetime

        Returns:
            Official MLB game ID (gamePk) if found, None otherwise
        """
        try:
            # Note: MLBStatsAPIService has been migrated to unified architecture
            # Using MLB-StatsAPI directly as recommended in the rules
            import mlbstatsapi as mlb

            mlb_service = mlb

            # Get games for the date
            game_date = game_datetime.date()
            games = mlb_service.get_games_for_date(game_date)

            # Normalize team names for matching
            home_normalized = mlb_service.normalize_team_name(home_team)
            away_normalized = mlb_service.normalize_team_name(away_team)

            # Find matching game
            for game in games:
                if (
                    game.home_team == home_normalized
                    and game.away_team == away_normalized
                ):
                    self.logger.info(
                        "Found MLB game match",
                        action_home=home_team,
                        action_away=away_team,
                        mlb_home=game.home_team,
                        mlb_away=game.away_team,
                        mlb_game_pk=game.game_pk,
                    )
                    return str(game.game_pk)

            self.logger.warning(
                "No MLB game match found",
                home_team=home_team,
                away_team=away_team,
                game_date=game_date,
                available_games=len(games),
            )
            return None

        except Exception as e:
            self.logger.error("Error getting MLB game ID", error=str(e))
            return None

    async def _ensure_game_exists(
        self, conn, historical_data: ActionNetworkHistoricalData
    ) -> int:
        """
        Ensure the game exists in curated.games_complete table.

        This is the FIRST thing we do when processing Action Network data.
        We save to the central games table and get official MLB game IDs.

        Args:
            conn: Database connection
            historical_data: Action Network historical data

        Returns:
            Game ID from curated.games_complete table
        """
        # Get team abbreviations
        home_abbr = self._get_team_abbreviation(historical_data.home_team)
        away_abbr = self._get_team_abbreviation(historical_data.away_team)

        # First, check if a game already exists with these teams and datetime
        # We need to check for existing games to avoid duplicates
        existing_game = await conn.fetchval(
            """
            SELECT id FROM curated.games_complete
            WHERE home_team = $1 AND away_team = $2
            AND DATE(game_datetime) = DATE($3)
            ORDER BY ABS(EXTRACT(EPOCH FROM (game_datetime - $3))) ASC
            LIMIT 1
        """,
            home_abbr,
            away_abbr,
            historical_data.game_datetime,
        )

        if existing_game:
            self.logger.info(
                "Found existing game",
                game_id=existing_game,
                home_team=f"{historical_data.home_team} -> {home_abbr}",
                away_team=f"{historical_data.away_team} -> {away_abbr}",
                game_datetime=historical_data.game_datetime,
            )

            # Update the action_network_game_id if not already set
            await conn.execute(
                """
                UPDATE curated.games_complete
                SET action_network_game_id = $1,
                    updated_at = NOW()
                WHERE id = $2 AND action_network_game_id IS NULL
            """,
                historical_data.game_id,
                existing_game,
            )

            return existing_game

        # Get official MLB game ID
        mlb_game_id = await self._get_mlb_game_id(
            historical_data.home_team,
            historical_data.away_team,
            historical_data.game_datetime,
        )

        self.logger.info(
            "Creating new game record",
            action_game_id=historical_data.game_id,
            home_team=f"{historical_data.home_team} -> {home_abbr}",
            away_team=f"{historical_data.away_team} -> {away_abbr}",
            game_datetime=historical_data.game_datetime,
            mlb_game_id=mlb_game_id,
        )

        # Extract date and season from datetime
        game_date = historical_data.game_datetime.date()
        season = historical_data.game_datetime.year

        # Insert new game record into curated.games_complete
        game_id = await conn.fetchval(
            """
            INSERT INTO curated.games_complete (
                action_network_game_id,
                mlb_stats_api_game_id,
                home_team,
                away_team,
                game_date,
                game_datetime,
                season,
                game_status,
                data_quality,
                has_mlb_enrichment,
                created_at,
                updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW(), NOW()
            )
            RETURNING id
        """,
            historical_data.game_id,
            mlb_game_id,
            home_abbr,
            away_abbr,
            game_date,
            historical_data.game_datetime,
            season,
            "scheduled",  # Default status
            "HIGH" if mlb_game_id else "MEDIUM",  # Higher quality if we have MLB ID
            mlb_game_id is not None,  # has_mlb_enrichment
        )

        self.logger.info(
            "Created game record",
            new_game_id=game_id,
            action_game_id=historical_data.game_id,
            mlb_game_id=mlb_game_id,
        )

        return game_id

    async def save_historical_data(
        self, historical_data: ActionNetworkHistoricalData
    ) -> dict[str, Any]:
        """
        Save Action Network historical data to curated schema tables.

        Args:
            historical_data: Parsed Action Network data

        Returns:
            Dict with save results
        """
        if not historical_data.historical_entries:
            self.logger.warning("No historical entries to save")
            return {"success": True, "saved_count": 0}

        try:
            async with self.connection.get_async_connection() as conn:
                # STEP 1: ENSURE GAME EXISTS IN CENTRAL GAMES TABLE FIRST!
                # This is the most important step - save to central tracking
                game_id = await self._ensure_game_exists(conn, historical_data)

                # STEP 2: Ensure sportsbooks exist
                await self._ensure_sportsbooks_exist(conn)

                # Save each entry using the central game_id
                total_saved = 0
                for entry in historical_data.historical_entries:
                    saved_count = await self._save_historical_entry(
                        conn, game_id, historical_data, entry
                    )
                    total_saved += saved_count

                # Update extraction log with team abbreviations
                home_abbr = self._get_team_abbreviation(historical_data.home_team)
                away_abbr = self._get_team_abbreviation(historical_data.away_team)
                await self._update_extraction_log(
                    conn,
                    historical_data.game_id,
                    home_abbr,
                    away_abbr,
                    historical_data.game_datetime,
                    total_saved,
                    "success",
                    history_url=getattr(historical_data, "history_url", None),
                )

                self.logger.info(
                    "Historical data saved to curated schema",
                    game_id=historical_data.game_id,
                    saved_count=total_saved,
                )

                return {"success": True, "saved_count": total_saved}

        except Exception as e:
            self.logger.error(
                "Failed to save historical data",
                game_id=historical_data.game_id,
                error=str(e),
            )
            return {"success": False, "error": str(e)}

    async def _ensure_sportsbooks_exist(self, conn) -> None:
        """Ensure Action Network sportsbooks exist in curated.sportsbooks."""
        for book_id, book_name in self.action_network_books.items():
            abbreviation = book_name.replace(" ", "").upper()[:10]
            self.logger.info(
                "Ensuring sportsbook exists",
                book_id=book_id,
                book_name=book_name,
                abbreviation=abbreviation,
            )

            await conn.execute(
                """
                INSERT INTO curated.sportsbooks (name, display_name, abbreviation, is_active, supports_live_betting)
                VALUES ($1, $2, $3, true, true)
                ON CONFLICT (name) DO NOTHING
            """,
                book_name,
                book_name,
                abbreviation,
            )

    async def _save_historical_entry(
        self,
        conn,
        game_id: int,
        historical_data: ActionNetworkHistoricalData,
        entry: ActionNetworkHistoricalEntry,
    ) -> int:
        """Save a single historical entry to appropriate curated schema tables."""
        saved_count = 0

        # Extract market data from the raw event data
        event_data = entry.event

        self.logger.info(
            "Processing historical entry",
            game_id=historical_data.game_id,
            timestamp=entry.timestamp,
            event_keys=list(event_data.keys()) if event_data else [],
        )

        # Process each market type from the event data
        for market_name in ["moneyline", "spread", "total"]:
            if market_name in event_data and event_data[market_name]:
                market_data = event_data[market_name]
                self.logger.info(
                    "Found market data",
                    market_name=market_name,
                    data_type=type(market_data).__name__,
                    data_length=len(market_data)
                    if isinstance(market_data, list)
                    else "N/A",
                )

                if isinstance(market_data, list):
                    if market_name == "moneyline":
                        saved_count += await self._save_moneyline_data(
                            conn, game_id, historical_data, entry, market_data
                        )
                    elif market_name == "spread":
                        saved_count += await self._save_spread_data(
                            conn, game_id, historical_data, entry, market_data
                        )
                    elif market_name == "total":
                        saved_count += await self._save_total_data(
                            conn, game_id, historical_data, entry, market_data
                        )

        self.logger.info(
            "Completed historical entry processing",
            game_id=historical_data.game_id,
            saved_count=saved_count,
        )
        return saved_count

    async def resolve_sportsbook_id(
        self, conn, external_id: str, source: str = "ACTION_NETWORK"
    ) -> int | None:
        """Resolve external sportsbook ID to internal database ID using new mapping system."""
        try:
            # Use the new sportsbook mapping system
            sportsbook_id = await conn.fetchval(
                "SELECT curated.resolve_sportsbook_id($1, NULL, $2)",
                external_id,
                source,
            )
            return sportsbook_id
        except Exception as e:
            self.logger.error(
                "Failed to resolve sportsbook ID",
                external_id=external_id,
                source=source,
                error=str(e),
            )
            return None

    def calculate_data_completeness(self, **fields) -> float:
        """Calculate data completeness score for a record."""
        total_fields = len(fields)
        filled_fields = sum(1 for value in fields.values() if value is not None)
        return filled_fields / total_fields if total_fields > 0 else 0.0

    async def _save_moneyline_data(
        self,
        conn,
        game_id: int,
        historical_data: ActionNetworkHistoricalData,
        entry: ActionNetworkHistoricalEntry,
        market_data: list[dict],
    ) -> int:
        """Save moneyline data to curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline'."""
        saved_count = 0

        self.logger.info(
            "Processing moneyline data",
            game_id=historical_data.game_id,
            market_items=len(market_data),
        )

        # Group market items by sportsbook
        sportsbook_data = {}
        for market_item in market_data:
            book_id = str(market_item.get("book_id", ""))
            if book_id not in self.action_network_books:
                continue

            if book_id not in sportsbook_data:
                sportsbook_data[book_id] = {"home": None, "away": None}

            side = market_item.get("side", "").lower()
            if side in ["home", "away"]:
                sportsbook_data[book_id][side] = market_item

        # Process each sportsbook's data
        for book_id, sides_data in sportsbook_data.items():
            sportsbook_name = self.action_network_books[book_id]

            # Use new sportsbook ID resolution system
            sportsbook_id_result = await self.resolve_sportsbook_id(
                conn, book_id, "ACTION_NETWORK"
            )

            # Extract odds from home and away sides
            home_odds = sides_data["home"].get("odds") if sides_data["home"] else None
            away_odds = sides_data["away"].get("odds") if sides_data["away"] else None

            if home_odds is None and away_odds is None:
                continue

            # Extract betting splits (if available)
            home_bets_pct = None
            away_bets_pct = None
            home_money_pct = None
            away_money_pct = None

            if sides_data["home"]:
                home_bet_info = sides_data["home"].get("bet_info", {})
                if home_bet_info:
                    tickets_info = home_bet_info.get("tickets", {})
                    money_info = home_bet_info.get("money", {})
                    if "percent" in tickets_info:
                        home_bets_pct = tickets_info["percent"]
                    if "percent" in money_info:
                        home_money_pct = money_info["percent"]

            if sides_data["away"]:
                away_bet_info = sides_data["away"].get("bet_info", {})
                if away_bet_info:
                    tickets_info = away_bet_info.get("tickets", {})
                    money_info = away_bet_info.get("money", {})
                    if "percent" in tickets_info:
                        away_bets_pct = tickets_info["percent"]
                    if "percent" in money_info:
                        away_money_pct = money_info["percent"]

            # Get team abbreviations
            home_abbr = self._get_team_abbreviation(historical_data.home_team)
            away_abbr = self._get_team_abbreviation(historical_data.away_team)

            # Use current timestamp if entry timestamp is None
            odds_timestamp = entry.timestamp if entry.timestamp else datetime.now()

            self.logger.info(
                "Inserting moneyline data",
                game_id=historical_data.game_id,
                sportsbook=sportsbook_name,
                home_team=f"{historical_data.home_team} -> {home_abbr}",
                away_team=f"{historical_data.away_team} -> {away_abbr}",
                home_odds=home_odds,
                away_odds=away_odds,
                home_bets_pct=home_bets_pct,
                away_bets_pct=away_bets_pct,
                home_money_pct=home_money_pct,
                away_money_pct=away_money_pct,
                odds_timestamp=odds_timestamp,
            )

            # Insert into moneyline table
            try:
                await conn.execute(
                    """
                    INSERT INTO curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline' (
                        game_id, sportsbook_id, sportsbook, home_ml, away_ml, odds_timestamp,
                        home_bets_percentage, away_bets_percentage, home_money_percentage, away_money_percentage,
                        source, data_quality, game_datetime, home_team, away_team
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
                    )
                """,
                    game_id,
                    sportsbook_id_result,
                    sportsbook_name,
                    home_odds,
                    away_odds,
                    odds_timestamp,
                    home_bets_pct,
                    away_bets_pct,
                    home_money_pct,
                    away_money_pct,
                    "ACTION_NETWORK",
                    "HIGH",
                    historical_data.game_datetime,
                    home_abbr,
                    away_abbr,
                )
                self.logger.info(
                    "Successfully inserted moneyline data",
                    game_id=historical_data.game_id,
                    sportsbook=sportsbook_name,
                )
            except Exception as e:
                self.logger.error(
                    "Failed to insert moneyline data",
                    game_id=historical_data.game_id,
                    sportsbook=sportsbook_name,
                    error=str(e),
                    home_abbr=home_abbr,
                    away_abbr=away_abbr,
                    source="ACTION_NETWORK",
                    data_quality="HIGH",
                )
            saved_count += 1

        return saved_count

    async def _save_spread_data(
        self,
        conn,
        game_id: int,
        historical_data: ActionNetworkHistoricalData,
        entry: ActionNetworkHistoricalEntry,
        market_data: list[dict],
    ) -> int:
        """Save spread data to curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread's."""
        saved_count = 0

        for market_item in market_data:
            book_id = str(market_item.get("book_id", ""))
            if book_id not in self.action_network_books:
                continue

            sportsbook_name = self.action_network_books[book_id]

            # Use new sportsbook ID resolution system
            sportsbook_id_result = await self.resolve_sportsbook_id(
                conn, book_id, "ACTION_NETWORK"
            )

            # Extract spread data - Action Network may provide data per side
            home_spread = market_item.get("home_spread")
            away_spread = market_item.get("away_spread")
            home_spread_price = market_item.get("home_spread_odds")
            away_spread_price = market_item.get("away_spread_odds")

            # If data is provided per side, extract from side-specific fields
            side = market_item.get("side", "").lower()
            if side and not (home_spread or away_spread):
                spread_value = market_item.get("value") or market_item.get("spread")
                spread_odds = market_item.get("odds")

                if side == "home":
                    home_spread = spread_value
                    home_spread_price = spread_odds
                elif side == "away":
                    away_spread = spread_value
                    away_spread_price = spread_odds

            if home_spread is None and away_spread is None:
                continue

            # Extract betting percentage data from Action Network format
            home_bets_pct = None
            away_bets_pct = None
            home_money_pct = None
            away_money_pct = None

            # Try to extract from bet_splits first (legacy format)
            bet_splits = market_item.get("bet_splits", {})
            if bet_splits:
                home_bets_pct = bet_splits.get("home_bets_percentage")
                away_bets_pct = bet_splits.get("away_bets_percentage")
                home_money_pct = bet_splits.get("home_money_percentage")
                away_money_pct = bet_splits.get("away_money_percentage")

            # Also try to extract from bet_info structure (Action Network API format)
            bet_info = market_item.get("bet_info", {})
            if bet_info:
                tickets_info = bet_info.get("tickets", {})
                money_info = bet_info.get("money", {})

                if "percent" in tickets_info:
                    side = market_item.get("side", "").lower()
                    if side == "home":
                        home_bets_pct = tickets_info["percent"]
                    elif side == "away":
                        away_bets_pct = tickets_info["percent"]

                if "percent" in money_info:
                    side = market_item.get("side", "").lower()
                    if side == "home":
                        home_money_pct = money_info["percent"]
                    elif side == "away":
                        away_money_pct = money_info["percent"]

            # Log the spread data being inserted
            self.logger.info(
                "Inserting spread data",
                game_id=historical_data.game_id,
                sportsbook=sportsbook_name,
                home_spread=home_spread,
                away_spread=away_spread,
                home_spread_price=home_spread_price,
                away_spread_price=away_spread_price,
                home_bets_pct=home_bets_pct,
                away_bets_pct=away_bets_pct,
                home_money_pct=home_money_pct,
                away_money_pct=away_money_pct,
            )

            # Insert into spreads table
            await conn.execute(
                """
                INSERT INTO curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread's (
                    game_id, sportsbook_id, sportsbook, home_spread, away_spread,
                    home_spread_price, away_spread_price, odds_timestamp,
                    home_bets_percentage, away_bets_percentage, home_money_percentage, away_money_percentage,
                    source, data_quality, game_datetime, home_team, away_team
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
                )
                ON CONFLICT (game_id, sportsbook_id, odds_timestamp)
                DO UPDATE SET
                    home_spread = EXCLUDED.home_spread,
                    away_spread = EXCLUDED.away_spread,
                    home_spread_price = EXCLUDED.home_spread_price,
                    away_spread_price = EXCLUDED.away_spread_price,
                    home_bets_percentage = EXCLUDED.home_bets_percentage,
                    away_bets_percentage = EXCLUDED.away_bets_percentage,
                    home_money_percentage = EXCLUDED.home_money_percentage,
                    away_money_percentage = EXCLUDED.away_money_percentage,
                    updated_at = NOW()
            """,
                game_id,
                sportsbook_id_result,
                sportsbook_name,
                home_spread,
                away_spread,
                home_spread_price,
                away_spread_price,
                entry.timestamp,
                home_bets_pct,
                away_bets_pct,
                home_money_pct,
                away_money_pct,
                "ACTION_NETWORK",
                "HIGH",
                historical_data.game_datetime,
                self._get_team_abbreviation(historical_data.home_team),
                self._get_team_abbreviation(historical_data.away_team),
            )
            saved_count += 1

        return saved_count

    async def _save_total_data(
        self,
        conn,
        game_id: int,
        historical_data: ActionNetworkHistoricalData,
        entry: ActionNetworkHistoricalEntry,
        market_data: list[dict],
    ) -> int:
        """Save total data to curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals'."""
        saved_count = 0

        for market_item in market_data:
            book_id = str(market_item.get("book_id", ""))
            if book_id not in self.action_network_books:
                continue

            sportsbook_name = self.action_network_books[book_id]

            # Use new sportsbook ID resolution system
            sportsbook_id_result = await self.resolve_sportsbook_id(
                conn, book_id, "ACTION_NETWORK"
            )

            # Extract total data
            total_line = market_item.get("total")
            over_odds = market_item.get("over_odds")
            under_odds = market_item.get("under_odds")

            if total_line is None:
                continue

            # Extract betting percentage data from Action Network format
            # The market_item structure depends on the data format
            over_bets_pct = None
            under_bets_pct = None
            over_money_pct = None
            under_money_pct = None

            # Try to extract from bet_splits first (legacy format)
            bet_splits = market_item.get("bet_splits", {})
            if bet_splits:
                over_bets_pct = bet_splits.get("over_bets_percentage")
                under_bets_pct = bet_splits.get("under_bets_percentage")
                over_money_pct = bet_splits.get("over_money_percentage")
                under_money_pct = bet_splits.get("under_money_percentage")

            # Also try to extract from bet_info structure (Action Network API format)
            bet_info = market_item.get("bet_info", {})
            if bet_info:
                tickets_info = bet_info.get("tickets", {})
                money_info = bet_info.get("money", {})

                # For totals, bet_info might contain overall percentages
                # that need to be split into over/under
                if "percent" in tickets_info:
                    # This would be the percentage for this specific side
                    side = market_item.get("side", "").lower()
                    if side == "over":
                        over_bets_pct = tickets_info["percent"]
                    elif side == "under":
                        under_bets_pct = tickets_info["percent"]

                if "percent" in money_info:
                    side = market_item.get("side", "").lower()
                    if side == "over":
                        over_money_pct = money_info["percent"]
                    elif side == "under":
                        under_money_pct = money_info["percent"]

            # Log the totals data being inserted
            self.logger.info(
                "Inserting totals data",
                game_id=historical_data.game_id,
                sportsbook=sportsbook_name,
                total_line=total_line,
                over_odds=over_odds,
                under_odds=under_odds,
                over_bets_pct=over_bets_pct,
                under_bets_pct=under_bets_pct,
                over_money_pct=over_money_pct,
                under_money_pct=under_money_pct,
            )

            # Insert into totals table
            await conn.execute(
                """
                INSERT INTO curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals' (
                    game_id, sportsbook_id, sportsbook, total_line, over_price, under_price, odds_timestamp,
                    over_bets_percentage, under_bets_percentage, over_money_percentage, under_money_percentage,
                    source, data_quality, game_datetime, home_team, away_team
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
                )
                ON CONFLICT (game_id, sportsbook_id, odds_timestamp)
                DO UPDATE SET
                    total_line = EXCLUDED.total_line,
                    over_price = EXCLUDED.over_price,
                    under_price = EXCLUDED.under_price,
                    over_bets_percentage = EXCLUDED.over_bets_percentage,
                    under_bets_percentage = EXCLUDED.under_bets_percentage,
                    over_money_percentage = EXCLUDED.over_money_percentage,
                    under_money_percentage = EXCLUDED.under_money_percentage,
                    updated_at = NOW()
            """,
                game_id,
                sportsbook_id_result,
                sportsbook_name,
                total_line,
                over_odds,
                under_odds,
                entry.timestamp,
                over_bets_pct,
                under_bets_pct,
                over_money_pct,
                under_money_pct,
                "ACTION_NETWORK",
                "HIGH",
                historical_data.game_datetime,
                self._get_team_abbreviation(historical_data.home_team),
                self._get_team_abbreviation(historical_data.away_team),
            )
            saved_count += 1

        return saved_count

    async def _update_extraction_log(
        self,
        conn,
        game_id: int,
        home_team: str,
        away_team: str,
        game_datetime: datetime,
        total_lines_extracted: int,
        extraction_status: str,
        error_message: str | None = None,
        history_url: str | None = None,
    ) -> None:
        """Update extraction log in operational schema."""
        await conn.execute(
            """
            INSERT INTO operational.extraction_log (
                source, game_id, home_team, away_team, game_datetime,
                last_extracted_at, total_extractions, total_lines_extracted,
                extraction_status, error_message, history_url
            ) VALUES (
                'ACTION_NETWORK', $1, $2, $3, $4, NOW(), 1, $5, $6, $7, $8
            )
            ON CONFLICT (source, game_id)
            DO UPDATE SET
                last_extracted_at = NOW(),
                total_extractions = operational.extraction_log.total_extractions + 1,
                total_lines_extracted = operational.extraction_log.total_lines_extracted + $5,
                extraction_status = $6,
                error_message = $7,
                history_url = $8
        """,
            game_id,
            home_team,
            away_team,
            game_datetime,
            total_lines_extracted,
            extraction_status,
            error_message,
            history_url,
        )

    async def get_last_extraction_time(self, game_id: int) -> datetime | None:
        """Get last extraction time for a game."""
        try:
            async with self.connection.get_async_connection() as conn:
                result = await conn.fetchval(
                    """
                    SELECT last_extracted_at
                    FROM operational.extraction_log
                    WHERE source = 'ACTION_NETWORK' AND game_id = $1
                """,
                    game_id,
                )
                return result
        except Exception as e:
            self.logger.error(
                "Failed to get last extraction time", game_id=game_id, error=str(e)
            )
            return None

    async def get_new_lines_since_last_extraction(
        self,
        game_id: int,
        book_id: int | None = None,
        market_type: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get new betting lines since last extraction."""
        try:
            last_extraction = await self.get_last_extraction_time(game_id)
            if not last_extraction:
                last_extraction = datetime.now() - timedelta(
                    days=7
                )  # Default to 7 days ago

            results = []
            async with self.connection.get_async_connection() as conn:
                # Query moneyline data
                if not market_type or market_type == "moneyline":
                    query = """
                        SELECT 'moneyline' as market_type, *
                        FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline'
                        WHERE game_id = $1 AND created_at > $2 AND source = 'ACTION_NETWORK'
                    """
                    if book_id:
                        query += " AND sportsbook_id = $3"
                        rows = await conn.fetch(
                            query, game_id, last_extraction, book_id
                        )
                    else:
                        rows = await conn.fetch(query, game_id, last_extraction)
                    results.extend([dict(row) for row in rows])

                # Query spread data
                if not market_type or market_type == "spread":
                    query = """
                        SELECT 'spread' as market_type, *
                        FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread's
                        WHERE game_id = $1 AND created_at > $2 AND source = 'ACTION_NETWORK'
                    """
                    if book_id:
                        query += " AND sportsbook_id = $3"
                        rows = await conn.fetch(
                            query, game_id, last_extraction, book_id
                        )
                    else:
                        rows = await conn.fetch(query, game_id, last_extraction)
                    results.extend([dict(row) for row in rows])

                # Query total data
                if not market_type or market_type == "total":
                    query = """
                        SELECT 'total' as market_type, *
                        FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals'
                        WHERE game_id = $1 AND created_at > $2 AND source = 'ACTION_NETWORK'
                    """
                    if book_id:
                        query += " AND sportsbook_id = $3"
                        rows = await conn.fetch(
                            query, game_id, last_extraction, book_id
                        )
                    else:
                        rows = await conn.fetch(query, game_id, last_extraction)
                    results.extend([dict(row) for row in rows])

            return results

        except Exception as e:
            self.logger.error("Failed to get new lines", game_id=game_id, error=str(e))
            return []

    async def health_check(self) -> dict[str, Any]:
        """Perform health check on the repository."""
        try:
            async with self.connection.get_async_connection() as conn:
                # Check if we can query each table
                moneyline_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline' WHERE source = 'ACTION_NETWORK'"
                )
                spread_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread's WHERE source = 'ACTION_NETWORK'"
                )
                total_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals' WHERE source = 'ACTION_NETWORK'"
                )
                sportsbook_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM curated.sportsbooks WHERE name IN ('DraftKings', 'FanDuel', 'BetMGM', 'Caesars', 'BetRivers', 'ESPN BET')"
                )

                return {
                    "status": "healthy",
                    "action_network_moneyline_records": moneyline_count,
                    "action_network_spread_records": spread_count,
                    "action_network_total_records": total_count,
                    "action_network_sportsbooks": sportsbook_count,
                    "timestamp": datetime.now().isoformat(),
                }
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
            }
