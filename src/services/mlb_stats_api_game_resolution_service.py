#!/usr/bin/env python3
"""
MLB Stats API Game Resolution Service

Enhanced service for resolving external game IDs to MLB Stats API game IDs and standardizing team names.
Integrates with the unified betting lines system for comprehensive game matching.
"""

import asyncio
from dataclasses import dataclass
from datetime import date, timedelta
from enum import Enum
from typing import Any

import aiohttp
import psycopg2
import structlog
from psycopg2.extras import RealDictCursor

from ..core.config import UnifiedSettings
from ..data.collection.base import DataSource

logger = structlog.get_logger(__name__)


class MatchConfidence(Enum):
    """Confidence levels for game matching."""
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    NONE = "NONE"


@dataclass
class GameMatchResult:
    """Result of a game matching operation."""
    game_id: int | None
    mlb_game_id: str | None
    confidence: MatchConfidence
    match_method: str
    match_details: dict[str, Any]


@dataclass
class TeamMapping:
    """Team name mapping information."""
    official_name: str
    abbreviation: str
    aliases: list[str]
    mlb_team_id: int
    conference: str  # AL or NL
    division: str    # EAST, CENTRAL, WEST


class MLBStatsAPIGameResolutionService:
    """
    Enhanced MLB Stats API Game Resolution Service.
    
    Provides comprehensive game ID resolution and team name standardization
    for the unified betting lines system.
    """

    def __init__(self):
        self.settings = UnifiedSettings()
        self.logger = logger.bind(component="MLBStatsAPIGameResolutionService")
        self.session = None
        self.team_mappings = {}
        self.game_cache = {}

        # Initialize team mappings
        self._initialize_team_mappings()

    def _initialize_team_mappings(self):
        """Initialize comprehensive team name mappings."""
        self.team_mappings = {
            # American League East
            "BAL": TeamMapping("Baltimore Orioles", "BAL", ["Baltimore", "Orioles", "O's"], 110, "AL", "EAST"),
            "BOS": TeamMapping("Boston Red Sox", "BOS", ["Boston", "Red Sox", "Sox"], 111, "AL", "EAST"),
            "NYY": TeamMapping("New York Yankees", "NYY", ["New York", "Yankees", "NY Yankees", "Yanks"], 147, "AL", "EAST"),
            "TB": TeamMapping("Tampa Bay Rays", "TB", ["Tampa Bay", "Rays", "Tampa", "Devil Rays"], 139, "AL", "EAST"),
            "TOR": TeamMapping("Toronto Blue Jays", "TOR", ["Toronto", "Blue Jays", "Jays"], 141, "AL", "EAST"),

            # American League Central
            "CHW": TeamMapping("Chicago White Sox", "CHW", ["Chicago", "White Sox", "ChiSox"], 145, "AL", "CENTRAL"),
            "CLE": TeamMapping("Cleveland Guardians", "CLE", ["Cleveland", "Guardians", "Indians"], 114, "AL", "CENTRAL"),
            "DET": TeamMapping("Detroit Tigers", "DET", ["Detroit", "Tigers"], 116, "AL", "CENTRAL"),
            "KC": TeamMapping("Kansas City Royals", "KC", ["Kansas City", "Royals", "K.C."], 118, "AL", "CENTRAL"),
            "MIN": TeamMapping("Minnesota Twins", "MIN", ["Minnesota", "Twins"], 142, "AL", "CENTRAL"),

            # American League West
            "HOU": TeamMapping("Houston Astros", "HOU", ["Houston", "Astros"], 117, "AL", "WEST"),
            "LAA": TeamMapping("Los Angeles Angels", "LAA", ["Los Angeles", "Angels", "LA Angels", "Anaheim"], 108, "AL", "WEST"),
            "OAK": TeamMapping("Oakland Athletics", "OAK", ["Oakland", "Athletics", "A's"], 133, "AL", "WEST"),
            "SEA": TeamMapping("Seattle Mariners", "SEA", ["Seattle", "Mariners", "M's"], 136, "AL", "WEST"),
            "TEX": TeamMapping("Texas Rangers", "TEX", ["Texas", "Rangers"], 140, "AL", "WEST"),

            # National League East
            "ATL": TeamMapping("Atlanta Braves", "ATL", ["Atlanta", "Braves"], 144, "NL", "EAST"),
            "MIA": TeamMapping("Miami Marlins", "MIA", ["Miami", "Marlins", "Florida"], 146, "NL", "EAST"),
            "NYM": TeamMapping("New York Mets", "NYM", ["New York", "Mets", "NY Mets"], 121, "NL", "EAST"),
            "PHI": TeamMapping("Philadelphia Phillies", "PHI", ["Philadelphia", "Phillies", "Phils"], 143, "NL", "EAST"),
            "WSH": TeamMapping("Washington Nationals", "WSH", ["Washington", "Nationals", "Nats"], 120, "NL", "EAST"),

            # National League Central
            "CHC": TeamMapping("Chicago Cubs", "CHC", ["Chicago", "Cubs", "ChiCubs"], 112, "NL", "CENTRAL"),
            "CIN": TeamMapping("Cincinnati Reds", "CIN", ["Cincinnati", "Reds"], 113, "NL", "CENTRAL"),
            "MIL": TeamMapping("Milwaukee Brewers", "MIL", ["Milwaukee", "Brewers"], 158, "NL", "CENTRAL"),
            "PIT": TeamMapping("Pittsburgh Pirates", "PIT", ["Pittsburgh", "Pirates"], 134, "NL", "CENTRAL"),
            "STL": TeamMapping("St. Louis Cardinals", "STL", ["St. Louis", "Cardinals", "StL", "Cards"], 138, "NL", "CENTRAL"),

            # National League West
            "ARI": TeamMapping("Arizona Diamondbacks", "ARI", ["Arizona", "Diamondbacks", "D-backs"], 109, "NL", "WEST"),
            "COL": TeamMapping("Colorado Rockies", "COL", ["Colorado", "Rockies"], 115, "NL", "WEST"),
            "LAD": TeamMapping("Los Angeles Dodgers", "LAD", ["Los Angeles", "Dodgers", "LA Dodgers"], 119, "NL", "WEST"),
            "SD": TeamMapping("San Diego Padres", "SD", ["San Diego", "Padres"], 135, "NL", "WEST"),
            "SF": TeamMapping("San Francisco Giants", "SF", ["San Francisco", "Giants", "SF Giants"], 137, "NL", "WEST"),
        }

    async def initialize(self):
        """Initialize the service."""
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(timeout=timeout)

        # Load recent games into cache
        await self._load_recent_games_cache()

    async def cleanup(self):
        """Cleanup resources."""
        if self.session:
            await self.session.close()

    async def _load_recent_games_cache(self):
        """Load recent games from MLB Stats API into cache."""
        try:
            # Load games from past 30 days
            end_date = date.today()
            start_date = end_date - timedelta(days=30)

            current_date = start_date
            while current_date <= end_date:
                games = await self._fetch_mlb_games_for_date(current_date)
                for game in games:
                    self.game_cache[game['gamePk']] = game
                current_date += timedelta(days=1)

                # Rate limiting
                await asyncio.sleep(0.1)

            self.logger.info(f"Loaded {len(self.game_cache)} games into cache")

        except Exception as e:
            self.logger.error("Error loading games cache", error=str(e))

    async def _fetch_mlb_games_for_date(self, game_date: date) -> list[dict[str, Any]]:
        """Fetch MLB games for a specific date."""
        try:
            url = "https://statsapi.mlb.com/api/v1/schedule"
            params = {
                'sportId': '1',
                'date': game_date.isoformat(),
                'hydrate': 'game(content(editorial(recap)),decisions),linescore,boxscore'
            }

            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    games = []

                    for date_entry in data.get('dates', []):
                        for game in date_entry.get('games', []):
                            games.append(game)

                    return games
                else:
                    self.logger.warning("MLB API request failed", status=response.status, date=game_date)
                    return []

        except Exception as e:
            self.logger.error("Error fetching MLB games", date=game_date, error=str(e))
            return []

    def standardize_team_name(self, team_name: str) -> str | None:
        """
        Standardize team name to official abbreviation.
        
        Args:
            team_name: Team name in any format
            
        Returns:
            Standardized team abbreviation or None if not found
        """
        if not team_name:
            return None

        # Clean input
        clean_name = team_name.strip()

        # Direct abbreviation match
        if clean_name.upper() in self.team_mappings:
            return clean_name.upper()

        # Search through aliases
        for abbr, mapping in self.team_mappings.items():
            if clean_name.lower() in [alias.lower() for alias in mapping.aliases]:
                return abbr

            # Partial matches
            if any(alias.lower() in clean_name.lower() for alias in mapping.aliases):
                return abbr

        # Fuzzy matching for common variations
        fuzzy_mappings = {
            'yanks': 'NYY',
            'sox': 'BOS',  # Default to Red Sox for 'sox' - context needed for White Sox
            'mets': 'NYM',
            'cubs': 'CHC',
            'cards': 'STL',
            'nats': 'WSH',
            'jays': 'TOR',
            'rays': 'TB',
            'astros': 'HOU',
            'dodgers': 'LAD',
            'giants': 'SF',
            'padres': 'SD',
            'mariners': 'SEA',
            'angels': 'LAA',
            'rangers': 'TEX',
            'athletics': 'OAK',
            'twins': 'MIN',
            'tigers': 'DET',
            'royals': 'KC',
            'guardians': 'CLE',
            'orioles': 'BAL',
            'phillies': 'PHI',
            'braves': 'ATL',
            'marlins': 'MIA',
            'reds': 'CIN',
            'brewers': 'MIL',
            'pirates': 'PIT',
            'diamondbacks': 'ARI',
            'rockies': 'COL',
        }

        for keyword, abbr in fuzzy_mappings.items():
            if keyword in clean_name.lower():
                return abbr

        self.logger.warning(f"Could not standardize team name: {team_name}")
        return None

    def get_team_info(self, team_identifier: str) -> TeamMapping | None:
        """
        Get comprehensive team information.
        
        Args:
            team_identifier: Team name or abbreviation
            
        Returns:
            TeamMapping object or None if not found
        """
        abbr = self.standardize_team_name(team_identifier)
        if abbr:
            return self.team_mappings.get(abbr)
        return None

    async def resolve_game_id(
        self,
        external_id: str,
        source: DataSource,
        home_team: str = None,
        away_team: str = None,
        game_date: date = None
    ) -> GameMatchResult:
        """
        Resolve external game ID to internal game ID.
        
        Args:
            external_id: External game identifier
            source: Data source
            home_team: Home team name (optional, improves matching)
            away_team: Away team name (optional, improves matching)
            game_date: Game date (optional, improves matching)
            
        Returns:
            GameMatchResult with matching information
        """
        try:
            # First try direct database lookup
            db_result = await self._lookup_game_in_database(external_id, source)
            if db_result.game_id:
                return db_result

            # If not found in database, try to match with MLB Stats API
            if home_team and away_team:
                api_result = await self._match_game_with_mlb_api(
                    home_team, away_team, game_date, external_id, source
                )
                if api_result.game_id:
                    # Store the match in database for future use
                    await self._store_game_match(api_result, external_id, source)
                    return api_result

            # No match found
            return GameMatchResult(
                game_id=None,
                mlb_game_id=None,
                confidence=MatchConfidence.NONE,
                match_method="no_match",
                match_details={"external_id": external_id, "source": source.value}
            )

        except Exception as e:
            self.logger.error("Error resolving game ID",
                            external_id=external_id,
                            source=source.value,
                            error=str(e))
            return GameMatchResult(
                game_id=None,
                mlb_game_id=None,
                confidence=MatchConfidence.NONE,
                match_method="error",
                match_details={"error": str(e)}
            )

    async def _lookup_game_in_database(self, external_id: str, source: DataSource) -> GameMatchResult:
        """Look up game in database by external ID."""
        try:
            with psycopg2.connect(
                host=self.settings.database.host,
                port=self.settings.database.port,
                database=self.settings.database.database,
                user=self.settings.database.user,
                password=self.settings.database.password,
                cursor_factory=RealDictCursor
            ) as conn:
                with conn.cursor() as cur:
                    # Map source to database column
                    source_column_map = {
                        DataSource.SPORTS_BOOK_REVIEW_DEPRECATED: 'sportsbookreview_game_id',
                        DataSource.ACTION_NETWORK: 'action_network_game_id',
                        DataSource.VSIN: 'vsin_game_id',
                        DataSource.SPORTS_BETTING_DIME: 'sbd_game_id',
                        DataSource.MLB_STATS_API: 'mlb_stats_api_game_id'
                    }

                    column = source_column_map.get(source)
                    if not column:
                        return GameMatchResult(
                            game_id=None,
                            mlb_game_id=None,
                            confidence=MatchConfidence.NONE,
                            match_method="unsupported_source",
                            match_details={"source": source.value}
                        )

                    # Query the database
                    cur.execute(
                        f"SELECT id, mlb_stats_api_game_id, home_team, away_team, game_date FROM core_betting.games WHERE {column} = %s",
                        (external_id,)
                    )

                    result = cur.fetchone()
                    if result:
                        return GameMatchResult(
                            game_id=result['id'],
                            mlb_game_id=result['mlb_stats_api_game_id'],
                            confidence=MatchConfidence.HIGH,
                            match_method="database_lookup",
                            match_details={
                                "external_id": external_id,
                                "source": source.value,
                                "home_team": result['home_team'],
                                "away_team": result['away_team'],
                                "game_date": result['game_date'].isoformat() if result['game_date'] else None
                            }
                        )

                    return GameMatchResult(
                        game_id=None,
                        mlb_game_id=None,
                        confidence=MatchConfidence.NONE,
                        match_method="not_found_in_database",
                        match_details={"external_id": external_id, "source": source.value}
                    )

        except Exception as e:
            self.logger.error("Database lookup failed", error=str(e))
            raise

    async def _match_game_with_mlb_api(
        self,
        home_team: str,
        away_team: str,
        game_date: date = None,
        external_id: str = None,
        source: DataSource = None
    ) -> GameMatchResult:
        """Match game with MLB Stats API data."""
        try:
            # Standardize team names
            home_abbr = self.standardize_team_name(home_team)
            away_abbr = self.standardize_team_name(away_team)

            if not home_abbr or not away_abbr:
                return GameMatchResult(
                    game_id=None,
                    mlb_game_id=None,
                    confidence=MatchConfidence.NONE,
                    match_method="team_standardization_failed",
                    match_details={
                        "home_team": home_team,
                        "away_team": away_team,
                        "home_abbr": home_abbr,
                        "away_abbr": away_abbr
                    }
                )

            # Get team info
            home_team_info = self.team_mappings[home_abbr]
            away_team_info = self.team_mappings[away_abbr]

            # Search for matching games
            search_dates = []
            if game_date:
                search_dates = [game_date]
            else:
                # Search recent days if no date provided
                today = date.today()
                search_dates = [today - timedelta(days=i) for i in range(7)]

            for search_date in search_dates:
                games = await self._fetch_mlb_games_for_date(search_date)

                for game in games:
                    game_home_id = game.get('teams', {}).get('home', {}).get('team', {}).get('id')
                    game_away_id = game.get('teams', {}).get('away', {}).get('team', {}).get('id')

                    if (game_home_id == home_team_info.mlb_team_id and
                        game_away_id == away_team_info.mlb_team_id):

                        # Found a match!
                        game_pk = str(game['gamePk'])
                        confidence = MatchConfidence.HIGH if game_date else MatchConfidence.MEDIUM

                        return GameMatchResult(
                            game_id=None,  # Will be set when stored
                            mlb_game_id=game_pk,
                            confidence=confidence,
                            match_method="mlb_api_match",
                            match_details={
                                "game_pk": game_pk,
                                "home_team": home_abbr,
                                "away_team": away_abbr,
                                "game_date": search_date.isoformat(),
                                "external_id": external_id,
                                "source": source.value if source else None
                            }
                        )

            # No match found
            return GameMatchResult(
                game_id=None,
                mlb_game_id=None,
                confidence=MatchConfidence.NONE,
                match_method="no_mlb_api_match",
                match_details={
                    "home_team": home_team,
                    "away_team": away_team,
                    "home_abbr": home_abbr,
                    "away_abbr": away_abbr,
                    "search_dates": [d.isoformat() for d in search_dates]
                }
            )

        except Exception as e:
            self.logger.error("Error matching with MLB API", error=str(e))
            raise

    async def _store_game_match(self, match_result: GameMatchResult, external_id: str, source: DataSource):
        """Store a successful game match in the database."""
        try:
            with psycopg2.connect(
                host=self.settings.database.host,
                port=self.settings.database.port,
                database=self.settings.database.database,
                user=self.settings.database.user,
                password=self.settings.database.password,
                cursor_factory=RealDictCursor
            ) as conn:
                with conn.cursor() as cur:
                    # Map source to database column
                    source_column_map = {
                        DataSource.SPORTS_BOOK_REVIEW_DEPRECATED: 'sportsbookreview_game_id',
                        DataSource.ACTION_NETWORK: 'action_network_game_id',
                        DataSource.VSIN: 'vsin_game_id',
                        DataSource.SPORTS_BETTING_DIME: 'sbd_game_id'
                    }

                    column = source_column_map.get(source)
                    if not column:
                        return

                    details = match_result.match_details

                    # Check if game already exists
                    cur.execute(
                        "SELECT id FROM core_betting.games WHERE mlb_stats_api_game_id = %s",
                        (match_result.mlb_game_id,)
                    )

                    existing_game = cur.fetchone()
                    if existing_game:
                        # Update existing game with new external ID
                        cur.execute(
                            f"UPDATE core_betting.games SET {column} = %s, updated_at = NOW() WHERE id = %s",
                            (external_id, existing_game['id'])
                        )
                        match_result.game_id = existing_game['id']
                    else:
                        # Insert new game
                        cur.execute(
                            f"""
                            INSERT INTO core_betting.games 
                            (mlb_stats_api_game_id, {column}, home_team, away_team, game_date, game_datetime, data_quality, has_mlb_enrichment)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            RETURNING id
                            """,
                            (
                                match_result.mlb_game_id,
                                external_id,
                                details.get('home_team'),
                                details.get('away_team'),
                                details.get('game_date'),
                                details.get('game_date'),  # Use game_date as datetime for now
                                'HIGH' if match_result.confidence == MatchConfidence.HIGH else 'MEDIUM',
                                True
                            )
                        )

                        result = cur.fetchone()
                        match_result.game_id = result['id']

                    conn.commit()

        except Exception as e:
            self.logger.error("Error storing game match", error=str(e))
            raise

    async def enrich_game_with_mlb_data(self, game_id: int) -> bool:
        """
        Enrich game with MLB Stats API data.
        
        Args:
            game_id: Internal game ID
            
        Returns:
            True if enrichment was successful, False otherwise
        """
        try:
            with psycopg2.connect(
                host=self.settings.database.host,
                port=self.settings.database.port,
                database=self.settings.database.database,
                user=self.settings.database.user,
                password=self.settings.database.password,
                cursor_factory=RealDictCursor
            ) as conn:
                with conn.cursor() as cur:
                    # Get game info
                    cur.execute(
                        "SELECT mlb_stats_api_game_id, has_mlb_enrichment FROM core_betting.games WHERE id = %s",
                        (game_id,)
                    )

                    game_record = cur.fetchone()
                    if not game_record or not game_record['mlb_stats_api_game_id']:
                        return False

                    if game_record['has_mlb_enrichment']:
                        return True  # Already enriched

                    # Fetch game data from MLB API
                    game_data = await self._fetch_game_details(game_record['mlb_stats_api_game_id'])
                    if not game_data:
                        return False

                    # Extract enrichment data
                    enrichment_data = self._extract_enrichment_data(game_data)

                    # Update game record
                    cur.execute(
                        """
                        UPDATE core_betting.games 
                        SET 
                            venue_name = %s,
                            venue_id = %s,
                            season = %s,
                            game_status = %s,
                            home_score = %s,
                            away_score = %s,
                            winning_team = %s,
                            weather_condition = %s,
                            temperature = %s,
                            wind_speed = %s,
                            wind_direction = %s,
                            has_mlb_enrichment = TRUE,
                            data_quality = 'HIGH',
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (
                            enrichment_data.get('venue_name'),
                            enrichment_data.get('venue_id'),
                            enrichment_data.get('season'),
                            enrichment_data.get('game_status'),
                            enrichment_data.get('home_score'),
                            enrichment_data.get('away_score'),
                            enrichment_data.get('winning_team'),
                            enrichment_data.get('weather_condition'),
                            enrichment_data.get('temperature'),
                            enrichment_data.get('wind_speed'),
                            enrichment_data.get('wind_direction'),
                            game_id
                        )
                    )

                    conn.commit()
                    return True

        except Exception as e:
            self.logger.error("Error enriching game with MLB data", game_id=game_id, error=str(e))
            return False

    async def _fetch_game_details(self, mlb_game_id: str) -> dict[str, Any] | None:
        """Fetch detailed game information from MLB Stats API."""
        try:
            url = f"https://statsapi.mlb.com/api/v1/game/{mlb_game_id}/feed/live"

            async with self.session.get(url) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    self.logger.warning("MLB API request failed", status=response.status, mlb_game_id=mlb_game_id)
                    return None

        except Exception as e:
            self.logger.error("Error fetching game details", mlb_game_id=mlb_game_id, error=str(e))
            return None

    def _extract_enrichment_data(self, game_data: dict[str, Any]) -> dict[str, Any]:
        """Extract enrichment data from MLB API response."""
        enrichment = {}

        try:
            game_info = game_data.get('gameData', {})
            live_data = game_data.get('liveData', {})

            # Venue information
            venue = game_info.get('venue', {})
            enrichment['venue_name'] = venue.get('name')
            enrichment['venue_id'] = venue.get('id')

            # Season information
            enrichment['season'] = game_info.get('game', {}).get('season')

            # Game status
            status = game_info.get('status', {})
            enrichment['game_status'] = status.get('detailedState', '').lower()

            # Scores
            linescore = live_data.get('linescore', {})
            if linescore:
                enrichment['home_score'] = linescore.get('teams', {}).get('home', {}).get('runs')
                enrichment['away_score'] = linescore.get('teams', {}).get('away', {}).get('runs')

                # Determine winner
                home_score = enrichment.get('home_score', 0)
                away_score = enrichment.get('away_score', 0)
                if home_score > away_score:
                    enrichment['winning_team'] = 'home'
                elif away_score > home_score:
                    enrichment['winning_team'] = 'away'

            # Weather information
            weather = game_info.get('weather', {})
            if weather:
                enrichment['weather_condition'] = weather.get('condition')
                enrichment['temperature'] = weather.get('temp')
                enrichment['wind_speed'] = weather.get('wind')
                enrichment['wind_direction'] = weather.get('windDirection')

        except Exception as e:
            self.logger.error("Error extracting enrichment data", error=str(e))

        return enrichment

    async def bulk_resolve_games(self, game_requests: list[dict[str, Any]]) -> list[GameMatchResult]:
        """
        Bulk resolve multiple games for efficiency.
        
        Args:
            game_requests: List of dicts with keys: external_id, source, home_team, away_team, game_date
            
        Returns:
            List of GameMatchResult objects
        """
        results = []

        for request in game_requests:
            try:
                result = await self.resolve_game_id(
                    external_id=request['external_id'],
                    source=request['source'],
                    home_team=request.get('home_team'),
                    away_team=request.get('away_team'),
                    game_date=request.get('game_date')
                )
                results.append(result)

                # Rate limiting for bulk operations
                await asyncio.sleep(0.1)

            except Exception as e:
                self.logger.error("Error in bulk resolve", request=request, error=str(e))
                results.append(GameMatchResult(
                    game_id=None,
                    mlb_game_id=None,
                    confidence=MatchConfidence.NONE,
                    match_method="bulk_error",
                    match_details={"error": str(e)}
                ))

        return results

    def validate_team_matchup(self, home_team: str, away_team: str) -> bool:
        """
        Validate that a team matchup is valid (teams exist and are different).
        
        Args:
            home_team: Home team identifier
            away_team: Away team identifier
            
        Returns:
            True if valid matchup, False otherwise
        """
        home_abbr = self.standardize_team_name(home_team)
        away_abbr = self.standardize_team_name(away_team)

        return (home_abbr and away_abbr and
                home_abbr != away_abbr and
                home_abbr in self.team_mappings and
                away_abbr in self.team_mappings)

    def get_division_matchups(self, division: str, conference: str) -> list[tuple[str, str]]:
        """
        Get all possible matchups within a division.
        
        Args:
            division: Division name (EAST, CENTRAL, WEST)
            conference: Conference name (AL, NL)
            
        Returns:
            List of (home_team, away_team) tuples
        """
        division_teams = [
            abbr for abbr, mapping in self.team_mappings.items()
            if mapping.division == division and mapping.conference == conference
        ]

        matchups = []
        for i, home_team in enumerate(division_teams):
            for j, away_team in enumerate(division_teams):
                if i != j:
                    matchups.append((home_team, away_team))

        return matchups


# Example usage
if __name__ == "__main__":
    async def main():
        service = MLBStatsAPIGameResolutionService()
        await service.initialize()

        try:
            # Test team standardization
            print("Team standardization tests:")
            print(f"Yankees -> {service.standardize_team_name('Yankees')}")
            print(f"Red Sox -> {service.standardize_team_name('Red Sox')}")
            print(f"LAD -> {service.standardize_team_name('LAD')}")

            # Test game resolution
            print("\nGame resolution test:")
            result = await service.resolve_game_id(
                external_id="test_12345",
                source=DataSource.ACTION_NETWORK,
                home_team="Yankees",
                away_team="Red Sox",
                game_date=date.today()
            )
            print(f"Resolution result: {result}")

            # Test team info
            print("\nTeam info test:")
            team_info = service.get_team_info("Yankees")
            print(f"Yankees info: {team_info}")

        finally:
            await service.cleanup()

    asyncio.run(main())
