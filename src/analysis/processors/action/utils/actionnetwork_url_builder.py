"""
Action Network URL Builder

This module builds Action Network game data URLs by combining:
1. Game data from Action Network's scoreboard API
2. Dynamically extracted Next.js build ID
3. Team name mapping and date formatting
"""

import re
from datetime import datetime

import requests

try:
    import structlog

    logger = structlog.get_logger(__name__)
except ImportError:
    import logging

    logger = logging.getLogger(__name__)

import psycopg2
from psycopg2.extras import RealDictCursor

from .actionnetwork_build_extractor import ActionNetworkBuildExtractor


class ActionNetworkURLBuilder:
    """Build Action Network game data URLs dynamically."""

    SCOREBOARD_API_URL = "https://api.actionnetwork.com/web/v2/scoreboard/proreport/mlb"
    BASE_PARAMS = {
        "bookIds": "15,30,2194,2292,2888,2889,2890,3118,3120,2891,281",
        "periods": "event",
    }

    def __init__(self, cache_build_id: bool = True, db_config: dict | None = None):
        """
        Initialize the URL builder.

        Args:
            cache_build_id: Whether to cache the build ID (recommended)
            db_config: Optional database configuration dict. If None, attempts to load from settings.
        """
        self.cache_build_id = cache_build_id
        self._cached_build_id: str | None = None
        self._build_id_timestamp: float | None = None
        self.logger = logger.bind(builder="ActionNetworkURL")

        # Build ID cache TTL (1 hour)
        self.build_id_ttl = 3600

        # Team slug mapping (loaded from database)
        self._team_slug_map: dict[str, str] | None = None
        self._team_slug_cache_time: float | None = None
        self.team_slug_ttl = 3600  # Cache team slugs for 1 hour

        # Database configuration
        self.db_config = db_config or self._get_db_config()

    def _get_db_config(self) -> dict:
        """Get database configuration from settings or environment."""
        try:
            # Try to import from the main project settings
            import os
            import sys

            # Add the project root to the path to import settings
            project_root = os.path.dirname(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            )
            sys.path.insert(0, project_root)

            from src.core.config import get_settings

            settings = get_settings()

            return {
                "host": settings.database.host,
                "port": settings.database.port,
                "database": settings.database.database,
                "user": settings.database.user,
                "password": settings.database.password,
            }
        except ImportError:
            # Fallback to environment variables
            self.logger.warning(
                "Could not import settings, using environment variables"
            )
            return {
                "host": os.getenv("POSTGRES_HOST", "localhost"),
                "port": int(os.getenv("POSTGRES_PORT", "5432")),
                "database": os.getenv("POSTGRES_DB", "mlb_betting"),
                "user": os.getenv("POSTGRES_USER", "samlafell"),
                "password": os.getenv("POSTGRES_PASSWORD", ""),
            }

    def _load_team_slug_map(self) -> dict[str, str]:
        """Load team slug mapping from the action.dim_teams table."""
        try:
            with psycopg2.connect(
                cursor_factory=RealDictCursor, **self.db_config
            ) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT full_name, url_slug 
                        FROM action.dim_teams 
                        ORDER BY full_name;
                    """)
                    results = cursor.fetchall()

                    slug_map = {row["full_name"]: row["url_slug"] for row in results}

                    self.logger.info(
                        f"Loaded {len(slug_map)} team slug mappings from database"
                    )
                    return slug_map

        except Exception as e:
            self.logger.error(f"Failed to load team slugs from database: {e}")

            # Fallback to hardcoded mapping
            self.logger.warning("Using fallback hardcoded team slug mappings")
            return {
                "New York Yankees": "new-york-yankees",
                "Toronto Blue Jays": "toronto-blue-jays",
                "St. Louis Cardinals": "st.-louis-cardinals",
                "Pittsburgh Pirates": "pittsburgh-pirates",
                "San Diego Padres": "san-diego-padres",
                "Philadelphia Phillies": "philadelphia-phillies",
                "Minnesota Twins": "minnesota-twins",
                "Miami Marlins": "miami-marlins",
                "Detroit Tigers": "detroit-tigers",
                "Washington Nationals": "washington-nationals",
                "Oakland Athletics": "oakland-athletics",
                "Tampa Bay Rays": "tampa-bay-rays",
                "Cincinnati Reds": "cincinnati-reds",
                "Boston Red Sox": "boston-red-sox",
                "Los Angeles Angels": "los-angeles-angels",
                "Atlanta Braves": "atlanta-braves",
                "Milwaukee Brewers": "milwaukee-brewers",
                "New York Mets": "new-york-mets",
                "Baltimore Orioles": "baltimore-orioles",
                "Texas Rangers": "texas-rangers",
                "Cleveland Guardians": "cleveland-guardians",
                "Chicago Cubs": "chicago-cubs",
                "Houston Astros": "houston-astros",
                "Colorado Rockies": "colorado-rockies",
                "San Francisco Giants": "san-francisco-giants",
                "Arizona Diamondbacks": "arizona-diamondbacks",
                "Kansas City Royals": "kansas-city-royals",
                "Seattle Mariners": "seattle-mariners",
                "Chicago White Sox": "chicago-white-sox",
                "Los Angeles Dodgers": "los-angeles-dodgers",
            }

    def _get_team_slug_map(self) -> dict[str, str]:
        """Get team slug mapping, using cache if available."""
        now = datetime.now().timestamp()

        # Check if we have a valid cached mapping
        if (
            self._team_slug_map
            and self._team_slug_cache_time
            and (now - self._team_slug_cache_time) < self.team_slug_ttl
        ):
            return self._team_slug_map

        # Load fresh mapping from database
        self._team_slug_map = self._load_team_slug_map()
        self._team_slug_cache_time = now

        return self._team_slug_map

    def _get_build_id(self) -> str:
        """Get the current build ID, using cache if available."""
        now = datetime.now().timestamp()

        # Check if we have a valid cached build ID
        if (
            self.cache_build_id
            and self._cached_build_id
            and self._build_id_timestamp
            and (now - self._build_id_timestamp) < self.build_id_ttl
        ):
            self.logger.debug("Using cached build ID", build_id=self._cached_build_id)
            return self._cached_build_id

        # Extract new build ID
        self.logger.info("Extracting fresh build ID")
        extractor = ActionNetworkBuildExtractor(headless=True)
        build_id = extractor.extract_build_id()

        if not build_id:
            if self._cached_build_id:
                self.logger.warning("Build ID extraction failed, using stale cached ID")
                return self._cached_build_id
            else:
                raise ValueError(
                    "Failed to extract build ID and no cached version available"
                )

        # Cache the new build ID
        if self.cache_build_id:
            self._cached_build_id = build_id
            self._build_id_timestamp = now

        self.logger.info("Successfully extracted build ID", build_id=build_id)
        return build_id

    def _normalize_team_name(self, team_name: str) -> str:
        """Normalize team name to URL slug format."""
        # Get the current team slug mapping from database
        team_slug_map = self._get_team_slug_map()

        # First try direct mapping
        if team_name in team_slug_map:
            return team_slug_map[team_name]

        # Try common name variations
        name_variations = [
            # Remove location prefixes for teams that might not include them in DB
            re.sub(
                r"^(Oakland|Los Angeles|New York|San Francisco|San Diego|St\.|Saint)\s+",
                "",
                team_name,
            ),
            # Add common location prefixes for shortened names
            f"Oakland {team_name}" if team_name == "Athletics" else None,
            f"Los Angeles {team_name}" if team_name == "Angels" else None,
            f"New York {team_name}" if team_name in ["Yankees", "Mets"] else None,
            f"San Francisco {team_name}" if team_name == "Giants" else None,
            f"San Diego {team_name}" if team_name == "Padres" else None,
        ]

        # Remove None values and duplicates
        name_variations = list(
            set([v for v in name_variations if v and v != team_name])
        )

        # Try each variation
        for variation in name_variations:
            if variation in team_slug_map:
                self.logger.debug(
                    f"Found team '{team_name}' using variation '{variation}'"
                )
                return team_slug_map[variation]

        # Fallback: convert to slug format
        # Remove common words and convert to lowercase with hyphens
        slug = team_name.lower()

        # Remove common location words if they're standalone
        slug = re.sub(r"\b(new|los|san|st\.?|saint)\b", "", slug)

        # Replace spaces and dots with hyphens
        slug = re.sub(r"[\s\.]+", "-", slug)

        # Remove leading/trailing hyphens
        slug = slug.strip("-")

        self.logger.warning(
            f"No mapping found for team '{team_name}', using generated slug: {slug}"
        )
        return slug

    def _format_date(self, date: datetime) -> str:
        """Format date for Action Network URLs (e.g., 'july-1-2025')."""
        months = [
            "january",
            "february",
            "march",
            "april",
            "may",
            "june",
            "july",
            "august",
            "september",
            "october",
            "november",
            "december",
        ]
        month_name = months[date.month - 1]
        return f"{month_name}-{date.day}-{date.year}"

    def _create_team_slug(self, away_team: str, home_team: str) -> str:
        """Create the team slug part of the URL."""
        away_slug = self._normalize_team_name(away_team)
        home_slug = self._normalize_team_name(home_team)
        return f"{away_slug}-{home_slug}"

    def get_games_from_api(self, date: datetime) -> list[dict]:
        """
        Fetch games from Action Network's scoreboard API.

        Args:
            date: Date to fetch games for

        Returns:
            List of game dictionaries
        """
        date_str = date.strftime("%Y%m%d")
        params = {**self.BASE_PARAMS, "date": date_str}

        # Headers to mimic a real browser request
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://www.actionnetwork.com/mlb/sharp-report",
            "Origin": "https://www.actionnetwork.com",
            "DNT": "1",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Sec-GPC": "1",
        }

        try:
            # Use a session to maintain cookies
            session = requests.Session()
            session.headers.update(headers)

            # First, visit the main page to establish a session (optional)
            try:
                session.get(
                    "https://www.actionnetwork.com/mlb/sharp-report", timeout=10
                )
            except:
                pass  # Continue even if this fails

            # Now make the API request
            response = session.get(self.SCOREBOARD_API_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            games = data.get("games", [])
            self.logger.info(f"Retrieved {len(games)} games from API", date=date_str)
            return games

        except requests.RequestException as e:
            self.logger.error("Failed to fetch games from API", error=str(e))
            raise

    def build_game_data_url(
        self, game_data: dict, date: datetime, build_id: str | None = None
    ) -> str:
        """
        Build the complete game data URL from API game data.

        Args:
            game_data: Game data from the scoreboard API
            date: Game date
            build_id: Optional build ID (will extract if not provided)

        Returns:
            Complete Action Network game data URL
        """
        if build_id is None:
            build_id = self._get_build_id()

        # Extract game info
        game_id = str(game_data["id"])

        # Get team names (adjust these field names based on actual API response)
        away_team = game_data.get("away_team", {}).get("full_name", "")
        home_team = game_data.get("home_team", {}).get("full_name", "")

        if not away_team or not home_team:
            # Try alternative field names
            teams = game_data.get("teams", [])
            if len(teams) >= 2:
                away_team = teams[1].get("full_name", teams[1].get("display_name", ""))
                home_team = teams[0].get("full_name", teams[0].get("display_name", ""))

        if not away_team or not home_team:
            raise ValueError(
                f"Could not extract team names from game data: {game_data}"
            )

        # Build URL components
        team_slug = self._create_team_slug(away_team, home_team)
        date_slug = self._format_date(date)

        # Construct the full URL
        url = (
            f"https://www.actionnetwork.com/_next/data/{build_id}/mlb-game/"
            f"{team_slug}-score-odds-{date_slug}/{game_id}.json"
            f"?league=mlb-game&slug={team_slug}-score-odds-{date_slug}&gameId={game_id}"
        )

        self.logger.debug(
            "Built game data URL",
            game_id=game_id,
            teams=f"{away_team} @ {home_team}",
            url=url,
        )

        return url

    def build_all_game_urls(self, date: datetime) -> list[tuple[dict, str]]:
        """
        Build URLs for all games on a given date.

        Args:
            date: Date to build URLs for

        Returns:
            List of tuples: (game_data, url)
        """
        games = self.get_games_from_api(date)
        build_id = self._get_build_id()  # Get once for all games

        results = []
        for game in games:
            try:
                url = self.build_game_data_url(game, date, build_id)
                results.append((game, url))
            except Exception as e:
                self.logger.warning(
                    "Failed to build URL for game", game_id=game.get("id"), error=str(e)
                )
                continue

        self.logger.info(f"Built URLs for {len(results)} out of {len(games)} games")
        return results

    def get_game_url_by_id(self, game_id: str, date: datetime) -> str | None:
        """
        Get the game data URL for a specific game ID.

        Args:
            game_id: The game ID to find
            date: Date of the game

        Returns:
            The game data URL or None if not found
        """
        games = self.get_games_from_api(date)

        for game in games:
            if str(game.get("id")) == str(game_id):
                return self.build_game_data_url(game, date)

        self.logger.warning("Game not found", game_id=game_id, date=date)
        return None

    def fetch_game_data(self, url: str) -> dict | None:
        """
        Fetch game data from a constructed Action Network URL.

        Args:
            url: Complete Action Network game data URL

        Returns:
            Game data dictionary or None if request fails
        """
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://www.actionnetwork.com/mlb/sharp-report",
            "DNT": "1",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Sec-GPC": "1",
        }

        try:
            session = requests.Session()
            session.headers.update(headers)

            # First establish session
            try:
                session.get(
                    "https://www.actionnetwork.com/mlb/sharp-report", timeout=10
                )
            except:
                pass

            response = session.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()

            self.logger.info("Successfully fetched game data", url=url)
            return data

        except requests.RequestException as e:
            self.logger.error("Failed to fetch game data", url=url, error=str(e))
            return None


def build_url_for_game(game_id: str, date: datetime) -> str:
    """
    Convenience function to build a URL for a specific game.

    Args:
        game_id: The game ID
        date: Date of the game

    Returns:
        Complete Action Network game data URL
    """
    builder = ActionNetworkURLBuilder()
    url = builder.get_game_url_by_id(game_id, date)

    if not url:
        raise ValueError(f"Could not build URL for game {game_id} on {date}")

    return url


def get_all_game_urls_for_date(date: datetime) -> list[tuple[dict, str]]:
    """
    Get all game URLs for a specific date.

    Args:
        date: Date to get games for

    Returns:
        List of (game_data, url) tuples
    """
    builder = ActionNetworkURLBuilder()
    return builder.build_all_game_urls(date)


if __name__ == "__main__":
    # Demo usage
    from datetime import datetime

    print("🔗 Action Network URL Builder Demo")
    print("=" * 50)

    # Test for today's games
    today = datetime.now()

    try:
        builder = ActionNetworkURLBuilder()

        print(f"📅 Building URLs for {today.strftime('%Y-%m-%d')}")

        # Get all games for today
        game_urls = builder.build_all_game_urls(today)

        print(f"✅ Found {len(game_urls)} games:")

        for i, (game_data, url) in enumerate(game_urls[:3], 1):  # Show first 3
            print(f"\n🏀 Game {i}:")
            print(f"   ID: {game_data.get('id')}")
            print(f"   URL: {url}")

        if len(game_urls) > 3:
            print(f"\n... and {len(game_urls) - 3} more games")

    except Exception as e:
        print(f"❌ Error: {e}")
        print("Make sure you have Chrome installed and internet access")
