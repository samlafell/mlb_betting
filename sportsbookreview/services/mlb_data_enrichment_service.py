"""
MLB Data Enrichment Service for SportsbookReview system.

This service provides comprehensive MLB Stats API integration for enriching
SportsbookReview game data with venue, weather, pitcher, and context information.
"""

import asyncio

# Import the established patterns
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

src_path = Path(__file__).parent.parent.parent / "src"
sys.path.insert(0, str(src_path))

from mlb_sharp_betting.core.logging import get_logger
from mlb_sharp_betting.services.config_service import get_config_service
from mlb_sharp_betting.services.retry_service import OperationType, RetryService

# Import our SportsbookReview models
from sportsbookreview.models import (
    EnhancedGame,
    GameContext,
    PitcherInfo,
    PitcherMatchup,
    VenueInfo,
    WeatherCondition,
    WeatherData,
)


@dataclass
class MLBGameCorrelation:
    """Result of correlating a SportsbookReview game with MLB Stats API."""

    mlb_game_id: str
    confidence_score: float
    venue_info: VenueInfo | None = None
    weather_data: WeatherData | None = None
    pitcher_matchup: PitcherMatchup | None = None
    game_context: GameContext | None = None
    context_metrics: dict[str, Any] | None = None


@dataclass
class EnrichmentStats:
    """Statistics for MLB data enrichment operations."""

    games_processed: int = 0
    successful_correlations: int = 0
    failed_correlations: int = 0
    venue_data_fetched: int = 0
    weather_data_fetched: int = 0
    pitcher_data_fetched: int = 0
    context_data_fetched: int = 0
    context_metrics_fetched: int = 0
    api_calls_made: int = 0
    cache_hits: int = 0


class MLBDataEnrichmentService:
    """
    Service for enriching SportsbookReview games with MLB Stats API data.

    Provides comprehensive game context enrichment including:
    - Game ID correlation and matching
    - Venue information (stadium, capacity, location)
    - Weather conditions at game time
    - Starting pitcher information and matchups
    - Game context (series info, playoff status, attendance)
    - Context metrics (advanced game metrics)
    """

    def __init__(self):
        """Initialize MLB Data Enrichment Service."""
        self.logger = get_logger(__name__)
        self.config_service = get_config_service()
        self.retry_service = RetryService()

        # MLB Stats API configuration
        self.base_url = "https://statsapi.mlb.com/api/v1"
        self.base_url_v1_1 = "https://statsapi.mlb.com/api/v1.1"

        # Service configuration
        self.config = self.config_service.get_service_config("mlb_data_enrichment")

        # Rate limiting and caching
        self.request_delay = self.config.get("rate_limits", {}).get(
            "request_delay_seconds", 0.5
        )
        self.max_requests_per_minute = self.config.get("rate_limits", {}).get(
            "max_requests_per_minute", 60
        )

        # Cache for MLB data (in-memory for now)
        self._venue_cache: dict[int, VenueInfo] = {}
        self._game_cache: dict[str, dict[str, Any]] = {}
        self._schedule_cache: dict[str, list[dict[str, Any]]] = {}

        # Session with retry strategy
        self.session = self._create_session()

        # Statistics
        self.stats = EnrichmentStats()

        self.logger.info("MLBDataEnrichmentService initialized")

    def _create_session(self) -> requests.Session:
        """Create requests session with retry strategy."""
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            backoff_factor=1,
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Set headers
        session.headers.update(
            {
                "User-Agent": "SportsbookReview-Integration/1.0",
                "Accept": "application/json",
            }
        )

        return session

    async def enrich_game(self, game: EnhancedGame) -> MLBGameCorrelation:
        """
        Enrich a SportsbookReview game with comprehensive MLB Stats API data.

        Args:
            game: Enhanced game model to enrich

        Returns:
            MLBGameCorrelation with enrichment data
        """
        self.stats.games_processed += 1

        try:
            # Step 1: Correlate with MLB Game ID if not already done
            if not game.mlb_game_id:
                mlb_game_id, confidence = await self._correlate_game_id(game)
                if not mlb_game_id:
                    self.stats.failed_correlations += 1
                    raise ValueError(
                        f"Could not correlate game {game.sbr_game_id} with MLB Stats API"
                    )
            else:
                mlb_game_id = game.mlb_game_id
                confidence = game.mlb_correlation_confidence or 1.0

            self.stats.successful_correlations += 1

            # Step 2: Fetch comprehensive enrichment data in parallel
            enrichment_tasks = [
                self._fetch_venue_info(mlb_game_id),
                self._fetch_weather_data(mlb_game_id),
                self._fetch_pitcher_matchup(mlb_game_id),
                self._fetch_game_context(mlb_game_id),
                self._fetch_context_metrics(mlb_game_id),
            ]

            results = await asyncio.gather(*enrichment_tasks, return_exceptions=True)

            venue_info = results[0] if not isinstance(results[0], Exception) else None
            weather_data = results[1] if not isinstance(results[1], Exception) else None
            pitcher_matchup = (
                results[2] if not isinstance(results[2], Exception) else None
            )
            game_context = results[3] if not isinstance(results[3], Exception) else None
            context_metrics = (
                results[4] if not isinstance(results[4], Exception) else None
            )

            # Update statistics
            if venue_info:
                self.stats.venue_data_fetched += 1
            if weather_data:
                self.stats.weather_data_fetched += 1
            if pitcher_matchup:
                self.stats.pitcher_data_fetched += 1
            if game_context:
                self.stats.context_data_fetched += 1
            if context_metrics:
                self.stats.context_metrics_fetched += 1

            # Create correlation result
            correlation = MLBGameCorrelation(
                mlb_game_id=mlb_game_id,
                confidence_score=confidence,
                venue_info=venue_info,
                weather_data=weather_data,
                pitcher_matchup=pitcher_matchup,
                game_context=game_context,
                context_metrics=context_metrics,
            )

            self.logger.info(
                "Game enrichment completed",
                game_id=game.sbr_game_id,
                mlb_game_id=mlb_game_id,
                confidence=confidence,
                venue_available=bool(venue_info),
                weather_available=bool(weather_data),
                pitcher_available=bool(pitcher_matchup),
                context_available=bool(game_context),
                metrics_available=bool(context_metrics),
            )

            return correlation

        except Exception as e:
            self.logger.error(
                "Game enrichment failed", game_id=game.sbr_game_id, error=str(e)
            )
            raise

    async def _correlate_game_id(
        self, game: EnhancedGame
    ) -> tuple[str | None, float]:
        """
        Correlate SportsbookReview game with MLB Stats API Game ID.

        Args:
            game: Game to correlate

        Returns:
            Tuple of (MLB Game ID, confidence score)
        """
        try:
            # Get games for the specific date
            game_date = game.game_date.date()
            schedule_data = await self._fetch_schedule_for_date(game_date)

            if not schedule_data:
                return None, 0.0

            # Find matching game
            best_match = None
            best_confidence = 0.0

            for mlb_game in schedule_data:
                confidence = self._calculate_game_match_confidence(game, mlb_game)

                if confidence > best_confidence:
                    best_confidence = confidence
                    best_match = mlb_game

            # Require minimum confidence for correlation
            if best_confidence >= 0.8:
                mlb_game_id = str(best_match["gamePk"])
                self.logger.info(
                    "Game correlation found",
                    sbr_game_id=game.sbr_game_id,
                    mlb_game_id=mlb_game_id,
                    confidence=best_confidence,
                )
                return mlb_game_id, best_confidence

            self.logger.warning(
                "No confident game correlation found",
                sbr_game_id=game.sbr_game_id,
                best_confidence=best_confidence,
            )
            return None, 0.0

        except Exception as e:
            self.logger.error(
                "Game correlation failed", game_id=game.sbr_game_id, error=str(e)
            )
            return None, 0.0

    def _calculate_game_match_confidence(
        self, sbr_game: EnhancedGame, mlb_game: dict[str, Any]
    ) -> float:
        """
        Calculate confidence score for game matching.

        Args:
            sbr_game: SportsbookReview game
            mlb_game: MLB Stats API game data

        Returns:
            Confidence score (0.0-1.0)
        """
        confidence = 0.0

        try:
            # Extract team information from MLB game
            mlb_home_team = mlb_game["teams"]["home"]["team"]["abbreviation"]
            mlb_away_team = mlb_game["teams"]["away"]["team"]["abbreviation"]

            # Team matching (most important factor - 70% weight)
            if (
                sbr_game.home_team.value == mlb_home_team
                and sbr_game.away_team.value == mlb_away_team
            ):
                confidence += 0.7
            elif (
                sbr_game.home_team.value in mlb_home_team
                or mlb_home_team in sbr_game.home_team.value
            ) and (
                sbr_game.away_team.value in mlb_away_team
                or mlb_away_team in sbr_game.away_team.value
            ):
                confidence += 0.5  # Partial team match

            # Date matching (20% weight)
            mlb_game_date = datetime.fromisoformat(
                mlb_game["gameDate"].replace("Z", "+00:00")
            ).date()
            sbr_game_date = sbr_game.game_date.date()

            if mlb_game_date == sbr_game_date:
                confidence += 0.2
            elif abs((mlb_game_date - sbr_game_date).days) <= 1:
                confidence += 0.1  # Within 1 day

            # Time proximity (10% weight)
            mlb_game_time = datetime.fromisoformat(
                mlb_game["gameDate"].replace("Z", "+00:00")
            )
            time_diff = (
                abs((mlb_game_time - sbr_game.game_date).total_seconds()) / 3600
            )  # Hours

            if time_diff <= 2:  # Within 2 hours
                confidence += 0.1
            elif time_diff <= 6:  # Within 6 hours
                confidence += 0.05

        except (KeyError, ValueError, TypeError) as e:
            self.logger.warning(
                "Error calculating game match confidence",
                error=str(e),
                mlb_game_id=mlb_game.get("gamePk"),
            )
            return 0.0

        return min(confidence, 1.0)

    async def _fetch_schedule_for_date(
        self, date: datetime.date
    ) -> list[dict[str, Any]]:
        """
        Fetch MLB schedule for a specific date.

        Args:
            date: Date to fetch schedule for

        Returns:
            List of game data from MLB Stats API
        """
        date_str = date.strftime("%Y-%m-%d")

        # Check cache first
        if date_str in self._schedule_cache:
            self.stats.cache_hits += 1
            return self._schedule_cache[date_str]

        try:
            # Fetch from MLB Stats API
            url = f"{self.base_url}/schedule"
            params = {
                "date": date_str,
                "sportId": 1,  # MLB
                "hydrate": "team,venue",
            }

            response = await self._make_api_call(url, params)

            if response and "dates" in response and response["dates"]:
                games = response["dates"][0].get("games", [])
                self._schedule_cache[date_str] = games
                return games

            return []

        except Exception as e:
            self.logger.error("Failed to fetch schedule", date=date_str, error=str(e))
            return []

    async def _fetch_venue_info(self, mlb_game_id: str) -> VenueInfo | None:
        """
        Fetch venue information for a game.

        Args:
            mlb_game_id: MLB Game ID

        Returns:
            VenueInfo model or None if not available
        """
        try:
            # Get game data to extract venue ID
            game_data = await self._fetch_game_data(mlb_game_id)
            if not game_data:
                return None

            venue_data = game_data.get("gameData", {}).get("venue", {})
            if not venue_data:
                return None

            venue_id = venue_data.get("id")
            if not venue_id:
                return None

            # Check cache first
            if venue_id in self._venue_cache:
                self.stats.cache_hits += 1
                return self._venue_cache[venue_id]

            # Create VenueInfo from game data
            venue_info = VenueInfo(
                venue_id=venue_id,
                venue_name=venue_data.get("name"),
                city=venue_data.get("location", {}).get("city"),
                state=venue_data.get("location", {}).get("stateAbbrev"),
                timezone=venue_data.get("timeZone", {}).get("id"),
                capacity=venue_data.get("capacity"),
                surface=venue_data.get("surface"),
                roof_type=venue_data.get("roofType"),
            )

            # Cache the result
            self._venue_cache[venue_id] = venue_info

            return venue_info

        except Exception as e:
            self.logger.error(
                "Failed to fetch venue info", mlb_game_id=mlb_game_id, error=str(e)
            )
            return None

    async def _fetch_weather_data(self, mlb_game_id: str) -> WeatherData | None:
        """
        Fetch weather data for a game.

        Args:
            mlb_game_id: MLB Game ID

        Returns:
            WeatherData model or None if not available
        """
        try:
            game_data = await self._fetch_game_data(mlb_game_id)
            if not game_data:
                return None

            weather_data = game_data.get("gameData", {}).get("weather", {})
            if not weather_data:
                return None

            # Map weather condition
            condition_str = weather_data.get("condition", "").lower()
            condition = WeatherCondition.UNKNOWN

            if "clear" in condition_str or "sunny" in condition_str:
                condition = WeatherCondition.CLEAR
            elif "partly cloudy" in condition_str:
                condition = WeatherCondition.PARTLY_CLOUDY
            elif "cloudy" in condition_str:
                condition = WeatherCondition.CLOUDY
            elif "overcast" in condition_str:
                condition = WeatherCondition.OVERCAST
            elif "rain" in condition_str:
                condition = WeatherCondition.RAIN
            elif "drizzle" in condition_str:
                condition = WeatherCondition.DRIZZLE
            elif "fog" in condition_str:
                condition = WeatherCondition.FOG
            elif "dome" in condition_str or "indoor" in condition_str:
                condition = WeatherCondition.DOME

            return WeatherData(
                condition=condition,
                temperature=weather_data.get("temp"),
                wind_speed=weather_data.get("wind"),
                wind_direction=weather_data.get("windDirection"),
                humidity=weather_data.get("humidity"),
            )

        except Exception as e:
            self.logger.error(
                "Failed to fetch weather data", mlb_game_id=mlb_game_id, error=str(e)
            )
            return None

    async def _fetch_pitcher_matchup(
        self, mlb_game_id: str
    ) -> PitcherMatchup | None:
        """
        Fetch starting pitcher information for a game.

        Args:
            mlb_game_id: MLB Game ID

        Returns:
            PitcherMatchup model or None if not available
        """
        try:
            game_data = await self._fetch_game_data(mlb_game_id)
            if not game_data:
                return None

            # Extract starting pitchers
            probable_pitchers = game_data.get("gameData", {}).get(
                "probablePitchers", {}
            )

            home_pitcher = None
            away_pitcher = None

            if "home" in probable_pitchers:
                home_pitcher_data = probable_pitchers["home"]
                home_pitcher = PitcherInfo(
                    player_id=home_pitcher_data.get("id"),
                    full_name=home_pitcher_data.get("fullName"),
                    throws=home_pitcher_data.get("pitchHand", {}).get("code"),
                )

            if "away" in probable_pitchers:
                away_pitcher_data = probable_pitchers["away"]
                away_pitcher = PitcherInfo(
                    player_id=away_pitcher_data.get("id"),
                    full_name=away_pitcher_data.get("fullName"),
                    throws=away_pitcher_data.get("pitchHand", {}).get("code"),
                )

            if home_pitcher or away_pitcher:
                return PitcherMatchup(
                    home_pitcher=home_pitcher, away_pitcher=away_pitcher
                )

            return None

        except Exception as e:
            self.logger.error(
                "Failed to fetch pitcher matchup", mlb_game_id=mlb_game_id, error=str(e)
            )
            return None

    async def _fetch_game_context(self, mlb_game_id: str) -> GameContext | None:
        """
        Fetch additional game context information.

        Args:
            mlb_game_id: MLB Game ID

        Returns:
            GameContext model or None if not available
        """
        try:
            game_data = await self._fetch_game_data(mlb_game_id)
            if not game_data:
                return None

            game_info = game_data.get("gameData", {}).get("game", {})

            return GameContext(
                series_description=game_info.get("seriesDescription"),
                series_game_number=game_info.get("seriesGameNumber"),
                games_in_series=game_info.get("gamesInSeries"),
                is_playoff_game=game_info.get("type") == "P",
                attendance=game_data.get("liveData", {})
                .get("boxscore", {})
                .get("info", [{}])[0]
                .get("attendance"),
            )

        except Exception as e:
            self.logger.error(
                "Failed to fetch game context", mlb_game_id=mlb_game_id, error=str(e)
            )
            return None

    async def _fetch_context_metrics(
        self, mlb_game_id: str
    ) -> dict[str, Any] | None:
        """
        Fetch context metrics for a game (user-requested endpoint).

        Args:
            mlb_game_id: MLB Game ID

        Returns:
            Context metrics dictionary or None if not available
        """
        try:
            url = f"{self.base_url}/game/{mlb_game_id}/contextMetrics"
            response = await self._make_api_call(url)

            if response:
                return response

            return None

        except Exception as e:
            self.logger.error(
                "Failed to fetch context metrics", mlb_game_id=mlb_game_id, error=str(e)
            )
            return None

    async def _fetch_game_data(self, mlb_game_id: str) -> dict[str, Any] | None:
        """
        Fetch comprehensive game data from MLB Stats API.

        Args:
            mlb_game_id: MLB Game ID

        Returns:
            Game data dictionary or None if not available
        """
        # Check cache first
        if mlb_game_id in self._game_cache:
            self.stats.cache_hits += 1
            return self._game_cache[mlb_game_id]

        try:
            url = f"{self.base_url_v1_1}/game/{mlb_game_id}/feed/live"
            response = await self._make_api_call(url)

            if response:
                # Cache the result
                self._game_cache[mlb_game_id] = response
                return response

            return None

        except Exception as e:
            self.logger.error(
                "Failed to fetch game data", mlb_game_id=mlb_game_id, error=str(e)
            )
            return None

    async def _make_api_call(
        self, url: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
        """
        Make API call to MLB Stats API with retry logic and rate limiting.

        Args:
            url: API endpoint URL
            params: Query parameters

        Returns:
            API response data or None if failed
        """

        async def api_call():
            await asyncio.sleep(self.request_delay)  # Rate limiting
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()

        try:
            result = await self.retry_service.execute_with_retry(
                operation=api_call,
                operation_name=f"MLB API call: {url}",
                operation_type=OperationType.API_CALL,
                service_name="mlb_data_enrichment",
            )

            self.stats.api_calls_made += 1
            return result

        except Exception as e:
            self.logger.error(
                "MLB API call failed", url=url, params=params, error=str(e)
            )
            return None

    def get_enrichment_stats(self) -> dict[str, Any]:
        """
        Get enrichment service statistics.

        Returns:
            Dictionary with service statistics
        """
        return {
            "games_processed": self.stats.games_processed,
            "successful_correlations": self.stats.successful_correlations,
            "failed_correlations": self.stats.failed_correlations,
            "correlation_success_rate": (
                self.stats.successful_correlations / max(self.stats.games_processed, 1)
            ),
            "venue_data_fetched": self.stats.venue_data_fetched,
            "weather_data_fetched": self.stats.weather_data_fetched,
            "pitcher_data_fetched": self.stats.pitcher_data_fetched,
            "context_data_fetched": self.stats.context_data_fetched,
            "context_metrics_fetched": self.stats.context_metrics_fetched,
            "api_calls_made": self.stats.api_calls_made,
            "cache_hits": self.stats.cache_hits,
            "cache_hit_rate": self.stats.cache_hits / max(self.stats.api_calls_made, 1),
        }

    def clear_cache(self) -> None:
        """Clear all cached data."""
        self._venue_cache.clear()
        self._game_cache.clear()
        self._schedule_cache.clear()
        self.logger.info("MLB data cache cleared")


# Service factory function following established patterns
_mlb_enrichment_service_instance: MLBDataEnrichmentService | None = None


def get_mlb_data_enrichment_service() -> MLBDataEnrichmentService:
    """Get singleton instance of MLB Data Enrichment Service."""
    global _mlb_enrichment_service_instance
    if _mlb_enrichment_service_instance is None:
        _mlb_enrichment_service_instance = MLBDataEnrichmentService()
    return _mlb_enrichment_service_instance
