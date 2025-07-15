"""
Pinnacle scraper for MLB betting odds data.

Scrapes JSON data from Pinnacle's public endpoints to extract essential
MLB betting information including moneylines, spreads, and totals.
"""

import asyncio
from datetime import datetime
from typing import Any

import structlog

from ..models.game import Team
from .base import JSONScraper, RateLimitConfig, ScrapingResult

logger = structlog.get_logger(__name__)


class PinnacleScraper(JSONScraper):
    """
    Scraper for Pinnacle MLB betting odds.

    Follows a clean three-step process:
    1. Get matchup IDs from league endpoint
    2. Get team info from matchup details endpoint
    3. Get market data (moneyline, total, spread only) from markets endpoint
    """

    def __init__(self):
        """Initialize the Pinnacle scraper."""
        # Configure for respectful scraping
        rate_config = RateLimitConfig(
            requests_per_second=1.0,
            requests_per_minute=30.0,
            burst_size=2,
            backoff_factor=2.0,
        )

        super().__init__(
            source_name="pinnacle", rate_limit_config=rate_config, timeout=30.0
        )

        self.base_url = "https://guest.api.arcadia.pinnacle.com/0.1"
        self.mlb_league_id = 246

        # Essential team name mappings for MLB
        self.team_mappings = {
            # AL Teams
            "Athletics": Team.OAK,
            "Oakland Athletics": Team.OAK,
            "Houston Astros": Team.HOU,
            "Astros": Team.HOU,
            "New York Yankees": Team.NYY,
            "Yankees": Team.NYY,
            "Boston Red Sox": Team.BOS,
            "Red Sox": Team.BOS,
            "Tampa Bay Rays": Team.TB,
            "Rays": Team.TB,
            "Toronto Blue Jays": Team.TOR,
            "Blue Jays": Team.TOR,
            "Chicago White Sox": Team.CWS,
            "White Sox": Team.CWS,
            "Cleveland Guardians": Team.CLE,
            "Guardians": Team.CLE,
            "Detroit Tigers": Team.DET,
            "Tigers": Team.DET,
            "Kansas City Royals": Team.KC,
            "Royals": Team.KC,
            "Minnesota Twins": Team.MIN,
            "Twins": Team.MIN,
            "Los Angeles Angels": Team.LAA,
            "Angels": Team.LAA,
            "Seattle Mariners": Team.SEA,
            "Mariners": Team.SEA,
            "Texas Rangers": Team.TEX,
            "Rangers": Team.TEX,
            # NL Teams
            "Atlanta Braves": Team.ATL,
            "Braves": Team.ATL,
            "Miami Marlins": Team.MIA,
            "Marlins": Team.MIA,
            "New York Mets": Team.NYM,
            "Mets": Team.NYM,
            "Philadelphia Phillies": Team.PHI,
            "Phillies": Team.PHI,
            "Washington Nationals": Team.WSH,
            "Nationals": Team.WSH,
            "Chicago Cubs": Team.CHC,
            "Cubs": Team.CHC,
            "Cincinnati Reds": Team.CIN,
            "Reds": Team.CIN,
            "Milwaukee Brewers": Team.MIL,
            "Brewers": Team.MIL,
            "Pittsburgh Pirates": Team.PIT,
            "Pirates": Team.PIT,
            "St. Louis Cardinals": Team.STL,
            "Cardinals": Team.STL,
            "Arizona Diamondbacks": Team.ARI,
            "Diamondbacks": Team.ARI,
            "Colorado Rockies": Team.COL,
            "Rockies": Team.COL,
            "Los Angeles Dodgers": Team.LAD,
            "Dodgers": Team.LAD,
            "San Diego Padres": Team.SD,
            "Padres": Team.SD,
            "San Francisco Giants": Team.SF,
            "Giants": Team.SF,
        }

    async def scrape(self, **kwargs) -> ScrapingResult:
        """
        Scrape all current MLB betting data from Pinnacle using the three-step process.

        Returns:
            ScrapingResult containing complete matchup and market data
        """
        errors = []
        data = []
        request_count = 0
        start_time = datetime.now()

        try:
            self.logger.info("Starting Pinnacle MLB data scrape (3-step process)")

            # Step 1: Get unique matchup IDs from league endpoint
            self.logger.info("Step 1: Getting matchup IDs")
            matchup_ids = await self._get_matchup_ids()
            request_count += 1

            if not matchup_ids:
                self.logger.warning("No matchup IDs found")
                return self._create_result(
                    success=False,
                    data=[],
                    errors=["No matchup IDs found"],
                    request_count=request_count,
                    response_time_ms=(datetime.now() - start_time).total_seconds()
                    * 1000,
                )

            self.logger.info("Found unique matchups", count=len(matchup_ids))

            # Step 2 & 3: For each matchup, get team info and market data
            semaphore = asyncio.Semaphore(3)  # Limit concurrent requests

            async def process_matchup(
                matchup_id: int,
            ) -> tuple[dict[str, Any] | None, list[str], int]:
                async with semaphore:
                    return await self._process_single_matchup(matchup_id)

            # Process all matchups concurrently
            tasks = [process_matchup(matchup_id) for matchup_id in matchup_ids]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Collect results
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    error_msg = (
                        f"Failed to process matchup {matchup_ids[i]}: {str(result)}"
                    )
                    errors.append(error_msg)
                    request_count += 2  # Would have made 2 requests
                else:
                    matchup_data, matchup_errors, requests_made = result
                    if matchup_data:
                        data.append(matchup_data)
                    errors.extend(matchup_errors)
                    request_count += requests_made

            success = len(data) > 0

            self.logger.info(
                "Pinnacle scraping completed",
                matchups_processed=len(matchup_ids),
                complete_matchups=len(data),
                errors=len(errors),
                success=success,
            )

            return self._create_result(
                success=success,
                data=data,
                errors=errors,
                metadata={
                    "matchups_found": len(matchup_ids),
                    "complete_matchups": len(data),
                    "step_1_url": f"{self.base_url}/leagues/{self.mlb_league_id}/markets/straight",
                    "total_requests": request_count,
                },
                request_count=request_count,
                response_time_ms=(datetime.now() - start_time).total_seconds() * 1000,
            )

        except Exception as e:
            error_msg = f"Pinnacle scraping failed: {str(e)}"
            self.logger.error("Scraping error", error=error_msg)
            errors.append(error_msg)

            return self._create_result(
                success=False,
                data=data,
                errors=errors,
                request_count=request_count,
                response_time_ms=(datetime.now() - start_time).total_seconds() * 1000,
            )

    async def _get_matchup_ids(self) -> list[int]:
        """Step 1: Get unique matchup IDs from the league endpoint."""
        url = f"{self.base_url}/leagues/{self.mlb_league_id}/markets/straight"

        try:
            raw_data = await self._get_json(url)

            if not raw_data or not isinstance(raw_data, list):
                self.logger.warning("Invalid league market data format")
                return []

            # Extract unique matchup IDs only
            matchup_ids: set[int] = set()
            for item in raw_data:
                if isinstance(item, dict) and "matchupId" in item:
                    matchup_ids.add(item["matchupId"])

            unique_ids = list(matchup_ids)
            self.logger.debug("Extracted unique matchup IDs", count=len(unique_ids))
            return unique_ids

        except Exception as e:
            self.logger.error("Failed to get matchup IDs", error=str(e))
            return []

    async def _process_single_matchup(
        self, matchup_id: int
    ) -> tuple[dict[str, Any] | None, list[str], int]:
        """Steps 2 & 3: Get team info and market data for a single matchup."""
        errors = []
        requests_made = 0

        try:
            # Step 2: Get team information
            team_info = await self._get_matchup_details(matchup_id)
            requests_made += 1

            if not team_info:
                errors.append(f"No team info found for matchup {matchup_id}")
                return None, errors, requests_made

            # Step 3: Get market data (moneyline, total, spread only)
            markets = await self._get_matchup_markets(matchup_id)
            requests_made += 1

            if not markets:
                errors.append(f"No markets found for matchup {matchup_id}")
                return None, errors, requests_made

            # Combine team info and markets into complete matchup data
            complete_matchup = {
                "matchup_id": matchup_id,
                "home_team": team_info.get("home_team"),
                "away_team": team_info.get("away_team"),
                "start_time": team_info.get("start_time"),
                "markets": markets,
                "scraped_at": datetime.now().isoformat(),
            }

            self.logger.debug(
                "Processed complete matchup",
                matchup_id=matchup_id,
                markets_count=len(markets),
            )

            return complete_matchup, errors, requests_made

        except Exception as e:
            error_msg = f"Failed to process matchup {matchup_id}: {str(e)}"
            errors.append(error_msg)
            return None, errors, requests_made

    async def _get_matchup_details(self, matchup_id: int) -> dict[str, Any] | None:
        """Step 2: Get team names, home/away designation, and start time."""
        url = f"{self.base_url}/matchups/{matchup_id}/related"

        try:
            raw_data = await self._get_json(url)

            if not raw_data or not isinstance(raw_data, dict):
                return None

            # Extract participants (teams)
            participants = raw_data.get("participants", [])
            if len(participants) < 2:
                return None

            teams = {}
            for participant in participants:
                if not isinstance(participant, dict):
                    continue

                alignment = participant.get("alignment", "").lower()
                name = participant.get("name", "")

                if alignment in ["home", "away"] and name:
                    normalized_team = self._normalize_team_name(name)
                    if normalized_team:
                        teams[f"{alignment}_team"] = normalized_team.value
                    else:
                        # Keep original name if we can't normalize it
                        teams[f"{alignment}_team"] = name

            # Must have both home and away
            if "home_team" not in teams or "away_team" not in teams:
                return None

            # Extract start time
            start_time = raw_data.get("startTime")

            return {
                "home_team": teams["home_team"],
                "away_team": teams["away_team"],
                "start_time": start_time,
            }

        except Exception as e:
            self.logger.debug(
                "Failed to get matchup details", matchup_id=matchup_id, error=str(e)
            )
            return None

    async def _get_matchup_markets(self, matchup_id: int) -> list[dict[str, Any]]:
        """Step 3: Get market data (moneyline, total, spread only)."""
        url = f"{self.base_url}/matchups/{matchup_id}/markets/related/straight"

        try:
            raw_data = await self._get_json(url)

            if not raw_data or not isinstance(raw_data, list):
                return []

            # Filter for only moneyline, total, and spread markets
            filtered_markets = []
            for market in raw_data:
                if not isinstance(market, dict):
                    continue

                market_type = market.get("type", "").lower()
                if market_type not in ["moneyline", "total", "spread"]:
                    continue

                # Extract essential market information
                essential_market = self._extract_essential_market_info(market)
                if essential_market:
                    filtered_markets.append(essential_market)

            return filtered_markets

        except Exception as e:
            self.logger.debug(
                "Failed to get matchup markets", matchup_id=matchup_id, error=str(e)
            )
            return []

    def _extract_essential_market_info(
        self, raw_market: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Extract only essential market information."""
        try:
            market_type = raw_market.get("type", "").lower()
            if market_type not in ["moneyline", "total", "spread"]:
                return None

            # Extract prices
            prices = self._extract_price_info(raw_market.get("prices", []))
            if not prices:
                return None

            # Build essential market info
            market_info = {
                "type": market_type,
                "prices": prices,
                "key": raw_market.get("key", ""),
                "period": raw_market.get("period", 0),
                "status": raw_market.get("status", "open").lower(),
                "cutoff_at": raw_market.get("cutoffAt"),
                "version": raw_market.get("version", 0),
            }

            # Add line value for spread/total markets
            if market_type in ["spread", "total"]:
                # Check if any price has points (line value)
                for price in raw_market.get("prices", []):
                    if isinstance(price, dict) and "points" in price:
                        market_info["line"] = float(price["points"])
                        break

            # Add limits if available (important for sharp betting)
            limits = self._extract_limit_info(raw_market.get("limits", []))
            if limits:
                market_info["limits"] = limits

            return market_info

        except Exception as e:
            self.logger.debug("Failed to extract market info", error=str(e))
            return None

    def _extract_price_info(
        self, prices_data: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Extract essential price information."""
        prices = []

        for price_data in prices_data:
            if not isinstance(price_data, dict):
                continue

            price_value = price_data.get("price")
            if price_value is None:
                continue

            # Build price info
            price_info = {"price": int(price_value)}

            # Add designation if available (home/away/over/under)
            if "designation" in price_data:
                price_info["designation"] = price_data["designation"].lower()

            # Add participant ID if available
            if "participantId" in price_data:
                price_info["participant_id"] = price_data["participantId"]

            # Add points/line if available
            if "points" in price_data:
                price_info["points"] = float(price_data["points"])

            prices.append(price_info)

        return prices

    def _extract_limit_info(
        self, limits_data: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Extract betting limit information."""
        limits = []

        for limit_data in limits_data:
            if not isinstance(limit_data, dict):
                continue

            amount = limit_data.get("amount")
            limit_type = limit_data.get("type", "").lower()

            if amount is not None and limit_type in ["maxriskstake", "maxwinstake"]:
                limits.append({"amount": float(amount), "type": limit_type})

        return limits

    def _normalize_team_name(self, team_name: str) -> Team | None:
        """Normalize team name to Team enum."""
        if not team_name:
            return None

        # Direct lookup
        if team_name in self.team_mappings:
            return self.team_mappings[team_name]

        # Case-insensitive lookup
        for key, value in self.team_mappings.items():
            if key.lower() == team_name.lower():
                return value

        # Partial matching as fallback
        team_lower = team_name.lower()
        for key, value in self.team_mappings.items():
            if team_lower in key.lower() or key.lower() in team_lower:
                return value

        self.logger.debug("Could not normalize team name", team_name=team_name)
        return None

    async def scrape_team_matchup(
        self, home_team: Team, away_team: Team
    ) -> ScrapingResult:
        """Scrape betting data for a specific team matchup."""
        try:
            # Get all current data
            all_data_result = await self.scrape()

            if not all_data_result.success:
                return all_data_result

            # Filter for the specific teams
            matching_matchups = []
            for matchup in all_data_result.data:
                if (
                    matchup.get("home_team") == home_team.value
                    and matchup.get("away_team") == away_team.value
                ):
                    matching_matchups.append(matchup)

            self.logger.info(
                "Found matching matchups for teams",
                home_team=home_team.value,
                away_team=away_team.value,
                matchups_found=len(matching_matchups),
            )

            return self._create_result(
                success=len(matching_matchups) > 0,
                data=matching_matchups,
                errors=all_data_result.errors,
                metadata={
                    "home_team": home_team.value,
                    "away_team": away_team.value,
                    "total_matchups_checked": len(all_data_result.data),
                    "matching_found": len(matching_matchups),
                },
                request_count=all_data_result.request_count,
                response_time_ms=all_data_result.response_time_ms,
            )

        except Exception as e:
            error_msg = f"Failed to scrape team matchup data: {str(e)}"
            self.logger.error(
                "Team matchup scraping error",
                home_team=home_team,
                away_team=away_team,
                error=error_msg,
            )

            return self._create_result(success=False, data=[], errors=[error_msg])
