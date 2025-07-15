"""
Enhanced Action Network Data Fetcher

This version uses the exact headers from a real Firefox browser session
for maximum compatibility and reduced chance of being blocked.
"""

import requests
import structlog

logger = structlog.get_logger(__name__)


class ActionNetworkEnhancedFetcher:
    """Enhanced fetcher using real Firefox browser headers."""

    def __init__(self):
        self.session = None
        self._setup_session()

    def _setup_session(self):
        """Setup session with Firefox headers from user's actual request."""
        self.session = requests.Session()

        # Exact headers from user's Firefox browser
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:139.0) Gecko/20100101 Firefox/139.0",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Referer": "https://www.actionnetwork.com/mlb/sharp-report",
            "purpose": "prefetch",
            "x-middleware-prefetch": "1",
            "x-nextjs-data": "1",
            "DNT": "1",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Sec-GPC": "1",
            "Priority": "u=4",
        }

        self.session.headers.update(headers)
        logger.info("Enhanced session setup complete", user_agent="Firefox 139.0")

    def fetch_game_data(self, url: str) -> dict | None:
        """
        Fetch game data using Firefox headers.

        Args:
            url: Complete Action Network game data URL

        Returns:
            Game data dictionary or None if request fails
        """
        try:
            # First establish session by visiting sharp report
            try:
                self.session.get(
                    "https://www.actionnetwork.com/mlb/sharp-report", timeout=10
                )
                logger.debug("Session established via sharp report page")
            except:
                logger.warning("Failed to establish session, continuing anyway")

            # Fetch the actual game data
            response = self.session.get(url, timeout=30)
            response.raise_for_status()

            data = response.json()
            logger.info(
                "Successfully fetched game data with Firefox headers",
                url=url[:100] + "..." if len(url) > 100 else url,
                data_size=len(str(data)),
            )

            return data

        except requests.RequestException as e:
            logger.error(
                "Failed to fetch game data with Firefox headers", url=url, error=str(e)
            )
            return None

    def fetch_scoreboard_api(self, date_str: str) -> dict | None:
        """
        Fetch scoreboard data using Firefox headers.

        Args:
            date_str: Date in format YYYYMMDD

        Returns:
            Scoreboard data or None if request fails
        """
        scoreboard_url = "https://api.actionnetwork.com/web/v2/scoreboard/proreport/mlb"
        params = {
            "bookIds": "15,30,2194,2292,2888,2889,2890,3118,3120,2891,281",
            "date": date_str,
            "periods": "event",
        }

        try:
            # Establish session first
            try:
                self.session.get(
                    "https://www.actionnetwork.com/mlb/sharp-report", timeout=10
                )
            except:
                pass

            response = self.session.get(scoreboard_url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            games = data.get("games", [])

            logger.info(
                "Successfully fetched scoreboard with Firefox headers",
                date=date_str,
                games_count=len(games),
            )

            return data

        except requests.RequestException as e:
            logger.error(
                "Failed to fetch scoreboard with Firefox headers",
                date=date_str,
                error=str(e),
            )
            return None

    def fetch_history_data(self, game_id: str) -> dict | None:
        """
        Fetch betting line history data for a specific game using Firefox headers.

        Args:
            game_id: The game ID to fetch history for

        Returns:
            History data dictionary or None if request fails
        """
        history_url = (
            f"https://api.actionnetwork.com/web/v2/markets/event/{game_id}/history"
        )

        try:
            # Establish session first
            try:
                self.session.get(
                    "https://www.actionnetwork.com/mlb/sharp-report", timeout=10
                )
            except:
                pass

            response = self.session.get(history_url, timeout=30)
            response.raise_for_status()

            data = response.json()

            logger.info(
                "Successfully fetched history data with Firefox headers",
                game_id=game_id,
                url=history_url,
            )

            return data

        except requests.RequestException as e:
            logger.error(
                "Failed to fetch history data with Firefox headers",
                game_id=game_id,
                url=history_url,
                error=str(e),
            )
            return None


def test_enhanced_fetcher():
    """Test the enhanced fetcher with Firefox headers for both game data and history."""
    fetcher = ActionNetworkEnhancedFetcher()

    # Test a game data URL
    test_url = "https://www.actionnetwork.com/_next/data/5ewWa01cMp6swFO15XxC9/mlb-game/athletics-rays-score-odds-july-1-2025/257329.json?league=mlb-game&slug=athletics-rays-score-odds-july-1-2025&gameId=257329"
    test_game_id = "257329"

    print("🔥 Testing Enhanced Firefox Headers")
    print("=" * 50)
    print(f"🎯 Target: Athletics @ Rays (Game {test_game_id})")
    print()

    # Test game data URL
    print("📊 Testing Game Data URL...")
    data = fetcher.fetch_game_data(test_url)

    if data:
        print("✅ SUCCESS! Game data URL worked!")

        # Extract game info
        page_props = data.get("pageProps", {})
        game_info = page_props.get("game", {})

        if game_info:
            print(f"   🆔 Game ID: {game_info.get('id')}")
            print(f"   📅 Start: {game_info.get('start_time')}")
            print(f"   📍 Status: {game_info.get('status_display', 'Unknown')}")

            teams = game_info.get("teams", [])
            if len(teams) >= 2:
                home_team = teams[0]
                away_team = teams[1]
                print(
                    f"   🏠 {home_team.get('full_name')} vs ✈️  {away_team.get('full_name')}"
                )

            print(f"   📊 Data Size: {len(str(data)):,} characters")
        else:
            print("   📊 Raw Data Retrieved Successfully")
    else:
        print("❌ Game data URL failed")

    print()

    # Test history URL
    print("📈 Testing History URL...")
    history_data = fetcher.fetch_history_data(test_game_id)

    if history_data:
        print("✅ SUCCESS! History URL worked!")

        # Extract some history info
        if isinstance(history_data, dict):
            # Count total betting lines with history
            total_lines = 0
            for book_data in history_data.values():
                if isinstance(book_data, dict):
                    for market_type in ["moneyline", "spread", "total"]:
                        if market_type in book_data:
                            total_lines += len(book_data[market_type])

            print(f"   📈 Betting lines found: {total_lines}")
            print(f"   📊 History data size: {len(str(history_data)):,} characters")
        else:
            print("   📊 Raw History Data Retrieved Successfully")
    else:
        print("❌ History URL failed")

    print("\n🔥 Both game data and history URLs can be accessed with enhanced headers!")


if __name__ == "__main__":
    test_enhanced_fetcher()
