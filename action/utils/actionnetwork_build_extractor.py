"""
Action Network Build ID Extractor

This module extracts the current Next.js build ID from Action Network
by monitoring network traffic on their sharp report page.
"""

import json
import re
import time
from typing import Any

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

try:
    import structlog

    logger = structlog.get_logger(__name__)
except ImportError:
    import logging

    logger = logging.getLogger(__name__)


class ActionNetworkBuildExtractor:
    """Extract Next.js build ID from Action Network by monitoring network requests."""

    SHARP_REPORT_URL = "https://www.actionnetwork.com/mlb/sharp-report"
    DATA_URL_PREFIX = "https://www.actionnetwork.com/_next/data/"

    def __init__(self, headless: bool = True, timeout: int = 30):
        """
        Initialize the build extractor.

        Args:
            headless: Whether to run browser in headless mode
            timeout: Timeout for page loads
        """
        self.headless = headless
        self.timeout = timeout
        self.driver: webdriver.Chrome | None = None
        self.logger = logger.bind(extractor="ActionNetworkBuild")

    def _setup_driver_with_network_logging(self) -> webdriver.Chrome:
        """Setup Chrome driver with network logging enabled."""
        options = Options()
        if self.headless:
            options.add_argument("--headless=new")

        # Basic options for stealth
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)

        # Enable performance logging to capture network requests
        options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
        options.add_experimental_option(
            "perfLoggingPrefs",
            {
                "enableNetwork": True,
                "enablePage": False,
            },
        )
        options.add_argument("--enable-logging")
        options.add_argument("--log-level=0")

        # User agent
        options.add_argument(
            "--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        # Window size
        options.add_argument("--window-size=1920,1080")

        driver = webdriver.Chrome(options=options)

        # Hide webdriver property
        driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        return driver

    def _extract_build_id_from_logs(self) -> str | None:
        """Extract build ID from Chrome network logs."""
        logs = self.driver.get_log("performance")

        for log in logs:
            message = json.loads(log["message"])

            # Look for Network.responseReceived events
            if message["message"]["method"] == "Network.responseReceived":
                response = message["message"]["params"]["response"]
                url = response.get("url", "")

                # Check if this is a _next/data request
                if url.startswith(self.DATA_URL_PREFIX):
                    # Extract build ID from URL
                    # Format: https://www.actionnetwork.com/_next/data/{BUILD_ID}/...
                    match = re.search(r"/_next/data/([^/]+)/", url)
                    if match:
                        build_id = match.group(1)
                        self.logger.info(
                            f"Found build ID in URL: {url}", build_id=build_id
                        )
                        return build_id

        return None

    def _extract_build_id_from_html(self) -> str | None:
        """Extract build ID from HTML page source as fallback."""
        try:
            page_source = self.driver.page_source

            # Look for Next.js script tags that might contain build ID
            patterns = [
                r"/_next/static/([^/]+)/_buildManifest\.js",
                r"/_next/static/([^/]+)/_ssgManifest\.js",
                r'"buildId":"([^"]+)"',
                r"/_next/data/([^/]+)/",
            ]

            for pattern in patterns:
                matches = re.finditer(pattern, page_source)
                for match in matches:
                    build_id = match.group(1)
                    if build_id and len(build_id) > 10:  # Build IDs are typically long
                        self.logger.info(
                            f"Found build ID in HTML with pattern: {pattern}",
                            build_id=build_id,
                        )
                        return build_id

        except Exception as e:
            self.logger.warning("Failed to extract build ID from HTML", error=str(e))

        return None

    def extract_build_id(self) -> str | None:
        """
        Extract the current Next.js build ID from Action Network.

        Returns:
            The build ID string, or None if extraction failed
        """
        try:
            self.logger.info("Starting build ID extraction")
            self.driver = self._setup_driver_with_network_logging()

            # Navigate to sharp report page
            self.logger.info("Loading Action Network sharp report page")
            self.driver.get(self.SHARP_REPORT_URL)

            # Wait for page to load
            try:
                WebDriverWait(self.driver, self.timeout).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            except Exception:
                self.logger.warning("Page load timeout, continuing anyway")

            # Give it a moment for network requests to complete
            time.sleep(5)

            # Try to extract from network logs first
            build_id = self._extract_build_id_from_logs()

            if not build_id:
                self.logger.info("Build ID not found in logs, trying HTML extraction")
                build_id = self._extract_build_id_from_html()

            if build_id:
                self.logger.info("Successfully extracted build ID", build_id=build_id)
                return build_id
            else:
                self.logger.error("Failed to extract build ID from any source")
                return None

        except Exception as e:
            self.logger.error("Build ID extraction failed", error=str(e))
            return None
        finally:
            if self.driver:
                self.driver.quit()

    def get_game_data_url(
        self, game_id: str, date: str, team_slug: str, build_id: str | None = None
    ) -> str:
        """
        Construct the full Action Network game data URL.

        Args:
            game_id: The game ID (e.g., "257324")
            date: Date in format "july-1-2025"
            team_slug: Team slug like "yankees-blue-jays"
            build_id: Build ID (if None, will extract dynamically)

        Returns:
            Complete URL for the game data
        """
        if build_id is None:
            build_id = self.extract_build_id()
            if not build_id:
                raise ValueError("Could not extract build ID")

        # Construct the URL
        url = (
            f"https://www.actionnetwork.com/_next/data/{build_id}/mlb-game/"
            f"{team_slug}-score-odds-{date}/{game_id}.json"
            f"?league=mlb-game&slug={team_slug}-score-odds-{date}&gameId={game_id}"
        )

        return url


def extract_build_id() -> str | None:
    """
    Convenience function to extract build ID.

    Returns:
        The current Next.js build ID or None if extraction failed
    """
    extractor = ActionNetworkBuildExtractor()
    return extractor.extract_build_id()


def get_current_build_id_info() -> dict[str, Any]:
    """
    Get comprehensive information about the current build ID.

    Returns:
        Dictionary with build ID and metadata
    """
    extractor = ActionNetworkBuildExtractor()
    build_id = extractor.extract_build_id()

    return {
        "build_id": build_id,
        "extracted_at": time.time(),
        "success": build_id is not None,
        "base_url": extractor.DATA_URL_PREFIX,
        "example_url": (
            f"{extractor.DATA_URL_PREFIX}{build_id}/mlb-game/yankees-blue-jays-score-odds-july-1-2025/257324.json"
            if build_id
            else None
        ),
    }


if __name__ == "__main__":
    # Demo usage
    print("🔍 Extracting Action Network Build ID...")

    info = get_current_build_id_info()

    if info["success"]:
        print(f"✅ Build ID: {info['build_id']}")
        print(f"🔗 Example URL: {info['example_url']}")

        # Test URL construction
        extractor = ActionNetworkBuildExtractor()
        test_url = extractor.get_game_data_url(
            game_id="257324",
            date="july-1-2025",
            team_slug="yankees-blue-jays",
            build_id=info["build_id"],
        )
        print(f"🧪 Test URL: {test_url}")
    else:
        print("❌ Failed to extract build ID")
