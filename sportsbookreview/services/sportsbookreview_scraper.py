"""
SportsbookReview.com scraper service for collecting historical betting data.

This service handles the web scraping of SportsbookReview.com for historical
MLB betting data from April 4, 2021 to current date.
"""

import asyncio
import logging
import time
from datetime import date, datetime, timedelta
from typing import Any

import aiohttp
import backoff
from bs4 import BeautifulSoup

from ..parsers.sportsbookreview_parser import SportsbookReviewParser
from ..utils.circuit_breaker import CircuitBreaker, CircuitBreakerOpenException
from .data_storage_service import DataStorageService

logger = logging.getLogger(__name__)


class SportsbookReviewScraper:
    """
    Web scraper for SportsbookReview.com historical betting data.

    Handles rate limiting, error recovery, and data extraction for the
    specified date range (April 4, 2021 to current date).
    """

    def __init__(
        self,
        storage_service: DataStorageService | None = None,
        base_url: str = "https://www.sportsbookreview.com",
        rate_limit_delay: float = 2.0,
        max_concurrent_requests: int = 3,
        timeout: int = 30,
        cb_failure_threshold: int = 5,
        cb_recovery_timeout: int = 60,
    ):
        """
        Initialize the scraper with configuration.

        Args:
            storage_service: Instance of DataStorageService
            base_url: Base URL for SportsbookReview
            rate_limit_delay: Delay between requests in seconds
            max_concurrent_requests: Maximum concurrent requests
            timeout: Request timeout in seconds
            cb_failure_threshold: Circuit breaker failure threshold
            cb_recovery_timeout: Circuit breaker recovery timeout
        """
        self.storage_service = storage_service
        self.base_url = base_url
        self.rate_limit_delay = rate_limit_delay
        self.max_concurrent_requests = max_concurrent_requests
        self.timeout = timeout

        # SportsbookReview URL patterns for different bet types
        self.url_patterns = {
            "moneyline": f"{base_url}/betting-odds/mlb-baseball/?date={{date}}",
            "spread": f"{base_url}/betting-odds/mlb-baseball/pointspread/full-game/?date={{date}}",
            "totals": f"{base_url}/betting-odds/mlb-baseball/totals/full-game/?date={{date}}",
        }

        # Session management
        self.session: aiohttp.ClientSession | None = None
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)

        # --------------------------------------------------
        # Adaptive rate-limiter (Phase-2 requirement)
        # --------------------------------------------------
        try:
            from mlb_sharp_betting.services.rate_limiter import (
                RequestRateTracker,
                get_rate_limiter,
            )

            self.unified_rate_limiter = get_rate_limiter()

            # Dynamically register a tracker for SBR if not defined
            if (
                "sportsbookreview_scraper"
                not in self.unified_rate_limiter.rate_trackers
            ):
                self.unified_rate_limiter.rate_trackers["sportsbookreview_scraper"] = (
                    RequestRateTracker(
                        service_name="sportsbookreview_scraper",
                        max_requests_per_minute=60,  # Increased from 10 (6x faster)
                        max_requests_per_hour=1000,  # Increased from 300 (3.3x faster)
                        request_delay_seconds=0.3,  # Reduced from 1.0 (3.3x faster)
                        burst_limit=15,  # Increased from 5 (3x more)
                    )
                )
        except ImportError:
            # Fallback to None if core package unavailable (tests)
            self.unified_rate_limiter = None

        # Request tracking
        self.requests_made = 0
        self.last_request_time = 0.0
        self.failed_urls: set[str] = set()

        # Parser for HTML content
        self.parser = SportsbookReviewParser()

        # --------------------------------------------------
        # Simple in-process fetch cache (URL → (timestamp, html))
        # --------------------------------------------------
        self._fetch_cache: dict[str, tuple[float, str]] = {}
        self.cache_ttl_seconds: float = 600  # 10-minute TTL

        # Circuit Breaker
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=cb_failure_threshold,
            recovery_timeout=cb_recovery_timeout,
            expected_exception=(aiohttp.ClientError, asyncio.TimeoutError),
        )
        self.fetch_url = self.circuit_breaker(self._fetch_url)

        # Headers to appear more like a real browser
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:140.0) Gecko/20100101 Firefox/140.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Referer": "https://www.reddit.com/",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-User": "?1",
            "Sec-GPC": "1",
            "Priority": "u=0, i",
            "TE": "trailers",
        }

    async def __aenter__(self):
        """Async context manager entry."""
        await self.start_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close_session()

    async def start_session(self):
        """Start the HTTP session."""
        if self.session is None:
            connector = aiohttp.TCPConnector(
                limit=self.max_concurrent_requests,
                limit_per_host=self.max_concurrent_requests,
                ttl_dns_cache=300,
                use_dns_cache=True,
            )

            timeout = aiohttp.ClientTimeout(total=self.timeout)

            self.session = aiohttp.ClientSession(
                connector=connector, timeout=timeout, headers=self.headers
            )

    async def close_session(self):
        """Close the HTTP session."""
        if self.session:
            await self.session.close()
            self.session = None

    async def scrape_historical_data(
        self,
        start_date: date = date(2021, 4, 4),
        end_date: date | None = None,
        progress_callback: callable | None = None,
    ) -> list[dict[str, Any]]:
        """
        Scrape historical betting data for the specified date range.

        Args:
            start_date: Start date for scraping (default: April 4, 2021)
            end_date: End date for scraping (default: current date)
            progress_callback: Optional callback for progress reporting

        Returns:
            List of dictionaries containing game and betting data
        """
        if end_date is None:
            end_date = date.today()

        logger.info(f"Starting historical scrape from {start_date} to {end_date}")

        total_days = (end_date - start_date).days + 1
        days_processed = 0

        current_date = start_date
        while current_date <= end_date:
            try:
                logger.info(f"Scraping data for {current_date}")

                # Scrape all three bet types for this date
                await self.scrape_date_all_bet_types(current_date)

                days_processed += 1

                # Progress callback
                if progress_callback:
                    progress = days_processed / total_days * 100
                    progress_callback(
                        progress,
                        f"Completed {current_date} ({days_processed}/{total_days} days)",
                    )

                # Rate limiting between days
                await asyncio.sleep(self.rate_limit_delay)

            except Exception as e:
                logger.error(f"Error scraping data for {current_date}: {e}")

            current_date += timedelta(days=1)

        logger.info("Historical scrape completed.")

    async def scrape_date_all_bet_types(self, game_date: date) -> None:
        """
        Scrape all bet types (moneyline, spread, totals) for a specific date.

        Args:
            game_date: Date to scrape
        """
        date_str = game_date.strftime("%Y-%m-%d")

        # Create URLs for all three bet types
        urls = {}
        for bet_type, pattern in self.url_patterns.items():
            urls[bet_type] = pattern.format(date=date_str)

        # Scrape all bet types concurrently
        tasks = []
        for bet_type, url in urls.items():
            task = asyncio.create_task(
                self.scrape_bet_type_page(url, bet_type, game_date)
            )
            tasks.append(task)

        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Task failed with exception: {result}")

    async def scrape_bet_type_page(
        self, url: str, bet_type: str, game_date: date
    ) -> None:
        """
        Scrape a specific bet type page for a given date.

        Args:
            url: URL to scrape
            bet_type: Type of bet (moneyline, spread, totals)
            game_date: Date of the games

        Returns:
            List of game data dictionaries
        """
        try:
            async with self.semaphore:
                # Rate limiting
                await self.rate_limit()

                # Fetch the page
                html_content = await self.fetch_url(url)

                # Store raw HTML
                raw_html_id = None
                if self.storage_service and html_content:
                    raw_html_id = await self.storage_service.store_raw_html(
                        url, html_content
                    )

                # Parse the content
                parsed_data = self.parser.parse_bet_type_page(
                    html_content, bet_type, game_date, url
                )

                # Store parsed data
                if self.storage_service and raw_html_id and parsed_data:
                    await self.storage_service.store_parsed_data(
                        raw_html_id, parsed_data
                    )

                logger.debug(
                    f"Successfully scraped {bet_type} for {game_date}: {len(parsed_data) if parsed_data else 0} games"
                )

        except CircuitBreakerOpenException:
            logger.error(f"Circuit breaker is open for {url}. Skipping.")
            self.failed_urls.add(url)
        except Exception as e:
            logger.error(f"Error scraping {bet_type} page for {game_date}: {e}")
            self.failed_urls.add(url)

    async def rate_limit(self):
        """Adaptive rate limiting using UnifiedRateLimiter (fallback to sleep)."""
        if self.unified_rate_limiter:
            try:
                result = await self.unified_rate_limiter.wait_for_rate_limit(
                    "sportsbookreview_scraper",
                    max_wait_seconds=60,
                )
                if not result.allowed:
                    # If we exhausted max_wait_seconds without allowance, raise
                    raise Exception(
                        f"Rate limit still active after waiting ({result.reason})"
                    )
                await self.unified_rate_limiter.record_request(
                    "sportsbookreview_scraper"
                )
                return
            except Exception as e:
                logger.warning(f"UnifiedRateLimiter wait failed: {e}. Falling back.")

        # Fallback behaviour – original fixed delay
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time

        if time_since_last_request < self.rate_limit_delay:
            await asyncio.sleep(self.rate_limit_delay - time_since_last_request)

        self.last_request_time = time.time()

    @backoff.on_exception(
        backoff.expo, (aiohttp.ClientError, asyncio.TimeoutError), max_tries=3, factor=2
    )
    async def _fetch_url(self, url: str) -> str:
        """
        Fetch the content of a URL with backoff for retries.

        Args:
            url: The URL to fetch

        Returns:
            The HTML content of the page as a string
        """
        if self.session is None:
            await self.start_session()

        logger.info(f"Fetching URL: {url}")

        # Cache lookup – return cached HTML if still fresh
        now_ts = time.time()
        if url in self._fetch_cache:
            ts, cached_html = self._fetch_cache[url]
            if now_ts - ts < self.cache_ttl_seconds:
                return cached_html

        async with self.session.get(url, allow_redirects=True) as response:
            response.raise_for_status()
            self.requests_made += 1

            # Use a more robust way to get the encoding
            content_type = response.headers.get("content-type", "").lower()
            charset = "utf-8"
            if "charset=" in content_type:
                charset = content_type.split("charset=")[-1].split(";")[0]

            try:
                content = await response.text(encoding=charset)
            except (UnicodeDecodeError, LookupError):
                logger.warning(
                    f"Failed to decode with {charset}, falling back to bytes for {url}"
                )
                # Fallback to reading bytes and decoding with replacement
                content_bytes = await response.read()
                content = content_bytes.decode(charset, errors="replace")

            if len(content) < 100:
                raise ValueError(
                    f"Response content too short for {url}: {len(content)} characters"
                )

            # store in cache
            self._fetch_cache[url] = (now_ts, content)
            return content

    async def scrape_specific_games(self, game_urls: list[str]) -> list[dict[str, Any]]:
        """
        Scrape specific games by URL.

        Args:
            game_urls: List of specific game URLs to scrape

        Returns:
            List of game data dictionaries
        """
        logger.info(f"Scraping {len(game_urls)} specific games")

        all_data = []

        # Process URLs one at a time to be respectful
        for url in game_urls:
            try:
                await self.rate_limit()

                html_content = await self.fetch_url(url)
                if html_content:
                    # Parse the page content
                    parsed_data = self.parser.parse_page(html_content, url)
                    all_data.extend(parsed_data)

                    logger.debug(f"Scraped {len(parsed_data)} records from {url}")

            except Exception as e:
                logger.error(f"Error scraping URL {url}: {e}")
                self.failed_urls.add(url)

        logger.info(f"Completed scraping {len(all_data)} games")
        return all_data

    def get_stats(self) -> dict[str, Any]:
        """
        Get scraping statistics.

        Returns:
            Dictionary with scraping stats
        """
        return {
            "requests_made": self.requests_made,
            "failed_urls": len(self.failed_urls),
            "success_rate": (self.requests_made - len(self.failed_urls))
            / max(self.requests_made, 1)
            * 100,
            "rate_limit_delay": self.rate_limit_delay,
            "max_concurrent_requests": self.max_concurrent_requests,
            "session_active": self.session is not None,
        }

    async def check_url_exists(self, url: str) -> bool:
        """
        Check if a URL exists and has MLB data without downloading full content.

        Args:
            url: URL to check

        Returns:
            True if URL exists and has MLB data, False otherwise
        """
        try:
            if self.session is None:
                await self.start_session()

            # Use HEAD request first to check if URL exists
            async with self.session.head(url, allow_redirects=True) as response:
                if response.status != 200:
                    return False

            # If HEAD succeeds, do a quick GET to check for MLB content
            async with self.session.get(url, allow_redirects=True) as response:
                if response.status != 200:
                    return False

                # Read just the first chunk to check for MLB indicators
                chunk = await response.content.read(1024)  # First 1KB
                content_sample = chunk.decode("utf-8", errors="ignore").lower()

                # Check for MLB-specific content indicators
                mlb_indicators = [
                    "tbody-mlb",
                    "mlb-baseball",
                    "baseball",
                    "moneyline",
                    "spread",
                ]
                has_mlb_content = any(
                    indicator in content_sample for indicator in mlb_indicators
                )

                logger.debug(
                    f"URL check for {url}: status={response.status}, has_mlb={has_mlb_content}"
                )
                return has_mlb_content

        except Exception as e:
            logger.debug(f"URL check failed for {url}: {e}")
            return False

    async def find_season_start_date(
        self, start_search_date: date = None
    ) -> date | None:
        """
        Find the actual start date of the MLB season by checking URLs.

        Args:
            start_search_date: Date to start searching from (default: March 15 of current year)

        Returns:
            Date when MLB data first becomes available, or None if not found
        """
        if start_search_date is None:
            # Start from March 15 of current year
            current_year = date.today().year
            start_search_date = date(current_year, 3, 15)

        logger.info(f"Searching for MLB season start from {start_search_date}")

        # Search forward from start date
        current_date = start_search_date
        max_search_days = 60  # Search up to 60 days forward

        for days_ahead in range(max_search_days):
            search_date = current_date + timedelta(days=days_ahead)

            # Don't search beyond today
            if search_date > date.today():
                break

            # Check moneyline URL for this date (most likely to have data)
            test_url = self.url_patterns["moneyline"].format(
                date=search_date.strftime("%Y-%m-%d")
            )

            logger.debug(f"Checking for MLB data on {search_date}")

            if await self.check_url_exists(test_url):
                logger.info(f"Found MLB season start date: {search_date}")
                return search_date

            # Small delay between checks
            await asyncio.sleep(0.1)

        logger.warning(f"No MLB data found starting from {start_search_date}")
        return None

    async def test_connectivity(self) -> bool:
        """
        Test connectivity to SportsbookReview.com.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Test with a recent date's moneyline page
            test_date = date.today() - timedelta(days=1)
            test_url = self.url_patterns["moneyline"].format(
                date=test_date.strftime("%Y-%m-%d")
            )

            # Use the new URL check method
            if await self.check_url_exists(test_url):
                logger.info("Connectivity test passed")
                return True
            else:
                logger.warning("Connectivity test failed - no MLB content found")
                return False

        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            return False

    async def get_available_sportsbooks(self, game_url: str) -> list[str]:
        """
        Get list of available sportsbooks for a specific game.

        Args:
            game_url: URL of the game

        Returns:
            List of sportsbook names
        """
        try:
            html_content = await self.fetch_url(game_url)
            soup = BeautifulSoup(html_content, "html.parser")

            # Extract sportsbook names from the page
            sportsbooks = []

            # This would need to be adjusted based on actual HTML structure
            sportsbook_elements = soup.find_all(class_="sportsbook-name")
            for element in sportsbook_elements:
                sportsbook_name = self.parser.extract_sportsbook_name(element)
                if sportsbook_name:
                    sportsbooks.append(sportsbook_name)

            return list(set(sportsbooks))  # Remove duplicates

        except Exception as e:
            logger.error(f"Error getting sportsbooks for {game_url}: {e}")
            return []

    async def get_game_urls_for_date(self, game_date: date) -> list[str]:
        """
        Get all game URLs for a specific date.

        Args:
            game_date: Date to get URLs for

        Returns:
            List of game URLs for the date
        """
        date_str = game_date.strftime("%Y-%m-%d")

        # Create URLs for all three bet types
        urls = []
        for bet_type, pattern in self.url_patterns.items():
            url = pattern.format(date=date_str)
            urls.append(url)

        return urls

    async def scrape_games_batch(self, game_urls: list[str]) -> list[dict[str, Any]]:
        """
        Scrape a batch of game URLs.

        Args:
            game_urls: List of URLs to scrape

        Returns:
            List of game data dictionaries
        """
        logger.info(f"Scraping batch of {len(game_urls)} URLs")

        all_game_data = []

        # Process URLs one at a time to be respectful
        for url in game_urls:
            try:
                await self.rate_limit()

                # Determine bet type from URL
                bet_type = "moneyline"
                if "/pointspread/" in url:
                    bet_type = "spread"
                elif "/totals/" in url:
                    bet_type = "totals"

                # Extract date from URL
                if "?date=" in url:
                    date_str = url.split("?date=")[1].split("&")[0]
                    try:
                        game_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                    except ValueError:
                        logger.warning(f"Could not parse date from URL: {url}")
                        continue
                else:
                    logger.warning(f"No date found in URL: {url}")
                    continue

                # Scrape the page
                page_data = await self.scrape_bet_type_page(url, bet_type, game_date)
                all_game_data.extend(page_data)

                logger.debug(f"Scraped {len(page_data)} games from {url}")

            except Exception as e:
                logger.error(f"Error scraping URL {url}: {e}")
                self.failed_urls.add(url)

        logger.info(
            f"Batch scraping completed. Collected {len(all_game_data)} game records"
        )
        return all_game_data


# Convenience functions for common use cases
async def scrape_historical_data(
    start_date: date = date(2021, 4, 4),
    end_date: date | None = None,
    progress_callback: callable | None = None,
) -> list[dict[str, Any]]:
    """
    Convenience function to scrape historical data.

    Args:
        start_date: Start date for scraping
        end_date: End date for scraping
        progress_callback: Optional progress callback

    Returns:
        List of game data dictionaries
    """
    async with SportsbookReviewScraper() as scraper:
        return await scraper.scrape_historical_data(
            start_date=start_date,
            end_date=end_date,
            progress_callback=progress_callback,
        )


async def scrape_specific_games(game_urls: list[str]) -> list[dict[str, Any]]:
    """
    Convenience function to scrape specific games.

    Args:
        game_urls: List of game URLs to scrape

    Returns:
        List of game data dictionaries
    """
    async with SportsbookReviewScraper() as scraper:
        return await scraper.scrape_specific_games(game_urls)


async def test_scraper_connectivity() -> bool:
    """
    Test scraper connectivity to SportsbookReview.com.

    Returns:
        True if connection successful, False otherwise
    """
    async with SportsbookReviewScraper() as scraper:
        return await scraper.test_connectivity()
