"""
Base scraper classes and interfaces.

This module provides abstract base classes for data scrapers with
retry logic, rate limiting, and comprehensive error handling.
"""

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import httpx
import structlog
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from ..core.exceptions import NetworkError, RateLimitError, ScrapingError

logger = structlog.get_logger(__name__)


@dataclass
class ScrapingResult:
    """Result of a scraping operation."""

    success: bool
    data: list[dict[str, Any]]
    source: str
    timestamp: datetime
    errors: list[str]
    metadata: dict[str, Any] | None = None
    request_count: int = 0
    response_time_ms: float = 0.0

    @property
    def has_data(self) -> bool:
        """Check if scraping result contains data."""
        return self.success and bool(self.data)

    @property
    def error_count(self) -> int:
        """Get number of errors encountered."""
        return len(self.errors)

    @property
    def data_count(self) -> int:
        """Get number of data items scraped."""
        return len(self.data)


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""

    requests_per_second: float = 1.0
    requests_per_minute: float = 30.0
    burst_size: int = 5
    backoff_factor: float = 2.0
    max_backoff: float = 300.0  # 5 minutes


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""

    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True


class ScraperError(ScrapingError):
    """Base exception for scraper-related errors."""

    pass


class RateLimiter:
    """Rate limiter for controlling request frequency."""

    def __init__(self, config: RateLimitConfig) -> None:
        """
        Initialize rate limiter.

        Args:
            config: Rate limiting configuration
        """
        self.config = config
        self.request_times: list[float] = []
        self.burst_count = 0
        self.last_burst_reset = time.time()

    async def acquire(self) -> None:
        """Acquire permission to make a request."""
        now = time.time()

        # Clean old request times (older than 1 minute)
        self.request_times = [t for t in self.request_times if now - t < 60]

        # Reset burst count if needed
        if now - self.last_burst_reset >= 60:  # Reset every minute
            self.burst_count = 0
            self.last_burst_reset = now

        # Check burst limit
        if self.burst_count >= self.config.burst_size:
            burst_delay = 60 - (now - self.last_burst_reset)
            if burst_delay > 0:
                logger.debug("Burst limit reached, waiting", delay=burst_delay)
                await asyncio.sleep(burst_delay)
                self.burst_count = 0
                self.last_burst_reset = time.time()

        # Check per-minute limit
        minute_requests = len(self.request_times)
        if minute_requests >= self.config.requests_per_minute:
            delay = 60 - (now - self.request_times[0])
            if delay > 0:
                logger.debug("Per-minute limit reached, waiting", delay=delay)
                await asyncio.sleep(delay)

        # Check per-second limit
        recent_requests = [t for t in self.request_times if now - t < 1]
        if len(recent_requests) >= self.config.requests_per_second:
            delay = 1 - (now - recent_requests[0])
            if delay > 0:
                logger.debug("Per-second limit reached, waiting", delay=delay)
                await asyncio.sleep(delay)

        # Record this request
        self.request_times.append(time.time())
        self.burst_count += 1


class BaseScraper(ABC):
    """
    Base class for all data scrapers with retry logic and rate limiting.

    Provides common functionality for web scraping including:
    - HTTP client management
    - Rate limiting
    - Retry logic with exponential backoff
    - Error handling and logging
    """

    def __init__(
        self,
        source_name: str,
        rate_limit_config: RateLimitConfig | None = None,
        retry_config: RetryConfig | None = None,
        timeout: float = 30.0,
        user_agent: str | None = None,
    ) -> None:
        """
        Initialize base scraper.

        Args:
            source_name: Name of the data source
            rate_limit_config: Rate limiting configuration
            retry_config: Retry behavior configuration
            timeout: Request timeout in seconds
            user_agent: Custom user agent string
        """
        self.source_name = source_name
        self.last_scraped: datetime | None = None
        self.logger = logger.bind(scraper=source_name)

        # Configuration
        self.rate_limit_config = rate_limit_config or RateLimitConfig()
        self.retry_config = retry_config or RetryConfig()
        self.timeout = timeout

        # Rate limiter
        self.rate_limiter = RateLimiter(self.rate_limit_config)

        # HTTP client
        self.user_agent = user_agent or (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )

        self._client: httpx.AsyncClient | None = None

        # Metrics
        self.total_requests = 0
        self.failed_requests = 0
        self.total_response_time = 0.0

    async def __aenter__(self):
        """Async context manager entry."""
        await self._ensure_client()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

    async def _ensure_client(self) -> None:
        """Ensure HTTP client is initialized."""
        if self._client is None:
            headers = {
                "User-Agent": self.user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }

            self._client = httpx.AsyncClient(
                headers=headers,
                timeout=self.timeout,
                follow_redirects=True,
                limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
            )

    async def close(self) -> None:
        """Close HTTP client and cleanup resources."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=60),
        retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
        before_sleep=before_sleep_log(logger, log_level=logging.WARNING),
    )
    async def _make_request(
        self,
        url: str,
        method: str = "GET",
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> httpx.Response:
        """
        Make HTTP request with retry logic and rate limiting.

        Args:
            url: Request URL
            method: HTTP method
            headers: Additional headers
            params: Query parameters
            **kwargs: Additional request arguments

        Returns:
            HTTP response

        Raises:
            NetworkError: If request fails after retries
            RateLimitError: If rate limited
        """
        await self._ensure_client()
        await self.rate_limiter.acquire()

        start_time = time.time()

        try:
            # Merge headers
            request_headers = {}
            if headers:
                request_headers.update(headers)

            self.logger.debug("Making request", url=url, method=method)

            response = await self._client.request(
                method=method, url=url, headers=request_headers, params=params, **kwargs
            )

            response_time = (time.time() - start_time) * 1000
            self.total_requests += 1
            self.total_response_time += response_time

            # Check for rate limiting
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    delay = float(retry_after)
                    self.logger.warning("Rate limited, waiting", delay=delay)
                    await asyncio.sleep(delay)
                    raise RateLimitError(f"Rate limited, retry after {delay}s")
                else:
                    raise RateLimitError("Rate limited (no retry-after header)")

            # Raise for HTTP errors
            response.raise_for_status()

            self.logger.debug(
                "Request successful",
                status_code=response.status_code,
                response_time_ms=response_time,
            )

            return response

        except httpx.RequestError as e:
            self.failed_requests += 1
            self.logger.error("Request failed", url=url, error=str(e))
            raise NetworkError(f"Request failed: {e}")
        except httpx.HTTPStatusError as e:
            self.failed_requests += 1
            self.logger.error("HTTP error", status_code=e.response.status_code, url=url)
            raise NetworkError(f"HTTP {e.response.status_code}: {e}")

    @abstractmethod
    async def scrape(self, **kwargs: Any) -> ScrapingResult:
        """
        Scrape data from the source.

        Args:
            **kwargs: Scraping parameters

        Returns:
            ScrapingResult containing scraped data
        """
        pass

    def _create_result(
        self,
        success: bool,
        data: list[dict[str, Any]],
        errors: list[str],
        metadata: dict[str, Any] | None = None,
        request_count: int = 0,
        response_time_ms: float = 0.0,
    ) -> ScrapingResult:
        """Create a standardized scraping result."""
        result = ScrapingResult(
            success=success,
            data=data,
            source=self.source_name,
            timestamp=datetime.now(),
            errors=errors,
            metadata=metadata or {},
            request_count=request_count,
            response_time_ms=response_time_ms,
        )

        self.last_scraped = result.timestamp

        # Log scraping metrics
        self.logger.info(
            "Scraping completed",
            success=success,
            data_count=len(data),
            error_count=len(errors),
            response_time_ms=response_time_ms,
        )

        return result

    @property
    def success_rate(self) -> float:
        """Calculate request success rate."""
        if self.total_requests == 0:
            return 0.0
        return (self.total_requests - self.failed_requests) / self.total_requests

    @property
    def average_response_time(self) -> float:
        """Calculate average response time in milliseconds."""
        if self.total_requests == 0:
            return 0.0
        return self.total_response_time / self.total_requests

    def get_metrics(self) -> dict[str, Any]:
        """Get scraper performance metrics."""
        return {
            "source": self.source_name,
            "total_requests": self.total_requests,
            "failed_requests": self.failed_requests,
            "success_rate": self.success_rate,
            "average_response_time_ms": self.average_response_time,
            "last_scraped": self.last_scraped.isoformat()
            if self.last_scraped
            else None,
        }


class HTMLScraper(BaseScraper):
    """Base class for HTML-based scrapers."""

    async def _get_soup(self, url: str, **kwargs: Any):
        """
        Get BeautifulSoup object from URL.

        Args:
            url: URL to scrape
            **kwargs: Additional request arguments

        Returns:
            BeautifulSoup object
        """
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            raise ImportError("BeautifulSoup4 is required for HTML scraping")

        response = await self._make_request(url, **kwargs)
        return BeautifulSoup(response.text, "html.parser")


class JSONScraper(BaseScraper):
    """Base class for JSON API scrapers."""

    async def _get_json(self, url: str, **kwargs: Any) -> dict[str, Any]:
        """
        Get JSON data from URL.

        Args:
            url: URL to scrape
            **kwargs: Additional request arguments

        Returns:
            Parsed JSON data
        """
        response = await self._make_request(url, **kwargs)
        try:
            return response.json()
        except Exception as e:
            raise ScrapingError(f"Failed to parse JSON response: {e}")


__all__ = [
    "ScraperError",
    "BaseScraper",
    "HTMLScraper",
    "JSONScraper",
    "ScrapingResult",
    "RateLimitConfig",
    "RetryConfig",
    "RateLimiter",
]
