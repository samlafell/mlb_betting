"""
Test VSIN date validation functionality.

This module tests the enhanced date validation features added to the VSIN scraper
to ensure we're scraping the correct day's data.
"""

from datetime import datetime
from unittest.mock import patch

import pytest
import pytz
from bs4 import BeautifulSoup

from src.mlb_sharp_betting.scrapers.vsin import VSINScraper


class TestVSINDateValidation:
    """Test date validation functionality in VSIN scraper."""

    @pytest.fixture
    def scraper(self):
        """Create a VSIN scraper instance."""
        return VSINScraper()

    def test_get_est_now(self, scraper):
        """Test EST timezone handling."""
        est_time = scraper._get_est_now()
        assert est_time.tzinfo is not None
        assert est_time.tzinfo.zone == "US/Eastern"

    def test_get_target_date_est_none(self, scraper):
        """Test target date with None input."""
        target_date = scraper._get_target_date_est(None)
        today_est = scraper._get_est_now().date()
        assert target_date == today_est

    def test_get_target_date_est_naive_datetime(self, scraper):
        """Test target date with naive datetime."""
        naive_dt = datetime(2025, 6, 30, 15, 30)
        target_date = scraper._get_target_date_est(naive_dt)
        assert target_date == datetime(2025, 6, 30).date()

    def test_get_target_date_est_aware_datetime(self, scraper):
        """Test target date with timezone-aware datetime."""
        utc_tz = pytz.UTC
        aware_dt = utc_tz.localize(datetime(2025, 6, 30, 19, 30))  # 3:30 PM EST
        target_date = scraper._get_target_date_est(aware_dt)
        assert target_date == datetime(2025, 6, 30).date()

    def test_build_url_tomorrow_view(self, scraper):
        """Test URL building with tomorrow view."""
        url = scraper.build_url("mlb", "circa", use_tomorrow=True)
        expected = "https://data.vsin.com/betting-splits/?bookid=circa&view=tomorrow"
        assert url == expected

    def test_build_url_regular_view(self, scraper):
        """Test URL building with regular view."""
        url = scraper.build_url("mlb", "circa", use_tomorrow=False)
        expected = "https://data.vsin.com/betting-splits/?bookid=circa&view=mlb"
        assert url == expected

    def test_parse_date_from_text_various_formats(self, scraper):
        """Test parsing dates from various text formats."""
        test_cases = [
            ("Monday,Jun 30", 6, 30),
            ("Monday, Jun 30", 6, 30),
            ("Tuesday Jul 15", 7, 15),
            ("Jun 30", 6, 30),
            ("December 25", 12, 25),
        ]

        for text, expected_month, expected_day in test_cases:
            result = scraper._parse_date_from_text(text)
            assert result is not None, f"Failed to parse: {text}"
            assert result.month == expected_month
            assert result.day == expected_day

    def test_extract_date_from_html(self, scraper):
        """Test extracting date from HTML content."""
        # Mock HTML similar to VSIN's structure
        html_content = """
        <html>
            <body>
                <table>
                    <thead>
                        <tr>
                            <th class="text-center">
                                MLB - <span>
                                    <a class="txt-color-white text-center bold" 
                                       href="/mlb/games/?gamedate=2025-06-30">Monday,Jun 30</a>
                                </span>
                            </th>
                        </tr>
                    </thead>
                </table>
            </body>
        </html>
        """

        soup = BeautifulSoup(html_content, "html.parser")
        result = scraper._extract_date_from_html(soup)

        assert result is not None
        assert result == datetime(2025, 6, 30).date()

    def test_validate_date_match_exact(self, scraper):
        """Test date validation with exact match."""
        date1 = datetime(2025, 6, 30).date()
        date2 = datetime(2025, 6, 30).date()
        assert scraper._validate_date_match(date1, date2) is True

    def test_validate_date_match_tolerance(self, scraper):
        """Test date validation within tolerance."""
        date1 = datetime(2025, 6, 30).date()
        date2 = datetime(2025, 6, 29).date()  # 1 day difference
        assert scraper._validate_date_match(date1, date2) is True

    def test_validate_date_match_outside_tolerance(self, scraper):
        """Test date validation outside tolerance."""
        date1 = datetime(2025, 6, 30).date()
        date2 = datetime(2025, 6, 27).date()  # 3 days difference
        assert scraper._validate_date_match(date1, date2) is False

    def test_should_use_tomorrow_view_early_morning(self, scraper):
        """Test tomorrow view logic for early morning."""
        est_tz = pytz.timezone("US/Eastern")
        early_morning = est_tz.localize(datetime(2025, 6, 30, 3, 30))  # 3:30 AM EST
        assert scraper._should_use_tomorrow_view(early_morning) is True

    def test_should_use_tomorrow_view_later_morning(self, scraper):
        """Test tomorrow view logic for later morning."""
        est_tz = pytz.timezone("US/Eastern")
        later_morning = est_tz.localize(datetime(2025, 6, 30, 8, 30))  # 8:30 AM EST
        assert scraper._should_use_tomorrow_view(later_morning) is False

    @pytest.mark.asyncio
    async def test_scrape_with_date_validation_success(self, scraper):
        """Test scraping with successful date validation."""
        # Mock the _get_soup method to return our test HTML
        html_content = """
        <html>
            <body>
                <table class="freezetable table table-sm mb-0">
                    <thead>
                        <tr>
                            <th>
                                <a href="/mlb/games/?gamedate=2025-06-30">Monday,Jun 30</a>
                            </th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>Test Data</td>
                        </tr>
                    </tbody>
                </table>
            </body>
        </html>
        """

        soup = BeautifulSoup(html_content, "html.parser")

        with patch.object(scraper, "_get_soup", return_value=soup):
            with patch.object(
                scraper,
                "_parse_betting_splits",
                return_value=[{"game": "Test Game", "sport": "mlb"}],
            ):
                target_date = datetime(2025, 6, 30)
                result = await scraper.scrape(sport="mlb", target_date=target_date)

                assert result.success is True
                assert len(result.data) > 0
                assert result.metadata["extracted_date"] == "2025-06-30"
                assert result.metadata["target_date"] == "2025-06-30"


if __name__ == "__main__":
    pytest.main([__file__])
