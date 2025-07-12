"""
Integration tests for the Phase 1 refactoring of the SportsbookReview collection pipeline.
"""
import asyncio
import pytest
import asyncpg
import os
from datetime import date
from pathlib import Path
import json
import aiohttp

from sportsbookreview.services.collection_orchestrator import CollectionOrchestrator
from sportsbookreview.services.data_storage_service import DataStorageService
from sportsbookreview.services.sportsbookreview_scraper import SportsbookReviewScraper
from sportsbookreview.utils.circuit_breaker import CircuitBreakerOpenException

# It is a good practice to use a separate test database
# We will assume it's configured via environment variables
# e.g., TEST_DB_URL=postgresql://user:pass@host/db
DB_CONFIG = {
    "user": os.environ.get("TEST_DB_USER", "samlafell"),
    "host": os.environ.get("TEST_DB_HOST", "localhost"),
    "port": os.environ.get("TEST_DB_PORT", 5432),
    "database": "mlb_betting",
}
TEST_DB_NAME = "test_sbr_phase1"

async def setup_staging_schema(conn):
    """Applies the staging schema to the test database."""
    schema_path = Path(__file__).parent.parent.parent / "sql" / "sportsbookreview_staging_schema.sql"
    with open(schema_path, "r") as f:
        await conn.execute(f.read())

@pytest.fixture(scope="module")
def event_loop():
    """Create an instance of the default event loop for each test module."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(scope="module")
async def test_db():
    """Creates a temporary test database for the module."""
    conn = await asyncpg.connect(**DB_CONFIG)
    try:
        await conn.execute(f"DROP DATABASE IF EXISTS {TEST_DB_NAME}")
        await conn.execute(f"CREATE DATABASE {TEST_DB_NAME}")
        yield
    finally:
        await conn.execute(f"DROP DATABASE IF EXISTS {TEST_DB_NAME}")
        await conn.close()

@pytest.fixture(scope="function")
async def db_connection(test_db):
    """Fixture for a test database connection."""
    test_db_config = DB_CONFIG.copy()
    test_db_config["database"] = TEST_DB_NAME
    conn = await asyncpg.connect(**test_db_config)
    await setup_staging_schema(conn)
    yield conn
    # Teardown: drop tables
    await conn.execute("DROP TABLE IF EXISTS sbr_parsed_games, sbr_raw_html CASCADE;")
    await conn.close()


@pytest.fixture
def mock_sbr_html():
    """Returns content of the sample SBR HTML file."""
    fixture_path = Path(__file__).parent.parent / "fixtures" / "sample_sbr_page.html"
    with open(fixture_path, "r") as f:
        return f.read()

@pytest.fixture
def mock_invalid_sbr_html():
    """Returns content of the invalid sample SBR HTML file."""
    fixture_path = Path(__file__).parent.parent / "fixtures" / "invalid_sbr_page.html"
    with open(fixture_path, "r") as f:
        return f.read()

# Mark the entire class to be run with asyncio
@pytest.mark.asyncio
class TestPhase1Refactor:
    """
    Test suite for the Phase 1 refactoring of the SportsbookReview scraper.
    """

    async def test_successful_run_stores_data_in_staging(self, db_connection, aiohttp_mocker, mock_sbr_html):
        """
        Tests a successful run where data is scraped, parsed, validated,
        and stored in the staging tables.
        """
        test_date = date(2024, 7, 27)
        # Mock all potential URLs for the day
        base_url = "https://www.sportsbookreview.com/betting-odds/mlb-baseball"
        aiohttp_mocker.get(f"{base_url}/?date={test_date.strftime('%Y-%m-%d')}", text=mock_sbr_html)
        aiohttp_mocker.get(f"{base_url}/pointspread/full-game/?date={test_date.strftime('%Y-%m-%d')}", text=mock_sbr_html)
        aiohttp_mocker.get(f"{base_url}/totals/full-game/?date={test_date.strftime('%Y-%m-%d')}", text=mock_sbr_html)

        # Create a connection pool for the test
        test_db_config = DB_CONFIG.copy()
        test_db_config["database"] = TEST_DB_NAME
        pool = await asyncpg.create_pool(**test_db_config)
        
        storage_service = DataStorageService(pool=pool)
        scraper = SportsbookReviewScraper(storage_service=storage_service)

        try:
            await scraper.start_session()
            await scraper.scrape_date_all_bet_types(test_date)

            # Assertions
            raw_count = await db_connection.fetchval("SELECT COUNT(*) FROM sbr_raw_html")
            assert raw_count == 3  # One for each bet type

            parsed_count = await db_connection.fetchval("SELECT COUNT(*) FROM sbr_parsed_games")
            assert parsed_count == 3 # One for each bet type

            parsed_game = await db_connection.fetchrow("SELECT * FROM sbr_parsed_games")
            game_data = json.loads(parsed_game['game_data'])
            assert game_data['sbr_game_id'] == '12345'
            assert game_data['home_team'] == 'New York Yankees'
        finally:
            await scraper.close_session()
            await pool.close()

    async def test_validation_failure_prevents_storage(self, db_connection, aiohttp_mocker, mock_invalid_sbr_html):
        """
        Tests that if parsed data fails validation, it is not stored in
        the sbr_parsed_games staging table.
        """
        test_date = date(2024, 7, 27)
        # Mock all potential URLs for the day
        base_url = "https://www.sportsbookreview.com/betting-odds/mlb-baseball"
        aiohttp_mocker.get(f"{base_url}/?date={test_date.strftime('%Y-%m-%d')}", text=mock_invalid_sbr_html)
        aiohttp_mocker.get(f"{base_url}/pointspread/full-game/?date={test_date.strftime('%Y-%m-%d')}", text=mock_invalid_sbr_html)
        aiohttp_mocker.get(f"{base_url}/totals/full-game/?date={test_date.strftime('%Y-%m-%d')}", text=mock_invalid_sbr_html)

        # Create a connection pool for the test
        test_db_config = DB_CONFIG.copy()
        test_db_config["database"] = TEST_DB_NAME
        pool = await asyncpg.create_pool(**test_db_config)
        
        storage_service = DataStorageService(pool=pool)
        scraper = SportsbookReviewScraper(storage_service=storage_service)

        try:
            await scraper.start_session()
            await scraper.scrape_date_all_bet_types(test_date)

            # Assertions
            raw_count = await db_connection.fetchval("SELECT COUNT(*) FROM sbr_raw_html")
            assert raw_count == 3  # Raw HTML is always stored

            parsed_count = await db_connection.fetchval("SELECT COUNT(*) FROM sbr_parsed_games")
            assert parsed_count == 0  # Parsed data should be zero due to validation failure
        finally:
            await scraper.close_session()
            await pool.close()

    async def test_circuit_breaker_opens_on_failures(self, aiohttp_mocker, monkeypatch):
        """
        Tests that the circuit breaker opens after repeated failures
        and prevents further requests.
        """
        # Disable backoff sleep to make the test faster and more predictable
        monkeypatch.setattr(asyncio, "sleep", lambda x: asyncio.sleep(0))

        test_date = date(2024, 7, 27)
        url = f"https://www.sportsbookreview.com/betting-odds/mlb-baseball/?date={test_date.strftime('%Y-%m-%d')}"
        aiohttp_mocker.get(url, status=500) # Always fail

        # Configure scraper with a low threshold
        scraper = SportsbookReviewScraper(
            cb_failure_threshold=2,
            cb_recovery_timeout=10
        )
        
        # This is needed to avoid a real DB connection
        scraper.storage_service = None

        try:
            await scraper.start_session()
            
            # First two calls should fail with the underlying exception
            with pytest.raises(aiohttp.ClientResponseError):
                await scraper.scrape_bet_type_page(url, "moneyline", test_date)
            
            with pytest.raises(aiohttp.ClientResponseError):
                await scraper.scrape_bet_type_page(url, "moneyline", test_date)
            
            # The circuit should now be open. Third call should raise CircuitBreakerOpenException.
            with pytest.raises(CircuitBreakerOpenException):
                await scraper.scrape_bet_type_page(url, "moneyline", test_date)

            # Verify that no more requests are made.
            # The backoff decorator will retry 3 times for each of the 2 calls that fail.
            # So, 2 calls * 3 tries = 6 requests
            assert len(aiohttp_mocker.requests) == 6

        finally:
            await scraper.close_session() 