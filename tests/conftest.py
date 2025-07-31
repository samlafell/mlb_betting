import sys
import pytest
from pathlib import Path
from typing import Generator

# Add the project root to the Python path
# This is necessary for the tests to be able to import the `sportsbookreview` module
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import testing infrastructure
from tests.utils.test_config import (
    get_test_environment_manager, 
    get_test_config, 
    is_integration_test_enabled,
    is_load_test_enabled,
    skip_if_no_integration,
    skip_if_no_load_tests
)
from tests.utils.logging_utils import setup_secure_test_logging
from tests.utils.database_utils import get_test_db_manager, cleanup_test_environment
from tests.mocks.database import create_mock_db_pool, create_in_memory_db
from tests.mocks.collectors import create_mock_collector_environment
from tests.mocks.external_apis import create_mock_api_environment


# Configure pytest markers
pytest_configure = pytest.hookimpl(hookwrapper=True)


def pytest_configure(config):
    """Configure pytest with custom markers and settings."""
    config.addinivalue_line("markers", "unit: mark test as a unit test")
    config.addinivalue_line("markers", "integration: mark test as an integration test")
    config.addinivalue_line("markers", "load: mark test as a load/performance test")
    config.addinivalue_line("markers", "security: mark test as a security test")
    config.addinivalue_line("markers", "slow: mark test as slow running")


def pytest_collection_modifyitems(config, items):
    """Modify test collection to handle markers and skipping."""
    # Skip integration tests if disabled
    if not is_integration_test_enabled():
        skip_integration = pytest.mark.skip(reason="Integration tests disabled")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_integration)
    
    # Skip load tests if disabled
    if not is_load_test_enabled():
        skip_load = pytest.mark.skip(reason="Load tests disabled")
        for item in items:
            if "load" in item.keywords:
                item.add_marker(skip_load)


@pytest.fixture(scope="session", autouse=True)
def test_environment() -> Generator[None, None, None]:
    """Setup and teardown test environment for entire test session."""
    env_manager = get_test_environment_manager()
    test_config = get_test_config()
    
    # Setup test environment
    env_manager.setup_test_environment()
    setup_secure_test_logging(
        log_level=test_config.log_level,
        include_sanitization=test_config.enable_credential_sanitization
    )
    
    yield
    
    # Teardown test environment
    env_manager.teardown_test_environment()
    cleanup_test_environment()


@pytest.fixture
def test_config():
    """Get test configuration."""
    return get_test_config()


@pytest.fixture
def mock_db_pool():
    """Create mock database connection pool."""
    pool = create_mock_db_pool()
    yield pool
    # Cleanup is automatic with mock pool


@pytest.fixture
def in_memory_db():
    """Create in-memory database for testing."""
    db = create_in_memory_db()
    yield db
    db.clear_all()


@pytest.fixture
def mock_collector_environment():
    """Create mock collector environment."""
    env = create_mock_collector_environment()
    yield env
    # Reset all collectors
    env["utilities"]["reset_all"]()


@pytest.fixture
def mock_api_environment():
    """Create mock API environment."""
    return create_mock_api_environment()


@pytest.fixture
def test_db_manager():
    """Get test database manager."""
    return get_test_db_manager()


# Database-related fixtures for integration tests
@pytest.fixture
@pytest.mark.integration
async def db_connection():
    """Get database connection for integration tests."""
    if skip_if_no_integration():
        pytest.skip("Integration tests disabled")
    
    db_manager = get_test_db_manager()
    await db_manager.initialize()
    
    async with db_manager.get_connection() as conn:
        yield conn
    
    await db_manager.cleanup()


@pytest.fixture
@pytest.mark.integration
async def clean_database():
    """Provide clean database state for integration tests."""
    if skip_if_no_integration():
        pytest.skip("Integration tests disabled")
    
    db_manager = get_test_db_manager()
    await db_manager.initialize()
    
    # Clean up any existing test data
    await db_manager.cleanup_all_test_data()
    
    yield db_manager
    
    # Clean up after test
    await db_manager.cleanup_all_test_data()
    await db_manager.cleanup()


# Performance testing fixtures
@pytest.fixture
@pytest.mark.load
def performance_test_timeout():
    """Get performance test timeout."""
    if skip_if_no_load_tests():
        pytest.skip("Load tests disabled")
    
    return get_test_config().performance_test_timeout


@pytest.fixture
@pytest.mark.load
def load_test_duration():
    """Get load test duration."""
    if skip_if_no_load_tests():
        pytest.skip("Load tests disabled")
    
    return get_test_config().load_test_duration


# Utility functions for tests
def create_test_data_cleanup(test_name: str):
    """Create a cleanup function for test data."""
    def cleanup():
        import asyncio
        db_manager = get_test_db_manager()
        asyncio.run(db_manager.cleanup_test_data(test_name))
    return cleanup
