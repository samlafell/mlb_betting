"""
Test configuration and utilities.

Provides centralized configuration for test environments and common testing utilities.
"""

import os
from dataclasses import dataclass
from typing import Dict, Any, Optional, List
from pathlib import Path

from src.core.config import get_settings


@dataclass
class TestConfig:
    """Configuration for test environments."""
    
    # Test environment settings
    use_mock_database: bool = True
    use_mock_apis: bool = True
    log_level: str = "INFO"
    enable_credential_sanitization: bool = True
    
    # Performance test settings
    performance_test_timeout: int = 300  # 5 minutes
    load_test_duration: int = 30  # 30 seconds
    max_concurrent_operations: int = 10
    
    # Database test settings
    test_db_prefix: str = "test_"
    cleanup_after_tests: bool = True
    use_transaction_rollback: bool = True
    
    # API test settings
    mock_api_delay: float = 0.1  # 100ms mock delay
    simulate_rate_limits: bool = False
    simulate_api_failures: bool = False
    
    # Security test settings
    test_sensitive_data_masking: bool = True
    validate_sql_injection_prevention: bool = True
    test_credential_sanitization: bool = True
    
    # Coverage settings
    minimum_unit_test_coverage: float = 80.0
    minimum_integration_test_coverage: float = 70.0
    
    @classmethod
    def from_environment(cls) -> 'TestConfig':
        """Create test config from environment variables."""
        return cls(
            use_mock_database=os.getenv("TEST_USE_MOCK_DB", "true").lower() == "true",
            use_mock_apis=os.getenv("TEST_USE_MOCK_APIS", "true").lower() == "true",
            log_level=os.getenv("TEST_LOG_LEVEL", "INFO").upper(),
            enable_credential_sanitization=os.getenv("TEST_SANITIZE_CREDENTIALS", "true").lower() == "true",
            performance_test_timeout=int(os.getenv("TEST_PERFORMANCE_TIMEOUT", "300")),
            load_test_duration=int(os.getenv("TEST_LOAD_DURATION", "30")),
            max_concurrent_operations=int(os.getenv("TEST_MAX_CONCURRENT", "10")),
            simulate_rate_limits=os.getenv("TEST_SIMULATE_RATE_LIMITS", "false").lower() == "true",
            simulate_api_failures=os.getenv("TEST_SIMULATE_FAILURES", "false").lower() == "true",
            minimum_unit_test_coverage=float(os.getenv("TEST_MIN_UNIT_COVERAGE", "80.0")),
            minimum_integration_test_coverage=float(os.getenv("TEST_MIN_INTEGRATION_COVERAGE", "70.0"))
        )


class TestEnvironmentManager:
    """Manages test environment setup and teardown."""
    
    def __init__(self, config: Optional[TestConfig] = None):
        self.config = config or TestConfig.from_environment()
        self.original_env_vars: Dict[str, Optional[str]] = {}
        self.test_data_cleanup_callbacks: List[callable] = []
    
    def setup_test_environment(self):
        """Setup test environment with proper isolation."""
        # Store original environment variables
        test_env_vars = {
            "DATABASE_URL": self._get_test_database_url(),
            "LOG_LEVEL": self.config.log_level,
            "TESTING": "true"
        }
        
        for key, value in test_env_vars.items():
            self.original_env_vars[key] = os.getenv(key)
            os.environ[key] = value
    
    def teardown_test_environment(self):
        """Clean up test environment."""
        # Restore original environment variables
        for key, original_value in self.original_env_vars.items():
            if original_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original_value
        
        # Run cleanup callbacks
        for cleanup_callback in self.test_data_cleanup_callbacks:
            try:
                cleanup_callback()
            except Exception as e:
                print(f"Warning: Cleanup callback failed: {e}")
        
        self.test_data_cleanup_callbacks.clear()
    
    def register_cleanup_callback(self, callback: callable):
        """Register a cleanup callback to run during teardown."""
        self.test_data_cleanup_callbacks.append(callback)
    
    def _get_test_database_url(self) -> str:
        """Get test database URL."""
        if self.config.use_mock_database:
            return "mock://localhost/test_db"
        
        # Use test database (modify production URL to point to test DB)
        try:
            settings = get_settings()
            db_url = settings.database.url
            if db_url:
                # Replace database name with test version
                if "/" in db_url:
                    base_url, db_name = db_url.rsplit("/", 1)
                    return f"{base_url}/{self.config.test_db_prefix}{db_name}"
        except Exception:
            pass
        
        return "postgresql://localhost/test_mlb_betting"
    
    def get_test_data_directory(self) -> Path:
        """Get test data directory."""
        return Path(__file__).parent.parent / "fixtures"
    
    def get_temp_directory(self) -> Path:
        """Get temporary directory for test files."""
        temp_dir = Path("/tmp/mlb_betting_tests")
        temp_dir.mkdir(exist_ok=True)
        return temp_dir


@dataclass
class TestThresholds:
    """Performance and quality thresholds for tests."""
    
    # Performance thresholds
    max_response_time_seconds: float = 1.0
    min_throughput_per_second: float = 1.0
    max_memory_usage_mb: float = 500.0
    max_cpu_usage_percent: float = 50.0
    
    # Database thresholds
    max_query_time_seconds: float = 0.1
    max_connection_time_seconds: float = 0.05
    max_bulk_insert_time_seconds: float = 2.0
    
    # Quality thresholds
    max_error_rate_percent: float = 1.0
    min_success_rate_percent: float = 99.0
    max_retry_attempts: int = 3
    
    # Load test thresholds
    sustained_load_duration_seconds: int = 30
    concurrent_users: int = 10
    peak_load_multiplier: float = 2.0
    
    @classmethod
    def for_environment(cls, environment: str) -> 'TestThresholds':
        """Get thresholds appropriate for environment."""
        if environment.lower() == "ci":
            # More lenient thresholds for CI environment
            return cls(
                max_response_time_seconds=2.0,
                max_memory_usage_mb=1000.0,
                max_cpu_usage_percent=80.0,
                max_query_time_seconds=0.2
            )
        elif environment.lower() == "local":
            # Stricter thresholds for local development
            return cls(
                max_response_time_seconds=0.5,
                max_memory_usage_mb=300.0,
                max_cpu_usage_percent=30.0,
                max_query_time_seconds=0.05
            )
        else:
            # Default thresholds
            return cls()


class TestDataFactory:
    """Factory for creating test data."""
    
    @staticmethod
    def create_test_game_id() -> str:
        """Create a test game ID."""
        import uuid
        return f"test_game_{uuid.uuid4().hex[:8]}"
    
    @staticmethod
    def create_test_user_id() -> str:
        """Create a test user ID."""
        import uuid
        return f"test_user_{uuid.uuid4().hex[:8]}"
    
    @staticmethod
    def create_test_external_ids(count: int = 5) -> List[str]:
        """Create multiple test external IDs."""
        return [TestDataFactory.create_test_game_id() for _ in range(count)]
    
    @staticmethod
    def create_test_datetime_range(days_back: int = 7) -> Dict[str, str]:
        """Create a test date range."""
        from datetime import datetime, timedelta
        
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=days_back)
        
        return {
            "start": start_date.strftime("%Y-%m-%d"),
            "end": end_date.strftime("%Y-%m-%d")
        }
    
    @staticmethod
    def create_test_collection_request(source: str = "action_network") -> Dict[str, Any]:
        """Create a test collection request."""
        return {
            "source": source,
            "date_range": TestDataFactory.create_test_datetime_range(),
            "parameters": {
                "test_mode": True,
                "timeout": 30,
                "include_odds": True,
                "include_history": True
            }
        }


def get_test_config() -> TestConfig:
    """Get global test configuration."""
    return TestConfig.from_environment()


def get_test_thresholds(environment: Optional[str] = None) -> TestThresholds:
    """Get test thresholds for environment."""
    env = environment or os.getenv("TEST_ENVIRONMENT", "default")
    return TestThresholds.for_environment(env)


def is_ci_environment() -> bool:
    """Check if running in CI environment."""
    return os.getenv("CI", "false").lower() == "true" or os.getenv("GITHUB_ACTIONS", "false").lower() == "true"


def is_integration_test_enabled() -> bool:
    """Check if integration tests are enabled."""
    return os.getenv("ENABLE_INTEGRATION_TESTS", "true").lower() == "true"


def is_load_test_enabled() -> bool:
    """Check if load tests are enabled."""
    return os.getenv("ENABLE_LOAD_TESTS", "false").lower() == "true"


def skip_if_no_integration() -> bool:
    """Skip test if integration tests are disabled."""
    return not is_integration_test_enabled()


def skip_if_no_load_tests() -> bool:
    """Skip test if load tests are disabled."""
    return not is_load_test_enabled()


# Global test configuration instance
_test_config: Optional[TestConfig] = None
_test_environment_manager: Optional[TestEnvironmentManager] = None


def get_global_test_config() -> TestConfig:
    """Get global test configuration instance."""
    global _test_config
    if _test_config is None:
        _test_config = TestConfig.from_environment()
    return _test_config


def get_test_environment_manager() -> TestEnvironmentManager:
    """Get global test environment manager."""
    global _test_environment_manager
    if _test_environment_manager is None:
        _test_environment_manager = TestEnvironmentManager(get_global_test_config())
    return _test_environment_manager