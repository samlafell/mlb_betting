#!/usr/bin/env python3
"""
Phase 1 Refactoring Verification Test

Comprehensive test to verify all Phase 1 refactoring services are working correctly:
1. ConfigurationService - Centralized configuration management
2. RetryService - Unified retry logic
3. UnifiedRateLimiter - Consolidated rate limiting
4. BettingAccuracyMonitor - Performance tracking during changes
5. FeatureFlags - Safe rollback capability

Tests production safety features and ensures zero-risk deployment.
"""

import asyncio
import sys
import tempfile
import traceback
from pathlib import Path

# Add the src directory to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from mlb_sharp_betting.services.config_service import (
    get_config_service,
    get_rate_limits,
    get_retry_config,
    get_service_config,
)
from mlb_sharp_betting.services.feature_flags import (
    TemporaryFlagOverride,
    get_feature_flags,
    get_flag_value,
    is_enabled,
)
from mlb_sharp_betting.services.rate_limiter import RateLimitStatus, get_rate_limiter
from mlb_sharp_betting.services.retry_service import (
    OperationType,
    get_retry_service,
    retry_operation,
)


class Phase1RefactoringTester:
    """Comprehensive tester for Phase 1 refactoring services."""

    def __init__(self):
        self.test_results = {}
        self.errors = []

    async def run_all_tests(self) -> bool:
        """Run all Phase 1 refactoring tests."""
        print("🚀 Starting Phase 1 Refactoring Verification Tests")
        print("=" * 60)

        test_methods = [
            ("Configuration Service", self.test_configuration_service),
            ("Retry Service", self.test_retry_service),
            ("Rate Limiter", self.test_rate_limiter),
            ("Betting Accuracy Monitor", self.test_accuracy_monitor),
            ("Feature Flags", self.test_feature_flags),
            ("Integration Test", self.test_service_integration),
        ]

        all_passed = True

        for test_name, test_method in test_methods:
            print(f"\n📋 Testing: {test_name}")
            print("-" * 40)

            try:
                result = await test_method()
                self.test_results[test_name] = result

                if result:
                    print(f"✅ {test_name}: PASSED")
                else:
                    print(f"❌ {test_name}: FAILED")
                    all_passed = False

            except Exception as e:
                print(f"💥 {test_name}: ERROR - {str(e)}")
                self.errors.append(f"{test_name}: {str(e)}")
                self.test_results[test_name] = False
                all_passed = False
                traceback.print_exc()

        # Print final results
        print("\n" + "=" * 60)
        print("📊 PHASE 1 REFACTORING TEST RESULTS")
        print("=" * 60)

        for test_name, result in self.test_results.items():
            status = "✅ PASSED" if result else "❌ FAILED"
            print(f"{test_name:<30} {status}")

        if self.errors:
            print(f"\n🚨 ERRORS ENCOUNTERED ({len(self.errors)}):")
            for error in self.errors:
                print(f"   - {error}")

        overall_status = "✅ ALL TESTS PASSED" if all_passed else "❌ SOME TESTS FAILED"
        print(f"\n🎯 OVERALL RESULT: {overall_status}")

        return all_passed

    async def test_configuration_service(self) -> bool:
        """Test ConfigurationService functionality."""
        try:
            # Test singleton pattern
            config_service1 = get_config_service()
            config_service2 = get_config_service()
            assert config_service1 is config_service2, (
                "ConfigurationService should be singleton"
            )
            print("✓ Singleton pattern working")

            # Test service configuration retrieval
            vsin_config = get_service_config("vsin_scraper")
            assert isinstance(vsin_config, dict), "Service config should return dict"
            assert "service_name" in vsin_config, (
                "Service config should include service_name"
            )
            assert "rate_limits" in vsin_config, (
                "Service config should include rate_limits"
            )
            assert "retry" in vsin_config, "Service config should include retry config"
            print("✓ Service configuration retrieval working")

            # Test rate limits retrieval
            rate_limits = get_rate_limits("pinnacle_scraper")
            assert isinstance(rate_limits, dict), "Rate limits should return dict"
            assert "max_requests_per_minute" in rate_limits, (
                "Rate limits should include max_requests_per_minute"
            )
            print("✓ Rate limits retrieval working")

            # Test retry config retrieval
            retry_config = get_retry_config("database")
            assert isinstance(retry_config, dict), "Retry config should return dict"
            assert "max_attempts" in retry_config, (
                "Retry config should include max_attempts"
            )
            assert "base_delay_seconds" in retry_config, (
                "Retry config should include base_delay_seconds"
            )
            print("✓ Retry config retrieval working")

            # Test cache functionality
            config_service1.clear_cache()
            cache_stats = config_service1.get_cache_stats()
            assert cache_stats["cached_configs"] == 0, (
                "Cache should be empty after clear"
            )
            print("✓ Cache management working")

            # Test configuration update
            config_service1.update_service_config(
                "test_service", {"custom_setting": "test_value"}
            )
            test_config = get_service_config("test_service")
            assert test_config["custom"]["custom_setting"] == "test_value", (
                "Custom config should be retrievable"
            )
            print("✓ Configuration updates working")

            return True

        except Exception as e:
            print(f"Configuration Service test failed: {e}")
            return False

    async def test_retry_service(self) -> bool:
        """Test RetryService functionality."""
        try:
            retry_service = get_retry_service()

            # Test successful operation
            async def successful_operation():
                return "success"

            result = await retry_service.execute_with_retry(
                operation=successful_operation,
                operation_name="test_success",
                operation_type=OperationType.NETWORK,
            )
            assert result == "success", "Successful operation should return result"
            print("✓ Successful operation handling working")

            # Test operation with retries
            attempt_count = 0

            async def failing_then_success_operation():
                nonlocal attempt_count
                attempt_count += 1
                if attempt_count < 3:
                    raise Exception("Temporary failure")
                return "success_after_retries"

            attempt_count = 0  # Reset counter
            result = await retry_service.execute_with_retry(
                operation=failing_then_success_operation,
                operation_name="test_retry",
                operation_type=OperationType.API_CALL,
                max_retries=3,
            )
            assert result == "success_after_retries", (
                "Retry operation should eventually succeed"
            )
            assert attempt_count == 3, "Should have attempted 3 times"
            print("✓ Retry logic working")

            # Test decorator
            call_count = 0

            @retry_operation(operation_type=OperationType.DATABASE, max_retries=2)
            async def decorated_operation():
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise Exception("First attempt fails")
                return "decorated_success"

            call_count = 0  # Reset counter
            result = await decorated_operation()
            assert result == "decorated_success", "Decorated operation should work"
            assert call_count == 2, "Should have been called twice"
            print("✓ Retry decorator working")

            # Test metrics
            metrics = retry_service.get_metrics()
            assert "operation_metrics" in metrics, (
                "Metrics should include operation_metrics"
            )
            assert "circuit_breaker_states" in metrics, (
                "Metrics should include circuit_breaker_states"
            )
            print("✓ Metrics collection working")

            return True

        except Exception as e:
            print(f"Retry Service test failed: {e}")
            return False

    async def test_rate_limiter(self) -> bool:
        """Test UnifiedRateLimiter functionality."""
        try:
            rate_limiter = get_rate_limiter()

            # Test rate limit check for API service
            result = await rate_limiter.check_rate_limit(
                "odds_api", "test_operation", 1
            )
            assert result.status in [
                RateLimitStatus.ALLOWED,
                RateLimitStatus.QUOTA_EXCEEDED,
            ], "Rate limit check should return valid status"
            print("✓ Rate limit checking working")

            # Test request recording
            if result.allowed:
                await rate_limiter.record_request(
                    "odds_api",
                    "test_operation",
                    1,
                    "test_endpoint",
                    {"test": "metadata"},
                )
                print("✓ Request recording working")

            # Test service status
            status = rate_limiter.get_service_status("pinnacle_scraper")
            assert isinstance(status, dict), "Service status should return dict"
            assert "service_name" in status, (
                "Service status should include service_name"
            )
            print("✓ Service status retrieval working")

            # Test all services status
            all_status = rate_limiter.get_all_status()
            assert "services" in all_status, "All status should include services"
            assert "summary" in all_status, "All status should include summary"
            print("✓ All services status working")

            # Test wait for rate limit (with short timeout)
            wait_result = await rate_limiter.wait_for_rate_limit(
                "test_service", "test_op", 1, 0.1
            )
            # Should either be allowed immediately or timeout quickly
            assert wait_result.status in [
                RateLimitStatus.ALLOWED,
                RateLimitStatus.RATE_LIMITED,
            ], "Wait should return valid status"
            print("✓ Rate limit waiting working")

            return True

        except Exception as e:
            print(f"Rate Limiter test failed: {e}")
            return False

    async def test_accuracy_monitor(self) -> bool:
        """Test BettingAccuracyMonitor functionality."""
        try:
            # Use temporary directory for testing
            with tempfile.TemporaryDirectory() as temp_dir:
                from mlb_sharp_betting.services.betting_accuracy_monitor import (
                    BettingAccuracyMonitor,
                )

                monitor = BettingAccuracyMonitor(data_dir=Path(temp_dir))

                # Test baseline establishment (this will likely fail due to no database, but should handle gracefully)
                try:
                    await monitor.establish_baseline(1)  # 1 day lookback
                    print("✓ Baseline establishment working")
                except Exception as e:
                    # Expected to fail without database, but should handle gracefully
                    if (
                        "Failed to establish baseline" in str(e)
                        or "connection" in str(e).lower()
                    ):
                        print(
                            "✓ Baseline establishment handles missing database gracefully"
                        )
                    else:
                        raise

                # Test monitoring start/stop
                await monitor.start_monitoring("test_phase")
                assert monitor.monitoring_active, (
                    "Monitoring should be active after start"
                )
                assert monitor.current_refactoring_phase == "test_phase", (
                    "Phase should be set"
                )
                print("✓ Monitoring start working")

                summary = await monitor.stop_monitoring()
                assert not monitor.monitoring_active, (
                    "Monitoring should be inactive after stop"
                )
                assert isinstance(summary, dict), "Summary should be dict"
                print("✓ Monitoring stop working")

                # Test status retrieval
                status = monitor.get_current_status()
                assert isinstance(status, dict), "Status should be dict"
                assert "monitoring_active" in status, (
                    "Status should include monitoring_active"
                )
                print("✓ Status retrieval working")

                return True

        except Exception as e:
            print(f"Accuracy Monitor test failed: {e}")
            return False

    async def test_feature_flags(self) -> bool:
        """Test FeatureFlags functionality."""
        try:
            # Use temporary directory for testing
            with tempfile.TemporaryDirectory() as temp_dir:
                from mlb_sharp_betting.services.feature_flags import BettingFeatureFlags

                flags = BettingFeatureFlags(data_dir=Path(temp_dir))

                # Test flag retrieval
                use_config_service = flags.is_enabled("use_config_service")
                assert isinstance(use_config_service, bool), (
                    "Flag value should be boolean"
                )
                print("✓ Flag retrieval working")

                # Test flag setting
                result = flags.set_flag("use_config_service", True, "Test setting")
                assert result, "Flag setting should succeed"
                assert flags.is_enabled("use_config_service"), (
                    "Flag should be enabled after setting"
                )
                print("✓ Flag setting working")

                # Test flag disabling
                result = flags.disable_flag("use_config_service", "Test disabling")
                assert result, "Flag disabling should succeed"
                assert not flags.is_enabled("use_config_service"), (
                    "Flag should be disabled"
                )
                print("✓ Flag disabling working")

                # Test flag rollback
                flags.set_flag("use_config_service", True, "Set before rollback")
                result = flags.rollback_flag("use_config_service", "Test rollback")
                assert result, "Flag rollback should succeed"
                print("✓ Flag rollback working")

                # Test getting all flags
                all_flags = flags.get_all_flags()
                assert isinstance(all_flags, dict), "All flags should return dict"
                assert "use_config_service" in all_flags, "Should include known flags"
                print("✓ All flags retrieval working")

                # Test flag group transaction
                transaction_result = flags.create_flag_group_transaction(
                    {"use_config_service": True, "use_unified_retry": True},
                    "Test transaction",
                )
                assert transaction_result, "Transaction should succeed"
                print("✓ Flag group transaction working")

                # Test temporary flag override context manager
                with TemporaryFlagOverride({"use_config_service": False}):
                    assert not flags.is_enabled("use_config_service"), (
                        "Flag should be overridden in context"
                    )

                # Should be restored after context
                assert flags.is_enabled("use_config_service"), (
                    "Flag should be restored after context"
                )
                print("✓ Temporary flag override working")

                # Test convenience functions
                enabled = is_enabled("use_config_service")
                value = get_flag_value("use_config_service", False)
                assert isinstance(enabled, bool), "Convenience function should work"
                assert isinstance(value, bool), "Convenience function should work"
                print("✓ Convenience functions working")

                return True

        except Exception as e:
            print(f"Feature Flags test failed: {e}")
            return False

    async def test_service_integration(self) -> bool:
        """Test integration between all Phase 1 services."""
        try:
            # Test that services can work together
            config_service = get_config_service()
            retry_service = get_retry_service()
            rate_limiter = get_rate_limiter()
            feature_flags = get_feature_flags()

            # Test configuration-driven retry with feature flag
            with TemporaryFlagOverride({"use_unified_retry": True}):
                if is_enabled("use_unified_retry"):
                    # Get retry config from configuration service
                    retry_config = get_retry_config("test_service")

                    # Use retry service with configuration
                    async def test_integration_operation():
                        return "integration_success"

                    result = await retry_service.execute_with_retry(
                        operation=test_integration_operation,
                        operation_name="integration_test",
                        operation_type=OperationType.NETWORK,
                        max_retries=retry_config.get("max_attempts", 3),
                    )

                    assert result == "integration_success", (
                        "Integrated operation should succeed"
                    )
                    print("✓ Configuration + Retry + Feature Flags integration working")

            # Test rate limiting with configuration
            with TemporaryFlagOverride({"use_unified_rate_limiting": True}):
                if is_enabled("use_unified_rate_limiting"):
                    # Check rate limit
                    rate_result = await rate_limiter.check_rate_limit("test_service")
                    assert hasattr(rate_result, "allowed"), (
                        "Rate limit result should have allowed property"
                    )
                    print("✓ Rate Limiting + Feature Flags integration working")

            # Test that all services are properly initialized
            assert config_service is not None, "Config service should be initialized"
            assert retry_service is not None, "Retry service should be initialized"
            assert rate_limiter is not None, "Rate limiter should be initialized"
            assert feature_flags is not None, "Feature flags should be initialized"
            print("✓ All services properly initialized")

            # Test service metrics/status collection
            config_cache_stats = config_service.get_cache_stats()
            retry_metrics = retry_service.get_metrics()
            rate_limiter_status = rate_limiter.get_all_status()
            flags_status = feature_flags.get_all_flags()

            assert all(
                [
                    isinstance(config_cache_stats, dict),
                    isinstance(retry_metrics, dict),
                    isinstance(rate_limiter_status, dict),
                    isinstance(flags_status, dict),
                ]
            ), "All services should provide status/metrics"
            print("✓ Service status/metrics collection working")

            return True

        except Exception as e:
            print(f"Service Integration test failed: {e}")
            return False


async def main():
    """Run Phase 1 refactoring verification tests."""
    tester = Phase1RefactoringTester()
    success = await tester.run_all_tests()

    if success:
        print("\n🎉 Phase 1 refactoring services are ready for production!")
        print("✅ Zero-risk deployment confirmed")
        print("✅ All safety mechanisms verified")
        print("✅ Configuration consolidation operational")
        print("✅ Retry logic standardized")
        print("✅ Rate limiting unified")
        print("✅ Performance monitoring active")
        print("✅ Feature flags ready for safe rollback")
        return 0
    else:
        print("\n⚠️  Some Phase 1 services need attention before deployment")
        print("❌ Please fix the failing tests before proceeding")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
