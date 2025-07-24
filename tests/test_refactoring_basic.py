#!/usr/bin/env python3
"""
Basic Refactoring Validation Test

This test validates the core refactoring changes without requiring
full dependency setup, focusing on the structural changes made.
"""

import inspect
import sys
from pathlib import Path

# Add the src directory to the path to allow imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def test_file_imports():
    """Test that refactored files can be imported."""
    results = {"success": [], "failed": [], "errors": {}}

    files_to_test = [
        "data.collection.base",
        "data.collection.sbd_unified_collector_api",
        "data.collection.vsin_unified_collector",
        "data.collection.consolidated_action_network_collector",
        "data.collection.migration_helper",
    ]

    for file_path in files_to_test:
        try:
            module = __import__(file_path, fromlist=[""])
            results["success"].append(file_path)
            print(f"✅ Successfully imported {file_path}")
        except Exception as e:
            results["failed"].append(file_path)
            results["errors"][file_path] = str(e)
            print(f"❌ Failed to import {file_path}: {str(e)}")

    return results


def test_base_collector_structure():
    """Test that BaseCollector has the expected structure."""
    try:
        from data.collection.base import (
            BaseCollector,
        )

        # Check BaseCollector methods
        base_methods = [m for m in dir(BaseCollector) if not m.startswith("_")]
        expected_methods = [
            "collect_data",
            "validate_record",
            "normalize_record",
            "test_connection",
        ]

        missing_methods = [m for m in expected_methods if m not in base_methods]

        if missing_methods:
            print(f"❌ BaseCollector missing methods: {missing_methods}")
            return False
        else:
            print("✅ BaseCollector has all expected methods")
            return True

    except Exception as e:
        print(f"❌ Error testing BaseCollector structure: {str(e)}")
        return False


def test_pydantic_models_structure():
    """Test that Pydantic models have expected structure."""
    try:
        from data.collection.base import CollectorConfig, DataSource

        # Test CollectorConfig structure
        config_fields = (
            CollectorConfig.__fields__ if hasattr(CollectorConfig, "__fields__") else {}
        )
        expected_config_fields = [
            "source",
            "enabled",
            "base_url",
            "api_key",
            "rate_limit_per_minute",
        ]

        # Test DataSource enum
        data_sources = [ds.value for ds in DataSource]
        expected_sources = ["vsin", "sbd", "action_network"]

        missing_sources = [s for s in expected_sources if s not in data_sources]

        if missing_sources:
            print(f"❌ DataSource missing values: {missing_sources}")
            return False
        else:
            print("✅ DataSource enum has expected values")
            return True

    except Exception as e:
        print(f"❌ Error testing Pydantic models: {str(e)}")
        return False


def test_collector_class_inheritance():
    """Test that collectors inherit from BaseCollector."""
    results = {}

    # Test SBD Collector
    try:
        from data.collection.base import BaseCollector
        from data.collection.sbd_unified_collector_api import SBDUnifiedCollectorAPI

        is_subclass = issubclass(SBDUnifiedCollectorAPI, BaseCollector)
        results["SBD"] = is_subclass
        print(
            f"{'✅' if is_subclass else '❌'} SBD collector inherits from BaseCollector: {is_subclass}"
        )

    except Exception as e:
        results["SBD"] = False
        print(f"❌ Error testing SBD collector inheritance: {str(e)}")

    # Test VSIN Collector
    try:
        from data.collection.base import BaseCollector
        from data.collection.vsin_unified_collector import VSINUnifiedCollector

        is_subclass = issubclass(VSINUnifiedCollector, BaseCollector)
        results["VSIN"] = is_subclass
        print(
            f"{'✅' if is_subclass else '❌'} VSIN collector inherits from BaseCollector: {is_subclass}"
        )

    except Exception as e:
        results["VSIN"] = False
        print(f"❌ Error testing VSIN collector inheritance: {str(e)}")

    # Test Action Network Collector
    try:
        from data.collection.base import BaseCollector
        from data.collection.consolidated_action_network_collector import (
            ActionNetworkCollector,
        )

        is_subclass = issubclass(ActionNetworkCollector, BaseCollector)
        results["ActionNetwork"] = is_subclass
        print(
            f"{'✅' if is_subclass else '❌'} Action Network collector inherits from BaseCollector: {is_subclass}"
        )

    except Exception as e:
        results["ActionNetwork"] = False
        print(f"❌ Error testing Action Network collector inheritance: {str(e)}")

    return results


def test_collector_method_signatures():
    """Test that collectors have the expected method signatures."""
    results = {}

    collectors_to_test = [
        ("SBD", "data.collection.sbd_unified_collector_api", "SBDUnifiedCollectorAPI"),
        ("VSIN", "data.collection.vsin_unified_collector", "VSINUnifiedCollector"),
        (
            "ActionNetwork",
            "data.collection.consolidated_action_network_collector",
            "ActionNetworkCollector",
        ),
    ]

    for name, module_path, class_name in collectors_to_test:
        try:
            module = __import__(module_path, fromlist=[class_name])
            collector_class = getattr(module, class_name)

            # Check for required methods
            required_methods = ["collect_data", "validate_record", "normalize_record"]
            methods_present = []

            for method_name in required_methods:
                if hasattr(collector_class, method_name):
                    method = getattr(collector_class, method_name)
                    methods_present.append(method_name)

                    # Check if collect_data is async
                    if method_name == "collect_data":
                        sig = inspect.signature(method)
                        is_async = inspect.iscoroutinefunction(method)
                        print(f"  - collect_data is async: {is_async}")

            missing_methods = [m for m in required_methods if m not in methods_present]

            results[name] = {
                "methods_present": methods_present,
                "missing_methods": missing_methods,
                "all_present": len(missing_methods) == 0,
            }

            status = "✅" if len(missing_methods) == 0 else "❌"
            print(
                f"{status} {name} collector method check: {len(methods_present)}/{len(required_methods)} methods present"
            )

            if missing_methods:
                print(f"  Missing: {missing_methods}")

        except Exception as e:
            results[name] = {"error": str(e), "all_present": False}
            print(f"❌ Error testing {name} collector methods: {str(e)}")

    return results


def test_migration_helper_functions():
    """Test that migration helper functions work."""
    try:
        from data.collection.base import DataSource
        from data.collection.migration_helper import (
            create_collection_request,
            create_collector_config,
        )

        # Test create_collector_config
        config = create_collector_config(DataSource.SBD)
        config_valid = hasattr(config, "source") and config.source == DataSource.SBD
        print(
            f"{'✅' if config_valid else '❌'} create_collector_config works: {config_valid}"
        )

        # Test create_collection_request
        request = create_collection_request(DataSource.VSIN, sport="mlb")
        request_valid = hasattr(request, "source") and request.source == DataSource.VSIN
        print(
            f"{'✅' if request_valid else '❌'} create_collection_request works: {request_valid}"
        )

        return config_valid and request_valid

    except Exception as e:
        print(f"❌ Error testing migration helper: {str(e)}")
        return False


def test_factory_registration():
    """Test that factory can create collectors."""
    try:
        from data.collection.base import CollectorConfig, CollectorFactory, DataSource

        # Try to create collectors through factory
        sources_to_test = [DataSource.SBD, DataSource.VSIN, DataSource.ACTION_NETWORK]
        results = {}

        for source in sources_to_test:
            try:
                config = CollectorConfig(source=source)
                collector = CollectorFactory.create_collector(config)

                # Check if it's a BaseCollector instance
                from data.collection.base import BaseCollector

                is_base_collector = isinstance(collector, BaseCollector)

                results[source.value] = is_base_collector
                print(
                    f"{'✅' if is_base_collector else '❌'} Factory creates {source.value} collector: {is_base_collector}"
                )

            except Exception as e:
                results[source.value] = False
                print(f"❌ Factory failed to create {source.value} collector: {str(e)}")

        return results

    except Exception as e:
        print(f"❌ Error testing factory registration: {str(e)}")
        return {}


def run_all_tests():
    """Run all basic validation tests."""
    print("=" * 60)
    print("REFACTORING BASIC VALIDATION TESTS")
    print("=" * 60)

    all_results = {}

    print("\n1. Testing File Imports...")
    all_results["imports"] = test_file_imports()

    print("\n2. Testing BaseCollector Structure...")
    all_results["base_structure"] = test_base_collector_structure()

    print("\n3. Testing Pydantic Models...")
    all_results["pydantic_models"] = test_pydantic_models_structure()

    print("\n4. Testing Collector Inheritance...")
    all_results["inheritance"] = test_collector_class_inheritance()

    print("\n5. Testing Method Signatures...")
    all_results["methods"] = test_collector_method_signatures()

    print("\n6. Testing Migration Helper...")
    all_results["migration"] = test_migration_helper_functions()

    print("\n7. Testing Factory Registration...")
    all_results["factory"] = test_factory_registration()

    # Generate summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    successes = 0
    total = 0

    for test_name, result in all_results.items():
        if isinstance(result, bool):
            total += 1
            if result:
                successes += 1
                print(f"✅ {test_name}: PASS")
            else:
                print(f"❌ {test_name}: FAIL")
        elif isinstance(result, dict):
            if "success" in result and "failed" in result:
                # Import results
                success_count = len(result["success"])
                total_count = len(result["success"]) + len(result["failed"])
                total += 1
                if success_count == total_count:
                    successes += 1
                    print(f"✅ {test_name}: PASS ({success_count}/{total_count})")
                else:
                    print(f"❌ {test_name}: PARTIAL ({success_count}/{total_count})")
            else:
                # Other dict results
                sub_successes = sum(1 for v in result.values() if v is True)
                sub_total = len(result)
                total += 1
                if sub_successes == sub_total:
                    successes += 1
                    print(f"✅ {test_name}: PASS ({sub_successes}/{sub_total})")
                else:
                    print(f"❌ {test_name}: PARTIAL ({sub_successes}/{sub_total})")

    print(f"\nOVERALL: {successes}/{total} test categories passed")
    print(f"Success Rate: {(successes / total * 100):.1f}%")

    if successes == total:
        print("\n🎉 ALL BASIC REFACTORING TESTS PASSED!")
        return True
    else:
        print(f"\n⚠️  {total - successes} test categories need attention")
        return False


if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)
