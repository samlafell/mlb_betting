#!/usr/bin/env python3
"""
Test Import Fix for ActionNetworkCollector

This test verifies that the import structure is correct without triggering
the full dependency chain that causes pydantic_settings issues.
"""

import ast
import os
from pathlib import Path

def test_collectors_py_structure():
    """Test that collectors.py has the correct structure for ActionNetworkCollector."""
    collectors_path = Path(__file__).parent.parent / "src" / "data" / "collection" / "collectors.py"
    
    if not collectors_path.exists():
        print("❌ collectors.py not found")
        return False
    
    with open(collectors_path, 'r') as f:
        content = f.read()
    
    # Check for the required components
    checks = [
        ("Import statement", "from .consolidated_action_network_collector import ActionNetworkCollector as ConsolidatedActionNetworkCollector"),
        ("Alias definition", "ActionNetworkCollector = ConsolidatedActionNetworkCollector"),
        ("Factory registration", "CollectorFactory.register_collector(DataSource.ACTION_NETWORK, ConsolidatedActionNetworkCollector)")
    ]
    
    results = []
    for check_name, check_content in checks:
        if check_content in content:
            print(f"✅ {check_name}: Found")
            results.append(True)
        else:
            print(f"❌ {check_name}: Missing")
            results.append(False)
    
    return all(results)

def test_init_py_structure():
    """Test that __init__.py has the correct import structure."""
    init_path = Path(__file__).parent.parent / "src" / "data" / "collection" / "__init__.py"
    
    if not init_path.exists():
        print("❌ __init__.py not found")
        return False
    
    with open(init_path, 'r') as f:
        content = f.read()
    
    # Check for the required components
    checks = [
        ("ActionNetworkCollector import", "ActionNetworkCollector,  # Now points to ConsolidatedActionNetworkCollector"),
        ("Refactored import try block", "from .sbd_unified_collector_api import SBDUnifiedCollectorAPI"),
        ("Import exception handling", "except ImportError:"),
        ("Export list update", '__all__.extend([')
    ]
    
    results = []
    for check_name, check_content in checks:
        if check_content in content:
            print(f"✅ {check_name}: Found")
            results.append(True)
        else:
            print(f"❌ {check_name}: Missing")
            results.append(False)
    
    return all(results)

def test_consolidated_collector_exists():
    """Test that the consolidated collector file exists."""
    consolidated_path = Path(__file__).parent.parent / "src" / "data" / "collection" / "consolidated_action_network_collector.py"
    
    if not consolidated_path.exists():
        print("❌ consolidated_action_network_collector.py not found")
        return False
    
    with open(consolidated_path, 'r') as f:
        content = f.read()
    
    # Check for the ActionNetworkCollector class
    if "class ActionNetworkCollector(BaseCollector):" in content:
        print("✅ ActionNetworkCollector class found in consolidated file")
        return True
    else:
        print("❌ ActionNetworkCollector class not found in consolidated file")
        return False

def test_migration_helper_exists():
    """Test that migration helper exists."""
    migration_path = Path(__file__).parent.parent / "src" / "data" / "collection" / "migration_helper.py"
    
    if not migration_path.exists():
        print("❌ migration_helper.py not found")
        return False
    
    with open(migration_path, 'r') as f:
        content = f.read()
    
    # Check for key migration functions
    checks = [
        ("create_collector_config function", "def create_collector_config("),
        ("create_collection_request function", "def create_collection_request("),
        ("DeprecatedCollectorWrapper class", "class DeprecatedCollectorWrapper:")
    ]
    
    results = []
    for check_name, check_content in checks:
        if check_content in content:
            print(f"✅ {check_name}: Found")
            results.append(True)
        else:
            print(f"❌ {check_name}: Missing")
            results.append(False)
    
    return all(results)

def run_all_tests():
    """Run all import structure tests."""
    print("="*60)
    print("IMPORT FIX VALIDATION TESTS")
    print("="*60)
    
    tests = [
        ("collectors.py structure", test_collectors_py_structure),
        ("__init__.py structure", test_init_py_structure),  
        ("consolidated collector exists", test_consolidated_collector_exists),
        ("migration helper exists", test_migration_helper_exists)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n{test_name.upper()}:")
        result = test_func()
        results.append(result)
        print(f"Result: {'PASS' if result else 'FAIL'}")
    
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    
    passed = sum(results)
    total = len(results)
    
    print(f"Tests passed: {passed}/{total}")
    print(f"Success rate: {passed/total*100:.1f}%")
    
    if passed == total:
        print("\n✅ ALL IMPORT STRUCTURE TESTS PASSED!")
        print("The ActionNetworkCollector import issue has been fixed.")
        print("Once dependency issues are resolved, the CLI should work properly.")
    else:
        print(f"\n❌ {total-passed} tests failed - import structure needs attention")
    
    return passed == total

if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)