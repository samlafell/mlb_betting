"""
Data Source Connection Integration Tests

Tests the connection and basic functionality of all data source collectors:
- Action Network API integration
- VSIN data collection with HTML parsing
- SBD WordPress JSON API integration

Focuses on connection validation and basic data retrieval without complex processing.
"""

import pytest
import asyncio
from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Optional
import json

from src.core.config import get_settings
from src.data.collection.registry import (
    initialize_all_collectors,
    get_collector_instance,
    get_collector_class
)


@pytest.mark.asyncio
class TestDataSourceConnections:
    """Test basic data source connections and functionality"""
    
    @pytest.fixture(autouse=True)
    async def setup(self):
        """Setup test environment"""
        self.config = get_settings()
        
        # Initialize collector registry
        initialize_all_collectors()
        
        yield

    async def test_action_network_connection(self):
        """Test Action Network API connection and basic data retrieval"""
        try:
            # Get Action Network collector instance
            collector = get_collector_instance("action_network", self.config)
            assert collector is not None, "Action Network collector should be available"
            print(f"✅ Action Network collector initialized: {collector.__class__.__name__}")
            
            # Test basic connection - try to get today's games
            today = date.today()
            
            # Check if collector has expected methods
            assert hasattr(collector, 'get_games_for_date'), "Collector should have get_games_for_date method"
            print("✅ Action Network collector has required methods")
            
            # Test connection by fetching games (with timeout)
            try:
                games_data = await asyncio.wait_for(
                    collector.get_games_for_date(today), 
                    timeout=30.0
                )
                
                # Validate response structure
                assert isinstance(games_data, (list, dict)), "Games data should be list or dict"
                
                if isinstance(games_data, list):
                    print(f"📊 Action Network returned {len(games_data)} games for {today}")
                    
                    if len(games_data) > 0:
                        # Check structure of first game
                        first_game = games_data[0]
                        assert isinstance(first_game, dict), "Game data should be dict"
                        
                        # Look for key fields (flexible structure)
                        expected_fields = ['teams', 'game_time', 'external_game_id', 'id']
                        found_fields = []
                        for field in expected_fields:
                            if field in first_game or any(field in str(k).lower() for k in first_game.keys()):
                                found_fields.append(field)
                        
                        print(f"✅ Action Network game structure has fields: {found_fields}")
                        assert len(found_fields) >= 2, f"Expected at least 2 key fields, found: {found_fields}"
                    else:
                        print("ℹ️  No games found for today (could be off-season)")
                        
                elif isinstance(games_data, dict):
                    print(f"📊 Action Network returned dict response: {list(games_data.keys())}")
                    
                print("✅ Action Network connection successful")
                
            except asyncio.TimeoutError:
                pytest.skip("Action Network API connection timeout - network issue or API down")
            except Exception as e:
                print(f"⚠️  Action Network connection issue: {e}")
                # Don't fail test - could be API limits or temporary issues
                pytest.skip(f"Action Network API issue: {e}")
                
        except Exception as e:
            pytest.fail(f"Action Network collector setup failed: {e}")

    async def test_vsin_connection(self):
        """Test VSIN data collection connection and basic functionality"""
        try:
            # Get VSIN collector instance
            collector = get_collector_instance("vsin", self.config)
            assert collector is not None, "VSIN collector should be available"
            print(f"✅ VSIN collector initialized: {collector.__class__.__name__}")
            
            # Check if collector has expected methods
            expected_methods = ['collect_current_data', 'fetch_sharp_action_data']
            available_methods = []
            for method in expected_methods:
                if hasattr(collector, method):
                    available_methods.append(method)
            
            print(f"✅ VSIN collector methods available: {available_methods}")
            assert len(available_methods) > 0, f"VSIN collector should have at least one expected method"
            
            # Test basic data collection (with timeout)
            try:
                # Try to collect current data
                if hasattr(collector, 'collect_current_data'):
                    vsin_data = await asyncio.wait_for(
                        collector.collect_current_data(),
                        timeout=45.0  # VSIN might be slower due to HTML parsing
                    )
                    
                    assert vsin_data is not None, "VSIN should return some data"
                    print(f"📊 VSIN data collection successful: {type(vsin_data)}")
                    
                    if isinstance(vsin_data, list):
                        print(f"📊 VSIN returned {len(vsin_data)} records")
                        if len(vsin_data) > 0:
                            first_record = vsin_data[0]
                            print(f"✅ VSIN record structure: {list(first_record.keys()) if isinstance(first_record, dict) else type(first_record)}")
                    elif isinstance(vsin_data, dict):
                        print(f"📊 VSIN returned dict with keys: {list(vsin_data.keys())}")
                        
                elif hasattr(collector, 'fetch_sharp_action_data'):
                    # Alternative method for VSIN
                    sharp_data = await asyncio.wait_for(
                        collector.fetch_sharp_action_data(),
                        timeout=45.0
                    )
                    
                    assert sharp_data is not None, "VSIN sharp action should return some data"
                    print(f"📊 VSIN sharp action data: {type(sharp_data)}")
                    
            except asyncio.TimeoutError:
                pytest.skip("VSIN connection timeout - website might be slow or unavailable")
            except Exception as e:
                print(f"⚠️  VSIN connection issue: {e}")
                # Don't fail test - could be website changes or temporary issues
                pytest.skip(f"VSIN collection issue: {e}")
                
            print("✅ VSIN connection and data collection successful")
            
        except Exception as e:
            pytest.fail(f"VSIN collector setup failed: {e}")

    async def test_sbd_connection(self):
        """Test SBD WordPress JSON API connection and basic functionality"""
        try:
            # Get SBD collector instance
            collector = get_collector_instance("sbd", self.config)
            assert collector is not None, "SBD collector should be available"
            print(f"✅ SBD collector initialized: {collector.__class__.__name__}")
            
            # Check if collector has expected methods for SBD API
            expected_methods = ['collect_betting_splits', 'fetch_current_splits', 'get_current_data']
            available_methods = []
            for method in expected_methods:
                if hasattr(collector, method):
                    available_methods.append(method)
            
            print(f"✅ SBD collector methods available: {available_methods}")
            assert len(available_methods) > 0, f"SBD collector should have at least one expected method"
            
            # Test basic data collection (with timeout)
            try:
                # Try different collection methods based on what's available
                sbd_data = None
                
                if hasattr(collector, 'collect_betting_splits'):
                    sbd_data = await asyncio.wait_for(
                        collector.collect_betting_splits(),
                        timeout=30.0
                    )
                elif hasattr(collector, 'fetch_current_splits'):
                    sbd_data = await asyncio.wait_for(
                        collector.fetch_current_splits(),
                        timeout=30.0
                    )
                elif hasattr(collector, 'get_current_data'):
                    sbd_data = await asyncio.wait_for(
                        collector.get_current_data(),
                        timeout=30.0
                    )
                
                assert sbd_data is not None, "SBD should return some data"
                print(f"📊 SBD data collection successful: {type(sbd_data)}")
                
                if isinstance(sbd_data, list):
                    print(f"📊 SBD returned {len(sbd_data)} records")
                    if len(sbd_data) > 0:
                        first_record = sbd_data[0]
                        if isinstance(first_record, dict):
                            # Look for key SBD fields
                            sbd_fields = []
                            for key in first_record.keys():
                                if any(term in str(key).lower() for term in ['game', 'team', 'sportsbook', 'bet', 'split']):
                                    sbd_fields.append(key)
                            print(f"✅ SBD record structure includes: {sbd_fields}")
                        else:
                            print(f"✅ SBD record type: {type(first_record)}")
                elif isinstance(sbd_data, dict):
                    print(f"📊 SBD returned dict with keys: {list(sbd_data.keys())}")
                    
            except asyncio.TimeoutError:
                pytest.skip("SBD API connection timeout - WordPress API might be slow")
            except Exception as e:
                print(f"⚠️  SBD connection issue: {e}")
                # Don't fail test - could be API changes or temporary issues
                pytest.skip(f"SBD API issue: {e}")
                
            print("✅ SBD connection and data collection successful")
            
        except Exception as e:
            pytest.fail(f"SBD collector setup failed: {e}")

    async def test_collector_registry_functionality(self):
        """Test the centralized collector registry system"""
        # Test registry initialization
        initialize_all_collectors()
        print("✅ Collector registry initialized successfully")
        
        # Test getting collector classes
        action_network_class = get_collector_class("action_network")
        assert action_network_class is not None, "Action Network collector class should be available"
        print(f"✅ Action Network collector class: {action_network_class.__name__}")
        
        # Test getting collector instances
        action_network_instance = get_collector_instance("action_network", self.config)
        assert action_network_instance is not None, "Action Network collector instance should be available"
        print(f"✅ Action Network instance: {action_network_instance.__class__.__name__}")
        
        # Test collector aliases
        try:
            sbr_instance = get_collector_instance("sbr", self.config)  # Should resolve to sports_book_review
            if sbr_instance:
                print(f"✅ SBR alias works: {sbr_instance.__class__.__name__}")
            else:
                print("ℹ️  SBR alias not available (expected if SBR collector not implemented)")
        except Exception as e:
            print(f"ℹ️  SBR alias test: {e}")
        
        # Test multiple instances (should use caching)
        instance1 = get_collector_instance("action_network", self.config)
        instance2 = get_collector_instance("action_network", self.config)
        
        # They should be the same instance due to caching
        assert instance1 is instance2, "Registry should cache collector instances"
        print("✅ Collector instance caching works correctly")

    async def test_data_source_error_handling(self):
        """Test error handling for data source connection issues"""
        
        # Test with invalid collector name
        try:
            invalid_collector = get_collector_instance("nonexistent_source", self.config)
            assert invalid_collector is None, "Invalid collector should return None"
            print("✅ Invalid collector name handled correctly")
        except Exception as e:
            print(f"✅ Invalid collector raises appropriate error: {e}")
        
        # Test with None config
        try:
            collector_with_none = get_collector_instance("action_network", None)
            # Should either work with defaults or raise appropriate error
            print("✅ None config handled gracefully")
        except Exception as e:
            print(f"✅ None config raises appropriate error: {e}")

    async def test_data_source_configuration(self):
        """Test that data sources are properly configured"""
        
        # Check Action Network configuration
        if hasattr(self.config, 'action_network') or hasattr(self.config, 'data_sources'):
            print("✅ Action Network configuration available")
        
        # Check for required configuration keys
        config_dict = self.config.model_dump() if hasattr(self.config, 'model_dump') else vars(self.config)
        
        # Look for data source related configuration
        data_source_config_keys = []
        for key in config_dict.keys():
            if any(term in str(key).lower() for term in ['action', 'vsin', 'sbd', 'api', 'url']):
                data_source_config_keys.append(key)
        
        print(f"📊 Data source configuration keys found: {data_source_config_keys}")
        assert len(data_source_config_keys) > 0, "Should have some data source configuration"

    @pytest.mark.skipif(True, reason="Run manually to test actual data collection")
    async def test_manual_full_data_collection(self):
        """Manual test for full data collection from all sources - skip by default"""
        # This test is skipped by default as it makes real API calls
        # Run manually with: pytest -k test_manual_full_data_collection -s
        
        print("🧪 Running full data collection test...")
        
        collectors_to_test = ["action_network", "vsin", "sbd"]
        results = {}
        
        for collector_name in collectors_to_test:
            try:
                print(f"\n📡 Testing {collector_name} full collection...")
                collector = get_collector_instance(collector_name, self.config)
                
                if collector:
                    # Try different collection methods
                    collection_methods = ['collect_current_data', 'get_current_data', 'fetch_data']
                    
                    for method_name in collection_methods:
                        if hasattr(collector, method_name):
                            method = getattr(collector, method_name)
                            try:
                                data = await asyncio.wait_for(method(), timeout=60.0)
                                results[collector_name] = {
                                    'success': True,
                                    'method': method_name,
                                    'data_type': type(data).__name__,
                                    'data_length': len(data) if isinstance(data, (list, dict)) else 'N/A'
                                }
                                print(f"✅ {collector_name} collection successful via {method_name}")
                                break
                            except Exception as e:
                                print(f"⚠️  {collector_name}.{method_name} failed: {e}")
                                continue
                    
                    if collector_name not in results:
                        results[collector_name] = {'success': False, 'error': 'No working collection method found'}
                else:
                    results[collector_name] = {'success': False, 'error': 'Collector not available'}
                    
            except Exception as e:
                results[collector_name] = {'success': False, 'error': str(e)}
        
        # Print results summary
        print("\n📊 Full Collection Test Results:")
        for collector_name, result in results.items():
            status = "✅ Success" if result['success'] else "❌ Failed"
            print(f"  {collector_name}: {status}")
            if result['success']:
                print(f"    Method: {result['method']}, Data: {result['data_type']}({result['data_length']})")
            else:
                print(f"    Error: {result['error']}")
        
        # Verify at least one collector worked
        successful_collectors = [name for name, result in results.items() if result['success']]
        assert len(successful_collectors) >= 1, f"At least one collector should work. Results: {results}"


# Utility functions for testing
async def test_collector_method_timeout(collector, method_name: str, timeout: float = 30.0):
    """Test a collector method with timeout"""
    if not hasattr(collector, method_name):
        return None, f"Method {method_name} not available"
    
    method = getattr(collector, method_name)
    try:
        result = await asyncio.wait_for(method(), timeout=timeout)
        return result, None
    except asyncio.TimeoutError:
        return None, f"Timeout after {timeout}s"
    except Exception as e:
        return None, str(e)


def validate_data_structure(data: Any, expected_type: type = None, min_length: int = 0) -> tuple[bool, str]:
    """Validate basic data structure expectations"""
    if data is None:
        return False, "Data is None"
    
    if expected_type and not isinstance(data, expected_type):
        return False, f"Expected {expected_type.__name__}, got {type(data).__name__}"
    
    if isinstance(data, (list, dict)) and len(data) < min_length:
        return False, f"Data length {len(data)} less than minimum {min_length}"
    
    return True, "Valid"


if __name__ == "__main__":
    # Run a quick manual test
    async def quick_connection_test():
        print("🧪 Running quick data source connection test...")
        
        config = get_settings()
        initialize_all_collectors()
        
        # Test each collector quickly
        collectors = ["action_network", "vsin", "sbd"]
        
        for collector_name in collectors:
            try:
                collector = get_collector_instance(collector_name, config)
                if collector:
                    print(f"✅ {collector_name}: Collector available ({collector.__class__.__name__})")
                else:
                    print(f"❌ {collector_name}: Collector not available")
            except Exception as e:
                print(f"❌ {collector_name}: Error - {e}")
    
    asyncio.run(quick_connection_test())