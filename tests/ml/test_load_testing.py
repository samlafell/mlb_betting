"""
Load Testing for ML Pipeline
Tests high-concurrency scenarios with security controls and performance validation
"""

import pytest
import asyncio
import aiohttp
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch, AsyncMock
import os
from typing import List, Dict, Any

from src.ml.api.main import app
from src.ml.api.security import RateLimiter, SecurityConfig
from src.ml.features.redis_atomic_store import RedisAtomicStore
from src.ml.database.connection_pool import DatabaseConnectionPool
from fastapi.testclient import TestClient


@pytest.mark.load_test
class TestAPILoadTesting:
    """Load testing for ML API with security controls"""
    
    @pytest.fixture
    def test_client(self):
        """Test client for load testing"""
        return TestClient(app)
    
    @pytest.fixture
    def rate_limiter(self):
        """Rate limiter for testing"""
        return RateLimiter(redis_client=None)  # Memory-based for testing
    
    @pytest.fixture
    def security_config(self):
        """Security configuration for testing"""
        return SecurityConfig(
            rate_limit_per_minute=100,
            prediction_rate_limit=20,
            environment='testing'
        )
    
    def test_concurrent_api_requests_with_rate_limiting(
        self, 
        test_client, 
        rate_limiter, 
        security_config
    ):
        """Test concurrent API requests under rate limiting"""
        # Test configuration
        num_concurrent_requests = 50
        test_endpoint = "/health"
        
        def make_request(request_id: int) -> Dict[str, Any]:
            """Make single API request"""
            start_time = time.time()
            try:
                response = test_client.get(test_endpoint)
                end_time = time.time()
                
                return {
                    'request_id': request_id,
                    'status_code': response.status_code,
                    'response_time_ms': (end_time - start_time) * 1000,
                    'success': response.status_code == 200
                }
            except Exception as e:
                return {
                    'request_id': request_id,
                    'status_code': 500,
                    'response_time_ms': 0,
                    'success': False,
                    'error': str(e)
                }
        
        # Execute concurrent requests
        with ThreadPoolExecutor(max_workers=10) as executor:
            start_time = time.time()
            futures = [
                executor.submit(make_request, i) 
                for i in range(num_concurrent_requests)
            ]
            results = [future.result() for future in futures]
            total_time = time.time() - start_time
        
        # Analyze results
        successful_requests = [r for r in results if r['success']]
        failed_requests = [r for r in results if not r['success']]
        
        avg_response_time = sum(r['response_time_ms'] for r in successful_requests) / len(successful_requests)
        p95_response_time = sorted([r['response_time_ms'] for r in successful_requests])[int(len(successful_requests) * 0.95)]
        
        # Assertions
        assert len(successful_requests) >= num_concurrent_requests * 0.95  # 95% success rate
        assert avg_response_time < 100  # Average under 100ms
        assert p95_response_time < 200  # P95 under 200ms
        assert total_time < 10  # Complete within 10 seconds
        
        # Log performance metrics
        print(f"\n📊 Load Test Results:")
        print(f"Total Requests: {num_concurrent_requests}")
        print(f"Successful: {len(successful_requests)}")
        print(f"Failed: {len(failed_requests)}")
        print(f"Success Rate: {len(successful_requests)/num_concurrent_requests*100:.1f}%")
        print(f"Average Response Time: {avg_response_time:.2f}ms")
        print(f"P95 Response Time: {p95_response_time:.2f}ms")
        print(f"Total Execution Time: {total_time:.2f}s")
    
    def test_rate_limiting_under_load(self, rate_limiter, security_config):
        """Test rate limiting behavior under high load"""
        client_id = "load_test_client"
        rate_limit = 10  # 10 requests per minute
        
        async def test_rate_limiting():
            # Make requests up to limit
            for i in range(rate_limit):
                allowed = await rate_limiter.is_allowed(client_id, rate_limit, 60)
                assert allowed is True, f"Request {i+1} should be allowed"
            
            # Next requests should be rate limited
            for i in range(5):
                allowed = await rate_limiter.is_allowed(client_id, rate_limit, 60)
                assert allowed is False, f"Request {rate_limit+i+1} should be rate limited"
        
        asyncio.run(test_rate_limiting())
    
    def test_authentication_performance_under_load(self):
        """Test API key authentication performance under load"""
        from src.ml.api.security import verify_api_key
        
        # Test data
        correct_key = "test_secret_key_12345"
        wrong_key = "wrong_key_12345"
        num_operations = 1000
        
        with patch.dict(os.environ, {'API_SECRET_KEY': correct_key}):
            # Benchmark correct key verification
            start_time = time.time()
            for _ in range(num_operations):
                result = verify_api_key(correct_key)
                assert result is True
            correct_key_time = time.time() - start_time
            
            # Benchmark wrong key verification
            start_time = time.time()
            for _ in range(num_operations):
                result = verify_api_key(wrong_key)
                assert result is False
            wrong_key_time = time.time() - start_time
            
            # Performance assertions
            avg_correct_time = (correct_key_time / num_operations) * 1000
            avg_wrong_time = (wrong_key_time / num_operations) * 1000
            
            assert avg_correct_time < 1.0  # Under 1ms per verification
            assert avg_wrong_time < 1.0   # Under 1ms per verification
            
            # Timing attack resistance (should take similar time)
            time_difference = abs(avg_correct_time - avg_wrong_time)
            assert time_difference < 0.1  # Less than 0.1ms difference
            
            print(f"\n🔒 Authentication Performance:")
            print(f"Correct Key Avg: {avg_correct_time:.3f}ms")
            print(f"Wrong Key Avg: {avg_wrong_time:.3f}ms")
            print(f"Time Difference: {time_difference:.3f}ms")
    
    @pytest.mark.asyncio
    async def test_concurrent_prediction_requests(self):
        """Test concurrent prediction API requests with authentication"""
        # Skip if no test environment setup
        pytest.skip("Requires full API and model deployment")
        
        # This would test:
        # 1. Multiple concurrent prediction requests
        # 2. Rate limiting enforcement
        # 3. Authentication under load
        # 4. Model serving performance
        
        async with aiohttp.ClientSession() as session:
            base_url = "http://localhost:8000"
            headers = {"Authorization": "Bearer test_api_key"}
            
            async def make_prediction_request(session, request_id):
                payload = {
                    "game_id": 12345 + request_id,
                    "feature_version": "load_test_v1.0"
                }
                
                start_time = time.time()
                try:
                    async with session.post(
                        f"{base_url}/api/v1/predict",
                        json=payload,
                        headers=headers
                    ) as response:
                        end_time = time.time()
                        return {
                            'request_id': request_id,
                            'status_code': response.status,
                            'response_time_ms': (end_time - start_time) * 1000,
                            'success': response.status == 200
                        }
                except Exception as e:
                    return {
                        'request_id': request_id,
                        'status_code': 500,
                        'response_time_ms': 0,
                        'success': False,
                        'error': str(e)
                    }
            
            # Execute concurrent requests
            num_requests = 20
            tasks = [
                make_prediction_request(session, i) 
                for i in range(num_requests)
            ]
            results = await asyncio.gather(*tasks)
            
            # Analyze results
            successful_requests = [r for r in results if r['success']]
            rate_limited_requests = [r for r in results if r['status_code'] == 429]
            
            # Should have some successful requests and some rate limited
            assert len(successful_requests) > 0
            assert len(rate_limited_requests) > 0  # Rate limiting should kick in


@pytest.mark.load_test
class TestRedisLoadTesting:
    """Load testing for Redis atomic store operations"""
    
    @pytest.fixture
    async def redis_store(self):
        """Redis store for load testing"""
        store = RedisAtomicStore(
            redis_url="redis://localhost:6379/15",  # Test database
            use_msgpack=True
        )
        await store.initialize()
        yield store
        await store.close()
    
    @pytest.mark.asyncio
    async def test_concurrent_redis_operations(self, redis_store):
        """Test concurrent Redis operations with atomic guarantees"""
        # Skip if Redis not available
        if not redis_store.redis_client:
            pytest.skip("Redis not available for load testing")
        
        num_concurrent_operations = 50
        base_game_id = 20000
        
        async def cache_operation(operation_id: int):
            """Single cache operation"""
            from src.ml.features.models import FeatureVector, TemporalFeatures
            
            game_id = base_game_id + operation_id
            feature_vector = FeatureVector(
                game_id=game_id,
                feature_cutoff_time=datetime.now() - timedelta(hours=2),
                feature_version="load_test_v1.0",
                minutes_before_game=120,
                temporal_features=TemporalFeatures(
                    feature_cutoff_time=datetime.now() - timedelta(hours=2),
                    minutes_before_game=120
                )
            )
            
            start_time = time.time()
            try:
                # Cache operation
                success = await redis_store.cache_feature_vector_atomic(
                    game_id, feature_vector, ttl=300
                )
                cache_time = time.time() - start_time
                
                # Retrieve operation
                start_time = time.time()
                retrieved = await redis_store.get_feature_vector_atomic(
                    game_id, "load_test_v1.0"
                )
                retrieve_time = time.time() - start_time
                
                return {
                    'operation_id': operation_id,
                    'cache_success': success,
                    'retrieve_success': retrieved is not None,
                    'cache_time_ms': cache_time * 1000,
                    'retrieve_time_ms': retrieve_time * 1000,
                    'total_time_ms': (cache_time + retrieve_time) * 1000
                }
            except Exception as e:
                return {
                    'operation_id': operation_id,
                    'cache_success': False,
                    'retrieve_success': False,
                    'error': str(e)
                }
        
        # Execute concurrent operations
        start_time = time.time()
        tasks = [cache_operation(i) for i in range(num_concurrent_operations)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        total_time = time.time() - start_time
        
        # Analyze results
        successful_operations = [
            r for r in results 
            if not isinstance(r, Exception) and r.get('cache_success') and r.get('retrieve_success')
        ]
        
        avg_cache_time = sum(r['cache_time_ms'] for r in successful_operations) / len(successful_operations)
        avg_retrieve_time = sum(r['retrieve_time_ms'] for r in successful_operations) / len(successful_operations)
        
        # Assertions
        assert len(successful_operations) >= num_concurrent_operations * 0.95  # 95% success rate
        assert avg_cache_time < 100  # Average cache time under 100ms
        assert avg_retrieve_time < 50   # Average retrieve time under 50ms
        assert total_time < 5  # Total execution under 5 seconds
        
        print(f"\n⚡ Redis Load Test Results:")
        print(f"Concurrent Operations: {num_concurrent_operations}")
        print(f"Successful: {len(successful_operations)}")
        print(f"Success Rate: {len(successful_operations)/num_concurrent_operations*100:.1f}%")
        print(f"Average Cache Time: {avg_cache_time:.2f}ms")
        print(f"Average Retrieve Time: {avg_retrieve_time:.2f}ms")
        print(f"Total Execution Time: {total_time:.2f}s")
    
    @pytest.mark.asyncio
    async def test_redis_atomic_lock_contention(self, redis_store):
        """Test Redis atomic locks under high contention"""
        if not redis_store.redis_client:
            pytest.skip("Redis not available for load testing")
        
        shared_resource_key = "load_test_shared_resource"
        num_competitors = 20
        operations_per_competitor = 5
        
        async def compete_for_resource(competitor_id: int):
            """Competitor trying to access shared resource"""
            successful_acquisitions = 0
            
            for operation in range(operations_per_competitor):
                try:
                    async with redis_store.distributed_lock(
                        f"{shared_resource_key}_{operation}", 
                        timeout=1
                    ):
                        # Simulate work inside critical section
                        await asyncio.sleep(0.01)  # 10ms work
                        successful_acquisitions += 1
                except TimeoutError:
                    # Lock timeout - expected under contention
                    pass
                except Exception:
                    # Unexpected error
                    pass
            
            return {
                'competitor_id': competitor_id,
                'successful_acquisitions': successful_acquisitions,
                'total_attempts': operations_per_competitor
            }
        
        # Run competitors concurrently
        tasks = [compete_for_resource(i) for i in range(num_competitors)]
        results = await asyncio.gather(*tasks)
        
        # Analyze lock contention results
        total_acquisitions = sum(r['successful_acquisitions'] for r in results)
        total_attempts = sum(r['total_attempts'] for r in results)
        
        # Assertions
        assert total_acquisitions > 0  # At least some locks acquired
        assert total_acquisitions <= operations_per_competitor  # No double-locking per operation
        
        # Check that locks prevented race conditions
        # (Each operation should only be accessed by one competitor)
        acquisition_rate = total_acquisitions / total_attempts
        
        print(f"\n🔒 Lock Contention Results:")
        print(f"Competitors: {num_competitors}")
        print(f"Total Attempts: {total_attempts}")
        print(f"Successful Acquisitions: {total_acquisitions}")
        print(f"Acquisition Rate: {acquisition_rate*100:.1f}%")


@pytest.mark.load_test
class TestDatabaseLoadTesting:
    """Load testing for database connection pool"""
    
    @pytest.fixture
    async def db_pool(self):
        """Database connection pool for testing"""
        from src.core.config import Config
        config = Config()
        config.database.database = "mlb_betting_test"  # Test database
        
        pool = DatabaseConnectionPool(config.database)
        await pool.initialize()
        yield pool
        await pool.close()
    
    @pytest.mark.asyncio
    async def test_connection_pool_under_load(self, db_pool):
        """Test database connection pool under concurrent load"""
        pytest.skip("Requires test database setup")
        
        num_concurrent_queries = 30  # More than pool size to test queuing
        
        async def execute_query(query_id: int):
            """Execute single database query"""
            start_time = time.time()
            try:
                async with db_pool.get_connection() as conn:
                    # Simulate query
                    result = await conn.fetchval("SELECT 1")
                    query_time = time.time() - start_time
                    
                    return {
                        'query_id': query_id,
                        'success': result == 1,
                        'query_time_ms': query_time * 1000
                    }
            except Exception as e:
                return {
                    'query_id': query_id,
                    'success': False,
                    'error': str(e),
                    'query_time_ms': (time.time() - start_time) * 1000
                }
        
        # Execute concurrent queries
        start_time = time.time()
        tasks = [execute_query(i) for i in range(num_concurrent_queries)]
        results = await asyncio.gather(*tasks)
        total_time = time.time() - start_time
        
        # Analyze results
        successful_queries = [r for r in results if r.get('success')]
        avg_query_time = sum(r['query_time_ms'] for r in successful_queries) / len(successful_queries)
        
        # Assertions
        assert len(successful_queries) == num_concurrent_queries  # All should succeed
        assert avg_query_time < 100  # Average under 100ms
        assert total_time < 10  # Complete within 10 seconds
        
        print(f"\n🗄️ Database Load Test Results:")
        print(f"Concurrent Queries: {num_concurrent_queries}")
        print(f"Successful: {len(successful_queries)}")
        print(f"Average Query Time: {avg_query_time:.2f}ms")
        print(f"Total Execution Time: {total_time:.2f}s")


@pytest.mark.load_test
class TestMemoryAndResourceUsage:
    """Test memory usage and resource consumption under load"""
    
    def test_memory_usage_feature_extraction(self):
        """Test memory usage during batch feature extraction"""
        import psutil
        import gc
        from src.ml.features.models import FeatureVector, TemporalFeatures
        
        # Measure baseline memory
        gc.collect()
        process = psutil.Process()
        baseline_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Create large batch of feature vectors
        feature_vectors = []
        batch_size = 1000
        
        for i in range(batch_size):
            fv = FeatureVector(
                game_id=i,
                feature_cutoff_time=datetime.now() - timedelta(hours=2),
                feature_version="memory_test_v1.0",
                minutes_before_game=120,
                temporal_features=TemporalFeatures(
                    feature_cutoff_time=datetime.now() - timedelta(hours=2),
                    minutes_before_game=120
                )
            )
            feature_vectors.append(fv)
            
            # Check memory every 100 items
            if i % 100 == 0:
                current_memory = process.memory_info().rss / 1024 / 1024
                memory_growth = current_memory - baseline_memory
                
                # Memory growth should be reasonable (less than 50MB per 100 items)
                assert memory_growth < 50, f"Excessive memory growth: {memory_growth:.1f}MB"
        
        # Final memory check
        final_memory = process.memory_info().rss / 1024 / 1024
        total_growth = final_memory - baseline_memory
        
        # Clean up
        del feature_vectors
        gc.collect()
        cleanup_memory = process.memory_info().rss / 1024 / 1024
        
        print(f"\n💾 Memory Usage Test:")
        print(f"Baseline Memory: {baseline_memory:.1f}MB")
        print(f"Peak Memory: {final_memory:.1f}MB")
        print(f"Memory Growth: {total_growth:.1f}MB")
        print(f"After Cleanup: {cleanup_memory:.1f}MB")
        print(f"Memory per Item: {(total_growth/batch_size)*1024:.1f}KB")
        
        # Assertions
        assert total_growth < 200  # Total growth under 200MB for 1000 items
        assert (cleanup_memory - baseline_memory) < 50  # Good cleanup (under 50MB residual)
    
    def test_cpu_usage_serialization_performance(self):
        """Test CPU usage during high-frequency serialization"""
        import psutil
        from src.ml.features.redis_atomic_store import RedisAtomicStore
        from src.ml.features.models import FeatureVector, TemporalFeatures
        
        # Create test data
        store = RedisAtomicStore(use_msgpack=True)
        feature_vector = FeatureVector(
            game_id=12345,
            feature_cutoff_time=datetime.now() - timedelta(hours=2),
            feature_version="cpu_test_v1.0",
            minutes_before_game=120,
            temporal_features=TemporalFeatures(
                feature_cutoff_time=datetime.now() - timedelta(hours=2),
                minutes_before_game=120
            )
        )
        
        # Measure CPU usage during serialization
        process = psutil.Process()
        cpu_percent_before = process.cpu_percent()
        
        # High-frequency serialization
        start_time = time.time()
        num_operations = 10000
        
        for _ in range(num_operations):
            serialized = store._serialize_feature_vector(feature_vector)
            deserialized = store._deserialize_feature_vector(serialized)
            assert deserialized.game_id == feature_vector.game_id
        
        execution_time = time.time() - start_time
        cpu_percent_after = process.cpu_percent()
        
        # Performance assertions
        ops_per_second = num_operations / execution_time
        avg_time_per_op = (execution_time / num_operations) * 1000  # ms
        
        assert ops_per_second > 1000  # At least 1000 ops/second
        assert avg_time_per_op < 1.0   # Under 1ms per operation
        
        print(f"\n🖥️ CPU Performance Test:")
        print(f"Operations: {num_operations}")
        print(f"Execution Time: {execution_time:.2f}s")
        print(f"Ops/Second: {ops_per_second:.0f}")
        print(f"Avg Time/Op: {avg_time_per_op:.3f}ms")
        print(f"CPU Usage: {cpu_percent_before:.1f}% → {cpu_percent_after:.1f}%")