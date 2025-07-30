# 🚀 ML Pipeline Scalability & Horizontal Scaling Strategy

**MLB Betting ML Pipeline - Production Scalability Architecture**

## Overview

This document outlines comprehensive scalability strategies for the MLB ML prediction pipeline, covering horizontal scaling, load balancing, auto-scaling, microservices architecture, and performance optimization for handling enterprise-scale workloads.

## 🏗️ Current Architecture Analysis

### Single-Node Limitations

**Current Bottlenecks**:
- Single API server handling all requests
- Single Redis instance (SPOF - Single Point of Failure)
- Database connection pool limits
- In-memory rate limiting (not distributed)
- Monolithic deployment model

**Resource Constraints**:
- CPU: Limited to single machine cores
- Memory: Bounded by single machine RAM
- I/O: Single machine disk and network limits
- Availability: Single point of failure risks

## 🌐 Horizontal Scaling Architecture

### Multi-Tier Distributed Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Load Balancer Layer                     │
├─────────────────────────────────────────────────────────────┤
│                   API Gateway Cluster                      │
├─────────────────────────────────────────────────────────────┤
│              Application Server Cluster                    │
├─────────────────────────────────────────────────────────────┤
│                ML Processing Cluster                       │
├─────────────────────────────────────────────────────────────┤
│              Distributed Cache Layer                       │
├─────────────────────────────────────────────────────────────┤
│              Database Cluster                              │
└─────────────────────────────────────────────────────────────┘
```

### Microservices Decomposition

```python
# src/ml/architecture/microservices_config.py
from typing import Dict, List
from pydantic import BaseModel

class ServiceConfig(BaseModel):
    """Microservice configuration"""
    name: str
    port: int
    replicas: int
    resources: Dict[str, str]
    dependencies: List[str]

class ScalabilityConfig(BaseModel):
    """Scalability configuration for microservices"""
    
    services = {
        "api-gateway": ServiceConfig(
            name="ml-api-gateway",
            port=8000,
            replicas=3,
            resources={"cpu": "500m", "memory": "512Mi"},
            dependencies=[]
        ),
        
        "prediction-service": ServiceConfig(
            name="ml-prediction-service", 
            port=8001,
            replicas=5,
            resources={"cpu": "1000m", "memory": "2Gi"},
            dependencies=["feature-service", "model-service"]
        ),
        
        "feature-service": ServiceConfig(
            name="ml-feature-service",
            port=8002, 
            replicas=4,
            resources={"cpu": "750m", "memory": "1Gi"},
            dependencies=["database-service", "cache-service"]
        ),
        
        "model-service": ServiceConfig(
            name="ml-model-service",
            port=8003,
            replicas=3,
            resources={"cpu": "2000m", "memory": "4Gi"}, 
            dependencies=["model-storage"]
        ),
        
        "auth-service": ServiceConfig(
            name="ml-auth-service",
            port=8004,
            replicas=2,
            resources={"cpu": "250m", "memory": "256Mi"},
            dependencies=["cache-service"]
        ),
        
        "monitoring-service": ServiceConfig(
            name="ml-monitoring-service",
            port=8005,
            replicas=2, 
            resources={"cpu": "500m", "memory": "1Gi"},
            dependencies=["database-service", "cache-service"]
        )
    }
```

## ⚖️ Load Balancing Strategy

### Multi-Layer Load Balancing

#### Layer 1: External Load Balancer (L7)

```yaml
# infrastructure/load-balancer/nginx.conf
upstream ml_api_gateway {
    least_conn;
    server ml-api-gateway-1:8000 max_fails=3 fail_timeout=30s;
    server ml-api-gateway-2:8000 max_fails=3 fail_timeout=30s;
    server ml-api-gateway-3:8000 max_fails=3 fail_timeout=30s;
}

upstream ml_prediction_service {
    ip_hash;  # Session affinity for model caching
    server ml-prediction-1:8001 max_fails=2 fail_timeout=20s;
    server ml-prediction-2:8001 max_fails=2 fail_timeout=20s;
    server ml-prediction-3:8001 max_fails=2 fail_timeout=20s;
    server ml-prediction-4:8001 max_fails=2 fail_timeout=20s;
    server ml-prediction-5:8001 max_fails=2 fail_timeout=20s;
}

server {
    listen 443 ssl http2;
    server_name ml-api.yourdomain.com;
    
    # SSL configuration
    ssl_certificate /etc/ssl/certs/ml-api.crt;
    ssl_certificate_key /etc/ssl/private/ml-api.key;
    ssl_protocols TLSv1.2 TLSv1.3;
    
    # Rate limiting
    limit_req_zone $binary_remote_addr zone=api_limit:10m rate=100r/m;
    limit_req zone=api_limit burst=20 nodelay;
    
    # API Gateway routing
    location /api/v1/predict {
        proxy_pass http://ml_prediction_service;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_connect_timeout 5s;
        proxy_send_timeout 60s;
        proxy_read_timeout 60s;
    }
    
    location / {
        proxy_pass http://ml_api_gateway;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }
}
```

#### Layer 2: Service Mesh (Istio)

```yaml
# infrastructure/service-mesh/virtual-service.yaml
apiVersion: networking.istio.io/v1beta1
kind: VirtualService
metadata:
  name: ml-prediction-routing
spec:
  hosts:
  - ml-prediction-service
  http:
  - match:
    - headers:
        priority:
          exact: "high"
    route:
    - destination:
        host: ml-prediction-service
        subset: high-performance
      weight: 100
  - route:
    - destination:
        host: ml-prediction-service 
        subset: standard
      weight: 80
    - destination:
        host: ml-prediction-service
        subset: high-performance  
      weight: 20
```

### Intelligent Load Balancing

```python
# src/ml/infrastructure/load_balancer.py
import asyncio
import time
from typing import Dict, List, Optional
from dataclasses import dataclass
from enum import Enum

class LoadBalancingStrategy(str, Enum):
    """Load balancing strategies"""
    ROUND_ROBIN = "round_robin"
    LEAST_CONNECTIONS = "least_connections" 
    WEIGHTED_ROUND_ROBIN = "weighted_round_robin"
    RESOURCE_BASED = "resource_based"
    PREDICTION_AWARE = "prediction_aware"

@dataclass
class ServiceInstance:
    """Service instance information"""
    host: str
    port: int
    weight: int = 1
    current_connections: int = 0
    avg_response_time: float = 0.0
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    healthy: bool = True
    model_cache_hit_rate: float = 0.0

class IntelligentLoadBalancer:
    """Intelligent load balancer with multiple strategies"""
    
    def __init__(self, strategy: LoadBalancingStrategy = LoadBalancingStrategy.RESOURCE_BASED):
        self.strategy = strategy
        self.instances: Dict[str, List[ServiceInstance]] = {}
        self.round_robin_counters: Dict[str, int] = {}
        
    def register_service(self, service_name: str, instances: List[ServiceInstance]):
        """Register service instances"""
        self.instances[service_name] = instances
        self.round_robin_counters[service_name] = 0
        
    def get_instance(self, service_name: str, context: Dict = None) -> Optional[ServiceInstance]:
        """Get best instance based on strategy"""
        if service_name not in self.instances:
            return None
            
        instances = [i for i in self.instances[service_name] if i.healthy]
        if not instances:
            return None
            
        if self.strategy == LoadBalancingStrategy.ROUND_ROBIN:
            return self._round_robin_selection(service_name, instances)
        elif self.strategy == LoadBalancingStrategy.LEAST_CONNECTIONS:
            return self._least_connections_selection(instances)
        elif self.strategy == LoadBalancingStrategy.WEIGHTED_ROUND_ROBIN:
            return self._weighted_round_robin_selection(service_name, instances)
        elif self.strategy == LoadBalancingStrategy.RESOURCE_BASED:
            return self._resource_based_selection(instances)
        elif self.strategy == LoadBalancingStrategy.PREDICTION_AWARE:
            return self._prediction_aware_selection(instances, context or {})
        
        return instances[0]  # Fallback
    
    def _round_robin_selection(self, service_name: str, instances: List[ServiceInstance]) -> ServiceInstance:
        """Round robin selection"""
        counter = self.round_robin_counters[service_name]
        instance = instances[counter % len(instances)]
        self.round_robin_counters[service_name] = (counter + 1) % len(instances)
        return instance
    
    def _least_connections_selection(self, instances: List[ServiceInstance]) -> ServiceInstance:
        """Least connections selection"""
        return min(instances, key=lambda i: i.current_connections)
    
    def _weighted_round_robin_selection(self, service_name: str, instances: List[ServiceInstance]) -> ServiceInstance:
        """Weighted round robin selection"""
        total_weight = sum(i.weight for i in instances)
        counter = self.round_robin_counters[service_name] % total_weight
        
        current_weight = 0
        for instance in instances:
            current_weight += instance.weight
            if counter < current_weight:
                self.round_robin_counters[service_name] += 1
                return instance
        return instances[0]
    
    def _resource_based_selection(self, instances: List[ServiceInstance]) -> ServiceInstance:
        """Resource-based selection considering CPU and memory"""
        def resource_score(instance: ServiceInstance) -> float:
            # Lower score is better
            cpu_factor = instance.cpu_usage / 100.0
            memory_factor = instance.memory_usage / 100.0
            response_time_factor = min(instance.avg_response_time / 1000.0, 1.0)  # Cap at 1 second
            connection_factor = instance.current_connections / 100.0
            
            return (cpu_factor * 0.3 + 
                   memory_factor * 0.3 + 
                   response_time_factor * 0.2 + 
                   connection_factor * 0.2)
        
        return min(instances, key=resource_score)
    
    def _prediction_aware_selection(self, instances: List[ServiceInstance], context: Dict) -> ServiceInstance:
        """ML prediction-aware selection"""
        model_name = context.get("model_name", "")
        
        # Prefer instances with higher cache hit rates for the model
        def prediction_score(instance: ServiceInstance) -> float:
            base_score = self._calculate_resource_score(instance)
            cache_bonus = instance.model_cache_hit_rate * 0.2  # 20% bonus for cache hits
            return base_score - cache_bonus  # Lower is better
        
        return min(instances, key=prediction_score)
    
    def _calculate_resource_score(self, instance: ServiceInstance) -> float:
        """Calculate resource utilization score"""
        return (instance.cpu_usage * 0.4 + 
                instance.memory_usage * 0.3 + 
                instance.current_connections * 0.2 + 
                instance.avg_response_time * 0.1)
```

## 📊 Auto-Scaling Configuration

### Kubernetes Horizontal Pod Autoscaler (HPA)

```yaml
# infrastructure/k8s/hpa-prediction-service.yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: ml-prediction-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: ml-prediction-service
  minReplicas: 5
  maxReplicas: 50
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
  - type: Pods
    pods:
      metric:
        name: prediction_requests_per_second
      target:
        type: AverageValue
        averageValue: "10"
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
      - type: Percent
        value: 10
        periodSeconds: 60
    scaleUp:
      stabilizationWindowSeconds: 60
      policies:
      - type: Percent
        value: 50
        periodSeconds: 60
      - type: Pods
        value: 5
        periodSeconds: 60
      selectPolicy: Max
```

### Custom Auto-Scaling Logic

```python
# src/ml/infrastructure/autoscaler.py
import asyncio
import time
from typing import Dict, List, Optional
from dataclasses import dataclass
from enum import Enum

@dataclass
class ScalingMetrics:
    """Metrics for scaling decisions"""
    cpu_usage_percent: float
    memory_usage_percent: float
    requests_per_second: float
    avg_response_time_ms: float
    queue_depth: int
    error_rate_percent: float
    prediction_cache_hit_rate: float

class ScalingDecision(str, Enum):
    """Scaling decision types"""
    SCALE_UP = "scale_up"
    SCALE_DOWN = "scale_down"
    NO_CHANGE = "no_change"

class CustomAutoScaler:
    """Custom auto-scaling logic for ML services"""
    
    def __init__(self):
        self.scaling_rules = {
            "prediction-service": {
                "min_replicas": 5,
                "max_replicas": 50,
                "scale_up_threshold": {
                    "cpu_usage": 70,
                    "memory_usage": 80, 
                    "requests_per_second": 100,
                    "avg_response_time": 1000,  # 1 second
                    "queue_depth": 50
                },
                "scale_down_threshold": {
                    "cpu_usage": 30,
                    "memory_usage": 40,
                    "requests_per_second": 20,
                    "avg_response_time": 200,  # 200ms
                    "queue_depth": 5
                },
                "cooldown_minutes": 5,
                "scale_up_step": 3,
                "scale_down_step": 1
            },
            
            "feature-service": {
                "min_replicas": 3,
                "max_replicas": 20,
                "scale_up_threshold": {
                    "cpu_usage": 60,
                    "memory_usage": 70,
                    "requests_per_second": 200,
                    "avg_response_time": 500,
                    "queue_depth": 30
                },
                "scale_down_threshold": {
                    "cpu_usage": 20,
                    "memory_usage": 30,
                    "requests_per_second": 50,
                    "avg_response_time": 100,
                    "queue_depth": 3
                },
                "cooldown_minutes": 3,
                "scale_up_step": 2,
                "scale_down_step": 1
            }
        }
        
        self.last_scaling_actions: Dict[str, float] = {}
    
    async def evaluate_scaling(self, service_name: str, current_replicas: int, metrics: ScalingMetrics) -> Dict:
        """Evaluate if scaling is needed"""
        
        if service_name not in self.scaling_rules:
            return {"decision": ScalingDecision.NO_CHANGE, "reason": "No scaling rules defined"}
        
        rules = self.scaling_rules[service_name]
        current_time = time.time()
        
        # Check cooldown period
        last_scaling = self.last_scaling_actions.get(service_name, 0)
        cooldown_seconds = rules["cooldown_minutes"] * 60
        
        if current_time - last_scaling < cooldown_seconds:
            return {"decision": ScalingDecision.NO_CHANGE, "reason": "Cooldown period active"}
        
        # Evaluate scale up conditions
        scale_up_score = self._calculate_scale_up_score(metrics, rules["scale_up_threshold"])
        scale_down_score = self._calculate_scale_down_score(metrics, rules["scale_down_threshold"])
        
        if scale_up_score >= 0.7 and current_replicas < rules["max_replicas"]:
            new_replicas = min(
                current_replicas + rules["scale_up_step"],
                rules["max_replicas"]
            )
            self.last_scaling_actions[service_name] = current_time
            
            return {
                "decision": ScalingDecision.SCALE_UP,
                "current_replicas": current_replicas,
                "target_replicas": new_replicas,
                "score": scale_up_score,
                "reason": "High resource utilization detected"
            }
        
        elif scale_down_score >= 0.7 and current_replicas > rules["min_replicas"]:
            new_replicas = max(
                current_replicas - rules["scale_down_step"],
                rules["min_replicas"]
            )
            self.last_scaling_actions[service_name] = current_time
            
            return {
                "decision": ScalingDecision.SCALE_DOWN,
                "current_replicas": current_replicas, 
                "target_replicas": new_replicas,
                "score": scale_down_score,
                "reason": "Low resource utilization detected"
            }
        
        return {
            "decision": ScalingDecision.NO_CHANGE,
            "reason": f"Metrics within normal range (up: {scale_up_score:.2f}, down: {scale_down_score:.2f})"
        }
    
    def _calculate_scale_up_score(self, metrics: ScalingMetrics, thresholds: Dict) -> float:
        """Calculate score for scaling up (0.0 to 1.0)"""
        scores = []
        
        if metrics.cpu_usage_percent > thresholds["cpu_usage"]:
            scores.append(min(metrics.cpu_usage_percent / thresholds["cpu_usage"], 2.0) - 1.0)
        
        if metrics.memory_usage_percent > thresholds["memory_usage"]:
            scores.append(min(metrics.memory_usage_percent / thresholds["memory_usage"], 2.0) - 1.0)
        
        if metrics.requests_per_second > thresholds["requests_per_second"]:
            scores.append(min(metrics.requests_per_second / thresholds["requests_per_second"], 2.0) - 1.0)
        
        if metrics.avg_response_time_ms > thresholds["avg_response_time"]:
            scores.append(min(metrics.avg_response_time_ms / thresholds["avg_response_time"], 2.0) - 1.0)
        
        if metrics.queue_depth > thresholds["queue_depth"]:
            scores.append(min(metrics.queue_depth / thresholds["queue_depth"], 2.0) - 1.0)
        
        return max(scores) if scores else 0.0
    
    def _calculate_scale_down_score(self, metrics: ScalingMetrics, thresholds: Dict) -> float:
        """Calculate score for scaling down (0.0 to 1.0)"""
        conditions_met = 0
        total_conditions = 5
        
        if metrics.cpu_usage_percent < thresholds["cpu_usage"]:
            conditions_met += 1
        if metrics.memory_usage_percent < thresholds["memory_usage"]:
            conditions_met += 1
        if metrics.requests_per_second < thresholds["requests_per_second"]:
            conditions_met += 1
        if metrics.avg_response_time_ms < thresholds["avg_response_time"]:
            conditions_met += 1
        if metrics.queue_depth < thresholds["queue_depth"]:
            conditions_met += 1
        
        # Require most conditions to be met for scale down
        return conditions_met / total_conditions
```

## 🗄️ Distributed Data Layer

### Redis Cluster Configuration

```yaml
# infrastructure/redis/redis-cluster.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: redis-cluster-config
data:
  redis.conf: |
    cluster-enabled yes
    cluster-config-file nodes.conf
    cluster-node-timeout 5000
    cluster-announce-ip ${REDIS_CLUSTER_ANNOUNCE_IP}
    cluster-announce-port 6379
    cluster-announce-bus-port 16379
    
    # Security
    requirepass ${REDIS_PASSWORD}
    masterauth ${REDIS_PASSWORD}
    
    # Memory management
    maxmemory 2gb
    maxmemory-policy allkeys-lru
    
    # Persistence
    save 900 1
    save 300 10
    save 60 10000
    
    # Network
    tcp-keepalive 300
    timeout 0
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: redis-cluster
spec:
  serviceName: redis-cluster-service
  replicas: 6
  selector:
    matchLabels:
      app: redis-cluster
  template:
    metadata:
      labels:
        app: redis-cluster
    spec:
      containers:
      - name: redis
        image: redis:7-alpine
        ports:
        - containerPort: 6379
          name: client
        - containerPort: 16379
          name: gossip
        command:
        - redis-server
        - /etc/redis/redis.conf
        env:
        - name: REDIS_CLUSTER_ANNOUNCE_IP
          valueFrom:
            fieldRef:
              fieldPath: status.podIP
        - name: REDIS_PASSWORD
          valueFrom:
            secretKeyRef:
              name: redis-secret
              key: password
        volumeMounts:
        - name: config
          mountPath: /etc/redis
        - name: data
          mountPath: /data
        resources:
          requests:
            memory: 1Gi
            cpu: 500m
          limits:
            memory: 2Gi
            cpu: 1000m
      volumes:
      - name: config
        configMap:
          name: redis-cluster-config
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 10Gi
```

### Database Clustering (PostgreSQL)

```yaml
# infrastructure/database/postgres-cluster.yaml
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: postgres-cluster
spec:
  instances: 3
  
  postgresql:
    parameters:
      max_connections: "200"
      shared_buffers: "256MB"
      effective_cache_size: "1GB"
      maintenance_work_mem: "64MB"
      checkpoint_completion_target: "0.9"
      wal_buffers: "16MB"
      default_statistics_target: "100"
      random_page_cost: "1.1"
      effective_io_concurrency: "200"
    
  bootstrap:
    initdb:
      database: mlb_betting
      owner: ml_user
      secret:
        name: postgres-credentials
  
  storage:
    size: 100Gi
    storageClass: fast-ssd
  
  resources:
    requests:
      memory: "2Gi"
      cpu: "1000m"
    limits:
      memory: "4Gi" 
      cpu: "2000m"
  
  monitoring:
    enabled: true
  
  backup:
    schedule: "0 0 2 * * *"  # Daily at 2 AM
    target: "primary"
```

## 🚀 Container Orchestration

### Kubernetes Deployment Strategy

```yaml
# infrastructure/k8s/ml-prediction-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ml-prediction-service
  labels:
    app: ml-prediction-service
spec:
  replicas: 5
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 1
      maxSurge: 2
  selector:
    matchLabels:
      app: ml-prediction-service
  template:
    metadata:
      labels:
        app: ml-prediction-service
    spec:
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        fsGroup: 1000
      containers:
      - name: ml-prediction
        image: ml-prediction-service:latest
        ports:
        - containerPort: 8001
        env:
        - name: ENVIRONMENT
          value: "production"
        - name: DB_HOST
          value: "postgres-cluster-rw"
        - name: REDIS_HOST
          value: "redis-cluster-service"
        - name: API_SECRET_KEY
          valueFrom:
            secretKeyRef:
              name: ml-secrets
              key: api-secret-key
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "1000m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8001
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8001
          initialDelaySeconds: 5
          periodSeconds: 5
        volumeMounts:
        - name: model-cache
          mountPath: /app/models
      volumes:
      - name: model-cache
        emptyDir:
          sizeLimit: 5Gi
---
apiVersion: v1
kind: Service
metadata:
  name: ml-prediction-service
spec:
  selector:
    app: ml-prediction-service
  ports:
  - protocol: TCP
    port: 80
    targetPort: 8001
  type: ClusterIP
```

## 📈 Performance Optimization

### Caching Strategy

```python
# src/ml/infrastructure/distributed_cache.py
import asyncio
import json
import hashlib
from typing import Any, Optional, List, Dict
from dataclasses import dataclass
import redis.asyncio as redis

@dataclass
class CacheConfig:
    """Cache configuration"""
    ttl_seconds: int = 3600
    max_memory_mb: int = 1024
    eviction_policy: str = "allkeys-lru"
    compression_enabled: bool = True

class DistributedCache:
    """Distributed caching with Redis cluster"""
    
    def __init__(self, redis_cluster_nodes: List[str], config: CacheConfig = None):
        self.config = config or CacheConfig()
        self.redis_cluster = redis.RedisCluster(
            startup_nodes=[{"host": node.split(":")[0], "port": int(node.split(":")[1])} 
                          for node in redis_cluster_nodes],
            decode_responses=True,
            skip_full_coverage_check=True
        )
        
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        try:
            cached_data = await self.redis_cluster.get(key)
            if cached_data:
                return json.loads(cached_data)
            return None
        except Exception as e:
            # Log error but don't fail the application
            print(f"Cache get error: {e}")
            return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set value in cache"""
        try:
            ttl = ttl or self.config.ttl_seconds
            serialized_value = json.dumps(value)
            await self.redis_cluster.setex(key, ttl, serialized_value)
            return True
        except Exception as e:
            print(f"Cache set error: {e}")
            return False
    
    async def get_or_set(self, key: str, factory_func, ttl: Optional[int] = None) -> Any:
        """Get from cache or set using factory function"""
        cached_value = await self.get(key)
        if cached_value is not None:
            return cached_value
        
        # Generate new value
        new_value = await factory_func() if asyncio.iscoroutinefunction(factory_func) else factory_func()
        await self.set(key, new_value, ttl)
        return new_value
    
    async def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate keys matching pattern"""
        try:
            keys = []
            async for key in self.redis_cluster.scan_iter(match=pattern):
                keys.append(key)
            
            if keys:
                return await self.redis_cluster.delete(*keys)
            return 0
        except Exception as e:
            print(f"Cache invalidation error: {e}")
            return 0

class ModelCache:
    """Specialized cache for ML models"""
    
    def __init__(self, distributed_cache: DistributedCache):
        self.cache = distributed_cache
        
    async def get_prediction(self, game_id: str, model_name: str, feature_hash: str) -> Optional[Dict]:
        """Get cached prediction"""
        cache_key = f"prediction:{model_name}:{game_id}:{feature_hash}"
        return await self.cache.get(cache_key)
    
    async def cache_prediction(self, game_id: str, model_name: str, feature_hash: str, 
                             prediction: Dict, ttl: int = 1800) -> bool:
        """Cache prediction result"""
        cache_key = f"prediction:{model_name}:{game_id}:{feature_hash}"
        return await self.cache.set(cache_key, prediction, ttl)
    
    async def get_features(self, game_id: str, feature_version: str) -> Optional[Dict]:
        """Get cached features"""
        cache_key = f"features:{feature_version}:{game_id}"
        return await self.cache.get(cache_key)
    
    async def cache_features(self, game_id: str, feature_version: str, 
                           features: Dict, ttl: int = 3600) -> bool:
        """Cache feature data"""
        cache_key = f"features:{feature_version}:{game_id}"
        return await self.cache.set(cache_key, features, ttl)
    
    def generate_feature_hash(self, features: Dict) -> str:
        """Generate hash for feature data"""
        sorted_features = json.dumps(features, sort_keys=True)
        return hashlib.md5(sorted_features.encode()).hexdigest()
```

### Connection Pooling

```python
# src/ml/infrastructure/connection_pool_manager.py
import asyncio
from typing import Dict, Any, Optional
from dataclasses import dataclass

@dataclass 
class PoolConfig:
    """Connection pool configuration"""
    min_size: int = 5
    max_size: int = 20
    max_queries: int = 50000
    max_inactive_connection_lifetime: float = 300.0
    
class ConnectionPoolManager:
    """Manage connection pools across services"""
    
    def __init__(self):
        self.pools: Dict[str, Any] = {}
        
    async def create_database_pool(self, service_name: str, database_url: str, 
                                 config: PoolConfig = None) -> Any:
        """Create database connection pool"""
        config = config or PoolConfig()
        
        import asyncpg
        pool = await asyncpg.create_pool(
            database_url,
            min_size=config.min_size,
            max_size=config.max_size,
            max_queries=config.max_queries,
            max_inactive_connection_lifetime=config.max_inactive_connection_lifetime,
            command_timeout=60
        )
        
        self.pools[f"db_{service_name}"] = pool
        return pool
    
    async def create_redis_pool(self, service_name: str, redis_url: str,
                              config: PoolConfig = None) -> Any:
        """Create Redis connection pool"""
        config = config or PoolConfig()
        
        import redis.asyncio as redis
        pool = redis.ConnectionPool.from_url(
            redis_url,
            max_connections=config.max_size,
            retry_on_timeout=True,
            socket_keepalive=True,
            socket_keepalive_options={},
            health_check_interval=30
        )
        
        self.pools[f"redis_{service_name}"] = pool
        return pool
    
    async def get_pool(self, pool_name: str) -> Optional[Any]:
        """Get connection pool by name"""
        return self.pools.get(pool_name)
    
    async def close_all_pools(self):
        """Close all connection pools"""
        for pool_name, pool in self.pools.items():
            try:
                if hasattr(pool, 'close'):
                    await pool.close()
                elif hasattr(pool, 'disconnect'):
                    await pool.disconnect()
            except Exception as e:
                print(f"Error closing pool {pool_name}: {e}")
```

## 🔧 Deployment Strategies

### Blue-Green Deployment

```yaml
# infrastructure/deployment/blue-green.yaml
apiVersion: argoproj.io/v1alpha1
kind: Rollout
metadata:
  name: ml-prediction-rollout
spec:
  replicas: 10
  strategy:
    blueGreen:
      activeService: ml-prediction-active
      previewService: ml-prediction-preview
      autoPromotionEnabled: false
      scaleDownDelaySeconds: 30
      prePromotionAnalysis:
        templates:
        - templateName: success-rate
        args:
        - name: service-name
          value: ml-prediction-preview
      postPromotionAnalysis:
        templates:
        - templateName: success-rate
        args:
        - name: service-name
          value: ml-prediction-active
  selector:
    matchLabels:
      app: ml-prediction
  template:
    metadata:
      labels:
        app: ml-prediction
    spec:
      containers:
      - name: ml-prediction
        image: ml-prediction:latest
        ports:
        - containerPort: 8001
        resources:
          requests:
            memory: 1Gi
            cpu: 500m
          limits:
            memory: 2Gi
            cpu: 1000m
```

### Canary Deployment

```yaml
# infrastructure/deployment/canary.yaml
apiVersion: argoproj.io/v1alpha1
kind: Rollout
metadata:
  name: ml-feature-rollout
spec:
  replicas: 8
  strategy:
    canary:
      steps:
      - setWeight: 10
      - pause:
          duration: 2m
      - setWeight: 25
      - pause:
          duration: 5m
      - setWeight: 50
      - pause:
          duration: 10m
      - setWeight: 75
      - pause:
          duration: 10m
      canaryService: ml-feature-canary
      stableService: ml-feature-stable
      trafficRouting:
        istio:
          virtualService:
            name: ml-feature-vs
            routes:
            - primary
      analysis:
        templates:
        - templateName: error-rate
        - templateName: response-time
        args:
        - name: service-name
          value: ml-feature-canary
        startingStep: 1
        interval: 30s
```

## 📊 Scalability Metrics and Monitoring

### Key Scalability Metrics

| Metric | Target | Critical Threshold | Auto-Scale Trigger |
|--------|--------|-------------------|-------------------|
| **Requests/Second** | 1000 | 5000 | >800 RPS |
| **Response Time P95** | <500ms | >2s | >1s |
| **CPU Utilization** | <70% | >90% | >75% |
| **Memory Usage** | <80% | >95% | >85% |
| **Database Connections** | <60% | >90% | >70% |
| **Redis Hit Rate** | >90% | <70% | <80% |
| **Error Rate** | <1% | >5% | >2% |

### Scalability Testing Framework

```python
# tests/scalability/load_test.py
import asyncio
import aiohttp
import time
from dataclasses import dataclass
from typing import List, Dict

@dataclass
class LoadTestConfig:
    """Load test configuration"""
    concurrent_users: int = 100
    test_duration_seconds: int = 300
    ramp_up_seconds: int = 60
    target_rps: int = 1000
    
class ScalabilityTester:
    """Scalability and load testing framework"""
    
    def __init__(self, config: LoadTestConfig):
        self.config = config
        
    async def run_load_test(self, base_url: str) -> Dict:
        """Run comprehensive load test"""
        
        # Ramp up phase
        await self._ramp_up_phase(base_url)
        
        # Sustained load phase
        results = await self._sustained_load_phase(base_url)
        
        # Ramp down phase
        await self._ramp_down_phase(base_url)
        
        return results
    
    async def _sustained_load_phase(self, base_url: str) -> Dict:
        """Run sustained load phase"""
        start_time = time.time()
        end_time = start_time + self.config.test_duration_seconds
        
        results = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "response_times": [],
            "error_types": {}
        }
        
        async with aiohttp.ClientSession() as session:
            while time.time() < end_time:
                # Create batch of concurrent requests
                tasks = []
                for i in range(self.config.concurrent_users):
                    task = self._make_prediction_request(session, base_url, i)
                    tasks.append(task)
                
                # Execute batch
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Process results
                for result in batch_results:
                    results["total_requests"] += 1
                    
                    if isinstance(result, dict) and result.get("success"):
                        results["successful_requests"] += 1
                        results["response_times"].append(result["response_time"])
                    else:
                        results["failed_requests"] += 1
                        error_type = type(result).__name__ if isinstance(result, Exception) else "unknown"
                        results["error_types"][error_type] = results["error_types"].get(error_type, 0) + 1
                
                # Wait before next batch to control RPS
                await asyncio.sleep(1.0)
        
        return self._calculate_metrics(results)
    
    async def _make_prediction_request(self, session: aiohttp.ClientSession, 
                                     base_url: str, request_id: int) -> Dict:
        """Make single prediction request"""
        start_time = time.time()
        
        try:
            async with session.post(
                f"{base_url}/api/v1/predict",
                json={
                    "game_id": f"load_test_{request_id}",
                    "model_name": "lightgbm_v2"
                },
                headers={"Authorization": "Bearer test_token"},
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                response_time = (time.time() - start_time) * 1000  # ms
                
                return {
                    "success": response.status == 200,
                    "status_code": response.status,
                    "response_time": response_time,
                    "request_id": request_id
                }
                
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "response_time": (time.time() - start_time) * 1000,
                "request_id": request_id
            }
    
    def _calculate_metrics(self, results: Dict) -> Dict:
        """Calculate performance metrics"""
        response_times = results["response_times"]
        
        if response_times:
            response_times.sort()
            p50 = response_times[int(0.5 * len(response_times))]
            p95 = response_times[int(0.95 * len(response_times))]
            p99 = response_times[int(0.99 * len(response_times))]
            avg = sum(response_times) / len(response_times)
        else:
            p50 = p95 = p99 = avg = 0
        
        success_rate = (results["successful_requests"] / results["total_requests"]) * 100 if results["total_requests"] > 0 else 0
        
        return {
            "total_requests": results["total_requests"],
            "success_rate_percent": success_rate,
            "avg_response_time_ms": avg,
            "p50_response_time_ms": p50,
            "p95_response_time_ms": p95,
            "p99_response_time_ms": p99,
            "max_response_time_ms": max(response_times) if response_times else 0,
            "requests_per_second": results["total_requests"] / self.config.test_duration_seconds,
            "error_distribution": results["error_types"]
        }
```

## 🎯 Scalability Roadmap

### Phase 1: Basic Horizontal Scaling (Weeks 1-2)
- [ ] Containerize all services
- [ ] Deploy Kubernetes cluster
- [ ] Implement basic load balancing
- [ ] Configure horizontal pod autoscaling
- [ ] Set up Redis cluster

### Phase 2: Advanced Scaling (Weeks 3-4)
- [ ] Implement service mesh (Istio)
- [ ] Deploy PostgreSQL cluster
- [ ] Advanced load balancing strategies
- [ ] Distributed caching implementation
- [ ] Performance monitoring setup

### Phase 3: Production Optimization (Weeks 5-6)
- [ ] Blue-green deployment setup
- [ ] Canary deployment implementation
- [ ] Custom auto-scaling logic
- [ ] Comprehensive monitoring
- [ ] Load testing and optimization

### Phase 4: Enterprise Scale (Weeks 7-8)
- [ ] Multi-region deployment
- [ ] Disaster recovery setup
- [ ] Advanced security hardening
- [ ] Cost optimization
- [ ] Documentation and training

---

## 📚 Summary

This scalability strategy provides:

1. **Horizontal Scaling Architecture**: Microservices-based distributed system
2. **Load Balancing**: Multi-layer load balancing with intelligent routing
3. **Auto-Scaling**: Kubernetes HPA and custom scaling logic
4. **Distributed Data**: Redis cluster and PostgreSQL replication
5. **Performance Optimization**: Caching, connection pooling, and optimization
6. **Deployment Strategies**: Blue-green and canary deployments
7. **Monitoring & Testing**: Comprehensive scalability monitoring and testing

**Expected Capacity**: 10,000 concurrent users, 5,000 RPS, 99.9% availability with sub-500ms response times.

---

**Document Version**: 1.0  
**Last Updated**: 2025-01-30  
**Next Review**: 2025-04-30  
**Classification**: Internal Use Only