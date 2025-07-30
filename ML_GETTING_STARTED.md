# MLB ML Prediction System - Getting Started

This guide helps you set up the containerized ML prediction system on your Mac Mini M2.

## 🚀 Quick Start

### 1. Prerequisites

**Required:**
- Docker Desktop for Mac (with 6GB+ memory allocated)
- PostgreSQL running locally (your existing setup)
- Python 3.11+ with uv

**Check your setup:**
```bash
docker --version          # Should be 20.10+
docker-compose --version  # Should be 2.0+
psql --version            # Should connect to your existing DB
```

### 2. Environment Setup

**Copy environment template:**
```bash
cp .env.example .env
```

**Edit `.env` with your database credentials:**
```bash
# Update these with your actual PostgreSQL settings
POSTGRES_HOST=localhost  
POSTGRES_PORT=5432
POSTGRES_DB=mlb_betting
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password
```

### 3. Initialize ML Database Tables

**Setup the ML database schema:**
```bash
# Run the database setup script
uv run docker/scripts/setup_ml_database.py
```

This creates the ML tables in your existing PostgreSQL database:
- `curated.enhanced_games`
- `curated.ml_predictions` 
- `curated.ml_model_performance`
- `curated.ml_experiments`

### 4. Start the Services

**Launch all services:**
```bash
docker-compose up -d
```

**Check service health:**
```bash
# Wait ~60 seconds for all services to start, then:
curl http://localhost/health
```

Expected response:
```json
{
  "service": "mlb-ml-prediction-api",
  "status": "healthy",
  "checks": {
    "redis": {"status": "healthy"},
    "database": {"status": "healthy"},
    "mlflow": {"status": "healthy"}
  }
}
```

## 📡 API Endpoints

### Health & Status
```bash
# Health check
curl http://localhost/health

# API information
curl http://localhost/
```

### Predictions
```bash
# Single game prediction
curl http://localhost/api/v1/predict/game_123

# Today's predictions
curl http://localhost/api/v1/predictions/today

# Batch predictions
curl -X POST http://localhost/api/v1/predict/batch \
  -H "Content-Type: application/json" \
  -d '{"game_ids": ["game_123", "game_456"]}'
```

### Models
```bash
# Active models
curl http://localhost/api/v1/models/active

# Model performance
curl http://localhost/api/v1/models/lightgbm_v1/performance

# Model leaderboard
curl http://localhost/api/v1/models/leaderboard?metric=roi_percentage
```

## 🔧 Service Management

### View Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f fastapi
docker-compose logs -f redis
docker-compose logs -f mlflow
```

### Service Status
```bash
docker-compose ps
```

### Stop Services
```bash
# Stop all services
docker-compose down

# Stop and remove volumes (reset)
docker-compose down -v
```

### Restart Services
```bash
# Restart all
docker-compose restart

# Restart specific service
docker-compose restart fastapi
```

## 📊 Monitoring

### Service URLs
- **API Gateway**: http://localhost (Nginx)
- **FastAPI Docs**: http://localhost/docs (if DEBUG=true)
- **MLflow UI**: http://localhost:5000
- **Redis**: localhost:6379

### Resource Usage
```bash
# Check container resource usage
docker stats

# Check disk usage
docker system df
```

### Memory Management
Services are configured with memory limits:
- Redis: 512MB limit
- MLflow: 256MB limit  
- FastAPI: 512MB limit
- Nginx: 128MB limit
- **Total**: ~1.4GB containers + your existing PostgreSQL

## 🧪 Testing the System

### 1. Test Health Endpoints
```bash
curl http://localhost/health
curl http://localhost/health/ready
curl http://localhost/health/live
```

### 2. Test Prediction API
```bash
# Get prediction for a game
curl http://localhost/api/v1/predict/game_123

# Expected response structure:
{
  "game_id": "game_123",
  "model_name": "lightgbm_v1",
  "total_over_probability": 0.65,
  "betting_recommendations": {...},
  "confidence_threshold_met": true
}
```

### 3. Test MLflow Integration
```bash
# Visit MLflow UI
open http://localhost:5000

# Should show experiments and runs
```

## ⚠️ Troubleshooting

### Common Issues

**1. Port Conflicts**
```bash
# Check what's using ports
lsof -i :80    # Nginx
lsof -i :5000  # MLflow
lsof -i :6379  # Redis
```

**2. Database Connection Issues**
```bash
# Test database connection
psql -h localhost -p 5432 -U your_username -d mlb_betting -c "SELECT 1;"

# Check if ML tables exist
psql -h localhost -p 5432 -U your_username -d mlb_betting -c "\\dt curated.ml_*"
```

**3. Memory Issues**
```bash
# Check Docker memory allocation
docker system info | grep -i memory

# Increase Docker Desktop memory to 6GB+ in preferences
```

**4. Service Won't Start**
```bash
# Check logs for specific service
docker-compose logs fastapi

# Common issues:
# - Missing .env file
# - Wrong database credentials
# - Port conflicts
```

### Reset Everything
```bash
# Nuclear option - reset all containers and data
docker-compose down -v
docker system prune -f
docker-compose up -d
```

## 🔄 Next Steps (Phase 2)

Phase 1 gives you the foundation. For Phase 2, you'll add:

1. **Real Model Training**: Replace stub predictions with actual LightGBM models
2. **Feature Engineering**: Connect to your existing data collectors
3. **Batch Scoring**: Automated daily predictions
4. **Performance Monitoring**: Real model performance tracking

## 📚 Additional Resources

- **Docker Configuration**: `docker/README.md`
- **MLflow Integration**: `src/ml/services/mlflow_integration.py`
- **API Documentation**: http://localhost/docs (when DEBUG=true)
- **Database Schema**: `sql/migrations/011-014_*.sql`

## 🆘 Getting Help

If you run into issues:

1. Check the logs: `docker-compose logs -f`
2. Verify your `.env` file has correct database credentials
3. Ensure PostgreSQL is running and accessible
4. Check Docker Desktop has sufficient memory allocated

The system is designed to be lightweight and work well on your Mac Mini M2 with 8GB RAM!