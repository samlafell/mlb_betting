# MLB Betting System - Working Endpoints Guide

**Corrected guide showing ONLY the endpoints that actually work right now**

## 🚨 Important Note
The previous documentation included many endpoints that are **designed but not yet implemented**. This guide shows only what's currently working in your system.

---

## ✅ Working API Endpoints

### System Health (✅ Works)
```bash
curl http://localhost:8000/health
```
**Returns**: System health status with service checks

### API Information (✅ Works)  
```bash
curl http://localhost:8000/
```
**Returns**: List of available endpoints

### Today's Predictions (✅ Works)
```bash
curl "http://localhost:8000/api/v1/predictions/today"

# With confidence filter
curl "http://localhost:8000/api/v1/predictions/today?min_confidence=0.7"
```
**Returns**: Array of today's betting predictions (currently returns mock data)

### Active Models (✅ Works)
```bash
curl http://localhost:8000/api/v1/models/active
```
**Returns**: List of currently active models with basic performance metrics

---

## ❌ Non-Working Endpoints (Documented but Not Implemented)

### Dashboard (❌ Not Available at port 8000)
```bash
# This DOESN'T work:
curl http://localhost:8000/dashboard
# Returns: {"detail":"Not Found"}
```

### Model Leaderboard (❌ Not Implemented)
```bash
# This DOESN'T work:
curl "http://localhost:8000/api/v1/models/leaderboard"
# Returns: {"error":"Model leaderboard not found","status_code":404}
```

### Individual Model Info (❌ Not Implemented)
```bash
# This DOESN'T work:
curl "http://localhost:8000/api/v1/models/lightgbm_v1"
# Returns: {"error":"Model lightgbm_v1 not found","status_code":404}
```

### Model Performance History (❌ Not Implemented)
```bash
# This DOESN'T work:
curl "http://localhost:8000/api/v1/models/lightgbm_v1/performance"
```

---

## 🔧 How to Start the Monitoring Dashboard

The dashboard is a **separate service** that needs to be started independently:

```bash
# Start the monitoring dashboard (runs on different port)
uv run -m src.interfaces.cli monitoring dashboard
```

This will start the dashboard on a different port (likely 8001 or 8080), not port 8000.

---

## 📊 What Actually Works for Daily Use

### 1. Check System Health
```bash
curl http://localhost:8000/health
```

### 2. Get Today's Predictions  
```bash
curl "http://localhost:8000/api/v1/predictions/today"
```

**Sample Working Response**:
```json
[
  {
    "game_id": "game_1",
    "model_name": "lightgbm_v1",
    "model_version": "1.0",
    "prediction_timestamp": "2025-07-31T02:54:33.450589",
    "total_over_probability": 0.65,
    "total_over_binary": 1,
    "total_over_confidence": 0.72,
    "home_ml_probability": 0.58,
    "home_ml_binary": 1,
    "home_ml_confidence": 0.68,
    "betting_recommendations": {
      "total_over": {
        "expected_value": 0.15,
        "kelly_fraction": 0.08,
        "recommended_bet_size": 5.0,
        "min_odds": -110
      }
    },
    "confidence_threshold_met": true,
    "risk_level": "medium"
  }
]
```

### 3. Check Active Models
```bash
curl http://localhost:8000/api/v1/models/active
```

**Sample Working Response**:
```json
[
  {
    "model_name": "lightgbm_total_over_v1",
    "model_version": "1.0",
    "model_type": "lightgbm",
    "is_active": true,
    "created_at": "2025-07-24T02:51:43.617900",
    "recent_accuracy": 0.67,
    "recent_roi": 8.5,
    "total_predictions": 150,
    "target_variable": "total_over"
  }
]
```

---

## 🛠️ Working CLI Commands

### ML Commands (✅ Work)
```bash
# List available ML commands
uv run -m src.interfaces.cli ml --help

# Check ML system health  
uv run -m src.interfaces.cli ml health

# Test ML infrastructure connections
uv run -m src.interfaces.cli ml test-connection

# List experiments
uv run -m src.interfaces.cli ml list-experiments
```

### Data Commands (✅ Work)
```bash
# Check data collection status
uv run -m src.interfaces.cli data status

# Test data connections
uv run -m src.interfaces.cli data test --source action_network --real
```

### Monitoring Commands (✅ Work)
```bash
# Start dashboard (separate service)
uv run -m src.interfaces.cli monitoring dashboard

# Check system status
uv run -m src.interfaces.cli monitoring status
```

---

## 🎯 Realistic Daily Workflow

Given what actually works, here's a practical daily routine:

### Morning Check (What Works Now)
```bash
# 1. Check if system is up
curl http://localhost:8000/health

# 2. Get today's predictions
curl "http://localhost:8000/api/v1/predictions/today?min_confidence=0.7" | jq '.'

# 3. Check which models are active
curl http://localhost:8000/api/v1/models/active | jq '.[] | {name: .model_name, roi: .recent_roi, accuracy: .recent_accuracy}'
```

### Using CLI Tools
```bash
# Check data freshness
uv run -m src.interfaces.cli data status

# Check ML system health
uv run -m src.interfaces.cli ml health

# Start monitoring dashboard (if needed)
uv run -m src.interfaces.cli monitoring dashboard
```

---

## 🚧 What Needs to Be Implemented

Based on this analysis, here's what's missing:

### Missing API Endpoints
1. `/api/v1/models/leaderboard` - Model performance ranking
2. `/api/v1/models/{model_name}` - Individual model details  
3. `/api/v1/models/{model_name}/performance` - Performance history
4. `/dashboard` - Web dashboard (needs to be mounted)
5. Single game prediction endpoints
6. Batch prediction endpoints

### Missing Dashboard
- The dashboard exists as code but isn't accessible at port 8000
- Needs to be started as separate service
- Dashboard HTML template exists but isn't served

---

## 💡 Next Steps to Fix

1. **Start the monitoring dashboard service separately**:
   ```bash
   uv run -m src.interfaces.cli monitoring dashboard
   ```

2. **Check if more endpoints become available after full system startup**

3. **The current predictions API returns mock data** - needs real model integration

4. **Many documented features exist in code but aren't wired up properly**

---

## 📚 Updated Documentation

I recommend using this guide instead of the previous ones until the missing endpoints are implemented. The system has a solid foundation but many advanced features are still in development.

**Working Features**: ✅ Health checks, ✅ Basic predictions, ✅ Model status, ✅ CLI tools
**Planned Features**: 🚧 Dashboard, 🚧 Model leaderboards, 🚧 Performance analytics, 🚧 Real predictions

---

*Last Updated: July 31, 2025 - Based on actual endpoint testing*