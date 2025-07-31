# MLB Betting API - Quick Reference

**Essential API endpoints for getting betting recommendations and monitoring models**

## 🚀 Base URL
```
http://localhost:8000
```

## 📋 Quick Navigation
- [Health & Status](#health--status)
- [Getting Predictions](#getting-predictions)
- [Model Information](#model-information)
- [Performance Metrics](#performance-metrics)
- [Response Examples](#response-examples)

---

## 🏥 Health & Status

### System Health Check
```bash
GET /health
curl http://localhost:8000/health
```
**Use case**: Check if system is running before making other requests

### API Information
```bash
GET /
curl http://localhost:8000/
```
**Use case**: Get API version and available endpoints

---

## 🎯 Getting Predictions

### Today's Betting Recommendations ⭐ MOST IMPORTANT
```bash
GET /api/v1/predictions/today
curl http://localhost:8000/api/v1/predictions/today

# With confidence filter (RECOMMENDED)
curl "http://localhost:8000/api/v1/predictions/today?min_confidence=0.7"

# Filter by specific model
curl "http://localhost:8000/api/v1/predictions/today?model_name=lightgbm_v2.1"
```
**Use case**: Get all betting recommendations for today's games

### Single Game Prediction
```bash
GET /api/v1/predict/{game_id}
curl http://localhost:8000/api/v1/predict/2025-01-31-NYY-BOS
```

**Use case**: Get prediction for a specific game

### Detailed Single Prediction (with explanation)
```bash
POST /api/v1/predict
curl -X POST http://localhost:8000/api/v1/predict \
  -H "Content-Type: application/json" \
  -d '{
    "game_id": "2025-01-31-NYY-BOS",
    "include_explanation": true
  }'
```
**Use case**: Get detailed prediction with model reasoning

### Batch Predictions (Multiple Games)
```bash
POST /api/v1/predict/batch
curl -X POST http://localhost:8000/api/v1/predict/batch \
  -H "Content-Type: application/json" \
  -d '{
    "game_ids": ["2025-01-31-NYY-BOS", "2025-01-31-LAD-SF"],
    "include_explanation": false
  }'
```
**Use case**: Get predictions for multiple games at once (max 50)

---

## 📊 Model Information

### List Active Models
```bash
GET /api/v1/models/active
curl http://localhost:8000/api/v1/models/active
```
**Use case**: See what models are currently running and their basic stats

### Model Leaderboard ⭐ IMPORTANT
```bash
GET /api/v1/models/leaderboard
# By ROI (best for betting decisions)
curl "http://localhost:8000/api/v1/models/leaderboard?metric=roi_percentage&days=30"

# By accuracy (technical assessment)
curl "http://localhost:8000/api/v1/models/leaderboard?metric=accuracy&days=30"

# Limit results
curl "http://localhost:8000/api/v1/models/leaderboard?metric=roi_percentage&limit=3"
```
**Use case**: Find the best performing models to guide betting decisions

### Detailed Model Information
```bash
GET /api/v1/models/{model_name}
curl http://localhost:8000/api/v1/models/lightgbm_v2.1

# Specific version
curl "http://localhost:8000/api/v1/models/lightgbm_v2.1?model_version=v2.1"
```
**Use case**: Get comprehensive information about a specific model

---

## 📈 Performance Metrics

### Model Performance History
```bash
GET /api/v1/models/{model_name}/performance
curl "http://localhost:8000/api/v1/models/lightgbm_v2.1/performance?days=30"

# Filter by prediction type
curl "http://localhost:8000/api/v1/models/lightgbm_v2.1/performance?prediction_type=total_over&days=7"
```
**Use case**: Analyze model performance over time

### Recent Model Predictions
```bash
GET /api/v1/models/{model_name}/recent-predictions
curl "http://localhost:8000/api/v1/models/lightgbm_v2.1/recent-predictions?days=7&limit=20"
```
**Use case**: Review recent predictions to assess current model performance

---

## 📝 Response Examples

### Today's Predictions Response
```json
[
  {
    "game_id": "2025-01-31-NYY-BOS",
    "model_name": "lightgbm_v2.1",
    "model_version": "v2.1",
    "prediction_timestamp": "2025-01-31T14:30:00Z",
    
    // Total Over/Under Prediction
    "total_over_probability": 0.73,
    "total_over_binary": 1,          // 1 = OVER, 0 = UNDER
    "total_over_confidence": 0.81,
    
    // Moneyline Prediction  
    "home_ml_probability": 0.45,
    "home_ml_binary": 0,             // 1 = HOME wins, 0 = AWAY wins
    "home_ml_confidence": 0.67,
    
    // Spread Prediction
    "home_spread_probability": 0.58,
    "home_spread_binary": 1,         // 1 = HOME covers, 0 = AWAY covers
    "home_spread_confidence": 0.72,
    
    // Betting Guidance
    "betting_recommendations": {
      "recommended_bets": ["total_over", "home_spread"],
      "confidence_level": "high",
      "risk_level": "medium"
    },
    "confidence_threshold_met": true,
    "risk_level": "medium"
  }
]
```

### Model Leaderboard Response
```json
[
  {
    "model_name": "lightgbm_v2.1",
    "model_version": "v2.1",
    "roi_percentage": 15.7,
    "win_rate": 54.2,
    "total_predictions": 247,
    "accuracy": 0.634,
    "rank": 1
  },
  {
    "model_name": "ensemble_v1.0",
    "model_version": "v1.0", 
    "roi_percentage": 12.3,
    "win_rate": 52.8,
    "total_predictions": 198,
    "accuracy": 0.618,
    "rank": 2
  }
]
```

### Model Performance Response
```json
[
  {
    "model_name": "lightgbm_v2.1",
    "model_version": "v2.1",
    "prediction_type": "total_over",
    "evaluation_period_start": "2025-01-01",
    "evaluation_period_end": "2025-01-31",
    
    // Technical Metrics
    "accuracy": 0.634,
    "precision_score": 0.642,
    "recall_score": 0.618,
    "f1_score": 0.630,
    "roc_auc": 0.672,
    
    // Business Metrics
    "total_bets_made": 247,
    "winning_bets": 134,
    "hit_rate": 54.2,
    "roi_percentage": 15.7,
    
    // Risk Metrics
    "sharpe_ratio": 1.34,
    "max_drawdown_pct": -8.2
  }
]
```

---

## 🎲 Quick Decision Guide

### Reading Predictions
- **Binary = 1**: Bet YES (Over, Home win, Home covers spread)
- **Binary = 0**: Bet NO (Under, Away win, Away covers spread)
- **Confidence > 0.7**: Safe to bet
- **Confidence < 0.6**: Avoid betting

### Model Selection Priority
1. **Highest ROI** (>10% is good)
2. **Recent performance** (last 7-30 days)
3. **High confidence predictions** (>0.7)
4. **Reasonable sample size** (>50 predictions)

### Common Query Patterns
```bash
# Morning routine: Get today's best bets
curl "http://localhost:8000/api/v1/predictions/today?min_confidence=0.75"

# Check best model for this week  
curl "http://localhost:8000/api/v1/models/leaderboard?metric=roi_percentage&days=7&limit=1"

# Verify specific game prediction
curl http://localhost:8000/api/v1/predict/GAME_ID_HERE
```

---

## ⚡ Response Codes

- **200**: Success
- **404**: Game/Model not found
- **400**: Bad request (invalid parameters)  
- **500**: Server error (check system health)
- **429**: Rate limited (too many requests)

---

## 🛠️ Testing Commands

```bash
# Test basic connectivity
curl -f http://localhost:8000/health || echo "System down"

# Test predictions endpoint
curl -f "http://localhost:8000/api/v1/predictions/today?limit=1" || echo "No predictions available"

# Test model endpoint
curl -f http://localhost:8000/api/v1/models/active || echo "Models not available"
```

---

*For detailed examples and troubleshooting, see [ML_USER_GUIDE.md](ML_USER_GUIDE.md)*