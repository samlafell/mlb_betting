# MLB Betting System - User Guide

**Quick start guide for evaluating models, monitoring performance, and getting betting recommendations**

## 🚀 Quick Start

### System Access
- **Web Dashboard**: [http://localhost:8000/dashboard](http://localhost:8000/dashboard) - Real-time monitoring interface
- **API Documentation**: [http://localhost:8000/docs](http://localhost:8000/docs) - Interactive API docs
- **Health Check**: [http://localhost:8000/health](http://localhost:8000/health) - System status

### Prerequisites
Make sure the ML services are running:
```bash
# Check system health
curl http://localhost:8000/health

# Expected response: {"status": "healthy", "service": "mlb-ml-prediction-api"}
```

---

## 📊 Understanding Model Performance

### Technical Metrics (For Algorithm Assessment)
- **Accuracy**: Overall correctness (0.0-1.0, higher is better)
- **Precision**: True positive rate (0.0-1.0, higher is better)  
- **Recall**: How many actual positives were found (0.0-1.0, higher is better)
- **F1 Score**: Balance of precision and recall (0.0-1.0, higher is better)
- **ROC AUC**: Model's ability to distinguish classes (0.5-1.0, higher is better)

### Business Metrics (For Betting Decisions)
- **ROI (Return on Investment)**: Percentage profit/loss (-100% to +∞, positive is profitable)
- **Win Rate**: Percentage of winning bets (0-100%, higher is better)  
- **Hit Rate**: Successful predictions percentage (0-100%, higher is better)
- **Sharpe Ratio**: Risk-adjusted returns (higher is better, >1.0 is good)
- **Max Drawdown**: Largest loss from peak (-100% to 0%, closer to 0% is better)

### Quick Performance Assessment
**Excellent Model**: ROI >15%, Win Rate >55%, ROC AUC >0.65
**Good Model**: ROI 5-15%, Win Rate 52-55%, ROC AUC 0.60-0.65  
**Poor Model**: ROI <5%, Win Rate <52%, ROC AUC <0.60

---

## 🔍 Monitoring Active Models

### View All Active Models
```bash
# CLI method
uv run -m src.interfaces.cli ml list-experiments

# API method
curl http://localhost:8000/api/v1/models/active
```

**What you'll see:**
```json
[
  {
    "model_name": "lightgbm_v2.1",
    "model_version": "v2.1", 
    "is_active": true,
    "recent_accuracy": 0.634,
    "recent_roi": 12.5,
    "total_predictions": 1847,
    "description": "Enhanced LightGBM with temporal features"
  }
]
```

### Get Detailed Model Information
```bash
# CLI method  
uv run -m src.interfaces.cli ml experiment-info lightgbm_v2.1

# API method
curl http://localhost:8000/api/v1/models/lightgbm_v2.1
```

### View Model Leaderboard (Best Performers)
```bash
# By ROI (recommended for betting)
curl "http://localhost:8000/api/v1/models/leaderboard?metric=roi_percentage&days=30"

# By Accuracy (for technical assessment)  
curl "http://localhost:8000/api/v1/models/leaderboard?metric=accuracy&days=30"
```

---

## 🎯 Getting Betting Recommendations

### Today's Recommendations (Most Common Use Case)
```bash
# Get all predictions for today
curl http://localhost:8000/api/v1/predictions/today

# Get only high-confidence predictions (recommended)
curl "http://localhost:8000/api/v1/predictions/today?min_confidence=0.7"
```

**Sample Response:**
```json
[
  {
    "game_id": "2025-01-31-NYY-BOS",
    "model_name": "lightgbm_v2.1",
    "prediction_timestamp": "2025-01-31T14:30:00Z",
    "total_over_probability": 0.73,
    "total_over_binary": 1,
    "total_over_confidence": 0.81,
    "home_ml_probability": 0.45,
    "home_ml_binary": 0, 
    "home_ml_confidence": 0.67,
    "betting_recommendations": {
      "recommended_bets": ["total_over"],
      "confidence_level": "high",
      "risk_level": "medium"
    },
    "confidence_threshold_met": true
  }
]
```

**How to Read This:**
- **total_over_binary = 1**: Model predicts OVER the total
- **total_over_confidence = 0.81**: High confidence (>0.7 is good)
- **home_ml_binary = 0**: Model predicts AWAY team wins
- **recommended_bets**: Only bet on recommendations with high confidence

### Single Game Prediction
```bash
# Get prediction for specific game
curl http://localhost:8000/api/v1/predict/2025-01-31-NYY-BOS

# With detailed explanation
curl -X POST http://localhost:8000/api/v1/predict \
  -H "Content-Type: application/json" \
  -d '{"game_id": "2025-01-31-NYY-BOS", "include_explanation": true}'
```

### Batch Predictions (Multiple Games)
```bash
curl -X POST http://localhost:8000/api/v1/predict/batch \
  -H "Content-Type: application/json" \
  -d '{
    "game_ids": ["2025-01-31-NYY-BOS", "2025-01-31-LAD-SF"],
    "include_explanation": false
  }'
```

---

## 📈 Using the Web Dashboard

### Accessing the Dashboard
1. Open [http://localhost:8000/dashboard](http://localhost:8000/dashboard)
2. You'll see real-time system status and metrics

### Dashboard Sections

#### System Health
- **Green**: All systems operational
- **Yellow**: Some warnings, system still functional  
- **Red**: Critical issues, investigate immediately

#### Active Pipelines
- Shows data collection and model training status
- **Running**: Currently collecting data or training
- **Success**: Completed successfully
- **Failed**: Needs attention

#### Model Performance Cards
- Real-time performance metrics
- Recent prediction accuracy  
- ROI trends over time

#### Recent Predictions
- Latest betting recommendations
- Confidence levels
- Success rates

---

## 🛠️ Common Tasks

### Morning Routine: Check Today's Bets
```bash
# 1. Check system health
curl http://localhost:8000/health

# 2. Get today's high-confidence recommendations
curl "http://localhost:8000/api/v1/predictions/today?min_confidence=0.7"

# 3. Review model leaderboard for best performers
curl "http://localhost:8000/api/v1/models/leaderboard?metric=roi_percentage&days=7"
```

### Weekly Review: Evaluate Model Performance
```bash
# 1. List all active models
uv run -m src.interfaces.cli ml list-experiments

# 2. Get detailed performance for best model
curl "http://localhost:8000/api/v1/models/lightgbm_v2.1/performance?days=7"

# 3. Check recent predictions success rate
curl "http://localhost:8000/api/v1/models/lightgbm_v2.1/recent-predictions?days=7"
```

### Model Comparison: Find Best Performer
```bash
# Compare models by ROI over last 30 days
curl "http://localhost:8000/api/v1/models/leaderboard?metric=roi_percentage&days=30&limit=5"

# Compare by accuracy
curl "http://localhost:8000/api/v1/models/leaderboard?metric=accuracy&days=30&limit=5"

# Compare by win rate  
curl "http://localhost:8000/api/v1/models/leaderboard?metric=hit_rate&days=30&limit=5"
```

---

## ⚠️ Betting Guidelines

### Confidence Thresholds
- **High Confidence (>0.8)**: Safe to bet, lower risk
- **Medium Confidence (0.6-0.8)**: Moderate risk, consider position sizing
- **Low Confidence (<0.6)**: Avoid betting, high risk

### ROI Interpretation
- **>20% ROI**: Excellent, but verify sample size
- **10-20% ROI**: Very good, sustainable profits
- **5-10% ROI**: Good, solid performance  
- **0-5% ROI**: Break-even, marginal
- **<0% ROI**: Losing money, avoid this model

### Risk Management
- Never bet more than 2-5% of bankroll per game
- Only bet on predictions with confidence >0.7
- Track your results vs. model predictions
- Diversify across multiple models if available

---

## 🚨 Troubleshooting

### System Not Responding
```bash
# Check if services are running
curl http://localhost:8000/health

# Check system health via CLI
uv run -m src.interfaces.cli ml health

# View service logs
docker-compose logs -f fastapi
```

### No Predictions Available
```bash
# Check if models are active
curl http://localhost:8000/api/v1/models/active

# Check data pipeline status
uv run -m src.interfaces.cli monitoring status

# Manually trigger data collection
uv run -m src.interfaces.cli data collect --source action_network --real
```

### Poor Model Performance
```bash
# Check model performance history
curl "http://localhost:8000/api/v1/models/lightgbm_v2.1/performance?days=30"

# Review recent predictions vs outcomes
curl "http://localhost:8000/api/v1/models/lightgbm_v2.1/recent-predictions?days=7"

# Check if model needs retraining
uv run -m src.interfaces.cli ml experiment-info lightgbm_v2.1
```

### API Authentication Errors
- Check if API key is configured (if using authenticated endpoints)
- Verify you're using the correct endpoints
- Check if rate limiting is applied

---

## 📚 Additional Resources

- **API Documentation**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **Model Training Guide**: [ML_GETTING_STARTED.md](ML_GETTING_STARTED.md)
- **System Architecture**: [CLAUDE.md](../CLAUDE.md)
- **Monitoring Dashboard**: [http://localhost:8000/dashboard](http://localhost:8000/dashboard)

---

## 💡 Tips for Success

1. **Start Simple**: Begin with today's high-confidence predictions only
2. **Track Performance**: Keep records of your bets vs. predictions  
3. **Use Multiple Metrics**: Don't rely on ROI alone, check win rate and confidence
4. **Monitor System Health**: Check dashboard regularly for issues
5. **Understand the Models**: Learn what each model specializes in
6. **Practice Risk Management**: Never bet more than you can afford to lose

---

*Last Updated: January 31, 2025*
*For technical support, see troubleshooting section or check system logs*