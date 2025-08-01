# MLB Betting CLI - Quick Reference

**Essential command-line tools for model management and system monitoring**

## 🚀 Prerequisites
All commands should be run from the project root directory using `uv`:
```bash
cd /path/to/mlb_betting_program
uv sync  # Ensure dependencies are installed
```

## 📋 Quick Navigation
- [System Health](#system-health)
- [Model Management](#model-management)
- [Making Predictions](#making-predictions)
- [Data Collection](#data-collection)
- [Monitoring](#monitoring)

---

## 🏥 System Health

### Check ML System Health ⭐ START HERE
```bash
uv run -m src.interfaces.cli ml health
```
**Use case**: Verify all ML services (MLflow, Redis, Database, FastAPI) are running

### Test Infrastructure Connections
```bash
uv run -m src.interfaces.cli ml test-connection
```
**Use case**: Detailed connection test for each service with troubleshooting info

### General System Health
```bash
uv run -m src.interfaces.cli monitoring status
```
**Use case**: Overall system health including data pipelines

---

## 🤖 Model Management

### List All ML Experiments ⭐ IMPORTANT
```bash
uv run -m src.interfaces.cli ml list-experiments

# Include archived experiments
uv run -m src.interfaces.cli ml list-experiments --include-archived
```
**Use case**: See all available models and their current status

### Get Detailed Model Information
```bash
uv run -m src.interfaces.cli ml experiment-info MODEL_NAME

# Example
uv run -m src.interfaces.cli ml experiment-info lightgbm_v2.1
```
**Use case**: Get comprehensive details about a specific model's performance

### Create New Experiment
```bash
uv run -m src.interfaces.cli ml create-experiment "my_new_model" \
  --description "Testing new features" \
  --model-type classification \
  --target total_over \
  --start-date 2025-01-01 \
  --end-date 2025-01-31
```
**Use case**: Set up a new model experiment

### Train a Model
```bash
uv run -m src.interfaces.cli ml train EXPERIMENT_NAME \
  --model-type lightgbm \
  --target total_over \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --test-size 0.2
```
**Use case**: Train a model using existing experiment setup

### Evaluate Model Performance
```bash
uv run -m src.interfaces.cli ml evaluate EXPERIMENT_NAME \
  --start-date 2025-01-15 \
  --end-date 2025-01-31 \
  --target total_over
```
**Use case**: Assess model performance on historical data

---

## 🔮 Making Predictions

### Predict Single Game
```bash
uv run -m src.interfaces.cli ml predict \
  --game-id "2025-01-31-NYY-BOS" \
  --model lightgbm_v2.1
```
**Use case**: Get prediction for a specific game

### Predict Games for Specific Date
```bash
uv run -m src.interfaces.cli ml predict \
  --date "2025-01-31" \
  --model lightgbm_v2.1 \
  --batch-size 10
```
**Use case**: Get predictions for all games on a specific date

### Predict Using Best Model
```bash
# Let system choose best performing model
uv run -m src.interfaces.cli ml predict --date today
```
**Use case**: Get predictions using the currently best-performing model

---

## 📊 Data Collection

### Check Data Collection Status ⭐ IMPORTANT
```bash
uv run -m src.interfaces.cli data status

# Detailed status with health metrics
uv run -m src.interfaces.cli data status --detailed
```
**Use case**: See current data pipeline status and last collection times

### Collect Fresh Data
```bash
# Action Network (primary source)
uv run -m src.interfaces.cli data collect --source action_network --real

# VSIN data
uv run -m src.interfaces.cli data collect --source vsin --real

# SBD data  
uv run -m src.interfaces.cli data collect --source sbd --real

# All sources
uv run -m src.interfaces.cli data collect --source all --real
```
**Use case**: Manually trigger data collection to ensure fresh data for predictions

### Test Data Collection
```bash
# Test without storing data
uv run -m src.interfaces.cli data test --source action_network --real
```
**Use case**: Verify data collection is working without affecting database

---

## 📈 Monitoring

### Start Real-time Dashboard ⭐ RECOMMENDED
```bash
uv run -m src.interfaces.cli monitoring dashboard
```
**Use case**: Launch web dashboard for visual monitoring (opens in browser)

### Live Terminal Monitoring
```bash
uv run -m src.interfaces.cli monitoring live
```
**Use case**: Real-time system monitoring in terminal

### Check System Performance
```bash
uv run -m src.interfaces.cli monitoring performance --hours 24
```
**Use case**: Analyze system performance over the last 24 hours

### Health Check Specific Collector
```bash
uv run -m src.interfaces.cli monitoring health-check --collector vsin
uv run -m src.interfaces.cli monitoring health-check --collector action_network
```
**Use case**: Test specific data collector health

### Manual Pipeline Execution (Break-glass)
```bash
uv run -m src.interfaces.cli monitoring execute
```
**Use case**: Manually trigger complete data pipeline in emergency situations

---

## 🛠️ Common Workflows

### Morning Routine: Check System & Get Predictions
```bash
# 1. Check system health
uv run -m src.interfaces.cli ml health

# 2. Check data freshness
uv run -m src.interfaces.cli data status

# 3. Get today's predictions
uv run -m src.interfaces.cli ml predict --date today

# 4. Review best models
uv run -m src.interfaces.cli ml list-experiments
```

### Weekly Model Review
```bash
# 1. List all experiments with performance
uv run -m src.interfaces.cli ml list-experiments

# 2. Get detailed info for top model
uv run -m src.interfaces.cli ml experiment-info TOP_MODEL_NAME

# 3. Evaluate recent performance
uv run -m src.interfaces.cli ml evaluate TOP_MODEL_NAME \
  --start-date 2025-01-24 --end-date 2025-01-31
```

### Troubleshooting Workflow
```bash
# 1. Check overall system health
uv run -m src.interfaces.cli monitoring status

# 2. Test ML infrastructure
uv run -m src.interfaces.cli ml test-connection

# 3. Check data collection
uv run -m src.interfaces.cli data status --detailed

# 4. Manual data collection if needed
uv run -m src.interfaces.cli data collect --source action_network --real

# 5. Review system logs
uv run -m src.interfaces.cli monitoring performance --hours 1
```

---

## 🎯 Command Output Examples

### Model List Output
```
┌─────┬──────────────────┬─────────┬──────────────┬─────────────┐
│ ID  │ Name             │ Status  │ Best Accuracy│ Created     │
├─────┼──────────────────┼─────────┼──────────────┼─────────────┤
│ 1   │ lightgbm_v2.1    │ active  │ 0.634        │ 2025-01-15  │
│ 2   │ ensemble_v1.0    │ active  │ 0.618        │ 2025-01-20  │
└─────┴──────────────────┴─────────┴──────────────┴─────────────┘

Found 2 experiments
```

### Health Check Output
```
┌──────────┬─────────────┬─────────────────────────────┬──────────────────┐
│ Service  │ Status      │ Details                     │ URL              │
├──────────┼─────────────┼─────────────────────────────┼──────────────────┤
│ Mlflow   │ ✅ Connected│ Found 5 experiments         │ http://localhost:5001│
│ Redis    │ ✅ Connected│ Redis ping successful       │ localhost:6379   │
│ Database │ ✅ Connected│ Found 2 ML experiments      │ PostgreSQL       │
│ Fastapi  │ ✅ Connected│ FastAPI health check passed │ http://localhost:8000│
└──────────┴─────────────┴─────────────────────────────┴──────────────────┘

🎉 All 4 services connected successfully!
```

### Prediction Output
```
┌──────────────────────┬────────────┬─────────┬────────────┐
│ Game ID              │ Total Over │ Home ML │ Confidence │
├──────────────────────┼────────────┼─────────┼────────────┤
│ 2025-01-31-NYY-BOS   │ 0.730      │ 0.450   │ 0.810      │
│ 2025-01-31-LAD-SF    │ 0.620      │ 0.580   │ 0.670      │
└──────────────────────┴────────────┴─────────┴────────────┘
```

---

## ⚠️ Common Issues

### "Command not found"
```bash
# Make sure you're in the project directory
cd /path/to/mlb_betting_program

# Ensure dependencies are installed
uv sync
```

### "No experiments found"
```bash
# Check if ML database is set up
uv run docker/scripts/setup_ml_database.py

# Create a test experiment
uv run -m src.interfaces.cli ml create-experiment "test_model"
```

### "Services not responding"
```bash
# Check if Docker services are running
docker-compose ps

# Start services if needed
docker-compose up -d

# Wait 60 seconds then test again
uv run -m src.interfaces.cli ml health
```

### "No predictions available"
```bash
# Check data collection status
uv run -m src.interfaces.cli data status

# Collect fresh data
uv run -m src.interfaces.cli data collect --source action_network --real

# Check if models are trained
uv run -m src.interfaces.cli ml list-experiments
```

---

## 🔗 Integration with Web Interface

### Launch Dashboard from CLI
```bash
# Start dashboard and open in browser
uv run -m src.interfaces.cli monitoring dashboard

# Dashboard will be available at: http://localhost:8000/dashboard
```

### URLs for Manual Access
- **API Docs**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/health  
- **MLflow UI**: http://localhost:5001
- **Dashboard**: http://localhost:8000/dashboard

---

## 📚 Advanced Usage

### Batch Operations
```bash
# Train multiple models
for target in total_over home_ml home_spread; do
  uv run -m src.interfaces.cli ml train "lightgbm_${target}" --target "$target"
done

# Check multiple model performance
uv run -m src.interfaces.cli ml list-experiments | grep active
```

### Automation Examples
```bash
# Daily prediction script
#!/bin/bash
uv run -m src.interfaces.cli data collect --source action_network --real
uv run -m src.interfaces.cli ml predict --date today > daily_predictions.json

# Weekly model evaluation
#!/bin/bash
WEEK_AGO=$(date -d '7 days ago' +%Y-%m-%d)
TODAY=$(date +%Y-%m-%d)
uv run -m src.interfaces.cli ml evaluate lightgbm_v2.1 \
  --start-date "$WEEK_AGO" --end-date "$TODAY"
```

---

*For web-based alternatives, see [API_QUICK_REFERENCE.md](API_QUICK_REFERENCE.md)*
*For detailed explanations, see [ML_USER_GUIDE.md](ML_USER_GUIDE.md)*