# MLB ML Prediction System - Docker Integration

## How MLflow Connects to Database Tables

The ML system integrates with existing database tables through multiple layers:

### 1. MLflow Backend Configuration
```python
# MLflow uses the SAME PostgreSQL database as your existing system
backend_store_uri = "postgresql://user:pass@localhost:5432/mlb_betting"

# This creates MLflow's internal tables:
# - experiments (MLflow experiment metadata)
# - runs (MLflow run tracking) 
# - metrics (model performance metrics)
# - params (hyperparameters)
# - tags (experiment/run tags)
```

### 2. Our Custom ML Tables (Already Created)
```sql
-- These tables were created by migrations 011-014:
curated.enhanced_games         -- Master games table
curated.ml_predictions         -- Model predictions with betting recommendations
curated.ml_model_performance   -- Model performance tracking
curated.ml_experiments         -- Custom experiment tracking
```

### 3. Integration Flow
```
MLflow Tables          Our Custom Tables
─────────────          ─────────────────
experiments       ←→   curated.ml_experiments (sync experiment metadata)
runs              ←→   curated.ml_predictions (link run_id to predictions)
metrics           ←→   curated.ml_model_performance (aggregate metrics)
```

### 4. Database Connection Architecture
```
┌─────────────────────────────────────────────────────────────┐
│                PostgreSQL Database                          │
│                                                             │
│  MLflow Internal Tables        Our Custom ML Tables         │
│  ├── experiments               ├── curated.enhanced_games   │
│  ├── runs                      ├── curated.ml_predictions   │
│  ├── metrics                   ├── curated.ml_model_performance│
│  ├── params                    └── curated.ml_experiments   │
│  └── tags                                                   │
│                                                             │
│  Existing Betting Tables                                    │
│  ├── staging.action_network_odds_historical                 │
│  ├── raw_data.* (SBD, VSIN, etc.)                         │
│  └── curated.* (other tables)                             │
└─────────────────────────────────────────────────────────────┘
                            ↑
                    Same Database Connection
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                   ML Services                               │
│  ┌─────────────────┐  ┌─────────────────┐                  │
│  │  MLflow Service │  │ Prediction API  │                  │
│  │                 │  │                 │                  │
│  │ • Experiments   │  │ • Game predictions                 │
│  │ • Model tracking│  │ • Betting recommendations         │
│  │ • Metrics       │  │ • Kelly Criterion                 │
│  └─────────────────┘  └─────────────────┘                  │
└─────────────────────────────────────────────────────────────┘
```

## Key Integration Points

1. **Same Database**: MLflow uses your existing PostgreSQL database
2. **Shared Connection**: All services use the same database connection configuration
3. **Linked Data**: MLflow run_id links to our curated.ml_predictions table
4. **Performance Tracking**: MLflow metrics sync to curated.ml_model_performance
5. **Experiment Management**: Custom experiment data in curated.ml_experiments

## Environment Variables

MLflow automatically connects using these variables:
```bash
# Same database as your existing system
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=mlb_betting
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password

# MLflow configuration
MLFLOW_BACKEND_STORE_URI=postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}
MLFLOW_DEFAULT_ARTIFACT_ROOT=./mlruns
```

This design ensures MLflow integrates seamlessly with your existing database while maintaining separation between MLflow's internal tracking and your custom ML business logic.