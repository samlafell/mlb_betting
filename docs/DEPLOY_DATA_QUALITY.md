# Quick Deploy: Data Quality Improvements

Since the CLI integration is having some dependency issues, here's the simplest way to deploy the data quality improvements:

## Option 1: Direct SQL Execution (Recommended)

### Step 1: Deploy Sportsbook Mapping System
```bash
# Connect to your PostgreSQL database and run:
psql -h localhost -d mlb_betting -U samlafell -f sql/improvements/01_sportsbook_mapping_system.sql
```

### Step 2: Deploy Data Validation & Completeness Scoring
```bash
# Connect to your PostgreSQL database and run:
psql -h localhost -d mlb_betting -U samlafell -f sql/improvements/02_data_validation_and_completeness.sql
```

## Option 2: Manual SQL Copy-Paste

If you prefer to run the SQL manually:

1. **Open your database client** (pgAdmin, DataGrip, psql, etc.)

2. **Connect to your `mlb_betting` database**

3. **Run Phase 1 SQL**: Copy and paste the contents of `sql/improvements/01_sportsbook_mapping_system.sql`

4. **Run Phase 2 SQL**: Copy and paste the contents of `sql/improvements/02_data_validation_and_completeness.sql`

## Verification

After running both SQL scripts, you can verify the deployment with these queries:

### Check if mapping table was created:
```sql
SELECT COUNT(*) FROM curated.sportsbook_mappings;
-- Should return > 0 (some pre-populated mappings)
```

### Check if data quality views were created:
```sql
SELECT * FROM curated.data_quality_dashboard;
-- Should show quality metrics for all 3 betting lines tables
```

### Check if completeness scoring was added:
```sql
SELECT column_name 
FROM information_schema.columns 
WHERE table_schema = 'core_betting' 
  AND table_name = 'betting_lines_moneyline' 
  AND column_name = 'data_completeness_score';
-- Should return 1 row
```

## Expected Results After Deployment

1. **Sportsbook ID Resolution**: The null sportsbook_id issue should start resolving automatically for new data
2. **Data Quality Scoring**: All new betting lines will get completeness scores
3. **Monitoring Views**: You can track quality improvements using the dashboard views

## Monitor Progress

Check progress with:
```sql
-- Overall quality status
SELECT * FROM curated.data_quality_dashboard;

-- Recent quality trends
SELECT * FROM curated.data_quality_trend 
WHERE quality_date >= CURRENT_DATE - INTERVAL '7 days';

-- Sportsbook mapping effectiveness
SELECT * FROM curated.sportsbook_mapping_status;
```

## Next Steps

1. **Deploy the improvements** using one of the methods above
2. **Monitor the quality metrics** over the next few days as new data comes in
3. **Run data collection** to see the improvements in action
4. **Check back in a week** to see the dramatic improvement in data completeness

The system will automatically start improving data quality for all new betting lines data!

## Troubleshooting

If you get permission errors, make sure you're connected as a user with appropriate database permissions. The scripts create:

- New tables in `core_betting` schema
- New functions in `core_betting` schema  
- New triggers on existing tables
- New views for monitoring

All of these require appropriate permissions on the `core_betting` schema.