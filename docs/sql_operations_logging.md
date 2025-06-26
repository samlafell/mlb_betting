# SQL Operations Logging System

## Overview

The MLB Sharp Betting system now includes comprehensive SQL operations logging that captures detailed information about every database operation for performance monitoring, debugging, and optimization.

## Features

### Structured Logging
- **Pipe-delimited format** for easy PostgreSQL import
- **Consistent field structure** across all operation types
- **No console output** - all logs go to file only
- **Automatic log rotation** support

### Captured Data
- Query execution time (milliseconds)
- Query hash for grouping similar queries
- Parameter information (truncated for security)
- Success/failure status
- Row counts (affected/returned)
- Operation type classification
- Error details when applicable

## Log Format

Each log entry is pipe-delimited with the following fields:

```
timestamp|level|operation_type|query_hash|execution_time_ms|success|error_type|error_message|rows_affected|rows_returned|query_preview|parameters_preview|coordinator_type|transaction_id|batch_size|has_returning
```

### Field Descriptions

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | ISO8601 | When the operation occurred |
| `level` | String | Log level (DEBUG, INFO, ERROR) |
| `operation_type` | String | Type of operation (SINGLE_QUERY, BATCH_QUERY, TRANSACTION) |
| `query_hash` | Integer | Hash of the SQL query for grouping |
| `execution_time_ms` | Decimal | Execution time in milliseconds |
| `success` | Boolean | Whether the operation succeeded |
| `error_type` | String | Type of error if failed |
| `error_message` | String | Error message if failed |
| `rows_affected` | Integer | Number of rows affected (for writes) |
| `rows_returned` | Integer | Number of rows returned (for reads) |
| `query_preview` | String | First 100 characters of query |
| `parameters_preview` | String | First 200 characters of parameters |
| `coordinator_type` | String | Which coordinator handled the operation |
| `transaction_id` | String | Transaction identifier for grouped operations |
| `batch_size` | Integer | Size of batch operations |
| `has_returning` | Boolean | Whether query has RETURNING clause |

## PostgreSQL Integration

### Creating the Table

Run the schema creation script:

```sql
\i sql/sql_operations_schema.sql
```

This creates:
- `logging.sql_operations` table
- Appropriate indexes for performance
- Analysis views for common queries
- Import function for log files

### Importing Log Data

```sql
-- Import log file into PostgreSQL
SELECT logging.import_sql_operations_log('/path/to/logs/sql_operations.log');
```

### Analysis Queries

#### Performance Summary
```sql
SELECT * FROM logging.sql_operations_summary 
WHERE hour_bucket >= NOW() - INTERVAL '24 hours'
ORDER BY hour_bucket DESC;
```

#### Slowest Queries
```sql
SELECT * FROM logging.slowest_queries;
```

#### Error Analysis
```sql
SELECT * FROM logging.error_analysis;
```

#### Performance Trends
```sql
SELECT * FROM logging.performance_trends
WHERE hour_bucket >= NOW() - INTERVAL '7 days';
```

## Configuration

### Log File Location
Default: `logs/sql_operations.log`

### Log Rotation
The system supports standard log rotation tools. Example logrotate configuration:

```
/path/to/logs/sql_operations.log {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    notifempty
    postrotate
        # Signal application to reopen log file if needed
    endscript
}
```

## Monitoring and Alerting

### Key Metrics to Monitor

1. **Average Execution Time** - Trending upward indicates performance degradation
2. **Error Rate** - Percentage of failed operations
3. **Query Volume** - Operations per hour/minute
4. **Long-Running Queries** - Queries exceeding thresholds
5. **Transaction Success Rate** - Percentage of successful transactions

### Sample Monitoring Queries

```sql
-- Operations per hour in last 24 hours
SELECT 
    DATE_TRUNC('hour', timestamp) as hour,
    COUNT(*) as operations,
    AVG(execution_time_ms) as avg_time_ms,
    COUNT(*) FILTER (WHERE success = FALSE) as errors
FROM logging.sql_operations
WHERE timestamp >= NOW() - INTERVAL '24 hours'
GROUP BY DATE_TRUNC('hour', timestamp)
ORDER BY hour DESC;

-- Queries exceeding 1 second
SELECT 
    query_hash,
    query_preview,
    execution_time_ms,
    timestamp
FROM logging.sql_operations
WHERE execution_time_ms > 1000
    AND timestamp >= NOW() - INTERVAL '1 hour'
ORDER BY execution_time_ms DESC;
```

## Security Considerations

- **Parameter Truncation**: Parameters are truncated to 200 characters to prevent sensitive data exposure
- **Query Preview**: Only first 100 characters of queries are logged
- **Special Character Escaping**: Pipe delimiters are escaped in text fields
- **No Full Query Storage**: Complete SQL queries are not stored in logs

## Performance Impact

- **Minimal Overhead**: Logging adds ~1-2ms per operation
- **Asynchronous**: Log writes don't block SQL execution
- **Efficient Format**: Pipe-delimited format is fast to write and parse
- **Indexed Fields**: PostgreSQL indexes on key fields for fast analysis

## Troubleshooting

### Common Issues

1. **Log File Not Created**
   - Check directory permissions for `logs/` folder
   - Verify application has write access

2. **Import Failures**
   - Check file path in import function
   - Verify pipe delimiter format
   - Check for special characters in data

3. **Performance Impact**
   - Monitor log file size growth
   - Implement log rotation
   - Consider archiving old data

### Debug Mode

To enable verbose SQL logging:

```python
import logging
logging.getLogger("sql_operations").setLevel(logging.DEBUG)
```

## Future Enhancements

- **Real-time Dashboards**: Grafana/Kibana integration
- **Automated Alerting**: Threshold-based alerts
- **Query Optimization**: Automatic slow query detection
- **Historical Analysis**: Long-term performance trending
- **Cost Analysis**: Query cost estimation and tracking 