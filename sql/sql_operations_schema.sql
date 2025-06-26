-- SQL Operations Logging Table Schema
-- This table stores structured logs of all SQL operations for performance monitoring and debugging

CREATE SCHEMA IF NOT EXISTS logging;

CREATE TABLE IF NOT EXISTS logging.sql_operations (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    log_level VARCHAR(10) NOT NULL,
    operation_type VARCHAR(50) NOT NULL,
    query_hash BIGINT NOT NULL,
    execution_time_ms NUMERIC(10,2) NOT NULL DEFAULT 0,
    success BOOLEAN NOT NULL DEFAULT TRUE,
    error_type VARCHAR(100),
    error_message TEXT,
    rows_affected INTEGER DEFAULT 0,
    rows_returned INTEGER DEFAULT 0,
    query_preview TEXT,
    parameters_preview TEXT,
    coordinator_type VARCHAR(50),
    transaction_id VARCHAR(50),
    batch_size INTEGER DEFAULT 0,
    has_returning BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_sql_operations_timestamp ON logging.sql_operations(timestamp);
CREATE INDEX IF NOT EXISTS idx_sql_operations_operation_type ON logging.sql_operations(operation_type);
CREATE INDEX IF NOT EXISTS idx_sql_operations_query_hash ON logging.sql_operations(query_hash);
CREATE INDEX IF NOT EXISTS idx_sql_operations_success ON logging.sql_operations(success);
CREATE INDEX IF NOT EXISTS idx_sql_operations_execution_time ON logging.sql_operations(execution_time_ms);

-- Create a view for easy analysis
CREATE OR REPLACE VIEW logging.sql_operations_summary AS
SELECT 
    operation_type,
    coordinator_type,
    COUNT(*) as total_operations,
    COUNT(*) FILTER (WHERE success = TRUE) as successful_operations,
    COUNT(*) FILTER (WHERE success = FALSE) as failed_operations,
    ROUND(AVG(execution_time_ms), 2) as avg_execution_time_ms,
    ROUND(MAX(execution_time_ms), 2) as max_execution_time_ms,
    ROUND(MIN(execution_time_ms), 2) as min_execution_time_ms,
    SUM(rows_returned) as total_rows_returned,
    SUM(rows_affected) as total_rows_affected,
    DATE_TRUNC('hour', timestamp) as hour_bucket
FROM logging.sql_operations
GROUP BY operation_type, coordinator_type, DATE_TRUNC('hour', timestamp)
ORDER BY hour_bucket DESC, total_operations DESC;

-- Function to import SQL operations log file
CREATE OR REPLACE FUNCTION logging.import_sql_operations_log(log_file_path TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    imported_count INTEGER;
BEGIN
    -- Copy data from pipe-delimited log file
    -- Format: timestamp|level|operation_type|query_hash|execution_time_ms|success|error_type|error_message|rows_affected|rows_returned|query_preview|parameters_preview|coordinator_type|transaction_id|batch_size|has_returning
    
    EXECUTE format('
        COPY logging.sql_operations (
            timestamp, log_level, operation_type, query_hash, execution_time_ms, 
            success, error_type, error_message, rows_affected, rows_returned,
            query_preview, parameters_preview, coordinator_type, transaction_id, 
            batch_size, has_returning
        )
        FROM %L
        WITH (FORMAT csv, DELIMITER ''|'', HEADER false)
    ', log_file_path);
    
    GET DIAGNOSTICS imported_count = ROW_COUNT;
    
    RETURN imported_count;
END;
$$;

-- Example usage:
-- SELECT logging.import_sql_operations_log('/path/to/logs/sql_operations.log');

-- Useful analysis queries:

-- Top slowest queries by hash
CREATE OR REPLACE VIEW logging.slowest_queries AS
SELECT 
    query_hash,
    operation_type,
    COUNT(*) as execution_count,
    ROUND(AVG(execution_time_ms), 2) as avg_execution_time_ms,
    ROUND(MAX(execution_time_ms), 2) as max_execution_time_ms,
    MAX(query_preview) as sample_query,
    MAX(timestamp) as last_executed
FROM logging.sql_operations
WHERE success = TRUE
GROUP BY query_hash, operation_type
HAVING COUNT(*) >= 5  -- Only queries executed at least 5 times
ORDER BY avg_execution_time_ms DESC
LIMIT 20;

-- Error analysis
CREATE OR REPLACE VIEW logging.error_analysis AS
SELECT 
    error_type,
    operation_type,
    COUNT(*) as error_count,
    ROUND(AVG(execution_time_ms), 2) as avg_execution_time_ms,
    array_agg(DISTINCT SUBSTRING(error_message, 1, 100)) as sample_errors,
    MAX(timestamp) as last_occurrence
FROM logging.sql_operations
WHERE success = FALSE
GROUP BY error_type, operation_type
ORDER BY error_count DESC;

-- Performance trends over time
CREATE OR REPLACE VIEW logging.performance_trends AS
SELECT 
    DATE_TRUNC('hour', timestamp) as hour_bucket,
    operation_type,
    COUNT(*) as total_operations,
    ROUND(AVG(execution_time_ms), 2) as avg_execution_time_ms,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY execution_time_ms), 2) as p95_execution_time_ms,
    COUNT(*) FILTER (WHERE success = FALSE) as error_count
FROM logging.sql_operations
GROUP BY DATE_TRUNC('hour', timestamp), operation_type
ORDER BY hour_bucket DESC, total_operations DESC; 