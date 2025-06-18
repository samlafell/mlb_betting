#!/usr/bin/env python3
"""
Migration script to convert from wide format to long format schema
"""
import duckdb
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import config

def migrate_to_long_format():
    """Migrate existing data from wide format to long format"""
    con = duckdb.connect(config.database_path)
    
    print("Starting migration from wide format to long format...")
    
    # Check if old table exists and has data
    try:
        old_count = con.execute(f"SELECT COUNT(*) FROM {config.full_table_name}").fetchone()[0]
        print(f"Found {old_count} records in existing table")
        
        if old_count > 0:
            print("Backing up existing data...")
            # Create backup table
            backup_table = f"{config.schema_name}.backup_wide_format_{int(__import__('time').time())}"
            con.execute(f"CREATE TABLE {backup_table} AS SELECT * FROM {config.full_table_name}")
            print(f"Backup created: {backup_table}")
    except Exception as e:
        print(f"No existing table found or error reading: {e}")
        old_count = 0
    
    # Drop existing table
    print(f"Dropping existing table: {config.full_table_name}")
    con.execute(f"DROP TABLE IF EXISTS {config.full_table_name}")
    
    # Create new schema
    print("Creating new long format schema...")
    with open('sql/schema.sql', 'r') as f:
        schema_sql = f.read()
    con.execute(schema_sql)
    
    print("Migration completed!")
    print(f"New table created: {config.full_table_name}")
    print("You can now run the data collection script to populate with new data.")
    
    con.close()

if __name__ == '__main__':
    migrate_to_long_format() 