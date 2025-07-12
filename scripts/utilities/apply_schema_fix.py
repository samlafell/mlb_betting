#!/usr/bin/env python3

import logging
from pathlib import Path
from src.mlb_sharp_betting.db.connection import get_db_manager

# Set up basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Apply the database schema fix"""
    try:
        # Get database manager
        db_manager = get_db_manager()
        logger.info("Database manager initialized")
        
        # Read schema fix file
        schema_file = Path("sql/strategy_config_history_schema_fix.sql")
        if not schema_file.exists():
            logger.error(f"Schema fix file not found: {schema_file}")
            return
            
        with open(schema_file, 'r') as f:
            schema_sql = f.read()
        
        logger.info("Applying schema fix...")
        
        # Execute the schema fix
        db_manager.execute_script(schema_sql)
        
        logger.info("✅ Schema fix applied successfully!")
        print("✅ Database schema updated - column names now match orchestrator expectations")
        
    except Exception as e:
        logger.error(f"❌ Schema fix failed: {e}")
        print(f"❌ Schema fix failed: {e}")

if __name__ == "__main__":
    main() 