#!/usr/bin/env python3
"""
Process SQL files for PostgreSQL compatibility.

This script processes all SQL files in the analysis_scripts directory through 
the SQL preprocessor to create PostgreSQL-compatible versions that can be run 
directly in PostgreSQL without errors.
"""

import os
import glob
from pathlib import Path
from src.mlb_sharp_betting.services.sql_preprocessor import SQLPreprocessor


def process_sql_files():
    """Process all SQL files in analysis_scripts directory."""
    
    preprocessor = SQLPreprocessor()
    analysis_dir = Path("analysis_scripts")
    
    if not analysis_dir.exists():
        print("❌ analysis_scripts directory not found")
        return
    
    # Find all SQL files
    sql_files = list(analysis_dir.glob("*.sql"))
    
    if not sql_files:
        print("❌ No SQL files found in analysis_scripts directory")
        return
    
    print(f"📁 Found {len(sql_files)} SQL files to process")
    print("=" * 50)
    
    total_transformations = 0
    processed_files = []
    
    for sql_file in sql_files:
        try:
            print(f"🔄 Processing: {sql_file.name}")
            
            # Process the SQL file
            processed_sql = preprocessor.process_sql_file(str(sql_file))
            
            # Create output filename
            output_file = sql_file.parent / f"{sql_file.stem}_postgres.sql"
            
            # Write processed SQL
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(processed_sql)
            
            transformations = preprocessor.transformation_count
            total_transformations += transformations
            
            print(f"✅ Created: {output_file.name} ({transformations} transformations)")
            processed_files.append(output_file.name)
            
            # Reset counter for next file
            preprocessor.transformation_count = 0
            
        except Exception as e:
            print(f"❌ Error processing {sql_file.name}: {e}")
    
    print("=" * 50)
    print(f"🎉 Processing complete!")
    print(f"📊 Total files processed: {len(processed_files)}")
    print(f"🔧 Total transformations applied: {total_transformations}")
    
    if processed_files:
        print("\n📝 PostgreSQL-compatible files created:")
        for filename in processed_files:
            print(f"   • {filename}")
        
        print("\n💡 Usage:")
        print("   You can now run these *_postgres.sql files directly in PostgreSQL")
        print("   without encountering ROUND function compatibility errors.")


if __name__ == "__main__":
    process_sql_files() 