#!/usr/bin/env python3
"""
Demo script showing how the configuration system centralizes all settings
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import config

def main():
    print("=== MLB Betting Splits Configuration Demo ===\n")
    
    print("Database Settings:")
    print(f"  Database Path: {config.database_path}")
    print(f"  Schema Name: {config.schema_name}")
    print()
    
    print("Table Names:")
    print(f"  Main Table: {config.mlb_betting_splits_table}")
    print(f"  Full Table Name: {config.full_table_name}")
    print(f"  Legacy Table: {config.legacy_splits_table}")
    print()
    
    print("Data Sources:")
    print(f"  SportsBettingDime: {config.sbd_source}")
    print(f"  VSIN: {config.vsin_source}")
    print()
    
    print("API Configuration:")
    print(f"  SBD API URL: {config.sbd_api_url}")
    print()
    
    print("Sample Insert Query (Spread):")
    spread_query = config.get_insert_query('spread')
    print(f"  {spread_query.strip()}")
    print()
    
    print("=== Benefits of Centralized Configuration ===")
    print("✓ All table names managed in one place (config.toml)")
    print("✓ Easy to change table names without touching code")
    print("✓ Consistent naming across all scripts and SQL files")
    print("✓ Environment-specific configurations possible")
    print("✓ Type-safe access through Python properties")

if __name__ == '__main__':
    main() 