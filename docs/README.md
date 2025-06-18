# MLB Betting Splits Documentation

## Overview
This project collects and analyzes MLB betting splits data from various sources.

## Data Sources
- **SBD (SportsBettingDime)**: Primary source for real-time betting splits
- **VSIN**: Secondary source with book-specific data

## Database Schema
The main table `MLB_BETTING_SPLITS` contains:
- Game information (teams, datetime)
- Split types (Spread, Total, Moneyline)
- Betting data (bets, percentages, stakes)
- Source attribution (SOURCE, BOOK columns)

## Scripts
- `scripts/parse_and_save_betting_splits.py`: Main script to fetch and save data
- `scripts/parse_betting_splits.py`: Parse betting splits from API
- `scripts/save_split_to_duckdb.py`: Save splits to database

## Usage
Run the main data collection script:
```bash
uv run scripts/parse_and_save_betting_splits.py
``` 