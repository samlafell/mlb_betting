#!/usr/bin/env python3
"""
MLB Sharp Betting Data Pipeline Entrypoint

This script demonstrates the complete data pipeline:
1. Scrape betting splits data from VSIN
2. Parse and validate the raw data
3. Store validated data in DuckDB
4. Analyze data for sharp action indicators
5. Generate summary reports

Usage:
    python src/mlb_sharp_betting/entrypoint.py [options]
    
Options:
    --sport SPORT        Sport to scrape (default: mlb)
    --sportsbook BOOK    Sportsbook to use (default: circa)
    --dry-run           Run without making web requests
    --verbose           Enable verbose logging
    --output OUTPUT     Output file for results (optional)
"""

import asyncio
import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import structlog

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.dev.ConsoleRenderer(colors=True)
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)

# Import our components
from mlb_sharp_betting.db.connection import get_db_manager
from mlb_sharp_betting.db.repositories import BettingSplitRepository, GameRepository
from mlb_sharp_betting.services.data_persistence import DataPersistenceService
from mlb_sharp_betting.services.data_collector import DataCollector
from mlb_sharp_betting.utils.validators import validate_betting_split, assess_data_quality
from mlb_sharp_betting.utils.team_mapper import normalize_team_name
from mlb_sharp_betting.models.splits import BettingSplit, SplitType, BookType, DataSource
from mlb_sharp_betting.models.game import Team


class DataPipeline:
    """Complete data pipeline for MLB betting splits."""
    
    def __init__(self, sport: str = "mlb", sportsbook: str = "circa", dry_run: bool = False):
        """
        Initialize the data pipeline.
        
        Args:
            sport: Sport to scrape data for
            sportsbook: Sportsbook to use as data source
            dry_run: If True, skip actual web requests
        """
        self.sport = sport
        self.sportsbook = sportsbook
        self.dry_run = dry_run
        
        # Initialize components
        self.db_manager = get_db_manager()
        self.data_persistence_service = DataPersistenceService(self.db_manager)
        self.betting_split_repo = BettingSplitRepository()
        self.game_repo = GameRepository()
        self.data_collector = DataCollector()
        
        # Metrics
        self.metrics = {
            "scraped_records": 0,
            "parsed_records": 0,
            "valid_records": 0,
            "stored_records": 0,
            "sharp_indicators": 0,
            "errors": 0,
            "start_time": None,
            "end_time": None
        }
        
        logger.info("Data pipeline initialized", 
                   sport=sport, sportsbook=sportsbook, dry_run=dry_run)
    
    async def setup_database(self) -> None:
        """Set up database schema and tables using the proper schema manager."""
        logger.info("Setting up database schema via data persistence service")
        
        try:
            # The data persistence service will handle schema setup automatically
            # through its _ensure_schema() method in __init__
            logger.info("Database schema setup completed via repository pattern")
            
        except Exception as e:
            logger.error("Failed to setup database schema", error=str(e))
            raise
    
    async def collect_data(self) -> List[BettingSplit]:
        """Collect betting splits data from all sources."""
        logger.info("Starting data collection", sport=self.sport, sportsbook=self.sportsbook)
        
        if self.dry_run:
            logger.info("Dry run mode - using mock data")
            mock_data = self._get_mock_data()
            # Convert mock data to BettingSplit objects for consistency
            return self._convert_mock_to_splits(mock_data)
        
        try:
            # Collect from all sources (SBD + VSIN)
            splits = await self.data_collector.collect_all(sport=self.sport)
            
            self.metrics["scraped_records"] = len(splits)
            self.metrics["parsed_records"] = len(splits)  # Already parsed by data collector
            logger.info("Data collection completed successfully", 
                       splits_collected=len(splits))
            return splits
                    
        except Exception as e:
            logger.error("Data collection failed with exception", error=str(e))
            self.metrics["errors"] += 1
            return []
    
    def _get_mock_data(self) -> List[Dict]:
        """Generate mock data for testing."""
        return [
            {
                "Game": "Yankees @ Red Sox",
                "Spread": "-1.5",
                "Home Bets %": "65%",
                "Away Bets %": "35%",
                "Home Money %": "58%",
                "Away Money %": "42%",
                "source": DataSource.VSIN.value,
                "book": self.sportsbook,
                "sport": self.sport,
                "scraped_at": datetime.now().isoformat()
            },
            {
                "Game": "Dodgers vs Giants",
                "Total": "8.5",
                "Over Bets %": "72%",
                "Under Bets %": "28%",
                "Over Money %": "68%",
                "Under Money %": "32%",
                "source": DataSource.VSIN.value,
                "book": self.sportsbook,
                "sport": self.sport,
                "scraped_at": datetime.now().isoformat()
            },
            {
                "Game": "Cubs @ Cardinals",
                "Home Bets %": "45%",
                "Away Bets %": "55%",
                "Home Money %": "38%",  # Sharp action: money opposite to bets
                "Away Money %": "62%",
                "source": DataSource.VSIN.value,
                "book": self.sportsbook,
                "sport": self.sport,
                "scraped_at": datetime.now().isoformat()
            },
            {
                "Game": "Astros @ Mariners",
                "Home Bets %": "75%",
                "Away Bets %": "25%",
                "Home Money %": "45%",  # SHARP ACTION: 30% difference!
                "Away Money %": "55%",
                "source": DataSource.VSIN.value,
                "book": self.sportsbook,
                "sport": self.sport,
                "scraped_at": datetime.now().isoformat()
            }
        ]
    
    def _convert_mock_to_splits(self, mock_data: List[Dict]) -> List[BettingSplit]:
        """Convert mock data to BettingSplit objects."""
        splits = []
        
        for i, data in enumerate(mock_data):
            try:
                # Extract percentages
                home_bets_pct = float(data.get("Home Bets %", "50").replace("%", ""))
                away_bets_pct = float(data.get("Away Bets %", "50").replace("%", ""))
                home_money_pct = float(data.get("Home Money %", "50").replace("%", ""))
                away_money_pct = float(data.get("Away Money %", "50").replace("%", ""))
                
                # Calculate sharp action
                sharp_action = None
                if abs(home_bets_pct - home_money_pct) >= 10.0:
                    sharp_action = "home" if home_money_pct > home_bets_pct else "away"
                
                # Create a basic BettingSplit from mock data
                split = BettingSplit(
                    game_id=f"mock_game_{i}",
                    home_team=Team.NYY,  # New York Yankees
                    away_team=Team.BOS,  # Boston Red Sox
                    game_datetime=datetime.now(),
                    split_type=SplitType.SPREAD,  # Default type
                    split_value=None,
                    source=DataSource.VSIN,
                    book=BookType.CIRCA,
                    last_updated=datetime.now(),
                    home_or_over_bets_percentage=home_bets_pct,
                    away_or_under_bets_percentage=away_bets_pct,
                    home_or_over_stake_percentage=home_money_pct,
                    away_or_under_stake_percentage=away_money_pct,
                    sharp_action=sharp_action
                )
                splits.append(split)
            except Exception as e:
                logger.warning("Failed to convert mock data item", item=data, error=str(e))
                
        return splits
    

    
    def validate_and_store_data(self, betting_splits: List[BettingSplit]) -> List[BettingSplit]:
        """Validate and store betting splits in the database using the repository pattern."""
        logger.info("Starting data validation and storage", records_to_validate=len(betting_splits))
        
        try:
            # Use the data persistence service for proper validation and storage
            storage_stats = self.data_persistence_service.store_betting_splits(
                splits=betting_splits,
                batch_size=100,
                validate=True,
                skip_duplicates=True
            )
            
            # Update metrics based on storage results
            self.metrics["stored_records"] = storage_stats["stored"]
            self.metrics["valid_records"] = storage_stats["stored"] + storage_stats["skipped"]
            self.metrics["errors"] += storage_stats["errors"] + storage_stats["validation_errors"]
            
            logger.info("Data validation and storage completed using repository pattern",
                       storage_stats=storage_stats)
            
            # Return the splits that were successfully processed
            return betting_splits[:storage_stats["stored"]]
            
        except Exception as e:
            logger.error("Failed to store betting splits via repository", error=str(e))
            self.metrics["errors"] += len(betting_splits)
            return []
    
    def analyze_data(self) -> Dict:
        """Analyze stored data for insights and sharp action."""
        logger.info("Starting data analysis")
        
        try:
            with self.db_manager.get_cursor() as cursor:
                # Get total splits count
                cursor.execute("SELECT COUNT(*) FROM splits.raw_mlb_betting_splits")
                total_splits = cursor.fetchone()[0]
                
                # Find sharp action indicators (money percentage differs significantly from bet percentage)
                cursor.execute("""
                    SELECT game_id, home_team, away_team, split_type, split_value,
                           home_or_over_bets_percentage, home_or_over_stake_percentage,
                           away_or_under_bets_percentage, away_or_under_stake_percentage,
                           (ABS(home_or_over_bets_percentage - home_or_over_stake_percentage)) as bet_money_diff
                    FROM splits.raw_mlb_betting_splits 
                    WHERE ABS(home_or_over_bets_percentage - home_or_over_stake_percentage) >= 10.0
                    ORDER BY bet_money_diff DESC
                """)
                sharp_indicators = cursor.fetchall()
                
                # Get splits by type
                cursor.execute("""
                    SELECT split_type, COUNT(*) 
                    FROM splits.raw_mlb_betting_splits 
                    GROUP BY split_type
                """)
                splits_by_type = dict(cursor.fetchall())
                
                # Get average bet vs money percentages
                cursor.execute("""
                    SELECT 
                        AVG(home_or_over_bets_percentage) as avg_home_bets,
                        AVG(home_or_over_stake_percentage) as avg_home_money,
                        AVG(away_or_under_bets_percentage) as avg_away_bets,
                        AVG(away_or_under_stake_percentage) as avg_away_money
                    FROM splits.raw_mlb_betting_splits
                """)
                averages = cursor.fetchone()
                
                # Update sharp action flags only for records that don't already have sharp action set
                # This preserves the specific direction ("home", "away", "over", "under") set by parsers
                cursor.execute("""
                    UPDATE splits.raw_mlb_betting_splits 
                    SET sharp_action = CASE 
                        WHEN split_type IN ('spread', 'moneyline') THEN
                            CASE WHEN home_or_over_stake_percentage > home_or_over_bets_percentage THEN 'home' ELSE 'away' END
                        WHEN split_type = 'total' THEN
                            CASE WHEN home_or_over_stake_percentage > home_or_over_bets_percentage THEN 'over' ELSE 'under' END
                    END
                    WHERE ABS(home_or_over_bets_percentage - home_or_over_stake_percentage) >= 10.0
                      AND (sharp_action IS NULL OR sharp_action = 'true' OR sharp_action = 'false')
                """)
                
                self.metrics["sharp_indicators"] = len(sharp_indicators)
                
                analysis_results = {
                    "total_splits": total_splits,
                    "sharp_indicators": sharp_indicators,
                    "sharp_count": len(sharp_indicators),
                    "splits_by_type": splits_by_type,
                    "averages": {
                        "home_bets_pct": averages[0] if averages[0] else 0,
                        "home_money_pct": averages[1] if averages[1] else 0,
                        "away_bets_pct": averages[2] if averages[2] else 0,
                        "away_money_pct": averages[3] if averages[3] else 0
                    }
                }
                
                logger.info("Data analysis completed",
                           total_splits=total_splits,
                           sharp_indicators=len(sharp_indicators),
                           splits_by_type=splits_by_type)
                
                return analysis_results
                
        except Exception as e:
            logger.error("Data analysis failed", error=str(e))
            self.metrics["errors"] += 1
            return {}
    
    def generate_report(self, analysis_results: Dict, output_file: Optional[str] = None) -> str:
        """Generate a summary report of the pipeline results."""
        
        duration = (self.metrics["end_time"] - self.metrics["start_time"]).total_seconds()
        
        report = f"""
MLB Sharp Betting Analysis Report
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Duration: {duration:.2f} seconds

=== PIPELINE METRICS ===
Records Scraped: {self.metrics['scraped_records']}
Records Parsed: {self.metrics['parsed_records']}
Valid Records: {self.metrics['valid_records']}
Records Stored: {self.metrics['stored_records']}
Sharp Indicators Found: {self.metrics['sharp_indicators']}
Errors: {self.metrics['errors']}

=== DATA ANALYSIS ===
Total Betting Splits: {analysis_results.get('total_splits', 0)}
Sharp Action Indicators: {analysis_results.get('sharp_count', 0)}

Split Types:
"""
        
        for split_type, count in analysis_results.get('splits_by_type', {}).items():
            report += f"  {split_type}: {count}\n"
        
        averages = analysis_results.get('averages', {})
        report += f"""
Average Percentages:
  Home/Over Bets: {averages.get('home_bets_pct', 0):.1f}%
  Home/Over Money: {averages.get('home_money_pct', 0):.1f}%
  Away/Under Bets: {averages.get('away_bets_pct', 0):.1f}%
  Away/Under Money: {averages.get('away_money_pct', 0):.1f}%

=== SHARP ACTION INDICATORS ===
"""
        
        for sharp in analysis_results.get('sharp_indicators', [])[:5]:  # Show top 5
            bet_diff = sharp[9]  # bet_money_diff
            report += f"  {sharp[1]} vs {sharp[2]} ({sharp[3]}): {bet_diff:.1f}% difference\n"
        
        if output_file:
            Path(output_file).write_text(report)
            logger.info("Report saved to file", output_file=output_file)
        
        return report
    
    async def run(self, output_file: Optional[str] = None) -> Dict:
        """Run the complete data pipeline."""
        self.metrics["start_time"] = datetime.now()
        
        logger.info("Starting MLB Sharp Betting data pipeline")
        
        try:
            # 1. Setup database
            await self.setup_database()
            
            # 2. Collect data from all sources
            collected_splits = await self.collect_data()
            if not collected_splits:
                logger.error("No data collected, aborting pipeline")
                return self.metrics
            
            # 3. Validate and store data
            valid_splits = self.validate_and_store_data(collected_splits)
            if not valid_splits:
                logger.error("No valid data to analyze")
                return self.metrics
            
            # 5. Analyze data
            analysis_results = self.analyze_data()
            
            # 6. Generate report
            self.metrics["end_time"] = datetime.now()
            report = self.generate_report(analysis_results, output_file)
            
            print("\n" + "="*60)
            print(report)
            print("="*60)
            
            logger.info("Data pipeline completed successfully", 
                       duration=(self.metrics["end_time"] - self.metrics["start_time"]).total_seconds())
            
            return self.metrics
            
        except Exception as e:
            logger.error("Data pipeline failed", error=str(e))
            self.metrics["errors"] += 1
            return self.metrics


async def main():
    """Main entrypoint function."""
    parser = argparse.ArgumentParser(description="MLB Sharp Betting Data Pipeline")
    parser.add_argument("--sport", default="mlb", help="Sport to scrape (default: mlb)")
    parser.add_argument("--sportsbook", default="circa", help="Sportsbook to use (default: circa)")
    parser.add_argument("--dry-run", action="store_true", help="Run without making web requests")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument("--output", help="Output file for results")
    
    args = parser.parse_args()
    
    # Configure logging level
    if args.verbose:
        import logging
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create and run pipeline
    pipeline = DataPipeline(
        sport=args.sport,
        sportsbook=args.sportsbook,
        dry_run=args.dry_run
    )
    
    metrics = await pipeline.run(output_file=args.output)
    
    # Exit with error code if there were significant issues
    if metrics["errors"] > 0 and metrics["stored_records"] == 0:
        sys.exit(1)
    
    sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main()) 