"""
Database Integration Demo for MLB Sharp Betting System

This demo script shows the complete flow of database integration:
1. Schema setup and verification
2. Data scraping and parsing
3. Data validation and storage
4. Querying and retrieval
"""

import asyncio
from datetime import datetime, timedelta
from typing import List

import structlog

from ..db.connection import DatabaseManager
from ..db.schema import SchemaManager, setup_database_schema
from ..services.data_service import get_data_service
from ..scrapers.sbd import SBDScraper
from ..scrapers.vsin import VSINScraper
from ..parsers.sbd import SBDParser
from ..parsers.vsin import VSINParser
from ..models.splits import BettingSplit, DataSource, BookType
from ..core.config import get_settings

logger = structlog.get_logger(__name__)


class DatabaseIntegrationDemo:
    """
    Demonstrates complete database integration for BettingSplit storage.
    """

    def __init__(self) -> None:
        """Initialize demo components."""
        self.settings = get_settings()
        self.db_manager = DatabaseManager()
        self.schema_manager = SchemaManager(self.db_manager)
        self.data_service = get_data_service(self.db_manager)
        
        # Initialize scrapers and parsers
        self.sbd_scraper = SBDScraper()
        self.vsin_scraper = VSINScraper()
        self.sbd_parser = SBDParser()
        self.vsin_parser = VSINParser()
        
        self.logger = logger.bind(component="DatabaseIntegrationDemo")

    async def run_complete_demo(self) -> None:
        """Run the complete database integration demonstration."""
        self.logger.info("Starting database integration demo")
        
        try:
            # 1. Setup and verify database schema
            await self._demo_schema_setup()
            
            # 2. Scrape and parse data from multiple sources
            splits_data = await self._demo_data_collection()
            
            # 3. Store data with validation
            await self._demo_data_storage(splits_data)
            
            # 4. Query and verify stored data
            await self._demo_data_retrieval()
            
            # 5. Show data integrity and statistics
            await self._demo_data_analysis()
            
            self.logger.info("Database integration demo completed successfully")
            
        except Exception as e:
            self.logger.error("Demo failed", error=str(e))
            raise

    async def _demo_schema_setup(self) -> None:
        """Demonstrate database schema setup and verification."""
        self.logger.info("=== SCHEMA SETUP DEMO ===")
        
        try:
            # Check if schema exists
            schema_exists = self.schema_manager.verify_schema()
            self.logger.info("Schema verification result", exists=schema_exists)
            
            if not schema_exists:
                self.logger.info("Setting up database schema")
                self.schema_manager.setup_complete_schema()
                self.logger.info("Schema setup completed")
            
            # Get schema information
            schema_info = self.schema_manager.get_schema_info()
            self.logger.info("Schema information", 
                           tables=list(schema_info["tables"].keys()),
                           table_count=len(schema_info["tables"]))
            
            # Verify setup
            if self.schema_manager.verify_schema():
                self.logger.info("✅ Schema verification passed")
            else:
                self.logger.error("❌ Schema verification failed")
                
        except Exception as e:
            self.logger.error("Schema setup demo failed", error=str(e))
            raise

    async def _demo_data_collection(self) -> List[BettingSplit]:
        """Demonstrate data collection from multiple sources."""
        self.logger.info("=== DATA COLLECTION DEMO ===")
        
        all_splits = []
        
        try:
            # Collect from SportsBettingDime
            self.logger.info("Scraping data from SportsBettingDime")
            sbd_result = await self.sbd_scraper.scrape(sport="mlb")
            
            if sbd_result.success and sbd_result.data:
                self.logger.info("Parsing SBD data", raw_count=len(sbd_result.data))
                sbd_splits = self.sbd_parser.parse_all_splits(sbd_result.data)
                all_splits.extend(sbd_splits)
                self.logger.info("✅ SBD data parsed", splits_count=len(sbd_splits))
            else:
                self.logger.warning("No SBD data available", errors=sbd_result.errors if sbd_result else [])
            
            # Collect from VSIN (with rate limiting)
            self.logger.info("Scraping data from VSIN")
            try:
                vsin_result = await self.vsin_scraper.scrape(sport="mlb")
                
                if vsin_result.success and vsin_result.data:
                    self.logger.info("Parsing VSIN data", raw_count=len(vsin_result.data))
                    # Note: VSIN parser may not exist yet, so skip for now
                    self.logger.info("✅ VSIN data collected", splits_count=len(vsin_result.data))
                else:
                    self.logger.warning("No VSIN data available", errors=vsin_result.errors if vsin_result else [])
                    
            except Exception as e:
                self.logger.warning("VSIN scraping failed (rate limited?)", error=str(e))
            
            self.logger.info("Data collection completed", 
                           total_splits=len(all_splits),
                           sources=len(set(split.source for split in all_splits)))
            
            return all_splits
            
        except Exception as e:
            self.logger.error("Data collection demo failed", error=str(e))
            raise

    async def _demo_data_storage(self, splits: List[BettingSplit]) -> None:
        """Demonstrate data storage with validation."""
        self.logger.info("=== DATA STORAGE DEMO ===")
        
        if not splits:
            # Create some demo data if no real data available
            splits = self._create_demo_splits()
            self.logger.info("Created demo splits for testing", count=len(splits))
        
        try:
            # Store splits with validation
            self.logger.info("Storing betting splits with validation")
            storage_stats = self.data_service.persistence.store_betting_splits(
                splits=splits,
                batch_size=50,
                validate=True,
                skip_duplicates=True
            )
            
            self.logger.info("✅ Storage completed", stats=storage_stats)
            
            # Show storage efficiency
            if storage_stats["processed"] > 0:
                success_rate = (storage_stats["stored"] / storage_stats["processed"]) * 100
                self.logger.info(f"Storage success rate: {success_rate:.1f}%")
            
        except Exception as e:
            self.logger.error("Data storage demo failed", error=str(e))
            raise

    async def _demo_data_retrieval(self) -> None:
        """Demonstrate data retrieval and querying."""
        self.logger.info("=== DATA RETRIEVAL DEMO ===")
        
        try:
            # Get recent splits
            recent_splits = self.data_service.persistence.get_recent_splits(hours=24)
            self.logger.info("Recent splits retrieved", count=len(recent_splits))
            
            if recent_splits:
                # Show breakdown by source
                by_source = {}
                by_type = {}
                
                for split in recent_splits:
                    # Handle both enum objects and string values from database
                    source = split.source if isinstance(split.source, str) else split.source.value
                    split_type = split.split_type if isinstance(split.split_type, str) else split.split_type.value
                    
                    by_source[source] = by_source.get(source, 0) + 1
                    by_type[split_type] = by_type.get(split_type, 0) + 1
                
                self.logger.info("Splits by source", breakdown=by_source)
                self.logger.info("Splits by type", breakdown=by_type)
                
                # Get splits for a specific game
                first_split = recent_splits[0]
                game_splits = self.data_service.persistence.get_splits_by_game(first_split.game_id)
                self.logger.info("Example game splits", 
                               game_id=first_split.game_id,
                               splits_count=len(game_splits))
            
            self.logger.info("✅ Data retrieval demo completed")
            
        except Exception as e:
            self.logger.error("Data retrieval demo failed", error=str(e))
            raise

    async def _demo_data_analysis(self) -> None:
        """Demonstrate data analysis and integrity checks."""
        self.logger.info("=== DATA ANALYSIS DEMO ===")
        
        try:
            # Get storage statistics
            stats = self.data_service.persistence.get_storage_statistics()
            self.logger.info("Storage statistics", stats=stats)
            
            # Verify data integrity
            integrity_results = self.data_service.persistence.verify_data_integrity()
            self.logger.info("Data integrity check", results=integrity_results)
            
            if integrity_results["overall_health"] == "healthy":
                self.logger.info("✅ Data integrity check passed")
            else:
                self.logger.warning("⚠️ Data integrity issues found",
                                  warnings=integrity_results.get("warnings", []),
                                  errors=integrity_results.get("errors", []))
            
            self.logger.info("✅ Data analysis demo completed")
            
        except Exception as e:
            self.logger.error("Data analysis demo failed", error=str(e))
            raise

    def _create_demo_splits(self) -> List[BettingSplit]:
        """Create demo betting splits for testing."""
        demo_splits = []
        
        # Create some realistic demo data
        game_id = f"2024-{datetime.now().strftime('%m-%d')}-LAD-SF-1"
        base_time = datetime.now()
        
        # Spread split
        spread_split = BettingSplit(
            game_id=game_id,
            home_team="SF",
            away_team="LAD",
            game_datetime=base_time + timedelta(hours=3),
            split_type="spread",
            split_value=-1.5,
            source=DataSource.SBD,
            book=BookType.DRAFTKINGS,
            last_updated=base_time,
            home_or_over_bets=1200,
            home_or_over_bets_percentage=35.5,
            home_or_over_stake_percentage=42.1,
            away_or_under_bets=2180,
            away_or_under_bets_percentage=64.5,
            away_or_under_stake_percentage=57.9,
            sharp_action="away"
        )
        
        # Total split
        total_split = BettingSplit(
            game_id=game_id,
            home_team="SF",
            away_team="LAD",
            game_datetime=base_time + timedelta(hours=3),
            split_type="total",
            split_value=8.5,
            source=DataSource.SBD,
            book=BookType.DRAFTKINGS,
            last_updated=base_time,
            home_or_over_bets=1850,
            home_or_over_bets_percentage=54.8,
            home_or_over_stake_percentage=61.2,
            away_or_under_bets=1530,
            away_or_under_bets_percentage=45.2,
            away_or_under_stake_percentage=38.8,
            sharp_action="over"
        )
        
        # Moneyline split
        ml_split = BettingSplit(
            game_id=game_id,
            home_team="SF",
            away_team="LAD",
            game_datetime=base_time + timedelta(hours=3),
            split_type="moneyline",
            split_value=None,
            source=DataSource.SBD,
            book=BookType.DRAFTKINGS,
            last_updated=base_time,
            home_or_over_bets=980,
            home_or_over_bets_percentage=29.1,
            home_or_over_stake_percentage=33.7,
            away_or_under_bets=2390,
            away_or_under_bets_percentage=70.9,
            away_or_under_stake_percentage=66.3,
            sharp_action="away"
        )
        
        demo_splits.extend([spread_split, total_split, ml_split])
        
        return demo_splits

    async def cleanup_demo_data(self) -> None:
        """Clean up demo data (optional)."""
        self.logger.info("=== CLEANUP DEMO ===")
        
        try:
            # Clean up old data
            cleanup_stats = self.data_service.persistence.cleanup_old_data(days_to_keep=7)
            self.logger.info("Cleanup completed", stats=cleanup_stats)
            
        except Exception as e:
            self.logger.error("Cleanup failed", error=str(e))


async def run_database_integration_demo():
    """Run the complete database integration demonstration."""
    demo = DatabaseIntegrationDemo()
    await demo.run_complete_demo()


if __name__ == "__main__":
    # Configure logging
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Run the demo
    asyncio.run(run_database_integration_demo()) 