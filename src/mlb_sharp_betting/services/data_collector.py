"""Data collection service for coordinating scraping operations."""

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import structlog

from ..core.exceptions import MLBSharpBettingError
from ..models.splits import BettingSplit, DataSource, BookType
from ..scrapers.sbd import SBDScraper
from ..scrapers.vsin import VSINScraper
from ..parsers.sbd import SBDParser
from ..parsers.vsin import VSINParser
from ..services.data_persistence import DataPersistenceService

logger = structlog.get_logger(__name__)


class DataCollectionError(MLBSharpBettingError):
    """Exception for data collection errors."""
    pass


class DataCollector:
    """Service for orchestrating data collection from multiple sources."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        """Initialize data collector with configuration."""
        self.config = config or {}
        self.logger = logger.bind(service="DataCollector")
        
        # Initialize scrapers
        self.sbd_scraper = SBDScraper()
        self.vsin_scraper = VSINScraper()
        
        # Initialize parsers
        self.sbd_parser = SBDParser()
        self.vsin_parser = VSINParser()
        
        # Initialize persistence service with PostgreSQL-compatible manager
        from ..db.connection import get_db_manager
        self.persistence_service = DataPersistenceService(get_db_manager())
        
        # Initialize cross-market flip detector for automatic detection
        try:
            from .cross_market_flip_detector import CrossMarketFlipDetector
            self.flip_detector = CrossMarketFlipDetector(get_db_manager())
            self.flip_detection_enabled = True
            self.logger.info("Cross-market flip detector initialized")
        except ImportError as e:
            self.logger.warning("Cross-market flip detector not available", error=str(e))
            self.flip_detector = None
            self.flip_detection_enabled = False
        
    async def collect_all(self, sport: str = "mlb") -> List[BettingSplit]:
        """
        Collect data from all configured sources: SBD, VSIN Circa, and VSIN DK.
        
        Args:
            sport: Sport to collect data for (default: mlb)
            
        Returns:
            List of all collected BettingSplit objects
        """
        self.logger.info("Starting full data collection", sport=sport)
        
        all_splits = []
        collection_stats = {
            "sources_attempted": 0,
            "sources_successful": 0,
            "total_splits": 0,
            "errors": []
        }
        
        # Define sources to collect from
        sources = [
            ("SBD", "aggregated", None),  # SBD aggregated data (book=NULL)
            ("VSIN", "circa", "circa"),   # VSIN Circa data
            ("VSIN", "dk", "dk")          # VSIN DK data  
        ]
        
        # Collect from each source
        for source_name, source_type, sportsbook in sources:
            collection_stats["sources_attempted"] += 1
            
            try:
                self.logger.info("Collecting from source", 
                               source=source_name, 
                               type=source_type,
                               sportsbook=sportsbook)
                
                splits = await self._collect_from_source(source_name, sport, sportsbook)
                
                if splits:
                    all_splits.extend(splits)
                    collection_stats["sources_successful"] += 1
                    collection_stats["total_splits"] += len(splits)
                    
                    self.logger.info("Successfully collected from source",
                                   source=source_name,
                                   splits_count=len(splits))
                else:
                    self.logger.warning("No data collected from source", 
                                      source=source_name)
                    
            except Exception as e:
                error_msg = f"Failed to collect from {source_name}: {str(e)}"
                collection_stats["errors"].append(error_msg)
                self.logger.error("Source collection failed", 
                                source=source_name,
                                error=str(e))
        
        self.logger.info("Full data collection completed", 
                        stats=collection_stats)
        
        return all_splits
        
    async def _collect_from_source(
        self, 
        source: str, 
        sport: str, 
        sportsbook: Optional[str]
    ) -> List[BettingSplit]:
        """
        Collect data from a specific source.
        
        Args:
            source: Source name ("SBD" or "VSIN")
            sport: Sport to collect
            sportsbook: Sportsbook for VSIN (None for SBD)
            
        Returns:
            List of BettingSplit objects
        """
        try:
            if source.upper() == "SBD":
                return await self._collect_sbd_data(sport)
            elif source.upper() == "VSIN":
                return await self._collect_vsin_data(sport, sportsbook)
            else:
                raise DataCollectionError(f"Unknown source: {source}")
                
        except Exception as e:
            self.logger.error("Source collection failed", 
                            source=source, 
                            sportsbook=sportsbook,
                            error=str(e))
            raise
    
    async def _collect_sbd_data(self, sport: str) -> List[BettingSplit]:
        """
        Collect and parse SBD data.
        
        Args:
            sport: Sport to collect
            
        Returns:
            List of BettingSplit objects with book=NULL
        """
        try:
            # Scrape SBD data
            scrape_result = await self.sbd_scraper.scrape(sport=sport)
            
            if not scrape_result.success or not scrape_result.data:
                self.logger.warning("SBD scraping failed or returned no data",
                                  errors=scrape_result.errors)
                return []
            
            # Parse the data
            splits = self.sbd_parser.parse_all_splits(scrape_result.data)
            
            # Ensure book field is NULL for SBD data (set to None)
            for split in splits:
                split.book = None  # SBD aggregated data has no specific book
            
            self.logger.info("SBD data collection completed", 
                           games_scraped=len(scrape_result.data),
                           splits_parsed=len(splits))
            
            return splits
            
        except Exception as e:
            self.logger.error("SBD data collection failed", error=str(e))
            return []
    
    async def _collect_vsin_data(self, sport: str, sportsbook: str) -> List[BettingSplit]:
        """
        Collect and parse VSIN data for a specific sportsbook.
        
        Args:
            sport: Sport to collect
            sportsbook: Sportsbook ("circa" or "dk")
            
        Returns:
            List of BettingSplit objects with proper book assignment
        """
        try:
            # Scrape VSIN data
            scrape_result = await self.vsin_scraper.scrape(sport=sport, sportsbook=sportsbook)
            
            if not scrape_result.success or not scrape_result.data:
                self.logger.warning("VSIN scraping failed or returned no data",
                                  sportsbook=sportsbook,
                                  errors=scrape_result.errors)
                return []
            
            # Parse the data
            splits = await self.vsin_parser.parse_all_splits(scrape_result.data)
            
            # Ensure proper book assignment for VSIN data
            book_type = BookType.CIRCA if sportsbook == "circa" else BookType.DRAFTKINGS
            for split in splits:
                split.book = book_type
                split.source = DataSource.VSIN
            
            self.logger.info("VSIN data collection completed", 
                           sportsbook=sportsbook,
                           games_scraped=len(scrape_result.data),
                           splits_parsed=len(splits))
            
            return splits
            
        except Exception as e:
            self.logger.error("VSIN data collection failed", 
                            sportsbook=sportsbook,
                            error=str(e))
            return []
    
    async def collect_and_store(self, sport: str = "mlb") -> Dict[str, Any]:
        """
        Collect data from all sources and store in database.
        
        Args:
            sport: Sport to collect data for
            
        Returns:
            Dictionary with collection and storage statistics
        """
        self.logger.info("Starting collect and store operation", sport=sport)
        
        start_time = datetime.now()
        
        # Collect from all sources
        all_splits = await self.collect_all(sport)
        
        if not all_splits:
            self.logger.warning("No data collected from any source")
            return {
                "collection_time": (datetime.now() - start_time).total_seconds(),
                "splits_collected": 0,
                "storage_stats": {},
                "flip_detection_results": None
            }
        
        # Store in database
        storage_stats = self.persistence_service.store_betting_splits(
            splits=all_splits,
            validate=True,
            skip_duplicates=True
        )
        
        # Run automatic flip detection after successful data storage
        flip_detection_results = await self._run_automatic_flip_detection()
        
        end_time = datetime.now()
        total_time = (end_time - start_time).total_seconds()
        
        result = {
            "collection_time": total_time,
            "splits_collected": len(all_splits),
            "storage_stats": storage_stats,
            "sources_breakdown": self._analyze_sources(all_splits),
            "flip_detection_results": flip_detection_results
        }
        
        self.logger.info("Collect and store completed", 
                        total_time=total_time,
                        result=result)
        
        return result
    
    def _analyze_sources(self, splits: List[BettingSplit]) -> Dict[str, int]:
        """Analyze splits by source and book."""
        breakdown = {}
        
        for split in splits:
            source = split.source.value if hasattr(split.source, 'value') else str(split.source)
            book = split.book.value if split.book and hasattr(split.book, 'value') else (str(split.book) if split.book else "NULL")
            
            key = f"{source}_{book}"
            breakdown[key] = breakdown.get(key, 0) + 1
        
        return breakdown
    
    async def _run_automatic_flip_detection(self) -> Optional[Dict[str, Any]]:
        """
        Run automatic flip detection after data collection.
        
        Returns:
            Dictionary with flip detection results or None if disabled/failed
        """
        if not self.flip_detection_enabled or not self.flip_detector:
            self.logger.debug("Flip detection disabled or not available")
            return None
        
        try:
            self.logger.info("Running automatic cross-market flip detection")
            
            # Run today's flip detection with summary
            flips, summary = await self.flip_detector.detect_todays_flips_with_summary(
                min_confidence=75.0  # Use high confidence threshold for automatic detection
            )
            
            # Log results
            if flips:
                self.logger.info("Automatic flip detection completed",
                               flips_found=len(flips),
                               games_evaluated=summary.get("games_evaluated", 0),
                               avg_confidence=summary.get("avg_confidence", 0))
                
                # Log high-confidence flips for immediate attention
                high_confidence_flips = [f for f in flips if f.confidence_score >= 85.0]
                if high_confidence_flips:
                    self.logger.warning("HIGH CONFIDENCE FLIPS DETECTED",
                                      count=len(high_confidence_flips),
                                      flips=[{
                                          "game": f"{f.away_team} @ {f.home_team}",
                                          "confidence": f.confidence_score,
                                          "recommendation": f.strategy_recommendation,
                                          "flip_type": f.flip_type.value
                                      } for f in high_confidence_flips[:3]])  # Show top 3
            else:
                self.logger.info("Automatic flip detection completed - no qualifying flips found",
                               games_evaluated=summary.get("games_evaluated", 0))
            
            return {
                "flips_found": len(flips),
                "high_confidence_flips": len([f for f in flips if f.confidence_score >= 85.0]),
                "summary": summary,
                "execution_time": datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error("Automatic flip detection failed", error=str(e))
            return {
                "error": str(e),
                "execution_time": datetime.now().isoformat()
            }
    
    async def collect_from_source(self, source: str, sport: str = "mlb", **kwargs: Any) -> List[BettingSplit]:
        """
        Collect data from a specific source with additional parameters.
        
        Args:
            source: Source name ("SBD", "VSIN_CIRCA", "VSIN_DK")
            sport: Sport to collect
            **kwargs: Additional parameters
            
        Returns:
            List of BettingSplit objects
        """
        if source.upper() == "SBD":
            return await self._collect_sbd_data(sport)
        elif source.upper() == "VSIN_CIRCA":
            return await self._collect_vsin_data(sport, "circa")
        elif source.upper() == "VSIN_DK":
            return await self._collect_vsin_data(sport, "dk")
        else:
            raise DataCollectionError(f"Unknown source: {source}")
        
    def validate_collection(self, data: List[BettingSplit]) -> bool:
        """
        Validate collected data for consistency and quality.
        
        Args:
            data: List of BettingSplit objects to validate
            
        Returns:
            True if data passes validation
        """
        if not data:
            return False
        
        # Check data types
        if not all(isinstance(split, BettingSplit) for split in data):
            return False
        
        # Check for required fields
        for split in data:
            if not split.game_id or not split.home_team or not split.away_team:
                return False
            
            # Validate source-specific requirements
            if split.source == DataSource.SBD and split.book is not None:
                self.logger.warning("SBD split has non-NULL book", game_id=split.game_id)
                return False
            
            if split.source == DataSource.VSIN and split.book is None:
                self.logger.warning("VSIN split has NULL book", game_id=split.game_id)
                return False
        
        return True


__all__ = ["DataCollectionError", "DataCollector"] 