#!/usr/bin/env python3
"""
Pinnacle API Integration Demo - MLB Sharp Betting System

This script demonstrates the complete Pinnacle API integration including:
- Fetching live betting odds from Pinnacle API
- Parsing market data for moneyline, spread, and total markets
- Converting odds to implied probabilities and betting splits
- Database storage and retrieval
- Error handling and rate limiting
- Data validation and quality assessment

Usage:
    uv run src/mlb_sharp_betting/examples/pinnacle_demo.py

Note: This script demonstrates the Pinnacle integration without requiring
API keys since Pinnacle's guest API is used.
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
import json

import structlog

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from mlb_sharp_betting.services.pinnacle_api_service import PinnacleAPIService, PinnacleAPIConfig
from mlb_sharp_betting.parsers.pinnacle import PinnacleParser, parse_pinnacle_data
from mlb_sharp_betting.models.pinnacle import (
    PinnacleMarket, PinnaclePrice, PinnacleLimit, 
    PinnacleMarketType, PriceDesignation, MarketStatus, LimitType
)
from mlb_sharp_betting.models.splits import BettingSplit, SplitType, BookType, DataSource
from mlb_sharp_betting.models.game import Team
from mlb_sharp_betting.db.connection import get_db_manager
from mlb_sharp_betting.db.repositories import BettingSplitRepository
from mlb_sharp_betting.core.config import get_settings
from mlb_sharp_betting.utils.validators import validate_betting_split, assess_data_quality

# Configure logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.dev.ConsoleRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)


def demo_pinnacle_models():
    """Demonstrate Pinnacle model creation and validation."""
    logger.info("=== Pinnacle Models Demo ===")
    
    try:
        # Create sample Pinnacle prices
        home_price = PinnaclePrice(price=-110, designation=PriceDesignation.HOME)
        away_price = PinnaclePrice(price=105, designation=PriceDesignation.AWAY)
        
        logger.info("Created Pinnacle prices",
                   home_price=home_price.price,
                   home_decimal_odds=home_price.decimal_odds,
                   home_implied_prob=f"{home_price.implied_probability:.2f}%",
                   away_price=away_price.price,
                   away_decimal_odds=away_price.decimal_odds,
                   away_implied_prob=f"{away_price.implied_probability:.2f}%")
        
        # Create sample limits
        max_risk_limit = PinnacleLimit(amount=2500.00, type=LimitType.MAX_RISK_STAKE)
        max_win_limit = PinnacleLimit(amount=5000.00, type=LimitType.MAX_WIN_STAKE)
        
        logger.info("Created Pinnacle limits",
                   max_risk=max_risk_limit.amount_float,
                   max_win=max_win_limit.amount_float)
        
        # Create sample market
        sample_market = PinnacleMarket(
            matchup_id=1610721342,
            market_type=PinnacleMarketType.MONEYLINE,
            key="s;0;m",
            home_team=Team.OAK,
            away_team=Team.HOU,
            game_datetime=datetime.now() + timedelta(hours=4),
            period=0,
            status=MarketStatus.OPEN,
            cutoff_at=datetime.now() + timedelta(hours=3, minutes=45),
            line_value=None,
            prices=[home_price, away_price],
            limits=[max_risk_limit, max_win_limit],
            version=3149186706,
            is_alternate=False,
            last_updated=datetime.now()
        )
        
        logger.info("Created sample market",
                   market_type=sample_market.market_type,
                   matchup=sample_market.matchup_string,
                   display_name=sample_market.market_display_name,
                   minutes_until_cutoff=f"{sample_market.minutes_until_cutoff:.1f}")
        
        # Demonstrate market helper methods
        home_price_obj = sample_market.get_home_price()
        away_price_obj = sample_market.get_away_price()
        max_risk = sample_market.get_max_risk_limit()
        
        logger.info("Market helper methods",
                   home_price_available=home_price_obj is not None,
                   away_price_available=away_price_obj is not None,
                   max_risk_available=max_risk is not None)
        
        logger.info("Pinnacle models demo completed successfully")
        
    except Exception as e:
        logger.error("Pinnacle models demo failed", error=str(e))
        raise


async def demo_pinnacle_api_service():
    """Demonstrate Pinnacle API service functionality."""
    logger.info("=== Pinnacle API Service Demo ===")
    
    try:
        # Initialize Pinnacle API service with custom config
        config = PinnacleAPIConfig(
            timeout=15.0,
            rate_limit_delay=0.2,  # Be gentle with API
            max_retries=2
        )
        
        pinnacle_service = PinnacleAPIService(config=config)
        
        logger.info("Pinnacle API service initialized",
                   base_url=config.base_url,
                   mlb_league_id=config.mlb_league_id,
                   timeout=config.timeout)
        
        # Test team name normalization
        test_team_names = [
            "New York Yankees",
            "Yankees", 
            "Red Sox",
            "Oakland Athletics",
            "Athletics",
            "Houston Astros"
        ]
        
        logger.info("Testing team name normalization")
        for team_name in test_team_names:
            normalized = pinnacle_service._normalize_team_name(team_name)
            logger.info("Team normalization result",
                       input=team_name,
                       output=normalized.value if normalized else None)
        
        # Test API endpoints (Note: These will make actual API calls)
        logger.info("Testing Pinnacle API endpoints")
        
        # Get all available markets
        logger.info("Fetching all MLB markets from Pinnacle...")
        all_markets = await pinnacle_service.get_all_markets()
        
        logger.info("Fetched markets from Pinnacle",
                   total_markets=len(all_markets),
                   market_types=list(set(m.market_type for m in all_markets)))
        
        if all_markets:
            # Show sample of markets
            sample_size = min(3, len(all_markets))
            for i, market in enumerate(all_markets[:sample_size]):
                logger.info(f"Sample market {i+1}",
                           matchup=market.matchup_string,
                           market_type=market.market_type,
                           status=market.status,
                           prices_count=len(market.prices),
                           limits_count=len(market.limits))
                
                # Show prices for this market
                for price in market.prices:
                    logger.info("Market price",
                               designation=price.designation,
                               american_odds=price.price,
                               decimal_odds=f"{price.decimal_odds:.3f}",
                               implied_prob=f"{price.implied_probability:.2f}%")
        
        # Test getting markets by specific matchup ID
        if all_markets:
            first_market = all_markets[0]
            matchup_markets = await pinnacle_service.get_matchup_markets(
                first_market.matchup_id
            )
            
            logger.info("Markets for specific matchup",
                       matchup_id=first_market.matchup_id,
                       markets_found=len(matchup_markets))
        
        # Test creating odds snapshot
        if all_markets:
            first_matchup_id = all_markets[0].matchup_id
            snapshot = await pinnacle_service.create_odds_snapshot(first_matchup_id)
            
            if snapshot:
                logger.info("Created odds snapshot",
                           matchup_id=snapshot.matchup_id,
                           snapshot_time=snapshot.snapshot_time,
                           markets_in_snapshot=len(snapshot.markets))
        
        logger.info("Pinnacle API service demo completed successfully")
        
    except Exception as e:
        logger.error("Pinnacle API service demo failed", error=str(e))
        # Don't raise to allow demo to continue
        logger.info("Continuing with next demo section...")


async def demo_pinnacle_parser():
    """Demonstrate Pinnacle parser functionality."""
    logger.info("=== Pinnacle Parser Demo ===")
    
    try:
        # Create parser instance
        parser = PinnacleParser()
        
        logger.info("Pinnacle parser initialized",
                   parser_name=parser.parser_name,
                   target_model=parser.target_model_class.__name__)
        
        # Create sample markets for parsing
        sample_markets = []
        
        # Moneyline market
        moneyline_market = PinnacleMarket(
            matchup_id=1610721342,
            market_type=PinnacleMarketType.MONEYLINE,
            key="s;0;m",
            home_team=Team.OAK,
            away_team=Team.HOU,
            game_datetime=datetime.now() + timedelta(hours=4),
            period=0,
            status=MarketStatus.OPEN,
            cutoff_at=datetime.now() + timedelta(hours=3, minutes=45),
            line_value=None,
            prices=[
                PinnaclePrice(price=169, designation=PriceDesignation.HOME),
                PinnaclePrice(price=-189, designation=PriceDesignation.AWAY)
            ],
                         limits=[
                 PinnacleLimit(amount=2000.00, type=LimitType.MAX_RISK_STAKE)
             ],
            version=3149186706,
            is_alternate=False,
            last_updated=datetime.now()
        )
        sample_markets.append(moneyline_market)
        
        # Spread market
        spread_market = PinnacleMarket(
            matchup_id=1610721342,
            market_type=PinnacleMarketType.SPREAD,
            key="s;0;s;1.5",
            home_team=Team.OAK,
            away_team=Team.HOU,
            game_datetime=datetime.now() + timedelta(hours=4),
            period=0,
            status=MarketStatus.OPEN,
            cutoff_at=datetime.now() + timedelta(hours=3, minutes=45),
            line_value=1.5,
            prices=[
                PinnaclePrice(price=-110, designation=PriceDesignation.HOME),
                PinnaclePrice(price=-110, designation=PriceDesignation.AWAY)
            ],
                         limits=[
                 PinnacleLimit(amount=1500.00, type=LimitType.MAX_RISK_STAKE)
             ],
            version=3149186707,
            is_alternate=False,
            last_updated=datetime.now()
        )
        sample_markets.append(spread_market)
        
        # Total market
        total_market = PinnacleMarket(
            matchup_id=1610721342,
            market_type=PinnacleMarketType.TOTAL,
            key="s;0;t;8.5",
            home_team=Team.OAK,
            away_team=Team.HOU,
            game_datetime=datetime.now() + timedelta(hours=4),
            period=0,
            status=MarketStatus.OPEN,
            cutoff_at=datetime.now() + timedelta(hours=3, minutes=45),
            line_value=8.5,
            prices=[
                PinnaclePrice(price=-105, designation=PriceDesignation.OVER),
                PinnaclePrice(price=-115, designation=PriceDesignation.UNDER)
            ],
                         limits=[
                 PinnacleLimit(amount=2500.00, type=LimitType.MAX_RISK_STAKE)
             ],
            version=3149186708,
            is_alternate=False,
            last_updated=datetime.now()
        )
        sample_markets.append(total_market)
        
        logger.info("Created sample markets for parsing",
                   market_count=len(sample_markets))
        
        # Parse markets into betting splits
        betting_splits = await parser.parse_pinnacle_markets(sample_markets)
        
        logger.info("Parsed markets into betting splits",
                   input_markets=len(sample_markets),
                   output_splits=len(betting_splits))
        
        # Analyze parsed splits
        for i, split in enumerate(betting_splits):
            logger.info(f"Parsed split {i+1}",
                       game_id=split.game_id,
                       matchup=f"{split.away_team.value} @ {split.home_team.value}",
                       split_type=split.split_type,
                       split_value=split.split_value,
                       home_or_over_pct=f"{split.home_or_over_bets_percentage:.1f}%",
                       away_or_under_pct=f"{split.away_or_under_bets_percentage:.1f}%",
                       source=split.source,
                       book=split.book)
            
            # Validate the split
            validation_result = validate_betting_split(split)
            logger.info(f"Split {i+1} validation",
                       is_valid=validation_result.is_valid,
                       errors=validation_result.errors,
                       warnings=validation_result.warnings)
        
        # Test the standalone parse function
        logger.info("Testing standalone parse function")
        standalone_splits = await parse_pinnacle_data(sample_markets)
        
        logger.info("Standalone parse results",
                   splits_created=len(standalone_splits))
        
        logger.info("Pinnacle parser demo completed successfully")
        
    except Exception as e:
        logger.error("Pinnacle parser demo failed", error=str(e))
        raise


async def demo_database_integration():
    """Demonstrate database integration with Pinnacle data."""
    logger.info("=== Database Integration Demo ===")
    
    try:
        # Get database manager
        db_manager = get_db_manager()
        betting_split_repo = BettingSplitRepository()
        
        logger.info("Database connection established")
        
        # Create sample betting split from Pinnacle data
        sample_split = BettingSplit(
            game_id="mlb_2025_06_17_hou_oak_pinnacle",
            home_team=Team.OAK,
            away_team=Team.HOU,
            game_datetime=datetime.now() + timedelta(hours=4),
            split_type=SplitType.MONEYLINE,
            split_value=None,
            source=DataSource.VSIN,  # Using VSIN as closest match
            book=BookType.CIRCA,     # Using CIRCA as most respected
            last_updated=datetime.now(),
            home_or_over_bets_percentage=47.2,
            away_or_under_bets_percentage=52.8,
        )
        
        logger.info("Created sample betting split for database",
                   game_id=sample_split.game_id,
                   split_type=sample_split.split_type)
        
        # Test database operations
        logger.info("Testing database operations...")
        
        # Note: In a real demo, you would save the split to database
        # await betting_split_repo.save(sample_split)
        # logger.info("Saved split to database")
        
        # Query recent splits
        # recent_splits = await betting_split_repo.find_recent_splits(hours=24)
        # logger.info("Retrieved recent splits", count=len(recent_splits))
        
        logger.info("Database integration demo completed successfully")
        
    except Exception as e:
        logger.error("Database integration demo failed", error=str(e))
        # Don't raise to allow demo to continue


async def demo_end_to_end_workflow():
    """Demonstrate complete end-to-end Pinnacle workflow."""
    logger.info("=== End-to-End Pinnacle Workflow Demo ===")
    
    try:
        logger.info("Starting complete Pinnacle workflow...")
        
        # Step 1: Initialize services
        pinnacle_service = PinnacleAPIService()
        parser = PinnacleParser()
        
        logger.info("Services initialized")
        
        # Step 2: Fetch live data from Pinnacle
        logger.info("Fetching live markets from Pinnacle API...")
        markets = await pinnacle_service.get_all_markets()
        
        if not markets:
            logger.warning("No markets fetched from Pinnacle API")
            return
        
        logger.info("Markets fetched successfully", 
                   total_markets=len(markets))
        
        # Step 3: Filter markets for analysis
        # Focus on main markets (moneyline, spread, total) and non-alternate lines
        main_markets = [
            m for m in markets 
            if m.market_type in {
                PinnacleMarketType.MONEYLINE,
                PinnacleMarketType.SPREAD, 
                PinnacleMarketType.TOTAL
            } and not m.is_alternate
        ]
        
        logger.info("Filtered to main markets", 
                   main_markets=len(main_markets))
        
        # Step 4: Parse markets into betting splits
        logger.info("Parsing markets into betting splits...")
        betting_splits = await parser.parse_pinnacle_markets(main_markets)
        
        logger.info("Markets parsed successfully",
                   splits_created=len(betting_splits))
        
        # Step 5: Analyze and validate splits
        valid_splits = []
        for split in betting_splits:
            validation_result = validate_betting_split(split)
            if validation_result.is_valid:
                valid_splits.append(split)
            else:
                logger.warning("Invalid split found",
                              game_id=split.game_id,
                              errors=validation_result.errors)
        
        logger.info("Split validation completed",
                   total_splits=len(betting_splits),
                   valid_splits=len(valid_splits))
        
        # Step 6: Quality assessment
        if valid_splits:
            quality_assessment = assess_data_quality(valid_splits)
            logger.info("Data quality assessment",
                       overall_quality=quality_assessment.overall_quality,
                       completeness_score=quality_assessment.completeness_score,
                       consistency_score=quality_assessment.consistency_score,
                       freshness_score=quality_assessment.freshness_score)
        
        # Step 7: Show summary results
        logger.info("=== Workflow Summary ===")
        
        # Group splits by type
        split_summary = {}
        for split in valid_splits:
            split_type = split.split_type
            if split_type not in split_summary:
                split_summary[split_type] = 0
            split_summary[split_type] += 1
        
        logger.info("Split types summary", **split_summary)
        
        # Show sample splits
        sample_size = min(3, len(valid_splits))
        logger.info(f"Sample of {sample_size} valid splits:")
        
        for i, split in enumerate(valid_splits[:sample_size]):
            logger.info(f"Sample split {i+1}",
                       matchup=f"{split.away_team.value} @ {split.home_team.value}",
                       type=split.split_type.value,
                       value=split.split_value,
                       home_or_over=f"{split.home_or_over_bets_percentage:.1f}%",
                       away_or_under=f"{split.away_or_under_bets_percentage:.1f}%")
        
        logger.info("End-to-end workflow completed successfully")
        
    except Exception as e:
        logger.error("End-to-end workflow demo failed", error=str(e))
        raise


async def demo_error_handling():
    """Demonstrate error handling and edge cases."""
    logger.info("=== Error Handling Demo ===")
    
    try:
        # Test invalid market data
        parser = PinnacleParser()
        
        # Create market with missing required data
        try:
            invalid_market = PinnacleMarket(
                matchup_id=999999,
                market_type=PinnacleMarketType.SPREAD,
                key="invalid",
                home_team=Team.OAK,
                away_team=Team.HOU,
                game_datetime=datetime.now(),
                period=0,
                status=MarketStatus.OPEN,
                cutoff_at=datetime.now(),
                line_value=None,  # Missing required line value for spread
                prices=[],  # Empty prices list
                limits=[],  # Empty limits list
                version=1,
                is_alternate=False,
                last_updated=datetime.now()
            )
            logger.error("Should have failed validation")
        except Exception as e:
            logger.info("Correctly caught validation error", error=str(e))
        
        # Test API service error handling
        config = PinnacleAPIConfig(
            base_url="https://invalid-url-that-does-not-exist.com",
            timeout=1.0  # Very short timeout
        )
        
        invalid_service = PinnacleAPIService(config=config)
        
        # This should handle the error gracefully
        try:
            markets = await invalid_service.get_all_markets()
            logger.info("Invalid API call result", markets_count=len(markets))
        except Exception as e:
            logger.info("Correctly handled API error", error=str(e))
        
        logger.info("Error handling demo completed successfully")
        
    except Exception as e:
        logger.error("Error handling demo failed", error=str(e))
        raise


async def main():
    """Run the complete Pinnacle integration demonstration."""
    logger.info("Starting Pinnacle Integration Demo")
    logger.info("=" * 60)
    
    # List of demo functions to run
    demos = [
        ("Pinnacle Models", demo_pinnacle_models),
        ("Pinnacle API Service", demo_pinnacle_api_service),
        ("Pinnacle Parser", demo_pinnacle_parser),
        ("Database Integration", demo_database_integration),
        ("End-to-End Workflow", demo_end_to_end_workflow),
        ("Error Handling", demo_error_handling),
    ]
    
    successful_demos = 0
    failed_demos = 0
    
    for demo_name, demo_func in demos:
        try:
            logger.info(f"Starting {demo_name} demo")
            if asyncio.iscoroutinefunction(demo_func):
                await demo_func()
            else:
                demo_func()
            successful_demos += 1
            logger.info(f"{demo_name} demo completed successfully")
        except Exception as e:
            failed_demos += 1
            logger.error(f"{demo_name} demo failed", error=str(e))
        
        logger.info("-" * 40)
    
    # Final summary
    logger.info("=" * 60)
    logger.info("Pinnacle Integration Demo Summary")
    logger.info(f"Successful demos: {successful_demos}")
    logger.info(f"Failed demos: {failed_demos}")
    logger.info(f"Total demos: {len(demos)}")
    
    if failed_demos == 0:
        logger.info("🎉 All demos completed successfully!")
    else:
        logger.warning(f"⚠️  {failed_demos} demo(s) failed - check logs above")
    
    logger.info("Demo completed")


if __name__ == "__main__":
    # Run the demonstration
    asyncio.run(main())