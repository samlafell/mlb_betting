"""
SportsbookReview System Demo

This demo script showcases the SportsbookReview integration system with:
- Enhanced Game models with MLB Stats API integration
- Comprehensive odds data tracking
- Sportsbook mapping and normalization
- MLB Data Enrichment Service functionality

Run this to test the system and see how the components work together.
"""

import asyncio
from datetime import datetime, timezone
from pprint import pprint
import sys
from pathlib import Path

# Add the parent directory to Python path so we can import sportsbookreview
parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))

# Import SportsbookReview models
from sportsbookreview.models import (
    EnhancedGame,
    VenueInfo,
    WeatherData,
    WeatherCondition,
    PitcherInfo,
    PitcherMatchup,
    GameContext,
    OddsData,
    OddsSnapshot,
    LineMovementData,
    SportsbookMapping,
    SportsbookCapabilities,
    MarketMapping,
    BetType,
    SportsbookName,
    MarketSide,
    MarketAvailability,
    GameType,
    DataQuality
)

# Import services
from sportsbookreview.services.mlb_data_enrichment_service import (
    get_mlb_data_enrichment_service,
    MLBGameCorrelation
)

# Import main system models for consistency
import sys
from pathlib import Path
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

from mlb_sharp_betting.models.game import Team, GameStatus


def create_sample_enhanced_game() -> EnhancedGame:
    """Create a sample enhanced game for testing."""
    
    print("🎯 Creating sample Enhanced Game...")
    
    # Create venue info
    venue_info = VenueInfo(
        venue_id=2395,
        venue_name="Oracle Park",
        city="San Francisco",
        state="CA",
        timezone="America/Los_Angeles",
        capacity=41915,
        surface="Grass",
        roof_type="Open"
    )
    
    # Create weather data
    weather_data = WeatherData(
        condition=WeatherCondition.CLEAR,
        temperature=68,
        wind_speed=8,
        wind_direction="W",
        humidity=65
    )
    
    # Create pitcher info
    home_pitcher = PitcherInfo(
        player_id=592789,
        full_name="Logan Webb",
        throws="R",
        era=3.25,
        wins=8,
        losses=5
    )
    
    away_pitcher = PitcherInfo(
        player_id=621111,
        full_name="Walker Buehler",
        throws="R",
        era=2.97,
        wins=10,
        losses=4
    )
    
    # Create pitcher matchup
    pitcher_matchup = PitcherMatchup(
        home_pitcher=home_pitcher,
        away_pitcher=away_pitcher
    )
    
    # Create game context
    game_context = GameContext(
        series_description="Regular Season Series",
        series_game_number=2,
        games_in_series=3,
        is_playoff_game=False,
        attendance=35847,
        game_duration_minutes=175
    )
    
    # Create enhanced game
    game = EnhancedGame(
        sbr_game_id="sbr-2025-07-15-LAD-SF-1",
        mlb_game_id="123456",
        home_team=Team.SF,
        away_team=Team.LAD,
        game_date=datetime(2025, 7, 15, 19, 45, tzinfo=timezone.utc),
        game_type=GameType.REGULAR,
        game_status=GameStatus.SCHEDULED,
        venue_info=venue_info,
        weather_data=weather_data,
        pitcher_matchup=pitcher_matchup,
        game_context=game_context,
        mlb_correlation_confidence=0.95,
        data_quality=DataQuality.HIGH
    )
    
    print(f"✅ Created game: {game.matchup_display}")
    print(f"   MLB Game ID: {game.mlb_game_id}")
    print(f"   Venue: {game.venue_info.venue_name}")
    condition_str = game.weather_data.condition.value if hasattr(game.weather_data.condition, 'value') else str(game.weather_data.condition)
    print(f"   Weather: {condition_str} ({game.weather_data.temperature}°F)")
    print(f"   Pitching: {game.pitcher_matchup.handedness_matchup}")
    quality_str = game.data_quality.value if hasattr(game.data_quality, 'value') else str(game.data_quality)
    print(f"   Data Quality: {quality_str}")
    print()
    
    return game


def create_sample_odds_data(game_id: str) -> OddsData:
    """Create sample odds data with line movement."""
    
    print("📊 Creating sample Odds Data...")
    
    # Create line movement data
    line_movement = LineMovementData(
        line_value=-1.5  # Home team -1.5
    )
    
    # Create odds data
    odds_data = OddsData(
        game_id=game_id,
        sportsbook=SportsbookName.DRAFTKINGS,
        bet_type=BetType.SPREAD,
        market_side=MarketSide.HOME_SPREAD,
        line_movement=line_movement,
        data_quality=DataQuality.HIGH,
        source_confidence=0.95
    )
    
    # Add some odds movement
    print("   Adding odds movement...")
    odds_data.add_odds_update(-110)  # Opening odds
    odds_data.add_odds_update(-115)  # Line moved down
    odds_data.add_odds_update(-108)  # Line moved back up
    odds_data.add_odds_update(-112)  # Current odds
    
    bet_type_str = odds_data.bet_type.value if hasattr(odds_data.bet_type, 'value') else str(odds_data.bet_type)
    market_side_str = odds_data.market_side.value if hasattr(odds_data.market_side, 'value') else str(odds_data.market_side)
    sportsbook_str = odds_data.sportsbook.value if hasattr(odds_data.sportsbook, 'value') else str(odds_data.sportsbook)
    movement_str = odds_data.line_movement.movement_direction.value if hasattr(odds_data.line_movement.movement_direction, 'value') else str(odds_data.line_movement.movement_direction)
    
    print(f"✅ Created odds for {bet_type_str} - {market_side_str}")
    print(f"   Sportsbook: {sportsbook_str}")
    print(f"   Line: {odds_data.line_movement.line_value}")
    print(f"   Opening odds: {odds_data.line_movement.opening_odds.american_odds}")
    print(f"   Current odds: {odds_data.current_odds.american_odds}")
    print(f"   Total movements: {odds_data.line_movement.total_movements}")
    print(f"   Movement direction: {movement_str}")
    print()
    
    return odds_data


def create_sample_sportsbook_mapping() -> SportsbookMapping:
    """Create sample sportsbook mapping."""
    
    print("🏢 Creating sample Sportsbook Mapping...")
    
    # Create capabilities
    capabilities = SportsbookCapabilities(
        supports_live_betting=True,
        supports_props=True,
        supports_parlays=True,
        supports_early_cash_out=True,
        typical_juice_percentage=4.5,
        odds_update_frequency_minutes=2,
        minimum_bet_amount=0.10,
        maximum_bet_amount=25000.0,
        available_states=["CA", "NY", "NJ", "PA", "MI"]
    )
    
    # Create sportsbook mapping
    sportsbook_mapping = SportsbookMapping(
        sportsbook_name=SportsbookName.DRAFTKINGS,
        display_name="DraftKings Sportsbook",
        short_name="DK",
        base_url="https://sportsbook.draftkings.com",
        odds_page_pattern="https://sportsbook.draftkings.com/leagues/baseball/mlb",
        capabilities=capabilities,
        rate_limit_requests_per_minute=30,
        rate_limit_delay_seconds=1.0,
        is_active=True,
        reliability_score=0.95
    )
    
    # Add market mappings
    moneyline_mapping = MarketMapping(
        bet_type=BetType.MONEYLINE,
        availability=MarketAvailability.ALWAYS_AVAILABLE,
        display_name="Moneyline",
        internal_name="ML",
        opens_hours_before_game=72.0,
        closes_minutes_before_game=5.0,
        typical_juice_range={"min": 2.0, "max": 8.0}
    )
    
    spread_mapping = MarketMapping(
        bet_type=BetType.SPREAD,
        availability=MarketAvailability.ALWAYS_AVAILABLE,
        display_name="Run Line",
        internal_name="RL",
        typical_lines=[-1.5, +1.5],
        line_increment=0.5,
        opens_hours_before_game=72.0,
        closes_minutes_before_game=5.0,
        typical_juice_range={"min": 3.0, "max": 10.0}
    )
    
    sportsbook_mapping.add_market_mapping(BetType.MONEYLINE, moneyline_mapping)
    sportsbook_mapping.add_market_mapping(BetType.SPREAD, spread_mapping)
    
    print(f"✅ Created mapping for {sportsbook_mapping.display_name}")
    supported_types = [bt.value if hasattr(bt, 'value') else str(bt) for bt in sportsbook_mapping.supported_bet_types]
    print(f"   Supported bet types: {supported_types}")
    print(f"   Reliability score: {sportsbook_mapping.reliability_score}")
    print(f"   Rate limit: {sportsbook_mapping.rate_limit_requests_per_minute} req/min")
    print()
    
    return sportsbook_mapping


async def test_mlb_enrichment_service():
    """Test the MLB Data Enrichment Service."""
    
    print("🔬 Testing MLB Data Enrichment Service...")
    
    try:
        # Get the service
        enrichment_service = get_mlb_data_enrichment_service()
        print("✅ MLB Data Enrichment Service initialized")
        
        # Create a game without MLB enrichment data
        basic_game = EnhancedGame(
            sbr_game_id="sbr-test-game-1",
            home_team=Team.LAD,
            away_team=Team.SF,
            game_date=datetime(2024, 7, 15, 19, 45, tzinfo=timezone.utc),
            game_type=GameType.REGULAR,
            game_status=GameStatus.SCHEDULED
        )
        
        print(f"📋 Created basic game: {basic_game.matchup_display}")
        print(f"   Has MLB enrichment: {basic_game.has_mlb_enrichment}")
        
        # Note: This would normally enrich with real MLB data
        print("ℹ️  Note: Live MLB API enrichment would happen here in production")
        print("   For demo purposes, we'll show the enrichment structure")
        
        # Simulate what enrichment would look like
        correlation = MLBGameCorrelation(
            mlb_game_id="746078",
            confidence_score=0.95,
            venue_info=VenueInfo(
                venue_id=22,
                venue_name="Dodger Stadium",
                city="Los Angeles",
                state="CA"
            ),
            weather_data=WeatherData(
                condition=WeatherCondition.CLEAR,
                temperature=72
            )
        )
        
        print(f"✅ Simulated enrichment correlation:")
        print(f"   MLB Game ID: {correlation.mlb_game_id}")
        print(f"   Confidence: {correlation.confidence_score}")
        print(f"   Venue: {correlation.venue_info.venue_name if correlation.venue_info else 'N/A'}")
        
        # Get service statistics
        stats = enrichment_service.get_enrichment_stats()
        print(f"📈 Service statistics:")
        for key, value in stats.items():
            print(f"   {key}: {value}")
        
    except Exception as e:
        print(f"❌ Error testing MLB enrichment service: {e}")
    
    print()


def test_model_validation():
    """Test model validation capabilities."""
    
    print("🔍 Testing Model Validation...")
    
    try:
        # Test odds validation
        odds_data = OddsData(
            game_id="test-game",
            sportsbook=SportsbookName.FANDUEL,
            bet_type=BetType.MONEYLINE,
            market_side=MarketSide.HOME,
            line_movement=LineMovementData()
        )
        
        # Add valid odds
        odds_data.add_odds_update("+150")
        print("✅ Valid odds format accepted: +150")
        
        # Test validation methods
        issues = odds_data.validate_odds_consistency()
        print(f"✅ Odds validation completed: {len(issues)} issues found")
        
        # Test enhanced game validation
        game = EnhancedGame(
            sbr_game_id="validation-test",
            home_team=Team.NYY,
            away_team=Team.BOS,  # Different from home team - should pass
            game_date=datetime.now(timezone.utc),
            game_type=GameType.REGULAR,
            game_status=GameStatus.FINAL
        )
        
        print("✅ Enhanced game validation passed")
        
        # Test computed properties
        print(f"   Matchup display: {game.matchup_display}")
        print(f"   Is completed: {game.is_completed}")
        print(f"   Has MLB enrichment: {game.has_mlb_enrichment}")
        print(f"   Correlation key: {game.get_correlation_key()}")
        
    except Exception as e:
        print(f"❌ Validation error: {e}")
    
    print()


async def main():
    """Run the complete demo."""
    
    print("=" * 60)
    print("🚀 SportsbookReview System Demo")
    print("=" * 60)
    print()
    
    # Create sample data
    enhanced_game = create_sample_enhanced_game()
    odds_data = create_sample_odds_data(enhanced_game.sbr_game_id)
    sportsbook_mapping = create_sample_sportsbook_mapping()
    
    # Test validation
    test_model_validation()
    
    # Test MLB enrichment service
    await test_mlb_enrichment_service()
    
    # Show integration example
    print("🔗 Integration Example...")
    print(f"   Game: {enhanced_game.sbr_game_id}")
    print(f"   MLB Game ID: {enhanced_game.mlb_game_id}")
    print(f"   Odds available: {odds_data.is_active}")
    print(f"   Sportsbook reliable: {sportsbook_mapping.is_reliable}")
    print(f"   Market available: {sportsbook_mapping.is_market_available(odds_data.bet_type)}")
    print()
    
    # Show model export example
    print("📤 Model Export Examples...")
    
    # Game as dictionary
    game_dict = enhanced_game.model_dump(exclude_none=True)
    print(f"   Enhanced Game fields: {len(game_dict)} fields")
    
    # Odds as dictionary  
    odds_dict = odds_data.model_dump(exclude_none=True)
    print(f"   Odds Data fields: {len(odds_dict)} fields")
    
    # Sportsbook mapping as dictionary
    mapping_dict = sportsbook_mapping.model_dump(exclude_none=True)
    print(f"   Sportsbook Mapping fields: {len(mapping_dict)} fields")
    print()
    
    print("✅ Demo completed successfully!")
    print("   📋 Models: Enhanced games with MLB API integration")
    print("   📊 Odds: Comprehensive line movement tracking")
    print("   🏢 Mapping: Sportsbook normalization and capabilities")
    print("   🔬 Service: MLB Stats API enrichment ready")
    print()
    print("🎯 Next Steps:")
    print("   1. Implement SportsbookReview scrapers")
    print("   2. Add game results orchestrator service")
    print("   3. Build historical data backfill (April 4th → current)")
    print("   4. Set up daily update workflows")


if __name__ == "__main__":
    asyncio.run(main()) 