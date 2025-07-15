"""
Tests for Pinnacle API integration.

This module provides tests for the Pinnacle API service, parser,
and data models to ensure proper functionality.
"""

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from mlb_sharp_betting.models.game import Team
from mlb_sharp_betting.models.pinnacle import (
    LimitType,
    MarketStatus,
    PinnacleLimit,
    PinnacleMarket,
    PinnacleMarketType,
    PinnaclePrice,
    PriceDesignation,
)
from mlb_sharp_betting.models.splits import BettingSplit, SplitType
from mlb_sharp_betting.parsers.pinnacle import PinnacleParser, parse_pinnacle_data
from mlb_sharp_betting.services.pinnacle_api_service import (
    PinnacleAPIConfig,
    PinnacleAPIService,
)


class TestPinnacleModels:
    """Test Pinnacle data models."""

    def test_pinnacle_price_creation(self):
        """Test PinnaclePrice model creation and properties."""
        price = PinnaclePrice(price=-110, designation=PriceDesignation.HOME)

        assert price.price == -110
        assert price.designation == PriceDesignation.HOME
        assert price.is_favorite is True
        assert price.is_underdog is False
        assert abs(price.decimal_odds - 1.909) < 0.01
        assert abs(price.implied_probability - 52.38) < 0.1

    def test_pinnacle_limit_creation(self):
        """Test PinnacleLimit model creation."""
        limit = PinnacleLimit(amount=2000.00, type=LimitType.MAX_RISK_STAKE)

        assert limit.amount == Decimal("2000.00")
        assert limit.type == LimitType.MAX_RISK_STAKE
        assert limit.amount_float == 2000.0

    def test_pinnacle_market_creation(self):
        """Test PinnacleMarket model creation with validation."""
        prices = [
            PinnaclePrice(price=169, designation=PriceDesignation.HOME),
            PinnaclePrice(price=-189, designation=PriceDesignation.AWAY),
        ]

        limits = [PinnacleLimit(amount=2000, type=LimitType.MAX_RISK_STAKE)]

        market = PinnacleMarket(
            matchup_id=1610721342,
            market_type=PinnacleMarketType.MONEYLINE,
            key="s;0;m",
            home_team=Team.OAK,
            away_team=Team.HOU,
            game_datetime=datetime(2025, 6, 17, 2, 5, tzinfo=timezone.utc),
            period=0,
            status=MarketStatus.OPEN,
            cutoff_at=datetime(2025, 6, 17, 22, 40, tzinfo=timezone.utc),
            prices=prices,
            limits=limits,
            version=3149186706,
        )

        assert market.matchup_id == 1610721342
        assert market.market_type == PinnacleMarketType.MONEYLINE
        assert market.home_team == Team.OAK
        assert market.away_team == Team.HOU
        assert len(market.prices) == 2
        assert len(market.limits) == 1

        # Test convenience methods
        home_price = market.get_home_price()
        away_price = market.get_away_price()
        assert home_price.price == 169
        assert away_price.price == -189

        max_limit = market.get_max_risk_limit()
        assert max_limit.amount == Decimal("2000")

    def test_market_validation_errors(self):
        """Test that market validation catches errors."""
        prices = [
            PinnaclePrice(price=169, designation=PriceDesignation.HOME)
            # Missing away price for moneyline
        ]

        limits = [PinnacleLimit(amount=2000, type=LimitType.MAX_RISK_STAKE)]

        with pytest.raises(
            ValueError, match="Moneyline market must have home and away prices"
        ):
            PinnacleMarket(
                matchup_id=1610721342,
                market_type=PinnacleMarketType.MONEYLINE,
                key="s;0;m",
                home_team=Team.OAK,
                away_team=Team.HOU,
                game_datetime=datetime.now(timezone.utc),
                period=0,
                status=MarketStatus.OPEN,
                cutoff_at=datetime.now(timezone.utc),
                prices=prices,
                limits=limits,
                version=1,
            )


class TestPinnacleAPIService:
    """Test Pinnacle API service."""

    @pytest.fixture
    def api_service(self):
        """Create a Pinnacle API service instance."""
        config = PinnacleAPIConfig(
            rate_limit_delay=0.01,  # Faster for tests
            max_retries=1,  # Faster failure for tests
        )
        return PinnacleAPIService(config)

    def test_team_name_normalization(self, api_service):
        """Test team name normalization."""
        assert api_service._normalize_team_name("Athletics") == Team.OAK
        assert api_service._normalize_team_name("Oakland Athletics") == Team.OAK
        assert api_service._normalize_team_name("Houston Astros") == Team.HOU
        assert api_service._normalize_team_name("Yankees") == Team.NYY
        assert api_service._normalize_team_name("Invalid Team") is None

    def test_market_type_determination(self, api_service):
        """Test market type determination."""
        assert (
            api_service._determine_market_type({"type": "moneyline"})
            == PinnacleMarketType.MONEYLINE
        )
        assert (
            api_service._determine_market_type({"type": "spread"})
            == PinnacleMarketType.SPREAD
        )
        assert (
            api_service._determine_market_type({"type": "total"})
            == PinnacleMarketType.TOTAL
        )
        assert (
            api_service._determine_market_type({"type": "special"})
            == PinnacleMarketType.SPECIAL
        )
        assert api_service._determine_market_type({"type": "unknown"}) is None

    def test_price_parsing(self, api_service):
        """Test price parsing from raw data."""
        prices_data = [
            {"price": 169, "designation": "home"},
            {"price": -189, "designation": "away"},
        ]

        prices = api_service._parse_prices(prices_data, PinnacleMarketType.MONEYLINE)

        assert len(prices) == 2
        assert prices[0].price == 169
        assert prices[0].designation == PriceDesignation.HOME
        assert prices[1].price == -189
        assert prices[1].designation == PriceDesignation.AWAY

    def test_limit_parsing(self, api_service):
        """Test limit parsing from raw data."""
        limits_data = [
            {"amount": 2000, "type": "maxRiskStake"},
            {"amount": 1500, "type": "maxWinStake"},
        ]

        limits = api_service._parse_limits(limits_data)

        assert len(limits) == 2
        assert limits[0].amount == Decimal("2000")
        assert limits[0].type == LimitType.MAX_RISK_STAKE
        assert limits[1].amount == Decimal("1500")
        assert limits[1].type == LimitType.MAX_WIN_STAKE


class TestPinnacleParser:
    """Test Pinnacle parser."""

    @pytest.fixture
    def parser(self):
        """Create a Pinnacle parser instance."""
        return PinnacleParser()

    @pytest.fixture
    def sample_moneyline_market(self):
        """Create a sample moneyline market."""
        prices = [
            PinnaclePrice(price=169, designation=PriceDesignation.HOME),
            PinnaclePrice(price=-189, designation=PriceDesignation.AWAY),
        ]

        limits = [PinnacleLimit(amount=2000, type=LimitType.MAX_RISK_STAKE)]

        return PinnacleMarket(
            matchup_id=1610721342,
            market_type=PinnacleMarketType.MONEYLINE,
            key="s;0;m",
            home_team=Team.OAK,
            away_team=Team.HOU,
            game_datetime=datetime(2025, 6, 17, 2, 5, tzinfo=timezone.utc),
            period=0,
            status=MarketStatus.OPEN,
            cutoff_at=datetime(2025, 6, 17, 22, 40, tzinfo=timezone.utc),
            prices=prices,
            limits=limits,
            version=3149186706,
            last_updated=datetime.now(timezone.utc),
        )

    @pytest.fixture
    def sample_spread_market(self):
        """Create a sample spread market."""
        prices = [
            PinnaclePrice(price=-110, designation=PriceDesignation.HOME),
            PinnaclePrice(price=-110, designation=PriceDesignation.AWAY),
        ]

        limits = [PinnacleLimit(amount=2000, type=LimitType.MAX_RISK_STAKE)]

        return PinnacleMarket(
            matchup_id=1610721342,
            market_type=PinnacleMarketType.SPREAD,
            key="s;0;s",
            home_team=Team.OAK,
            away_team=Team.HOU,
            game_datetime=datetime(2025, 6, 17, 2, 5, tzinfo=timezone.utc),
            period=0,
            status=MarketStatus.OPEN,
            cutoff_at=datetime(2025, 6, 17, 22, 40, tzinfo=timezone.utc),
            line_value=-1.5,
            prices=prices,
            limits=limits,
            version=3149186706,
            last_updated=datetime.now(timezone.utc),
        )

    async def test_parse_moneyline_market(self, parser, sample_moneyline_market):
        """Test parsing a moneyline market."""
        raw_data = {"market": sample_moneyline_market}

        betting_split = await parser.parse_raw_data(raw_data)

        assert betting_split is not None
        assert isinstance(betting_split, BettingSplit)
        assert betting_split.split_type == SplitType.MONEYLINE
        assert betting_split.home_team == Team.OAK
        assert betting_split.away_team == Team.HOU
        assert betting_split.split_value is None  # No line value for moneyline

        # Check that percentages are normalized and reasonable
        assert betting_split.home_or_over_bets_percentage is not None
        assert betting_split.away_or_under_bets_percentage is not None
        assert 0 <= betting_split.home_or_over_bets_percentage <= 100
        assert 0 <= betting_split.away_or_under_bets_percentage <= 100

        # Percentages should roughly add up to 100 (allowing for small rounding errors)
        total = (
            betting_split.home_or_over_bets_percentage
            + betting_split.away_or_under_bets_percentage
        )
        assert abs(total - 100) < 1.0

    async def test_parse_spread_market(self, parser, sample_spread_market):
        """Test parsing a spread market."""
        raw_data = {"market": sample_spread_market}

        betting_split = await parser.parse_raw_data(raw_data)

        assert betting_split is not None
        assert isinstance(betting_split, BettingSplit)
        assert betting_split.split_type == SplitType.SPREAD
        assert betting_split.home_team == Team.OAK
        assert betting_split.away_team == Team.HOU
        assert betting_split.split_value == -1.5

        # Check that percentages are normalized
        assert betting_split.home_or_over_bets_percentage is not None
        assert betting_split.away_or_under_bets_percentage is not None

    async def test_parse_multiple_markets(
        self, parser, sample_moneyline_market, sample_spread_market
    ):
        """Test parsing multiple markets."""
        markets = [sample_moneyline_market, sample_spread_market]

        betting_splits = await parser.parse_pinnacle_markets(markets)

        assert len(betting_splits) == 2
        assert all(isinstance(split, BettingSplit) for split in betting_splits)

        # Check that we got both market types
        split_types = {split.split_type for split in betting_splits}
        assert SplitType.MONEYLINE in split_types
        assert SplitType.SPREAD in split_types

    async def test_parse_invalid_market(self, parser):
        """Test parsing invalid market data."""
        # Test missing market data
        raw_data = {}
        result = await parser.parse_raw_data(raw_data)
        assert result is None

        # Test invalid market object
        raw_data = {"market": "not a market object"}
        result = await parser.parse_raw_data(raw_data)
        assert result is None


class TestPinnacleIntegration:
    """Integration tests for the complete Pinnacle system."""

    async def test_end_to_end_workflow(self):
        """Test a complete end-to-end workflow."""
        # Create sample market data (as would come from API)
        prices = [
            PinnaclePrice(price=169, designation=PriceDesignation.HOME),
            PinnaclePrice(price=-189, designation=PriceDesignation.AWAY),
        ]

        limits = [PinnacleLimit(amount=2000, type=LimitType.MAX_RISK_STAKE)]

        market = PinnacleMarket(
            matchup_id=1610721342,
            market_type=PinnacleMarketType.MONEYLINE,
            key="s;0;m",
            home_team=Team.OAK,
            away_team=Team.HOU,
            game_datetime=datetime(2025, 6, 17, 2, 5, tzinfo=timezone.utc),
            period=0,
            status=MarketStatus.OPEN,
            cutoff_at=datetime(2025, 6, 17, 22, 40, tzinfo=timezone.utc),
            prices=prices,
            limits=limits,
            version=3149186706,
            last_updated=datetime.now(timezone.utc),
        )

        # Parse market into betting split
        betting_splits = await parse_pinnacle_data([market])

        assert len(betting_splits) == 1
        split = betting_splits[0]

        # Verify the split contains expected data
        assert split.game_id is not None
        assert split.home_team == Team.OAK
        assert split.away_team == Team.HOU
        assert split.split_type == SplitType.MONEYLINE

        # Verify implied probabilities make sense
        home_prob = split.home_or_over_bets_percentage
        away_prob = split.away_or_under_bets_percentage

        # Home team has positive odds (+169) so should have lower probability
        # Away team has negative odds (-189) so should have higher probability
        assert away_prob > home_prob
        assert abs((home_prob + away_prob) - 100) < 1.0  # Should sum to ~100%


if __name__ == "__main__":
    pytest.main([__file__])
