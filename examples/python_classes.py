from datetime import datetime
from enum import Enum


class OutcomeType(Enum):
    HOME = "Home"
    AWAY = "Away"
    OVER = "Over"
    UNDER = "Under"
    PUSH = "Push"
    YES = "Yes"  # For potential future expansion (e.g., prop bets)
    NO = "No"  # For potential future expansion (e.g., prop bets)


class SplitType(Enum):
    SPREAD = "Spread"
    TOTAL = "Total"
    MONEYLINE = "Moneyline"


class Source(Enum):
    SBD = "SBD"  # SportsBettingDime
    VSIN = "VSIN"  # VSIN source


class Game:
    def __init__(
        self, game_id: str, home_team: str, away_team: str, game_datetime: datetime
    ):
        self.game_id = (
            game_id  # A unique identifier for the game (e.g., "PIT@CHC_2025-06-13")
        )
        self.home_team = home_team
        self.away_team = away_team
        self.game_datetime = game_datetime

    def __repr__(self):
        return f"Game(ID='{self.game_id}', {self.away_team} @ {self.home_team} on {self.game_datetime.strftime('%Y-%m-%d %H:%M')})"


class BaseSplit:
    def __init__(
        self,
        game_id: str,
        home_team: str,
        away_team: str,
        game_datetime: datetime,
        last_updated: datetime,
        source: str = "SBD",
        book: str = None,
        sharp_action: bool = False,
        outcome: str = None,
    ):
        self.game_id = game_id
        self.home_team = home_team
        self.away_team = away_team
        self.game_datetime = game_datetime
        self.last_updated = last_updated
        self.source = source  # "SBD" or "VSIN"
        self.book = book  # NULL for SBD, specific book for VSIN (e.g., "DK", "Circa")
        self.sharp_action = sharp_action
        self.outcome = outcome

    @property
    def home_or_over_bets(self):
        """Abstract property for home/over bets"""
        raise NotImplementedError

    @property
    def home_or_over_bets_percentage(self):
        """Abstract property for home/over bets percentage"""
        raise NotImplementedError

    @property
    def home_or_over_stake_percentage(self):
        """Abstract property for home/over stake percentage"""
        raise NotImplementedError

    @property
    def away_or_under_bets(self):
        """Abstract property for away/under bets"""
        raise NotImplementedError

    @property
    def away_or_under_bets_percentage(self):
        """Abstract property for away/under bets percentage"""
        raise NotImplementedError

    @property
    def away_or_under_stake_percentage(self):
        """Abstract property for away/under stake percentage"""
        raise NotImplementedError

    @property
    def split_value(self):
        """Abstract property for split-specific value (spread line, total, moneyline)"""
        raise NotImplementedError


class SpreadSplit(BaseSplit):
    def __init__(
        self,
        game_id: str,
        home_team: str,
        away_team: str,
        game_datetime: datetime,
        last_updated: datetime,
        spread_value: str,
        home_team_bets: int = None,
        home_team_bets_percentage: float = None,
        home_team_stake_percentage: float = None,
        away_team_bets: int = None,
        away_team_bets_percentage: float = None,
        away_team_stake_percentage: float = None,
        source: str = "SBD",
        book: str = None,
        sharp_action: bool = False,
        outcome: str = None,
    ):
        super().__init__(
            game_id,
            home_team,
            away_team,
            game_datetime,
            last_updated,
            source,
            book,
            sharp_action,
            outcome,
        )
        self.split_type = SplitType.SPREAD
        self.spread_value = spread_value
        self.home_team_bets = home_team_bets
        self.home_team_bets_percentage = home_team_bets_percentage
        self.home_team_stake_percentage = home_team_stake_percentage
        self.away_team_bets = away_team_bets
        self.away_team_bets_percentage = away_team_bets_percentage
        self.away_team_stake_percentage = away_team_stake_percentage

    # Long format properties - for spread, home_or_over = home team
    @property
    def home_or_over_bets(self):
        return self.home_team_bets

    @property
    def home_or_over_bets_percentage(self):
        return self.home_team_bets_percentage

    @property
    def home_or_over_stake_percentage(self):
        return self.home_team_stake_percentage

    @property
    def away_or_under_bets(self):
        return self.away_team_bets

    @property
    def away_or_under_bets_percentage(self):
        return self.away_team_bets_percentage

    @property
    def away_or_under_stake_percentage(self):
        return self.away_team_stake_percentage

    @property
    def split_value(self):
        return self.spread_value

    def __repr__(self):
        return (
            f"SpreadSplit(Game: {self.away_team} @ {self.home_team}, Spread: {self.spread_value}, "
            f"Home: {self.home_team_bets} bets ({self.home_team_bets_percentage}%), "
            f"Away: {self.away_team_bets} bets ({self.away_team_bets_percentage}%), "
            f"Outcome: {self.outcome}, Sharp: {self.sharp_action})"
        )


class TotalSplit(BaseSplit):
    def __init__(
        self,
        game_id: str,
        home_team: str,
        away_team: str,
        game_datetime: datetime,
        last_updated: datetime,
        total_value: str,
        over_bets: int = None,
        over_bets_percentage: float = None,
        over_stake_percentage: float = None,
        under_bets: int = None,
        under_bets_percentage: float = None,
        under_stake_percentage: float = None,
        source: str = "SBD",
        book: str = None,
        sharp_action: bool = False,
        outcome: str = None,
    ):
        super().__init__(
            game_id,
            home_team,
            away_team,
            game_datetime,
            last_updated,
            source,
            book,
            sharp_action,
            outcome,
        )
        self.split_type = SplitType.TOTAL
        self.total_value = total_value
        self.over_bets = over_bets
        self.over_bets_percentage = over_bets_percentage
        self.over_stake_percentage = over_stake_percentage
        self.under_bets = under_bets
        self.under_bets_percentage = under_bets_percentage
        self.under_stake_percentage = under_stake_percentage

    # Long format properties - for total, home_or_over = over
    @property
    def home_or_over_bets(self):
        return self.over_bets

    @property
    def home_or_over_bets_percentage(self):
        return self.over_bets_percentage

    @property
    def home_or_over_stake_percentage(self):
        return self.over_stake_percentage

    @property
    def away_or_under_bets(self):
        return self.under_bets

    @property
    def away_or_under_bets_percentage(self):
        return self.under_bets_percentage

    @property
    def away_or_under_stake_percentage(self):
        return self.under_stake_percentage

    @property
    def split_value(self):
        return self.total_value

    def __repr__(self):
        return (
            f"TotalSplit(Game: {self.away_team} @ {self.home_team}, Total: {self.total_value}, "
            f"Over: {self.over_bets} bets ({self.over_bets_percentage}%), "
            f"Under: {self.under_bets} bets ({self.under_bets_percentage}%), "
            f"Outcome: {self.outcome}, Sharp: {self.sharp_action})"
        )


class MoneylineSplit(BaseSplit):
    def __init__(
        self,
        game_id: str,
        home_team: str,
        away_team: str,
        game_datetime: datetime,
        last_updated: datetime,
        moneyline_value: str,
        home_team_bets: int = None,
        home_team_bets_percentage: float = None,
        home_team_stake_percentage: float = None,
        away_team_bets: int = None,
        away_team_bets_percentage: float = None,
        away_team_stake_percentage: float = None,
        source: str = "SBD",
        book: str = None,
        sharp_action: bool = False,
        outcome: str = None,
    ):
        super().__init__(
            game_id,
            home_team,
            away_team,
            game_datetime,
            last_updated,
            source,
            book,
            sharp_action,
            outcome,
        )
        self.split_type = SplitType.MONEYLINE
        self.moneyline_value = moneyline_value
        self.home_team_bets = home_team_bets
        self.home_team_bets_percentage = home_team_bets_percentage
        self.home_team_stake_percentage = home_team_stake_percentage
        self.away_team_bets = away_team_bets
        self.away_team_bets_percentage = away_team_bets_percentage
        self.away_team_stake_percentage = away_team_stake_percentage

    # Long format properties - for moneyline, home_or_over = home team
    @property
    def home_or_over_bets(self):
        return self.home_team_bets

    @property
    def home_or_over_bets_percentage(self):
        return self.home_team_bets_percentage

    @property
    def home_or_over_stake_percentage(self):
        return self.home_team_stake_percentage

    @property
    def away_or_under_bets(self):
        return self.away_team_bets

    @property
    def away_or_under_bets_percentage(self):
        return self.away_team_bets_percentage

    @property
    def away_or_under_stake_percentage(self):
        return self.away_team_stake_percentage

    @property
    def split_value(self):
        return self.moneyline_value

    def __repr__(self):
        return (
            f"MoneylineSplit(Game: {self.away_team} @ {self.home_team}, ML: {self.moneyline_value}, "
            f"Home: {self.home_team_bets} bets ({self.home_team_bets_percentage}%), "
            f"Away: {self.away_team_bets} bets ({self.away_team_bets_percentage}%), "
            f"Outcome: {self.outcome}, Sharp: {self.sharp_action})"
        )


# Legacy compatibility - keep the old classes for backward compatibility
class SplitDetail:
    def __init__(
        self,
        bets: int = None,
        bets_percentage: float = None,
        stake_percentage: float = None,
        money: float = None,
    ):
        self.bets = bets
        self.bets_percentage = bets_percentage
        self.stake_percentage = stake_percentage
        self.money = money  # For Website 2, which has a single 'Money' field

    def __repr__(self):
        details = []
        if self.bets is not None:
            details.append(f"Bets: {self.bets}")
        if self.bets_percentage is not None:
            details.append(f"Bets%: {self.bets_percentage:.2f}%")
        if self.stake_percentage is not None:
            details.append(f"Stake%: {self.stake_percentage:.2f}%")
        if self.money is not None:
            details.append(f"Money: {self.money:.2f}%")
        return f"{{{', '.join(details)}}}"


# Legacy Split class for backward compatibility
class Split:
    def __init__(
        self,
        game_id: str,
        source: Source,
        split_type: SplitType,
        last_updated: datetime,
        home_or_over_outcome: SplitDetail,
        away_or_under_outcome: SplitDetail,
        spread_value: str = None,
        total_value: str = None,
        moneyline_value: str = None,
        outcome: str = None,
    ):
        self.game_id = game_id
        self.source = source
        self.split_type = split_type
        self.last_updated = last_updated
        self.home_or_over_outcome = home_or_over_outcome
        self.away_or_under_outcome = away_or_under_outcome
        self.spread_value = spread_value
        self.total_value = total_value
        self.moneyline_value = moneyline_value
        self.outcome = outcome

    def __repr__(self):
        repr_str = (
            f"Split(Game ID: '{self.game_id}', Source: {self.source.value}, Type: {self.split_type.value}, "
            f"Last Updated: {self.last_updated.strftime('%Y-%m-%d %H:%M')}, "
            f"Home/Over: {self.home_or_over_outcome}, Away/Under: {self.away_or_under_outcome}"
        )
        if self.split_type == SplitType.SPREAD and self.spread_value:
            repr_str += f", Spread Value: {self.spread_value}"
        if self.split_type == SplitType.TOTAL and self.total_value:
            repr_str += f", Total Value: {self.total_value}"
        if self.split_type == SplitType.MONEYLINE and self.moneyline_value:
            repr_str += f", Moneyline Value: {self.moneyline_value}"
        if self.outcome is not None:
            repr_str += f", Outcome: {self.outcome}"
        return repr_str + ")"


#!/usr/bin/env python3
"""
Multi-Strategy Processor Example

Demonstrates how to use the StrategyProcessorFactory to load and utilize
multiple betting strategy processors for comprehensive bet detection.
"""

# Note: These legacy services have been migrated to the unified architecture
# from src.analysis.strategies.factory import StrategyFactory
# from src.services.strategy.strategy_manager_service import StrategyManagerService
# from src.data.database.connection import get_db_connection


async def demonstrate_multiple_processors():
    """
    Example of using multiple strategy processors for comprehensive bet detection
    """
    print("🏭 MULTI-STRATEGY PROCESSOR DEMONSTRATION")
    print("=" * 60)

    # Initialize dependencies
    config = SignalProcessorConfig()
    conn = get_db_connection()
    repo = BettingSignalRepository(conn)
    validator = StrategyValidation()

    # Create factory - this automatically loads all 9+ processors
    factory = StrategyProcessorFactory(repo, validator, config)

    # Get all loaded processors
    processors = factory.get_all_processors()
    print(f"\n✅ Loaded {len(processors)} strategy processors:")

    for name, processor in processors.items():
        signal_type = processor.get_signal_type()
        category = processor.get_strategy_category()
        description = processor.get_strategy_description()

        print(f"\n📊 {name.upper()}")
        print(f"   Class: {processor.__class__.__name__}")
        print(f"   Signal Type: {signal_type}")
        print(f"   Category: {category}")
        print(f"   Description: {description}")

    # Example: Get processors by signal type
    print("\n🔍 PROCESSORS BY SIGNAL TYPE:")
    signal_types = ["SHARP_ACTION", "BOOK_CONFLICTS", "PUBLIC_FADE"]

    for signal_type in signal_types:
        matching_processors = factory.get_processors_by_type(signal_type)
        print(f"   {signal_type}: {len(matching_processors)} processors")
        for proc in matching_processors:
            print(f"     - {proc.__class__.__name__}")

    # Example: Run multiple processors (simulated)
    print("\n🚀 RUNNING MULTIPLE PROCESSORS:")

    # Mock profitable strategies for demonstration
    mock_strategies = [
        ProfitableStrategy(
            strategy_name="sharp_action_vsin_draftkings",
            source_book="VSIN-DraftKings",
            split_type="moneyline",
            win_rate=0.58,
            roi=0.12,
            total_bets=45,
            confidence=0.85,
        ),
        ProfitableStrategy(
            strategy_name="book_conflicts_general",
            source_book="Multi-Book",
            split_type="spread",
            win_rate=0.62,
            roi=0.18,
            total_bets=32,
            confidence=0.78,
        ),
    ]

    # Process with each strategy type
    for name, processor in processors.items():
        try:
            print(f"   🔄 Processing with {name}...")

            # In real usage, you'd call: signals = await processor.process(120, mock_strategies)
            # For demo, just show the processor is ready
            print(f"     ✅ {processor.__class__.__name__} ready")

        except Exception as e:
            print(f"     ❌ {name} failed: {e}")

    print(f"\n🎯 CONCLUSION: {len(processors)} different strategy processors")
    print("   are working together to detect betting opportunities!")


if __name__ == "__main__":
    import asyncio

    asyncio.run(demonstrate_multiple_processors())
