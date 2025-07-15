"""
Basic tests for MLB betting splits functionality
"""

import os
import sys

import pytest

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime

from examples.python_classes import MoneylineSplit, SpreadSplit, TotalSplit


def test_spread_split_creation():
    """Test that SpreadSplit objects can be created correctly"""
    split = SpreadSplit(
        game_id="test_game",
        home_team="Cubs",
        away_team="Pirates",
        game_datetime=datetime.now(),
        last_updated=datetime.now(),
        spread_value="-1.5/+1.5",
        source="SBD",
        book=None,
    )

    assert split.game_id == "test_game"
    assert split.home_team == "Cubs"
    assert split.away_team == "Pirates"
    assert split.source == "SBD"
    assert split.book is None
    assert split.spread_value == "-1.5/+1.5"


def test_total_split_creation():
    """Test that TotalSplit objects can be created correctly"""
    split = TotalSplit(
        game_id="test_game",
        home_team="Cubs",
        away_team="Pirates",
        game_datetime=datetime.now(),
        last_updated=datetime.now(),
        total_value="8.5",
        source="SBD",
        book=None,
    )

    assert split.total_value == "8.5"
    assert split.source == "SBD"


def test_moneyline_split_creation():
    """Test that MoneylineSplit objects can be created correctly"""
    split = MoneylineSplit(
        game_id="test_game",
        home_team="Cubs",
        away_team="Pirates",
        game_datetime=datetime.now(),
        last_updated=datetime.now(),
        moneyline_value="-110/+105",
        source="SBD",
        book=None,
    )

    assert split.moneyline_value == "-110/+105"
    assert split.source == "SBD"


if __name__ == "__main__":
    pytest.main([__file__])
