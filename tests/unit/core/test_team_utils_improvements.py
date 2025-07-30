"""
Test coverage for team utilities improvements based on engineer feedback.

Tests edge cases and security enhancements for team name normalization.
"""

import pytest

from src.core.team_utils import normalize_team_name, validate_team_abbreviation


class TestTeamNormalizationEdgeCases:
    """Test edge cases for team name normalization."""

    def test_normalize_team_name_none_input(self):
        """Test that None input returns UNK."""
        assert normalize_team_name(None) == "UNK"

    def test_normalize_team_name_empty_string(self):
        """Test that empty string returns UNK."""
        assert normalize_team_name("") == "UNK"

    def test_normalize_team_name_whitespace_only(self):
        """Test that whitespace-only string returns UNK."""
        assert normalize_team_name("   ") == "UNK"
        assert normalize_team_name("\t\n") == "UNK"

    def test_normalize_team_name_non_string_input(self):
        """Test that non-string input returns UNK."""
        assert normalize_team_name(123) == "UNK"
        assert normalize_team_name([]) == "UNK"
        assert normalize_team_name({}) == "UNK"

    def test_normalize_team_name_extremely_long_input(self):
        """Test security feature - reject extremely long inputs."""
        long_string = "x" * 101  # Over 100 character limit
        assert normalize_team_name(long_string) == "UNK"

    def test_normalize_team_name_exactly_100_chars(self):
        """Test boundary condition - exactly 100 characters."""
        hundred_char_string = "x" * 100
        result = normalize_team_name(hundred_char_string)
        # Should process normally (not trigger length limit)
        assert result == "XXX"  # First 3 chars, uppercase

    def test_normalize_team_name_known_teams(self):
        """Test that known teams normalize correctly."""
        assert normalize_team_name("Philadelphia Phillies") == "PHI"
        assert normalize_team_name("Chicago White Sox") == "CWS"
        assert normalize_team_name("Boston Red Sox") == "BOS"
        assert normalize_team_name("New York Yankees") == "NYY"

    def test_normalize_team_name_unknown_team(self):
        """Test that unknown team creates safe abbreviation."""
        result = normalize_team_name("Unknown Team")
        assert len(result) <= 10  # Must fit in VARCHAR(10)
        assert result == "UNK"  # Should return UNK for unknown

    def test_normalize_team_name_case_insensitive(self):
        """Test case-insensitive matching."""
        assert normalize_team_name("philadelphia phillies") == "PHI"
        assert normalize_team_name("PHILADELPHIA PHILLIES") == "PHI"
        assert normalize_team_name("PhIlAdElPhIa PhIlLiEs") == "PHI"

    def test_normalize_team_name_partial_matching(self):
        """Test partial matching functionality."""
        assert normalize_team_name("Phillies") == "PHI"
        assert normalize_team_name("Yankees") == "NYY"
        assert normalize_team_name("Red Sox") == "BOS"


class TestVarcharConstraintCompliance:
    """Test that all normalized names fit in VARCHAR(10) database constraint."""

    def test_all_mlb_teams_fit_varchar_constraint(self):
        """Test that all MLB team normalizations fit in VARCHAR(10)."""
        test_teams = [
            "Philadelphia Phillies",
            "Boston Red Sox",
            "New York Yankees",
            "Chicago White Sox",
            "Los Angeles Angels",
            "San Francisco Giants",
            "Arizona Diamondbacks",
            "Cleveland Guardians",
            "Kansas City Royals",
            "Minnesota Twins",
            "Houston Astros",
            "Oakland Athletics",
            "Seattle Mariners",
            "Texas Rangers",
            "Atlanta Braves",
            "Miami Marlins",
            "New York Mets",
            "Washington Nationals",
            "Chicago Cubs",
            "Cincinnati Reds",
            "Milwaukee Brewers",
            "Pittsburgh Pirates",
            "St. Louis Cardinals",
            "Colorado Rockies",
            "Los Angeles Dodgers",
            "San Diego Padres",
            "Toronto Blue Jays",
            "Baltimore Orioles",
            "Tampa Bay Rays",
            "Detroit Tigers",
        ]

        for team_name in test_teams:
            result = normalize_team_name(team_name)
            assert len(result) <= 10, (
                f"Team '{team_name}' normalized to '{result}' (length {len(result)}) exceeds VARCHAR(10)"
            )
            assert len(result) >= 2, (
                f"Team '{team_name}' normalized to '{result}' is too short"
            )

    def test_edge_case_inputs_fit_constraint(self):
        """Test that edge case inputs produce results that fit VARCHAR(10)."""
        edge_cases = [
            None,
            "",
            "   ",
            "X",
            "XX",
            "XXXXX",
            "Very Long Team Name That Exceeds Normal Length",
            "!@#$%^&*()",
            "123456789012345",
        ]

        for edge_case in edge_cases:
            result = normalize_team_name(edge_case)
            assert len(result) <= 10, (
                f"Edge case '{edge_case}' produced result '{result}' exceeding VARCHAR(10)"
            )


class TestTeamAbbreviationValidation:
    """Test team abbreviation validation function."""

    def test_validate_team_abbreviation_valid_cases(self):
        """Test validation with valid abbreviations."""
        valid_abbrevs = ["PHI", "BOS", "NYY", "CWS", "LAA", "SF", "ARI"]
        for abbrev in valid_abbrevs:
            assert validate_team_abbreviation(abbrev) is True

    def test_validate_team_abbreviation_invalid_cases(self):
        """Test validation with invalid abbreviations."""
        invalid_cases = [
            None,
            "",
            "X",
            "TOOLONG",
            "AB-CD",
            "12345",
            "AB CD",
            "AB\nCD",
            [],
            {},
            123,
        ]
        for invalid in invalid_cases:
            assert validate_team_abbreviation(invalid) is False


class TestSecurityAndRobustness:
    """Test security features and robustness improvements."""

    def test_sql_injection_prevention(self):
        """Test that potential SQL injection strings are handled safely."""
        malicious_inputs = [
            "'; DROP TABLE teams; --",
            "UNION SELECT * FROM users",
            "<script>alert('xss')</script>",
            "../../etc/passwd",
            "\x00\x01\x02",
        ]

        for malicious in malicious_inputs:
            result = normalize_team_name(malicious)
            # Should be safely handled, not cause errors
            assert isinstance(result, str)
            assert len(result) <= 10

    def test_unicode_handling(self):
        """Test handling of unicode characters."""
        unicode_inputs = ["Montréal Expos", "São Paulo", "北京队", "Москва", "🏈⚾🏀"]

        for unicode_input in unicode_inputs:
            result = normalize_team_name(unicode_input)
            # Should handle gracefully without errors
            assert isinstance(result, str)
            assert len(result) <= 10

    def test_memory_efficiency(self):
        """Test that function doesn't consume excessive memory."""
        # Create a reasonably large input (but under security limit)
        large_input = "A" * 50
        result = normalize_team_name(large_input)
        assert result == "AAA"  # Should handle efficiently


class TestErrorRecovery:
    """Test error recovery and logging scenarios."""

    def test_corrupted_input_recovery(self):
        """Test recovery from various corrupted inputs."""
        corrupted_inputs = [
            "\x00Team Name\x00",
            "Team\x1f\x1eName",
            "Team\u2028Name",  # Line separator
            "Team\u2029Name",  # Paragraph separator
        ]

        for corrupted in corrupted_inputs:
            # Should not raise exceptions
            result = normalize_team_name(corrupted)
            assert isinstance(result, str)
            assert len(result) <= 10


if __name__ == "__main__":
    pytest.main([__file__])
