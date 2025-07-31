"""
Unit tests for datetime utilities.

Tests timezone handling, EST/EDT conversion, and datetime operations.
"""

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, Mock

from src.core.datetime_utils import (
    get_eastern_timezone,
    convert_to_eastern,
    convert_utc_to_eastern,
    get_current_eastern_time,
    is_eastern_dst,
    format_eastern_datetime,
    parse_datetime_string
)
from tests.utils.logging_utils import create_test_logger, setup_secure_test_logging


class TestDateTimeUtils:
    """Test datetime utility functions."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test environment."""
        setup_secure_test_logging(log_level="INFO", include_sanitization=True)
        self.logger = create_test_logger("datetime_utils_test")
        self.logger.info("Starting datetime utils tests")
    
    def test_get_eastern_timezone(self):
        """Test getting Eastern timezone."""
        tz = get_eastern_timezone()
        assert tz is not None
        assert hasattr(tz, 'localize')
        self.logger.info("✅ Eastern timezone retrieved successfully")
    
    def test_convert_to_eastern_with_naive_datetime(self):
        """Test converting naive datetime to Eastern."""
        # Test with naive UTC datetime
        utc_dt = datetime(2024, 7, 30, 15, 30, 0)  # July 30, 3:30 PM UTC
        
        eastern_dt = convert_to_eastern(utc_dt, assume_utc=True)
        
        assert eastern_dt is not None
        assert eastern_dt.tzinfo is not None
        
        # In July, Eastern time is EDT (UTC-4)
        expected_hour = 11  # 15:30 UTC - 4 hours = 11:30 EDT
        assert eastern_dt.hour == expected_hour
        
        self.logger.info(f"✅ Naive datetime converted: {utc_dt} -> {eastern_dt}")
    
    def test_convert_to_eastern_with_aware_datetime(self):
        """Test converting timezone-aware datetime to Eastern."""
        # Test with timezone-aware datetime
        utc_dt = datetime(2024, 7, 30, 15, 30, 0, tzinfo=timezone.utc)
        
        eastern_dt = convert_to_eastern(utc_dt)
        
        assert eastern_dt is not None
        assert eastern_dt.tzinfo is not None
        
        # Should be EDT in July
        expected_hour = 11  # 15:30 UTC - 4 hours = 11:30 EDT
        assert eastern_dt.hour == expected_hour
        
        self.logger.info(f"✅ Aware datetime converted: {utc_dt} -> {eastern_dt}")
    
    def test_convert_utc_to_eastern_summer(self):
        """Test UTC to Eastern conversion in summer (EDT)."""
        # July date - should be EDT (UTC-4)
        utc_dt = datetime(2024, 7, 30, 20, 0, 0, tzinfo=timezone.utc)
        
        eastern_dt = convert_utc_to_eastern(utc_dt)
        
        assert eastern_dt.hour == 16  # 20:00 UTC - 4 hours = 16:00 EDT
        assert eastern_dt.tzinfo is not None
        
        self.logger.info(f"✅ Summer UTC conversion: {utc_dt} -> {eastern_dt}")
    
    def test_convert_utc_to_eastern_winter(self):
        """Test UTC to Eastern conversion in winter (EST)."""
        # January date - should be EST (UTC-5)
        utc_dt = datetime(2024, 1, 15, 20, 0, 0, tzinfo=timezone.utc)
        
        eastern_dt = convert_utc_to_eastern(utc_dt)
        
        assert eastern_dt.hour == 15  # 20:00 UTC - 5 hours = 15:00 EST
        assert eastern_dt.tzinfo is not None
        
        self.logger.info(f"✅ Winter UTC conversion: {utc_dt} -> {eastern_dt}")
    
    @patch('src.core.datetime_utils.datetime')
    def test_get_current_eastern_time(self, mock_datetime):
        """Test getting current Eastern time."""
        # Mock current UTC time
        mock_utc_now = datetime(2024, 7, 30, 18, 45, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = mock_utc_now
        
        eastern_time = get_current_eastern_time()
        
        assert eastern_time is not None
        assert eastern_time.tzinfo is not None
        
        # In July, should be EDT (UTC-4)
        expected_hour = 14  # 18:45 UTC - 4 hours = 14:45 EDT
        assert eastern_time.hour == expected_hour
        
        self.logger.info(f"✅ Current Eastern time: {eastern_time}")
    
    def test_is_eastern_dst_summer(self):
        """Test DST detection in summer."""
        # July - should be DST
        summer_dt = datetime(2024, 7, 30, 12, 0, 0)
        assert is_eastern_dst(summer_dt) is True
        
        self.logger.info("✅ Summer DST detection correct")
    
    def test_is_eastern_dst_winter(self):
        """Test DST detection in winter."""
        # January - should not be DST
        winter_dt = datetime(2024, 1, 15, 12, 0, 0)
        assert is_eastern_dst(winter_dt) is False
        
        self.logger.info("✅ Winter DST detection correct")
    
    def test_format_eastern_datetime(self):
        """Test formatting Eastern datetime."""
        dt = datetime(2024, 7, 30, 14, 30, 0)
        eastern_dt = convert_to_eastern(dt, assume_utc=True)
        
        # Test default format
        formatted = format_eastern_datetime(eastern_dt)
        assert isinstance(formatted, str)
        assert "2024" in formatted
        assert "07" in formatted or "7" in formatted
        assert "30" in formatted
        
        # Test custom format
        custom_format = "%Y-%m-%d %H:%M"
        formatted_custom = format_eastern_datetime(eastern_dt, fmt=custom_format)
        assert len(formatted_custom.split()) == 2  # Date and time parts
        
        self.logger.info(f"✅ Datetime formatted: {formatted}")
    
    def test_parse_datetime_string_iso_format(self):
        """Test parsing ISO format datetime string."""
        iso_string = "2024-07-30T19:00:00Z"
        
        parsed_dt = parse_datetime_string(iso_string)
        
        assert parsed_dt is not None
        assert parsed_dt.year == 2024
        assert parsed_dt.month == 7
        assert parsed_dt.day == 30
        assert parsed_dt.hour == 19
        
        self.logger.info(f"✅ ISO string parsed: {iso_string} -> {parsed_dt}")
    
    def test_parse_datetime_string_custom_format(self):
        """Test parsing datetime string with custom format."""
        datetime_string = "07/30/2024 7:00 PM"
        custom_format = "%m/%d/%Y %I:%M %p"
        
        parsed_dt = parse_datetime_string(datetime_string, fmt=custom_format)
        
        assert parsed_dt is not None
        assert parsed_dt.year == 2024
        assert parsed_dt.month == 7
        assert parsed_dt.day == 30
        assert parsed_dt.hour == 19  # 7 PM in 24-hour format
        
        self.logger.info(f"✅ Custom format parsed: {datetime_string} -> {parsed_dt}")
    
    def test_parse_datetime_string_invalid(self):
        """Test parsing invalid datetime string."""
        invalid_string = "not a datetime"
        
        parsed_dt = parse_datetime_string(invalid_string)
        
        # Should return None for invalid strings
        assert parsed_dt is None
        
        self.logger.info("✅ Invalid datetime string handled correctly")
    
    def test_timezone_consistency(self):
        """Test timezone consistency across operations."""
        # Create a UTC datetime
        utc_dt = datetime(2024, 7, 30, 15, 30, 0, tzinfo=timezone.utc)
        
        # Convert to Eastern multiple times
        eastern_dt1 = convert_utc_to_eastern(utc_dt)
        eastern_dt2 = convert_to_eastern(utc_dt)
        
        # Both should produce the same result
        assert eastern_dt1.hour == eastern_dt2.hour
        assert eastern_dt1.minute == eastern_dt2.minute
        assert eastern_dt1.day == eastern_dt2.day
        
        self.logger.info("✅ Timezone conversion consistency verified")
    
    def test_edge_case_dst_transition(self):
        """Test datetime handling around DST transitions."""
        # Spring forward - 2:00 AM becomes 3:00 AM
        spring_transition = datetime(2024, 3, 10, 7, 0, 0, tzinfo=timezone.utc)  # 2:00 AM EST -> 3:00 AM EDT
        
        eastern_spring = convert_utc_to_eastern(spring_transition)
        assert eastern_spring.hour == 3  # Should jump to 3:00 AM EDT
        
        # Fall back - 2:00 AM becomes 1:00 AM  
        fall_transition = datetime(2024, 11, 3, 6, 0, 0, tzinfo=timezone.utc)  # 1:00 AM EST (fall back)
        
        eastern_fall = convert_utc_to_eastern(fall_transition)
        assert eastern_fall.hour == 1  # Should be 1:00 AM EST
        
        self.logger.info("✅ DST transition edge cases handled")
    
    def test_performance_with_multiple_conversions(self):
        """Test performance with multiple datetime conversions."""
        import time
        
        start_time = time.time()
        
        # Perform 100 conversions
        for i in range(100):
            utc_dt = datetime(2024, 7, 30, 15, i % 60, 0, tzinfo=timezone.utc)
            eastern_dt = convert_utc_to_eastern(utc_dt)
            assert eastern_dt is not None
        
        end_time = time.time()
        elapsed = end_time - start_time
        
        # Should complete in reasonable time (less than 1 second)
        assert elapsed < 1.0
        
        self.logger.info(f"✅ 100 conversions completed in {elapsed:.3f} seconds")