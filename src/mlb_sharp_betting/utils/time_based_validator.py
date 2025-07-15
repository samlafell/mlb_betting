"""
Time-based validator for betting splits data.

This validator ensures only actionable, time-sensitive betting data is processed
by rejecting data for games that started more than 5 minutes ago.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

import structlog

from ..models.splits import BettingSplit
from ..services.mlb_api_service import MLBStatsAPIService

logger = structlog.get_logger(__name__)


class ValidationResult(Enum):
    """Validation result types."""

    VALID = "valid"
    EXPIRED = "expired"
    DELAYED = "delayed"
    POSTPONED = "postponed"
    INVALID_TIME = "invalid_time"
    ERROR = "error"


class GameTimeValidator:
    """
    Validator for ensuring betting splits are only processed for games
    that haven't started more than 5 minutes ago.
    """

    def __init__(self, grace_period_minutes: int = 5):
        """
        Initialize the time-based validator.

        Args:
            grace_period_minutes: Number of minutes after game start to allow data
        """
        self.grace_period = timedelta(minutes=grace_period_minutes)
        self.mlb_api_service = MLBStatsAPIService()
        self.logger = logger.bind(component="GameTimeValidator")

        # Validation metrics for monitoring
        self.validation_stats = {
            "total_validations": 0,
            "valid_splits": 0,
            "expired_splits": 0,
            "delayed_games": 0,
            "postponed_games": 0,
            "api_errors": 0,
            "invalid_times": 0,
            "last_reset": datetime.now(timezone.utc),
        }

    def validate_split_timing(
        self, split: BettingSplit, current_time: datetime | None = None
    ) -> tuple[ValidationResult, dict[str, Any]]:
        """
        Validate if a betting split is within the acceptable time window.

        Args:
            split: BettingSplit object to validate
            current_time: Current UTC time (defaults to now)

        Returns:
            Tuple of (ValidationResult, metadata dict)
        """
        if current_time is None:
            current_time = datetime.now(timezone.utc)

        # Ensure current_time is timezone-aware
        if current_time.tzinfo is None:
            current_time = current_time.replace(tzinfo=timezone.utc)

        self.validation_stats["total_validations"] += 1

        metadata = {
            "game_id": split.game_id,
            "game_datetime": split.game_datetime,
            "current_time": current_time,
            "validation_time": current_time.isoformat(),
        }

        try:
            # Check if game_datetime is available
            if not split.game_datetime:
                self.validation_stats["invalid_times"] += 1
                self.logger.warning(
                    "Split missing game_datetime",
                    game_id=split.game_id,
                    home_team=split.home_team,
                    away_team=split.away_team,
                )
                return ValidationResult.INVALID_TIME, {
                    **metadata,
                    "reason": "missing_game_datetime",
                }

            # Ensure game_datetime is timezone-aware
            game_start = split.game_datetime
            if game_start.tzinfo is None:
                game_start = game_start.replace(tzinfo=timezone.utc)

            # Calculate time difference
            time_since_start = current_time - game_start
            metadata.update(
                {
                    "game_start_utc": game_start.isoformat(),
                    "time_since_start_minutes": time_since_start.total_seconds() / 60,
                    "grace_period_minutes": self.grace_period.total_seconds() / 60,
                }
            )

            # Check if game hasn't started yet (future game)
            if time_since_start.total_seconds() < 0:
                self.validation_stats["valid_splits"] += 1
                self.logger.debug(
                    "Split validated - future game",
                    game_id=split.game_id,
                    minutes_until_start=abs(time_since_start.total_seconds() / 60),
                )
                return ValidationResult.VALID, {**metadata, "reason": "future_game"}

            # Check if within grace period
            if time_since_start <= self.grace_period:
                self.validation_stats["valid_splits"] += 1
                self.logger.debug(
                    "Split validated - within grace period",
                    game_id=split.game_id,
                    minutes_since_start=time_since_start.total_seconds() / 60,
                )
                return ValidationResult.VALID, {
                    **metadata,
                    "reason": "within_grace_period",
                }

            # Game started more than grace period ago
            self.validation_stats["expired_splits"] += 1
            self.logger.info(
                "Split rejected - game started too long ago",
                game_id=split.game_id,
                minutes_since_start=time_since_start.total_seconds() / 60,
                grace_period_minutes=self.grace_period.total_seconds() / 60,
            )

            return ValidationResult.EXPIRED, {
                **metadata,
                "reason": "exceeded_grace_period",
            }

        except Exception as e:
            self.validation_stats["api_errors"] += 1
            self.logger.error("Validation error", game_id=split.game_id, error=str(e))
            return ValidationResult.ERROR, {**metadata, "error": str(e)}

    async def validate_with_mlb_api(
        self, split: BettingSplit, current_time: datetime | None = None
    ) -> tuple[ValidationResult, dict[str, Any]]:
        """
        Validate split timing with MLB API check for delays/postponements.

        Args:
            split: BettingSplit object to validate
            current_time: Current UTC time (defaults to now)

        Returns:
            Tuple of (ValidationResult, metadata dict)
        """
        # First do basic time validation
        basic_result, metadata = self.validate_split_timing(split, current_time)

        # If basic validation passes or if we need to check for delays
        if basic_result in [ValidationResult.VALID, ValidationResult.EXPIRED]:
            try:
                # Check actual game status via MLB API
                game_status = await self.mlb_api_service.get_game_status(split.game_id)

                if game_status:
                    metadata.update(
                        {
                            "mlb_api_status": game_status.get("status"),
                            "actual_start_time": game_status.get("actual_start_time"),
                            "scheduled_start_time": game_status.get(
                                "scheduled_start_time"
                            ),
                        }
                    )

                    # Handle postponed games
                    if game_status.get("status") in [
                        "Postponed",
                        "Cancelled",
                        "Suspended",
                    ]:
                        self.validation_stats["postponed_games"] += 1
                        self.logger.info(
                            "Game postponed/cancelled",
                            game_id=split.game_id,
                            status=game_status.get("status"),
                        )
                        return ValidationResult.POSTPONED, {
                            **metadata,
                            "reason": f"game_{game_status.get('status').lower()}",
                        }

                    # Handle delayed games
                    actual_start = game_status.get("actual_start_time")
                    if actual_start and actual_start != split.game_datetime:
                        # Recalculate validation with actual start time
                        if isinstance(actual_start, str):
                            actual_start = datetime.fromisoformat(
                                actual_start.replace("Z", "+00:00")
                            )

                        if actual_start.tzinfo is None:
                            actual_start = actual_start.replace(tzinfo=timezone.utc)

                        current_utc = current_time or datetime.now(timezone.utc)
                        if current_utc.tzinfo is None:
                            current_utc = current_utc.replace(tzinfo=timezone.utc)

                        time_since_actual_start = current_utc - actual_start

                        if time_since_actual_start <= self.grace_period:
                            self.validation_stats["delayed_games"] += 1
                            self.validation_stats["valid_splits"] += 1
                            self.logger.info(
                                "Split validated - delayed game within grace period",
                                game_id=split.game_id,
                                original_start=split.game_datetime.isoformat(),
                                actual_start=actual_start.isoformat(),
                                minutes_since_actual_start=time_since_actual_start.total_seconds()
                                / 60,
                            )
                            return ValidationResult.DELAYED, {
                                **metadata,
                                "reason": "delayed_game_valid",
                            }
                        else:
                            self.validation_stats["expired_splits"] += 1
                            self.logger.info(
                                "Split rejected - delayed game exceeded grace period",
                                game_id=split.game_id,
                                actual_start=actual_start.isoformat(),
                                minutes_since_actual_start=time_since_actual_start.total_seconds()
                                / 60,
                            )
                            return ValidationResult.EXPIRED, {
                                **metadata,
                                "reason": "delayed_game_expired",
                            }

            except Exception as e:
                self.validation_stats["api_errors"] += 1
                self.logger.warning(
                    "MLB API check failed, using basic validation",
                    game_id=split.game_id,
                    error=str(e),
                )
                # Fall back to basic validation result

        return basic_result, metadata

    def validate_batch(
        self, splits: list[BettingSplit], use_mlb_api: bool = False
    ) -> tuple[list[BettingSplit], list[dict[str, Any]]]:
        """
        Validate a batch of betting splits.

        Args:
            splits: List of BettingSplit objects to validate
            use_mlb_api: Whether to use MLB API for enhanced validation

        Returns:
            Tuple of (valid_splits, rejection_metadata)
        """
        valid_splits = []
        rejection_metadata = []
        current_time = datetime.now(timezone.utc)

        self.logger.info(
            "Starting batch validation",
            total_splits=len(splits),
            use_mlb_api=use_mlb_api,
        )

        for split in splits:
            try:
                if use_mlb_api:
                    # Use asyncio.run for individual API calls in batch
                    result, metadata = asyncio.run(
                        self.validate_with_mlb_api(split, current_time)
                    )
                else:
                    result, metadata = self.validate_split_timing(split, current_time)

                if result in [ValidationResult.VALID, ValidationResult.DELAYED]:
                    valid_splits.append(split)
                else:
                    rejection_metadata.append(
                        {"split": split, "result": result.value, "metadata": metadata}
                    )

            except Exception as e:
                self.logger.error(
                    "Error validating split in batch",
                    game_id=getattr(split, "game_id", "unknown"),
                    error=str(e),
                )
                rejection_metadata.append(
                    {
                        "split": split,
                        "result": ValidationResult.ERROR.value,
                        "metadata": {"error": str(e)},
                    }
                )

        self.logger.info(
            "Batch validation completed",
            total_splits=len(splits),
            valid_splits=len(valid_splits),
            rejected_splits=len(rejection_metadata),
        )

        return valid_splits, rejection_metadata

    def get_validation_stats(self) -> dict[str, Any]:
        """Get validation statistics for monitoring."""
        total_validations = self.validation_stats["total_validations"]

        stats = {
            **self.validation_stats,
            "rejection_rate": (
                self.validation_stats["expired_splits"]
                + self.validation_stats["postponed_games"]
                + self.validation_stats["invalid_times"]
            )
            / max(total_validations, 1),
            "api_error_rate": self.validation_stats["api_errors"]
            / max(total_validations, 1),
            "uptime_hours": (
                datetime.now(timezone.utc) - self.validation_stats["last_reset"]
            ).total_seconds()
            / 3600,
        }

        return stats

    def reset_stats(self):
        """Reset validation statistics."""
        self.validation_stats = {
            "total_validations": 0,
            "valid_splits": 0,
            "expired_splits": 0,
            "delayed_games": 0,
            "postponed_games": 0,
            "api_errors": 0,
            "invalid_times": 0,
            "last_reset": datetime.now(timezone.utc),
        }
        self.logger.info("Validation statistics reset")

    def should_alert(self) -> tuple[bool, list[str]]:
        """
        Check if validation metrics indicate issues requiring alerts.

        Returns:
            Tuple of (should_alert, alert_reasons)
        """
        alerts = []
        stats = self.get_validation_stats()

        # Alert on high rejection rate (>20%)
        if stats["rejection_rate"] > 0.20 and stats["total_validations"] > 10:
            alerts.append(f"High rejection rate: {stats['rejection_rate']:.1%}")

        # Alert on high API error rate (>10%)
        if stats["api_error_rate"] > 0.10 and stats["total_validations"] > 10:
            alerts.append(f"High API error rate: {stats['api_error_rate']:.1%}")

        # Alert if too many invalid times (indicates data quality issues)
        if stats["invalid_times"] > 5 and stats["total_validations"] > 10:
            invalid_rate = stats["invalid_times"] / stats["total_validations"]
            if invalid_rate > 0.15:
                alerts.append(f"High invalid time rate: {invalid_rate:.1%}")

        return len(alerts) > 0, alerts


# Global validator instance
_validator_instance: GameTimeValidator | None = None


def get_game_time_validator(grace_period_minutes: int = 5) -> GameTimeValidator:
    """
    Get the global game time validator instance.

    Args:
        grace_period_minutes: Grace period in minutes (only used for new instances)

    Returns:
        GameTimeValidator instance
    """
    global _validator_instance

    if _validator_instance is None:
        _validator_instance = GameTimeValidator(grace_period_minutes)
        logger.info(
            "Created new GameTimeValidator instance",
            grace_period_minutes=grace_period_minutes,
        )

    return _validator_instance


def validate_split_timing(
    split: BettingSplit, use_mlb_api: bool = False
) -> tuple[bool, dict[str, Any]]:
    """
    Convenience function to validate a single split's timing.

    Args:
        split: BettingSplit to validate
        use_mlb_api: Whether to use MLB API for enhanced validation

    Returns:
        Tuple of (is_valid, metadata)
    """
    validator = get_game_time_validator()

    if use_mlb_api:
        result, metadata = asyncio.run(validator.validate_with_mlb_api(split))
    else:
        result, metadata = validator.validate_split_timing(split)

    is_valid = result in [ValidationResult.VALID, ValidationResult.DELAYED]
    return is_valid, metadata


def validate_splits_batch(
    splits: list[BettingSplit], use_mlb_api: bool = False
) -> tuple[list[BettingSplit], list[dict[str, Any]]]:
    """
    Convenience function to validate a batch of splits.

    Args:
        splits: List of BettingSplit objects to validate
        use_mlb_api: Whether to use MLB API for enhanced validation

    Returns:
        Tuple of (valid_splits, rejection_metadata)
    """
    validator = get_game_time_validator()
    return validator.validate_batch(splits, use_mlb_api)


__all__ = [
    "GameTimeValidator",
    "ValidationResult",
    "get_game_time_validator",
    "validate_split_timing",
    "validate_splits_batch",
]
