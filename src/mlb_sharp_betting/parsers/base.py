"""
Base parser classes and interfaces for data parsing and validation.

This module provides abstract base classes for data parsers with
validation capabilities and comprehensive error handling.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, TypeVar

import structlog
from pydantic import BaseModel

from ..core.exceptions import ParsingError

logger = structlog.get_logger(__name__)

# Type variable for model types
ModelType = TypeVar("ModelType", bound=BaseModel)


@dataclass
class ParsingResult:
    """Result of a parsing operation."""

    success: bool
    parsed_data: list[BaseModel]
    raw_data: list[dict[str, Any]]
    errors: list[str]
    warnings: list[str]
    metadata: dict[str, Any] | None = None
    parse_time_ms: float = 0.0

    @property
    def has_data(self) -> bool:
        """Check if parsing result contains valid data."""
        return self.success and bool(self.parsed_data)

    @property
    def error_count(self) -> int:
        """Get number of errors encountered."""
        return len(self.errors)

    @property
    def warning_count(self) -> int:
        """Get number of warnings encountered."""
        return len(self.warnings)

    @property
    def parsed_count(self) -> int:
        """Get number of successfully parsed items."""
        return len(self.parsed_data)

    @property
    def success_rate(self) -> float:
        """Calculate parsing success rate."""
        if not self.raw_data:
            return 0.0
        return len(self.parsed_data) / len(self.raw_data)


@dataclass
class ValidationConfig:
    """Configuration for data validation."""

    strict_mode: bool = False  # Fail on any validation error
    allow_partial: bool = True  # Allow partial success
    max_errors: int = 100  # Maximum errors before stopping
    validate_required_fields: bool = True
    validate_data_types: bool = True
    validate_ranges: bool = True
    skip_invalid_records: bool = True


class ParserError(ParsingError):
    """Base exception for parser-related errors."""

    pass


class BaseParser(ABC):
    """
    Abstract base parser class with validation capabilities.

    Provides common functionality for parsing raw data into
    validated model instances with comprehensive error handling.
    """

    def __init__(
        self, parser_name: str, validation_config: ValidationConfig | None = None
    ) -> None:
        """
        Initialize base parser.

        Args:
            parser_name: Name of the parser
            validation_config: Validation configuration
        """
        self.parser_name = parser_name
        self.validation_config = validation_config or ValidationConfig()
        self.logger = logger.bind(parser=parser_name)

        # Parsing metrics
        self.total_parsed = 0
        self.successful_parses = 0
        self.failed_parses = 0

    @property
    @abstractmethod
    def target_model_class(self) -> type[ModelType]:
        """Get the target model class for this parser."""
        pass

    @abstractmethod
    async def parse_raw_data(self, raw_data: dict[str, Any]) -> ModelType | None:
        """
        Parse a single raw data item into a model instance.

        Args:
            raw_data: Raw data dictionary

        Returns:
            Parsed model instance or None if parsing fails
        """
        pass

    async def parse(self, data: list[dict[str, Any]]) -> ParsingResult:
        """
        Parse a list of raw data items.

        Args:
            data: List of raw data dictionaries

        Returns:
            ParsingResult with parsed models and errors
        """
        start_time = datetime.now()

        parsed_data = []
        errors = []
        warnings = []

        self.logger.info(
            "Starting data parsing", item_count=len(data), parser=self.parser_name
        )

        for i, raw_item in enumerate(data):
            try:
                # Parse individual item
                parsed_item = await self.parse_raw_data(raw_item)

                if parsed_item is not None:
                    # Validate the parsed item
                    validation_result = await self._validate_item(parsed_item, i)

                    if validation_result.is_valid:
                        parsed_data.append(parsed_item)
                        self.successful_parses += 1
                    else:
                        if self.validation_config.skip_invalid_records:
                            warnings.extend(validation_result.warnings)
                            errors.extend(validation_result.errors)
                            self.failed_parses += 1
                        else:
                            errors.extend(validation_result.errors)
                            if self.validation_config.strict_mode:
                                break
                else:
                    error_msg = f"Failed to parse item {i}: returned None"
                    errors.append(error_msg)
                    self.failed_parses += 1

            except Exception as e:
                error_msg = f"Error parsing item {i}: {str(e)}"
                errors.append(error_msg)
                self.logger.debug("Item parsing failed", item_index=i, error=str(e))
                self.failed_parses += 1

                if self.validation_config.strict_mode:
                    break

            # Check error limit
            if len(errors) >= self.validation_config.max_errors:
                error_msg = (
                    f"Maximum error limit ({self.validation_config.max_errors}) reached"
                )
                errors.append(error_msg)
                self.logger.warning(
                    "Maximum error limit reached", error_count=len(errors)
                )
                break

        self.total_parsed += len(data)

        parse_time = (datetime.now() - start_time).total_seconds() * 1000

        success = len(parsed_data) > 0 and (
            self.validation_config.allow_partial or len(errors) == 0
        )

        result = ParsingResult(
            success=success,
            parsed_data=parsed_data,
            raw_data=data,
            errors=errors,
            warnings=warnings,
            metadata={
                "parser": self.parser_name,
                "total_items": len(data),
                "parsed_items": len(parsed_data),
                "failed_items": len(data) - len(parsed_data),
                "validation_config": self.validation_config.__dict__,
            },
            parse_time_ms=parse_time,
        )

        self.logger.info(
            "Parsing completed",
            success=success,
            parsed_count=len(parsed_data),
            error_count=len(errors),
            warning_count=len(warnings),
            parse_time_ms=parse_time,
        )

        return result

    async def _validate_item(self, item: ModelType, index: int) -> "ValidationResult":
        """
        Validate a parsed item.

        Args:
            item: Parsed model instance
            index: Item index for error reporting

        Returns:
            ValidationResult with validation status
        """
        validation_errors = []
        validation_warnings = []

        try:
            # Basic Pydantic validation (already done during model creation)
            # Additional custom validation can be added here

            # Validate required fields if configured
            if self.validation_config.validate_required_fields:
                required_validation = await self._validate_required_fields(item)
                validation_errors.extend(required_validation.errors)
                validation_warnings.extend(required_validation.warnings)

            # Validate data types if configured
            if self.validation_config.validate_data_types:
                type_validation = await self._validate_data_types(item)
                validation_errors.extend(type_validation.errors)
                validation_warnings.extend(type_validation.warnings)

            # Validate ranges if configured
            if self.validation_config.validate_ranges:
                range_validation = await self._validate_ranges(item)
                validation_errors.extend(range_validation.errors)
                validation_warnings.extend(range_validation.warnings)

            # Custom validation
            custom_validation = await self._custom_validation(item)
            validation_errors.extend(custom_validation.errors)
            validation_warnings.extend(custom_validation.warnings)

        except Exception as e:
            validation_errors.append(f"Validation failed for item {index}: {str(e)}")

        return ValidationResult(
            is_valid=len(validation_errors) == 0,
            errors=[f"Item {index}: {error}" for error in validation_errors],
            warnings=[f"Item {index}: {warning}" for warning in validation_warnings],
        )

    async def _validate_required_fields(self, item: ModelType) -> "ValidationResult":
        """Validate required fields are present and not empty."""
        errors = []
        warnings = []

        # This is a base implementation - subclasses can override
        # Pydantic already validates required fields during model creation

        return ValidationResult(is_valid=True, errors=errors, warnings=warnings)

    async def _validate_data_types(self, item: ModelType) -> "ValidationResult":
        """Validate data types are correct."""
        errors = []
        warnings = []

        # This is a base implementation - subclasses can override
        # Pydantic already validates data types during model creation

        return ValidationResult(is_valid=True, errors=errors, warnings=warnings)

    async def _validate_ranges(self, item: ModelType) -> "ValidationResult":
        """Validate numeric values are within expected ranges."""
        errors = []
        warnings = []

        # This is a base implementation - subclasses can override
        # for specific range validation logic

        return ValidationResult(is_valid=True, errors=errors, warnings=warnings)

    async def _custom_validation(self, item: ModelType) -> "ValidationResult":
        """
        Perform custom validation logic.

        Subclasses should override this method to implement
        parser-specific validation rules.
        """
        return ValidationResult(is_valid=True, errors=[], warnings=[])

    def _safe_convert(
        self,
        value: Any,
        target_type: type,
        default: Any = None,
        field_name: str = "unknown",
    ) -> Any:
        """
        Safely convert value to target type.

        Args:
            value: Value to convert
            target_type: Target type
            default: Default value if conversion fails
            field_name: Field name for error reporting

        Returns:
            Converted value or default
        """
        if value is None:
            return default

        try:
            if target_type == str:
                return str(value).strip() if value else default
            elif target_type == int:
                if isinstance(value, str):
                    # Handle common string formats
                    value = value.replace(",", "").replace("%", "")
                return int(float(value)) if value else default
            elif target_type == float:
                if isinstance(value, str):
                    # Handle common string formats
                    value = value.replace(",", "").replace("%", "")
                return float(value) if value else default
            elif target_type == bool:
                if isinstance(value, str):
                    return value.lower() in ("true", "yes", "1", "on")
                return bool(value)
            else:
                return target_type(value)

        except (ValueError, TypeError) as e:
            self.logger.debug(
                "Type conversion failed",
                field=field_name,
                value=value,
                target_type=target_type.__name__,
                error=str(e),
            )
            return default

    def get_parsing_metrics(self) -> dict[str, Any]:
        """Get parser performance metrics."""
        success_rate = (
            self.successful_parses / self.total_parsed if self.total_parsed > 0 else 0.0
        )

        return {
            "parser": self.parser_name,
            "total_parsed": self.total_parsed,
            "successful_parses": self.successful_parses,
            "failed_parses": self.failed_parses,
            "success_rate": success_rate,
        }

    def reset_metrics(self) -> None:
        """Reset parsing metrics."""
        self.total_parsed = 0
        self.successful_parses = 0
        self.failed_parses = 0


@dataclass
class ValidationResult:
    """Result of a validation operation."""

    is_valid: bool
    errors: list[str]
    warnings: list[str]

    @property
    def has_errors(self) -> bool:
        """Check if validation has errors."""
        return len(self.errors) > 0

    @property
    def has_warnings(self) -> bool:
        """Check if validation has warnings."""
        return len(self.warnings) > 0


class DataQualityChecker:
    """Utility class for performing data quality checks."""

    @staticmethod
    def check_completeness(
        data: list[dict[str, Any]], required_fields: list[str]
    ) -> dict[str, float]:
        """
        Check data completeness for required fields.

        Args:
            data: List of data items
            required_fields: List of required field names

        Returns:
            Dictionary mapping field names to completeness percentages
        """
        if not data:
            return dict.fromkeys(required_fields, 0.0)

        completeness = {}
        total_records = len(data)

        for field in required_fields:
            non_null_count = sum(
                1
                for item in data
                if item.get(field) is not None and str(item.get(field)).strip() != ""
            )
            completeness[field] = (non_null_count / total_records) * 100

        return completeness

    @staticmethod
    def check_data_types(
        data: list[dict[str, Any]], field_types: dict[str, type]
    ) -> dict[str, float]:
        """
        Check data type consistency.

        Args:
            data: List of data items
            field_types: Dictionary mapping field names to expected types

        Returns:
            Dictionary mapping field names to type consistency percentages
        """
        if not data:
            return dict.fromkeys(field_types.keys(), 0.0)

        consistency = {}
        total_records = len(data)

        for field, expected_type in field_types.items():
            valid_type_count = 0

            for item in data:
                value = item.get(field)
                if value is not None:
                    try:
                        if expected_type == str:
                            valid_type_count += 1  # Most things can be strings
                        elif expected_type == int:
                            int(float(str(value).replace(",", "")))
                            valid_type_count += 1
                        elif expected_type == float:
                            float(str(value).replace(",", ""))
                            valid_type_count += 1
                        elif isinstance(value, expected_type):
                            valid_type_count += 1
                    except (ValueError, TypeError):
                        pass

            consistency[field] = (valid_type_count / total_records) * 100

        return consistency

    @staticmethod
    def check_ranges(
        data: list[dict[str, Any]], field_ranges: dict[str, tuple[float, float]]
    ) -> dict[str, float]:
        """
        Check numeric values are within expected ranges.

        Args:
            data: List of data items
            field_ranges: Dictionary mapping field names to (min, max) tuples

        Returns:
            Dictionary mapping field names to range compliance percentages
        """
        if not data:
            return dict.fromkeys(field_ranges.keys(), 0.0)

        compliance = {}
        total_records = len(data)

        for field, (min_val, max_val) in field_ranges.items():
            in_range_count = 0

            for item in data:
                value = item.get(field)
                if value is not None:
                    try:
                        numeric_value = float(
                            str(value).replace(",", "").replace("%", "")
                        )
                        if min_val <= numeric_value <= max_val:
                            in_range_count += 1
                    except (ValueError, TypeError):
                        pass

            compliance[field] = (in_range_count / total_records) * 100

        return compliance


__all__ = [
    "BaseParser",
    "ParsingResult",
    "ValidationResult",
    "ValidationConfig",
    "ParserError",
    "DataQualityChecker",
]
