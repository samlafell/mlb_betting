"""
Data validation utilities for the MLB Sharp Betting system.

This module provides comprehensive validation functions for data quality
checking, field validation, and business rule enforcement.
"""

from typing import Any, Dict, List, Optional, Set, Union, Callable
from decimal import Decimal, InvalidOperation
import re
from datetime import datetime, timedelta

import structlog
from pydantic import BaseModel, ValidationError as PydanticValidationError

from ..core.exceptions import ValidationError
from ..models.splits import BettingSplit, SplitType, BookType
from ..models.game import Team

logger = structlog.get_logger(__name__)


class ValidationRule:
    """
    A validation rule that can be applied to data.
    """
    
    def __init__(
        self,
        name: str,
        validator: Callable[[Any], bool],
        error_message: str,
        is_warning: bool = False
    ) -> None:
        """
        Initialize validation rule.
        
        Args:
            name: Rule name
            validator: Validation function
            error_message: Error message for failures
            is_warning: Whether failures are warnings vs errors
        """
        self.name = name
        self.validator = validator
        self.error_message = error_message
        self.is_warning = is_warning
    
    def validate(self, value: Any) -> tuple[bool, str]:
        """
        Validate a value against this rule.
        
        Args:
            value: Value to validate
            
        Returns:
            Tuple of (is_valid, message)
        """
        try:
            is_valid = self.validator(value)
            return is_valid, "" if is_valid else self.error_message
        except Exception as e:
            return False, f"{self.error_message} (validation error: {e})"


class FieldValidator:
    """
    Validator for individual fields with multiple rules.
    """
    
    def __init__(self, field_name: str) -> None:
        """
        Initialize field validator.
        
        Args:
            field_name: Name of the field being validated
        """
        self.field_name = field_name
        self.rules: List[ValidationRule] = []
    
    def add_rule(self, rule: ValidationRule) -> 'FieldValidator':
        """Add a validation rule."""
        self.rules.append(rule)
        return self
    
    def required(self, error_message: Optional[str] = None) -> 'FieldValidator':
        """Add required field validation."""
        message = error_message or f"{self.field_name} is required"
        rule = ValidationRule(
            "required",
            lambda x: x is not None and str(x).strip() != "",
            message
        )
        return self.add_rule(rule)
    
    def min_value(self, min_val: Union[int, float], error_message: Optional[str] = None) -> 'FieldValidator':
        """Add minimum value validation."""
        message = error_message or f"{self.field_name} must be >= {min_val}"
        rule = ValidationRule(
            "min_value",
            lambda x: x is None or float(x) >= min_val,
            message
        )
        return self.add_rule(rule)
    
    def max_value(self, max_val: Union[int, float], error_message: Optional[str] = None) -> 'FieldValidator':
        """Add maximum value validation."""
        message = error_message or f"{self.field_name} must be <= {max_val}"
        rule = ValidationRule(
            "max_value",
            lambda x: x is None or float(x) <= max_val,
            message
        )
        return self.add_rule(rule)
    
    def in_range(
        self, 
        min_val: Union[int, float], 
        max_val: Union[int, float],
        error_message: Optional[str] = None
    ) -> 'FieldValidator':
        """Add range validation."""
        message = error_message or f"{self.field_name} must be between {min_val} and {max_val}"
        rule = ValidationRule(
            "in_range",
            lambda x: x is None or (min_val <= float(x) <= max_val),
            message
        )
        return self.add_rule(rule)
    
    def percentage_range(self, error_message: Optional[str] = None) -> 'FieldValidator':
        """Add percentage validation (0-100)."""
        message = error_message or f"{self.field_name} must be a percentage between 0 and 100"
        return self.in_range(0, 100, message)
    
    def positive(self, error_message: Optional[str] = None) -> 'FieldValidator':
        """Add positive number validation."""
        message = error_message or f"{self.field_name} must be positive"
        rule = ValidationRule(
            "positive",
            lambda x: x is None or float(x) > 0,
            message
        )
        return self.add_rule(rule)
    
    def matches_pattern(self, pattern: str, error_message: Optional[str] = None) -> 'FieldValidator':
        """Add regex pattern validation."""
        message = error_message or f"{self.field_name} must match pattern {pattern}"
        regex = re.compile(pattern)
        rule = ValidationRule(
            "pattern",
            lambda x: x is None or bool(regex.match(str(x))),
            message
        )
        return self.add_rule(rule)
    
    def in_enum(self, enum_class: type, error_message: Optional[str] = None) -> 'FieldValidator':
        """Add enum validation."""
        valid_values = [e.value for e in enum_class]
        message = error_message or f"{self.field_name} must be one of: {valid_values}"
        rule = ValidationRule(
            "enum",
            lambda x: x is None or x in valid_values,
            message
        )
        return self.add_rule(rule)
    
    def custom(
        self, 
        validator: Callable[[Any], bool], 
        error_message: str,
        is_warning: bool = False
    ) -> 'FieldValidator':
        """Add custom validation rule."""
        rule = ValidationRule("custom", validator, error_message, is_warning)
        return self.add_rule(rule)
    
    def validate(self, value: Any) -> Dict[str, List[str]]:
        """
        Validate value against all rules.
        
        Args:
            value: Value to validate
            
        Returns:
            Dictionary with 'errors' and 'warnings' lists
        """
        errors = []
        warnings = []
        
        for rule in self.rules:
            is_valid, message = rule.validate(value)
            if not is_valid:
                if rule.is_warning:
                    warnings.append(message)
                else:
                    errors.append(message)
        
        return {"errors": errors, "warnings": warnings}


class BettingSplitValidator:
    """
    Specialized validator for BettingSplit models.
    """
    
    def __init__(self) -> None:
        """Initialize betting split validator."""
        self.logger = logger.bind(validator="BettingSplit")
        
        # Define field validators
        self.validators = {
            'home_or_over_bets_percentage': (
                FieldValidator('home_or_over_bets_percentage')
                .percentage_range()
            ),
            'away_or_under_bets_percentage': (
                FieldValidator('away_or_under_bets_percentage')
                .percentage_range()
            ),
            'home_or_over_stake_percentage': (
                FieldValidator('home_or_over_stake_percentage')
                .percentage_range()
            ),
            'away_or_under_stake_percentage': (
                FieldValidator('away_or_under_stake_percentage')
                .percentage_range()
            ),
            'home_or_over_bets': (
                FieldValidator('home_or_over_bets')
                .min_value(0)
            ),
            'away_or_under_bets': (
                FieldValidator('away_or_under_bets')
                .min_value(0)
            ),
            'split_value': (
                FieldValidator('split_value')
                .custom(
                    self._validate_split_value,
                    "Split value out of reasonable range for sport"
                )
            ),
            'game_datetime': (
                FieldValidator('game_datetime')
                .required()
                .custom(
                    lambda x: self._validate_game_datetime(x),
                    "Game datetime is too far in past or future"
                )
            ),
            'last_updated': (
                FieldValidator('last_updated')
                .required()
                .custom(
                    lambda x: self._validate_last_updated(x),
                    "Last updated timestamp is invalid"
                )
            )
        }
    
    def validate(self, betting_split: BettingSplit) -> Dict[str, Any]:
        """
        Validate a BettingSplit instance.
        
        Args:
            betting_split: BettingSplit to validate
            
        Returns:
            Validation result dictionary
        """
        all_errors = []
        all_warnings = []
        field_results = {}
        
        # Validate individual fields
        for field_name, validator in self.validators.items():
            value = getattr(betting_split, field_name, None)
            result = validator.validate(value)
            
            field_results[field_name] = result
            all_errors.extend(result['errors'])
            all_warnings.extend(result['warnings'])
        
        # Cross-field validation
        cross_validation = self._cross_field_validation(betting_split)
        all_errors.extend(cross_validation['errors'])
        all_warnings.extend(cross_validation['warnings'])
        
        # Business rule validation
        business_validation = self._business_rule_validation(betting_split)
        all_errors.extend(business_validation['errors'])
        all_warnings.extend(business_validation['warnings'])
        
        is_valid = len(all_errors) == 0
        
        result = {
            'is_valid': is_valid,
            'errors': all_errors,
            'warnings': all_warnings,
            'field_results': field_results,
            'summary': {
                'total_errors': len(all_errors),
                'total_warnings': len(all_warnings),
                'fields_validated': len(self.validators),
                'validation_timestamp': datetime.now().isoformat()
            }
        }
        
        self.logger.debug("BettingSplit validation completed",
                         is_valid=is_valid,
                         error_count=len(all_errors),
                         warning_count=len(all_warnings))
        
        return result
    
    def _validate_split_value(self, split_value: Optional[Union[float, str]]) -> bool:
        """Validate split value based on context."""
        if split_value is None:
            return True  # Optional field
        
        # For string values (like moneyline JSON), don't apply numeric range validation
        if isinstance(split_value, str):
            return True  # String validation should be handled by split_value validator in model
        
        # General reasonable ranges for numeric values (spreads and totals)
        if isinstance(split_value, (int, float)) and -50 <= split_value <= 50:
            return True
        
        return False
    
    def _validate_game_datetime(self, game_datetime: datetime) -> bool:
        """Validate game datetime is reasonable."""
        if not isinstance(game_datetime, datetime):
            return False
        
        now = datetime.now()
        # Allow games from 1 year ago to 1 year in future
        min_date = now - timedelta(days=365)
        max_date = now + timedelta(days=365)
        
        return min_date <= game_datetime <= max_date
    
    def _validate_last_updated(self, last_updated: datetime) -> bool:
        """Validate last updated timestamp."""
        if not isinstance(last_updated, datetime):
            return False
        
        now = datetime.now()
        # Allow updates from 30 days ago to 1 hour in future (clock skew)
        min_date = now - timedelta(days=30)
        max_date = now + timedelta(hours=1)
        
        return min_date <= last_updated <= max_date
    
    def _cross_field_validation(self, betting_split: BettingSplit) -> Dict[str, List[str]]:
        """Validate relationships between fields."""
        errors = []
        warnings = []
        
        # Validate percentage pairs sum to ~100%
        bet_pct_total = 0
        stake_pct_total = 0
        
        if (betting_split.home_or_over_bets_percentage is not None and
            betting_split.away_or_under_bets_percentage is not None):
            bet_pct_total = (betting_split.home_or_over_bets_percentage + 
                           betting_split.away_or_under_bets_percentage)
            
            if not (95 <= bet_pct_total <= 105):  # 5% tolerance
                warnings.append(f"Bet percentages sum to {bet_pct_total:.1f}%, expected ~100%")
        
        if (betting_split.home_or_over_stake_percentage is not None and
            betting_split.away_or_under_stake_percentage is not None):
            stake_pct_total = (betting_split.home_or_over_stake_percentage + 
                             betting_split.away_or_under_stake_percentage)
            
            if not (95 <= stake_pct_total <= 105):  # 5% tolerance
                warnings.append(f"Stake percentages sum to {stake_pct_total:.1f}%, expected ~100%")
        
        # Validate split type specific constraints
        if betting_split.split_type == SplitType.SPREAD:
            if betting_split.split_value is None:
                warnings.append("Spread split missing spread value")
            elif not (-30 <= betting_split.split_value <= 30):
                warnings.append(f"Unusual spread value: {betting_split.split_value}")
        
        elif betting_split.split_type == SplitType.TOTAL:
            if betting_split.split_value is None:
                warnings.append("Total split missing total value")
            elif not (2 <= betting_split.split_value <= 20):  # MLB typical range
                warnings.append(f"Unusual total value for MLB: {betting_split.split_value}")
        
        elif betting_split.split_type == SplitType.MONEYLINE:
            if betting_split.split_value is not None:
                warnings.append("Moneyline split should not have split_value")
        
        # Validate bet counts vs percentages consistency
        if (betting_split.home_or_over_bets is not None and
            betting_split.away_or_under_bets is not None and
            betting_split.home_or_over_bets_percentage is not None):
            
            total_bets = betting_split.home_or_over_bets + betting_split.away_or_under_bets
            if total_bets > 0:
                calculated_pct = (betting_split.home_or_over_bets / total_bets) * 100
                actual_pct = betting_split.home_or_over_bets_percentage
                
                if abs(calculated_pct - actual_pct) > 5:  # 5% tolerance
                    warnings.append(
                        f"Bet count percentage mismatch: calculated {calculated_pct:.1f}%, "
                        f"reported {actual_pct:.1f}%"
                    )
        
        return {"errors": errors, "warnings": warnings}
    
    def _business_rule_validation(self, betting_split: BettingSplit) -> Dict[str, List[str]]:
        """Validate business rules and constraints."""
        errors = []
        warnings = []
        
        # Check for reasonable data freshness
        if betting_split.last_updated is not None:
            age_hours = (datetime.now() - betting_split.last_updated).total_seconds() / 3600
            if age_hours > 24:
                warnings.append(f"Data is {age_hours:.1f} hours old")
            elif age_hours > 72:
                errors.append(f"Data is too old: {age_hours:.1f} hours")
        
        # Check for missing essential data
        if (betting_split.home_or_over_bets_percentage is None and
            betting_split.home_or_over_stake_percentage is None):
            warnings.append("No percentage data available for home/over side")
        
        if (betting_split.away_or_under_bets_percentage is None and
            betting_split.away_or_under_stake_percentage is None):
            warnings.append("No percentage data available for away/under side")
        
        # Check for suspicious patterns
        if (betting_split.home_or_over_bets_percentage is not None and
            betting_split.home_or_over_stake_percentage is not None):
            
            bet_pct = betting_split.home_or_over_bets_percentage
            stake_pct = betting_split.home_or_over_stake_percentage
            
            # Large difference might indicate sharp money
            if abs(bet_pct - stake_pct) > 15:
                warnings.append(
                    f"Large bet/stake percentage difference: {bet_pct:.1f}% vs {stake_pct:.1f}%"
                )
        
        return {"errors": errors, "warnings": warnings}


class DataQualityValidator:
    """
    Validator for overall data quality assessment.
    """
    
    @staticmethod
    def assess_data_quality(data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Assess overall data quality for a dataset.
        
        Args:
            data: List of data records
            
        Returns:
            Data quality assessment
        """
        if not data:
            return {
                'quality_score': 0.0,
                'issues': ['No data provided'],
                'metrics': {}
            }
        
        total_records = len(data)
        issues = []
        metrics = {}
        
        # Check completeness
        completeness = DataQualityValidator._check_completeness(data)
        metrics['completeness'] = completeness
        
        if completeness['overall_score'] < 80:
            issues.append(f"Low data completeness: {completeness['overall_score']:.1f}%")
        
        # Check consistency
        consistency = DataQualityValidator._check_consistency(data)
        metrics['consistency'] = consistency
        
        if consistency['overall_score'] < 90:
            issues.append(f"Data consistency issues: {consistency['overall_score']:.1f}%")
        
        # Check for duplicates
        duplicates = DataQualityValidator._check_duplicates(data)
        metrics['duplicates'] = duplicates
        
        if duplicates['duplicate_rate'] > 5:
            issues.append(f"High duplicate rate: {duplicates['duplicate_rate']:.1f}%")
        
        # Check data freshness
        freshness = DataQualityValidator._check_freshness(data)
        metrics['freshness'] = freshness
        
        if freshness['average_age_hours'] > 48:
            issues.append(f"Data is stale: {freshness['average_age_hours']:.1f} hours average age")
        
        # Calculate overall quality score
        quality_score = (
            completeness['overall_score'] * 0.3 +
            consistency['overall_score'] * 0.3 +
            (100 - duplicates['duplicate_rate']) * 0.2 +
            freshness['freshness_score'] * 0.2
        )
        
        return {
            'quality_score': quality_score,
            'grade': DataQualityValidator._get_quality_grade(quality_score),
            'total_records': total_records,
            'issues': issues,
            'metrics': metrics,
            'assessment_timestamp': datetime.now().isoformat()
        }
    
    @staticmethod
    def _check_completeness(data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Check data completeness."""
        if not data:
            return {'overall_score': 0.0, 'field_completeness': {}}
        
        total_records = len(data)
        important_fields = [
            'home_team', 'away_team', 'game_datetime', 'split_type',
            'home_or_over_bets_percentage', 'away_or_under_bets_percentage'
        ]
        
        field_completeness = {}
        
        for field in important_fields:
            non_null_count = sum(
                1 for record in data
                if record.get(field) is not None and str(record.get(field)).strip()
            )
            completeness_pct = (non_null_count / total_records) * 100
            field_completeness[field] = completeness_pct
        
        overall_score = sum(field_completeness.values()) / len(field_completeness)
        
        return {
            'overall_score': overall_score,
            'field_completeness': field_completeness
        }
    
    @staticmethod
    def _check_consistency(data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Check data consistency."""
        if not data:
            return {'overall_score': 100.0, 'consistency_issues': []}
        
        issues = []
        consistency_scores = []
        
        # Check percentage consistency
        for record in data:
            home_pct = record.get('home_or_over_bets_percentage')
            away_pct = record.get('away_or_under_bets_percentage')
            
            if home_pct is not None and away_pct is not None:
                total_pct = home_pct + away_pct
                if not (95 <= total_pct <= 105):
                    issues.append(f"Percentages sum to {total_pct:.1f}%")
                    consistency_scores.append(0)
                else:
                    consistency_scores.append(100)
        
        overall_score = sum(consistency_scores) / len(consistency_scores) if consistency_scores else 100.0
        
        return {
            'overall_score': overall_score,
            'consistency_issues': issues[:10]  # Limit to first 10 issues
        }
    
    @staticmethod
    def _check_duplicates(data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Check for duplicate records."""
        if not data:
            return {'duplicate_rate': 0.0, 'duplicate_count': 0}
        
        # Create simple hash for duplicate detection
        seen_hashes = set()
        duplicates = 0
        
        for record in data:
            # Create hash from key fields
            hash_key = (
                record.get('game_id', ''),
                record.get('split_type', ''),
                record.get('book', ''),
                record.get('source', '')
            )
            
            if hash_key in seen_hashes:
                duplicates += 1
            else:
                seen_hashes.add(hash_key)
        
        duplicate_rate = (duplicates / len(data)) * 100
        
        return {
            'duplicate_rate': duplicate_rate,
            'duplicate_count': duplicates,
            'unique_count': len(seen_hashes)
        }
    
    @staticmethod
    def _check_freshness(data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Check data freshness."""
        if not data:
            return {'average_age_hours': 0.0, 'freshness_score': 100.0}
        
        now = datetime.now()
        ages = []
        
        for record in data:
            last_updated = record.get('last_updated')
            if last_updated:
                try:
                    if isinstance(last_updated, str):
                        last_updated = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
                    age_hours = (now - last_updated).total_seconds() / 3600
                    ages.append(age_hours)
                except:
                    continue
        
        if not ages:
            return {'average_age_hours': 0.0, 'freshness_score': 100.0}
        
        average_age_hours = sum(ages) / len(ages)
        
        # Score based on freshness (100 for < 1 hour, decreasing)
        if average_age_hours < 1:
            freshness_score = 100.0
        elif average_age_hours < 6:
            freshness_score = 90.0
        elif average_age_hours < 24:
            freshness_score = 75.0
        elif average_age_hours < 48:
            freshness_score = 50.0
        else:
            freshness_score = 25.0
        
        return {
            'average_age_hours': average_age_hours,
            'oldest_age_hours': max(ages),
            'newest_age_hours': min(ages),
            'freshness_score': freshness_score
        }
    
    @staticmethod
    def _get_quality_grade(score: float) -> str:
        """Get quality grade from score."""
        if score >= 95:
            return 'A+'
        elif score >= 90:
            return 'A'
        elif score >= 85:
            return 'B+'
        elif score >= 80:
            return 'B'
        elif score >= 75:
            return 'C+'
        elif score >= 70:
            return 'C'
        elif score >= 60:
            return 'D'
        else:
            return 'F'


# Convenience functions
def validate_betting_split(betting_split: BettingSplit) -> Dict[str, Any]:
    """
    Convenience function to validate a BettingSplit.
    
    Args:
        betting_split: BettingSplit instance to validate
        
    Returns:
        Validation result
    """
    validator = BettingSplitValidator()
    return validator.validate(betting_split)


def assess_data_quality(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Convenience function to assess data quality.
    
    Args:
        data: List of data records
        
    Returns:
        Data quality assessment
    """
    return DataQualityValidator.assess_data_quality(data)


__all__ = [
    'ValidationRule',
    'FieldValidator', 
    'BettingSplitValidator',
    'DataQualityValidator',
    'validate_betting_split',
    'assess_data_quality'
] 