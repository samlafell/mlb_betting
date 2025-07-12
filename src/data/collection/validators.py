"""
Unified Data Quality Validation and Deduplication System

Consolidates validation patterns from:
- sportsbookreview/parsers/base_parser.py (schema validation, data quality checks)
- mlb_sharp_betting/services/data_validator.py (business rule validation)
- action/scrapers/ (basic data validation)

Provides comprehensive data quality assurance with:
- Schema validation using Pydantic models
- Business rule validation with custom validators
- Cross-source data deduplication and consistency checking
- Data quality metrics and monitoring
- Configurable validation rules and thresholds
"""

import hashlib
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple, Union, Callable
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod
import uuid

from pydantic import BaseModel, ValidationError as PydanticValidationError
import structlog

from ...core.logging import get_logger, LogComponent
from ...core.exceptions import ValidationError, DataError
from ..models.unified.base import UnifiedBaseModel
from ..models.unified.game import UnifiedGame
from ..models.unified.odds import BettingMarket
from ..models.unified.betting_analysis import BettingAnalysis
from ..models.unified.sharp_data import SharpSignal

logger = get_logger(__name__, LogComponent.VALIDATOR)


class ValidationSeverity(Enum):
    """Severity levels for validation issues."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class ValidationRuleType(Enum):
    """Types of validation rules."""
    SCHEMA = "schema"
    BUSINESS_RULE = "business_rule"
    DATA_QUALITY = "data_quality"
    CONSISTENCY = "consistency"
    DEDUPLICATION = "deduplication"


@dataclass
class ValidationIssue:
    """Represents a validation issue."""
    
    rule_type: ValidationRuleType
    severity: ValidationSeverity
    message: str
    field_path: Optional[str] = None
    expected_value: Optional[Any] = None
    actual_value: Optional[Any] = None
    rule_name: Optional[str] = None
    item_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "rule_type": self.rule_type.value,
            "severity": self.severity.value,
            "message": self.message,
            "field_path": self.field_path,
            "expected_value": self.expected_value,
            "actual_value": self.actual_value,
            "rule_name": self.rule_name,
            "item_id": self.item_id
        }


@dataclass
class ValidationResult:
    """Result of data validation."""
    
    is_valid: bool
    validated_items: List[UnifiedBaseModel] = field(default_factory=list)
    invalid_items: List[Dict[str, Any]] = field(default_factory=list)
    issues: List[ValidationIssue] = field(default_factory=list)
    
    # Metrics
    total_items: int = 0
    valid_items_count: int = 0
    invalid_items_count: int = 0
    
    # Quality metrics
    schema_errors: int = 0
    business_rule_errors: int = 0
    data_quality_errors: int = 0
    consistency_errors: int = 0
    duplicate_items: int = 0
    
    # Processing metadata
    validation_time_ms: float = 0.0
    correlation_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    
    @property
    def success_rate(self) -> float:
        """Calculate validation success rate."""
        if self.total_items == 0:
            return 1.0
        return self.valid_items_count / self.total_items
    
    @property
    def error_rate(self) -> float:
        """Calculate validation error rate."""
        return 1.0 - self.success_rate
    
    @property
    def critical_issues(self) -> List[ValidationIssue]:
        """Get critical validation issues."""
        return [issue for issue in self.issues if issue.severity == ValidationSeverity.CRITICAL]
    
    @property
    def has_critical_issues(self) -> bool:
        """Check if there are critical validation issues."""
        return bool(self.critical_issues)
    
    def add_issue(self, issue: ValidationIssue) -> None:
        """Add a validation issue."""
        self.issues.append(issue)
        
        # Update counters
        if issue.rule_type == ValidationRuleType.SCHEMA:
            self.schema_errors += 1
        elif issue.rule_type == ValidationRuleType.BUSINESS_RULE:
            self.business_rule_errors += 1
        elif issue.rule_type == ValidationRuleType.DATA_QUALITY:
            self.data_quality_errors += 1
        elif issue.rule_type == ValidationRuleType.CONSISTENCY:
            self.consistency_errors += 1
        elif issue.rule_type == ValidationRuleType.DEDUPLICATION:
            self.duplicate_items += 1
    
    def get_summary(self) -> Dict[str, Any]:
        """Get validation result summary."""
        return {
            "correlation_id": self.correlation_id,
            "is_valid": self.is_valid,
            "total_items": self.total_items,
            "valid_items": self.valid_items_count,
            "invalid_items": self.invalid_items_count,
            "success_rate": self.success_rate,
            "validation_time_ms": self.validation_time_ms,
            "error_breakdown": {
                "schema_errors": self.schema_errors,
                "business_rule_errors": self.business_rule_errors,
                "data_quality_errors": self.data_quality_errors,
                "consistency_errors": self.consistency_errors,
                "duplicate_items": self.duplicate_items
            },
            "issue_count_by_severity": {
                severity.value: len([i for i in self.issues if i.severity == severity])
                for severity in ValidationSeverity
            }
        }


class ValidationRule(ABC):
    """Abstract base class for validation rules."""
    
    def __init__(
        self,
        name: str,
        rule_type: ValidationRuleType,
        severity: ValidationSeverity = ValidationSeverity.ERROR,
        enabled: bool = True
    ) -> None:
        """
        Initialize validation rule.
        
        Args:
            name: Rule name
            rule_type: Type of validation rule
            severity: Severity level for violations
            enabled: Whether the rule is enabled
        """
        self.name = name
        self.rule_type = rule_type
        self.severity = severity
        self.enabled = enabled
    
    @abstractmethod
    def validate(self, item: Dict[str, Any], context: Dict[str, Any] = None) -> List[ValidationIssue]:
        """
        Validate an item against this rule.
        
        Args:
            item: Item to validate
            context: Additional validation context
            
        Returns:
            List of validation issues
        """
        pass
    
    def create_issue(
        self,
        message: str,
        field_path: Optional[str] = None,
        expected_value: Optional[Any] = None,
        actual_value: Optional[Any] = None,
        item_id: Optional[str] = None
    ) -> ValidationIssue:
        """Create a validation issue for this rule."""
        return ValidationIssue(
            rule_type=self.rule_type,
            severity=self.severity,
            message=message,
            field_path=field_path,
            expected_value=expected_value,
            actual_value=actual_value,
            rule_name=self.name,
            item_id=item_id
        )


class SchemaValidationRule(ValidationRule):
    """Validates items against Pydantic schemas."""
    
    def __init__(
        self,
        name: str,
        model_class: type[UnifiedBaseModel],
        severity: ValidationSeverity = ValidationSeverity.ERROR
    ) -> None:
        """
        Initialize schema validation rule.
        
        Args:
            name: Rule name
            model_class: Pydantic model class for validation
            severity: Severity level for violations
        """
        super().__init__(name, ValidationRuleType.SCHEMA, severity)
        self.model_class = model_class
    
    def validate(self, item: Dict[str, Any], context: Dict[str, Any] = None) -> List[ValidationIssue]:
        """Validate item against Pydantic schema."""
        issues = []
        
        try:
            # Attempt to create model instance
            validated_item = self.model_class(**item)
            # If successful, no issues
            return issues
            
        except PydanticValidationError as e:
            for error in e.errors():
                field_path = ".".join(str(loc) for loc in error['loc'])
                issues.append(self.create_issue(
                    message=f"Schema validation failed: {error['msg']}",
                    field_path=field_path,
                    expected_value=error.get('ctx', {}).get('expected'),
                    actual_value=item.get(field_path.split('.')[0]) if '.' not in field_path else None,
                    item_id=item.get('id')
                ))
        
        except Exception as e:
            issues.append(self.create_issue(
                message=f"Schema validation error: {str(e)}",
                item_id=item.get('id')
            ))
        
        return issues


class BusinessRuleValidationRule(ValidationRule):
    """Validates items against business rules."""
    
    def __init__(
        self,
        name: str,
        validator_func: Callable[[Dict[str, Any]], bool],
        message_template: str,
        severity: ValidationSeverity = ValidationSeverity.ERROR
    ) -> None:
        """
        Initialize business rule validation.
        
        Args:
            name: Rule name
            validator_func: Function that returns True if valid
            message_template: Template for error messages
            severity: Severity level for violations
        """
        super().__init__(name, ValidationRuleType.BUSINESS_RULE, severity)
        self.validator_func = validator_func
        self.message_template = message_template
    
    def validate(self, item: Dict[str, Any], context: Dict[str, Any] = None) -> List[ValidationIssue]:
        """Validate item against business rule."""
        issues = []
        
        try:
            if not self.validator_func(item):
                issues.append(self.create_issue(
                    message=self.message_template.format(**item),
                    item_id=item.get('id')
                ))
        except Exception as e:
            issues.append(self.create_issue(
                message=f"Business rule validation error: {str(e)}",
                item_id=item.get('id')
            ))
        
        return issues


class DataQualityRule(ValidationRule):
    """Validates data quality metrics."""
    
    def __init__(
        self,
        name: str,
        field_name: str,
        quality_check: Callable[[Any], bool],
        message_template: str,
        severity: ValidationSeverity = ValidationSeverity.WARNING
    ) -> None:
        """
        Initialize data quality rule.
        
        Args:
            name: Rule name
            field_name: Field to check
            quality_check: Function that returns True if quality is good
            message_template: Template for error messages
            severity: Severity level for violations
        """
        super().__init__(name, ValidationRuleType.DATA_QUALITY, severity)
        self.field_name = field_name
        self.quality_check = quality_check
        self.message_template = message_template
    
    def validate(self, item: Dict[str, Any], context: Dict[str, Any] = None) -> List[ValidationIssue]:
        """Validate data quality."""
        issues = []
        
        if self.field_name not in item:
            issues.append(self.create_issue(
                message=f"Required field '{self.field_name}' is missing",
                field_path=self.field_name,
                item_id=item.get('id')
            ))
            return issues
        
        try:
            field_value = item[self.field_name]
            if not self.quality_check(field_value):
                issues.append(self.create_issue(
                    message=self.message_template.format(field=self.field_name, value=field_value),
                    field_path=self.field_name,
                    actual_value=field_value,
                    item_id=item.get('id')
                ))
        except Exception as e:
            issues.append(self.create_issue(
                message=f"Data quality check error for '{self.field_name}': {str(e)}",
                field_path=self.field_name,
                item_id=item.get('id')
            ))
        
        return issues


class DeduplicationService:
    """
    Service for detecting and handling duplicate data across sources.
    
    Consolidates deduplication patterns from legacy modules and provides
    comprehensive duplicate detection with configurable matching strategies.
    """
    
    def __init__(self) -> None:
        """Initialize deduplication service."""
        self.logger = get_logger(__name__)
        self.seen_hashes: Set[str] = set()
        self.seen_items: Dict[str, Dict[str, Any]] = {}
        self.duplicate_groups: Dict[str, List[Dict[str, Any]]] = {}
        
    def generate_content_hash(self, item: Dict[str, Any], fields: List[str] = None) -> str:
        """
        Generate a content hash for an item.
        
        Args:
            item: Item to hash
            fields: Specific fields to include in hash (if None, uses all)
            
        Returns:
            SHA256 hash of the item content
        """
        if fields:
            content = {k: v for k, v in item.items() if k in fields}
        else:
            content = item.copy()
        
        # Remove metadata fields that shouldn't affect uniqueness
        metadata_fields = ['id', 'created_at', 'updated_at', 'source', 'correlation_id']
        for field in metadata_fields:
            content.pop(field, None)
        
        # Sort keys for consistent hashing
        content_str = json.dumps(content, sort_keys=True, default=str)
        return hashlib.sha256(content_str.encode()).hexdigest()
    
    def generate_fuzzy_hash(self, item: Dict[str, Any], similarity_fields: List[str]) -> str:
        """
        Generate a fuzzy hash for similarity matching.
        
        Args:
            item: Item to hash
            similarity_fields: Fields to use for similarity matching
            
        Returns:
            Fuzzy hash for similarity grouping
        """
        # Extract and normalize similarity fields
        fuzzy_content = {}
        for field in similarity_fields:
            if field in item:
                value = item[field]
                if isinstance(value, str):
                    # Normalize strings for fuzzy matching
                    value = value.lower().strip()
                elif isinstance(value, (int, float)):
                    # Round numbers for fuzzy matching
                    value = round(float(value), 2)
                fuzzy_content[field] = value
        
        content_str = json.dumps(fuzzy_content, sort_keys=True, default=str)
        return hashlib.sha256(content_str.encode()).hexdigest()[:16]  # Shorter hash for grouping
    
    def detect_duplicates(
        self,
        items: List[Dict[str, Any]],
        exact_match_fields: List[str] = None,
        fuzzy_match_fields: List[str] = None
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
        """
        Detect duplicates in a list of items.
        
        Args:
            items: Items to check for duplicates
            exact_match_fields: Fields for exact duplicate detection
            fuzzy_match_fields: Fields for fuzzy duplicate detection
            
        Returns:
            Tuple of (unique_items, duplicate_items, duplicate_groups)
        """
        unique_items = []
        duplicate_items = []
        duplicate_groups = {}
        
        exact_hashes = set()
        fuzzy_groups = {}
        
        for item in items:
            item_id = item.get('id', str(uuid.uuid4()))
            
            # Check for exact duplicates
            if exact_match_fields:
                exact_hash = self.generate_content_hash(item, exact_match_fields)
                if exact_hash in exact_hashes:
                    duplicate_items.append(item)
                    # Add to duplicate group
                    if exact_hash not in duplicate_groups:
                        duplicate_groups[exact_hash] = []
                    duplicate_groups[exact_hash].append(item)
                    continue
                exact_hashes.add(exact_hash)
            
            # Check for fuzzy duplicates
            if fuzzy_match_fields:
                fuzzy_hash = self.generate_fuzzy_hash(item, fuzzy_match_fields)
                if fuzzy_hash in fuzzy_groups:
                    # Potential duplicate - need more sophisticated matching
                    is_duplicate = self._is_fuzzy_duplicate(item, fuzzy_groups[fuzzy_hash])
                    if is_duplicate:
                        duplicate_items.append(item)
                        if fuzzy_hash not in duplicate_groups:
                            duplicate_groups[fuzzy_hash] = fuzzy_groups[fuzzy_hash].copy()
                        duplicate_groups[fuzzy_hash].append(item)
                        continue
                    else:
                        fuzzy_groups[fuzzy_hash].append(item)
                else:
                    fuzzy_groups[fuzzy_hash] = [item]
            
            unique_items.append(item)
        
        self.logger.info("Duplicate detection completed",
                        total_items=len(items),
                        unique_items=len(unique_items),
                        duplicate_items=len(duplicate_items),
                        duplicate_groups=len(duplicate_groups))
        
        return unique_items, duplicate_items, duplicate_groups
    
    def _is_fuzzy_duplicate(self, item: Dict[str, Any], candidates: List[Dict[str, Any]]) -> bool:
        """
        Check if an item is a fuzzy duplicate of any candidate.
        
        Args:
            item: Item to check
            candidates: Candidate duplicates
            
        Returns:
            True if item is a fuzzy duplicate
        """
        # Simple fuzzy matching - can be enhanced with more sophisticated algorithms
        for candidate in candidates:
            similarity_score = self._calculate_similarity(item, candidate)
            if similarity_score > 0.85:  # 85% similarity threshold
                return True
        return False
    
    def _calculate_similarity(self, item1: Dict[str, Any], item2: Dict[str, Any]) -> float:
        """
        Calculate similarity score between two items.
        
        Args:
            item1: First item
            item2: Second item
            
        Returns:
            Similarity score between 0 and 1
        """
        # Simple similarity calculation based on common fields
        common_fields = set(item1.keys()) & set(item2.keys())
        if not common_fields:
            return 0.0
        
        matching_fields = 0
        for field in common_fields:
            if field in ['id', 'created_at', 'updated_at', 'source']:
                continue  # Skip metadata fields
            
            if item1[field] == item2[field]:
                matching_fields += 1
        
        return matching_fields / len(common_fields)
    
    def merge_duplicates(
        self,
        duplicate_group: List[Dict[str, Any]],
        merge_strategy: str = "newest"
    ) -> Dict[str, Any]:
        """
        Merge duplicate items using the specified strategy.
        
        Args:
            duplicate_group: List of duplicate items
            merge_strategy: Strategy for merging ("newest", "oldest", "most_complete")
            
        Returns:
            Merged item
        """
        if not duplicate_group:
            return {}
        
        if len(duplicate_group) == 1:
            return duplicate_group[0]
        
        if merge_strategy == "newest":
            # Return item with latest timestamp
            return max(duplicate_group, key=lambda x: x.get('created_at', ''))
        
        elif merge_strategy == "oldest":
            # Return item with earliest timestamp
            return min(duplicate_group, key=lambda x: x.get('created_at', ''))
        
        elif merge_strategy == "most_complete":
            # Return item with most non-null fields
            return max(duplicate_group, key=lambda x: len([v for v in x.values() if v is not None]))
        
        else:
            # Default: return first item
            return duplicate_group[0]


class DataQualityValidator:
    """
    Comprehensive data quality validator consolidating validation patterns
    from all legacy modules.
    
    Provides schema validation, business rule validation, data quality checks,
    and deduplication services in a unified interface.
    """
    
    def __init__(self) -> None:
        """Initialize data quality validator."""
        self.logger = get_logger(__name__)
        self.rules: List[ValidationRule] = []
        self.deduplication_service = DeduplicationService()
        
        # Initialize default rules
        self._initialize_default_rules()
    
    def _initialize_default_rules(self) -> None:
        """Initialize default validation rules."""
        # Schema validation rules for each model type
        self.add_rule(SchemaValidationRule("game_schema", UnifiedGame))
        self.add_rule(SchemaValidationRule("odds_schema", BettingMarket))
        self.add_rule(SchemaValidationRule("betting_analysis_schema", BettingAnalysis))
        self.add_rule(SchemaValidationRule("sharp_data_schema", SharpSignal))
        
        # Business rule validations
        self.add_rule(BusinessRuleValidationRule(
            "valid_game_date",
            lambda item: self._is_valid_game_date(item.get('game_date')),
            "Game date {game_date} is not valid"
        ))
        
        self.add_rule(BusinessRuleValidationRule(
            "valid_odds_range",
            lambda item: self._is_valid_odds_range(item.get('odds_decimal')),
            "Odds value {odds_decimal} is outside valid range"
        ))
        
        # Data quality rules
        self.add_rule(DataQualityRule(
            "non_empty_team_names",
            "home_team",
            lambda value: value and len(str(value).strip()) > 0,
            "Team name '{value}' is empty or invalid",
            ValidationSeverity.ERROR
        ))
        
        self.add_rule(DataQualityRule(
            "reasonable_odds_values",
            "odds_decimal",
            lambda value: value and 1.01 <= float(value) <= 100.0,
            "Odds value {value} is unreasonable",
            ValidationSeverity.WARNING
        ))
    
    def add_rule(self, rule: ValidationRule) -> None:
        """Add a validation rule."""
        self.rules.append(rule)
        self.logger.debug("Validation rule added", rule_name=rule.name, rule_type=rule.rule_type.value)
    
    def remove_rule(self, rule_name: str) -> None:
        """Remove a validation rule by name."""
        self.rules = [rule for rule in self.rules if rule.name != rule_name]
        self.logger.debug("Validation rule removed", rule_name=rule_name)
    
    def validate(
        self,
        items: List[Dict[str, Any]],
        model_type: Optional[type[UnifiedBaseModel]] = None,
        strict_mode: bool = False,
        enable_deduplication: bool = True
    ) -> ValidationResult:
        """
        Validate a list of items.
        
        Args:
            items: Items to validate
            model_type: Expected model type for schema validation
            strict_mode: If True, stop on first critical error
            enable_deduplication: Whether to perform deduplication
            
        Returns:
            ValidationResult with validation outcomes
        """
        start_time = datetime.now()
        result = ValidationResult()
        result.total_items = len(items)
        
        self.logger.info("Starting validation",
                        total_items=len(items),
                        model_type=model_type.__name__ if model_type else None,
                        strict_mode=strict_mode)
        
        # Deduplication first if enabled
        if enable_deduplication:
            unique_items, duplicate_items, duplicate_groups = self.deduplication_service.detect_duplicates(
                items,
                exact_match_fields=['home_team', 'away_team', 'game_date'],  # Game-specific
                fuzzy_match_fields=['home_team', 'away_team', 'game_date', 'odds_decimal']
            )
            
            result.duplicate_items = len(duplicate_items)
            for dup_hash, dup_group in duplicate_groups.items():
                result.add_issue(ValidationIssue(
                    rule_type=ValidationRuleType.DEDUPLICATION,
                    severity=ValidationSeverity.WARNING,
                    message=f"Found {len(dup_group)} duplicate items",
                    rule_name="deduplication_check"
                ))
            
            # Use unique items for further validation
            items = unique_items
        
        # Validate each item
        for item in items:
            item_issues = []
            
            # Apply all enabled rules
            for rule in self.rules:
                if not rule.enabled:
                    continue
                
                # Skip schema rules if model type doesn't match
                if (rule.rule_type == ValidationRuleType.SCHEMA and 
                    model_type and 
                    hasattr(rule, 'model_class') and 
                    rule.model_class != model_type):
                    continue
                
                try:
                    rule_issues = rule.validate(item)
                    item_issues.extend(rule_issues)
                    
                    # Check for critical issues in strict mode
                    if strict_mode and any(issue.severity == ValidationSeverity.CRITICAL for issue in rule_issues):
                        result.add_issue(ValidationIssue(
                            rule_type=ValidationRuleType.SCHEMA,
                            severity=ValidationSeverity.CRITICAL,
                            message="Validation stopped due to critical error in strict mode",
                            rule_name="strict_mode_check"
                        ))
                        break
                        
                except Exception as e:
                    self.logger.error("Rule validation failed", 
                                    rule_name=rule.name, 
                                    error=str(e))
                    item_issues.append(ValidationIssue(
                        rule_type=ValidationRuleType.SCHEMA,
                        severity=ValidationSeverity.ERROR,
                        message=f"Rule execution failed: {str(e)}",
                        rule_name=rule.name
                    ))
            
            # Determine if item is valid
            has_critical_issues = any(issue.severity == ValidationSeverity.CRITICAL for issue in item_issues)
            has_error_issues = any(issue.severity == ValidationSeverity.ERROR for issue in item_issues)
            
            if has_critical_issues or (strict_mode and has_error_issues):
                result.invalid_items.append(item)
                result.invalid_items_count += 1
            else:
                # Try to create validated model instance
                try:
                    if model_type:
                        validated_item = model_type(**item)
                        result.validated_items.append(validated_item)
                    else:
                        # Generic validation - keep as dict
                        result.validated_items.append(item)
                    result.valid_items_count += 1
                except Exception as e:
                    result.invalid_items.append(item)
                    result.invalid_items_count += 1
                    item_issues.append(ValidationIssue(
                        rule_type=ValidationRuleType.SCHEMA,
                        severity=ValidationSeverity.ERROR,
                        message=f"Model instantiation failed: {str(e)}",
                        rule_name="model_instantiation"
                    ))
            
            # Add all issues to result
            for issue in item_issues:
                result.add_issue(issue)
        
        # Finalize result
        result.is_valid = result.invalid_items_count == 0 and not result.has_critical_issues
        result.validation_time_ms = (datetime.now() - start_time).total_seconds() * 1000
        
        self.logger.info("Validation completed",
                        correlation_id=result.correlation_id,
                        is_valid=result.is_valid,
                        success_rate=result.success_rate,
                        validation_time_ms=result.validation_time_ms,
                        total_issues=len(result.issues))
        
        return result
    
    def _is_valid_game_date(self, game_date: Any) -> bool:
        """Validate game date is reasonable."""
        if not game_date:
            return False
        
        try:
            if isinstance(game_date, str):
                date_obj = datetime.fromisoformat(game_date.replace('Z', '+00:00'))
            elif isinstance(game_date, datetime):
                date_obj = game_date
            else:
                return False
            
            # Check if date is within reasonable range (not too far in past/future)
            now = datetime.now()
            min_date = now - timedelta(days=365)  # 1 year ago
            max_date = now + timedelta(days=365)  # 1 year from now
            
            return min_date <= date_obj <= max_date
            
        except Exception:
            return False
    
    def _is_valid_odds_range(self, odds: Any) -> bool:
        """Validate odds are in reasonable range."""
        if not odds:
            return False
        
        try:
            odds_value = float(odds)
            return 1.01 <= odds_value <= 1000.0  # Reasonable odds range
        except (ValueError, TypeError):
            return False
    
    def get_validation_metrics(self) -> Dict[str, Any]:
        """Get validation metrics and statistics."""
        return {
            "total_rules": len(self.rules),
            "enabled_rules": len([r for r in self.rules if r.enabled]),
            "rules_by_type": {
                rule_type.value: len([r for r in self.rules if r.rule_type == rule_type])
                for rule_type in ValidationRuleType
            },
            "rules_by_severity": {
                severity.value: len([r for r in self.rules if r.severity == severity])
                for severity in ValidationSeverity
            }
        }


__all__ = [
    "DataQualityValidator",
    "ValidationResult",
    "ValidationRule",
    "ValidationIssue",
    "ValidationSeverity",
    "ValidationRuleType",
    "SchemaValidationRule",
    "BusinessRuleValidationRule",
    "DataQualityRule",
    "DeduplicationService"
] 