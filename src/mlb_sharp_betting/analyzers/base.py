"""Base analyzer classes and interfaces."""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional
from dataclasses import dataclass

from ..core.exceptions import MLBSharpBettingError, AnalysisError
from ..models.base import BaseModel


class AnalyzerError(MLBSharpBettingError):
    """Base exception for analyzer-related errors."""
    pass


@dataclass
class AnalysisResult:
    """Result of an analysis operation."""
    
    success: bool
    data: Dict[str, Any]
    analyzer: str
    timestamp: datetime
    errors: List[str]
    confidence: Optional[float] = None
    metadata: Optional[Dict[str, Any]] = None
    
    @property
    def has_data(self) -> bool:
        """Check if analysis result contains data."""
        return self.success and bool(self.data)
    
    @property
    def error_count(self) -> int:
        """Get number of errors encountered."""
        return len(self.errors)
    
    @property
    def is_confident(self) -> bool:
        """Check if analysis has high confidence."""
        return self.confidence is not None and self.confidence > 0.7


class BaseAnalyzer(ABC):
    """Base class for all data analyzers."""
    
    def __init__(self, analyzer_name: str):
        """
        Initialize base analyzer.
        
        Args:
            analyzer_name: Name of the analyzer
        """
        self.analyzer_name = analyzer_name
        self.last_analyzed: Optional[datetime] = None
    
    @abstractmethod
    def analyze(self, data: Any) -> AnalysisResult:
        """
        Analyze data and provide insights.
        
        Args:
            data: Data to analyze
            
        Returns:
            AnalysisResult containing insights
        """
        pass
    
    def _create_result(
        self,
        success: bool,
        data: Dict[str, Any],
        errors: List[str],
        confidence: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> AnalysisResult:
        """Create a standardized analysis result."""
        return AnalysisResult(
            success=success,
            data=data,
            analyzer=self.analyzer_name,
            timestamp=datetime.now(),
            errors=errors,
            confidence=confidence,
            metadata=metadata
        )


__all__ = ["AnalyzerError", "BaseAnalyzer", "AnalysisResult"] 