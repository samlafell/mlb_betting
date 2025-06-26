"""
Strategy Processor Factory

Dynamic factory for creating and managing all strategy processors.
Provides centralized processor creation, registration, and error handling.

Part of Phase 1 foundation architecture for comprehensive strategy processing.
"""

from typing import Dict, List, Type, Optional
import importlib
import inspect

from .base_strategy_processor import BaseStrategyProcessor
from .real_time_processor import RealTimeProcessor
from ...services.betting_signal_repository import BettingSignalRepository
from ...services.strategy_validator import StrategyValidator
from ...models.betting_analysis import SignalProcessorConfig
from ...core.logging import get_logger


class StrategyProcessorFactory:
    """
    Factory for creating and managing all strategy processors
    
    Provides dynamic processor creation, registration, and graceful
    error handling for missing or failed processor implementations.
    """
    
    # Map SQL strategies to processor classes
    PROCESSOR_MAPPING = {
        # Existing processor
        'sharp_action': 'RealTimeProcessor',
        
        # High priority processors (Phase 2 - Week 2)
        'opposing_markets': 'OpposingMarketsProcessor',
        'book_conflicts': 'BookConflictProcessor',
        'public_money_fade': 'PublicFadeProcessor',
        
        # Medium priority processors (Phase 2 - Week 3)
        'late_sharp_flip': 'LateFlipProcessor',
        'consensus_moneyline': 'ConsensusProcessor',
        'underdog_ml_value': 'UnderdogValueProcessor',
        'timing_based': 'TimingProcessor',
        'line_movement': 'LineMovementProcessor',
        
        # Additional processors
        'team_specific_bias': 'TeamBiasProcessor',
        'hybrid_line_sharp': 'HybridLineProcessor',
        'total_line_sweet_spots': 'TotalSweetSpotsProcessor',
        'signal_combinations': 'SignalCombinationProcessor',
        'enhanced_late_sharp_flip': 'EnhancedLateFlipProcessor',
        'strategy_comparison_roi': 'StrategyComparisonProcessor',
    }
    
    def __init__(self, repository: BettingSignalRepository, 
                 validator: StrategyValidator, config: SignalProcessorConfig):
        """Initialize factory with required dependencies"""
        self.repository = repository
        self.validator = validator
        self.config = config
        self.logger = get_logger(__name__)
        
        # Cache for loaded processor classes
        self._processor_class_cache: Dict[str, Type[BaseStrategyProcessor]] = {}
        
        # Registry of successfully created processors
        self._processor_registry: Dict[str, BaseStrategyProcessor] = {}
    
    def create_all_processors(self) -> Dict[str, BaseStrategyProcessor]:
        """
        Create all available strategy processors
        
        Returns:
            Dict mapping strategy names to processor instances
        """
        processors = {}
        
        # Always include the existing sharp action processor
        try:
            processors['sharp_action'] = RealTimeProcessor(
                self.repository, self.validator, self.config
            )
            self.logger.info("✅ Sharp action processor loaded successfully")
        except Exception as e:
            self.logger.error(f"❌ Failed to load sharp action processor: {e}")
        
        # Create all other processors
        successful_loads = []
        failed_loads = []
        
        for strategy_name, processor_class_name in self.PROCESSOR_MAPPING.items():
            if strategy_name == 'sharp_action':
                continue  # Already handled above
                
            try:
                processor = self.create_processor(strategy_name)
                if processor:
                    processors[strategy_name] = processor
                    successful_loads.append(strategy_name)
                else:
                    failed_loads.append(strategy_name)
            except Exception as e:
                self.logger.warning(f"⚠️  Failed to create {strategy_name} processor: {e}")
                failed_loads.append(strategy_name)
        
        # Log summary
        total_available = len(self.PROCESSOR_MAPPING)
        total_loaded = len(processors)
        
        self.logger.info(
            f"🏭 Processor Factory Summary: {total_loaded}/{total_available} processors loaded"
        )
        
        if successful_loads:
            self.logger.info(f"✅ Successfully loaded: {', '.join(successful_loads)}")
        
        if failed_loads:
            self.logger.warning(f"⚠️  Failed to load: {', '.join(failed_loads)}")
            
        # Cache successful processors
        self._processor_registry = processors
        
        return processors
    
    def create_processor(self, strategy_name: str) -> Optional[BaseStrategyProcessor]:
        """
        Create a single strategy processor by name
        
        Args:
            strategy_name: Name of the strategy processor to create
            
        Returns:
            Processor instance or None if creation fails
        """
        if strategy_name not in self.PROCESSOR_MAPPING:
            self.logger.warning(f"Unknown strategy: {strategy_name}")
            return None
        
        processor_class_name = self.PROCESSOR_MAPPING[strategy_name]
        
        try:
            # Special handling for existing processor
            if strategy_name == 'sharp_action':
                return RealTimeProcessor(self.repository, self.validator, self.config)
            
            # Try to load the processor class
            processor_class = self._get_processor_class(processor_class_name)
            
            if not processor_class:
                return None
            
            # Create processor instance
            processor = processor_class(self.repository, self.validator, self.config)
            
            # Validate it's properly implemented
            if not self._validate_processor(processor):
                self.logger.error(f"Processor {processor_class_name} failed validation")
                return None
            
            return processor
            
        except Exception as e:
            self.logger.error(f"Error creating {processor_class_name}: {e}")
            return None
    
    def _get_processor_class(self, processor_class_name: str) -> Optional[Type[BaseStrategyProcessor]]:
        """
        Dynamically load processor class by name
        
        Args:
            processor_class_name: Name of the processor class to load
            
        Returns:
            Processor class or None if not found
        """
        # Check cache first
        if processor_class_name in self._processor_class_cache:
            return self._processor_class_cache[processor_class_name]
        
        try:
            # Try to import from the processors module
            module_name = f"mlb_sharp_betting.analysis.processors.{processor_class_name.lower().replace('processor', '_processor')}"
            
            try:
                module = importlib.import_module(module_name)
                processor_class = getattr(module, processor_class_name)
                
                # Validate it's a proper processor class
                if (inspect.isclass(processor_class) and 
                    issubclass(processor_class, BaseStrategyProcessor)):
                    
                    self._processor_class_cache[processor_class_name] = processor_class
                    return processor_class
                    
            except (ImportError, AttributeError):
                # Processor not yet implemented - this is expected during development
                self.logger.debug(f"Processor {processor_class_name} not yet implemented")
                return None
                
        except Exception as e:
            self.logger.error(f"Error loading processor class {processor_class_name}: {e}")
            return None
    
    def _validate_processor(self, processor: BaseStrategyProcessor) -> bool:
        """
        Validate that a processor is properly implemented
        
        Args:
            processor: Processor instance to validate
            
        Returns:
            True if processor is valid, False otherwise
        """
        try:
            # Check required methods exist and return reasonable values
            signal_type = processor.get_signal_type()
            category = processor.get_strategy_category()
            tables = processor.get_required_tables()
            description = processor.get_strategy_description()
            
            # Basic validation
            if not all([signal_type, category, isinstance(tables, list), description]):
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Processor validation failed: {e}")
            return False
    
    def get_available_strategies(self) -> List[str]:
        """Get list of all available strategy names"""
        return list(self.PROCESSOR_MAPPING.keys())
    
    def get_loaded_processors(self) -> Dict[str, BaseStrategyProcessor]:
        """Get dict of successfully loaded processors"""
        return self._processor_registry.copy()
    
    def get_processor_info(self) -> List[Dict]:
        """Get information about all loaded processors"""
        return [
            processor.get_processor_info() 
            for processor in self._processor_registry.values()
        ]
    
    def get_implementation_status(self) -> Dict[str, str]:
        """Get implementation status of all strategies"""
        status = {}
        
        for strategy_name in self.PROCESSOR_MAPPING.keys():
            if strategy_name in self._processor_registry:
                status[strategy_name] = "IMPLEMENTED"
            else:
                # Try to determine why it's not loaded
                processor_class_name = self.PROCESSOR_MAPPING[strategy_name]
                if self._get_processor_class(processor_class_name):
                    status[strategy_name] = "AVAILABLE_BUT_FAILED"
                else:
                    status[strategy_name] = "NOT_IMPLEMENTED"
        
        return status 