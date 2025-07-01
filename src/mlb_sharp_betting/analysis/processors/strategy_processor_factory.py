"""
Strategy Processor Factory

Dynamic factory for creating and managing all strategy processors.
Provides centralized processor creation, registration, and error handling.

🚨 CRITICAL: This factory replaces the problematic dual SQL+Factory approach
that was causing strategy duplication in backtesting. All SQL strategies
should be migrated to proper processor implementations.

Part of Phase 1 foundation architecture for comprehensive strategy processing.
"""

from typing import Dict, List, Type, Optional, Any
import importlib
import inspect

from mlb_sharp_betting.analysis.processors.base_strategy_processor import BaseStrategyProcessor
from mlb_sharp_betting.analysis.processors.sharpaction_processor import SharpActionProcessor
from mlb_sharp_betting.services.betting_signal_repository import BettingSignalRepository
from mlb_sharp_betting.services.strategy_validation import StrategyValidation
from mlb_sharp_betting.services.dynamic_threshold_manager import get_dynamic_threshold_manager
from mlb_sharp_betting.models.betting_analysis import SignalProcessorConfig
from mlb_sharp_betting.core.logging import get_logger


class StrategyProcessorFactory:
    """
    Factory for creating and managing all strategy processors
    
    🚨 ENHANCED: Now handles migration from SQL scripts to avoid duplication
    🚨 FIXED: Added circuit breaker to prevent infinite loops in status checking
    
    Provides dynamic processor creation, registration, and graceful
    error handling for missing or failed processor implementations.
    """
    
    # 🚨 UPDATED: Comprehensive mapping showing migration status
    # Map SQL strategies to processor classes with implementation status
    PROCESSOR_MAPPING = {
        # ✅ IMPLEMENTED - Working processors
        'sharp_action': {
            'class': 'SharpActionProcessor',
            'status': 'IMPLEMENTED',
            'sql_equivalent': 'sharp_action_detector_postgres.sql',
            'description': 'Core sharp action detection (money vs bet percentage differentials)'
        },
        'opposing_markets': {
            'class': 'OpposingMarketsProcessor',
            'status': 'IMPLEMENTED',
            'sql_equivalent': 'opposing_markets_strategy.sql',
            'description': 'Opposing market analysis'
        },
        'book_conflicts': {
            'class': 'BookConflictProcessor',
            'status': 'IMPLEMENTED',
            'sql_equivalent': 'book_conflicts_strategy.sql',
            'description': 'Book conflict detection'
        },
        'public_money_fade': {
            'class': 'PublicFadeProcessor',
            'status': 'IMPLEMENTED',
            'sql_equivalent': 'public_money_fade_strategy.sql',
            'description': 'Public money fade strategy'
        },
        
        'late_sharp_flip': {
            'class': 'LateFlipProcessor',
            'status': 'IMPLEMENTED',
            'sql_equivalent': 'enhanced_late_sharp_flip_strategy.sql',
            'description': 'Late sharp money flip detection'
        },
        'consensus_moneyline': {
            'class': 'ConsensusProcessor',
            'status': 'IMPLEMENTED',
            'sql_equivalent': 'consensus_moneyline_strategy.sql',
            'description': 'Consensus moneyline analysis'
        },
        'underdog_ml_value': {
            'class': 'UnderdogValueProcessor',
            'status': 'IMPLEMENTED',
            'sql_equivalent': 'underdog_ml_value_strategy.sql',
            'description': 'Underdog moneyline value detection'
        },
        
        # 📋 MEDIUM PRIORITY - Additional strategy migrations
        'timing_based': {
            'class': 'TimingBasedProcessor',
            'status': 'IMPLEMENTED',
            'sql_equivalent': 'timing_based_strategy_postgres.sql',
            'description': 'Advanced timing-based strategy with 9 timing categories (22KB legacy file)'
        },
        'line_movement': {
            'class': 'LineMovementProcessor',
            'status': 'IMPLEMENTED',
            'sql_equivalent': 'line_movement_strategy.sql',
            'description': 'Line movement analysis'
        },
        'team_specific_bias': {
            'class': 'TeamBiasProcessor',
            'status': 'PLANNED',
            'sql_equivalent': 'team_specific_bias_strategy.sql',
            'description': 'Team-specific bias detection'
        },
        'hybrid_line_sharp': {
            'class': 'HybridSharpProcessor',
            'status': 'PLANNED',  # Temporarily disabled due to SQL parameter issue
            'sql_equivalent': 'hybrid_line_sharp_strategy_postgres.sql',
            'description': 'Hybrid line movement + sharp action with confirmation signals (11KB legacy file)'
        },
        'total_line_sweet_spots': {
            'class': 'TotalSweetSpotsProcessor',
            'status': 'PLANNED',
            'sql_equivalent': 'total_line_sweet_spots_strategy.sql',
            'description': 'Total line sweet spot detection'
        },
        'signal_combinations': {
            'class': 'SignalCombinationProcessor',
            'status': 'PLANNED',
            'sql_equivalent': 'signal_combinations.sql',
            'description': 'Multi-signal combination analysis'
        },
        
        # 📊 ANALYTICAL - Summary and comparison processors
        'strategy_comparison_roi': {
            'class': 'StrategyComparisonProcessor',
            'status': 'PLANNED',
            'sql_equivalent': 'strategy_comparison_roi.sql',
            'description': 'Cross-strategy ROI comparison'
        },
        'executive_summary': {
            'class': 'ExecutiveSummaryProcessor',
            'status': 'PLANNED',
            'sql_equivalent': 'executive_summary_report.sql',
            'description': 'Executive summary reporting'
        }
    }
    
    def __init__(self, repository: BettingSignalRepository, 
                 validator: StrategyValidation, config: SignalProcessorConfig):
        """Initialize factory with required dependencies"""
        self.repository = repository
        self.validator = validator
        self.config = config
        self.logger = get_logger(__name__)
        
        # 🎯 DYNAMIC THRESHOLDS: Initialize threshold manager
        self.threshold_manager = get_dynamic_threshold_manager()
        
        # Cache for loaded processor classes
        self._processor_class_cache: Dict[str, Type[BaseStrategyProcessor]] = {}
        
        # Registry of successfully created processors
        self._processor_registry: Dict[str, BaseStrategyProcessor] = {}
        
        # 🚨 NEW: Track migration status
        self._migration_status = self._initialize_migration_status()
        
        # 🚨 CIRCUIT BREAKER: Prevent infinite loops in status checking
        self._loop_detection = {
            "check_count": 0,
            "max_checks": 50,  # Prevent infinite status checks
            "last_strategy_checked": None,
            "status_cache": {},  # Cache status to avoid repeated checks
            "reset_threshold": 10  # Reset counter after this many different strategies
        }
        
        # 🚨 FIX: Automatically load all implemented processors during initialization
        self.logger.info("Loading implemented processors during factory initialization...")
        self.logger.info("🎯 DYNAMIC THRESHOLDS ENABLED: Using progressive threshold optimization")
        self.logger.info(f"📋 Available Strategies: {len(self.PROCESSOR_MAPPING)} total")
        
        # Show which processors we expect to load
        implemented_strategies = [name for name, info in self.PROCESSOR_MAPPING.items() 
                                 if info['status'] == 'IMPLEMENTED']
        self.logger.info(f"🔄 Attempting to load {len(implemented_strategies)} implemented processors:")
        self.logger.info(f"   {', '.join(implemented_strategies)}")
        
        try:
            self.create_all_processors()
            loaded_names = list(self._processor_registry.keys())
            self.logger.info(f"✅ Factory initialized with {len(self._processor_registry)} processors:")
            self.logger.info(f"   Loaded: {', '.join(loaded_names)}")
            
            if len(loaded_names) != len(implemented_strategies):
                missing = set(implemented_strategies) - set(loaded_names)
                self.logger.warning(f"⚠️  Failed to load some processors: {', '.join(missing)}")
                
        except Exception as e:
            self.logger.warning(f"⚠️  Some processors failed to load during initialization: {e}")
    
    def _initialize_migration_status(self) -> Dict[str, str]:
        """Initialize migration status tracking."""
        status = {}
        for strategy_name, info in self.PROCESSOR_MAPPING.items():
            status[strategy_name] = info['status']
        return status
    
    def _check_processor_status_safely(self, strategy_name: str) -> str:
        """Check processor status with circuit breaker to prevent infinite loops."""
        
        # Check cache first
        if strategy_name in self._loop_detection["status_cache"]:
            return self._loop_detection["status_cache"][strategy_name]
        
        # Reset loop detection if we've moved to a different strategy
        if self._loop_detection["last_strategy_checked"] != strategy_name:
            if self._loop_detection["check_count"] > self._loop_detection["reset_threshold"]:
                self._loop_detection["check_count"] = 0
            self._loop_detection["last_strategy_checked"] = strategy_name
        
        # Increment check count
        self._loop_detection["check_count"] += 1
        
        # Circuit breaker - if we've checked too many times, assume PLANNED
        if self._loop_detection["check_count"] > self._loop_detection["max_checks"]:
            self.logger.warning(
                f"🚨 Circuit breaker activated for {strategy_name} - too many status checks"
            )
            status = "PLANNED"
            self._loop_detection["status_cache"][strategy_name] = status
            return status
        
        try:
            # Get status from mapping
            if strategy_name in self.PROCESSOR_MAPPING:
                status = self.PROCESSOR_MAPPING[strategy_name]['status']
                # Cache the status to prevent repeated checks
                self._loop_detection["status_cache"][strategy_name] = status
                return status
            else:
                status = "PLANNED"
                self._loop_detection["status_cache"][strategy_name] = status
                return status
                
        except Exception as e:
            self.logger.debug(f"Error checking status for {strategy_name}: {e}")
            status = "PLANNED"
            self._loop_detection["status_cache"][strategy_name] = status
            return status
    
    def create_all_processors(self) -> Dict[str, BaseStrategyProcessor]:
        """
        Create all available strategy processors
        
        🚨 ENHANCED: Now provides detailed migration reporting
        
        Returns:
            Dict mapping strategy names to processor instances
        """
        processors = {}
        
        # Track results by status
        implemented_processors = []
        in_progress_processors = []
        planned_processors = []
        failed_processors = []
        
        # Create all processors based on their status
        for strategy_name, info in self.PROCESSOR_MAPPING.items():
            status = self._check_processor_status_safely(strategy_name)
            
            try:
                processor = self.create_processor(strategy_name)
                if processor:
                    processors[strategy_name] = processor
                    implemented_processors.append(strategy_name)
                else:
                    if status == 'IMPLEMENTED':
                        failed_processors.append(strategy_name)
                    elif status == 'IN_PROGRESS':
                        in_progress_processors.append(strategy_name)
                    else:
                        planned_processors.append(strategy_name)
                        
            except Exception as e:
                self.logger.warning(f"⚠️  Failed to create {strategy_name} processor: {e}")
                failed_processors.append(strategy_name)
        
        # Comprehensive migration status report
        total_strategies = len(self.PROCESSOR_MAPPING)
        
        self.logger.info(
            f"🏭 STRATEGY PROCESSOR FACTORY REPORT:\n"
            f"{'='*60}\n"
            f"📊 Total Strategies Mapped: {total_strategies}\n"
            f"✅ Implemented & Working: {len(implemented_processors)}\n"
            f"🔄 In Progress: {len(in_progress_processors)}\n"
            f"📋 Planned: {len(planned_processors)}\n"
            f"❌ Failed to Load: {len(failed_processors)}\n"
            f"{'='*60}"
        )
        
        if implemented_processors:
            self.logger.info(f"✅ WORKING: {', '.join(implemented_processors)}")
        
        if in_progress_processors:
            self.logger.info(f"🔄 IN PROGRESS: {', '.join(in_progress_processors)}")
            
        if planned_processors:
            self.logger.info(f"📋 PLANNED (need implementation): {', '.join(planned_processors)}")
            
        if failed_processors:
            self.logger.warning(f"❌ FAILED TO LOAD: {', '.join(failed_processors)}")
        
        # 🚨 CRITICAL RECOMMENDATION
        sql_duplication_risk = len(planned_processors) + len(in_progress_processors)
        if sql_duplication_risk > 0:
            self.logger.warning(
                f"🚨 DUPLICATION RISK: {sql_duplication_risk} strategies still using SQL scripts\n"
                f"   This causes the 76→11 reliable strategy fragmentation issue\n"
                f"   Priority: Implement planned processors to eliminate SQL duplication"
            )
        
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

        processor_info = self.PROCESSOR_MAPPING[strategy_name]
        processor_class_name = processor_info['class']
        status = self._check_processor_status_safely(strategy_name)

        # Only attempt to create if marked as implemented
        if status != 'IMPLEMENTED':
            self.logger.debug(f"Skipping {strategy_name} - status: {status}")
            return None

        try:
            # 🎯 DYNAMIC THRESHOLDS: Pass threshold manager to all processors
            # Special handling for all IMPLEMENTED processors
            if strategy_name == 'sharp_action':
                from .sharpaction_processor import SharpActionProcessor
                processor = SharpActionProcessor(self.repository, self.validator, self.config)
                processor.threshold_manager = self.threshold_manager
                return processor
            elif strategy_name == 'opposing_markets':
                from .opposingmarkets_processor import OpposingMarketsProcessor
                processor = OpposingMarketsProcessor(self.repository, self.validator, self.config)
                processor.threshold_manager = self.threshold_manager
                return processor
            elif strategy_name == 'book_conflicts':
                from .bookconflict_processor import BookConflictProcessor
                processor = BookConflictProcessor(self.repository, self.validator, self.config)
                processor.threshold_manager = self.threshold_manager
                return processor
            elif strategy_name == 'public_money_fade':
                from .publicfade_processor import PublicFadeProcessor
                processor = PublicFadeProcessor(self.repository, self.validator, self.config)
                processor.threshold_manager = self.threshold_manager
                return processor
            elif strategy_name == 'late_sharp_flip':
                from .lateflip_processor import LateFlipProcessor
                processor = LateFlipProcessor(self.repository, self.validator, self.config)
                processor.threshold_manager = self.threshold_manager
                return processor
            elif strategy_name == 'consensus_moneyline':
                from .consensus_processor import ConsensusProcessor
                processor = ConsensusProcessor(self.repository, self.validator, self.config)
                processor.threshold_manager = self.threshold_manager
                return processor
            elif strategy_name == 'underdog_ml_value':
                from .underdogvalue_processor import UnderdogValueProcessor
                processor = UnderdogValueProcessor(self.repository, self.validator, self.config)
                processor.threshold_manager = self.threshold_manager
                return processor
            elif strategy_name == 'line_movement':
                from .linemovement_processor import LineMovementProcessor
                processor = LineMovementProcessor(self.repository, self.validator, self.config)
                processor.threshold_manager = self.threshold_manager
                return processor
            elif strategy_name == 'timing_based':
                from .timingbased_processor import TimingBasedProcessor
                processor = TimingBasedProcessor(self.repository, self.validator, self.config)
                processor.threshold_manager = self.threshold_manager
                return processor
            elif strategy_name == 'hybrid_line_sharp':
                from .hybridsharp_processor import HybridSharpProcessor
                processor = HybridSharpProcessor(self.repository, self.validator, self.config)
                processor.threshold_manager = self.threshold_manager
                return processor
            else:
                # Try dynamic loading for other processors
                processor_class = self._get_processor_class(processor_class_name)
                
                if not processor_class:
                    self.logger.warning(f"Processor class {processor_class_name} not found - needs implementation")
                    return None
                
                # Create processor instance
                processor = processor_class(self.repository, self.validator, self.config)
                
                # Validate it's properly implemented
                if not self._validate_processor(processor):
                    self.logger.error(f"Processor {processor_class_name} failed validation")
                    return None
                
                return processor
            
        except Exception as e:
            self.logger.error(f"Error creating {processor_class_name} for {strategy_name}: {e}")
            import traceback
            self.logger.debug(f"Import traceback: {traceback.format_exc()}")
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
            # Try to import from the processors module with proper naming conversion
            # Convert class names like OpposingMarketsProcessor to opposingmarkets_processor
            base_name = processor_class_name.replace('Processor', '').lower()
            
            module_name = f"mlb_sharp_betting.analysis.processors.{base_name}_processor"
            
            try:
                module = importlib.import_module(module_name)
                processor_class = getattr(module, processor_class_name)
                
                # Validate it's a proper processor class
                if (inspect.isclass(processor_class) and 
                    issubclass(processor_class, BaseStrategyProcessor)):
                    
                    self._processor_class_cache[processor_class_name] = processor_class
                    return processor_class
                    
            except (ImportError, AttributeError) as e:
                # Processor not yet implemented - this is expected during development
                self.logger.debug(f"Processor {processor_class_name} not yet implemented: {e}")
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
    
    def get_all_processors(self) -> Dict[str, BaseStrategyProcessor]:
        """Get all processors (loaded and attempt to load any missing ones)"""
        # First ensure we have all processors loaded
        if not self._processor_registry:
            self.create_all_processors()
        return self._processor_registry.copy()
    
    def get_processor_info(self) -> List[Dict]:
        """Get information about all loaded processors"""
        return [
            processor.get_processor_info() 
            for processor in self._processor_registry.values()
        ]
    
    def get_implementation_status(self) -> Dict[str, str]:
        """
        Get detailed implementation status of all strategies
        
        🚨 ENHANCED: Now includes migration planning info
        🚨 FIXED: Uses safe status checking to prevent infinite loops
        
        Returns:
            Dict mapping strategy names to detailed status
        """
        status = {}
        
        for strategy_name in self.PROCESSOR_MAPPING.keys():
            if strategy_name in self._processor_registry:
                status[strategy_name] = "IMPLEMENTED"
            else:
                # Use safe status checking to prevent loops
                base_status = self._check_processor_status_safely(strategy_name)
                
                # If marked as implemented but not in registry, try to create it
                if base_status == 'IMPLEMENTED':
                    try:
                        processor = self.create_processor(strategy_name)
                        if processor:
                            status[strategy_name] = "IMPLEMENTED"
                        else:
                            status[strategy_name] = "AVAILABLE_BUT_FAILED"
                    except Exception:
                        status[strategy_name] = "AVAILABLE_BUT_FAILED"
                else:
                    status[strategy_name] = base_status
        
        return status
    
    def get_migration_report(self) -> Dict[str, Any]:
        """
        Generate comprehensive migration report for SQL→Processor transition.
        
        This helps understand the current duplication issues and migration priorities.
        
        Returns:
            Detailed migration status report
        """
        implemented = []
        in_progress = []
        planned = []
        sql_scripts_still_active = []
        
        for strategy_name, info in self.PROCESSOR_MAPPING.items():
            status = info['status']
            sql_equivalent = info['sql_equivalent']
            
            if status == 'IMPLEMENTED':
                implemented.append({
                    'name': strategy_name,
                    'sql_script': sql_equivalent,
                    'description': info['description']
                })
            elif status == 'IN_PROGRESS':
                in_progress.append({
                    'name': strategy_name,
                    'sql_script': sql_equivalent,
                    'description': info['description']
                })
            else:
                planned.append({
                    'name': strategy_name,
                    'sql_script': sql_equivalent,
                    'description': info['description']
                })
                sql_scripts_still_active.append(sql_equivalent)
        
        return {
            'total_strategies': len(self.PROCESSOR_MAPPING),
            'implemented_count': len(implemented),
            'in_progress_count': len(in_progress),
            'planned_count': len(planned),
            'duplication_risk_count': len(planned) + len(in_progress),
            'implemented_details': implemented,
            'in_progress_details': in_progress,
            'planned_details': planned,
            'sql_scripts_still_active': sql_scripts_still_active,
            'migration_priority': 'HIGH' if len(planned) > 5 else 'MEDIUM',
            'recommendation': (
                f"🚨 CRITICAL: {len(sql_scripts_still_active)} SQL scripts still active, "
                f"causing strategy duplication. Priority: Implement {len(planned)} planned processors."
            )
        }
    
    def get_processors_by_type(self, signal_type: str) -> List[BaseStrategyProcessor]:
        """
        Get all processors that can handle the specified signal type.
        
        Args:
            signal_type: The type of signal to process (e.g., 'SHARP_ACTION', 'BOOK_CONFLICTS')
            
        Returns:
            List of processors capable of handling the signal type
        """
        matching_processors = []
        
        # 🚀 PERFORMANCE FIX: Add early logging to track processor requests
        self.logger.info(f"🔍 Requesting processors for signal type: {signal_type}")
        
        for strategy_name, processor in self._processor_registry.items():
            try:
                if self._processor_matches_signal_type(strategy_name, signal_type):
                    matching_processors.append(processor)
                    self.logger.debug(f"✅ Found matching processor: {strategy_name} for {signal_type}")
            except Exception as e:
                self.logger.warning(f"Error checking processor {strategy_name} for signal type {signal_type}: {e}")
                continue
        
        # 🚀 CONSOLIDATION FIX: Prevent duplicate processors for the same signal type
        unique_processors = []
        processor_classes_seen = set()
        
        for processor in matching_processors:
            processor_class = processor.__class__.__name__
            if processor_class not in processor_classes_seen:
                unique_processors.append(processor)
                processor_classes_seen.add(processor_class)
            else:
                self.logger.debug(f"🔄 Skipping duplicate processor: {processor_class}")
        
        self.logger.info(f"🎯 Found {len(unique_processors)} unique processors for {signal_type} (filtered from {len(matching_processors)} total matches)")
        
        if not unique_processors:
            self.logger.warning(f"⚠️  No processors found for signal type: {signal_type}")
            self.logger.info(f"📊 Available signal types: {list(set(self._processor_matches_signal_type(name, '') for name in self._processor_registry.keys()))}")
            
        return unique_processors
    
    def _processor_matches_signal_type(self, processor_name: str, signal_type: str) -> bool:
        """
        Fallback method to match processor names to signal types
        
        Args:
            processor_name: Name of the processor
            signal_type: Signal type to match
            
        Returns:
            True if processor likely handles this signal type
        """
        # Basic string matching for common patterns
        signal_type_lower = signal_type.lower()
        processor_name_lower = processor_name.lower()
        
        # Common mappings
        type_mappings = {
            'sharp_action': ['sharp', 'action'],
            'book_conflicts': ['book', 'conflict'],
            'public_fade': ['public', 'fade'],
            'late_flip': ['late', 'flip'],
            'consensus': ['consensus'],
            'underdog_value': ['underdog', 'value'],
            'line_movement': ['line', 'movement'],
            'opposing_markets': ['opposing', 'market']
        }
        
        # Check if any keywords from signal type appear in processor name
        for key, keywords in type_mappings.items():
            if key in signal_type_lower:
                return any(keyword in processor_name_lower for keyword in keywords)
        
        # Default: assume processor can handle if names are similar
        return signal_type_lower in processor_name_lower or processor_name_lower in signal_type_lower 