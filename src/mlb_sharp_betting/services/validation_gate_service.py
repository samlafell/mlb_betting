"""
Validation Gate Service - Production Integration Layer

Integrates the Strategy Validation Registry with existing architecture components
to enforce validation-first approach throughout the MLB betting system.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

from ..core.logging import get_logger


class ValidationGateResult(Enum):
    """Results of validation gate checks"""

    ALLOWED = "allowed"
    BLOCKED_UNVALIDATED = "blocked_unvalidated"
    BLOCKED_CIRCUIT_BREAKER = "blocked_circuit_breaker"
    BLOCKED_EMERGENCY = "blocked_emergency"
    BLOCKED_MODIFIED = "blocked_modified"
    BLOCKED_REGISTRY_UNHEALTHY = "blocked_registry_unhealthy"


@dataclass
class ValidationGateResponse:
    """Response from validation gate check"""

    result: ValidationGateResult
    strategy_name: str
    allowed: bool
    reason: str
    max_daily_recommendations: int = 0
    max_bet_size_multiplier: float = 0.0
    requires_manual_approval: bool = False
    requires_validation: bool = False
    can_recover: bool = False


class ValidationGateService:
    """Central validation gate that enforces strategy validation across all components."""

    def __init__(self):
        self.logger = get_logger(__name__)
        self.registry = None
        self._gate_checks_today: dict[str, int] = {}
        self._last_reset = datetime.now(timezone.utc).date()

    async def initialize(self):
        """Initialize validation gate service"""
        try:
            from .strategy_validation_registry import get_validation_registry

            self.registry = await get_validation_registry()
            self.logger.info("✅ Validation Gate Service initialized")
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize validation gate: {e}")
            raise

    async def check_strategy_gate(
        self, strategy_name: str, context: str = "unknown"
    ) -> ValidationGateResponse:
        """Main validation gate - checks if strategy can generate recommendations"""
        if not self.registry:
            await self.initialize()

        self._reset_daily_counters_if_needed()

        try:
            # Get validation decision from registry
            validation_result = self.registry.can_generate_recommendations(
                strategy_name
            )

            if validation_result["allowed"]:
                # Check daily limits
                daily_count = self._gate_checks_today.get(strategy_name, 0)
                max_daily = validation_result.get("max_daily_recommendations", 10)

                if daily_count >= max_daily:
                    return ValidationGateResponse(
                        result=ValidationGateResult.BLOCKED_REGISTRY_UNHEALTHY,
                        strategy_name=strategy_name,
                        allowed=False,
                        reason=f"Daily limit reached ({daily_count}/{max_daily})",
                    )

                # Increment counter and allow
                self._gate_checks_today[strategy_name] = daily_count + 1

                return ValidationGateResponse(
                    result=ValidationGateResult.ALLOWED,
                    strategy_name=strategy_name,
                    allowed=True,
                    reason="Strategy validated and approved",
                    max_daily_recommendations=max_daily,
                    max_bet_size_multiplier=validation_result.get(
                        "max_bet_size_multiplier", 1.0
                    ),
                    requires_manual_approval=validation_result.get(
                        "requires_manual_approval", False
                    ),
                )

            else:
                # Determine blocking reason
                reason = validation_result["reason"]

                if "kill switch" in reason.lower() or "emergency" in reason.lower():
                    result = ValidationGateResult.BLOCKED_EMERGENCY
                elif "circuit breaker" in reason.lower():
                    result = ValidationGateResult.BLOCKED_CIRCUIT_BREAKER
                elif "modified" in reason.lower():
                    result = ValidationGateResult.BLOCKED_MODIFIED
                elif "registry" in reason.lower():
                    result = ValidationGateResult.BLOCKED_REGISTRY_UNHEALTHY
                else:
                    result = ValidationGateResult.BLOCKED_UNVALIDATED

                return ValidationGateResponse(
                    result=result,
                    strategy_name=strategy_name,
                    allowed=False,
                    reason=reason,
                    requires_validation=validation_result.get(
                        "requires_validation", False
                    ),
                    can_recover=validation_result.get("can_recover", False),
                )

        except Exception as e:
            self.logger.error(f"Validation gate check failed for {strategy_name}: {e}")

            # Fail safe - block by default
            return ValidationGateResponse(
                result=ValidationGateResult.BLOCKED_REGISTRY_UNHEALTHY,
                strategy_name=strategy_name,
                allowed=False,
                reason=f"Validation gate error: {str(e)}",
            )

    def _reset_daily_counters_if_needed(self):
        """Reset daily counters if new day"""
        today = datetime.now(timezone.utc).date()
        if today != self._last_reset:
            self._gate_checks_today.clear()
            self._last_reset = today
            self.logger.debug("Daily validation gate counters reset")


# Singleton pattern
_gate_service_instance: ValidationGateService | None = None


async def get_validation_gate() -> ValidationGateService:
    """Get singleton instance of Validation Gate Service"""
    global _gate_service_instance
    if _gate_service_instance is None:
        _gate_service_instance = ValidationGateService()
        await _gate_service_instance.initialize()
    return _gate_service_instance
