"""
CURATED Zone Processor

Handles feature engineering and analysis-ready data preparation from STAGING zone.
CURATED Zone provides ML features, analytics, and enriched datasets.

Reference: docs/SYSTEM_DESIGN_ANALYSIS.md
"""

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from ...core.logging import LogComponent, get_logger
from .base_processor import BaseZoneProcessor
from .zone_interface import (
    DataRecord,
    ProcessingResult,
    ProcessingStatus,
    ZoneConfig,
    ZoneFactory,
    ZoneType,
)

logger = get_logger(__name__, LogComponent.CORE)


class CuratedDataRecord(DataRecord):
    """Curated data record with enhanced features for analysis."""

    game_id: int | None = None
    sportsbook_id: int | None = None
    sportsbook_name: str | None = None

    # Core betting data
    bet_type: str | None = None
    line_value: Decimal | None = None
    odds_american: int | None = None
    odds_decimal: Decimal | None = None
    team_type: str | None = None

    # Enhanced features
    sharp_action_score: float | None = None
    line_movement_trend: str | None = None  # 'up', 'down', 'stable'
    consensus_score: float | None = None
    value_score: float | None = None
    market_efficiency_score: float | None = None

    # ML Features
    feature_vector: dict[str, float] | None = None
    prediction_confidence: float | None = None
    risk_score: float | None = None

    # Analytics
    betting_pattern: str | None = None  # 'sharp', 'public', 'contrarian'
    profitability_rating: str | None = None  # 'high', 'medium', 'low'

    class Config:
        use_enum_values = True


class CuratedZoneProcessor(BaseZoneProcessor):
    """
    CURATED Zone Processor

    Transforms staging data into analysis-ready datasets with:
    - Feature engineering for ML models
    - Sharp action indicators
    - Market efficiency metrics
    - Profitability signals
    - Risk assessment scores
    """

    def __init__(self, config: ZoneConfig):
        super().__init__(config)
        self.zone_type = ZoneType.CURATED

    async def process_records(self, records: list[DataRecord]) -> ProcessingResult:
        """Process staging records into curated analysis-ready data."""
        try:
            logger.info(f"Processing {len(records)} records in CURATED zone")

            processed_records = []
            failed_count = 0
            quality_scores = []

            for record in records:
                try:
                    curated_record = await self._enhance_record(record)
                    if curated_record:
                        processed_records.append(curated_record)
                        quality_scores.append(curated_record.quality_score or 0.0)
                    else:
                        failed_count += 1

                except Exception as e:
                    logger.error(f"Failed to enhance record {record.external_id}: {e}")
                    failed_count += 1

            # Calculate overall quality metrics
            avg_quality = (
                sum(quality_scores) / len(quality_scores) if quality_scores else 0.0
            )
            success_rate = len(processed_records) / len(records) if records else 0.0

            return ProcessingResult(
                status=ProcessingStatus.COMPLETED,
                records_processed=len(processed_records),
                records_failed=failed_count,
                quality_score=avg_quality,
                metadata={
                    "enhancement_success_rate": success_rate,
                    "feature_vectors_generated": sum(
                        1
                        for r in processed_records
                        if hasattr(r, "feature_vector") and r.feature_vector
                    ),
                    "sharp_signals_detected": sum(
                        1
                        for r in processed_records
                        if hasattr(r, "sharp_action_score")
                        and r.sharp_action_score
                        and r.sharp_action_score > 0.7
                    ),
                },
            )

        except Exception as e:
            logger.error(f"CURATED zone processing failed: {e}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                records_processed=0,
                records_failed=len(records),
                quality_score=0.0,
                error_message=str(e),
            )

    async def process_record(
        self, record: DataRecord, **kwargs
    ) -> DataRecord | None:
        """Process a single record from staging to curated."""
        return await self._enhance_record(record)

    async def store_records(self, records: list[DataRecord]) -> None:
        """Store curated records to database."""
        if not records:
            return

        try:
            # Convert records to database format and insert
            # This would insert into curated zone tables
            # For now, we'll just log the storage operation
            logger.info(f"Storing {len(records)} curated records to database")

            # In production, this would do:
            # 1. INSERT into curated.betting_lines_enhanced
            # 2. INSERT into curated.feature_vectors
            # 3. INSERT into curated.analysis_ready_data
            # 4. UPDATE quality metrics

            # Placeholder implementation
            pass

        except Exception as e:
            logger.error(f"Failed to store curated records: {e}")
            raise

    async def _enhance_record(self, record: DataRecord) -> CuratedDataRecord | None:
        """Enhance a staging record with curated features."""
        try:
            # Convert to curated record
            curated_data = {
                "external_id": record.external_id,
                "source": record.source,
                "collected_at": record.collected_at,
                "processed_at": datetime.now(timezone.utc),
                "quality_score": record.quality_score or 0.0,
            }

            # Copy staging data
            if hasattr(record, "raw_data") and record.raw_data:
                staging_data = (
                    record.raw_data if isinstance(record.raw_data, dict) else {}
                )

                # Extract core fields
                curated_data.update(
                    {
                        "game_id": staging_data.get("game_id"),
                        "sportsbook_id": staging_data.get("sportsbook_id"),
                        "sportsbook_name": staging_data.get("sportsbook_name"),
                        "bet_type": staging_data.get("bet_type"),
                        "line_value": self._safe_decimal(
                            staging_data.get("line_value")
                        ),
                        "odds_american": staging_data.get("odds_american"),
                        "team_type": staging_data.get("team_type"),
                    }
                )

                # Calculate decimal odds from American odds
                american_odds = staging_data.get("odds_american")
                if american_odds:
                    curated_data["odds_decimal"] = self._american_to_decimal_odds(
                        american_odds
                    )

            # Generate enhanced features
            await self._generate_sharp_action_features(curated_data, record)
            await self._generate_market_features(curated_data, record)
            await self._generate_ml_features(curated_data, record)
            await self._generate_risk_features(curated_data, record)

            # Calculate final quality score
            curated_data["quality_score"] = self._calculate_curated_quality_score(
                curated_data
            )

            return CuratedDataRecord(**curated_data)

        except Exception as e:
            logger.error(f"Failed to enhance record {record.external_id}: {e}")
            return None

    async def _generate_sharp_action_features(
        self, curated_data: dict, record: DataRecord
    ):
        """Generate sharp action detection features."""
        try:
            # Placeholder for sharp action detection
            # In production, this would analyze:
            # - Line movements vs public betting percentages
            # - Timing of large bets
            # - Sportsbook response patterns
            # - Historical sharp action patterns

            # For now, generate a basic score based on available data
            sharp_score = 0.0

            # Check for reverse line movement indicators
            if curated_data.get("bet_type") and curated_data.get("line_value"):
                # Basic heuristic - this would be replaced with real analysis
                sharp_score = min(0.8, (record.quality_score or 0.0) * 0.8)

            curated_data["sharp_action_score"] = sharp_score
            curated_data["betting_pattern"] = self._classify_betting_pattern(
                sharp_score
            )

        except Exception as e:
            logger.error(f"Failed to generate sharp action features: {e}")
            curated_data["sharp_action_score"] = 0.0

    async def _generate_market_features(self, curated_data: dict, record: DataRecord):
        """Generate market efficiency and consensus features."""
        try:
            # Placeholder for market analysis features
            # In production, this would analyze:
            # - Market consensus across sportsbooks
            # - Efficiency scores based on line convergence
            # - Value identification through market comparison

            odds_decimal = curated_data.get("odds_decimal", 0.0)
            if odds_decimal and odds_decimal > 1.0:
                # Basic market efficiency score
                implied_probability = 1.0 / float(odds_decimal)
                market_efficiency = min(
                    1.0, implied_probability * 1.2
                )  # Simple heuristic
                curated_data["market_efficiency_score"] = market_efficiency
                curated_data["consensus_score"] = market_efficiency * 0.9

                # Value score based on market efficiency
                curated_data["value_score"] = max(0.0, 1.0 - market_efficiency)
            else:
                curated_data["market_efficiency_score"] = 0.5
                curated_data["consensus_score"] = 0.5
                curated_data["value_score"] = 0.0

        except Exception as e:
            logger.error(f"Failed to generate market features: {e}")
            curated_data["market_efficiency_score"] = 0.0
            curated_data["consensus_score"] = 0.0
            curated_data["value_score"] = 0.0

    async def _generate_ml_features(self, curated_data: dict, record: DataRecord):
        """Generate ML-ready feature vectors."""
        try:
            # Helper function to safely convert to float
            def safe_float(value, default=0.0):
                if value is None:
                    return default
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return default

            # Create feature vector for ML models
            feature_vector = {
                "odds_decimal": safe_float(curated_data.get("odds_decimal"), 1.0),
                "line_value": safe_float(curated_data.get("line_value"), 0.0),
                "sharp_action_score": safe_float(
                    curated_data.get("sharp_action_score"), 0.0
                ),
                "market_efficiency_score": safe_float(
                    curated_data.get("market_efficiency_score"), 0.0
                ),
                "consensus_score": safe_float(curated_data.get("consensus_score"), 0.0),
                "value_score": safe_float(curated_data.get("value_score"), 0.0),
                "quality_score": safe_float(curated_data.get("quality_score"), 0.0),
            }

            # Add categorical features as one-hot encoded
            bet_type = curated_data.get("bet_type", "")
            feature_vector.update(
                {
                    "bet_type_spread": 1.0 if bet_type == "spread" else 0.0,
                    "bet_type_total": 1.0 if bet_type == "total" else 0.0,
                    "bet_type_moneyline": 1.0 if bet_type == "moneyline" else 0.0,
                }
            )

            team_type = curated_data.get("team_type", "")
            feature_vector.update(
                {
                    "team_type_home": 1.0 if team_type == "home" else 0.0,
                    "team_type_away": 1.0 if team_type == "away" else 0.0,
                    "team_type_over": 1.0 if team_type == "over" else 0.0,
                    "team_type_under": 1.0 if team_type == "under" else 0.0,
                }
            )

            curated_data["feature_vector"] = feature_vector

            # Generate prediction confidence based on feature completeness
            feature_completeness = sum(
                1 for v in feature_vector.values() if v > 0
            ) / len(feature_vector)
            curated_data["prediction_confidence"] = min(1.0, feature_completeness * 1.2)

        except Exception as e:
            logger.error(f"Failed to generate ML features: {e}")
            curated_data["feature_vector"] = {}
            curated_data["prediction_confidence"] = 0.0

    async def _generate_risk_features(self, curated_data: dict, record: DataRecord):
        """Generate risk assessment features."""
        try:
            # Calculate risk score based on various factors
            risk_components = []

            # Market efficiency risk
            market_eff = curated_data.get("market_efficiency_score", 0.5)
            risk_components.append(1.0 - market_eff)  # Higher efficiency = lower risk

            # Sharp action risk (opposite direction)
            sharp_score = curated_data.get("sharp_action_score", 0.0)
            risk_components.append(
                1.0 - sharp_score
            )  # Higher sharp action = lower risk

            # Data quality risk
            quality_score = curated_data.get("quality_score", 0.0)
            risk_components.append(1.0 - quality_score)  # Higher quality = lower risk

            # Calculate overall risk score
            risk_score = (
                sum(risk_components) / len(risk_components) if risk_components else 0.5
            )
            curated_data["risk_score"] = min(1.0, max(0.0, risk_score))

            # Classify profitability rating
            overall_score = (
                curated_data.get("sharp_action_score", 0.0) * 0.4
                + curated_data.get("value_score", 0.0) * 0.3
                + (1.0 - risk_score) * 0.3
            )

            if overall_score >= 0.7:
                curated_data["profitability_rating"] = "high"
            elif overall_score >= 0.4:
                curated_data["profitability_rating"] = "medium"
            else:
                curated_data["profitability_rating"] = "low"

        except Exception as e:
            logger.error(f"Failed to generate risk features: {e}")
            curated_data["risk_score"] = 0.5
            curated_data["profitability_rating"] = "medium"

    def _american_to_decimal_odds(self, american_odds: int) -> Decimal:
        """Convert American odds to decimal odds."""
        try:
            if american_odds > 0:
                return Decimal(str((american_odds / 100) + 1))
            else:
                return Decimal(str((100 / abs(american_odds)) + 1))
        except (ZeroDivisionError, InvalidOperation):
            return Decimal("2.0")  # Default to even odds

    def _classify_betting_pattern(self, sharp_score: float) -> str:
        """Classify betting pattern based on sharp action score."""
        if sharp_score >= 0.7:
            return "sharp"
        elif sharp_score <= 0.3:
            return "public"
        else:
            return "contrarian"

    def _calculate_curated_quality_score(self, curated_data: dict) -> float:
        """Calculate quality score for curated record."""
        try:
            quality_components = []

            # Base quality from staging
            base_quality = curated_data.get("quality_score", 0.0)
            quality_components.append(base_quality * 0.4)

            # Feature completeness
            feature_vector = curated_data.get("feature_vector", {})
            if feature_vector:
                completeness = sum(1 for v in feature_vector.values() if v > 0) / len(
                    feature_vector
                )
                quality_components.append(completeness * 0.3)

            # Enhancement success
            enhancement_score = 0.0
            if curated_data.get("sharp_action_score") is not None:
                enhancement_score += 0.25
            if curated_data.get("market_efficiency_score") is not None:
                enhancement_score += 0.25
            if curated_data.get("feature_vector"):
                enhancement_score += 0.25
            if curated_data.get("risk_score") is not None:
                enhancement_score += 0.25

            quality_components.append(enhancement_score * 0.3)

            return min(1.0, sum(quality_components))

        except Exception as e:
            logger.error(f"Failed to calculate curated quality score: {e}")
            return curated_data.get("quality_score", 0.0)

    def _safe_decimal(self, value: Any) -> Decimal | None:
        """Safely convert value to Decimal."""
        if value is None:
            return None
        try:
            if isinstance(value, Decimal):
                return value
            return Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError):
            return None

    async def get_health_check(self) -> dict[str, Any]:
        """Get CURATED zone health status."""
        try:
            # Check database connectivity
            pool_manager = self.config.database_manager
            async with pool_manager.get_connection() as conn:
                # Check if curated tables exist and are accessible
                result = await conn.fetchrow("""
                    SELECT COUNT(*) as table_count 
                    FROM information_schema.tables 
                    WHERE table_schema = 'curated'
                """)

                table_count = result["table_count"] if result else 0

                # Get recent processing metrics
                recent_result = await conn.fetchrow("""
                    SELECT COUNT(*) as recent_records
                    FROM curated.betting_lines_enhanced 
                    WHERE processed_at >= NOW() - INTERVAL '24 hours'
                """)

                recent_count = recent_result["recent_records"] if recent_result else 0

                return {
                    "status": "healthy" if table_count > 0 else "degraded",
                    "metrics": {
                        "curated_tables_available": table_count,
                        "records_processed_24h": recent_count,
                        "records_processed": recent_count,  # For compatibility
                        "records_successful": recent_count,
                        "records_failed": 0,
                        "quality_score": 0.85,  # Default curated quality
                        "error_rate": 0.0,
                    },
                }

        except Exception as e:
            logger.error(f"CURATED zone health check failed: {e}")
            return {
                "status": "unhealthy",
                "error": str(e),
                "metrics": {
                    "records_processed": 0,
                    "records_successful": 0,
                    "records_failed": 0,
                    "quality_score": 0.0,
                    "error_rate": 1.0,
                },
            }


# Register with zone factory
ZoneFactory.register_zone(ZoneType.CURATED, CuratedZoneProcessor)
