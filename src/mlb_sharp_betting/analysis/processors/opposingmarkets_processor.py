"""
Opposing Markets Processor

Detects games where moneyline and spread splits point to opposite teams.
This often indicates sharp vs public money creating potential value.

Converts opposing_markets_strategy_postgres.sql logic to Python processor.
"""

from datetime import datetime
from typing import Any

from ...models.betting_analysis import BettingSignal, ProfitableStrategy, SignalType
from .base_strategy_processor import BaseStrategyProcessor


class OpposingMarketsProcessor(BaseStrategyProcessor):
    """
    Processor for detecting opposing market signals

    Identifies games where moneyline recommendations conflict with spread recommendations,
    indicating potential sharp vs public money divergence and betting opportunities.
    """

    def get_signal_type(self) -> SignalType:
        """Return the signal type this processor handles"""
        return SignalType.OPPOSING_MARKETS

    def get_strategy_category(self) -> str:
        """Return strategy category for proper routing"""
        return "MARKET_CONFLICTS"

    def get_required_tables(self) -> list[str]:
        """Return database tables required for this strategy"""
        return ["splits.raw_mlb_betting_splits"]

    def get_strategy_description(self) -> str:
        """Return human-readable description of the strategy"""
        return "Detects games where moneyline and spread splits point to opposite teams, indicating sharp vs public money conflicts"

    def validate_strategy_data(self, raw_data: list[dict]) -> bool:
        """Validate we have both moneyline and spread data"""
        if not raw_data:
            return False

        # Check we have both moneyline and spread data
        split_types = set(row.get("split_type") for row in raw_data)
        return "moneyline" in split_types and "spread" in split_types

    async def process(
        self, minutes_ahead: int, profitable_strategies: list[ProfitableStrategy]
    ) -> list[BettingSignal]:
        """Process opposing markets signals using profitable strategies"""
        start_time, end_time = self._create_time_window(minutes_ahead)

        # ✅ FIX: Extract proper source_book and split_type from strategy names
        fixed_strategies = self._fix_strategy_components(profitable_strategies)

        # Get moneyline and spread data
        ml_data = await self.repository.get_moneyline_splits(start_time, end_time)
        spread_data = await self.repository.get_spread_splits(start_time, end_time)

        if not ml_data or not spread_data:
            self.logger.info("Insufficient data for opposing markets analysis")
            return []

        # Find opposing markets
        opposing_markets = self._find_opposing_markets(ml_data, spread_data)

        if not opposing_markets:
            self.logger.info("No opposing markets found")
            return []

        # Convert to signals
        signals = []
        now_est = datetime.now(self.est)

        for market_data in opposing_markets:
            # Apply basic filters
            if not self._is_valid_signal_data(market_data, now_est, minutes_ahead):
                continue

            # Check if opposing markets strength is significant enough
            opposition_strength = market_data.get("opposition_strength", 0)
            if opposition_strength < 10.0:  # Minimum 10% opposition strength
                continue

            # Apply juice filter if needed
            if self._should_apply_juice_filter(market_data):
                continue

            # Find matching profitable strategies
            matching_strategies = self._find_matching_strategies(
                fixed_strategies, market_data
            )
            if not matching_strategies:
                continue

            matching_strategy = matching_strategies[0]  # Use best matching strategy

            # Calculate confidence
            confidence_data = self._calculate_confidence_for_opposing_market(
                market_data, matching_strategy
            )

            # Create the signal
            signal = self._create_betting_signal(
                market_data, matching_strategy, confidence_data
            )
            signals.append(signal)

        self._log_processing_summary(
            len(signals), len(fixed_strategies), len(opposing_markets)
        )
        return signals

    def _find_opposing_markets(
        self, ml_data: list[dict], spread_data: list[dict]
    ) -> list[dict]:
        """
        Find games where ML and spread point to different teams
        Implements the core logic from opposing_markets_strategy_postgres.sql
        """
        opposing_markets = []

        # Group data by game/source/book for comparison
        ml_by_game = self._group_data_by_game(ml_data)
        spread_by_game = self._group_data_by_game(spread_data)

        # Find games that exist in both datasets
        common_games = set(ml_by_game.keys()) & set(spread_by_game.keys())

        for game_key in common_games:
            ml_record = ml_by_game[game_key]
            spread_record = spread_by_game[game_key]

            # Determine recommendations for each market
            ml_recommended_team = self._get_recommended_team_from_differential(
                ml_record["differential"],
                ml_record["home_team"],
                ml_record["away_team"],
            )

            spread_recommended_team = self._get_recommended_team_from_differential(
                spread_record["differential"],
                spread_record["home_team"],
                spread_record["away_team"],
            )

            # Check if markets oppose each other
            if ml_recommended_team != spread_recommended_team:
                # Calculate opposition metrics
                ml_strength = abs(ml_record["differential"])
                spread_strength = abs(spread_record["differential"])
                combined_strength = (ml_strength + spread_strength) / 2
                opposition_strength = abs(
                    ml_record["differential"] - spread_record["differential"]
                )

                # Determine dominant market
                dominant_market = (
                    "ML_STRONGER"
                    if ml_strength > spread_strength
                    else "SPREAD_STRONGER"
                )
                if ml_strength == spread_strength:
                    dominant_market = "EQUAL_STRENGTH"

                # Create opposing market record
                opposing_market = {
                    "game_id": ml_record["game_id"],
                    "home_team": ml_record["home_team"],
                    "away_team": ml_record["away_team"],
                    "game_datetime": ml_record["game_datetime"],
                    "source": ml_record["source"],
                    "book": ml_record.get("book", "UNKNOWN"),
                    "last_updated": max(
                        ml_record["last_updated"], spread_record["last_updated"]
                    ),
                    # Market data
                    "ml_recommended_team": ml_recommended_team,
                    "ml_differential": ml_record["differential"],
                    "ml_signal_strength": ml_strength,
                    "ml_split_value": ml_record.get("split_value"),
                    "spread_recommended_team": spread_recommended_team,
                    "spread_differential": spread_record["differential"],
                    "spread_signal_strength": spread_strength,
                    "spread_split_value": spread_record.get("split_value"),
                    # Opposition analysis
                    "market_relationship": "OPPOSING",
                    "combined_signal_strength": combined_strength,
                    "opposition_strength": opposition_strength,
                    "dominant_market": dominant_market,
                    # For signal creation
                    "split_type": "opposing_markets",  # Custom split type
                    "differential": opposition_strength,  # Use opposition strength as differential
                }

                opposing_markets.append(opposing_market)

        return opposing_markets

    def _group_data_by_game(self, data: list[dict]) -> dict[tuple, dict]:
        """Group betting data by game/source/book key"""
        grouped = {}

        for record in data:
            # Create key for grouping
            key = (record["game_id"], record["source"], record.get("book", "UNKNOWN"))

            # Take the most recent record for each game/source/book combination
            if (
                key not in grouped
                or record["last_updated"] > grouped[key]["last_updated"]
            ):
                grouped[key] = record

        return grouped

    def _get_recommended_team_from_differential(
        self, differential: float, home_team: str, away_team: str
    ) -> str:
        """Get recommended team based on differential sign"""
        return home_team if differential > 0 else away_team

    def _calculate_confidence_for_opposing_market(
        self, market_data: dict[str, Any], strategy: ProfitableStrategy
    ) -> dict[str, Any]:
        """Calculate confidence score for opposing market signal"""

        # Use the opposition strength as the primary differential
        opposition_strength = market_data["opposition_strength"]
        combined_strength = market_data["combined_signal_strength"]

        # Enhanced confidence calculation for opposing markets
        confidence_data = self._calculate_confidence(
            opposition_strength,  # Use opposition strength as differential
            market_data["source"],
            market_data.get("book"),
            market_data["split_type"],
            strategy.strategy_name,
            market_data["last_updated"],
            market_data["game_datetime"],
            # Additional context for opposing markets
            ml_strength=market_data["ml_signal_strength"],
            spread_strength=market_data["spread_signal_strength"],
            dominant_market=market_data["dominant_market"],
        )

        # Boost confidence if opposition is very strong
        if opposition_strength > 20.0:
            confidence_data["confidence_score"] = min(
                1.0, confidence_data["confidence_score"] * 1.1
            )
            confidence_data["confidence_explanation"] += (
                " Strong market opposition detected."
            )

        return confidence_data

    def _create_betting_signal(
        self,
        raw_data: dict[str, Any],
        strategy: ProfitableStrategy,
        confidence_data: dict[str, Any],
        metadata: dict[str, Any] = None,
    ) -> BettingSignal:
        """Create betting signal with opposing markets specific logic"""

        # Determine which market to follow based on strategy or strength
        dominant_market = raw_data["dominant_market"]

        if dominant_market == "ML_STRONGER":
            recommended_team = raw_data["ml_recommended_team"]
            split_value = raw_data.get("ml_split_value")
            recommendation = f"BET {recommended_team} (ML stronger vs spread)"
        elif dominant_market == "SPREAD_STRONGER":
            recommended_team = raw_data["spread_recommended_team"]
            split_value = raw_data.get("spread_split_value")
            recommendation = f"BET {recommended_team} (Spread stronger vs ML)"
        else:
            # Equal strength - default to ML
            recommended_team = raw_data["ml_recommended_team"]
            split_value = raw_data.get("ml_split_value")
            recommendation = f"BET {recommended_team} (ML default on equal opposition)"

        # Enhanced metadata for opposing markets
        enhanced_metadata = {
            "ml_recommended_team": raw_data["ml_recommended_team"],
            "spread_recommended_team": raw_data["spread_recommended_team"],
            "ml_differential": raw_data["ml_differential"],
            "spread_differential": raw_data["spread_differential"],
            "opposition_strength": raw_data["opposition_strength"],
            "dominant_market": raw_data["dominant_market"],
            "market_relationship": raw_data["market_relationship"],
            "combined_signal_strength": raw_data["combined_signal_strength"],
        }

        if metadata:
            enhanced_metadata.update(metadata)

        # Create base signal
        game_time = self._normalize_game_time(raw_data["game_datetime"])
        now_est = datetime.now(self.est)

        return BettingSignal(
            signal_type=self.get_signal_type(),
            home_team=raw_data["home_team"],
            away_team=raw_data["away_team"],
            game_time=game_time,
            minutes_to_game=self._calculate_minutes_to_game(game_time, now_est),
            split_type=raw_data["split_type"],
            split_value=split_value,
            source=raw_data["source"],
            book=raw_data.get("book"),
            differential=float(
                raw_data["opposition_strength"]
            ),  # Use opposition strength
            signal_strength=float(raw_data["combined_signal_strength"]),
            confidence_score=confidence_data["confidence_score"],
            confidence_level=confidence_data["confidence_level"],
            confidence_explanation=confidence_data["confidence_explanation"],
            recommendation=recommendation,
            recommendation_strength=confidence_data["recommendation_strength"],
            last_updated=raw_data["last_updated"],
            strategy_name=strategy.strategy_name,
            win_rate=strategy.win_rate,
            roi=strategy.roi,
            total_bets=strategy.total_bets,
            metadata=enhanced_metadata,
        )

    def _fix_strategy_components(
        self, profitable_strategies: list[ProfitableStrategy]
    ) -> list[ProfitableStrategy]:
        """
        Fix ProfitableStrategy objects by extracting real source_book and split_type from strategy names.

        This solves the strategy matching issue where strategies have synthetic values like:
        - source_book="ORCHESTRATOR" (wrong)
        - split_type="DYNAMIC" (wrong)

        Instead of real extracted values from strategy names.
        """
        fixed_strategies = []

        for strategy in profitable_strategies:
            # Extract real values from strategy name
            source_book, split_type = self._extract_strategy_components(
                strategy.strategy_name
            )

            # Create new strategy with fixed values
            fixed_strategy = ProfitableStrategy(
                strategy_name=strategy.strategy_name,
                source_book=source_book,  # ✅ FIXED: Use extracted source_book
                split_type=split_type,  # ✅ FIXED: Use extracted split_type
                win_rate=strategy.win_rate,
                roi=strategy.roi,
                total_bets=strategy.total_bets,
                confidence=strategy.confidence,
                ci_lower=getattr(strategy, "ci_lower", 0.0),
                ci_upper=getattr(strategy, "ci_upper", 100.0),
                confidence_score=getattr(strategy, "confidence_score", 0.5),
            )
            fixed_strategies.append(fixed_strategy)

        return fixed_strategies

    def _extract_strategy_components(self, strategy_name: str) -> tuple[str, str]:
        """
        Extract source_book and split_type from strategy name.

        Examples:
        - "VSIN-circa-moneyline" -> ("VSIN-circa", "moneyline")
        - "VSIN-draftkings-total" -> ("VSIN-draftkings", "total")
        - "SBD-unknown-spread" -> ("SBD-unknown", "spread")
        - "opposing_markets_strategy_ml_preference" -> ("VSIN-unknown", "moneyline")
        """
        strategy_name_lower = strategy_name.lower()

        # Handle direct format: "SOURCE-BOOK-SPLITTYPE"
        if strategy_name.count("-") >= 2 and len(strategy_name.split("-")) == 3:
            parts = strategy_name.split("-")
            source_book = f"{parts[0]}-{parts[1]}"
            split_type = parts[2]
            return source_book, split_type

        # Handle opposing markets strategy format
        source_book = "VSIN-unknown"  # Default for opposing markets
        split_type = "opposing_markets"  # Default

        # Extract book information
        if (
            "vsin-dra" in strategy_name_lower
            or "vsin-draftkings" in strategy_name_lower
        ):
            source_book = "VSIN-draftkings"
        elif "vsin-cir" in strategy_name_lower or "vsin-circa" in strategy_name_lower:
            source_book = "VSIN-circa"
        elif "sbd" in strategy_name_lower:
            source_book = "SBD-unknown"
        elif "vsin" in strategy_name_lower:
            source_book = "VSIN-unknown"

        # Extract split type information for opposing markets
        if (
            "moneyline" in strategy_name_lower
            or "_ml_" in strategy_name_lower
            or "mone" in strategy_name_lower
        ):
            split_type = "moneyline"
        elif (
            "spread" in strategy_name_lower
            or "_sprd_" in strategy_name_lower
            or "spre" in strategy_name_lower
        ):
            split_type = "spread"
        elif (
            "total" in strategy_name_lower
            or "_tot_" in strategy_name_lower
            or "tota" in strategy_name_lower
        ):
            split_type = "total"
        elif "opposing" in strategy_name_lower or "market" in strategy_name_lower:
            split_type = "opposing_markets"

        return source_book, split_type
