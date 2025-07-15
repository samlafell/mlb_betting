"""
Line Movement Strategy Processor

Detects significant line movements that indicate sharp action or steam moves.
Focuses on identifying when lines move against public betting patterns,
suggesting informed money is driving the movement.

Key Strategies:
- REVERSE_LINE_MOVEMENT: Line moves opposite to public betting
- STEAM_MOVE: Rapid line movement across multiple books
- LATE_MOVEMENT: Significant movement close to game time
- SHARP_MOVEMENT: Movement with low bet count but high money

Enhanced with timing analysis and book consensus tracking.
"""

from datetime import datetime
from typing import Any

from ...models.betting_analysis import BettingSignal, ProfitableStrategy, SignalType
from .base_strategy_processor import BaseStrategyProcessor


class LineMovementProcessor(BaseStrategyProcessor):
    """
    Processor for line movement strategy detection.

    Identifies significant line movements that suggest sharp action
    or steam moves, particularly when movement contradicts public betting.
    """

    def get_signal_type(self) -> SignalType:
        """Return the signal type this processor handles"""
        return SignalType.LINE_MOVEMENT

    def get_strategy_category(self) -> str:
        """Return strategy category for proper routing"""
        return "MOVEMENT_ANALYSIS"

    def get_required_tables(self) -> list[str]:
        """Return database tables required for this strategy"""
        return ["splits.raw_mlb_betting_splits"]

    def get_strategy_description(self) -> str:
        """Return human-readable description of the strategy"""
        return (
            "Line movement analysis: Detect sharp action through line movement patterns"
        )

    async def process(
        self, minutes_ahead: int, profitable_strategies: list[ProfitableStrategy]
    ) -> list[BettingSignal]:
        """Process line movement signals"""
        start_time, end_time = self._create_time_window(minutes_ahead)

        # ✅ FIX: Extract proper source_book and split_type from strategy names
        fixed_strategies = self._fix_strategy_components(profitable_strategies)

        # Get line movement strategies
        movement_strategies = self._get_movement_strategies(fixed_strategies)

        if not movement_strategies:
            self.logger.warning("No profitable line movement strategies found")
            return []

        # Get line movement data
        raw_data = await self.repository.get_line_movement_data(start_time, end_time)

        if not raw_data:
            self.logger.info("No line movement data found for analysis")
            return []

        signals = []
        now_est = datetime.now(self.est)

        # Analyze movement patterns by game
        games_data = self._group_by_game(raw_data)

        for game_key, game_movements in games_data.items():
            # Analyze line movements for this game
            movement_analysis = self._analyze_line_movements(game_movements)

            if not movement_analysis:
                continue

            for movement in movement_analysis:
                # Basic validation
                if not self._is_valid_movement_data(movement, now_est, minutes_ahead):
                    continue

                # Find matching profitable strategy
                matching_strategy = self._find_movement_strategy(
                    movement, movement_strategies
                )

                if not matching_strategy:
                    continue

                # Apply juice filter if applicable
                if self._should_apply_juice_filter(movement):
                    continue

                # Calculate confidence with movement-specific adjustments
                confidence_data = self._calculate_movement_confidence(
                    movement, matching_strategy
                )

                # Create the line movement signal
                signal = self._create_movement_signal(
                    movement, matching_strategy, confidence_data
                )
                signals.append(signal)

        self._log_movement_summary(signals, movement_strategies, len(raw_data))
        return signals

    def _get_movement_strategies(
        self, profitable_strategies: list[ProfitableStrategy]
    ) -> list[ProfitableStrategy]:
        """Extract line movement strategies"""
        movement_strategies = []

        for strategy in profitable_strategies:
            strategy_name = strategy.strategy_name.lower()
            if any(
                keyword in strategy_name
                for keyword in [
                    "line_movement",
                    "movement",
                    "steam",
                    "reverse",
                    "late_movement",
                ]
            ):
                movement_strategies.append(strategy)

        self.logger.info(f"Found {len(movement_strategies)} line movement strategies")
        return movement_strategies

    def _group_by_game(
        self, raw_data: list[dict[str, Any]]
    ) -> dict[str, list[dict[str, Any]]]:
        """Group raw data by game for movement analysis"""
        games_data = {}

        for row in raw_data:
            game_key = f"{row.get('home_team', 'UNK')}_{row.get('away_team', 'UNK')}_{row.get('split_type', 'UNK')}"

            if game_key not in games_data:
                games_data[game_key] = []

            games_data[game_key].append(row)

        return games_data

    def _analyze_line_movements(
        self, game_movements: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """
        Analyze line movements for a specific game.

        Returns list of significant movement patterns found.
        """
        if len(game_movements) < 2:
            return []  # Need at least 2 data points for movement

        # Sort by timestamp
        sorted_movements = sorted(
            game_movements, key=lambda x: x.get("last_updated", datetime.min)
        )

        movements = []

        # Analyze movements between consecutive data points
        for i in range(1, len(sorted_movements)):
            current = sorted_movements[i]
            previous = sorted_movements[i - 1]

            movement_analysis = self._detect_movement_pattern(previous, current)

            if movement_analysis:
                # Add game context
                movement_analysis.update(
                    {
                        "home_team": current.get("home_team"),
                        "away_team": current.get("away_team"),
                        "game_datetime": current.get("game_datetime"),
                        "split_type": current.get("split_type"),
                        "source": current.get("source"),
                        "book": current.get("book"),
                        "last_updated": current.get("last_updated"),
                        "current_data": current,
                        "previous_data": previous,
                    }
                )
                movements.append(movement_analysis)

        return movements

    def _detect_movement_pattern(
        self, previous: dict[str, Any], current: dict[str, Any]
    ) -> dict[str, Any]:
        """Detect specific movement patterns between two data points"""
        try:
            # Get percentage changes
            prev_money = float(previous.get("home_or_over_stake_percentage", 50))
            curr_money = float(current.get("home_or_over_stake_percentage", 50))
            prev_bets = float(previous.get("home_or_over_bets_percentage", 50))
            curr_bets = float(current.get("home_or_over_bets_percentage", 50))

            # Calculate movements
            money_movement = curr_money - prev_money
            bet_movement = curr_bets - prev_bets

            # Check for significant movements (threshold: 3% change)
            significant_money_move = abs(money_movement) >= 3.0
            significant_bet_move = abs(bet_movement) >= 3.0

            if not (significant_money_move or significant_bet_move):
                return None

            # Detect reverse line movement (money moves opposite to bets)
            reverse_movement = False
            if significant_money_move and significant_bet_move:
                # Money and bets moving in opposite directions
                if (money_movement > 0 and bet_movement < 0) or (
                    money_movement < 0 and bet_movement > 0
                ):
                    reverse_movement = True

            # Detect steam move (both money and bets moving same direction with large differential)
            steam_move = False
            if significant_money_move and abs(money_movement) >= 5.0:
                # Large money movement suggests steam
                steam_move = True

            # Detect sharp movement (money moves significantly without proportional bet movement)
            sharp_movement = False
            if significant_money_move and not significant_bet_move:
                sharp_movement = True

            # Determine movement type
            movement_type = None
            if reverse_movement:
                movement_type = "REVERSE_LINE_MOVEMENT"
            elif steam_move:
                movement_type = "STEAM_MOVE"
            elif sharp_movement:
                movement_type = "SHARP_MOVEMENT"
            elif significant_money_move or significant_bet_move:
                movement_type = "GENERAL_MOVEMENT"
            else:
                return None

            # Check timing (late movement is more significant)
            time_to_game = self._calculate_time_to_game(current.get("game_datetime"))
            late_movement = time_to_game <= 2.0  # Within 2 hours

            return {
                "movement_type": movement_type,
                "money_movement": money_movement,
                "bet_movement": bet_movement,
                "movement_magnitude": max(abs(money_movement), abs(bet_movement)),
                "reverse_movement": reverse_movement,
                "steam_move": steam_move,
                "sharp_movement": sharp_movement,
                "late_movement": late_movement,
                "time_to_game_hours": time_to_game,
                "movement_direction": "HOME" if money_movement > 0 else "AWAY",
                "current_money_pct": curr_money,
                "current_bet_pct": curr_bets,
                "differential": curr_money - curr_bets,
            }

        except (ValueError, TypeError, KeyError) as e:
            self.logger.warning(f"Error detecting movement pattern: {e}")
            return None

    def _calculate_time_to_game(self, game_datetime) -> float:
        """Calculate hours until game time"""
        try:
            if isinstance(game_datetime, str):
                game_time = datetime.fromisoformat(game_datetime.replace("Z", "+00:00"))
            else:
                game_time = game_datetime

            now = datetime.now(game_time.tzinfo) if game_time.tzinfo else datetime.now()
            delta = game_time - now
            return delta.total_seconds() / 3600.0  # Convert to hours

        except Exception:
            return 24.0  # Default to 24 hours if calculation fails

    def _find_movement_strategy(
        self, movement: dict[str, Any], movement_strategies: list[ProfitableStrategy]
    ) -> ProfitableStrategy:
        """Find matching line movement strategy"""
        movement_type = movement["movement_type"]
        movement_magnitude = movement["movement_magnitude"]
        late_movement = movement["late_movement"]

        # Look for specific movement strategy matches
        for strategy in movement_strategies:
            strategy_name = strategy.strategy_name.lower()

            # Match movement type
            if "reverse" in movement_type.lower() and "reverse" in strategy_name:
                if self._meets_movement_threshold(strategy, movement_magnitude):
                    return strategy
            elif "steam" in movement_type.lower() and "steam" in strategy_name:
                if self._meets_movement_threshold(strategy, movement_magnitude):
                    return strategy
            elif "sharp" in movement_type.lower() and "sharp" in strategy_name:
                if self._meets_movement_threshold(strategy, movement_magnitude):
                    return strategy
            elif "late" in strategy_name and late_movement:
                if self._meets_movement_threshold(strategy, movement_magnitude):
                    return strategy

            # General movement matches
            elif "movement" in strategy_name:
                if self._meets_movement_threshold(strategy, movement_magnitude):
                    # Bonus for late movement
                    if late_movement and strategy.win_rate >= 55:
                        return strategy
                    elif not late_movement and strategy.win_rate >= 60:
                        return strategy

        return None

    def _meets_movement_threshold(
        self, strategy: ProfitableStrategy, movement_magnitude: float
    ) -> bool:
        """Check if movement meets strategy thresholds"""
        # Dynamic thresholds based on strategy performance
        if strategy.win_rate >= 65:
            threshold = 3.0  # Accept smaller movements for high performers
        elif strategy.win_rate >= 60:
            threshold = 4.0  # Moderate threshold
        elif strategy.win_rate >= 55:
            threshold = 5.0  # Conservative threshold
        else:
            threshold = 6.0  # Very conservative

        return movement_magnitude >= threshold

    def _calculate_movement_confidence(
        self, movement: dict[str, Any], matching_strategy: ProfitableStrategy
    ) -> dict[str, Any]:
        """Calculate confidence with movement-specific adjustments"""
        base_confidence = self._calculate_confidence(
            movement["differential"],
            movement.get("source", "unknown"),
            movement.get("book", "unknown"),
            movement.get("split_type", "unknown"),
            matching_strategy.strategy_name,
            movement.get("last_updated"),
            self._normalize_game_time(movement["game_datetime"]),
        )

        # Apply movement-specific modifiers
        movement_modifier = self._get_movement_confidence_modifier(movement)

        # Adjust confidence based on movement characteristics
        adjusted_confidence = base_confidence["confidence_score"] * movement_modifier
        adjusted_confidence = max(0.1, min(0.95, adjusted_confidence))

        return {
            **base_confidence,
            "confidence_score": adjusted_confidence,
            "movement_type": movement["movement_type"],
            "movement_magnitude": movement["movement_magnitude"],
            "movement_modifier": movement_modifier,
        }

    def _get_movement_confidence_modifier(self, movement: dict[str, Any]) -> float:
        """Get confidence modifier based on movement characteristics"""
        movement_type = movement["movement_type"]
        movement_magnitude = movement["movement_magnitude"]
        late_movement = movement["late_movement"]
        reverse_movement = movement["reverse_movement"]

        # Base modifier on movement type
        type_modifiers = {
            "REVERSE_LINE_MOVEMENT": 1.3,  # Very significant
            "STEAM_MOVE": 1.2,  # Strong signal
            "SHARP_MOVEMENT": 1.15,  # Good signal
            "GENERAL_MOVEMENT": 1.0,  # Standard
        }

        type_modifier = type_modifiers.get(movement_type, 1.0)

        # Magnitude bonus
        if movement_magnitude >= 8.0:
            magnitude_modifier = 1.2
        elif movement_magnitude >= 6.0:
            magnitude_modifier = 1.1
        elif movement_magnitude >= 4.0:
            magnitude_modifier = 1.05
        else:
            magnitude_modifier = 1.0

        # Late movement bonus
        late_modifier = 1.2 if late_movement else 1.0

        # Reverse movement bonus
        reverse_modifier = 1.1 if reverse_movement else 1.0

        return type_modifier * magnitude_modifier * late_modifier * reverse_modifier

    def _create_movement_signal(
        self,
        movement: dict[str, Any],
        matching_strategy: ProfitableStrategy,
        confidence_data: dict[str, Any],
    ) -> BettingSignal:
        """Create line movement betting signal"""
        movement_direction = movement["movement_direction"]
        movement_type = movement["movement_type"]
        movement_magnitude = movement["movement_magnitude"]

        # Create recommendation
        team = (
            movement.get("home_team")
            if movement_direction == "HOME"
            else movement.get("away_team")
        )
        recommendation = f"BET {team} {movement.get('split_type', '').upper()} - {movement_type.replace('_', ' ').title()}"

        signal = self._create_betting_signal(
            movement, matching_strategy, confidence_data
        )

        # Update signal with movement-specific information
        signal.recommendation = recommendation
        signal.metadata = signal.metadata or {}
        signal.metadata.update(
            {
                "movement_type": movement_type,
                "movement_magnitude": movement_magnitude,
                "movement_direction": movement_direction,
                "late_movement": movement["late_movement"],
                "reverse_movement": movement["reverse_movement"],
                "steam_move": movement["steam_move"],
                "sharp_movement": movement["sharp_movement"],
                "time_to_game_hours": movement["time_to_game_hours"],
                "money_movement": movement["money_movement"],
                "bet_movement": movement["bet_movement"],
            }
        )

        return signal

    def _is_valid_movement_data(
        self, movement: dict[str, Any], current_time: datetime, minutes_ahead: int
    ) -> bool:
        """Validate movement data quality and timing"""
        try:
            # Check time window
            game_time = self._normalize_game_time(movement["game_datetime"])
            time_diff_minutes = self._calculate_minutes_to_game(game_time, current_time)

            if not (0 <= time_diff_minutes <= minutes_ahead):
                return False

            # Check data completeness
            required_fields = [
                "home_team",
                "away_team",
                "movement_type",
                "movement_magnitude",
            ]
            if not all(movement.get(field) is not None for field in required_fields):
                return False

            # Check movement magnitude is significant
            movement_magnitude = movement.get("movement_magnitude", 0)
            if movement_magnitude < 2.0:  # Less than 2% movement is not significant
                return False

            return True

        except (ValueError, TypeError, KeyError) as e:
            self.logger.warning(f"Invalid movement data: {e}")
            return False

    def _should_apply_juice_filter(self, movement: dict[str, Any]) -> bool:
        """Check if juice filter should be applied to this movement signal"""
        if movement.get("split_type") != "moneyline":
            return False

        # Apply juice filter for moneyline movements
        movement_direction = movement.get("movement_direction")
        recommended_team = (
            movement.get("home_team")
            if movement_direction == "HOME"
            else movement.get("away_team")
        )

        return self._should_filter_juice(
            "moneyline",
            movement.get("current_data", {}).get("split_value"),
            recommended_team,
            movement.get("home_team"),
            movement.get("away_team"),
        )

    def _log_movement_summary(
        self,
        signals: list[BettingSignal],
        movement_strategies: list[ProfitableStrategy],
        raw_data_count: int,
    ):
        """Log summary of movement processing"""
        movement_type_counts = {}
        direction_counts = {"HOME": 0, "AWAY": 0}
        late_movement_count = 0

        for signal in signals:
            if signal.metadata:
                movement_type = signal.metadata.get("movement_type", "unknown")
                movement_direction = signal.metadata.get(
                    "movement_direction", "unknown"
                )
                late_movement = signal.metadata.get("late_movement", False)

                movement_type_counts[movement_type] = (
                    movement_type_counts.get(movement_type, 0) + 1
                )

                if movement_direction in direction_counts:
                    direction_counts[movement_direction] += 1

                if late_movement:
                    late_movement_count += 1

        self.logger.info(
            f"Line movement processing complete: {len(signals)} signals from {raw_data_count} raw records",
            extra={
                "total_signals": len(signals),
                "raw_data_count": raw_data_count,
                "movement_strategies": len(movement_strategies),
                "movement_types": movement_type_counts,
                "movement_directions": direction_counts,
                "late_movements": late_movement_count,
            },
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
        - "line_movement_strategy_reverse" -> ("VSIN-unknown", "line_movement")
        """
        strategy_name_lower = strategy_name.lower()

        # Handle direct format: "SOURCE-BOOK-SPLITTYPE"
        if strategy_name.count("-") >= 2 and len(strategy_name.split("-")) == 3:
            parts = strategy_name.split("-")
            source_book = f"{parts[0]}-{parts[1]}"
            split_type = parts[2]
            return source_book, split_type

        # Handle line movement strategy format
        source_book = "VSIN-unknown"  # Default for line movement
        split_type = "line_movement"  # Default

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

        # Extract split type information for line movement
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
        elif "movement" in strategy_name_lower or "line" in strategy_name_lower:
            split_type = "line_movement"

        return source_book, split_type
