"""
Advanced movement analysis processors for betting intelligence.
"""

from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal

from src.data.models.unified.movement_analysis import (
    BettingPercentageSnapshot,
    CrossBookMovement,
    GameMovementAnalysis,
    LineMovementDetail,
    MarketMovementSummary,
    MarketType,
    MovementDirection,
    MovementMagnitude,
    RLMIndicator,
)


class MovementAnalyzer:
    """Analyzes line movements to detect patterns and opportunities."""

    def __init__(self):
        self.sportsbook_names = {
            "15": "DraftKings",
            "30": "FanDuel",
            "68": "Caesars",
            "69": "BetMGM",
            "71": "PointsBet",
            "75": "Barstool",
        }

    def _parse_timestamp_robust(self, timestamp_str: str) -> datetime:
        """Robustly parse timestamp strings with various microsecond precisions."""
        if not timestamp_str:
            return datetime.now()
        
        try:
            # First try direct parsing
            dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            # Ensure timezone-aware
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            pass
        
        try:
            # Handle microsecond precision issues
            import re
            
            # Match ISO format with optional microseconds
            pattern = r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})?'
            match = re.match(pattern, timestamp_str)
            
            if match:
                date_time = match.group(1)
                microseconds = match.group(2) or "0"
                timezone = match.group(3) or "+00:00"
                
                # Normalize microseconds to 6 digits (pad or truncate)
                if len(microseconds) > 6:
                    microseconds = microseconds[:6]
                else:
                    microseconds = microseconds.ljust(6, '0')
                
                # Normalize timezone
                if timezone == "Z":
                    timezone = "+00:00"
                
                # Reconstruct timestamp
                normalized_timestamp = f"{date_time}.{microseconds}{timezone}"
                dt = datetime.fromisoformat(normalized_timestamp)
                # Ensure timezone-aware
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            else:
                # Fallback: try without microseconds
                pattern_simple = r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(Z|[+-]\d{2}:\d{2})?'
                match_simple = re.match(pattern_simple, timestamp_str)
                if match_simple:
                    date_time = match_simple.group(1)
                    timezone = match_simple.group(2) or "+00:00"
                    if timezone == "Z":
                        timezone = "+00:00"
                    dt = datetime.fromisoformat(f"{date_time}{timezone}")
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt
                
        except Exception as e:
            print(f"Warning: Could not parse timestamp '{timestamp_str}': {e}")
            return datetime.now(timezone.utc)
        
        # Final fallback
        print(f"Warning: Using current time for unparseable timestamp: {timestamp_str}")
        return datetime.now(timezone.utc)

    async def analyze_game_movements(self, game_data: dict) -> GameMovementAnalysis:
        """Analyze all movements for a single game."""
        game_id = game_data.get("game_id")
        home_team = game_data.get("home_team", "")
        away_team = game_data.get("away_team", "")
        game_datetime = datetime.fromisoformat(
            game_data.get("game_datetime", "").replace("Z", "+00:00")
        )

        # Extract detailed movements
        line_movements = await self._extract_line_movements(game_data)
        betting_snapshots = await self._extract_betting_snapshots(game_data)

        # Analyze patterns
        rlm_indicators = await self._detect_rlm(line_movements, betting_snapshots)
        cross_book_movements = await self._analyze_cross_book_movements(line_movements)

        # Create market summaries
        moneyline_summary = await self._create_market_summary(
            line_movements, MarketType.MONEYLINE
        )
        spread_summary = await self._create_market_summary(
            line_movements, MarketType.SPREAD
        )
        total_summary = await self._create_market_summary(
            line_movements, MarketType.TOTAL
        )

        # Generate insights
        sharp_money_indicators = await self._detect_sharp_money_patterns(
            line_movements, rlm_indicators, cross_book_movements
        )
        arbitrage_opportunities = await self._detect_arbitrage_opportunities(
            line_movements
        )
        recommended_actions = await self._generate_recommendations(
            rlm_indicators, cross_book_movements, sharp_money_indicators
        )

        return GameMovementAnalysis(
            game_id=game_id,
            home_team=home_team,
            away_team=away_team,
            game_datetime=game_datetime,
            analysis_timestamp=datetime.now(timezone.utc),
            moneyline_summary=moneyline_summary,
            spread_summary=spread_summary,
            total_summary=total_summary,
            line_movements=line_movements,
            rlm_indicators=rlm_indicators,
            cross_book_movements=cross_book_movements,
            betting_snapshots=betting_snapshots,
            sharp_money_indicators=sharp_money_indicators,
            arbitrage_opportunities=arbitrage_opportunities,
            recommended_actions=recommended_actions,
        )

    async def _extract_line_movements(
        self, game_data: dict
    ) -> list[LineMovementDetail]:
        """Extract detailed line movements from raw game data."""
        movements = []
        raw_data = game_data.get("raw_data", {})

        for sportsbook_id, sportsbook_data in raw_data.items():
            event_data = sportsbook_data.get("event", {})

            # Process each market type
            for market_type_str in ["moneyline", "spread", "total"]:
                try:
                    market_type = MarketType(market_type_str)
                except ValueError:
                    # Handle case where market_type_str is not a valid MarketType enum value
                    continue
                market_data = event_data.get(market_type_str, [])

                if isinstance(market_data, list):
                    for market_item in market_data:
                        history = market_item.get("history", [])
                        if len(history) < 2:
                            continue

                        # Analyze each movement in history
                        for i in range(1, len(history)):
                            prev_entry = history[i - 1]
                            curr_entry = history[i]

                            movement = await self._create_movement_detail(
                                sportsbook_id, market_type, prev_entry, curr_entry
                            )
                            if movement:
                                movements.append(movement)

        return movements

    async def _create_movement_detail(
        self,
        sportsbook_id: str,
        market_type: MarketType,
        prev_entry: dict,
        curr_entry: dict,
    ) -> LineMovementDetail | None:
        """Create a detailed movement record from two history entries."""
        try:
            # Handle timestamp parsing with microsecond precision issues
            timestamp_str = curr_entry.get("updated_at", "")
            if not timestamp_str:
                timestamp = datetime.now(timezone.utc)
            else:
                timestamp = self._parse_timestamp_robust(timestamp_str)

            prev_odds = prev_entry.get("odds")
            curr_odds = curr_entry.get("odds")
            prev_value = prev_entry.get("value")
            curr_value = curr_entry.get("value")

            if prev_odds is None or curr_odds is None:
                return None

            # Determine direction
            direction = MovementDirection.STABLE
            movement_amount = None

            if market_type == MarketType.MONEYLINE:
                if curr_odds > prev_odds:
                    direction = MovementDirection.UP
                elif curr_odds < prev_odds:
                    direction = MovementDirection.DOWN
                movement_amount = Decimal(str(abs(curr_odds - prev_odds)))

            elif market_type in [MarketType.SPREAD, MarketType.TOTAL]:
                if prev_value is not None and curr_value is not None:
                    if curr_value > prev_value:
                        direction = MovementDirection.UP
                    elif curr_value < prev_value:
                        direction = MovementDirection.DOWN
                    movement_amount = Decimal(
                        str(abs(float(curr_value) - float(prev_value)))
                    )

            return LineMovementDetail(
                timestamp=timestamp,
                sportsbook_id=sportsbook_id,
                market_type=market_type,
                previous_value=prev_value,
                new_value=curr_value,
                previous_odds=prev_odds,
                new_odds=curr_odds,
                direction=direction,
                magnitude=MovementMagnitude.MINOR,  # Will be auto-calculated by validator
                movement_amount=movement_amount,
            )

        except Exception as e:
            print(f"Error creating movement detail: {e}")
            return None

    async def _extract_betting_snapshots(
        self, game_data: dict
    ) -> list[BettingPercentageSnapshot]:
        """Extract betting percentage snapshots from raw data."""
        snapshots = []
        raw_data = game_data.get("raw_data", {})

        for sportsbook_id, sportsbook_data in raw_data.items():
            event_data = sportsbook_data.get("event", {})

            for market_type_str in ["moneyline", "spread", "total"]:
                try:
                    market_type = MarketType(market_type_str)
                except ValueError:
                    # Handle case where market_type_str is not a valid MarketType enum value
                    continue
                market_data = event_data.get(market_type_str, [])

                if isinstance(market_data, list):
                    for market_item in market_data:
                        bet_info = market_item.get("bet_info", {})
                        if bet_info:
                            tickets = bet_info.get("tickets", {})
                            money = bet_info.get("money", {})

                            snapshot = BettingPercentageSnapshot(
                                timestamp=datetime.now(timezone.utc),
                                sportsbook_id=sportsbook_id,
                                market_type=market_type,
                                tickets_percent=tickets.get("percent"),
                                money_percent=money.get("percent"),
                                tickets_count=tickets.get("value"),
                                money_amount=money.get("value"),
                            )
                            snapshots.append(snapshot)

        return snapshots

    async def _detect_rlm(
        self,
        movements: list[LineMovementDetail],
        snapshots: list[BettingPercentageSnapshot],
    ) -> list[RLMIndicator]:
        """Detect reverse line movement patterns."""
        rlm_indicators = []

        # Group movements by sportsbook and market type
        grouped_movements = defaultdict(list)
        for movement in movements:
            key = (movement.sportsbook_id, movement.market_type)
            grouped_movements[key].append(movement)

        # Group snapshots by sportsbook and market type
        grouped_snapshots = defaultdict(list)
        for snapshot in snapshots:
            key = (snapshot.sportsbook_id, snapshot.market_type)
            grouped_snapshots[key].append(snapshot)

        # Analyze each group for RLM
        for (sportsbook_id, market_type), movement_list in grouped_movements.items():
            snapshot_list = grouped_snapshots.get((sportsbook_id, market_type), [])

            if not snapshot_list:
                continue

            # Get most recent snapshot
            latest_snapshot = max(snapshot_list, key=lambda x: x.timestamp)

            # Analyze recent movements
            recent_movements = sorted(movement_list, key=lambda x: x.timestamp)[
                -5:
            ]  # Last 5 movements

            if len(recent_movements) < 2:
                continue

            # Determine overall line direction
            line_direction = self._get_dominant_direction(recent_movements)

            # Determine public betting direction based on percentages
            public_direction = self._get_public_direction(latest_snapshot)

            # Calculate RLM metrics
            total_movement = sum(
                movement.movement_amount or Decimal("0")
                for movement in recent_movements
            )

            rlm = RLMIndicator(
                market_type=market_type,
                sportsbook_id=sportsbook_id,
                line_direction=line_direction,
                public_betting_direction=public_direction,
                public_percentage=latest_snapshot.tickets_percent,
                line_movement_amount=total_movement,
            )

            if rlm.is_rlm:
                rlm_indicators.append(rlm)

        return rlm_indicators

    def _get_dominant_direction(
        self, movements: list[LineMovementDetail]
    ) -> MovementDirection:
        """Get the dominant direction from a list of movements."""
        if not movements:
            return MovementDirection.STABLE

        up_count = sum(1 for m in movements if m.direction == MovementDirection.UP)
        down_count = sum(1 for m in movements if m.direction == MovementDirection.DOWN)

        if up_count > down_count:
            return MovementDirection.UP
        elif down_count > up_count:
            return MovementDirection.DOWN
        else:
            return MovementDirection.STABLE

    def _get_public_direction(
        self, snapshot: BettingPercentageSnapshot
    ) -> MovementDirection:
        """Determine public betting direction based on percentages."""
        tickets_pct = snapshot.tickets_percent or 50

        if tickets_pct > 55:
            return MovementDirection.UP  # Public betting heavily on one side
        elif tickets_pct < 45:
            return MovementDirection.DOWN  # Public betting heavily on other side
        else:
            return MovementDirection.STABLE  # Balanced betting

    async def _analyze_cross_book_movements(
        self, movements: list[LineMovementDetail]
    ) -> list[CrossBookMovement]:
        """Analyze movements across multiple sportsbooks."""
        cross_book_movements = []

        # Group movements by market type and timestamp (within 5 minutes)
        time_groups = defaultdict(lambda: defaultdict(list))

        for movement in movements:
            # Round timestamp to 5-minute intervals
            rounded_time = movement.timestamp.replace(
                minute=movement.timestamp.minute // 5 * 5, second=0, microsecond=0
            )
            time_groups[rounded_time][movement.market_type].append(movement)

        # Analyze each time group
        for timestamp, market_groups in time_groups.items():
            for market_type, market_movements in market_groups.items():
                if (
                    len(market_movements) < 3
                ):  # Need at least 3 books for cross-book analysis
                    continue

                participating_books = list(
                    set(m.sportsbook_id for m in market_movements)
                )

                # Analyze consensus
                directions = [m.direction for m in market_movements]
                direction_counts = {
                    MovementDirection.UP: directions.count(MovementDirection.UP),
                    MovementDirection.DOWN: directions.count(MovementDirection.DOWN),
                    MovementDirection.STABLE: directions.count(
                        MovementDirection.STABLE
                    ),
                }

                consensus_direction = max(direction_counts, key=direction_counts.get)
                consensus_strength = direction_counts[consensus_direction] / len(
                    directions
                )

                # Find divergent books
                divergent_books = [
                    m.sportsbook_id
                    for m in market_movements
                    if m.direction != consensus_direction
                ]

                cross_book = CrossBookMovement(
                    market_type=market_type,
                    timestamp=timestamp,
                    participating_books=participating_books,
                    consensus_direction=consensus_direction,
                    consensus_strength="strong"
                    if consensus_strength > 0.8
                    else "moderate"
                    if consensus_strength > 0.6
                    else "weak",
                    divergent_books=divergent_books,
                    average_movement=Decimal(
                        str(
                            sum(float(m.movement_amount or 0) for m in market_movements)
                            / len(market_movements)
                        )
                    ),
                )

                cross_book_movements.append(cross_book)

        return cross_book_movements

    async def _create_market_summary(
        self, movements: list[LineMovementDetail], market_type: MarketType
    ) -> MarketMovementSummary:
        """Create a summary for a specific market type."""
        market_movements = [m for m in movements if m.market_type == market_type]

        if not market_movements:
            return MarketMovementSummary(
                market_type=market_type,
                total_movements=0,
                significant_movements=0,
                major_movements=0,
            )

        significant_count = sum(
            1
            for m in market_movements
            if m.magnitude in [MovementMagnitude.SIGNIFICANT, MovementMagnitude.MAJOR]
        )

        major_count = sum(
            1 for m in market_movements if m.magnitude == MovementMagnitude.MAJOR
        )

        dominant_direction = self._get_dominant_direction(market_movements)

        return MarketMovementSummary(
            market_type=market_type,
            total_movements=len(market_movements),
            significant_movements=significant_count,
            major_movements=major_count,
            dominant_direction=dominant_direction,
        )

    async def _detect_sharp_money_patterns(
        self,
        movements: list[LineMovementDetail],
        rlm_indicators: list[RLMIndicator],
        cross_book_movements: list[CrossBookMovement],
    ) -> list[str]:
        """Detect patterns indicating sharp money activity."""
        indicators = []

        # RLM indicators
        strong_rlm = [r for r in rlm_indicators if r.rlm_strength == "strong"]
        if strong_rlm:
            indicators.append(f"Strong RLM detected in {len(strong_rlm)} markets")

        # Steam moves
        steam_moves = [c for c in cross_book_movements if c.steam_move_detected]
        if steam_moves:
            indicators.append(f"Steam moves detected in {len(steam_moves)} markets")

        # High-magnitude movements
        major_movements = [
            m for m in movements if m.magnitude == MovementMagnitude.MAJOR
        ]
        if len(major_movements) > 5:
            indicators.append(
                f"High volume of major movements ({len(major_movements)})"
            )

        # Rapid consecutive movements
        rapid_movements = self._detect_rapid_movements(movements)
        if rapid_movements:
            indicators.append(
                f"Rapid consecutive movements detected in {len(rapid_movements)} markets"
            )

        return indicators

    def _detect_rapid_movements(self, movements: list[LineMovementDetail]) -> list[str]:
        """Detect rapid consecutive movements indicating sharp action."""
        rapid_patterns = []

        # Group by sportsbook and market
        grouped = defaultdict(list)
        for movement in movements:
            key = (movement.sportsbook_id, movement.market_type)
            grouped[key].append(movement)

        # Check for rapid movements (3+ movements within 10 minutes)
        for (sportsbook_id, market_type), movement_list in grouped.items():
            sorted_movements = sorted(movement_list, key=lambda x: x.timestamp)

            for i in range(len(sorted_movements) - 2):
                window_movements = sorted_movements[i : i + 3]
                time_span = (
                    window_movements[-1].timestamp - window_movements[0].timestamp
                ).total_seconds()

                if time_span <= 600:  # 10 minutes
                    # Handle both string and enum market types
                    market_type_str = market_type.value if hasattr(market_type, 'value') else str(market_type)
                    rapid_patterns.append(
                        f"{self.sportsbook_names.get(sportsbook_id, sportsbook_id)} {market_type_str}"
                    )

        return rapid_patterns

    async def _detect_arbitrage_opportunities(
        self, movements: list[LineMovementDetail]
    ) -> list[dict]:
        """Detect potential arbitrage opportunities from line discrepancies."""
        # This is a simplified arbitrage detection
        # In practice, you'd need current odds from all books
        opportunities = []

        # Group by market type and timestamp
        recent_movements = defaultdict(list)
        current_time = datetime.now(timezone.utc)
        one_hour_ago = current_time.replace(hour=current_time.hour - 1)
        
        for movement in movements:
            if movement.timestamp > one_hour_ago:  # Last hour
                recent_movements[movement.market_type].append(movement)

        # Look for significant discrepancies
        for market_type, market_movements in recent_movements.items():
            if len(set(m.sportsbook_id for m in market_movements)) >= 2:
                # Check for odds discrepancies
                odds_by_book = {}
                for movement in market_movements:
                    odds_by_book[movement.sportsbook_id] = movement.new_odds

                if len(odds_by_book) >= 2:
                    odds_values = list(odds_by_book.values())
                    max_odds = max(odds_values)
                    min_odds = min(odds_values)

                    if max_odds - min_odds > 20:  # Significant discrepancy
                        market_type_str = market_type.value if hasattr(market_type, 'value') else str(market_type)
                        opportunities.append(
                            {
                                "market_type": market_type_str,
                                "discrepancy": max_odds - min_odds,
                                "books": list(odds_by_book.keys()),
                                "potential_profit": None,  # Would calculate based on stake - using None for database compatibility
                            }
                        )

        return opportunities

    async def _generate_recommendations(
        self,
        rlm_indicators: list[RLMIndicator],
        cross_book_movements: list[CrossBookMovement],
        sharp_indicators: list[str],
    ) -> list[str]:
        """Generate actionable recommendations based on analysis."""
        recommendations = []

        # RLM recommendations
        strong_rlm = [r for r in rlm_indicators if r.rlm_strength == "strong"]
        for rlm in strong_rlm:
            book_name = self.sportsbook_names.get(rlm.sportsbook_id, rlm.sportsbook_id)
            recommendations.append(
                f"STRONG RLM: Consider {rlm.market_type.value} bet opposite to public on {book_name}"
            )

        # Steam move recommendations
        steam_moves = [c for c in cross_book_movements if c.steam_move_detected]
        for steam in steam_moves:
            recommendations.append(
                f"STEAM MOVE: Follow {steam.market_type.value} movement across {len(steam.participating_books)} books"
            )

        # Sharp money recommendations
        if len(sharp_indicators) >= 3:
            recommendations.append(
                "HIGH SHARP ACTIVITY: Exercise caution, professional money heavily involved"
            )

        return recommendations
