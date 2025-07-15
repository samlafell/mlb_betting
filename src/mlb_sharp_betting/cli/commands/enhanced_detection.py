"""
Enhanced detection commands with integrated pipeline.
"""

import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path

import click
import structlog

from ...analysis.processors.strategy_processor_factory import StrategyProcessorFactory
from ...db.connection import get_db_manager
from ...models.betting_analysis import (
    BettingSignal,
    ConfidenceLevel,
    SignalProcessorConfig,
)
from ...services.betting_recommendation_formatter import BettingRecommendationFormatter
from ...services.betting_signal_repository import BettingSignalRepository
from ...services.pipeline_orchestrator import PipelineOrchestrator
from ...services.pre_game_recommendation_tracker import (
    PreGameRecommendation,
    PreGameRecommendationTracker,
)
from ...services.strategy_validator import StrategyValidator

logger = structlog.get_logger(__name__)


def _convert_betting_signals_to_recommendations(
    signals: list[BettingSignal], recommended_at: datetime
) -> list[PreGameRecommendation]:
    """
    Convert BettingSignal objects to PreGameRecommendation objects for storage.

    Args:
        signals: List of betting signals to convert
        recommended_at: Timestamp when recommendations were made

    Returns:
        List of PreGameRecommendation objects
    """
    recommendations = []

    for signal in signals:
        try:
            # Generate unique recommendation ID
            rec_id = f"{signal.game_pk}_{signal.bet_type}_{signal.recommended_side}_{int(recommended_at.timestamp())}"

            # Map confidence level
            confidence_map = {
                ConfidenceLevel.HIGH: "HIGH",
                ConfidenceLevel.MODERATE: "MODERATE",
                ConfidenceLevel.LOW: "LOW",
            }
            confidence = confidence_map.get(signal.confidence_level, "MODERATE")

            # Create recommendation text
            if signal.bet_type == "moneyline":
                recommendation_text = f"BET {signal.recommended_side.upper()} ML"
            elif signal.bet_type == "spread":
                recommendation_text = f"BET {signal.recommended_side.upper()} SPREAD"
            elif signal.bet_type == "total":
                recommendation_text = f"BET {signal.recommended_side.upper()}"
            else:
                recommendation_text = f"BET {signal.recommended_side.upper()}"

            # Create PreGameRecommendation object
            recommendation = PreGameRecommendation(
                recommendation_id=rec_id,
                game_pk=signal.game_pk,
                home_team=signal.home_team,
                away_team=signal.away_team,
                game_datetime=signal.game_datetime,
                recommendation=recommendation_text,
                bet_type=signal.bet_type,
                confidence_level=confidence,
                signal_source=signal.signal_source,
                signal_strength=signal.confidence_score,
                recommended_at=recommended_at,
                email_sent=False,  # These are CLI-generated, not email-generated
            )

            recommendations.append(recommendation)

        except Exception as e:
            logger.error(
                "Failed to convert signal to recommendation",
                signal=signal,
                error=str(e),
            )
            continue

    return recommendations


def _convert_cross_market_flips_to_recommendations(
    flips: list, recommended_at: datetime
) -> list[PreGameRecommendation]:
    """
    Convert cross-market flip objects to PreGameRecommendation objects.

    Args:
        flips: List of cross-market flip objects
        recommended_at: Timestamp when recommendations were made

    Returns:
        List of PreGameRecommendation objects
    """
    recommendations = []

    for flip in flips:
        try:
            # Generate unique recommendation ID for cross-market flip
            rec_id = f"flip_{flip.game_id}_{flip.flip_type.value}_{int(recommended_at.timestamp())}"

            # Map confidence to standard levels
            if flip.confidence_score >= 80:
                confidence = "HIGH"
            elif flip.confidence_score >= 65:
                confidence = "MODERATE"
            else:
                confidence = "LOW"

            # Use the strategy recommendation as the bet text
            recommendation_text = flip.strategy_recommendation

            # Determine bet type from flip type
            bet_type_map = {
                "MONEYLINE_TO_SPREAD": "spread",
                "SPREAD_TO_MONEYLINE": "moneyline",
                "TOTAL_FLIP": "total",
            }
            bet_type = bet_type_map.get(flip.flip_type.value, "moneyline")

            # Create recommendation
            recommendation = PreGameRecommendation(
                recommendation_id=rec_id,
                game_pk=int(flip.game_id)
                if flip.game_id.isdigit()
                else hash(flip.game_id) % 1000000,
                home_team=flip.home_team,
                away_team=flip.away_team,
                game_datetime=flip.game_datetime,
                recommendation=recommendation_text,
                bet_type=bet_type,
                confidence_level=confidence,
                signal_source="CROSS_MARKET_FLIP",
                signal_strength=flip.confidence_score
                / 100.0,  # Convert percentage to decimal
                recommended_at=recommended_at,
                email_sent=False,
            )

            recommendations.append(recommendation)

        except Exception as e:
            logger.error(
                "Failed to convert cross-market flip to recommendation",
                flip=flip,
                error=str(e),
            )
            continue

    return recommendations


@click.group()
def detection_group():
    """🎯 Enhanced betting opportunity detection commands."""
    pass


@detection_group.command("opportunities")
@click.option(
    "--minutes",
    "-m",
    type=int,
    default=60,
    help="Minutes ahead to look for opportunities (default: 60)",
)
@click.option(
    "--fresh-data/--use-existing",
    default=True,
    help="Collect fresh data before detection (default: enabled)",
)
@click.option(
    "--run-backtesting/--skip-backtesting",
    default=True,
    help="Run backtesting before detection (default: enabled)",
)
@click.option(
    "--format",
    "-f",
    type=click.Choice(["console", "json"]),
    default="console",
    help="Output format (default: console)",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    help="Output file path for JSON format",
)
@click.option(
    "--include-cross-market/--no-cross-market",
    default=True,
    help="Include cross-market flip detection (default: enabled)",
)
@click.option(
    "--min-flip-confidence",
    type=float,
    default=60.0,
    help="Minimum confidence for cross-market flips (default: 60.0)",
)
@click.option(
    "--store-recommendations/--no-store",
    default=True,
    help="Store betting recommendations to PostgreSQL (default: enabled)",
)
@click.option(
    "--min-confidence",
    type=float,
    default=60.0,
    help="Minimum confidence to store recommendations (default: 60.0)",
)
@click.option(
    "--simple-mode",
    is_flag=True,
    help="Use simple detection mode (bypasses complex pipeline)",
)
@click.option(
    "--debug", is_flag=True, help="Enable debug output for performance monitoring"
)
@click.option(
    "--show-stats", is_flag=True, help="Show repository performance statistics"
)
def detect_opportunities(
    minutes: int,
    fresh_data: bool,
    run_backtesting: bool,
    format: str,
    output: Path | None,
    include_cross_market: bool,
    min_flip_confidence: float,
    store_recommendations: bool,
    min_confidence: float,
    simple_mode: bool,
    debug: bool,
    show_stats: bool,
):
    """🎯 MASTER BETTING DETECTOR with full data pipeline and PostgreSQL storage

    This command runs a complete betting detection pipeline that:
    1. ✅ Checks for current, active profitable strategies
    2. ✅ Checks data freshness and collects new data if needed
    3. ✅ Runs backtested strategies against current data
    4. ✅ Makes betting recommendations based on validated strategies
    5. ✅ Stores recommendations to PostgreSQL with full context
    6. ✅ Handles multiple recommendations per game (e.g., early vs late)

    NEW: --simple-mode bypasses complex pipeline for direct SQL detection

    Each recommendation is stored with:
    - Exact recommendation text and confidence level
    - Strategy source and signal strength at time of recommendation
    - Game context and timing information
    - Unique ID to track multiple recommendations for same game
    """
    if simple_mode:
        asyncio.run(_run_simple_detection(minutes, debug, show_stats, format, output))
    else:
        asyncio.run(
            run_detection(
                minutes,
                fresh_data,
                run_backtesting,
                format,
                output,
                include_cross_market,
                min_flip_confidence,
                store_recommendations,
                min_confidence,
            )
        )


async def _run_simple_detection(
    minutes_ahead: int,
    debug: bool,
    show_stats: bool,
    format: str,
    output: Path | None,
):
    """
    Robust opportunity detection that works with basic betting splits data
    """
    import sys
    from datetime import datetime, timedelta

    start_time = datetime.now()

    try:
        db_manager = get_db_manager()

        if debug:
            click.echo("🔍 DEBUG MODE: Enhanced logging enabled")
            click.echo(
                f"⚙️  Configuration: minutes_ahead={minutes_ahead}, simple_mode=True"
            )

        click.echo("🔄 Searching for betting opportunities...")

        # Calculate time window for upcoming games
        now = datetime.now()
        target_time = now + timedelta(minutes=minutes_ahead)

        # Robust query with proper type handling
        query = """
            SELECT DISTINCT
                game_id,
                home_team,
                away_team,
                split_type,
                CAST(home_or_over_bets_percentage AS FLOAT) as home_bets_pct,
                CAST(home_or_over_stake_percentage AS FLOAT) as home_stake_pct,
                CAST(ABS(home_or_over_bets_percentage - home_or_over_stake_percentage) AS FLOAT) as differential,
                last_updated,
                game_datetime,
                source,
                book
            FROM splits.raw_mlb_betting_splits
            WHERE 
                last_updated >= CURRENT_DATE - INTERVAL '2 days'
                AND home_or_over_bets_percentage IS NOT NULL
                AND home_or_over_stake_percentage IS NOT NULL
                AND ABS(home_or_over_bets_percentage - home_or_over_stake_percentage) >= 10.0
                AND (
                    game_datetime IS NULL 
                    OR game_datetime >= NOW()
                    OR game_datetime <= NOW() + INTERVAL %s
                )
            ORDER BY differential DESC, last_updated DESC
            LIMIT 50
        """

        with db_manager.get_cursor() as cursor:
            cursor.execute(query, (f"{minutes_ahead} minutes",))
            opportunities = cursor.fetchall()

        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()

        # Format results for output
        formatted_results = []

        if opportunities:
            click.echo(f"\n🎯 Found {len(opportunities)} potential opportunities:")

            # Group by game for better display
            games = {}
            for opp in opportunities:
                # Convert to dict format for consistent handling
                if isinstance(opp, dict):
                    opp_dict = opp
                else:
                    # Handle tuple format
                    opp_dict = {
                        "game_id": opp[0],
                        "home_team": opp[1],
                        "away_team": opp[2],
                        "split_type": opp[3],
                        "home_bets_pct": opp[4],
                        "home_stake_pct": opp[5],
                        "differential": opp[6],
                        "last_updated": opp[7],
                        "game_datetime": opp[8],
                        "source": opp[9] if len(opp) > 9 else None,
                        "book": opp[10] if len(opp) > 10 else None,
                    }

                # Safely convert values to float
                try:
                    opp_dict["home_bets_pct"] = (
                        float(opp_dict["home_bets_pct"])
                        if opp_dict["home_bets_pct"] is not None
                        else 0.0
                    )
                    opp_dict["home_stake_pct"] = (
                        float(opp_dict["home_stake_pct"])
                        if opp_dict["home_stake_pct"] is not None
                        else 0.0
                    )
                    opp_dict["differential"] = (
                        float(opp_dict["differential"])
                        if opp_dict["differential"] is not None
                        else 0.0
                    )
                except (ValueError, TypeError, AttributeError) as e:
                    if debug:
                        click.echo(
                            f"⚠️  Skipping record due to type conversion error: {e}"
                        )
                    continue

                game_key = f"{opp_dict['away_team']} @ {opp_dict['home_team']}"
                if game_key not in games:
                    games[game_key] = []
                games[game_key].append(opp_dict)

            # Display results or prepare for JSON output
            if format == "console":
                for game_key, game_opps in games.items():
                    click.echo(f"\n📋 {game_key}:")

                    for opp in game_opps[:3]:  # Show top 3 per game
                        split_type = opp["split_type"] or "unknown"
                        differential = opp["differential"]
                        home_bets = opp["home_bets_pct"]
                        home_stake = opp["home_stake_pct"]

                        # Determine which side has the sharp money (stake vs bets)
                        if home_stake > home_bets:
                            if split_type.lower() in ["moneyline", "spread"]:
                                sharp_side = "HOME"
                            else:
                                sharp_side = "OVER"
                            recommendation = f"Sharp money on {sharp_side}"
                        else:
                            if split_type.lower() in ["moneyline", "spread"]:
                                sharp_side = "AWAY"
                            else:
                                sharp_side = "UNDER"
                            recommendation = f"Sharp money on {sharp_side}"

                        # Simple confidence calculation
                        confidence = min(95.0, max(50.0, differential * 2.5))

                        click.echo(f"   • {split_type.title()}: {recommendation}")
                        click.echo(
                            f"     📊 Differential: {differential:.1f}% | Confidence: {confidence:.1f}%"
                        )
                        click.echo(
                            f"     🎯 Bets: {home_bets:.1f}% | Money: {home_stake:.1f}%"
                        )

                        # Add source info if available
                        if opp.get("source") and opp.get("book"):
                            source = opp.get("source")
                            book = opp.get("book")

                            # Format book attribution to make data source clear
                            if source == "VSIN" and book and book != "unknown":
                                book_attribution = f"VSIN ({book.title()})"
                            elif source == "SBD":
                                book_attribution = "SBD"
                            else:
                                if book and book != "unknown":
                                    book_attribution = f"{source} ({book.title()})"
                                else:
                                    book_attribution = source

                            click.echo(f"     📡 Source: {book_attribution}")
                        elif opp.get("source"):
                            click.echo(f"     📡 Source: {opp.get('source')}")

                        # Signal strength indicators
                        if differential >= 20:
                            click.echo("     🔥 STRONG SIGNAL - High differential")
                        elif differential >= 15:
                            click.echo("     ⭐ GOOD SIGNAL - Notable differential")
                        elif differential >= 10:
                            click.echo("     ✅ VALID SIGNAL - Above threshold")

                        # Store for JSON output
                        formatted_results.append(
                            {
                                "game": game_key,
                                "split_type": split_type,
                                "recommendation": recommendation,
                                "differential": differential,
                                "confidence": confidence,
                                "home_bets_pct": home_bets,
                                "home_stake_pct": home_stake,
                                "source": opp.get("source"),
                                "book": opp.get("book"),
                            }
                        )

                    if len(game_opps) > 3:
                        click.echo(f"   ... and {len(game_opps) - 3} more signals")

            # JSON output
            elif format == "json":
                json_output = {
                    "timestamp": datetime.now().isoformat(),
                    "search_minutes_ahead": minutes_ahead,
                    "opportunities_found": len(opportunities),
                    "games_with_opportunities": len(games),
                    "processing_time_seconds": processing_time,
                    "detection_method": "simple_mode_direct_sql",
                    "opportunities": formatted_results,
                }

                json_str = json.dumps(json_output, indent=2, default=str)
                if output:
                    output.write_text(json_str)
                    click.echo(f"✅ JSON output saved to: {output}")
                else:
                    click.echo(json_str)
        else:
            click.echo("📭 No betting opportunities found")
            click.echo("💡 Suggestions:")
            click.echo("   • Try increasing --minutes for more games")
            click.echo("   • Check if recent data is available with 'mlb-cli query'")
            click.echo("   • Run 'mlb-cli run' to collect fresh data")

        # Performance summary
        if format == "console":
            click.echo(f"\n⏱️  Processing completed in {processing_time:.2f} seconds")

            if show_stats:
                click.echo("\n📊 Detection Statistics:")
                click.echo("   • Database queries: 1 (optimized)")
                click.echo(
                    f"   • Records analyzed: {len(opportunities) if opportunities else 0}"
                )
                click.echo(
                    f"   • Games with opportunities: {len(games) if opportunities else 0}"
                )
                click.echo("   • Detection method: Direct SQL with type safety")
                click.echo(f"   • Time window: {minutes_ahead} minutes ahead")

    except Exception as e:
        click.echo(f"❌ Error detecting opportunities: {e}", err=True)
        if debug:
            import traceback

            click.echo(f"Full traceback:\n{traceback.format_exc()}", err=True)

        # Provide helpful troubleshooting info
        click.echo("\n🔧 Troubleshooting:")
        click.echo("   • Check if PostgreSQL is running")
        click.echo("   • Verify database 'mlb_betting' exists")
        click.echo("   • Ensure betting splits data is available")
        click.echo("   • Try 'mlb-cli status' to check system health")

        sys.exit(1)


async def run_detection(
    minutes: int,
    fresh_data: bool,
    run_backtesting: bool,
    format: str,
    output: Path | None,
    include_cross_market: bool,
    min_flip_confidence: float,
    store_recommendations: bool,
    min_confidence: float,
):
    """
    Enhanced detection command with full data pipeline and PostgreSQL storage
    """
    click.echo("🎯 MASTER BETTING DETECTOR")
    click.echo("=" * 60)
    click.echo("🤖 Using AI-optimized strategies with intelligent pipeline")

    if fresh_data:
        click.echo("📡 Fresh data collection: ENABLED")
    else:
        click.echo("📊 Using existing data: ENABLED")

    if run_backtesting:
        click.echo("🔬 Backtesting: ENABLED")
    else:
        click.echo("📋 Using existing backtesting: ENABLED")

    if include_cross_market:
        click.echo("🔀 Cross-market flips: ENABLED")

    if store_recommendations:
        click.echo("💾 PostgreSQL storage: ENABLED")
        click.echo(f"📊 Min confidence for storage: {min_confidence}%")

    # Initialize recommendation tracker if storing
    recommendation_tracker = None
    storage_enabled = store_recommendations  # Create local copy
    if storage_enabled:
        try:
            recommendation_tracker = PreGameRecommendationTracker()
            click.echo("✅ Recommendation tracker initialized")
        except Exception as e:
            click.echo(f"⚠️  Warning: Failed to initialize recommendation tracker: {e}")
            click.echo("   Continuing without recommendation storage...")
            storage_enabled = False

    try:
        # Initialize orchestrator
        orchestrator = PipelineOrchestrator()

        # Run smart pipeline
        results = await orchestrator.execute_smart_pipeline(
            detection_minutes=minutes,
            force_fresh_data=fresh_data,
            force_backtesting=run_backtesting,
        )

        # Store recommendations to PostgreSQL if enabled
        stored_recommendations = []
        if storage_enabled and recommendation_tracker:
            current_time = datetime.now()

            try:
                # Convert detection results to recommendations
                all_signals = []
                if results["detection_results"]:
                    detection_results = results["detection_results"]
                    for game_key, game_analysis in detection_results.games.items():
                        # Collect all signals from this game
                        all_signals.extend(game_analysis.sharp_signals)
                        all_signals.extend(game_analysis.opposing_markets)
                        all_signals.extend(game_analysis.steam_moves)
                        all_signals.extend(game_analysis.book_conflicts)

                # Filter signals by confidence threshold
                high_confidence_signals = [
                    signal
                    for signal in all_signals
                    if signal.confidence_score >= (min_confidence / 100.0)
                ]

                # Convert to recommendations
                signal_recommendations = _convert_betting_signals_to_recommendations(
                    high_confidence_signals, current_time
                )
                stored_recommendations.extend(signal_recommendations)

                # Convert cross-market flips to recommendations
                if include_cross_market and results["cross_market_flips"]:
                    flips = results["cross_market_flips"]["flips"]
                    flip_recommendations = (
                        _convert_cross_market_flips_to_recommendations(
                            flips, current_time
                        )
                    )
                    # Filter flip recommendations by confidence
                    high_confidence_flips = [
                        rec
                        for rec in flip_recommendations
                        if (rec.signal_strength * 100) >= min_confidence
                    ]
                    stored_recommendations.extend(high_confidence_flips)

                # Store all recommendations
                if stored_recommendations:
                    await recommendation_tracker.log_pre_game_recommendations(
                        stored_recommendations
                    )
                    click.echo("\n💾 RECOMMENDATIONS STORED TO POSTGRESQL")
                    click.echo(
                        f"   📊 Stored {len(stored_recommendations)} recommendations"
                    )

                    # Show breakdown by game
                    games_with_recommendations = {}
                    for rec in stored_recommendations:
                        game_key = f"{rec.away_team} @ {rec.home_team}"
                        if game_key not in games_with_recommendations:
                            games_with_recommendations[game_key] = []
                        games_with_recommendations[game_key].append(rec)

                    for game, recs in games_with_recommendations.items():
                        click.echo(f"   🎮 {game}: {len(recs)} recommendation(s)")
                        for rec in recs:
                            confidence_icon = (
                                "🔥"
                                if rec.confidence_level == "HIGH"
                                else "⭐"
                                if rec.confidence_level == "MODERATE"
                                else "💡"
                            )
                            click.echo(
                                f"      {confidence_icon} {rec.recommendation} ({rec.confidence_level} - {rec.signal_source})"
                            )
                else:
                    click.echo(
                        f"\n💾 No recommendations met {min_confidence}% confidence threshold for storage"
                    )

            except Exception as e:
                click.echo(f"⚠️  Warning: Failed to store recommendations: {e}")
                click.echo("   Detection results will still be displayed...")

        # Display results in specified format
        if format == "json":
            result_json = {
                "timestamp": datetime.now().isoformat(),
                "detection_results": results,
                "stored_recommendations": len(stored_recommendations)
                if stored_recommendations
                else 0,
                "recommendations": [
                    {
                        "game": f"{rec.away_team} @ {rec.home_team}",
                        "recommendation": rec.recommendation,
                        "confidence": rec.confidence_level,
                        "signal_source": rec.signal_source,
                        "signal_strength": rec.signal_strength,
                    }
                    for rec in stored_recommendations
                ]
                if stored_recommendations
                else [],
            }

            json_str = json.dumps(result_json, indent=2, default=str)
            if output:
                output.write_text(json_str)
                click.echo(f"✅ Results saved to: {output}")
            else:
                click.echo(json_str)
        else:
            # Console display handled by orchestrator
            pass

    except Exception as e:
        click.echo(f"❌ Error during detection: {e}", err=True)
        import traceback

        click.echo(f"Full traceback:\n{traceback.format_exc()}", err=True)

        click.echo("\n🔧 Troubleshooting:")
        click.echo("   • Check if PostgreSQL is running")
        click.echo("   • Verify all required database tables exist")
        click.echo("   • Try 'mlb-cli fix-schema --create-missing'")
        click.echo("   • Use --simple-mode for basic detection")

    # This function doesn't need to call itself


@detection_group.command("smart-pipeline")
@click.option(
    "--minutes",
    "-m",
    type=int,
    default=60,
    help="Minutes ahead to look for opportunities (default: 60)",
)
@click.option(
    "--force-fresh", is_flag=True, help="Force fresh data collection regardless of age"
)
@click.option(
    "--max-data-age",
    type=int,
    default=6,
    help="Maximum data age in hours before forcing collection (default: 6)",
)
def smart_pipeline(minutes: int, force_fresh: bool, max_data_age: int):
    """🧠 SMART PIPELINE - Automatically decides what needs to be run"""

    async def run_smart_pipeline():
        click.echo("🧠 SMART PIPELINE ORCHESTRATOR")
        click.echo("=" * 60)
        click.echo("🤖 Analyzing system state to determine optimal execution plan")

        try:
            orchestrator = PipelineOrchestrator()

            # Get recommendations first
            recommendations = await orchestrator.get_pipeline_recommendations()

            click.echo("\n📊 SYSTEM ANALYSIS:")
            click.echo(
                f"   🏥 System Health: {recommendations['system_health'].upper()}"
            )
            click.echo(
                f"   🚨 Priority Level: {recommendations['priority_level'].upper()}"
            )
            click.echo(
                f"   ⏱️  Estimated Runtime: {recommendations['estimated_runtime_minutes']} minutes"
            )

            # Show immediate actions
            if recommendations["immediate_actions"]:
                click.echo("\n🚀 IMMEDIATE ACTIONS PLANNED:")
                for action in recommendations["immediate_actions"]:
                    click.echo(f"   ✅ {action['action'].title()}: {action['reason']}")
                    click.echo(f"      ⏱️  Est. {action['estimated_minutes']} minutes")

            # Show optional actions
            if recommendations["optional_actions"]:
                click.echo("\n💡 OPTIONAL ACTIONS AVAILABLE:")
                for action in recommendations["optional_actions"]:
                    click.echo(f"   💡 {action['action'].title()}: {action['reason']}")

            # Execute smart pipeline
            click.echo("\n🚀 EXECUTING SMART PIPELINE...")

            results = await orchestrator.execute_smart_pipeline(
                detection_minutes=minutes, force_fresh_data=force_fresh
            )

            # Display results
            click.echo("\n✅ SMART PIPELINE COMPLETED")
            click.echo(f"   ⏱️  Execution Time: {results['total_execution_time']:.2f}s")
            click.echo(f"   🔧 Steps Executed: {', '.join(results['steps_executed'])}")

            if results["errors"]:
                click.echo(f"   ❌ Errors: {len(results['errors'])}")
                for error in results["errors"]:
                    click.echo(f"      • {error}")

            if results["warnings"]:
                click.echo(f"   ⚠️  Warnings: {len(results['warnings'])}")
                for warning in results["warnings"]:
                    click.echo(f"      • {warning}")

            # Show step-by-step results
            if "data_collection" in results["steps_executed"]:
                metrics = results["data_collection_metrics"]
                if metrics:
                    click.echo("\n📡 DATA COLLECTION RESULTS:")
                    click.echo(
                        f"   📥 Records Processed: {metrics.get('parsed_records', 0)}"
                    )
                    click.echo(
                        f"   🎯 Sharp Indicators: {metrics.get('sharp_indicators', 0)}"
                    )

            if "backtesting" in results["steps_executed"]:
                backtest = results["backtesting_results"]
                if backtest:
                    click.echo("\n🔬 BACKTESTING RESULTS:")
                    click.echo(
                        f"   📊 Strategies Analyzed: {backtest.total_strategies_analyzed}"
                    )
                    click.echo(
                        f"   💰 Profitable Strategies: {backtest.profitable_strategies}"
                    )

            if "detection" in results["steps_executed"]:
                detection = results["detection_results"]
                if detection:
                    total_opportunities = sum(
                        len(game_analysis.sharp_signals)
                        + len(game_analysis.opposing_markets)
                        + len(game_analysis.steam_moves)
                        + len(game_analysis.book_conflicts)
                        for game_analysis in detection.games.values()
                    )

                    click.echo("\n🎯 OPPORTUNITY DETECTION RESULTS:")
                    click.echo(f"   🎮 Games Analyzed: {len(detection.games)}")
                    click.echo(f"   🚨 Total Opportunities: {total_opportunities}")

                    if total_opportunities > 0:
                        click.echo(
                            "\n💡 Run 'mlb-cli detect opportunities' for detailed analysis"
                        )

            # Cross-market flips summary
            if results["cross_market_flips"]:
                flips = results["cross_market_flips"]["flips"]
                if flips:
                    click.echo("\n🔀 CROSS-MARKET FLIPS:")
                    click.echo(f"   🎯 High-Confidence Flips: {len(flips)}")
                    click.echo("   💡 Run 'mlb-cli cross-market-flips' for details")

            click.echo("\n🎉 Smart pipeline execution completed successfully!")

        except Exception as e:
            click.echo(f"❌ Smart pipeline failed: {e}")
            raise
        finally:
            # Cleanup
            try:
                if "orchestrator" in locals():
                    orchestrator.close()
            except Exception as cleanup_error:
                click.echo(f"⚠️  Cleanup warning: {cleanup_error}")

    try:
        asyncio.run(run_smart_pipeline())
    except KeyboardInterrupt:
        click.echo("\n⚠️  Smart pipeline interrupted by user")
    except Exception:
        click.echo("❌ Smart pipeline failed")
        raise


@detection_group.command("system-recommendations")
@click.option(
    "--minutes",
    "-m",
    type=int,
    default=60,
    help="Minutes ahead to look for opportunities (default: 60)",
)
def get_system_recommendations(minutes: int):
    """💡 Get intelligent system recommendations for what should be run"""

    async def show_recommendations():
        click.echo("💡 SYSTEM RECOMMENDATIONS")
        click.echo("=" * 50)

        try:
            orchestrator = PipelineOrchestrator()
            recommendations = await orchestrator.get_pipeline_recommendations()

            # System health
            health = recommendations["system_health"]
            health_emoji = {
                "excellent": "🟢",
                "good": "🟡",
                "fair": "🟠",
                "poor": "🔴",
                "unknown": "⚪",
            }.get(health, "⚪")

            click.echo(f"{health_emoji} System Health: {health.upper()}")
            click.echo(
                f"🚨 Priority Level: {recommendations['priority_level'].upper()}"
            )
            click.echo(
                f"⏱️  Estimated Runtime: {recommendations['estimated_runtime_minutes']} minutes"
            )

            # Immediate actions
            if recommendations["immediate_actions"]:
                click.echo("\n🚀 RECOMMENDED ACTIONS:")
                for i, action in enumerate(recommendations["immediate_actions"], 1):
                    click.echo(f"{i}. {action['action'].replace('_', ' ').title()}")
                    click.echo(f"   📝 Reason: {action['reason']}")
                    click.echo(f"   ⏱️  Time: ~{action['estimated_minutes']} minutes")
                    click.echo()

                click.echo(
                    "💡 Run 'mlb-cli detect smart-pipeline' to execute automatically"
                )
            else:
                click.echo("\n✅ No immediate actions needed")
                click.echo("💡 System is ready for detection")

            # Optional actions
            if recommendations["optional_actions"]:
                click.echo("\n💡 OPTIONAL IMPROVEMENTS:")
                for action in recommendations["optional_actions"]:
                    click.echo(
                        f"• {action['action'].replace('_', ' ').title()}: {action['reason']}"
                    )
                    if "issues" in action:
                        for issue in action["issues"]:
                            click.echo(f"    - {issue}")

            # Reasoning
            if recommendations["reasoning"]:
                click.echo("\n🧠 SYSTEM REASONING:")
                for reason in recommendations["reasoning"]:
                    click.echo(f"• {reason}")

        except Exception as e:
            click.echo(f"❌ Failed to get recommendations: {e}")
        finally:
            try:
                if "orchestrator" in locals():
                    orchestrator.close()
            except Exception:
                pass

    try:
        asyncio.run(show_recommendations())
    except Exception:
        click.echo("❌ Recommendations failed")
        raise


@detection_group.command("recommendations")
@click.option(
    "--minutes",
    "-m",
    type=int,
    default=240,
    help="Minutes ahead to look for opportunities (default: 240 = 4 hours)",
)
@click.option(
    "--min-confidence",
    type=float,
    default=70.0,
    help="Minimum confidence threshold for recommendations (default: 70%)",
)
@click.option(
    "--format",
    "-f",
    type=click.Choice(["console", "json"]),
    default="console",
    help="Output format (default: console)",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    help="Output file path for JSON format",
)
def betting_recommendations(
    minutes: int, min_confidence: float, format: str, output: Path | None
):
    """🎯 Generate actual betting recommendations with confidence scores and stake suggestions"""

    async def generate_recommendations():
        click.echo("🎯 MLB SHARP BETTING RECOMMENDATIONS")
        click.echo("=" * 60)
        click.echo(f"⏰ Looking {minutes} minutes ahead for betting opportunities")
        click.echo(f"📊 Minimum confidence threshold: {min_confidence}%")
        click.echo()

        try:
            # Initialize services
            config = SignalProcessorConfig()
            repository = BettingSignalRepository(config)

            # Get strategies and create validator
            strategies = await repository.get_profitable_strategies()
            if not strategies:
                click.echo(
                    "⚠️  No profitable strategies found - creating mock strategies"
                )
                from ...models.betting_analysis import ProfitableStrategy

                strategies = [
                    ProfitableStrategy(
                        strategy_name="VSIN_sharp_action",
                        source_book="VSIN-draftkings",
                        split_type="moneyline",
                        win_rate=68.0,
                        roi=22.0,
                        total_bets=18,
                        confidence="HIGH CONFIDENCE",
                    ),
                    ProfitableStrategy(
                        strategy_name="Public_fade_underdog",
                        source_book="CONSENSUS",
                        split_type="moneyline",
                        win_rate=63.0,
                        roi=18.0,
                        total_bets=15,
                        confidence="MODERATE CONFIDENCE",
                    ),
                ]

            from ...models.betting_analysis import StrategyThresholds

            thresholds = StrategyThresholds()
            validator = StrategyValidator(strategies, thresholds)

            # Create processor factory and generate signals
            factory = StrategyProcessorFactory(repository, validator, config)
            processors = factory.create_all_processors()

            click.echo(f"🔧 Created {len(processors)} betting processors")

            # Generate all signals
            all_signals = []
            for processor_name, processor in processors.items():
                try:
                    signals = await processor.process_with_error_handling(
                        minutes, strategies
                    )
                    if signals:
                        all_signals.extend(signals)
                        click.echo(f"   📊 {processor_name}: {len(signals)} signals")
                    else:
                        click.echo(f"   ℹ️  {processor_name}: No signals")
                except Exception as e:
                    click.echo(f"   ❌ {processor_name}: Failed - {e}")

            # Filter by confidence threshold
            high_confidence_signals = [
                signal
                for signal in all_signals
                if signal.confidence_score >= (min_confidence / 100.0)
            ]

            click.echo(f"\n📋 Generated {len(all_signals)} total signals")
            click.echo(
                f"🎯 {len(high_confidence_signals)} signals meet {min_confidence}% confidence threshold"
            )

            # Format and display recommendations
            formatter = BettingRecommendationFormatter()

            if format == "console":
                formatted_output = formatter.format_console_recommendations(
                    high_confidence_signals, min_confidence
                )
                click.echo(formatted_output)
            elif format == "json":
                json_output = formatter.format_json_recommendations(
                    high_confidence_signals, min_confidence
                )
                if output:
                    output.write_text(json.dumps(json_output, indent=2, default=str))
                    click.echo(f"✅ Recommendations saved to: {output}")
                else:
                    click.echo(json.dumps(json_output, indent=2, default=str))

        except Exception as e:
            click.echo(f"❌ Failed to generate betting recommendations: {e}")
            raise

    try:
        asyncio.run(generate_recommendations())
    except KeyboardInterrupt:
        click.echo("\n⚠️  Recommendation generation interrupted by user")
    except Exception:
        click.echo("❌ Recommendation generation failed")
        raise


@detection_group.command("test-storage")
@click.option(
    "--dry-run", is_flag=True, help="Test without actually storing to database"
)
def test_recommendation_storage(dry_run: bool):
    """🧪 Test the recommendation storage system"""

    async def run_test():
        click.echo("🧪 TESTING RECOMMENDATION STORAGE SYSTEM")
        click.echo("=" * 60)

        try:
            # Initialize tracker
            tracker = PreGameRecommendationTracker()
            click.echo("✅ Recommendation tracker initialized")

            # Create test recommendation
            test_rec = PreGameRecommendation(
                recommendation_id=f"test_{int(datetime.now().timestamp())}",
                game_pk=123456,
                home_team="TEST_HOME",
                away_team="TEST_AWAY",
                game_datetime=datetime.now() + timedelta(hours=2),
                recommendation="BET TEST_HOME ML",
                bet_type="moneyline",
                confidence_level="HIGH",
                signal_source="TEST_SIGNAL",
                signal_strength=0.85,
                recommended_at=datetime.now(),
                email_sent=False,
            )

            click.echo("📋 Created test recommendation:")
            click.echo(f"   🎮 Game: {test_rec.away_team} @ {test_rec.home_team}")
            click.echo(f"   🎯 Recommendation: {test_rec.recommendation}")
            click.echo(
                f"   📊 Confidence: {test_rec.confidence_level} ({test_rec.signal_strength * 100:.1f}%)"
            )
            click.echo(f"   🔍 Source: {test_rec.signal_source}")

            if not dry_run:
                # Store to database
                await tracker.log_pre_game_recommendations([test_rec])
                click.echo("✅ Test recommendation stored to PostgreSQL")

                # Verify storage
                with tracker.db_manager.get_cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT recommendation_id, recommendation, confidence_level, signal_source
                        FROM tracking.pre_game_recommendations 
                        WHERE recommendation_id = %s
                    """,
                        (test_rec.recommendation_id,),
                    )

                    result = cursor.fetchone()
                    if result:
                        click.echo("✅ Verification: Recommendation found in database")
                        click.echo(f"   ID: {result[0]}")
                        click.echo(f"   Text: {result[1]}")
                        click.echo(f"   Confidence: {result[2]}")
                        click.echo(f"   Source: {result[3]}")
                    else:
                        click.echo("❌ Verification failed: Recommendation not found")
            else:
                click.echo("🔍 DRY RUN: Would store recommendation to PostgreSQL")

            click.echo("\n🎉 Storage test completed successfully!")

        except Exception as e:
            click.echo(f"❌ Storage test failed: {e}")
            raise

    try:
        asyncio.run(run_test())
    except Exception:
        click.echo("❌ Test failed")
        raise


@detection_group.command("view-stored")
@click.option(
    "--hours-back",
    "-h",
    type=int,
    default=24,
    help="Hours back to look for stored recommendations (default: 24)",
)
@click.option("--game", "-g", help='Filter by specific game (e.g., "Yankees Pirates")')
@click.option(
    "--confidence",
    "-c",
    type=click.Choice(["HIGH", "MODERATE", "LOW"]),
    help="Filter by confidence level",
)
def view_stored_recommendations(
    hours_back: int, game: str | None, confidence: str | None
):
    """📋 View recommendations stored in PostgreSQL"""

    async def view_recommendations():
        click.echo("📋 STORED BETTING RECOMMENDATIONS")
        click.echo("=" * 60)
        click.echo(f"🔍 Looking back {hours_back} hours")
        if game:
            click.echo(f"🎮 Game filter: {game}")
        if confidence:
            click.echo(f"📊 Confidence filter: {confidence}")

        try:
            tracker = PreGameRecommendationTracker()

            # Build query
            query = """
                SELECT recommendation_id, game_pk, home_team, away_team, game_datetime,
                       recommendation, bet_type, confidence_level, signal_source, 
                       signal_strength, recommended_at, game_completed, bet_won, profit_loss
                FROM tracking.pre_game_recommendations
                WHERE recommended_at >= %s
            """
            params = [datetime.now() - timedelta(hours=hours_back)]

            if game:
                query += " AND (home_team ILIKE %s OR away_team ILIKE %s)"
                game_filter = f"%{game}%"
                params.extend([game_filter, game_filter])

            if confidence:
                query += " AND confidence_level = %s"
                params.append(confidence)

            query += " ORDER BY recommended_at DESC"

            with tracker.db_manager.get_cursor() as cursor:
                cursor.execute(query, params)
                recommendations = cursor.fetchall()

            if not recommendations:
                click.echo("❌ No stored recommendations found matching criteria")
                return

            click.echo(f"\n📊 Found {len(recommendations)} stored recommendations:")

            # Group by game for better display
            games = {}
            for rec in recommendations:
                game_key = f"{rec[3]} @ {rec[2]}"  # away @ home
                if game_key not in games:
                    games[game_key] = []
                games[game_key].append(rec)

            for game_key, recs in games.items():
                click.echo(f"\n🎮 {game_key}")
                click.echo("   " + "=" * 50)

                for rec in recs:
                    (
                        rec_id,
                        game_pk,
                        home_team,
                        away_team,
                        game_datetime,
                        recommendation,
                        bet_type,
                        conf_level,
                        signal_source,
                        signal_strength,
                        recommended_at,
                        game_completed,
                        bet_won,
                        profit_loss,
                    ) = rec

                    # Format timing
                    time_str = recommended_at.strftime("%Y-%m-%d %H:%M:%S EST")
                    game_time_str = game_datetime.strftime("%Y-%m-%d %H:%M EST")

                    # Confidence icon
                    conf_icon = (
                        "🔥"
                        if conf_level == "HIGH"
                        else "⭐"
                        if conf_level == "MODERATE"
                        else "💡"
                    )

                    # Status icon
                    if game_completed:
                        status_icon = (
                            "✅" if bet_won else "❌" if bet_won is False else "⏳"
                        )
                        status_text = (
                            f"Won (+${profit_loss:.2f})"
                            if bet_won
                            else f"Lost (-${abs(profit_loss):.2f})"
                            if bet_won is False
                            else "Pending"
                        )
                    else:
                        status_icon = "⏳"
                        status_text = "Pending"

                    click.echo(f"   {conf_icon} {recommendation}")
                    click.echo(f"      📅 Game: {game_time_str}")
                    click.echo(f"      ⏰ Recommended: {time_str}")
                    click.echo(
                        f"      📊 {conf_level} confidence ({signal_strength * 100:.1f}%) - {signal_source}"
                    )
                    click.echo(f"      {status_icon} Status: {status_text}")
                    click.echo(f"      🆔 ID: {rec_id}")
                    click.echo()

            # Summary statistics
            click.echo("📈 SUMMARY STATISTICS:")
            total_recs = len(recommendations)
            completed = [r for r in recommendations if r[11]]  # game_completed
            won = [r for r in completed if r[12] is True]  # bet_won
            lost = [r for r in completed if r[12] is False]

            click.echo(f"   📊 Total recommendations: {total_recs}")
            click.echo(f"   ✅ Completed games: {len(completed)}")
            if completed:
                win_rate = len(won) / len(completed) * 100
                total_profit = sum(r[13] for r in completed if r[13] is not None)
                click.echo(
                    f"   🎯 Win rate: {win_rate:.1f}% ({len(won)}/{len(completed)})"
                )
                click.echo(f"   💰 Total P&L: ${total_profit:+.2f}")

            # Confidence breakdown
            by_confidence = {}
            for rec in recommendations:
                conf = rec[7]  # confidence_level
                if conf not in by_confidence:
                    by_confidence[conf] = 0
                by_confidence[conf] += 1

            click.echo(f"   📊 By confidence: {dict(by_confidence)}")

        except Exception as e:
            click.echo(f"❌ Failed to view recommendations: {e}")
            raise

    try:
        asyncio.run(view_recommendations())
    except Exception:
        click.echo("❌ View failed")
        raise
