#!/usr/bin/env python3
"""
Daily Betting Performance Report CLI Command

This command evaluates the performance of yesterday's betting recommendations by
matching email alerts with actual game outcomes.

Features:
- Parse betting recommendations from pre-game emails
- Match recommendations to actual game outcomes
- Calculate win/loss rates and ROI metrics
- Support historical date analysis
- Multiple output formats (console, JSON, CSV)

Usage:
    uv run -m mlb_sharp_betting.cli betting-performance [options]

Examples:
    # Generate report for yesterday
    uv run -m mlb_sharp_betting.cli betting-performance

    # Historical analysis for specific date
    uv run -m mlb_sharp_betting.cli betting-performance --date 2024-01-15

    # Generate JSON output
    uv run -m mlb_sharp_betting.cli betting-performance --date 2024-01-15 --format json

    # Export to CSV
    uv run -m mlb_sharp_betting.cli betting-performance --format csv --output results.csv
"""

import asyncio
import csv
import json
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import click
import pytz
import structlog

from ...db.connection import get_db_manager
from ...services.pre_game_recommendation_tracker import PreGameRecommendationTracker
from ...services.team_mapper import TeamMapper

logger = structlog.get_logger(__name__)


@dataclass
class BettingPerformanceMetrics:
    """Performance metrics for betting recommendations."""

    # Date and summary
    report_date: date
    total_recommendations: int
    matched_recommendations: int
    unmatched_recommendations: int

    # Overall performance
    total_wins: int
    total_losses: int
    total_pushes: int
    win_percentage: float

    # Performance by bet type
    moneyline_record: str
    moneyline_win_pct: float
    spread_record: str
    spread_win_pct: float
    total_record: str
    total_win_pct: float

    # ROI Analysis
    total_risk_amount: float
    total_profit_loss: float
    roi_percentage: float

    # Performance by confidence level
    high_confidence_record: str
    moderate_confidence_record: str
    low_confidence_record: str

    # Timing analysis
    avg_minutes_before_game: float
    earliest_recommendation: int
    latest_recommendation: int


@dataclass
class RecommendationResult:
    """Individual recommendation with outcome."""

    game_id: str
    home_team: str
    away_team: str
    game_datetime: datetime
    recommendation: str
    bet_type: str
    confidence_level: str
    signal_source: str
    signal_strength: float
    minutes_before_game: int

    # Outcome data
    actual_result: str | None  # 'WIN', 'LOSS', 'PUSH', 'VOID'
    game_completed: bool
    home_score: int | None
    away_score: int | None
    profit_loss: float | None

    # Matching info
    match_quality: str  # 'EXACT', 'FUZZY', 'FAILED'
    match_notes: str


@click.group(name="betting-performance")
def betting_performance_group():
    """Daily betting performance report commands."""
    pass


@betting_performance_group.command("report")
@click.option(
    "--date", "-d", help="Date to analyze (YYYY-MM-DD format, default: yesterday)"
)
@click.option(
    "--format",
    "-f",
    type=click.Choice(["console", "json", "csv"]),
    default="console",
    help="Output format (default: console)",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    help="Output file path for JSON/CSV formats",
)
@click.option(
    "--include-unmatched",
    is_flag=True,
    help="Include unmatched recommendations in detailed output",
)
@click.option(
    "--min-confidence",
    type=click.Choice(["LOW", "MODERATE", "HIGH"]),
    help="Filter by minimum confidence level",
)
@click.option(
    "--bet-type",
    type=click.Choice(["moneyline", "spread", "total"]),
    help="Filter by bet type",
)
@click.option("--debug", is_flag=True, help="Enable debug logging")
def generate_performance_report(
    date: str | None,
    format: str,
    output: Path | None,
    include_unmatched: bool,
    min_confidence: str | None,
    bet_type: str | None,
    debug: bool,
):
    """Generate daily betting performance report."""

    # Configure logging
    if debug:
        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.dev.ConsoleRenderer(colors=True),
            ],
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )

    # Parse target date - default to yesterday
    if date:
        try:
            target_date = datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError:
            click.echo(
                f"❌ Invalid date format: {date}. Use YYYY-MM-DD format.", err=True
            )
            sys.exit(1)
    else:
        target_date = (datetime.now() - timedelta(days=1)).date()

    # Run the performance analysis
    try:
        report = asyncio.run(
            _generate_performance_report_async(
                target_date=target_date,
                min_confidence=min_confidence,
                bet_type=bet_type,
                include_unmatched=include_unmatched,
            )
        )

        # Output the report
        if format == "console":
            click.echo(_format_console_report(report))
        elif format == "json":
            json_output = json.dumps(asdict(report), indent=2, default=str)
            if output:
                output.write_text(json_output)
                click.echo(f"✅ JSON report saved to: {output}")
            else:
                click.echo(json_output)
        elif format == "csv":
            if not output:
                click.echo("❌ CSV format requires --output option", err=True)
                sys.exit(1)
            _save_csv_report(report, output)
            click.echo(f"✅ CSV report saved to: {output}")

    except Exception as e:
        click.echo(f"❌ Failed to generate performance report: {str(e)}", err=True)
        if debug:
            logger.exception("Performance report generation failed")
        sys.exit(1)


@betting_performance_group.command("summary")
@click.option(
    "--days", "-d", default=7, type=int, help="Number of days to analyze (default: 7)"
)
@click.option(
    "--format",
    "-f",
    type=click.Choice(["console", "json"]),
    default="console",
    help="Output format (default: console)",
)
def performance_summary(days: int, format: str):
    """Generate multi-day performance summary."""

    try:
        summary = asyncio.run(_generate_performance_summary_async(days))

        if format == "console":
            click.echo(_format_summary_console(summary))
        else:
            click.echo(json.dumps(summary, indent=2, default=str))

    except Exception as e:
        click.echo(f"❌ Failed to generate summary: {str(e)}", err=True)
        sys.exit(1)


async def _generate_performance_report_async(
    target_date: date,
    min_confidence: str | None = None,
    bet_type: str | None = None,
    include_unmatched: bool = False,
) -> BettingPerformanceMetrics:
    """Generate the performance report asynchronously."""

    logger.info("Generating betting performance report", date=target_date.isoformat())

    # Initialize services
    tracker = PreGameRecommendationTracker()
    db_manager = get_db_manager()

    # Get recommendations for the target date
    recommendations = await _get_recommendations_for_date(tracker, target_date)

    if not recommendations:
        logger.warning(
            "No betting recommendations found for date", date=target_date.isoformat()
        )
        return _create_empty_metrics(target_date)

    # Filter recommendations if requested
    if min_confidence:
        recommendations = [
            r
            for r in recommendations
            if _confidence_meets_minimum(r.confidence_level, min_confidence)
        ]

    if bet_type:
        recommendations = [r for r in recommendations if r.bet_type == bet_type]

    # Match recommendations with game outcomes
    matched_results = await _match_recommendations_with_outcomes(
        recommendations, target_date
    )

    # Calculate performance metrics
    metrics = _calculate_performance_metrics(matched_results, target_date)

    logger.info(
        "Performance report generated",
        date=target_date.isoformat(),
        total_recommendations=len(recommendations),
        matched_recommendations=metrics.matched_recommendations,
    )

    return metrics


async def _get_recommendations_for_date(
    tracker: PreGameRecommendationTracker, target_date: date
) -> list[RecommendationResult]:
    """Get all betting recommendations for a specific date."""

    try:
        with tracker.db_manager.get_cursor() as cursor:
            # Get recommendations from the tracking table
            cursor.execute(
                """
                SELECT recommendation_id, game_pk, home_team, away_team, game_datetime,
                       recommendation, bet_type, confidence_level, signal_source, signal_strength,
                       recommended_at, email_sent, game_completed, bet_won, actual_outcome, 
                       profit_loss, created_at, updated_at
                FROM tracking.pre_game_recommendations
                WHERE DATE(game_datetime) = %s
                ORDER BY game_datetime, recommended_at
            """,
                (target_date,),
            )

            rows = cursor.fetchall()

            recommendations = []
            for row in rows:
                # Calculate minutes before game
                game_dt = row["game_datetime"]
                recommended_at = row["recommended_at"]

                if isinstance(game_dt, str):
                    game_dt = datetime.fromisoformat(game_dt.replace("Z", "+00:00"))
                if isinstance(recommended_at, str):
                    recommended_at = datetime.fromisoformat(
                        recommended_at.replace("Z", "+00:00")
                    )

                if game_dt.tzinfo is None:
                    game_dt = pytz.UTC.localize(game_dt)
                if recommended_at.tzinfo is None:
                    recommended_at = pytz.UTC.localize(recommended_at)

                minutes_before = int((game_dt - recommended_at).total_seconds() / 60)

                rec = RecommendationResult(
                    game_id=row["game_pk"],
                    home_team=row["home_team"],
                    away_team=row["away_team"],
                    game_datetime=game_dt,
                    recommendation=row["recommendation"],
                    bet_type=row["bet_type"],
                    confidence_level=row["confidence_level"],
                    signal_source=row["signal_source"],
                    signal_strength=row["signal_strength"] or 0.0,
                    minutes_before_game=minutes_before,
                    actual_result=row["actual_outcome"],
                    game_completed=bool(row["game_completed"]),
                    home_score=None,  # Will be filled from game outcomes
                    away_score=None,  # Will be filled from game outcomes
                    profit_loss=row["profit_loss"],
                    match_quality="PENDING",
                    match_notes="",
                )

                recommendations.append(rec)

            return recommendations

    except Exception as e:
        logger.error("Failed to get recommendations", error=str(e))
        return []


async def _match_recommendations_with_outcomes(
    recommendations: list[RecommendationResult], target_date: date
) -> list[RecommendationResult]:
    """Match recommendations with actual game outcomes."""

    db_manager = get_db_manager()

    try:
        # Get game outcomes for the target date
        with db_manager.get_cursor() as cursor:
            cursor.execute(
                """
                SELECT game_id, home_team, away_team, home_score, away_score,
                       over, home_win, home_cover_spread, total_line, home_spread_line,
                       game_date, created_at, updated_at
                FROM game_outcomes
                WHERE DATE(game_date) = %s
            """,
                (target_date,),
            )

            outcomes = cursor.fetchall()
            outcome_map = {outcome["game_id"]: outcome for outcome in outcomes}

    except Exception as e:
        logger.error("Failed to get game outcomes", error=str(e))
        outcome_map = {}

    # Match recommendations with outcomes
    matched_results = []

    for rec in recommendations:
        matched_rec = rec

        # Try exact game ID match first
        if rec.game_id in outcome_map:
            outcome = outcome_map[rec.game_id]
            matched_rec = _apply_outcome_to_recommendation(rec, outcome)
            matched_rec.match_quality = "EXACT"
            matched_rec.match_notes = "Matched by game ID"
        else:
            # Try fuzzy matching by team names and date
            fuzzy_match = _find_fuzzy_match(rec, outcome_map)
            if fuzzy_match:
                matched_rec = _apply_outcome_to_recommendation(rec, fuzzy_match)
                matched_rec.match_quality = "FUZZY"
                matched_rec.match_notes = "Matched by team names and date"
            else:
                matched_rec.match_quality = "FAILED"
                matched_rec.match_notes = "No matching game outcome found"

        matched_results.append(matched_rec)

    return matched_results


def _apply_outcome_to_recommendation(
    rec: RecommendationResult, outcome: dict[str, Any]
) -> RecommendationResult:
    """Apply game outcome to recommendation result."""

    # Update basic game info
    rec.home_score = outcome["home_score"]
    rec.away_score = outcome["away_score"]
    rec.game_completed = True

    # Determine bet result based on recommendation
    bet_result = _determine_bet_result(rec, outcome)
    rec.actual_result = bet_result

    # Calculate profit/loss (assuming $100 bet at -110 odds for simplicity)
    if bet_result == "WIN":
        rec.profit_loss = 90.91  # Win $90.91 on $100 bet at -110
    elif bet_result == "LOSS":
        rec.profit_loss = -100.0  # Lose $100
    elif bet_result == "PUSH":
        rec.profit_loss = 0.0  # No money won or lost
    else:  # VOID
        rec.profit_loss = 0.0

    return rec


def _determine_bet_result(rec: RecommendationResult, outcome: dict[str, Any]) -> str:
    """Determine if the bet won, lost, or pushed."""

    recommendation = rec.recommendation.upper()
    bet_type = rec.bet_type

    try:
        if bet_type == "moneyline":
            if "BET " + rec.home_team.upper() in recommendation:
                return "WIN" if outcome["home_win"] else "LOSS"
            elif "BET " + rec.away_team.upper() in recommendation:
                return "WIN" if not outcome["home_win"] else "LOSS"

        elif bet_type == "total":
            if "BET OVER" in recommendation:
                return "WIN" if outcome["over"] else "LOSS"
            elif "BET UNDER" in recommendation:
                return "WIN" if not outcome["over"] else "LOSS"

        elif bet_type == "spread":
            if "BET " + rec.home_team.upper() in recommendation:
                if outcome["home_cover_spread"] is not None:
                    return "WIN" if outcome["home_cover_spread"] else "LOSS"
            elif "BET " + rec.away_team.upper() in recommendation:
                if outcome["home_cover_spread"] is not None:
                    return "WIN" if not outcome["home_cover_spread"] else "LOSS"

        # If we can't determine the result, mark as void
        return "VOID"

    except Exception as e:
        logger.warning(
            "Failed to determine bet result",
            recommendation=recommendation,
            bet_type=bet_type,
            error=str(e),
        )
        return "VOID"


def _find_fuzzy_match(
    rec: RecommendationResult, outcome_map: dict[str, dict[str, Any]]
) -> dict[str, Any] | None:
    """Find a fuzzy match for recommendation using team names."""

    # Map team names using TeamMapper
    home_team_enum = TeamMapper.map_team_name(rec.home_team)
    away_team_enum = TeamMapper.map_team_name(rec.away_team)

    if not home_team_enum or not away_team_enum:
        return None

    # Look for matching teams in outcomes
    for outcome in outcome_map.values():
        outcome_home = TeamMapper.map_team_name(outcome["home_team"])
        outcome_away = TeamMapper.map_team_name(outcome["away_team"])

        if outcome_home == home_team_enum and outcome_away == away_team_enum:
            return outcome

    return None


def _calculate_performance_metrics(
    results: list[RecommendationResult], target_date: date
) -> BettingPerformanceMetrics:
    """Calculate comprehensive performance metrics."""

    # Filter to only matched results with outcomes
    matched_results = [
        r for r in results if r.match_quality in ["EXACT", "FUZZY"] and r.actual_result
    ]

    total_wins = sum(1 for r in matched_results if r.actual_result == "WIN")
    total_losses = sum(1 for r in matched_results if r.actual_result == "LOSS")
    total_pushes = sum(1 for r in matched_results if r.actual_result == "PUSH")

    win_percentage = (
        (total_wins / len(matched_results) * 100) if matched_results else 0.0
    )

    # Performance by bet type
    ml_results = [r for r in matched_results if r.bet_type == "moneyline"]
    ml_wins = sum(1 for r in ml_results if r.actual_result == "WIN")
    ml_losses = sum(1 for r in ml_results if r.actual_result == "LOSS")
    ml_win_pct = (ml_wins / len(ml_results) * 100) if ml_results else 0.0

    spread_results = [r for r in matched_results if r.bet_type == "spread"]
    spread_wins = sum(1 for r in spread_results if r.actual_result == "WIN")
    spread_losses = sum(1 for r in spread_results if r.actual_result == "LOSS")
    spread_win_pct = (
        (spread_wins / len(spread_results) * 100) if spread_results else 0.0
    )

    total_results = [r for r in matched_results if r.bet_type == "total"]
    total_wins_bt = sum(1 for r in total_results if r.actual_result == "WIN")
    total_losses_bt = sum(1 for r in total_results if r.actual_result == "LOSS")
    total_win_pct = (total_wins_bt / len(total_results) * 100) if total_results else 0.0

    # ROI calculations
    total_risk = len(matched_results) * 100.0  # $100 per bet
    total_pnl = sum(r.profit_loss or 0.0 for r in matched_results)
    roi_pct = (total_pnl / total_risk * 100) if total_risk > 0 else 0.0

    # Performance by confidence
    high_conf = [r for r in matched_results if r.confidence_level == "HIGH"]
    high_wins = sum(1 for r in high_conf if r.actual_result == "WIN")
    high_losses = sum(1 for r in high_conf if r.actual_result == "LOSS")

    mod_conf = [r for r in matched_results if r.confidence_level == "MODERATE"]
    mod_wins = sum(1 for r in mod_conf if r.actual_result == "WIN")
    mod_losses = sum(1 for r in mod_conf if r.actual_result == "LOSS")

    low_conf = [r for r in matched_results if r.confidence_level == "LOW"]
    low_wins = sum(1 for r in low_conf if r.actual_result == "WIN")
    low_losses = sum(1 for r in low_conf if r.actual_result == "LOSS")

    # Timing analysis
    minutes_before = [
        r.minutes_before_game for r in results if r.minutes_before_game is not None
    ]
    avg_minutes = sum(minutes_before) / len(minutes_before) if minutes_before else 0.0
    earliest = min(minutes_before) if minutes_before else 0
    latest = max(minutes_before) if minutes_before else 0

    return BettingPerformanceMetrics(
        report_date=target_date,
        total_recommendations=len(results),
        matched_recommendations=len(matched_results),
        unmatched_recommendations=len(results) - len(matched_results),
        total_wins=total_wins,
        total_losses=total_losses,
        total_pushes=total_pushes,
        win_percentage=win_percentage,
        moneyline_record=f"{ml_wins}W-{ml_losses}L",
        moneyline_win_pct=ml_win_pct,
        spread_record=f"{spread_wins}W-{spread_losses}L",
        spread_win_pct=spread_win_pct,
        total_record=f"{total_wins_bt}W-{total_losses_bt}L",
        total_win_pct=total_win_pct,
        total_risk_amount=total_risk,
        total_profit_loss=total_pnl,
        roi_percentage=roi_pct,
        high_confidence_record=f"{high_wins}W-{high_losses}L",
        moderate_confidence_record=f"{mod_wins}W-{mod_losses}L",
        low_confidence_record=f"{low_wins}W-{low_losses}L",
        avg_minutes_before_game=avg_minutes,
        earliest_recommendation=earliest,
        latest_recommendation=latest,
    )


def _create_empty_metrics(target_date: date) -> BettingPerformanceMetrics:
    """Create empty metrics when no data is found."""
    return BettingPerformanceMetrics(
        report_date=target_date,
        total_recommendations=0,
        matched_recommendations=0,
        unmatched_recommendations=0,
        total_wins=0,
        total_losses=0,
        total_pushes=0,
        win_percentage=0.0,
        moneyline_record="0W-0L",
        moneyline_win_pct=0.0,
        spread_record="0W-0L",
        spread_win_pct=0.0,
        total_record="0W-0L",
        total_win_pct=0.0,
        total_risk_amount=0.0,
        total_profit_loss=0.0,
        roi_percentage=0.0,
        high_confidence_record="0W-0L",
        moderate_confidence_record="0W-0L",
        low_confidence_record="0W-0L",
        avg_minutes_before_game=0.0,
        earliest_recommendation=0,
        latest_recommendation=0,
    )


def _confidence_meets_minimum(confidence: str, minimum: str) -> bool:
    """Check if confidence level meets minimum threshold."""
    confidence_order = {"LOW": 1, "MODERATE": 2, "HIGH": 3}
    return confidence_order.get(confidence, 0) >= confidence_order.get(minimum, 0)


def _format_console_report(metrics: BettingPerformanceMetrics) -> str:
    """Format metrics for console output."""

    date_str = metrics.report_date.strftime("%B %d, %Y")

    report = f"""📊 Daily Betting Performance Report - {date_str}

📧 Email Recommendations Found: {metrics.total_recommendations}
🎯 Successfully Matched: {metrics.matched_recommendations}
❌ Unmatched: {metrics.unmatched_recommendations}

📈 Performance Summary:
- Overall Record: {metrics.total_wins}W-{metrics.total_losses}L-{metrics.total_pushes}P ({metrics.win_percentage:.1f}%)
- Moneyline: {metrics.moneyline_record} ({metrics.moneyline_win_pct:.1f}%)
- Spreads: {metrics.spread_record} ({metrics.spread_win_pct:.1f}%)
- Totals: {metrics.total_record} ({metrics.total_win_pct:.1f}%)

💰 ROI Analysis:
- Total Risk: ${metrics.total_risk_amount:,.2f}
- Profit/Loss: ${metrics.total_profit_loss:+,.2f}
- ROI: {metrics.roi_percentage:+.2f}%

🔍 Confidence Level Performance:
- High Confidence: {metrics.high_confidence_record}
- Moderate Confidence: {metrics.moderate_confidence_record}
- Low Confidence: {metrics.low_confidence_record}

⏰ Timing Analysis:
- Average Time Before Game: {metrics.avg_minutes_before_game:.0f} minutes
- Earliest Recommendation: {metrics.earliest_recommendation} minutes before
- Latest Recommendation: {metrics.latest_recommendation} minutes before

---
General Balls"""

    return report


def _save_csv_report(metrics: BettingPerformanceMetrics, output_path: Path) -> None:
    """Save performance metrics to CSV file."""

    with open(output_path, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)

        # Write header
        writer.writerow(
            [
                "Date",
                "Total_Recommendations",
                "Matched_Recommendations",
                "Unmatched_Recommendations",
                "Total_Wins",
                "Total_Losses",
                "Total_Pushes",
                "Win_Percentage",
                "Moneyline_Record",
                "Moneyline_Win_Pct",
                "Spread_Record",
                "Spread_Win_Pct",
                "Total_Record",
                "Total_Win_Pct",
                "Total_Risk_Amount",
                "Total_Profit_Loss",
                "ROI_Percentage",
                "High_Conf_Record",
                "Moderate_Conf_Record",
                "Low_Conf_Record",
                "Avg_Minutes_Before_Game",
                "Earliest_Recommendation",
                "Latest_Recommendation",
            ]
        )

        # Write data
        writer.writerow(
            [
                metrics.report_date.isoformat(),
                metrics.total_recommendations,
                metrics.matched_recommendations,
                metrics.unmatched_recommendations,
                metrics.total_wins,
                metrics.total_losses,
                metrics.total_pushes,
                metrics.win_percentage,
                metrics.moneyline_record,
                metrics.moneyline_win_pct,
                metrics.spread_record,
                metrics.spread_win_pct,
                metrics.total_record,
                metrics.total_win_pct,
                metrics.total_risk_amount,
                metrics.total_profit_loss,
                metrics.roi_percentage,
                metrics.high_confidence_record,
                metrics.moderate_confidence_record,
                metrics.low_confidence_record,
                metrics.avg_minutes_before_game,
                metrics.earliest_recommendation,
                metrics.latest_recommendation,
            ]
        )


async def _generate_performance_summary_async(days: int) -> dict[str, Any]:
    """Generate multi-day performance summary."""

    # This is a placeholder for the summary functionality
    # Would implement similar logic but aggregate across multiple days
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days - 1)

    return {
        "period": f"{start_date.isoformat()} to {end_date.isoformat()}",
        "days_analyzed": days,
        "message": "Multi-day summary functionality to be implemented",
        "note": "Use individual daily reports for now",
    }


def _format_summary_console(summary: dict[str, Any]) -> str:
    """Format summary for console output."""
    return f"""📊 {summary["days_analyzed"]}-Day Performance Summary

Period: {summary["period"]}

{summary["message"]}

💡 {summary["note"]}

---
General Balls"""


def main():
    """Main CLI entry point."""
    betting_performance_group()


if __name__ == "__main__":
    main()
