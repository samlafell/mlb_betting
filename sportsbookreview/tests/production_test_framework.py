"""
Production Testing Framework for SportsbookReview MLB Integration

This module provides comprehensive testing capabilities for:
1. Current season MLB data validation
2. Real-time API integration testing
3. Data quality and consistency checks
4. Performance and reliability monitoring
"""

import asyncio
import json
import logging
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from ..models.game import EnhancedGame, GameStatus, GameType, Team
from ..services.mlb_api_service import MLBAPIService

logger = logging.getLogger(__name__)


@dataclass
class TestResult:
    """Test result with details"""

    test_name: str
    passed: bool
    message: str
    details: dict[str, Any] | None = None
    execution_time: float | None = None


@dataclass
class TestSuite:
    """Collection of test results"""

    name: str
    results: list[TestResult]
    start_time: datetime
    end_time: datetime | None = None

    @property
    def total_tests(self) -> int:
        return len(self.results)

    @property
    def passed_tests(self) -> int:
        return sum(1 for r in self.results if r.passed)

    @property
    def failed_tests(self) -> int:
        return self.total_tests - self.passed_tests

    @property
    def success_rate(self) -> float:
        if self.total_tests == 0:
            return 0.0
        return self.passed_tests / self.total_tests

    @property
    def duration(self) -> float | None:
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None


class ProductionTestFramework:
    """
    Production testing framework for MLB data integration.

    Provides comprehensive testing of:
    - MLB API connectivity and data quality
    - Game correlation accuracy
    - Data storage and retrieval
    - Performance benchmarks
    - Error handling and recovery
    """

    def __init__(self, output_dir: str = "test_output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.test_suites: list[TestSuite] = []

    async def run_all_tests(self) -> dict[str, TestSuite]:
        """Run all production tests and return results"""
        logger.info("🚀 Starting Production Test Framework")

        # Test suites to run
        test_suites = {
            "mlb_api": await self.test_mlb_api_integration(),
            "data_correlation": await self.test_game_correlation(),
            "data_storage": await self.test_data_storage(),
            "performance": await self.test_performance_benchmarks(),
            "error_handling": await self.test_error_handling(),
            "current_season": await self.test_current_season_data(),
        }

        # Generate comprehensive report
        await self.generate_test_report(test_suites)

        return test_suites

    async def test_mlb_api_integration(self) -> TestSuite:
        """Test MLB API integration functionality"""
        suite = TestSuite("MLB API Integration", [], datetime.now())

        try:
            async with MLBAPIService() as mlb_service:
                # Test 1: API Connectivity
                start_time = datetime.now()
                try:
                    today = date.today()
                    games = await mlb_service.get_games_for_date(today)
                    execution_time = (datetime.now() - start_time).total_seconds()

                    suite.results.append(
                        TestResult(
                            test_name="API Connectivity",
                            passed=True,
                            message=f"Successfully connected to MLB API, found {len(games)} games",
                            details={
                                "games_found": len(games),
                                "test_date": str(today),
                            },
                            execution_time=execution_time,
                        )
                    )
                except Exception as e:
                    suite.results.append(
                        TestResult(
                            test_name="API Connectivity",
                            passed=False,
                            message=f"Failed to connect to MLB API: {e}",
                            details={"error": str(e)},
                        )
                    )

                # Test 2: Data Quality Validation
                start_time = datetime.now()
                try:
                    # Test multiple recent dates
                    quality_issues = []
                    total_games = 0

                    for days_back in range(7):  # Test last 7 days
                        test_date = today - timedelta(days=days_back)
                        games = await mlb_service.get_games_for_date(test_date)
                        total_games += len(games)

                        for game in games:
                            # Validate required fields
                            if not game.game_pk:
                                quality_issues.append(
                                    f"Missing game_pk for {test_date}"
                                )
                            if not game.home_team_name or not game.away_team_name:
                                quality_issues.append(
                                    f"Missing team names for game {game.game_pk}"
                                )
                            if not game.game_datetime:
                                quality_issues.append(
                                    f"Missing datetime for game {game.game_pk}"
                                )
                            if game.home_team_name == game.away_team_name:
                                quality_issues.append(
                                    f"Same home/away team for game {game.game_pk}"
                                )

                    execution_time = (datetime.now() - start_time).total_seconds()

                    if not quality_issues:
                        suite.results.append(
                            TestResult(
                                test_name="Data Quality Validation",
                                passed=True,
                                message=f"All {total_games} games passed quality checks",
                                details={
                                    "total_games_checked": total_games,
                                    "days_checked": 7,
                                },
                                execution_time=execution_time,
                            )
                        )
                    else:
                        suite.results.append(
                            TestResult(
                                test_name="Data Quality Validation",
                                passed=False,
                                message=f"Found {len(quality_issues)} quality issues",
                                details={
                                    "issues": quality_issues[:10],
                                    "total_issues": len(quality_issues),
                                },
                                execution_time=execution_time,
                            )
                        )

                except Exception as e:
                    suite.results.append(
                        TestResult(
                            test_name="Data Quality Validation",
                            passed=False,
                            message=f"Error during quality validation: {e}",
                            details={"error": str(e)},
                        )
                    )

                # Test 3: Caching Performance
                start_time = datetime.now()
                try:
                    test_date = today

                    # First call (should hit API)
                    first_call_start = datetime.now()
                    games1 = await mlb_service.get_games_for_date(test_date)
                    first_call_time = (
                        datetime.now() - first_call_start
                    ).total_seconds()

                    # Second call (should hit cache)
                    second_call_start = datetime.now()
                    games2 = await mlb_service.get_games_for_date(test_date)
                    second_call_time = (
                        datetime.now() - second_call_start
                    ).total_seconds()

                    cache_improvement = (
                        first_call_time / second_call_time
                        if second_call_time > 0
                        else float("inf")
                    )

                    suite.results.append(
                        TestResult(
                            test_name="Caching Performance",
                            passed=cache_improvement
                            > 10,  # Cache should be at least 10x faster
                            message=f"Cache improvement: {cache_improvement:.1f}x faster",
                            details={
                                "first_call_time": first_call_time,
                                "second_call_time": second_call_time,
                                "improvement_factor": cache_improvement,
                                "games_consistent": len(games1) == len(games2),
                            },
                            execution_time=(
                                datetime.now() - start_time
                            ).total_seconds(),
                        )
                    )

                except Exception as e:
                    suite.results.append(
                        TestResult(
                            test_name="Caching Performance",
                            passed=False,
                            message=f"Error testing cache performance: {e}",
                            details={"error": str(e)},
                        )
                    )

                # Test 4: Cache Statistics
                try:
                    cache_stats = mlb_service.get_cache_stats()

                    suite.results.append(
                        TestResult(
                            test_name="Cache Statistics",
                            passed=True,
                            message="Cache statistics retrieved successfully",
                            details=cache_stats,
                        )
                    )

                except Exception as e:
                    suite.results.append(
                        TestResult(
                            test_name="Cache Statistics",
                            passed=False,
                            message=f"Error retrieving cache stats: {e}",
                            details={"error": str(e)},
                        )
                    )

        except Exception as e:
            suite.results.append(
                TestResult(
                    test_name="MLB API Service Initialization",
                    passed=False,
                    message=f"Failed to initialize MLB API service: {e}",
                    details={"error": str(e)},
                )
            )

        suite.end_time = datetime.now()
        return suite

    async def test_game_correlation(self) -> TestSuite:
        """Test game correlation between SBR and MLB data"""
        suite = TestSuite("Game Correlation", [], datetime.now())

        try:
            async with MLBAPIService() as mlb_service:
                # Test correlation with known team matchups
                today = date.today()

                # Get MLB games for correlation testing
                for days_back in range(7):
                    test_date = today - timedelta(days=days_back)
                    mlb_games = await mlb_service.get_games_for_date(test_date)

                    if mlb_games:
                        # Test correlation with first few games
                        for game in mlb_games[:3]:
                            start_time = datetime.now()

                            try:
                                correlation = await mlb_service.correlate_game(
                                    home_team=game.home_team_name,
                                    away_team=game.away_team_name,
                                    game_datetime=game.game_datetime,
                                )

                                execution_time = (
                                    datetime.now() - start_time
                                ).total_seconds()

                                # Should find perfect match for exact data
                                if correlation.confidence >= 0.9:
                                    suite.results.append(
                                        TestResult(
                                            test_name=f"Perfect Correlation - {game.away_team_name}@{game.home_team_name}",
                                            passed=True,
                                            message=f"High confidence correlation: {correlation.confidence:.2f}",
                                            details={
                                                "confidence": correlation.confidence,
                                                "match_reasons": correlation.match_reasons,
                                                "mlb_game_pk": game.game_pk,
                                            },
                                            execution_time=execution_time,
                                        )
                                    )
                                else:
                                    suite.results.append(
                                        TestResult(
                                            test_name=f"Perfect Correlation - {game.away_team_name}@{game.home_team_name}",
                                            passed=False,
                                            message=f"Low confidence correlation: {correlation.confidence:.2f}",
                                            details={
                                                "confidence": correlation.confidence,
                                                "match_reasons": correlation.match_reasons,
                                            },
                                            execution_time=execution_time,
                                        )
                                    )

                            except Exception as e:
                                suite.results.append(
                                    TestResult(
                                        test_name=f"Correlation Error - {game.away_team_name}@{game.home_team_name}",
                                        passed=False,
                                        message=f"Correlation failed: {e}",
                                        details={"error": str(e)},
                                    )
                                )

                        break  # Only test one day with games

                if not suite.results:
                    suite.results.append(
                        TestResult(
                            test_name="Game Correlation",
                            passed=False,
                            message="No MLB games found for correlation testing",
                            details={"days_checked": 7},
                        )
                    )

        except Exception as e:
            suite.results.append(
                TestResult(
                    test_name="Game Correlation Setup",
                    passed=False,
                    message=f"Failed to setup correlation tests: {e}",
                    details={"error": str(e)},
                )
            )

        suite.end_time = datetime.now()
        return suite

    async def test_data_storage(self) -> TestSuite:
        """Test data storage functionality"""
        suite = TestSuite("Data Storage", [], datetime.now())

        try:
            # Test creating sample enhanced game
            start_time = datetime.now()

            sample_game = EnhancedGame(
                sbr_game_id="test-production-game",
                home_team=Team.NYY,
                away_team=Team.BOS,
                game_datetime=datetime.now(),
                game_type=GameType.REGULAR,
                game_status=GameStatus.SCHEDULED,
            )

            execution_time = (datetime.now() - start_time).total_seconds()

            suite.results.append(
                TestResult(
                    test_name="Enhanced Game Creation",
                    passed=True,
                    message="Successfully created enhanced game model",
                    details={
                        "game_id": sample_game.sbr_game_id,
                        "correlation_key": sample_game.get_correlation_key(),
                        "matchup": sample_game.matchup_display,
                    },
                    execution_time=execution_time,
                )
            )

            # Test model validation
            start_time = datetime.now()

            try:
                # Test invalid game (same home/away team)
                invalid_game = EnhancedGame(
                    sbr_game_id="test-invalid-game",
                    home_team=Team.NYY,
                    away_team=Team.NYY,  # Same as home team
                    game_datetime=datetime.now(),
                    game_type=GameType.REGULAR,
                    game_status=GameStatus.SCHEDULED,
                )

                suite.results.append(
                    TestResult(
                        test_name="Model Validation",
                        passed=False,
                        message="Model validation failed to catch invalid data",
                        details={"issue": "Same home/away team allowed"},
                    )
                )

            except Exception as e:
                # This is expected - validation should catch the error
                suite.results.append(
                    TestResult(
                        test_name="Model Validation",
                        passed=True,
                        message="Model validation correctly caught invalid data",
                        details={"validation_error": str(e)},
                        execution_time=(datetime.now() - start_time).total_seconds(),
                    )
                )

        except Exception as e:
            suite.results.append(
                TestResult(
                    test_name="Data Storage Setup",
                    passed=False,
                    message=f"Failed to setup storage tests: {e}",
                    details={"error": str(e)},
                )
            )

        suite.end_time = datetime.now()
        return suite

    async def test_performance_benchmarks(self) -> TestSuite:
        """Test performance benchmarks"""
        suite = TestSuite("Performance Benchmarks", [], datetime.now())

        try:
            async with MLBAPIService() as mlb_service:
                # Benchmark 1: API Response Time
                start_time = datetime.now()

                today = date.today()
                games = await mlb_service.get_games_for_date(today)
                api_response_time = (datetime.now() - start_time).total_seconds()

                # API should respond within 5 seconds
                suite.results.append(
                    TestResult(
                        test_name="API Response Time",
                        passed=api_response_time < 5.0,
                        message=f"API responded in {api_response_time:.2f} seconds",
                        details={
                            "response_time": api_response_time,
                            "games_returned": len(games),
                            "threshold": 5.0,
                        },
                        execution_time=api_response_time,
                    )
                )

                # Benchmark 2: Bulk Correlation Performance
                if games:
                    start_time = datetime.now()
                    correlation_count = 0

                    for game in games[:5]:  # Test first 5 games
                        await mlb_service.correlate_game(
                            home_team=game.home_team_name,
                            away_team=game.away_team_name,
                            game_datetime=game.game_datetime,
                        )
                        correlation_count += 1

                    bulk_correlation_time = (
                        datetime.now() - start_time
                    ).total_seconds()
                    avg_time_per_correlation = (
                        bulk_correlation_time / correlation_count
                        if correlation_count > 0
                        else 0
                    )

                    # Each correlation should take less than 1 second
                    suite.results.append(
                        TestResult(
                            test_name="Bulk Correlation Performance",
                            passed=avg_time_per_correlation < 1.0,
                            message=f"Average correlation time: {avg_time_per_correlation:.3f}s",
                            details={
                                "total_time": bulk_correlation_time,
                                "correlations": correlation_count,
                                "avg_time": avg_time_per_correlation,
                                "threshold": 1.0,
                            },
                            execution_time=bulk_correlation_time,
                        )
                    )

                # Benchmark 3: Cache Hit Rate
                cache_stats = mlb_service.get_cache_stats()

                suite.results.append(
                    TestResult(
                        test_name="Cache Performance",
                        passed=True,
                        message="Cache statistics collected",
                        details=cache_stats,
                    )
                )

        except Exception as e:
            suite.results.append(
                TestResult(
                    test_name="Performance Benchmark Setup",
                    passed=False,
                    message=f"Failed to setup performance tests: {e}",
                    details={"error": str(e)},
                )
            )

        suite.end_time = datetime.now()
        return suite

    async def test_error_handling(self) -> TestSuite:
        """Test error handling and recovery"""
        suite = TestSuite("Error Handling", [], datetime.now())

        try:
            # Test 1: Invalid Date Handling
            start_time = datetime.now()

            async with MLBAPIService() as mlb_service:
                # Test with future date (should return empty, not error)
                future_date = date.today() + timedelta(days=365)
                games = await mlb_service.get_games_for_date(future_date)

                execution_time = (datetime.now() - start_time).total_seconds()

                suite.results.append(
                    TestResult(
                        test_name="Future Date Handling",
                        passed=isinstance(games, list),
                        message=f"Future date returned {len(games)} games (expected behavior)",
                        details={
                            "test_date": str(future_date),
                            "games_returned": len(games),
                        },
                        execution_time=execution_time,
                    )
                )

                # Test 2: Invalid Team Correlation
                start_time = datetime.now()

                correlation = await mlb_service.correlate_game(
                    home_team="INVALID", away_team="TEAMS", game_datetime=datetime.now()
                )

                execution_time = (datetime.now() - start_time).total_seconds()

                suite.results.append(
                    TestResult(
                        test_name="Invalid Team Handling",
                        passed=correlation.confidence == 0.0,
                        message=f"Invalid teams returned confidence: {correlation.confidence}",
                        details={
                            "confidence": correlation.confidence,
                            "match_reasons": correlation.match_reasons,
                        },
                        execution_time=execution_time,
                    )
                )

        except Exception as e:
            suite.results.append(
                TestResult(
                    test_name="Error Handling Setup",
                    passed=False,
                    message=f"Failed to setup error handling tests: {e}",
                    details={"error": str(e)},
                )
            )

        suite.end_time = datetime.now()
        return suite

    async def test_current_season_data(self) -> TestSuite:
        """Test with current season MLB data"""
        suite = TestSuite("Current Season Data", [], datetime.now())

        try:
            async with MLBAPIService() as mlb_service:
                today = date.today()
                current_year = today.year

                # Test 1: Season Date Range
                start_time = datetime.now()

                # Check games across the current season
                season_start = date(current_year, 3, 20)  # Approximate season start
                season_end = date(current_year, 10, 31)  # Approximate season end

                if season_start <= today <= season_end:
                    # We're in season - test recent games
                    games_found = 0
                    days_with_games = 0

                    for days_back in range(14):  # Check last 2 weeks
                        test_date = today - timedelta(days=days_back)
                        if test_date >= season_start:
                            games = await mlb_service.get_games_for_date(test_date)
                            if games:
                                games_found += len(games)
                                days_with_games += 1

                    execution_time = (datetime.now() - start_time).total_seconds()

                    suite.results.append(
                        TestResult(
                            test_name="In-Season Game Availability",
                            passed=games_found > 0,
                            message=f"Found {games_found} games across {days_with_games} days",
                            details={
                                "total_games": games_found,
                                "days_with_games": days_with_games,
                                "days_checked": 14,
                                "season_status": "in_season",
                            },
                            execution_time=execution_time,
                        )
                    )
                else:
                    # Off-season testing
                    suite.results.append(
                        TestResult(
                            test_name="Off-Season Status",
                            passed=True,
                            message="Currently in off-season, limited game data expected",
                            details={
                                "current_date": str(today),
                                "season_start": str(season_start),
                                "season_end": str(season_end),
                                "season_status": "off_season",
                            },
                        )
                    )

                # Test 2: Team Coverage
                start_time = datetime.now()

                teams_seen = set()
                for days_back in range(30):  # Check last month
                    test_date = today - timedelta(days=days_back)
                    games = await mlb_service.get_games_for_date(test_date)

                    for game in games:
                        teams_seen.add(game.home_team_name)
                        teams_seen.add(game.away_team_name)

                execution_time = (datetime.now() - start_time).total_seconds()

                # Should see most MLB teams (30 teams total)
                team_coverage = len(teams_seen) / 30.0

                suite.results.append(
                    TestResult(
                        test_name="Team Coverage",
                        passed=team_coverage
                        > 0.5,  # Should see at least half the teams
                        message=f"Observed {len(teams_seen)} unique teams ({team_coverage:.1%} coverage)",
                        details={
                            "unique_teams": len(teams_seen),
                            "teams_observed": sorted(list(teams_seen)),
                            "coverage_percentage": team_coverage,
                            "days_checked": 30,
                        },
                        execution_time=execution_time,
                    )
                )

        except Exception as e:
            suite.results.append(
                TestResult(
                    test_name="Current Season Data Setup",
                    passed=False,
                    message=f"Failed to setup current season tests: {e}",
                    details={"error": str(e)},
                )
            )

        suite.end_time = datetime.now()
        return suite

    async def generate_test_report(self, test_suites: dict[str, TestSuite]):
        """Generate comprehensive test report"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = self.output_dir / f"production_test_report_{timestamp}.json"

        # Prepare report data
        report_data = {
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "total_suites": len(test_suites),
                "total_tests": sum(suite.total_tests for suite in test_suites.values()),
                "total_passed": sum(
                    suite.passed_tests for suite in test_suites.values()
                ),
                "total_failed": sum(
                    suite.failed_tests for suite in test_suites.values()
                ),
                "overall_success_rate": sum(
                    suite.passed_tests for suite in test_suites.values()
                )
                / max(1, sum(suite.total_tests for suite in test_suites.values())),
            },
            "test_suites": {},
        }

        # Add detailed results for each suite
        for suite_name, suite in test_suites.items():
            report_data["test_suites"][suite_name] = {
                "name": suite.name,
                "total_tests": suite.total_tests,
                "passed_tests": suite.passed_tests,
                "failed_tests": suite.failed_tests,
                "success_rate": suite.success_rate,
                "duration": suite.duration,
                "start_time": suite.start_time.isoformat(),
                "end_time": suite.end_time.isoformat() if suite.end_time else None,
                "results": [asdict(result) for result in suite.results],
            }

        # Save report
        with open(report_file, "w") as f:
            json.dump(report_data, f, indent=2, default=str)

        logger.info(f"📊 Test report saved to: {report_file}")

        # Print summary to console
        print("\n" + "=" * 60)
        print("🧪 PRODUCTION TEST FRAMEWORK RESULTS")
        print("=" * 60)

        for suite_name, suite in test_suites.items():
            status = (
                "✅ PASS"
                if suite.success_rate == 1.0
                else "❌ FAIL"
                if suite.success_rate == 0.0
                else "⚠️  PARTIAL"
            )
            print(
                f"{status} {suite.name}: {suite.passed_tests}/{suite.total_tests} tests passed ({suite.success_rate:.1%})"
            )

            # Show failed tests
            for result in suite.results:
                if not result.passed:
                    print(f"  ❌ {result.test_name}: {result.message}")

        print(
            f"\n📊 Overall: {report_data['summary']['total_passed']}/{report_data['summary']['total_tests']} tests passed ({report_data['summary']['overall_success_rate']:.1%})"
        )
        print(f"📄 Full report: {report_file}")
        print("=" * 60)

        return report_file


# Convenience function for easy testing
async def run_production_tests(output_dir: str = "test_output") -> dict[str, TestSuite]:
    """
    Run all production tests and return results.

    Args:
        output_dir: Directory to save test results

    Returns:
        Dictionary of test suite results
    """
    framework = ProductionTestFramework(output_dir)
    return await framework.run_all_tests()


if __name__ == "__main__":
    # Run tests if executed directly
    asyncio.run(run_production_tests())
