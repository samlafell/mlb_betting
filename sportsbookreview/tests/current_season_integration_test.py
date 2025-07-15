"""
Current Season MLB Data Integration Test

This script tests the complete integration with live current season MLB data,
validating real-world scenarios and data quality when games are active.
"""

import asyncio
import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from ..models.game import EnhancedGame, GameStatus, GameType, Team
from ..services.mlb_api_service import MLBAPIService

logger = logging.getLogger(__name__)


class CurrentSeasonIntegrationTest:
    """
    Test integration with live current season MLB data.

    This test suite focuses on:
    - Live game data validation
    - Real-time correlation accuracy
    - Double header handling
    - Data consistency across time
    - Performance with active games
    """

    def __init__(self, output_dir: str = "current_season_test_output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

    async def run_comprehensive_test(self) -> dict[str, Any]:
        """Run comprehensive current season testing"""
        print("🏀 Starting Current Season MLB Integration Test")
        print("=" * 60)

        results = {
            "timestamp": datetime.now().isoformat(),
            "season_status": await self._check_season_status(),
            "live_data_tests": await self._test_live_data_quality(),
            "correlation_tests": await self._test_live_correlation(),
            "double_header_tests": await self._test_double_header_handling(),
            "performance_tests": await self._test_live_performance(),
            "data_consistency_tests": await self._test_data_consistency(),
            "integration_tests": await self._test_full_integration(),
        }

        # Generate report
        await self._generate_report(results)

        return results

    async def _check_season_status(self) -> dict[str, Any]:
        """Check current MLB season status"""
        print("\n📅 Checking Season Status...")

        today = date.today()
        current_year = today.year

        # Approximate season dates (adjust based on actual MLB schedule)
        season_start = date(current_year, 3, 20)
        season_end = date(current_year, 10, 31)
        playoffs_start = date(current_year, 10, 1)

        status = {
            "current_date": str(today),
            "season_year": current_year,
            "season_start": str(season_start),
            "season_end": str(season_end),
            "is_regular_season": season_start <= today < playoffs_start,
            "is_playoffs": playoffs_start <= today <= season_end,
            "is_offseason": today < season_start or today > season_end,
        }

        if status["is_regular_season"]:
            print("✅ Currently in regular season")
        elif status["is_playoffs"]:
            print("🏆 Currently in playoffs")
        else:
            print("❄️ Currently in offseason")

        return status

    async def _test_live_data_quality(self) -> dict[str, Any]:
        """Test live data quality and availability"""
        print("\n🔍 Testing Live Data Quality...")

        results = {
            "games_by_date": {},
            "data_quality_issues": [],
            "team_coverage": {},
            "venue_coverage": {},
            "total_games_found": 0,
        }

        async with MLBAPIService() as mlb_service:
            today = date.today()
            teams_seen = set()
            venues_seen = set()

            # Check last 14 days for recent game data
            for days_back in range(14):
                test_date = today - timedelta(days=days_back)
                games = await mlb_service.get_games_for_date(test_date)

                date_str = str(test_date)
                results["games_by_date"][date_str] = {
                    "game_count": len(games),
                    "games": [],
                }

                for game in games:
                    # Track teams and venues
                    teams_seen.add(game.home_team_name)
                    teams_seen.add(game.away_team_name)
                    if game.venue_name:
                        venues_seen.add(game.venue_name)

                    # Data quality checks
                    issues = []
                    if not game.game_pk:
                        issues.append("Missing game_pk")
                    if not game.home_team_name or not game.away_team_name:
                        issues.append("Missing team names")
                    if not game.game_datetime:
                        issues.append("Missing datetime")
                    if game.home_team_name == game.away_team_name:
                        issues.append("Same home/away team")
                    if not game.venue_name:
                        issues.append("Missing venue")

                    game_info = {
                        "game_pk": game.game_pk,
                        "matchup": f"{game.away_team_name} @ {game.home_team_name}",
                        "datetime": game.game_datetime.isoformat()
                        if game.game_datetime
                        else None,
                        "venue": game.venue_name,
                        "status": game.game_status,
                        "double_header": game.double_header,
                        "quality_issues": issues,
                    }

                    results["games_by_date"][date_str]["games"].append(game_info)

                    if issues:
                        results["data_quality_issues"].extend(
                            [
                                f"{date_str} - Game {game.game_pk}: {issue}"
                                for issue in issues
                            ]
                        )

                results["total_games_found"] += len(games)

                # Don't overwhelm with too many days if we have good data
                if len(games) > 0 and days_back >= 7:
                    break

            results["team_coverage"] = {
                "unique_teams": len(teams_seen),
                "teams": sorted(list(teams_seen)),
                "coverage_percentage": len(teams_seen) / 30.0,  # 30 MLB teams
            }

            results["venue_coverage"] = {
                "unique_venues": len(venues_seen),
                "venues": sorted(list(venues_seen)),
            }

        print(f"📊 Found {results['total_games_found']} games across 14 days")
        print(
            f"🏟️ Team coverage: {results['team_coverage']['unique_teams']}/30 teams ({results['team_coverage']['coverage_percentage']:.1%})"
        )
        print(
            f"🏟️ Venue coverage: {results['venue_coverage']['unique_venues']} unique venues"
        )

        if results["data_quality_issues"]:
            print(f"⚠️ Found {len(results['data_quality_issues'])} data quality issues")
        else:
            print("✅ No data quality issues found")

        return results

    async def _test_live_correlation(self) -> dict[str, Any]:
        """Test correlation accuracy with live data"""
        print("\n🔗 Testing Live Game Correlation...")

        results = {
            "correlation_tests": [],
            "accuracy_stats": {
                "perfect_matches": 0,
                "high_confidence": 0,
                "medium_confidence": 0,
                "low_confidence": 0,
                "failures": 0,
            },
        }

        async with MLBAPIService() as mlb_service:
            today = date.today()

            # Test correlation with recent games
            for days_back in range(7):
                test_date = today - timedelta(days=days_back)
                games = await mlb_service.get_games_for_date(test_date)

                for game in games[:5]:  # Test first 5 games per day
                    try:
                        correlation = await mlb_service.correlate_game(
                            home_team=game.home_team_name,
                            away_team=game.away_team_name,
                            game_datetime=game.game_datetime,
                        )

                        test_result = {
                            "date": str(test_date),
                            "game_pk": game.game_pk,
                            "matchup": f"{game.away_team_name} @ {game.home_team_name}",
                            "confidence": correlation.confidence,
                            "match_reasons": correlation.match_reasons,
                            "status": "success",
                        }

                        # Categorize confidence levels
                        if correlation.confidence >= 0.95:
                            results["accuracy_stats"]["perfect_matches"] += 1
                        elif correlation.confidence >= 0.8:
                            results["accuracy_stats"]["high_confidence"] += 1
                        elif correlation.confidence >= 0.5:
                            results["accuracy_stats"]["medium_confidence"] += 1
                        elif correlation.confidence > 0:
                            results["accuracy_stats"]["low_confidence"] += 1
                        else:
                            results["accuracy_stats"]["failures"] += 1

                        results["correlation_tests"].append(test_result)

                    except Exception as e:
                        results["correlation_tests"].append(
                            {
                                "date": str(test_date),
                                "game_pk": game.game_pk,
                                "matchup": f"{game.away_team_name} @ {game.home_team_name}",
                                "error": str(e),
                                "status": "error",
                            }
                        )
                        results["accuracy_stats"]["failures"] += 1

                # Stop if we have enough test data
                if len(results["correlation_tests"]) >= 20:
                    break

        total_tests = len(results["correlation_tests"])
        print("📈 Correlation accuracy:")
        print(
            f"  Perfect matches (≥95%): {results['accuracy_stats']['perfect_matches']}/{total_tests}"
        )
        print(
            f"  High confidence (≥80%): {results['accuracy_stats']['high_confidence']}/{total_tests}"
        )
        print(
            f"  Medium confidence (≥50%): {results['accuracy_stats']['medium_confidence']}/{total_tests}"
        )
        print(
            f"  Low confidence (>0%): {results['accuracy_stats']['low_confidence']}/{total_tests}"
        )
        print(f"  Failures: {results['accuracy_stats']['failures']}/{total_tests}")

        return results

    async def _test_double_header_handling(self) -> dict[str, Any]:
        """Test double header detection and handling"""
        print("\n⚾ Testing Double Header Handling...")

        results = {
            "double_headers_found": [],
            "double_header_days": [],
            "correlation_accuracy": [],
        }

        async with MLBAPIService() as mlb_service:
            today = date.today()

            # Look for double headers in recent games
            for days_back in range(30):  # Check last month
                test_date = today - timedelta(days=days_back)
                games = await mlb_service.get_games_for_date(test_date)

                # Group games by teams to find double headers
                team_games = {}
                for game in games:
                    team_pair = tuple(
                        sorted([game.home_team_name, game.away_team_name])
                    )
                    if team_pair not in team_games:
                        team_games[team_pair] = []
                    team_games[team_pair].append(game)

                # Check for double headers
                date_double_headers = []
                for team_pair, team_games_list in team_games.items():
                    if len(team_games_list) > 1:
                        # Found potential double header
                        double_header_info = {
                            "date": str(test_date),
                            "teams": f"{team_pair[0]} vs {team_pair[1]}",
                            "games": [],
                        }

                        for game in team_games_list:
                            game_info = {
                                "game_pk": game.game_pk,
                                "time": game.game_datetime.strftime("%H:%M")
                                if game.game_datetime
                                else None,
                                "double_header_flag": game.double_header,
                            }
                            double_header_info["games"].append(game_info)

                            # Test correlation for double header games
                            try:
                                correlation = await mlb_service.correlate_game(
                                    home_team=game.home_team_name,
                                    away_team=game.away_team_name,
                                    game_datetime=game.game_datetime,
                                )

                                results["correlation_accuracy"].append(
                                    {
                                        "game_pk": game.game_pk,
                                        "confidence": correlation.confidence,
                                        "is_double_header": game.double_header,
                                    }
                                )

                            except Exception as e:
                                logger.warning(
                                    f"Correlation failed for double header game {game.game_pk}: {e}"
                                )

                        date_double_headers.append(double_header_info)
                        results["double_headers_found"].extend(date_double_headers)

                if date_double_headers:
                    results["double_header_days"].append(str(test_date))

        print(f"🎯 Found {len(results['double_headers_found'])} double headers")
        print(f"📅 Double header dates: {len(results['double_header_days'])}")

        if results["correlation_accuracy"]:
            avg_confidence = sum(
                r["confidence"] for r in results["correlation_accuracy"]
            ) / len(results["correlation_accuracy"])
            print(
                f"📊 Average correlation confidence for double headers: {avg_confidence:.2f}"
            )

        return results

    async def _test_live_performance(self) -> dict[str, Any]:
        """Test performance with live data"""
        print("\n⚡ Testing Live Performance...")

        import time

        results = {
            "api_response_times": [],
            "correlation_times": [],
            "cache_performance": {},
            "bulk_operation_times": {},
        }

        async with MLBAPIService() as mlb_service:
            today = date.today()

            # Test API response times
            for i in range(5):
                start_time = time.time()
                games = await mlb_service.get_games_for_date(today)
                response_time = time.time() - start_time
                results["api_response_times"].append(response_time)

            # Test correlation performance
            if games:
                for game in games[:3]:
                    start_time = time.time()
                    await mlb_service.correlate_game(
                        home_team=game.home_team_name,
                        away_team=game.away_team_name,
                        game_datetime=game.game_datetime,
                    )
                    correlation_time = time.time() - start_time
                    results["correlation_times"].append(correlation_time)

            # Test cache performance
            cache_stats = mlb_service.get_cache_stats()
            results["cache_performance"] = cache_stats

            # Test bulk operations
            start_time = time.time()
            for days_back in range(3):
                test_date = today - timedelta(days=days_back)
                await mlb_service.get_games_for_date(test_date)
            bulk_time = time.time() - start_time
            results["bulk_operation_times"]["3_day_fetch"] = bulk_time

        avg_api_time = sum(results["api_response_times"]) / len(
            results["api_response_times"]
        )
        avg_correlation_time = (
            sum(results["correlation_times"]) / len(results["correlation_times"])
            if results["correlation_times"]
            else 0
        )

        print(f"⏱️ Average API response time: {avg_api_time:.3f}s")
        print(f"🔗 Average correlation time: {avg_correlation_time:.3f}s")
        print(
            f"💾 Cache statistics: {results['cache_performance']['in_memory']['cache_size']} entries"
        )

        return results

    async def _test_data_consistency(self) -> dict[str, Any]:
        """Test data consistency over time"""
        print("\n🔄 Testing Data Consistency...")

        results = {
            "consistency_checks": [],
            "game_status_changes": [],
            "data_stability": {},
        }

        async with MLBAPIService() as mlb_service:
            today = date.today()

            # Test same day data consistency
            games1 = await mlb_service.get_games_for_date(today)
            await asyncio.sleep(1)  # Small delay
            games2 = await mlb_service.get_games_for_date(today)

            consistency_check = {
                "test": "same_day_consistency",
                "first_call_games": len(games1),
                "second_call_games": len(games2),
                "consistent": len(games1) == len(games2),
            }

            if len(games1) == len(games2):
                # Check individual game consistency
                game_matches = 0
                for g1, g2 in zip(games1, games2, strict=False):
                    if (
                        g1.game_pk == g2.game_pk
                        and g1.home_team_name == g2.home_team_name
                        and g1.away_team_name == g2.away_team_name
                    ):
                        game_matches += 1

                consistency_check["game_matches"] = game_matches
                consistency_check["fully_consistent"] = game_matches == len(games1)

            results["consistency_checks"].append(consistency_check)

            # Test game status tracking (for completed games)
            for days_back in range(7):
                test_date = today - timedelta(days=days_back)
                games = await mlb_service.get_games_for_date(test_date)

                for game in games:
                    if game.game_status:
                        results["game_status_changes"].append(
                            {
                                "date": str(test_date),
                                "game_pk": game.game_pk,
                                "status": game.game_status,
                                "matchup": f"{game.away_team_name} @ {game.home_team_name}",
                            }
                        )

        print(f"✅ Data consistency checks: {len(results['consistency_checks'])}")
        print(
            f"📊 Game status tracking: {len(results['game_status_changes'])} status records"
        )

        return results

    async def _test_full_integration(self) -> dict[str, Any]:
        """Test full integration pipeline"""
        print("\n🔧 Testing Full Integration Pipeline...")

        results = {
            "integration_tests": [],
            "end_to_end_success": False,
            "pipeline_performance": {},
        }

        try:
            import time

            start_time = time.time()

            # Test creating enhanced game from live data
            async with MLBAPIService() as mlb_service:
                today = date.today()
                games = await mlb_service.get_games_for_date(today)

                if games:
                    live_game = games[0]

                    # Create enhanced game model
                    enhanced_game = EnhancedGame(
                        sbr_game_id=f"live-test-{live_game.game_pk}",
                        home_team=Team(live_game.home_team_name),
                        away_team=Team(live_game.away_team_name),
                        game_datetime=live_game.game_datetime,
                        game_type=GameType.REGULAR,
                        game_status=GameStatus.SCHEDULED,
                        mlb_game_id=live_game.game_pk,
                    )

                    results["integration_tests"].append(
                        {
                            "test": "enhanced_game_creation",
                            "success": True,
                            "game_id": enhanced_game.sbr_game_id,
                            "correlation_key": enhanced_game.get_correlation_key(),
                        }
                    )

                    # Test correlation
                    correlation = await mlb_service.correlate_game(
                        home_team=live_game.home_team_name,
                        away_team=live_game.away_team_name,
                        game_datetime=live_game.game_datetime,
                    )

                    results["integration_tests"].append(
                        {
                            "test": "live_correlation",
                            "success": correlation.confidence > 0.8,
                            "confidence": correlation.confidence,
                            "match_reasons": correlation.match_reasons,
                        }
                    )

                    results["end_to_end_success"] = True

                else:
                    results["integration_tests"].append(
                        {
                            "test": "no_live_games",
                            "success": False,
                            "message": "No live games found for integration testing",
                        }
                    )

            end_time = time.time()
            results["pipeline_performance"]["total_time"] = end_time - start_time

        except Exception as e:
            results["integration_tests"].append(
                {"test": "integration_error", "success": False, "error": str(e)}
            )

        success_count = sum(
            1 for test in results["integration_tests"] if test.get("success", False)
        )
        print(
            f"🎯 Integration tests: {success_count}/{len(results['integration_tests'])} passed"
        )

        return results

    async def _generate_report(self, results: dict[str, Any]):
        """Generate comprehensive test report"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = self.output_dir / f"current_season_test_report_{timestamp}.json"

        # Save detailed results
        with open(report_file, "w") as f:
            json.dump(results, f, indent=2, default=str)

        # Generate summary report
        summary_file = self.output_dir / f"current_season_summary_{timestamp}.txt"

        with open(summary_file, "w") as f:
            f.write("CURRENT SEASON MLB INTEGRATION TEST SUMMARY\n")
            f.write("=" * 50 + "\n\n")

            f.write(f"Test Date: {results['timestamp']}\n")
            f.write(f"Season Status: {results['season_status']}\n\n")

            # Live data summary
            live_data = results["live_data_tests"]
            f.write("Live Data Quality:\n")
            f.write(f"  Total games found: {live_data['total_games_found']}\n")
            f.write(
                f"  Team coverage: {live_data['team_coverage']['unique_teams']}/30\n"
            )
            f.write(
                f"  Data quality issues: {len(live_data['data_quality_issues'])}\n\n"
            )

            # Correlation summary
            correlation = results["correlation_tests"]
            total_correlation_tests = len(correlation["correlation_tests"])
            f.write("Correlation Accuracy:\n")
            f.write(f"  Total tests: {total_correlation_tests}\n")
            f.write(
                f"  Perfect matches: {correlation['accuracy_stats']['perfect_matches']}\n"
            )
            f.write(
                f"  High confidence: {correlation['accuracy_stats']['high_confidence']}\n"
            )
            f.write(f"  Failures: {correlation['accuracy_stats']['failures']}\n\n")

            # Double header summary
            double_headers = results["double_header_tests"]
            f.write("Double Header Handling:\n")
            f.write(
                f"  Double headers found: {len(double_headers['double_headers_found'])}\n"
            )
            f.write(
                f"  Days with double headers: {len(double_headers['double_header_days'])}\n\n"
            )

            # Performance summary
            performance = results["performance_tests"]
            if performance["api_response_times"]:
                avg_api_time = sum(performance["api_response_times"]) / len(
                    performance["api_response_times"]
                )
                f.write("Performance:\n")
                f.write(f"  Average API response time: {avg_api_time:.3f}s\n")
                f.write(
                    f"  Cache entries: {performance['cache_performance']['in_memory']['cache_size']}\n\n"
                )

            # Integration summary
            integration = results["integration_tests"]
            success_count = sum(
                1
                for test in integration["integration_tests"]
                if test.get("success", False)
            )
            f.write("Integration Pipeline:\n")
            f.write(
                f"  Tests passed: {success_count}/{len(integration['integration_tests'])}\n"
            )
            f.write(f"  End-to-end success: {integration['end_to_end_success']}\n")

        print(f"\n📊 Detailed report saved to: {report_file}")
        print(f"📋 Summary report saved to: {summary_file}")


# Convenience function
async def run_current_season_test(
    output_dir: str = "current_season_test_output",
) -> dict[str, Any]:
    """
    Run current season integration test.

    Args:
        output_dir: Directory to save test results

    Returns:
        Test results dictionary
    """
    test = CurrentSeasonIntegrationTest(output_dir)
    return await test.run_comprehensive_test()


if __name__ == "__main__":
    # Run test if executed directly
    asyncio.run(run_current_season_test())
