#!/usr/bin/env python3
"""
Extract all Action Network game URLs for today or tomorrow.

This script uses the existing Action Network tools to:
1. Get games from the scoreboard API for the specified date
2. Extract the current build ID
3. Build complete game data URLs for all games
"""

import argparse
import glob
import json
import os
from datetime import datetime, timedelta

import structlog

from action.utils.actionnetwork_enhanced_fetcher import ActionNetworkEnhancedFetcher
from action.utils.actionnetwork_url_builder import ActionNetworkURLBuilder

# Configure logging
structlog.configure(
    processors=[structlog.stdlib.filter_by_level, structlog.dev.ConsoleRenderer()],
    wrapper_class=structlog.stdlib.BoundLogger,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)


def get_target_date(date_option: str = "today") -> datetime:
    """
    Get the target date based on the option provided.

    Args:
        date_option: Either "today" or "tomorrow"

    Returns:
        datetime object for the target date
    """
    today = datetime.now()
    if date_option.lower() == "tomorrow":
        return today + timedelta(days=1)
    return today


def extract_game_urls(date_option: str = "today") -> list[tuple[dict, str]]:
    """
    Extract all game URLs for the specified date.

    Args:
        date_option: Either "today" or "tomorrow"

    Returns:
        List of tuples: (game_data, url)
    """
    target_date = get_target_date(date_option)
    logger.info(
        "🔗 Extracting Action Network game URLs",
        date=target_date.strftime("%Y-%m-%d"),
        date_option=date_option,
    )

    try:
        # Initialize the URL builder
        builder = ActionNetworkURLBuilder(cache_build_id=True)

        # Get all games and build URLs for the target date
        game_urls = builder.build_all_game_urls(target_date)

        logger.info(
            "✅ Successfully extracted game URLs",
            total_games=len(game_urls),
            date=target_date.strftime("%Y-%m-%d"),
            date_option=date_option,
        )

        return game_urls

    except Exception as e:
        logger.error(
            "❌ Failed to extract game URLs", error=str(e), date_option=date_option
        )
        raise


def test_urls_with_enhanced_fetcher(
    game_urls: list[tuple[dict, str]],
) -> dict[str, bool]:
    """
    Test each URL to ensure it's working using the enhanced fetcher.

    Args:
        game_urls: List of (game_data, url) tuples

    Returns:
        Dictionary mapping URLs to success status
    """
    fetcher = ActionNetworkEnhancedFetcher()
    results = {}

    logger.info("🧪 Testing URLs with enhanced fetcher", total_urls=len(game_urls))

    for i, (game_data, url) in enumerate(game_urls, 1):
        try:
            # Extract team names for logging
            teams = game_data.get("teams", [])
            if len(teams) >= 2:
                away_team = teams[1].get(
                    "full_name", teams[1].get("display_name", "Unknown")
                )
                home_team = teams[0].get(
                    "full_name", teams[0].get("display_name", "Unknown")
                )
                game_display = f"{away_team} @ {home_team}"
            else:
                game_display = f"Game {game_data.get('id', 'Unknown')}"

            logger.info(f"🎯 Testing URL {i}/{len(game_urls)}", game=game_display)

            # Test the URL
            response_data = fetcher.fetch_game_data(url)
            success = response_data is not None
            results[url] = success

            if success:
                logger.info("✅ URL working", game=game_display)
            else:
                logger.warning("❌ URL failed", game=game_display)

        except Exception as e:
            logger.error("🔥 URL test error", game=game_display, error=str(e))
            results[url] = False

    working_count = sum(1 for success in results.values() if success)
    logger.info(
        "🧪 URL testing completed",
        working=working_count,
        total=len(results),
        success_rate=f"{working_count / len(results) * 100:.1f}%",
    )

    return results


def cleanup_old_json_files() -> None:
    """
    Delete existing Action Network JSON files to prevent accumulation.
    These files get large and become invalid when build IDs change.
    """
    pattern = "output/action_network_game_urls_*.json"
    old_files = glob.glob(pattern)

    if old_files:
        logger.info("🧹 Cleaning up old JSON files", count=len(old_files))
        for file_path in old_files:
            try:
                os.remove(file_path)
                logger.debug("Deleted old file", file=file_path)
            except Exception as e:
                logger.warning(
                    "Failed to delete old file", file=file_path, error=str(e)
                )
    else:
        logger.debug("No old JSON files to clean up")


def save_urls_to_file(
    game_urls: list[tuple[dict, str]], date_option: str = "today", filename: str = None
) -> str:
    """
    Save the extracted URLs to a JSON file.

    Args:
        game_urls: List of (game_data, url) tuples
        date_option: Either "today" or "tomorrow"
        filename: Optional filename (defaults to timestamped name)

    Returns:
        Path to saved file
    """
    # Clean up old JSON files first (they become invalid when build IDs change)
    cleanup_old_json_files()

    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"action_network_game_urls_{date_option}_{timestamp}.json"

    # Prepare data for JSON export
    target_date = get_target_date(date_option)
    export_data = {
        "extracted_at": datetime.now().isoformat(),
        "target_date": target_date.strftime("%Y-%m-%d"),
        "date_option": date_option,
        "total_games": len(game_urls),
        "games": [],
    }

    for game_data, url in game_urls:
        # Extract key information from game data
        teams = game_data.get("teams", [])
        if len(teams) >= 2:
            away_team = teams[1].get(
                "full_name", teams[1].get("display_name", "Unknown")
            )
            home_team = teams[0].get(
                "full_name", teams[0].get("display_name", "Unknown")
            )
        else:
            away_team = "Unknown"
            home_team = "Unknown"

        game_info = {
            "game_id": game_data.get("id"),
            "away_team": away_team,
            "home_team": home_team,
            "start_time": game_data.get("start_time"),
            "status": game_data.get("status"),
            "url": url,
            "history_url": f"https://api.actionnetwork.com/web/v2/markets/event/{game_data.get('id')}/history",
            "game_data": game_data,  # Include full game data for reference
        }
        export_data["games"].append(game_info)

    # Save to file
    output_path = f"output/{filename}"
    with open(output_path, "w") as f:
        json.dump(export_data, f, indent=2, default=str)

    logger.info(
        "💾 URLs saved to file",
        file=output_path,
        total_games=len(game_urls),
        date_option=date_option,
    )

    return output_path


def print_urls_summary(
    game_urls: list[tuple[dict, str]], date_option: str = "today"
) -> None:
    """Print a summary of extracted URLs."""
    target_date = get_target_date(date_option)
    print("\n" + "=" * 80)
    print(
        f"🔗 ACTION NETWORK GAME URLs - {target_date.strftime('%Y-%m-%d')} ({date_option.upper()})"
    )
    print("=" * 80)
    print(f"📊 Total Games Found: {len(game_urls)}")
    print()

    for i, (game_data, url) in enumerate(game_urls, 1):
        # Extract team information
        teams = game_data.get("teams", [])
        if len(teams) >= 2:
            away_team = teams[1].get(
                "full_name", teams[1].get("display_name", "Unknown")
            )
            home_team = teams[0].get(
                "full_name", teams[0].get("display_name", "Unknown")
            )
        else:
            away_team = "Unknown"
            home_team = "Unknown"

        game_id = game_data.get("id", "Unknown")
        start_time = game_data.get("start_time", "Unknown")

        print(f"🏟️  Game {i}: {away_team} @ {home_team}")
        print(f"   🆔 Game ID: {game_id}")
        print(f"   🕐 Start Time: {start_time}")
        print(f"   🔗 URL: {url}")
        print()

    print("=" * 80)


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Extract Action Network game URLs for today or tomorrow",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                    # Extract today's games
  %(prog)s --date today       # Extract today's games (explicit)
  %(prog)s --date tomorrow    # Extract tomorrow's games
  %(prog)s --no-test          # Skip URL testing
        """,
    )

    parser.add_argument(
        "--date",
        choices=["today", "tomorrow"],
        default="today",
        help="Which date to extract games for (default: today)",
    )

    parser.add_argument(
        "--no-test", action="store_true", help="Skip URL testing with enhanced fetcher"
    )

    return parser.parse_args()


def main():
    """Main execution function."""
    args = parse_arguments()

    try:
        # Extract game URLs for the specified date
        game_urls = extract_game_urls(args.date)

        if not game_urls:
            logger.warning(
                "⚠️  No games found for the specified date", date_option=args.date
            )
            return

        # Print summary
        print_urls_summary(game_urls, args.date)

        # Save to file (this will clean up old JSON files first)
        output_file = save_urls_to_file(game_urls, args.date)

        # Test URLs (optional - can be skipped with --no-test)
        if not args.no_test:
            print("\n🧪 Testing URLs with enhanced fetcher...")
            test_results = test_urls_with_enhanced_fetcher(game_urls)
            working_urls = sum(1 for success in test_results.values() if success)
        else:
            print("\n⏭️  Skipping URL testing")
            working_urls = "N/A"

        # Final summary
        target_date = get_target_date(args.date)
        print("\n✅ Extraction Complete!")
        print(f"   📅 Date: {target_date.strftime('%Y-%m-%d')} ({args.date.upper()})")
        print(f"   📊 Total URLs: {len(game_urls)}")
        print(f"   ✅ Working URLs: {working_urls}")
        print(f"   💾 Saved to: {output_file}")

        return game_urls

    except Exception as e:
        logger.error("💥 Main execution failed", error=str(e), date_option=args.date)
        raise


if __name__ == "__main__":
    main()
