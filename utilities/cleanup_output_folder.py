#!/usr/bin/env python3
"""
Output folder cleanup utility for MLB betting system.

Moves analysis reports and opportunities to PostgreSQL database,
keeps only essential URL files for manual evaluation.
"""

import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path

import click

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data.database.repositories.analysis_reports_repository import (
    AnalysisReportsRepository,
)


def get_files_by_type(output_dir: Path) -> dict[str, list[Path]]:
    """Categorize files in output directory."""
    files = {
        "urls": [],  # Keep - for manual evaluation
        "reports": [],  # Archive to database
        "opportunities": [],  # Archive to database
        "pipeline": [],  # Archive to database
        "historical": [],  # Archive to database (large files)
        "other": [],
    }

    for file_path in output_dir.glob("*.json"):
        name = file_path.name.lower()

        if "game_urls" in name:
            files["urls"].append(file_path)
        elif "analysis_report" in name:
            files["reports"].append(file_path)
        elif "betting_opportunities" in name:
            files["opportunities"].append(file_path)
        elif "pipeline_results" in name:
            files["pipeline"].append(file_path)
        elif "historical_line_movement" in name:
            files["historical"].append(file_path)
        else:
            files["other"].append(file_path)

    return files


async def migrate_reports_to_database(report_files: list[Path]) -> int:
    """Migrate analysis reports to PostgreSQL."""
    reports_repo = AnalysisReportsRepository()
    migrated_count = 0

    for report_file in report_files:
        try:
            with open(report_file) as f:
                data = json.load(f)

            # Skip empty reports
            if data.get("total_games_analyzed", 0) == 0:
                continue

            # Extract timestamp from filename or data
            timestamp_str = data.get("analysis_timestamp")
            if timestamp_str:
                if isinstance(timestamp_str, str):
                    analysis_timestamp = datetime.fromisoformat(
                        timestamp_str.replace("Z", "+00:00")
                    )
                else:
                    analysis_timestamp = datetime.now()
            else:
                analysis_timestamp = datetime.now()

            pipeline_run_id = f"migrated_{report_file.stem}"

            # Create analysis report
            report_id = await reports_repo.create_analysis_report(
                report_type="movement_analysis",
                analysis_timestamp=analysis_timestamp,
                pipeline_run_id=pipeline_run_id,
                total_games_analyzed=data.get("total_games_analyzed", 0),
                games_with_rlm=data.get("games_with_rlm", 0),
                games_with_steam_moves=data.get("games_with_steam_moves", 0),
                games_with_arbitrage=data.get("games_with_arbitrage", 0),
                total_movements=data.get("total_movements", 0),
            )

            migrated_count += 1
            print(f"✅ Migrated {report_file.name} to database (ID: {report_id})")

        except Exception as e:
            print(f"❌ Failed to migrate {report_file.name}: {e}")

    return migrated_count


def archive_files(files: list[Path], archive_dir: Path, category: str) -> int:
    """Archive files to a subdirectory."""
    if not files:
        return 0

    category_dir = archive_dir / category
    category_dir.mkdir(exist_ok=True)

    archived_count = 0
    for file_path in files:
        try:
            dest_path = category_dir / file_path.name
            file_path.rename(dest_path)
            archived_count += 1
        except Exception as e:
            print(f"❌ Failed to archive {file_path.name}: {e}")

    return archived_count


def get_directory_size(directory: Path) -> int:
    """Get total size of directory in bytes."""
    total_size = 0
    for file_path in directory.rglob("*"):
        if file_path.is_file():
            total_size += file_path.stat().st_size
    return total_size


def format_size(size_bytes: int) -> str:
    """Format bytes as human readable string."""
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


@click.command()
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(exists=True, path_type=Path),
    default="output",
    help="Output directory to clean up",
)
@click.option(
    "--migrate-to-db",
    is_flag=True,
    help="Migrate analysis reports to PostgreSQL database",
)
@click.option(
    "--archive",
    is_flag=True,
    help="Archive old files to subdirectories",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be done without making changes",
)
@click.option(
    "--keep-recent",
    type=int,
    default=5,
    help="Number of recent URL files to keep (default: 5)",
)
def cleanup_output(
    output_dir: Path,
    migrate_to_db: bool,
    archive: bool,
    dry_run: bool,
    keep_recent: int,
):
    """Clean up output folder, migrate reports to database, keep URLs for manual evaluation."""

    print(f"🔍 Analyzing output directory: {output_dir}")

    # Get current directory size
    initial_size = get_directory_size(output_dir)
    print(f"📊 Current directory size: {format_size(initial_size)}")

    # Categorize files
    files_by_type = get_files_by_type(output_dir)

    print("\n📁 File categories found:")
    for category, file_list in files_by_type.items():
        if file_list:
            total_size = sum(f.stat().st_size for f in file_list)
            print(
                f"  {category:12}: {len(file_list):3} files ({format_size(total_size)})"
            )

    if dry_run:
        print("\n🔍 DRY RUN - No changes will be made")

        # Show what would be migrated
        if migrate_to_db and files_by_type["reports"]:
            print(
                f"\n📊 Would migrate {len(files_by_type['reports'])} analysis reports to database"
            )

        # Show what would be archived
        if archive:
            for category in ["reports", "opportunities", "pipeline", "historical"]:
                if files_by_type[category]:
                    print(
                        f"📦 Would archive {len(files_by_type[category])} {category} files"
                    )

        # Show URL file management
        if len(files_by_type["urls"]) > keep_recent:
            excess = len(files_by_type["urls"]) - keep_recent
            print(
                f"🔗 Would keep {keep_recent} most recent URL files, archive {excess} old ones"
            )
        else:
            print(
                f"🔗 Would keep all {len(files_by_type['urls'])} URL files (within limit)"
            )

        return

    # Execute cleanup operations
    print("\n🚀 Starting cleanup operations...")

    # 1. Migrate reports to database
    if migrate_to_db and files_by_type["reports"]:
        print(
            f"\n📊 Migrating {len(files_by_type['reports'])} analysis reports to database..."
        )
        migrated_count = asyncio.run(
            migrate_reports_to_database(files_by_type["reports"])
        )
        print(f"✅ Successfully migrated {migrated_count} reports to PostgreSQL")

    # 2. Archive files if requested
    if archive:
        archive_dir = output_dir / "archive" / datetime.now().strftime("%Y-%m-%d")
        archive_dir.mkdir(parents=True, exist_ok=True)

        total_archived = 0
        for category in ["reports", "opportunities", "pipeline", "historical"]:
            if files_by_type[category]:
                # Remove migrated reports from archive list
                files_to_archive = files_by_type[category]
                if category == "reports" and migrate_to_db:
                    files_to_archive = []  # Already migrated, can be deleted

                if files_to_archive:
                    archived = archive_files(files_to_archive, archive_dir, category)
                    total_archived += archived
                    print(f"📦 Archived {archived} {category} files")

        if total_archived > 0:
            print(f"✅ Total archived: {total_archived} files to {archive_dir}")

    # 3. Manage URL files (keep recent ones)
    url_files = sorted(
        files_by_type["urls"], key=lambda f: f.stat().st_mtime, reverse=True
    )
    if len(url_files) > keep_recent:
        excess_urls = url_files[keep_recent:]
        if archive:
            archive_dir = output_dir / "archive" / datetime.now().strftime("%Y-%m-%d")
            archived_urls = archive_files(excess_urls, archive_dir, "old_urls")
            print(
                f"🔗 Archived {archived_urls} old URL files, kept {keep_recent} most recent"
            )
        else:
            print(
                f"🔗 Found {len(excess_urls)} excess URL files (use --archive to move them)"
            )
    else:
        print(f"🔗 Keeping all {len(url_files)} URL files (within limit)")

    # 4. Delete migrated report files if migration succeeded
    if migrate_to_db and not archive:
        for report_file in files_by_type["reports"]:
            try:
                report_file.unlink()
                print(f"🗑️  Deleted migrated report: {report_file.name}")
            except Exception as e:
                print(f"❌ Failed to delete {report_file.name}: {e}")

    # Show final results
    final_size = get_directory_size(output_dir)
    size_reduction = initial_size - final_size

    print("\n📊 Cleanup complete!")
    print(f"   Initial size: {format_size(initial_size)}")
    print(f"   Final size:   {format_size(final_size)}")
    print(
        f"   Reduction:    {format_size(size_reduction)} ({size_reduction / initial_size * 100:.1f}%)"
    )

    # Show recommendations
    print("\n💡 Recommendations:")
    print(
        "   • Use database-first approach: `uv run -m src.interfaces.cli action-network pipeline`"
    )
    print(
        "   • View opportunities from database: `uv run -m src.interfaces.cli action-network opportunities`"
    )
    print(f"   • URL files preserved for manual evaluation in {output_dir}")

    if not migrate_to_db:
        print("   • Run with --migrate-to-db to move analysis reports to PostgreSQL")

    if not archive:
        print("   • Run with --archive to organize old files")


if __name__ == "__main__":
    cleanup_output()
