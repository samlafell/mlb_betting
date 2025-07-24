"""
CLI command for output folder cleanup.
"""

from datetime import datetime
from pathlib import Path

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()


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
def cleanup(
    output_dir: Path,
    dry_run: bool,
    keep_recent: int,
):
    """Clean up output folder, show recommendations for database migration."""

    console.print(
        Panel.fit(
            "[bold blue]🧹 Output Folder Cleanup[/bold blue]\n"
            f"Directory: [yellow]{output_dir}[/yellow]\n"
            f"Mode: [yellow]{'DRY RUN' if dry_run else 'EXECUTE'}[/yellow]",
            title="Cleanup Analysis",
        )
    )

    # Get current directory size
    initial_size = get_directory_size(output_dir)
    console.print(
        f"📊 Current directory size: [bold]{format_size(initial_size)}[/bold]"
    )

    # Categorize files
    files_by_type = get_files_by_type(output_dir)

    # Create analysis table
    table = Table(title="File Analysis")
    table.add_column("Category", style="cyan")
    table.add_column("Count", style="green", justify="right")
    table.add_column("Size", style="yellow", justify="right")
    table.add_column("Recommendation", style="white")

    for category, file_list in files_by_type.items():
        if file_list:
            total_size = sum(f.stat().st_size for f in file_list)

            if category == "urls":
                if len(file_list) > keep_recent:
                    recommendation = f"Keep {keep_recent} recent, archive {len(file_list) - keep_recent} old"
                else:
                    recommendation = "Keep all (for manual evaluation)"
            elif category in ["reports", "opportunities", "pipeline"]:
                recommendation = "Move to PostgreSQL database"
            elif category == "historical":
                recommendation = "Archive (large files)"
            else:
                recommendation = "Review manually"

            table.add_row(
                category.title(),
                str(len(file_list)),
                format_size(total_size),
                recommendation,
            )

    console.print(table)

    # Calculate potential savings
    archive_categories = ["reports", "opportunities", "pipeline", "historical"]
    archive_files = []
    for cat in archive_categories:
        archive_files.extend(files_by_type[cat])

    if archive_files:
        archive_size = sum(f.stat().st_size for f in archive_files)
        savings_pct = (archive_size / initial_size) * 100 if initial_size > 0 else 0

        console.print(
            f"\n💾 Potential space savings: [bold green]{format_size(archive_size)}[/bold green] ({savings_pct:.1f}%)"
        )

    # Show specific recommendations
    console.print("\n💡 [bold]Recommendations:[/bold]")
    console.print(
        "   1. Run database schema setup: [cyan]psql -d mlb_betting -f sql/schemas/analysis_reports.sql[/cyan]"
    )
    console.print("   2. Future pipelines will use database storage automatically")
    console.print("   3. URL files preserved for manual evaluation")

    if files_by_type["reports"]:
        console.print(
            f"   4. [yellow]{len(files_by_type['reports'])} analysis reports[/yellow] can be migrated to database"
        )

    if not dry_run:
        console.print("\n🔧 [bold]Actions:[/bold]")

        # Archive excess URL files
        url_files = sorted(
            files_by_type["urls"], key=lambda f: f.stat().st_mtime, reverse=True
        )
        if len(url_files) > keep_recent:
            archive_dir = output_dir / "archive" / datetime.now().strftime("%Y-%m-%d")
            archive_dir.mkdir(parents=True, exist_ok=True)

            excess_urls = url_files[keep_recent:]
            for url_file in excess_urls:
                dest_path = archive_dir / "old_urls" / url_file.name
                dest_path.parent.mkdir(exist_ok=True)
                url_file.rename(dest_path)

            console.print(f"📦 Archived {len(excess_urls)} old URL files")

        # Note about other files
        if archive_files:
            console.print(
                f"📋 {len(archive_files)} analysis files ready for database migration"
            )
            console.print(
                "   Use the enhanced pipeline to start using database storage"
            )

    # Show final summary
    final_size = get_directory_size(output_dir)
    if not dry_run and final_size != initial_size:
        size_reduction = initial_size - final_size
        console.print(
            f"\n✅ Cleanup complete: {format_size(size_reduction)} saved ({size_reduction / initial_size * 100:.1f}%)"
        )

    console.print("\n🚀 [bold]Next Steps:[/bold]")
    console.print(
        "   • Use: [cyan]uv run -m src.interfaces.cli action-network pipeline[/cyan]"
    )
    console.print(
        "   • View opportunities: [cyan]uv run -m src.interfaces.cli action-network opportunities[/cyan]"
    )
    console.print("   • Database will replace JSON files for analysis results")


if __name__ == "__main__":
    cleanup()
