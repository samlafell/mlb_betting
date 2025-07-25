#!/usr/bin/env python3
"""
Simple deployment script for data quality improvements.
Executes the SQL scripts directly without complex service integration.
"""

import asyncio
import sys
from pathlib import Path

from src.data.database.connection import get_connection


async def deploy_phase_1():
    """Deploy Phase 1: Sportsbook Mapping System"""
    print("🚀 Deploying Phase 1: Sportsbook Mapping System")

    sql_file = Path("sql/improvements/01_sportsbook_mapping_system.sql")

    if not sql_file.exists():
        print(f"❌ SQL file not found: {sql_file}")
        return False

    try:
        connection = get_connection()
        await connection.connect()

        with open(sql_file) as f:
            sql_content = f.read()

        print(f"📋 Executing: {sql_file}")
        await connection.execute_async(sql_content, fetch=None, table="phase_1_setup")

        print("✅ Phase 1 completed successfully!")
        return True

    except Exception as e:
        print(f"❌ Phase 1 failed: {str(e)}")
        return False
    finally:
        await connection.close()


async def deploy_phase_2():
    """Deploy Phase 2: Data Validation and Completeness"""
    print("🚀 Deploying Phase 2: Data Validation and Completeness")

    sql_file = Path("sql/improvements/02_data_validation_and_completeness.sql")

    if not sql_file.exists():
        print(f"❌ SQL file not found: {sql_file}")
        return False

    try:
        connection = get_connection()
        await connection.connect()

        with open(sql_file) as f:
            sql_content = f.read()

        print(f"📋 Executing: {sql_file}")
        await connection.execute_async(sql_content, fetch=None, table="phase_2_setup")

        print("✅ Phase 2 completed successfully!")
        return True

    except Exception as e:
        print(f"❌ Phase 2 failed: {str(e)}")
        return False
    finally:
        await connection.close()


async def check_deployment_status():
    """Check if the improvements have been deployed"""
    print("📊 Checking deployment status...")

    try:
        connection = get_connection()
        await connection.connect()

        # Check if sportsbook mapping table exists
        mapping_exists = await connection.fetch_async("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'core_betting' 
                AND table_name = 'sportsbook_external_mappings'
            )
        """)

        # Check if data quality views exist
        views_exist = await connection.fetch_async("""
            SELECT EXISTS (
                SELECT FROM information_schema.views 
                WHERE table_schema = 'core_betting' 
                AND table_name = 'data_quality_dashboard'
            )
        """)

        # Check if completeness columns exist
        columns_exist = await connection.fetch_async("""
            SELECT EXISTS (
                SELECT FROM information_schema.columns 
                WHERE table_schema = 'core_betting' 
                AND table_name = 'betting_lines_moneyline'
                AND column_name = 'data_completeness_score'
            )
        """)

        print("\n📋 Deployment Status:")
        print(
            f"  Sportsbook Mapping System: {'✅ Deployed' if mapping_exists[0]['exists'] else '❌ Not Deployed'}"
        )
        print(
            f"  Data Quality Views: {'✅ Deployed' if views_exist[0]['exists'] else '❌ Not Deployed'}"
        )
        print(
            f"  Completeness Scoring: {'✅ Deployed' if columns_exist[0]['exists'] else '❌ Not Deployed'}"
        )

        if all(
            [
                mapping_exists[0]["exists"],
                views_exist[0]["exists"],
                columns_exist[0]["exists"],
            ]
        ):
            print("\n🎉 All improvements are deployed!")
            return True
        else:
            print("\n⚠️  Some improvements are missing")
            return False

    except Exception as e:
        print(f"❌ Status check failed: {str(e)}")
        return False
    finally:
        await connection.close()


async def show_data_quality_status():
    """Show current data quality metrics"""
    print("📊 Current Data Quality Status:")

    try:
        connection = get_connection()
        await connection.connect()

        # Get quality dashboard data
        dashboard_data = await connection.fetch_async("""
            SELECT * FROM curated.data_quality_dashboard 
            ORDER BY table_name
        """)

        print("\n🏆 Data Quality Dashboard:")
        print("-" * 70)

        for row in dashboard_data:
            table_name = row["table_name"]
            total_rows = row["total_rows"]
            sportsbook_pct = row["sportsbook_id_pct"]
            sharp_action_pct = (
                row["sharp_action_pct"] if "sharp_action_pct" in row else 0
            )
            avg_completeness = (
                row["avg_completeness"] if "avg_completeness" in row else 0
            )

            print(f"\n📋 {table_name.upper()} Table:")
            print(f"  Total Records: {total_rows:,}")
            print(f"  Sportsbook ID Mapping: {sportsbook_pct}%")
            print(f"  Sharp Action Data: {sharp_action_pct}%")
            print(f"  Avg Completeness: {avg_completeness}")

            # Quality indicator
            if sportsbook_pct >= 95 and avg_completeness >= 0.8:
                print("  Status: 🟢 EXCELLENT")
            elif sportsbook_pct >= 80 and avg_completeness >= 0.6:
                print("  Status: 🟡 GOOD")
            elif sportsbook_pct >= 50 and avg_completeness >= 0.4:
                print("  Status: 🟠 NEEDS IMPROVEMENT")
            else:
                print("  Status: 🔴 CRITICAL")

    except Exception as e:
        print(f"❌ Could not get quality status: {str(e)}")
        print("ℹ️  This is expected if improvements haven't been deployed yet")
    finally:
        await connection.close()


def main():
    """Main deployment script"""
    if len(sys.argv) < 2:
        print("Usage: python deploy_data_quality_improvements.py [command]")
        print("Commands:")
        print("  phase1    - Deploy Phase 1: Sportsbook Mapping System")
        print("  phase2    - Deploy Phase 2: Data Validation and Completeness")
        print("  all       - Deploy both phases")
        print("  status    - Check deployment status")
        print("  quality   - Show current data quality metrics")
        return

    command = sys.argv[1].lower()

    if command == "phase1":
        success = asyncio.run(deploy_phase_1())
        sys.exit(0 if success else 1)
    elif command == "phase2":
        success = asyncio.run(deploy_phase_2())
        sys.exit(0 if success else 1)
    elif command == "all":
        print("🚀 Deploying all data quality improvements...\n")
        success1 = asyncio.run(deploy_phase_1())
        if success1:
            print()
            success2 = asyncio.run(deploy_phase_2())
            if success2:
                print("\n🎉 All phases deployed successfully!")
                print("\nNext steps:")
                print("1. Run: python deploy_data_quality_improvements.py quality")
                print("2. Monitor data quality improvements over the next few days")
            sys.exit(0 if success2 else 1)
        else:
            sys.exit(1)
    elif command == "status":
        asyncio.run(check_deployment_status())
    elif command == "quality":
        asyncio.run(show_data_quality_status())
    else:
        print(f"❌ Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
