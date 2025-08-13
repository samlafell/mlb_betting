#!/usr/bin/env python3
"""
Assess current data state across RAW → STAGING → CURATED pipeline
"""
from src.core.config import get_settings
from src.data.database.connection import initialize_connections, get_connection
import asyncio
from datetime import datetime, timedelta

async def assess_pipeline_data():
    try:
        config = get_settings()
        initialize_connections(config)
        print("🔍 Assessing Pipeline Data State\n")
        
        async with get_connection() as conn:
            # RAW zone assessment
            print("📊 RAW ZONE ASSESSMENT")
            print("=" * 50)
            
            raw_tables = await conn.fetch("""
                SELECT table_name, 
                       (SELECT COUNT(*) FROM information_schema.tables t2 
                        WHERE t2.table_schema = 'raw_data' AND t2.table_name = t1.table_name) as exists
                FROM information_schema.tables t1
                WHERE table_schema = 'raw_data'
                ORDER BY table_name
            """)
            
            for table in raw_tables:
                table_name = table['table_name']
                try:
                    # Get record counts
                    total_count = await conn.fetchval(f"SELECT COUNT(*) FROM raw_data.{table_name}")
                    
                    # Check if created_at column exists
                    created_at_exists = await conn.fetchval(f"""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.columns 
                            WHERE table_schema = 'raw_data' AND table_name = '{table_name}' 
                            AND column_name = 'created_at'
                        )
                    """)
                    
                    # Check if collected_at column exists as fallback
                    collected_at_exists = await conn.fetchval(f"""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.columns 
                            WHERE table_schema = 'raw_data' AND table_name = '{table_name}' 
                            AND column_name = 'collected_at'
                        )
                    """)
                    
                    # Choose the appropriate timestamp column
                    timestamp_col = None
                    if created_at_exists:
                        timestamp_col = 'created_at'
                    elif collected_at_exists:
                        timestamp_col = 'collected_at'
                    
                    if timestamp_col:
                        recent_count = await conn.fetchval(f"""
                            SELECT COUNT(*) FROM raw_data.{table_name} 
                            WHERE {timestamp_col} > NOW() - INTERVAL '7 days'
                        """)
                    else:
                        recent_count = "N/A (no timestamp column)"
                    
                    # Check for unprocessed records (only if processed_at column exists)
                    processed_at_exists = await conn.fetchval(f"""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.columns 
                            WHERE table_schema = 'raw_data' AND table_name = '{table_name}' 
                            AND column_name = 'processed_at'
                        )
                    """)
                    
                    if processed_at_exists:
                        unprocessed_count = await conn.fetchval(f"""
                            SELECT COUNT(*) FROM raw_data.{table_name} 
                            WHERE processed_at IS NULL
                        """)
                    else:
                        unprocessed_count = "N/A (no processed_at column)"
                    
                    print(f"  📋 {table_name}")
                    print(f"      Total: {total_count:,} | Recent (7d): {recent_count} | Unprocessed: {unprocessed_count}")
                    
                except Exception as e:
                    print(f"  ❌ {table_name}: Error - {e}")
            
            # STAGING zone assessment
            print(f"\n📊 STAGING ZONE ASSESSMENT")
            print("=" * 50)
            
            staging_tables = await conn.fetch("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'staging'
                ORDER BY table_name
            """)
            
            for table in staging_tables:
                table_name = table['table_name']
                try:
                    total_count = await conn.fetchval(f"SELECT COUNT(*) FROM staging.{table_name}")
                    
                    # Check if created_at column exists
                    created_at_exists = await conn.fetchval(f"""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.columns 
                            WHERE table_schema = 'staging' AND table_name = '{table_name}' 
                            AND column_name = 'created_at'
                        )
                    """)
                    
                    if created_at_exists:
                        recent_count = await conn.fetchval(f"""
                            SELECT COUNT(*) FROM staging.{table_name} 
                            WHERE created_at > NOW() - INTERVAL '7 days'
                        """)
                    else:
                        recent_count = "N/A (no created_at column)"
                    
                    print(f"  📋 {table_name}")
                    print(f"      Total: {total_count:,} | Recent (7d): {recent_count}")
                    
                    # Additional details for key tables (only if they have the required columns)
                    if table_name == 'action_network_odds_historical' and created_at_exists:
                        # Check if market_type and side columns exist
                        market_cols_exist = await conn.fetchval(f"""
                            SELECT COUNT(*) FROM information_schema.columns 
                            WHERE table_schema = 'staging' AND table_name = '{table_name}' 
                            AND column_name IN ('market_type', 'side')
                        """)
                        
                        if market_cols_exist >= 2:  # Both columns exist
                            markets = await conn.fetch(f"""
                                SELECT market_type, side, COUNT(*) as count
                                FROM staging.{table_name}
                                WHERE created_at > NOW() - INTERVAL '7 days'
                                GROUP BY market_type, side
                                ORDER BY market_type, side
                            """)
                            
                            if markets:
                                print("      Market breakdown (7d):")
                                for market in markets:
                                    print(f"        {market['market_type']}.{market['side']}: {market['count']:,}")
                    
                except Exception as e:
                    print(f"  ❌ {table_name}: Error - {e}")
            
            # CURATED zone assessment
            print(f"\n📊 CURATED ZONE ASSESSMENT")
            print("=" * 50)
            
            curated_tables = await conn.fetch("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'curated'
                ORDER BY table_name
            """)
            
            for table in curated_tables:
                table_name = table['table_name']
                try:
                    total_count = await conn.fetchval(f"SELECT COUNT(*) FROM curated.{table_name}")
                    
                    # Check if created_at column exists
                    created_at_exists = await conn.fetchval(f"""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.columns 
                            WHERE table_schema = 'curated' AND table_name = '{table_name}' 
                            AND column_name = 'created_at'
                        )
                    """)
                    
                    if created_at_exists:
                        recent_count = await conn.fetchval(f"""
                            SELECT COUNT(*) FROM curated.{table_name} 
                            WHERE created_at > NOW() - INTERVAL '7 days'
                        """)
                    else:
                        recent_count = "N/A (no created_at column)"
                    
                    print(f"  📋 {table_name}")
                    print(f"      Total: {total_count:,} | Recent (7d): {recent_count}")
                    
                except Exception as e:
                    print(f"  ❌ {table_name}: Error - {e}")
            
            # Data flow analysis
            print(f"\n🔄 DATA FLOW ANALYSIS")
            print("=" * 50)
            
            # Check RAW → STAGING flow
            try:
                raw_to_staging = await conn.fetchrow("""
                    SELECT 
                        (SELECT COUNT(*) FROM raw_data.action_network_odds 
                         WHERE created_at > NOW() - INTERVAL '24 hours') as raw_24h,
                        (SELECT COUNT(*) FROM staging.action_network_odds_historical 
                         WHERE created_at > NOW() - INTERVAL '24 hours') as staging_24h,
                        (SELECT COUNT(*) FROM raw_data.action_network_odds 
                         WHERE processed_at IS NULL AND created_at > NOW() - INTERVAL '24 hours') as unprocessed_24h
                """)
                
                print(f"📈 Last 24 hours:")
                print(f"   RAW (action_network_odds): {raw_to_staging['raw_24h']:,}")
                print(f"   STAGING (odds_historical): {raw_to_staging['staging_24h']:,}")
                print(f"   Unprocessed RAW: {raw_to_staging['unprocessed_24h']:,}")
                
                if raw_to_staging['unprocessed_24h'] > 0:
                    print(f"   ⚠️  Pipeline backlog detected: {raw_to_staging['unprocessed_24h']:,} unprocessed records")
                else:
                    print(f"   ✅ Pipeline current (no backlog)")
                    
            except Exception as e:
                print(f"❌ Data flow analysis error: {e}")
            
            # Data gaps identification
            print(f"\n🕳️  DATA GAPS ANALYSIS")
            print("=" * 50)
            
            # Check for missing recent data
            current_time = datetime.now()
            
            # Check each source for recent activity
            sources_to_check = [
                ('action_network_odds', 'Action Network'),
                ('action_network_history', 'Action Network History'),
                ('sbd_betting_splits', 'SBD'),
                ('vsin_data', 'VSIN')
            ]
            
            for table_name, source_name in sources_to_check:
                try:
                    # First check if table exists
                    table_exists = await conn.fetchval(f"""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables 
                            WHERE table_schema = 'raw_data' AND table_name = '{table_name}'
                        )
                    """)
                    
                    if not table_exists:
                        print(f"   {source_name}: 🔴 Table does not exist (raw_data.{table_name})")
                        continue
                    
                    # Check if created_at column exists
                    created_at_exists = await conn.fetchval(f"""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.columns 
                            WHERE table_schema = 'raw_data' AND table_name = '{table_name}' 
                            AND column_name = 'created_at'
                        )
                    """)
                    
                    # Check if collected_at column exists as fallback
                    collected_at_exists = await conn.fetchval(f"""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.columns 
                            WHERE table_schema = 'raw_data' AND table_name = '{table_name}' 
                            AND column_name = 'collected_at'
                        )
                    """)
                    
                    # Choose the appropriate timestamp column
                    timestamp_col = None
                    if created_at_exists:
                        timestamp_col = 'created_at'
                    elif collected_at_exists:
                        timestamp_col = 'collected_at'
                    
                    if not timestamp_col:
                        print(f"   {source_name}: 🟡 No timestamp column available")
                        continue
                    
                    latest_record = await conn.fetchrow(f"""
                        SELECT MAX({timestamp_col}) as latest, COUNT(*) as count_24h
                        FROM raw_data.{table_name}
                        WHERE {timestamp_col} > NOW() - INTERVAL '24 hours'
                    """)
                    
                    if latest_record and latest_record['latest']:
                        hours_ago = (current_time - latest_record['latest'].replace(tzinfo=None)).total_seconds() / 3600
                        status = "🟢 Current" if hours_ago < 2 else "🟡 Recent" if hours_ago < 12 else "🔴 Stale"
                        print(f"   {source_name}: {status} (last: {hours_ago:.1f}h ago, count: {latest_record['count_24h']})")
                    else:
                        print(f"   {source_name}: 🔴 No recent data")
                        
                except Exception as e:
                    print(f"   {source_name}: ❌ Error checking - {e}")
            
            print(f"\n✅ Pipeline assessment complete!")
            
    except Exception as e:
        print(f"❌ Assessment error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(assess_pipeline_data())