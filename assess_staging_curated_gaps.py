#!/usr/bin/env python3
"""
Assess STAGING → CURATED data processing gaps

Analyzes the flow from staging.action_network_odds_historical and staging.action_network_games
to curated.enhanced_games, curated.unified_betting_splits, and curated.ml_temporal_features
"""
from src.core.config import get_settings
from src.data.database.connection import initialize_connections, get_connection
import asyncio
from datetime import datetime, timedelta
import json

async def assess_staging_to_curated_gaps():
    try:
        config = get_settings()
        initialize_connections(config)
        print("🔍 Assessing STAGING → CURATED Data Processing Gaps\n")
        
        async with get_connection() as conn:
            
            # 1. STAGING Zone Data Analysis
            print("📊 STAGING ZONE ANALYSIS")
            print("=" * 60)
            
            # Staging games analysis
            staging_games = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_games,
                    COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '7 days') as recent_games,
                    COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '24 hours') as today_games,
                    MIN(created_at) as earliest_game,
                    MAX(created_at) as latest_game,
                    COUNT(DISTINCT home_team_normalized) as unique_home_teams,
                    COUNT(DISTINCT away_team_normalized) as unique_away_teams
                FROM staging.action_network_games
            """)
            
            print(f"🏟️  Staging Games:")
            print(f"   Total games: {staging_games['total_games']:,}")
            print(f"   Recent (7d): {staging_games['recent_games']:,}")
            print(f"   Today (24h): {staging_games['today_games']:,}")
            print(f"   Unique home teams: {staging_games['unique_home_teams']}")
            print(f"   Unique away teams: {staging_games['unique_away_teams']}")
            print(f"   Date range: {staging_games['earliest_game']} → {staging_games['latest_game']}")
            
            # Staging odds analysis
            staging_odds = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_odds,
                    COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '7 days') as recent_odds,
                    COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '24 hours') as today_odds,
                    COUNT(DISTINCT external_game_id) as unique_games,
                    COUNT(DISTINCT sportsbook_name) as unique_sportsbooks,
                    COUNT(DISTINCT market_type) as unique_markets,
                    MIN(created_at) as earliest_odds,
                    MAX(created_at) as latest_odds
                FROM staging.action_network_odds_historical
            """)
            
            print(f"\n📈 Staging Odds Historical:")
            print(f"   Total odds records: {staging_odds['total_odds']:,}")
            print(f"   Recent (7d): {staging_odds['recent_odds']:,}")
            print(f"   Today (24h): {staging_odds['today_odds']:,}")
            print(f"   Unique games: {staging_odds['unique_games']:,}")
            print(f"   Unique sportsbooks: {staging_odds['unique_sportsbooks']}")
            print(f"   Unique markets: {staging_odds['unique_markets']}")
            print(f"   Date range: {staging_odds['earliest_odds']} → {staging_odds['latest_odds']}")
            
            # Market type breakdown
            market_breakdown = await conn.fetch("""
                SELECT 
                    market_type,
                    side,
                    COUNT(*) as count,
                    COUNT(DISTINCT external_game_id) as unique_games,
                    COUNT(DISTINCT sportsbook_name) as unique_sportsbooks
                FROM staging.action_network_odds_historical 
                WHERE created_at > NOW() - INTERVAL '7 days'
                GROUP BY market_type, side
                ORDER BY market_type, side
            """)
            
            if market_breakdown:
                print(f"\n   Market Type Breakdown (7d):")
                for market in market_breakdown:
                    print(f"     {market['market_type']}.{market['side']}: {market['count']:,} records, "
                          f"{market['unique_games']} games, {market['unique_sportsbooks']} books")
            
            # 2. CURATED Zone Data Analysis
            print(f"\n📊 CURATED ZONE ANALYSIS")
            print("=" * 60)
            
            # Enhanced games analysis
            curated_games = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_games,
                    COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '7 days') as recent_games,
                    COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '24 hours') as today_games,
                    COUNT(*) FILTER (WHERE mlb_stats_api_game_id IS NOT NULL) as with_mlb_id,
                    COUNT(*) FILTER (WHERE action_network_game_id IS NOT NULL) as with_an_id,
                    COUNT(*) FILTER (WHERE feature_data IS NOT NULL) as with_features,
                    MIN(created_at) as earliest_game,
                    MAX(created_at) as latest_game
                FROM curated.enhanced_games
            """)
            
            print(f"🏟️  Enhanced Games:")
            print(f"   Total games: {curated_games['total_games']:,}")
            print(f"   Recent (7d): {curated_games['recent_games']:,}")
            print(f"   Today (24h): {curated_games['today_games']:,}")
            print(f"   With MLB Stats ID: {curated_games['with_mlb_id']:,}")
            print(f"   With Action Network ID: {curated_games['with_an_id']:,}")
            print(f"   With feature data: {curated_games['with_features']:,}")
            if curated_games['latest_game']:
                print(f"   Date range: {curated_games['earliest_game']} → {curated_games['latest_game']}")
            else:
                print(f"   ⚠️  No enhanced games found")
            
            # Unified betting splits analysis
            betting_splits = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_splits,
                    COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '7 days') as recent_splits,
                    COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '24 hours') as today_splits,
                    COUNT(DISTINCT game_id) as unique_games,
                    COUNT(DISTINCT data_source) as unique_sources,
                    COUNT(DISTINCT sportsbook_name) as unique_sportsbooks,
                    COUNT(*) FILTER (WHERE sharp_action_direction IS NOT NULL) as with_sharp_action,
                    MIN(created_at) as earliest_split,
                    MAX(created_at) as latest_split
                FROM curated.unified_betting_splits
            """)
            
            print(f"\n📊 Unified Betting Splits:")
            print(f"   Total splits: {betting_splits['total_splits']:,}")
            print(f"   Recent (7d): {betting_splits['recent_splits']:,}")
            print(f"   Today (24h): {betting_splits['today_splits']:,}")
            print(f"   Unique games: {betting_splits['unique_games']:,}")
            print(f"   Unique sources: {betting_splits['unique_sources']}")
            print(f"   Unique sportsbooks: {betting_splits['unique_sportsbooks']}")
            print(f"   With sharp action: {betting_splits['with_sharp_action']:,}")
            if betting_splits['latest_split']:
                print(f"   Date range: {betting_splits['earliest_split']} → {betting_splits['latest_split']}")
            else:
                print(f"   ⚠️  No betting splits found")
            
            # ML temporal features analysis
            ml_features = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_features,
                    COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '7 days') as recent_features,
                    COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '24 hours') as today_features,
                    COUNT(DISTINCT game_id) as unique_games,
                    COUNT(*) FILTER (WHERE feature_cutoff_time IS NOT NULL) as with_features,
                    COUNT(*) FILTER (WHERE minutes_before_game >= 60) as ml_valid_features,
                    MIN(created_at) as earliest_feature,
                    MAX(created_at) as latest_feature
                FROM curated.ml_temporal_features
            """)
            
            print(f"\n🤖 ML Temporal Features:")
            print(f"   Total features: {ml_features['total_features']:,}")
            print(f"   Recent (7d): {ml_features['recent_features']:,}")
            print(f"   Today (24h): {ml_features['today_features']:,}")
            print(f"   Unique games: {ml_features['unique_games']:,}")
            print(f"   With features: {ml_features['with_features']:,}")
            print(f"   ML valid (≥60min): {ml_features['ml_valid_features']:,}")
            if ml_features['latest_feature']:
                print(f"   Date range: {ml_features['earliest_feature']} → {ml_features['latest_feature']}")
            else:
                print(f"   ⚠️  No ML features found")
            
            # 3. GAP ANALYSIS
            print(f"\n🕳️  GAP ANALYSIS")
            print("=" * 60)
            
            # Game coverage gap
            game_coverage_gap = await conn.fetchrow("""
                SELECT 
                    sg.unique_games as staging_games,
                    COALESCE(cg.unique_games, 0) as curated_games,
                    sg.unique_games - COALESCE(cg.unique_games, 0) as missing_games,
                    CASE 
                        WHEN sg.unique_games > 0 THEN 
                            ROUND((COALESCE(cg.unique_games, 0)::numeric / sg.unique_games::numeric) * 100, 1)
                        ELSE 0 
                    END as coverage_percentage
                FROM 
                    (SELECT COUNT(DISTINCT external_game_id) as unique_games 
                     FROM staging.action_network_odds_historical 
                     WHERE created_at > NOW() - INTERVAL '7 days') sg
                CROSS JOIN
                    (SELECT COUNT(DISTINCT action_network_game_id) as unique_games 
                     FROM curated.enhanced_games 
                     WHERE created_at > NOW() - INTERVAL '7 days') cg
            """)
            
            print(f"🎯 Game Coverage Analysis (7d):")
            print(f"   STAGING unique games: {game_coverage_gap['staging_games']:,}")
            print(f"   CURATED unique games: {game_coverage_gap['curated_games']:,}")
            print(f"   Missing games: {game_coverage_gap['missing_games']:,}")
            print(f"   Coverage: {game_coverage_gap['coverage_percentage']}%")
            
            # Data processing lag analysis
            processing_lag = await conn.fetchrow("""
                SELECT 
                    MAX(s.created_at) as latest_staging,
                    MAX(c.created_at) as latest_curated,
                    EXTRACT(EPOCH FROM (MAX(s.created_at) - MAX(c.created_at)))/3600 as lag_hours
                FROM staging.action_network_odds_historical s
                CROSS JOIN curated.enhanced_games c
                WHERE s.created_at > NOW() - INTERVAL '7 days'
            """)
            
            if processing_lag['latest_staging'] and processing_lag['latest_curated']:
                print(f"\n⏱️  Processing Lag Analysis:")
                print(f"   Latest STAGING: {processing_lag['latest_staging']}")
                print(f"   Latest CURATED: {processing_lag['latest_curated']}")
                print(f"   Processing lag: {processing_lag['lag_hours']:.1f} hours")
                
                if processing_lag['lag_hours'] > 24:
                    print(f"   🔴 CRITICAL: Processing lag > 24 hours")
                elif processing_lag['lag_hours'] > 6:
                    print(f"   🟡 WARNING: Processing lag > 6 hours")
                else:
                    print(f"   🟢 OK: Processing lag acceptable")
            else:
                print(f"\n⏱️  Processing Lag Analysis:")
                print(f"   🔴 CRITICAL: No recent CURATED data found")
            
            # Missing pipeline components
            print(f"\n🔧 PIPELINE COMPONENT ANALYSIS")
            print("=" * 60)
            
            # Check for games that should be processed
            unprocessed_games = await conn.fetch("""
                SELECT 
                    s.external_game_id,
                    COUNT(DISTINCT s.sportsbook_name) as sportsbook_count,
                    COUNT(*) as odds_records,
                    MIN(s.created_at) as first_seen,
                    MAX(s.created_at) as last_seen,
                    CASE WHEN c.action_network_game_id IS NOT NULL THEN 'PROCESSED' ELSE 'MISSING' END as curated_status
                FROM staging.action_network_odds_historical s
                LEFT JOIN curated.enhanced_games c ON s.external_game_id = c.action_network_game_id
                WHERE s.created_at > NOW() - INTERVAL '7 days'
                GROUP BY s.external_game_id, c.action_network_game_id
                ORDER BY odds_records DESC, first_seen DESC
                LIMIT 10
            """)
            
            print(f"🎮 Unprocessed Games Sample (Top 10 by odds volume):")
            missing_count = 0
            for game in unprocessed_games:
                status_icon = "✅" if game['curated_status'] == 'PROCESSED' else "❌"
                print(f"   {status_icon} Game {game['external_game_id']}: {game['odds_records']} odds, "
                      f"{game['sportsbook_count']} books, {game['first_seen']} → {game['last_seen']}")
                if game['curated_status'] == 'MISSING':
                    missing_count += 1
            
            print(f"\n📊 Processing Status: {len(unprocessed_games) - missing_count}/{len(unprocessed_games)} games processed")
            
            # 4. RECOMMENDATIONS
            print(f"\n💡 RECOMMENDATIONS")
            print("=" * 60)
            
            # Critical issues
            if game_coverage_gap['coverage_percentage'] < 50:
                print(f"🔴 CRITICAL: Low game coverage ({game_coverage_gap['coverage_percentage']}%)")
                print(f"   → Implement automated STAGING → CURATED pipeline")
                print(f"   → Process recent {game_coverage_gap['missing_games']} missing games")
            
            if betting_splits['total_splits'] == 0:
                print(f"🔴 CRITICAL: No unified betting splits found")
                print(f"   → Implement betting splits aggregation from multiple sources")
                print(f"   → Process SBD and VSIN data into unified format")
            
            if ml_features['total_features'] == 0:
                print(f"🔴 CRITICAL: No ML temporal features found")
                print(f"   → Implement ML feature generation pipeline")
                print(f"   → Ensure 60-minute data cutoff for ML compliance")
            
            # Warnings
            if processing_lag.get('lag_hours', 0) > 6:
                print(f"🟡 WARNING: Processing lag > 6 hours")
                print(f"   → Implement real-time or scheduled STAGING → CURATED processing")
            
            # Opportunities
            if staging_odds['recent_odds'] > 1000:
                print(f"🟢 OPPORTUNITY: Rich staging data available ({staging_odds['recent_odds']:,} odds)")
                print(f"   → Process into ML-ready features")
                print(f"   → Generate sharp action indicators")
            
            # 5. IMPLEMENTATION TASKS
            print(f"\n📋 IMPLEMENTATION TASKS")
            print("=" * 60)
            
            print(f"**Immediate (HIGH Priority):**")
            print(f"1. Implement enhanced_games population from staging.action_network_games")
            print(f"2. Create STAGING → CURATED orchestrator service") 
            print(f"3. Process {game_coverage_gap['missing_games']} missing games")
            
            print(f"\n**Short-term (MEDIUM Priority):**")
            print(f"4. Implement unified_betting_splits aggregation")
            print(f"5. Create ML temporal features pipeline")
            print(f"6. Add automated scheduling for CURATED zone updates")
            
            print(f"\n**Long-term (LOW Priority):**")
            print(f"7. Implement real-time feature streaming")
            print(f"8. Add cross-source data validation")
            print(f"9. Create ML model training pipeline integration")
            
            print(f"\n✅ STAGING → CURATED gap analysis complete!")
            
    except Exception as e:
        print(f"❌ Assessment error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(assess_staging_to_curated_gaps())