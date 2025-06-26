#!/usr/bin/env python3
"""
Simple Total Market Flip Analysis
Finds and validates profitability of total market flips
"""

import sys
sys.path.insert(0, '../src')

from mlb_sharp_betting.db.connection import get_db_manager

def main():
    manager = get_db_manager()
    
    try:
        print('🎯 SIMPLE TOTAL MARKET FLIP ANALYSIS')
        print('=' * 50)
        
        # Simple approach: Find games where early and late signals differ
        query = '''
        WITH game_signals AS (
            SELECT 
                s.game_id,
                s.home_team,
                s.away_team,
                s.source,
                s.book,
                CASE WHEN s.home_or_over_stake_percentage > s.home_or_over_bets_percentage 
                     THEN 'OVER' ELSE 'UNDER' END as recommended_team,
                s.home_or_over_stake_percentage - s.home_or_over_bets_percentage as differential,
                EXTRACT(EPOCH FROM (s.game_datetime - s.last_updated))/3600 as hours_before_game,
                CASE 
                    WHEN EXTRACT(EPOCH FROM (s.game_datetime - s.last_updated))/3600 >= 4 THEN 'EARLY'
                    WHEN EXTRACT(EPOCH FROM (s.game_datetime - s.last_updated))/3600 BETWEEN 1 AND 3 THEN 'LATE'
                    ELSE 'OTHER'
                END as timing_category
            FROM splits.raw_mlb_betting_splits s
            WHERE s.split_type = 'total'
            AND ABS(s.home_or_over_stake_percentage - s.home_or_over_bets_percentage) >= 5
            AND s.game_datetime >= '2025-06-16'
            AND s.source = 'VSIN' AND s.book = 'circa'  -- Start with known data
        ),
        early_late_pairs AS (
            SELECT 
                e.game_id,
                e.home_team,
                e.away_team,
                e.source,
                e.book,
                e.recommended_team as early_team,
                e.differential as early_diff,
                e.hours_before_game as early_hours,
                l.recommended_team as late_team,
                l.differential as late_diff,
                l.hours_before_game as late_hours
            FROM game_signals e
            INNER JOIN game_signals l ON e.game_id = l.game_id 
                                      AND e.source = l.source 
                                      AND COALESCE(e.book, 'NULL') = COALESCE(l.book, 'NULL')
            WHERE e.timing_category = 'EARLY' 
            AND l.timing_category = 'LATE'
            AND e.recommended_team != l.recommended_team  -- Only flips
        ),
        flip_results AS (
            SELECT 
                fp.*,
                go.home_score,
                go.away_score,
                go.home_score + go.away_score as total_runs,
                go.total_line,
                go.over as actual_over,
                -- Strategy: Follow early signal (fade the late movement)
                CASE 
                    WHEN fp.early_team = 'OVER' AND go.over = true THEN 1
                    WHEN fp.early_team = 'UNDER' AND go.over = false THEN 1
                    ELSE 0
                END as strategy_result
            FROM early_late_pairs fp
            LEFT JOIN game_outcomes go ON fp.game_id = go.game_id
            WHERE go.home_score IS NOT NULL AND go.away_score IS NOT NULL
            AND go.total_line IS NOT NULL
        )
        SELECT 
            source,
            COALESCE(book, 'NULL') as book,
            source || '-' || COALESCE(book, 'NULL') as combo,
            COUNT(*) as total_flips,
            SUM(strategy_result) as wins,
            ROUND((SUM(strategy_result)::numeric / COUNT(*)::numeric), 3) as win_rate,
            ROUND(((SUM(strategy_result) * 0.909 - (COUNT(*) - SUM(strategy_result))) * 100.0 / COUNT(*))::numeric, 2) as roi,
            ROUND(AVG(ABS(early_diff))::numeric, 1) as avg_early_strength,
            ROUND(AVG(ABS(late_diff))::numeric, 1) as avg_late_strength,
            COUNT(*) FILTER (WHERE early_team = 'OVER') as over_early,
            COUNT(*) FILTER (WHERE early_team = 'UNDER') as under_early,
            -- Sample games for verification
            STRING_AGG(DISTINCT home_team || ' @ ' || away_team, ', ' ORDER BY home_team || ' @ ' || away_team) as sample_games
        FROM flip_results
        GROUP BY source, book
        ORDER BY total_flips DESC;
        '''
        
        import pandas as pd
        with manager.get_connection() as conn:
            df = pd.read_sql(query, conn)
        
        if len(df) > 0:
            print(f'🔍 Found {len(df)} source/book combinations with total flips')
            print()
            
            for _, row in df.iterrows():
                combo = row['combo']
                flips = row['total_flips']
                wins = row['wins']
                win_rate = row['win_rate']
                roi = row['roi']
                early_strength = row['avg_early_strength']
                late_strength = row['avg_late_strength']
                over_early = row['over_early']
                under_early = row['under_early']
                sample_games = row['sample_games']
                
                if roi > 5:
                    status = '🔥 HIGHLY PROFITABLE'
                elif roi > 0:
                    status = '✅ PROFITABLE'
                else:
                    status = '❌ UNPROFITABLE'
                
                print(f'{status}: {combo}')
                print(f'   📊 Performance: {wins}/{flips} ({win_rate:.1%}) | ROI: {roi:.2f}%')
                print(f'   📈 Signal Strength: Early {early_strength:.1f}% vs Late {late_strength:.1f}%')
                print(f'   🎯 Direction: {over_early} OVER vs {under_early} UNDER early signals')
                print(f'   🎮 Sample Games: {sample_games[:100]}{"..." if len(sample_games) > 100 else ""}')
                print()
                
                # Recommendation
                if roi > 0:
                    print(f'✅ APPROVE {combo} for total flip recommendations')
                else:
                    print(f'❌ BAN {combo} from total flip recommendations')
                print()
        else:
            print('❌ No total flips found for VSIN-Circa')
            print('💡 Let me check other source/book combinations...')
            
            # Check all combinations
            query_all = '''
            WITH game_signals AS (
                SELECT 
                    s.game_id,
                    s.source,
                    s.book,
                    CASE WHEN s.home_or_over_stake_percentage > s.home_or_over_bets_percentage 
                         THEN 'OVER' ELSE 'UNDER' END as recommended_team,
                    EXTRACT(EPOCH FROM (s.game_datetime - s.last_updated))/3600 as hours_before_game,
                    CASE 
                        WHEN EXTRACT(EPOCH FROM (s.game_datetime - s.last_updated))/3600 >= 4 THEN 'EARLY'
                        WHEN EXTRACT(EPOCH FROM (s.game_datetime - s.last_updated))/3600 BETWEEN 1 AND 3 THEN 'LATE'
                        ELSE 'OTHER'
                    END as timing_category
                FROM splits.raw_mlb_betting_splits s
                WHERE s.split_type = 'total'
                AND ABS(s.home_or_over_stake_percentage - s.home_or_over_bets_percentage) >= 5
                AND s.game_datetime >= '2025-06-16'
            ),
            flip_counts AS (
                SELECT 
                    e.source,
                    e.book,
                    COUNT(*) as potential_flips
                FROM game_signals e
                INNER JOIN game_signals l ON e.game_id = l.game_id 
                                          AND e.source = l.source 
                                          AND COALESCE(e.book, 'NULL') = COALESCE(l.book, 'NULL')
                WHERE e.timing_category = 'EARLY' 
                AND l.timing_category = 'LATE'
                AND e.recommended_team != l.recommended_team  -- Only flips
                GROUP BY e.source, e.book
            )
            SELECT 
                source,
                COALESCE(book, 'NULL') as book,
                source || '-' || COALESCE(book, 'NULL') as combo,
                potential_flips
            FROM flip_counts
            ORDER BY potential_flips DESC;
            '''
            
            with manager.get_connection() as conn:
                df_all = pd.read_sql(query_all, conn)
                
            if len(df_all) > 0:
                print('📊 TOTAL FLIP POTENTIAL BY SOURCE/BOOK:')
                for _, row in df_all.iterrows():
                    print(f'   • {row["combo"]}: {row["potential_flips"]} potential flips')
            else:
                print('❌ No total flips found in any source/book combination')
                
    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        manager.close()

if __name__ == "__main__":
    main() 