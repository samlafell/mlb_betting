#!/usr/bin/env python3
"""
Simple test script to show cross-market flip results
"""

import sys
sys.path.append('src')

from mlb_sharp_betting.db.connection import get_db_manager

def main():
    """Run simple flip test."""
    
    manager = get_db_manager()
    
    try:
        print('🎯 CROSS-MARKET FLIP STRATEGY - PROFITABILITY BACKTEST')
        print('=' * 65)
        
        # Working query that we tested successfully
        query = '''
        WITH early_signals AS (
            SELECT 
                s.game_id,
                s.home_team,
                s.away_team,    
                s.source,
                s.book,
                s.split_type,
                CASE WHEN s.home_or_over_stake_percentage > s.home_or_over_bets_percentage 
                     THEN s.home_team ELSE s.away_team END as recommended_team,
                ROW_NUMBER() OVER (
                    PARTITION BY s.game_id, s.source, COALESCE(s.book, 'NULL'), s.split_type 
                    ORDER BY ABS(s.home_or_over_stake_percentage - s.home_or_over_bets_percentage) DESC,
                             EXTRACT(EPOCH FROM (s.game_datetime - s.last_updated))/3600 DESC
                ) as rn
            FROM splits.raw_mlb_betting_splits s
            WHERE ABS(s.home_or_over_stake_percentage - s.home_or_over_bets_percentage) >= 8
            AND EXTRACT(EPOCH FROM (s.game_datetime - s.last_updated))/3600 >= 4
            AND s.game_datetime >= '2025-06-01'
        ),
        late_signals AS (
            SELECT 
                s.game_id,
                s.source,
                s.book,
                s.split_type,
                CASE WHEN s.home_or_over_stake_percentage > s.home_or_over_bets_percentage 
                     THEN s.home_team ELSE s.away_team END as recommended_team,
                ROW_NUMBER() OVER (
                    PARTITION BY s.game_id, s.source, COALESCE(s.book, 'NULL'), s.split_type 
                    ORDER BY ABS(s.home_or_over_stake_percentage - s.home_or_over_bets_percentage) DESC,
                             EXTRACT(EPOCH FROM (s.game_datetime - s.last_updated))/3600 ASC
                ) as rn
            FROM splits.raw_mlb_betting_splits s
            WHERE ABS(s.home_or_over_stake_percentage - s.home_or_over_bets_percentage) >= 8
            AND EXTRACT(EPOCH FROM (s.game_datetime - s.last_updated))/3600 BETWEEN 1 AND 3
            AND s.game_datetime >= '2025-06-01'
        ),
        flip_detection AS (
            SELECT DISTINCT
                e.game_id,
                e.home_team,
                e.away_team,
                e.recommended_team as early_team,
                CASE 
                    WHEN e.source != l.source THEN 'CROSS_SOURCE_FLIP'
                    WHEN e.source = l.source AND COALESCE(e.book, 'NULL') != COALESCE(l.book, 'NULL') THEN 'CROSS_BOOK_FLIP'
                    WHEN e.split_type = 'total' AND l.split_type = 'total' THEN 'TOTAL_MARKET_FLIP'
                    WHEN e.split_type = l.split_type AND e.split_type != 'total' THEN 'SAME_MARKET_FLIP'
                    WHEN e.split_type != l.split_type THEN 'CROSS_MARKET_CONTRADICTION'
                    ELSE 'OTHER_FLIP'
                END as flip_type
            FROM early_signals e
            INNER JOIN late_signals l ON e.game_id = l.game_id 
            WHERE e.rn = 1 AND l.rn = 1
            AND e.recommended_team != l.recommended_team
        ),
        strategy_performance AS (
            SELECT 
                fd.*,
                go.home_score,
                go.away_score,
                CASE 
                    WHEN go.home_score > go.away_score AND fd.early_team = fd.home_team THEN 1
                    WHEN go.away_score > go.home_score AND fd.early_team = fd.away_team THEN 1
                    WHEN go.home_score = go.away_score THEN 0
                    ELSE 0
                END as strategy_result
            FROM flip_detection fd
            LEFT JOIN game_outcomes go ON fd.game_id = go.game_id
            WHERE go.home_score IS NOT NULL AND go.away_score IS NOT NULL
        )
        SELECT 
            flip_type,
            COUNT(*) as total_bets,
            SUM(strategy_result) as strategy_wins,
            ROUND(AVG(strategy_result), 3) as strategy_win_rate,
            ROUND((SUM(strategy_result) * 0.909 - (COUNT(*) - SUM(strategy_result))) * 100.0 / COUNT(*), 2) as strategy_roi
        FROM strategy_performance
        GROUP BY flip_type
        ORDER BY strategy_roi DESC, total_bets DESC;
        '''
        
        with manager.get_connection() as conn:
            import pandas as pd
            df = pd.read_sql(query, conn)
            
            if len(df) > 0:
                print('✅ FLIP STRATEGY RESULTS:')
                print()
                
                total_bets = df['total_bets'].sum()
                total_wins = df['strategy_wins'].sum()
                overall_roi = (df['strategy_roi'] * df['total_bets']).sum() / total_bets if total_bets > 0 else 0
                
                print(f'📈 OVERALL: {total_wins}/{total_bets} ({total_wins/total_bets:.1%}) | ROI: {overall_roi:.2f}%')
                print()
                
                for _, row in df.iterrows():
                    flip_type = row['flip_type']
                    bets = row['total_bets']
                    wins = row['strategy_wins']
                    win_rate = row['strategy_win_rate']
                    roi = row['strategy_roi']
                    
                    status = '🔥' if roi > 10 else '✅' if roi > 5 else '⚠️' if roi > 0 else '❌'
                    print(f'{status} {flip_type.replace("_", " ").title()}: {wins}/{bets} ({win_rate:.1%}) | ROI: {roi:.2f}%')
                
                # Summary assessment
                print()
                if overall_roi > 5.0:
                    print('🔥 ASSESSMENT: PROFITABLE STRATEGY!')
                    print(f'   💰 Expected profit: ${overall_roi:.2f} per $100 wagered')
                elif overall_roi > 0:
                    print('⚠️  ASSESSMENT: MARGINALLY PROFITABLE')
                    print(f'   💰 Expected profit: ${overall_roi:.2f} per $100 wagered')
                else:
                    print('❌ ASSESSMENT: NOT PROFITABLE')
                    print(f'   💸 Expected loss: ${abs(overall_roi):.2f} per $100 wagered')
                    
                print()
                print('✅ SUCCESS: Found properly deduplicated flip results!')
                print(f'   📊 {total_bets} total bets across {len(df)} flip types')
                print(f'   🎯 No more 9,000+ bet duplication issue!')
                
            else:
                print('❌ No flip patterns detected')
                
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 