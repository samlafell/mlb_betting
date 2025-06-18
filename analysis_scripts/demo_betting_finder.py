#!/usr/bin/env python3
"""
Demo Betting Opportunities Finder
=================================
Demonstrates how the betting opportunity finder works by simulating
games with sharp action starting within 5 minutes.

This shows exactly how you would identify betting signals after running entrypoint.py.
"""

from datetime import datetime, timedelta
import duckdb
from pathlib import Path

def create_demo_data(conn):
    """Create sample data showing games starting soon with sharp action."""
    
    # Get current time and create games starting soon
    now = datetime.now()
    game1_time = now + timedelta(minutes=3)  # Game starting in 3 minutes
    game2_time = now + timedelta(minutes=8)  # Game starting in 8 minutes
    
    print("📊 Creating demo data with games starting soon...")
    
    # Create demo data directly in the database
    demo_data = [
        # Game 1: Pirates @ Tigers - STRONG SHARP ACTION (Steam Move)
        ('PIT_DET_DEMO_1', 'Pirates', 'Tigers', game1_time, 'moneyline', None, 
         35.2, 68.4, 'vsin', 'circa', now - timedelta(minutes=30)),  # 33.2% differential - STRONG
        
        ('PIT_DET_DEMO_1', 'Pirates', 'Tigers', game1_time, 'spread', '-1.5',
         42.1, 59.3, 'vsin', 'circa', now - timedelta(minutes=25)),  # 17.2% differential - STRONG
        
        ('PIT_DET_DEMO_1', 'Pirates', 'Tigers', game1_time, 'total', '8.5',
         71.8, 58.2, 'vsin', 'circa', now - timedelta(minutes=20)),  # 13.6% differential - MODERATE
        
        # Game 2: Yankees @ Red Sox - MODERATE SHARP ACTION
        ('NYY_BOS_DEMO_2', 'Yankees', 'Red Sox', game2_time, 'moneyline', None,
         58.7, 47.2, 'vsin', 'circa', now - timedelta(minutes=45)),  # 11.5% differential - MODERATE
        
        ('NYY_BOS_DEMO_2', 'Yankees', 'Red Sox', game2_time, 'spread', '+1.5', 
         41.3, 52.8, 'vsin', 'circa', now - timedelta(minutes=40)),  # -11.5% differential - MODERATE
        
        ('NYY_BOS_DEMO_2', 'Yankees', 'Red Sox', game2_time, 'total', '9.0',
         48.9, 51.1, 'vsin', 'circa', now - timedelta(minutes=35)),  # -2.2% differential - WEAK
    ]
    
    # Insert demo data
    insert_query = """
    INSERT INTO mlb_betting.splits.raw_mlb_betting_splits 
    (game_id, away_team, home_team, game_datetime, split_type, split_value,
     away_or_under_bets_percentage, home_or_over_bets_percentage, 
     source, book, last_updated,
     away_or_under_stake_percentage, home_or_over_stake_percentage)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    for data in demo_data:
        game_id, away_team, home_team, game_datetime, split_type, split_value, \
        away_stake_pct, home_stake_pct, source, book, last_updated = data
        
        away_bet_pct = 100 - home_stake_pct  # Calculate complementary percentages
        home_bet_pct = home_stake_pct
        away_stake_pct_calc = 100 - away_stake_pct
        
        conn.execute(insert_query, [
            game_id, away_team, home_team, game_datetime, split_type, split_value,
            away_bet_pct, home_bet_pct, source, book, last_updated,
            away_stake_pct_calc, away_stake_pct
        ])
    
    print(f"✅ Created demo data for {len(set(d[0] for d in demo_data))} games")

def run_demo_finder(conn):
    """Run the betting opportunity finder on demo data."""
    
    print("\n🔍 Searching for betting opportunities...")
    
    # Same query as the main script, but adjusted for demo
    query = """
    WITH upcoming_games AS (
        SELECT DISTINCT 
            game_id,
            home_team,
            away_team,
            game_datetime,
            EXTRACT('epoch' FROM (game_datetime - NOW())) / 60 AS minutes_to_game
        FROM mlb_betting.splits.raw_mlb_betting_splits
        WHERE game_datetime BETWEEN NOW() AND NOW() + INTERVAL '10 minutes'
          AND game_datetime > NOW()
          AND game_id LIKE '%DEMO%'  -- Only demo data
    ),
    
    current_splits AS (
        SELECT 
            rmbs.game_id,
            ug.home_team,
            ug.away_team,
            ug.game_datetime,
            ug.minutes_to_game,
            rmbs.split_type,
            rmbs.split_value,
            rmbs.home_or_over_stake_percentage as stake_pct,
            rmbs.home_or_over_bets_percentage as bet_pct,
            rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage as differential,
            rmbs.last_updated,
            rmbs.source,
            rmbs.book,
            
            CASE 
                WHEN ABS(rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage) >= 15 THEN 'STRONG'
                WHEN ABS(rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage) >= 10 THEN 'MODERATE'
                WHEN ABS(rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage) >= 5 THEN 'WEAK'
                ELSE 'NONE'
            END as signal_strength,
            
            CASE 
                WHEN rmbs.split_type IN ('moneyline', 'spread') THEN
                    CASE WHEN rmbs.home_or_over_stake_percentage > rmbs.home_or_over_bets_percentage 
                         THEN CONCAT('BET ', ug.home_team)
                         ELSE CONCAT('BET ', ug.away_team) END
                WHEN rmbs.split_type = 'total' THEN
                    CASE WHEN rmbs.home_or_over_stake_percentage > rmbs.home_or_over_bets_percentage 
                         THEN 'BET OVER'
                         ELSE 'BET UNDER' END
            END as bet_recommendation,
            
            CASE 
                WHEN EXTRACT('epoch' FROM (rmbs.game_datetime - rmbs.last_updated)) / 3600 <= 2 
                     AND ABS(rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage) >= 12
                THEN 'STEAM_MOVE'
                ELSE 'NORMAL'
            END as timing_pattern
            
        FROM mlb_betting.splits.raw_mlb_betting_splits rmbs
        JOIN upcoming_games ug ON rmbs.game_id = ug.game_id
    ),
    
    betting_opportunities AS (
        SELECT *,
            CASE 
                WHEN signal_strength = 'STRONG' AND timing_pattern = 'STEAM_MOVE' THEN '🔥 STEAM MOVE - VERY HIGH'
                WHEN signal_strength = 'STRONG' THEN '🟢 HIGH CONFIDENCE'
                WHEN signal_strength = 'MODERATE' AND timing_pattern = 'STEAM_MOVE' THEN '🔥 STEAM MOVE - HIGH'
                WHEN signal_strength = 'MODERATE' THEN '🟡 MEDIUM CONFIDENCE'
                WHEN signal_strength = 'WEAK' AND timing_pattern = 'STEAM_MOVE' THEN '🟡 STEAM MOVE - MEDIUM'
                ELSE '🔴 LOW CONFIDENCE'
            END as confidence_level,
            
            CASE 
                WHEN signal_strength = 'STRONG' AND timing_pattern = 'STEAM_MOVE' THEN 65.0
                WHEN signal_strength = 'STRONG' THEN 58.0
                WHEN signal_strength = 'MODERATE' AND timing_pattern = 'STEAM_MOVE' THEN 60.0
                WHEN signal_strength = 'MODERATE' THEN 54.0
                ELSE 51.0
            END as estimated_win_rate
            
        FROM current_splits
        WHERE signal_strength IN ('STRONG', 'MODERATE')
           OR timing_pattern = 'STEAM_MOVE'
    )
    
    SELECT 
        game_id,
        home_team,
        away_team,
        ROUND(minutes_to_game, 0) as minutes_to_game,
        split_type,
        split_value,
        bet_recommendation,
        confidence_level,
        signal_strength,
        timing_pattern,
        ROUND(stake_pct, 1) as stake_pct,
        ROUND(bet_pct, 1) as bet_pct,
        ROUND(differential, 1) as differential,
        ROUND(estimated_win_rate, 1) as est_win_rate,
        source,
        book,
        last_updated
    FROM betting_opportunities
    ORDER BY 
        minutes_to_game ASC,
        ABS(differential) DESC,
        estimated_win_rate DESC
    """
    
    results = conn.execute(query).fetchall()
    
    if not results:
        print("🚫 No betting opportunities found in demo data.")
        return
    
    print(f"\n🎯 FOUND {len(results)} BETTING OPPORTUNITIES")
    print("=" * 80)
    
    current_game = None
    opportunity_count = 0
    
    for row in results:
        game_id, home_team, away_team, minutes_to_game, split_type, split_value, \
        bet_rec, confidence, signal_strength, timing_pattern, stake_pct, bet_pct, \
        differential, est_win_rate, source, book, last_updated = row
        
        # Group by game
        game_name = f"{away_team} @ {home_team}"
        if current_game != game_name:
            if current_game is not None:
                print()
            current_game = game_name
            print(f"\n🏟️  {game_name} (starts in {int(minutes_to_game)} minutes)")
            print("-" * 60)
        
        opportunity_count += 1
        
        # Format the opportunity
        confidence_emoji = "🔥" if "STEAM" in confidence else "🟢" if "HIGH" in confidence else "🟡"
        timing_emoji = "⚡" if timing_pattern == 'STEAM_MOVE' else "📊"
        
        bet_type_display = f"{split_type.upper()}"
        if split_value:
            bet_type_display += f" ({split_value})"
        
        roi_estimate = (est_win_rate/100 * 90.91) - ((100-est_win_rate)/100 * 100)
        
        print(f"  {confidence_emoji} {timing_emoji} {bet_type_display}")
        print(f"     💰 RECOMMENDATION: {bet_rec}")
        print(f"     📈 Confidence: {confidence}")
        print(f"     📊 Signal: {stake_pct}% money vs {bet_pct}% bets ({differential:+.1f}% diff)")
        print(f"     🎯 Est. Win Rate: {est_win_rate}% | ROI: ${roi_estimate:.2f} per $100")
        print(f"     📍 Source: {source.upper()}-{book.upper()} | Updated: {last_updated.strftime('%H:%M')}")
        
        if timing_pattern == 'STEAM_MOVE':
            print(f"     🔥 STEAM MOVE DETECTED - ACT FAST!")
    
    # Summary and guidelines
    print(f"\n{'='*80}")
    print(f"📊 SUMMARY: Found {opportunity_count} opportunities across {len(set(r[1] + ' @ ' + r[2] for r in results))} games")
    
    steam_moves = sum(1 for r in results if r[9] == 'STEAM_MOVE')
    high_conf = sum(1 for r in results if 'HIGH' in r[7])
    
    if steam_moves > 0:
        print(f"🔥 {steam_moves} STEAM MOVES detected - these are time-sensitive!")
    if high_conf > 0:
        print(f"🟢 {high_conf} HIGH CONFIDENCE signals")
    
    print(f"\n⚠️  EXECUTION GUIDELINES:")
    print(f"   • Verify current odds at your sportsbook")
    print(f"   • Steam moves require immediate action")
    print(f"   • Use 1-3% of bankroll per bet maximum")
    print(f"   • Line shop across multiple books if possible")
    print(f"   • Track results to validate strategy performance")

def main():
    """Main demo function."""
    
    db_path = "data/raw/mlb_betting.duckdb"
    
    if not Path(db_path).exists():
        print(f"❌ Database not found: {db_path}")
        print("💡 Run this first: uv run src/mlb_sharp_betting/entrypoint.py")
        return
    
    print("🎮 BETTING OPPORTUNITIES DEMO")
    print("="*50)
    print("This demonstrates how to identify betting opportunities")
    print("after running entrypoint.py to collect current data.\n")
    
    conn = duckdb.connect(db_path)
    
    try:
        # Create demo data
        create_demo_data(conn)
        
        # Run the opportunity finder
        run_demo_finder(conn)
        
        print(f"\n{'='*80}")
        print("🎯 HOW THIS WORKS IN PRACTICE:")
        print("="*80)
        print("1. Run entrypoint.py to collect current betting data:")
        print("   uv run src/mlb_sharp_betting/entrypoint.py --sportsbook circa")
        print()
        print("2. Within 5 minutes of game time, run the opportunity finder:")
        print("   uv run analysis_scripts/quick_bet_finder.py --minutes 5")
        print()
        print("3. The script will identify current sharp action and steam moves")
        print("4. Act quickly on high-confidence signals, especially steam moves")
        print("5. Always verify odds at your sportsbook before betting")
        print()
        print("💡 TIP: Set up a cron job to run entrypoint.py every 15-30 minutes")
        print("   to keep your data fresh for opportunity detection.")
        
    except Exception as e:
        print(f"❌ Demo error: {e}")
    
    finally:
        # Clean up demo data
        try:
            conn.execute("DELETE FROM mlb_betting.splits.raw_mlb_betting_splits WHERE game_id LIKE '%DEMO%'")
            print(f"\n🧹 Cleaned up demo data")
        except:
            pass
        conn.close()

if __name__ == "__main__":
    main() 