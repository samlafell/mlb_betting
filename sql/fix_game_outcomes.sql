-- Add game outcomes for synthetic test data (corrected version)
-- This allows the processors to analyze completed games during backtesting

-- Insert into game_outcomes table for backtesting analysis
INSERT INTO public.game_outcomes (
    game_id, home_team, away_team, game_date, 
    home_score, away_score,
    home_win, winning_team, run_differential,
    created_at, updated_at
) VALUES 
    -- SHARP ACTION: NYY won (sharp money was RIGHT) 
    ('test_sharp_action_001', 'BOS', 'NYY', CURRENT_DATE - INTERVAL '1 day', 
     4, 7, false, 'NYY', 3, NOW(), NOW()),
     
    -- OPPOSING MARKETS: LAD won (moneyline signal was RIGHT)
    ('test_opposing_markets_001', 'SD', 'LAD', CURRENT_DATE - INTERVAL '1 day',
     3, 8, false, 'LAD', 5, NOW(), NOW()),
     
    -- BOOK CONFLICTS: HOU won (sharper book line was RIGHT)
    ('test_book_conflicts_001', 'TEX', 'HOU', CURRENT_DATE - INTERVAL '1 day',
     2, 6, false, 'HOU', 4, NOW(), NOW()),
     
    -- PUBLIC FADE: ATL won (fade public NYM was RIGHT)
    ('test_public_fade_001', 'ATL', 'NYM', CURRENT_DATE - INTERVAL '1 day',
     5, 3, true, 'ATL', 2, NOW(), NOW()),
     
    -- LATE FLIP: CHC won (late money flip was RIGHT)
    ('test_late_flip_001', 'STL', 'CHC', CURRENT_DATE - INTERVAL '1 day',
     4, 9, false, 'CHC', 5, NOW(), NOW()),
     
    -- CONSENSUS: SF won (consensus was RIGHT)
    ('test_consensus_001', 'COL', 'SF', CURRENT_DATE - INTERVAL '1 day',
     1, 12, false, 'SF', 11, NOW(), NOW()),
     
    -- UNDERDOG VALUE: MIA won (underdog sharp money was RIGHT)
    ('test_underdog_value_001', 'PHI', 'MIA', CURRENT_DATE - INTERVAL '1 day',
     3, 7, false, 'MIA', 4, NOW(), NOW()),
     
    -- TIMING: LAA won (ultra-late sharp action was RIGHT)
    ('test_timing_001', 'OAK', 'LAA', CURRENT_DATE - INTERVAL '1 day',
     2, 8, false, 'LAA', 6, NOW(), NOW()),
     
    -- LINE MOVEMENT: MIL won (heavy action was RIGHT)
    ('test_line_movement_001', 'PIT', 'MIL', CURRENT_DATE - INTERVAL '1 day',
     4, 11, false, 'MIL', 7, NOW(), NOW()),
     
    -- HYBRID: BAL won (hybrid signal was RIGHT)
    ('test_hybrid_001', 'CLE', 'BAL', CURRENT_DATE - INTERVAL '1 day',
     3, 9, false, 'BAL', 6, NOW(), NOW())
     
ON CONFLICT (game_id) DO UPDATE SET
    updated_at = NOW();

-- Show results
SELECT 'GAME OUTCOMES ADDED FOR SYNTHETIC DATA' as status;
SELECT COUNT(*) as outcomes_count FROM public.game_outcomes WHERE game_id LIKE 'test_%';

-- Verify data is ready for backtesting
SELECT 
    go.game_id,
    go.home_team || ' vs ' || go.away_team as matchup,
    go.winning_team as winner,
    COUNT(s.id) as splits_count
FROM public.game_outcomes go
LEFT JOIN splits.raw_mlb_betting_splits s ON go.game_id = s.game_id AND s.source = 'SYNTHETIC_TEST'
WHERE go.game_id LIKE 'test_%'
GROUP BY go.game_id, go.home_team, go.away_team, go.winning_team
ORDER BY go.game_id; 