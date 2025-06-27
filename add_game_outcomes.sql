-- Add game outcomes for synthetic test data
-- This allows the processors to analyze completed games during backtesting

-- Insert into game_outcomes table for backtesting analysis
INSERT INTO public.game_outcomes (
    game_id, home_team, away_team, game_date, 
    home_score, away_score, total_score,
    home_ml_result, away_ml_result,
    created_at, updated_at
) VALUES 
    -- SHARP ACTION: NYY won (sharp money was RIGHT) 
    ('test_sharp_action_001', 'BOS', 'NYY', CURRENT_DATE - INTERVAL '1 day', 
     4, 7, 11, 'LOSS', 'WIN', NOW(), NOW()),
     
    -- OPPOSING MARKETS: LAD won (moneyline signal was RIGHT)
    ('test_opposing_markets_001', 'SD', 'LAD', CURRENT_DATE - INTERVAL '1 day',
     3, 8, 11, 'LOSS', 'WIN', NOW(), NOW()),
     
    -- BOOK CONFLICTS: HOU won (sharper book line was RIGHT)
    ('test_book_conflicts_001', 'TEX', 'HOU', CURRENT_DATE - INTERVAL '1 day',
     2, 6, 8, 'LOSS', 'WIN', NOW(), NOW()),
     
    -- PUBLIC FADE: ATL won (fade public NYM was RIGHT)
    ('test_public_fade_001', 'ATL', 'NYM', CURRENT_DATE - INTERVAL '1 day',
     5, 3, 8, 'WIN', 'LOSS', NOW(), NOW()),
     
    -- LATE FLIP: CHC won (late money flip was RIGHT)
    ('test_late_flip_001', 'STL', 'CHC', CURRENT_DATE - INTERVAL '1 day',
     4, 9, 13, 'LOSS', 'WIN', NOW(), NOW()),
     
    -- CONSENSUS: SF won (consensus was RIGHT)
    ('test_consensus_001', 'COL', 'SF', CURRENT_DATE - INTERVAL '1 day',
     1, 12, 13, 'LOSS', 'WIN', NOW(), NOW()),
     
    -- UNDERDOG VALUE: MIA won (underdog sharp money was RIGHT)
    ('test_underdog_value_001', 'PHI', 'MIA', CURRENT_DATE - INTERVAL '1 day',
     3, 7, 10, 'LOSS', 'WIN', NOW(), NOW()),
     
    -- TIMING: LAA won (ultra-late sharp action was RIGHT)
    ('test_timing_001', 'OAK', 'LAA', CURRENT_DATE - INTERVAL '1 day',
     2, 8, 10, 'LOSS', 'WIN', NOW(), NOW()),
     
    -- LINE MOVEMENT: MIL won (heavy action was RIGHT)
    ('test_line_movement_001', 'PIT', 'MIL', CURRENT_DATE - INTERVAL '1 day',
     4, 11, 15, 'LOSS', 'WIN', NOW(), NOW()),
     
    -- HYBRID: BAL won (hybrid signal was RIGHT)
    ('test_hybrid_001', 'CLE', 'BAL', CURRENT_DATE - INTERVAL '1 day',
     3, 9, 12, 'LOSS', 'WIN', NOW(), NOW())
     
ON CONFLICT (game_id) DO UPDATE SET
    updated_at = NOW();

-- Also update the synthetic games to be completed (change from future to past)
UPDATE splits.games 
SET game_datetime = CURRENT_DATE - INTERVAL '1 day' + INTERVAL '19:00'
WHERE game_id LIKE 'test_%';

-- Update the splits to match the completed game times
UPDATE splits.raw_mlb_betting_splits 
SET game_datetime = CURRENT_DATE - INTERVAL '1 day' + INTERVAL '19:00'
WHERE source = 'SYNTHETIC_TEST';

-- Show results
SELECT 'GAME OUTCOMES ADDED FOR SYNTHETIC DATA' as status;
SELECT COUNT(*) as outcomes_count FROM public.game_outcomes WHERE game_id LIKE 'test_%';
SELECT COUNT(*) as games_updated FROM splits.games WHERE game_id LIKE 'test_%' AND game_datetime < NOW();
 