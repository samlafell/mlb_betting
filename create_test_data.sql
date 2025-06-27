-- Synthetic Betting Data for Testing All 10 Processors
-- This creates realistic betting scenarios to trigger each processor

-- 1. SHARP ACTION TRIGGER - Yankees vs Red Sox
INSERT INTO splits.games (id, game_id, home_team, away_team, game_datetime, created_at, updated_at)
VALUES ('test_sharp_action_001', 'test_sharp_action_001', 'BOS', 'NYY', NOW() + INTERVAL '2 hours', NOW(), NOW())
ON CONFLICT (game_id) DO UPDATE SET updated_at = NOW();

INSERT INTO splits.raw_mlb_betting_splits (
    game_id, home_team, away_team, game_datetime, split_type, book, source,
    home_or_over_bets_percentage, away_or_under_bets_percentage,
    home_or_over_stake_percentage, away_or_under_stake_percentage,
    split_value, created_at
) VALUES (
    'test_sharp_action_001', 'BOS', 'NYY', NOW() + INTERVAL '2 hours', 'moneyline', 'draftkings', 'SYNTHETIC_TEST',
    75.0, 25.0, 25.0, 75.0, 'NYY -135 / BOS +115', NOW()
);

-- 2. OPPOSING MARKETS TRIGGER - Dodgers vs Padres
INSERT INTO splits.games (id, game_id, home_team, away_team, game_datetime, created_at, updated_at)
VALUES ('test_opposing_markets_001', 'test_opposing_markets_001', 'SD', 'LAD', NOW() + INTERVAL '2 hours', NOW(), NOW())
ON CONFLICT (game_id) DO UPDATE SET updated_at = NOW();

INSERT INTO splits.raw_mlb_betting_splits (
    game_id, home_team, away_team, game_datetime, split_type, book, source,
    home_or_over_bets_percentage, away_or_under_bets_percentage,
    home_or_over_stake_percentage, away_or_under_stake_percentage,
    split_value, created_at
) VALUES 
    ('test_opposing_markets_001', 'SD', 'LAD', NOW() + INTERVAL '2 hours', 'moneyline', 'draftkings', 'SYNTHETIC_TEST',
     30.0, 70.0, 25.0, 75.0, 'LAD -150 / SD +130', NOW()),
    ('test_opposing_markets_001', 'SD', 'LAD', NOW() + INTERVAL '2 hours', 'spread', 'draftkings', 'SYNTHETIC_TEST',
     65.0, 35.0, 70.0, 30.0, 'SD +1.5 (-110) / LAD -1.5 (-110)', NOW());

-- 3. BOOK CONFLICTS TRIGGER - Astros vs Rangers
INSERT INTO splits.games (id, game_id, home_team, away_team, game_datetime, created_at, updated_at)
VALUES ('test_book_conflicts_001', 'test_book_conflicts_001', 'TEX', 'HOU', NOW() + INTERVAL '2 hours', NOW(), NOW())
ON CONFLICT (game_id) DO UPDATE SET updated_at = NOW();

INSERT INTO splits.raw_mlb_betting_splits (
    game_id, home_team, away_team, game_datetime, split_type, book, source,
    home_or_over_bets_percentage, away_or_under_bets_percentage,
    home_or_over_stake_percentage, away_or_under_stake_percentage,
    split_value, created_at
) VALUES 
    ('test_book_conflicts_001', 'TEX', 'HOU', NOW() + INTERVAL '2 hours', 'moneyline', 'draftkings', 'SYNTHETIC_TEST',
     45.0, 55.0, 40.0, 60.0, 'HOU -140 / TEX +120', NOW()),
    ('test_book_conflicts_001', 'TEX', 'HOU', NOW() + INTERVAL '2 hours', 'moneyline', 'circa', 'SYNTHETIC_TEST',
     35.0, 65.0, 30.0, 70.0, 'HOU -165 / TEX +145', NOW());

-- 4. PUBLIC FADE TRIGGER - Mets vs Braves
INSERT INTO splits.games (id, game_id, home_team, away_team, game_datetime, created_at, updated_at)
VALUES ('test_public_fade_001', 'test_public_fade_001', 'ATL', 'NYM', NOW() + INTERVAL '2 hours', NOW(), NOW())
ON CONFLICT (game_id) DO UPDATE SET updated_at = NOW();

INSERT INTO splits.raw_mlb_betting_splits (
    game_id, home_team, away_team, game_datetime, split_type, book, source,
    home_or_over_bets_percentage, away_or_under_bets_percentage,
    home_or_over_stake_percentage, away_or_under_stake_percentage,
    split_value, created_at
) VALUES (
    'test_public_fade_001', 'ATL', 'NYM', NOW() + INTERVAL '2 hours', 'moneyline', 'draftkings', 'SYNTHETIC_TEST',
    20.0, 80.0, 35.0, 65.0, 'NYM -125 / ATL +105', NOW()
);

-- 5. LATE FLIP TRIGGER - Cubs vs Cardinals (game in 30 minutes)
INSERT INTO splits.games (id, game_id, home_team, away_team, game_datetime, created_at, updated_at)
VALUES ('test_late_flip_001', 'test_late_flip_001', 'STL', 'CHC', NOW() + INTERVAL '30 minutes', NOW(), NOW())
ON CONFLICT (game_id) DO UPDATE SET updated_at = NOW();

INSERT INTO splits.raw_mlb_betting_splits (
    game_id, home_team, away_team, game_datetime, split_type, book, source,
    home_or_over_bets_percentage, away_or_under_bets_percentage,
    home_or_over_stake_percentage, away_or_under_stake_percentage,
    split_value, created_at
) VALUES (
    'test_late_flip_001', 'STL', 'CHC', NOW() + INTERVAL '30 minutes', 'moneyline', 'draftkings', 'SYNTHETIC_TEST',
    60.0, 40.0, 35.0, 65.0, 'CHC -110 / STL -110', NOW()
);

-- 6. CONSENSUS TRIGGER - Giants vs Rockies
INSERT INTO splits.games (id, game_id, home_team, away_team, game_datetime, created_at, updated_at)
VALUES ('test_consensus_001', 'test_consensus_001', 'COL', 'SF', NOW() + INTERVAL '2 hours', NOW(), NOW())
ON CONFLICT (game_id) DO UPDATE SET updated_at = NOW();

INSERT INTO splits.raw_mlb_betting_splits (
    game_id, home_team, away_team, game_datetime, split_type, book, source,
    home_or_over_bets_percentage, away_or_under_bets_percentage,
    home_or_over_stake_percentage, away_or_under_stake_percentage,
    split_value, created_at
) VALUES (
    'test_consensus_001', 'COL', 'SF', NOW() + INTERVAL '2 hours', 'moneyline', 'draftkings', 'SYNTHETIC_TEST',
    25.0, 75.0, 20.0, 80.0, 'SF -180 / COL +155', NOW()
);

-- 7. UNDERDOG VALUE TRIGGER - Marlins vs Phillies
INSERT INTO splits.games (id, game_id, home_team, away_team, game_datetime, created_at, updated_at)
VALUES ('test_underdog_value_001', 'test_underdog_value_001', 'PHI', 'MIA', NOW() + INTERVAL '2 hours', NOW(), NOW())
ON CONFLICT (game_id) DO UPDATE SET updated_at = NOW();

INSERT INTO splits.raw_mlb_betting_splits (
    game_id, home_team, away_team, game_datetime, split_type, book, source,
    home_or_over_bets_percentage, away_or_under_bets_percentage,
    home_or_over_stake_percentage, away_or_under_stake_percentage,
    split_value, created_at
) VALUES (
    'test_underdog_value_001', 'PHI', 'MIA', NOW() + INTERVAL '2 hours', 'moneyline', 'draftkings', 'SYNTHETIC_TEST',
    70.0, 30.0, 45.0, 55.0, 'PHI -155 / MIA +135', NOW()
);

-- 8. TIMING BASED TRIGGER - Angels vs Athletics (ultra late timing)
INSERT INTO splits.games (id, game_id, home_team, away_team, game_datetime, created_at, updated_at)
VALUES ('test_timing_001', 'test_timing_001', 'OAK', 'LAA', NOW() + INTERVAL '30 minutes', NOW(), NOW())
ON CONFLICT (game_id) DO UPDATE SET updated_at = NOW();

INSERT INTO splits.raw_mlb_betting_splits (
    game_id, home_team, away_team, game_datetime, split_type, book, source,
    home_or_over_bets_percentage, away_or_under_bets_percentage,
    home_or_over_stake_percentage, away_or_under_stake_percentage,
    split_value, created_at
) VALUES (
    'test_timing_001', 'OAK', 'LAA', NOW() + INTERVAL '30 minutes', 'moneyline', 'circa', 'SYNTHETIC_TEST',
    40.0, 60.0, 25.0, 75.0, 'LAA -125 / OAK +105', NOW()
);

-- 9. LINE MOVEMENT TRIGGER - Brewers vs Pirates
INSERT INTO splits.games (id, game_id, home_team, away_team, game_datetime, created_at, updated_at)
VALUES ('test_line_movement_001', 'test_line_movement_001', 'PIT', 'MIL', NOW() + INTERVAL '2 hours', NOW(), NOW())
ON CONFLICT (game_id) DO UPDATE SET updated_at = NOW();

INSERT INTO splits.raw_mlb_betting_splits (
    game_id, home_team, away_team, game_datetime, split_type, book, source,
    home_or_over_bets_percentage, away_or_under_bets_percentage,
    home_or_over_stake_percentage, away_or_under_stake_percentage,
    split_value, created_at
) VALUES (
    'test_line_movement_001', 'PIT', 'MIL', NOW() + INTERVAL '2 hours', 'moneyline', 'draftkings', 'SYNTHETIC_TEST',
    35.0, 65.0, 20.0, 80.0, 'MIL -160 / PIT +140', NOW()
);

-- 10. HYBRID SHARP TRIGGER - Orioles vs Guardians
INSERT INTO splits.games (id, game_id, home_team, away_team, game_datetime, created_at, updated_at)
VALUES ('test_hybrid_001', 'test_hybrid_001', 'CLE', 'BAL', NOW() + INTERVAL '2 hours', NOW(), NOW())
ON CONFLICT (game_id) DO UPDATE SET updated_at = NOW();

INSERT INTO splits.raw_mlb_betting_splits (
    game_id, home_team, away_team, game_datetime, split_type, book, source,
    home_or_over_bets_percentage, away_or_under_bets_percentage,
    home_or_over_stake_percentage, away_or_under_stake_percentage,
    split_value, created_at
) VALUES 
    ('test_hybrid_001', 'CLE', 'BAL', NOW() + INTERVAL '2 hours', 'moneyline', 'draftkings', 'SYNTHETIC_TEST',
     30.0, 70.0, 15.0, 85.0, 'BAL -145 / CLE +125', NOW()),
    ('test_hybrid_001', 'CLE', 'BAL', NOW() + INTERVAL '2 hours', 'spread', 'draftkings', 'SYNTHETIC_TEST',
     35.0, 65.0, 20.0, 80.0, 'BAL -1.5 (-110) / CLE +1.5 (-110)', NOW());

-- Summary
SELECT 'SYNTHETIC TEST DATA INSERTED' as status;
SELECT COUNT(*) as game_count FROM splits.games WHERE game_id LIKE 'test_%';
SELECT COUNT(*) as splits_count FROM splits.raw_mlb_betting_splits WHERE source = 'SYNTHETIC_TEST';

-- Show inserted data
SELECT 
    g.game_id,
    g.home_team || ' vs ' || g.away_team as matchup,
    ROUND(EXTRACT(EPOCH FROM (g.game_datetime - NOW()))/60) as minutes_away,
    COUNT(s.id) as splits_count
FROM splits.games g
LEFT JOIN splits.raw_mlb_betting_splits s ON g.game_id = s.game_id AND s.source = 'SYNTHETIC_TEST'
WHERE g.game_id LIKE 'test_%'
GROUP BY g.game_id, g.home_team, g.away_team, g.game_datetime
ORDER BY g.game_datetime; 