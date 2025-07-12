-- Action Network Data Schema
-- Schema for storing Action Network sourced data including team dimensions

CREATE SCHEMA IF NOT EXISTS action;

-- Team Dimension Table
-- Stores core team attributes without time-varying data like standings
CREATE TABLE IF NOT EXISTS action.dim_teams (
    id SERIAL PRIMARY KEY,
    team_id INTEGER NOT NULL UNIQUE,
    full_name VARCHAR(100) NOT NULL,
    display_name VARCHAR(50) NOT NULL,
    short_name VARCHAR(50) NOT NULL,
    location VARCHAR(50) NOT NULL,
    abbr VARCHAR(5) NOT NULL,
    logo VARCHAR(500) NOT NULL,
    primary_color CHAR(6) NOT NULL,
    secondary_color CHAR(6) NOT NULL,
    conference_type VARCHAR(2) NOT NULL CHECK (conference_type IN ('AL', 'NL')),
    division_type VARCHAR(10) NOT NULL CHECK (division_type IN ('EAST', 'CENTRAL', 'WEST')),
    url_slug VARCHAR(100) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE,
    
    -- Constraints
    CONSTRAINT dim_teams_abbr_check CHECK (abbr ~ '^[A-Z]+$'),
    CONSTRAINT dim_teams_primary_color_check CHECK (primary_color ~ '^[0-9A-F]{6}$'),
    CONSTRAINT dim_teams_secondary_color_check CHECK (secondary_color ~ '^[0-9A-F]{6}$')
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_dim_teams_team_id ON action.dim_teams(team_id);
CREATE INDEX IF NOT EXISTS idx_dim_teams_abbr ON action.dim_teams(abbr);
CREATE INDEX IF NOT EXISTS idx_dim_teams_conference_division ON action.dim_teams(conference_type, division_type);
CREATE INDEX IF NOT EXISTS idx_dim_teams_url_slug ON action.dim_teams(url_slug);

-- Comments for documentation
COMMENT ON SCHEMA action IS 'Schema for Action Network sourced data';
COMMENT ON TABLE action.dim_teams IS 'Dimension table for MLB teams with core attributes from Action Network';
COMMENT ON COLUMN action.dim_teams.team_id IS 'Action Network unique team identifier';
COMMENT ON COLUMN action.dim_teams.full_name IS 'Full team name (e.g., "New York Yankees")';
COMMENT ON COLUMN action.dim_teams.display_name IS 'Display name for UI (e.g., "Yankees")';
COMMENT ON COLUMN action.dim_teams.short_name IS 'Short name variant (e.g., "Yankees")';
COMMENT ON COLUMN action.dim_teams.location IS 'Team city/location (e.g., "New York")';
COMMENT ON COLUMN action.dim_teams.abbr IS 'Team abbreviation (e.g., "NYY")';
COMMENT ON COLUMN action.dim_teams.logo IS 'URL to team logo image';
COMMENT ON COLUMN action.dim_teams.primary_color IS 'Primary team color as 6-digit hex code without #';
COMMENT ON COLUMN action.dim_teams.secondary_color IS 'Secondary team color as 6-digit hex code without #';
COMMENT ON COLUMN action.dim_teams.conference_type IS 'League conference: AL (American League) or NL (National League)';
COMMENT ON COLUMN action.dim_teams.division_type IS 'Division within conference: EAST, CENTRAL, or WEST';
COMMENT ON COLUMN action.dim_teams.url_slug IS 'URL-friendly team identifier for web links';

-- Function to upsert team data
CREATE OR REPLACE FUNCTION action.upsert_team(
    p_team_id INTEGER,
    p_full_name VARCHAR(100),
    p_display_name VARCHAR(50),
    p_short_name VARCHAR(50),
    p_location VARCHAR(50),
    p_abbr VARCHAR(5),
    p_logo VARCHAR(500),
    p_primary_color CHAR(6),
    p_secondary_color CHAR(6),
    p_conference_type VARCHAR(2),
    p_division_type VARCHAR(10),
    p_url_slug VARCHAR(100)
)
RETURNS void AS $$
BEGIN
    INSERT INTO action.dim_teams (
        team_id, full_name, display_name, short_name, location,
        abbr, logo, primary_color, secondary_color,
        conference_type, division_type, url_slug
    ) VALUES (
        p_team_id, p_full_name, p_display_name, p_short_name, p_location,
        p_abbr, p_logo, p_primary_color, p_secondary_color,
        p_conference_type, p_division_type, p_url_slug
    )
    ON CONFLICT (team_id) DO UPDATE SET
        full_name = EXCLUDED.full_name,
        display_name = EXCLUDED.display_name,
        short_name = EXCLUDED.short_name,
        location = EXCLUDED.location,
        abbr = EXCLUDED.abbr,
        logo = EXCLUDED.logo,
        primary_color = EXCLUDED.primary_color,
        secondary_color = EXCLUDED.secondary_color,
        conference_type = EXCLUDED.conference_type,
        division_type = EXCLUDED.division_type,
        url_slug = EXCLUDED.url_slug,
        updated_at = NOW();
END;
$$ LANGUAGE plpgsql;

-- Game Fact Table
-- Stores game data from Action Network with links to team dimensions and external systems
CREATE TABLE IF NOT EXISTS action.fact_games (
    id SERIAL PRIMARY KEY,
    id_action INTEGER NOT NULL UNIQUE,
    id_mlbstatsapi INTEGER,
    dim_home_team_actionid INTEGER NOT NULL,
    dim_away_team_actionid INTEGER NOT NULL,
    dim_date DATE NOT NULL,
    dim_time TIME,
    dim_datetime TIMESTAMP WITH TIME ZONE,
    game_status VARCHAR(20) NOT NULL DEFAULT 'scheduled',
    venue_name VARCHAR(200),
    season INTEGER,
    season_type VARCHAR(20) DEFAULT 'regular',
    game_number INTEGER DEFAULT 1,
    weather_conditions TEXT,
    temperature INTEGER,
    wind_speed INTEGER,
    wind_direction VARCHAR(10),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE,
    
    -- Foreign key constraints
    CONSTRAINT fk_fact_games_home_team FOREIGN KEY (dim_home_team_actionid) 
        REFERENCES action.dim_teams(team_id) ON DELETE RESTRICT,
    CONSTRAINT fk_fact_games_away_team FOREIGN KEY (dim_away_team_actionid) 
        REFERENCES action.dim_teams(team_id) ON DELETE RESTRICT,
    
    -- Check constraints
    CONSTRAINT fact_games_status_check CHECK (game_status IN ('scheduled', 'live', 'final', 'postponed', 'cancelled', 'suspended', 'delayed')),
    CONSTRAINT fact_games_teams_different CHECK (dim_home_team_actionid != dim_away_team_actionid),
    CONSTRAINT fact_games_season_check CHECK (season >= 1876 AND season <= 2030),
    CONSTRAINT fact_games_season_type_check CHECK (season_type IN ('regular', 'postseason', 'spring')),
    CONSTRAINT fact_games_game_number_check CHECK (game_number >= 1 AND game_number <= 2)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_fact_games_action_id ON action.fact_games(id_action);
CREATE INDEX IF NOT EXISTS idx_fact_games_mlbstats_id ON action.fact_games(id_mlbstatsapi);
CREATE INDEX IF NOT EXISTS idx_fact_games_date ON action.fact_games(dim_date);
CREATE INDEX IF NOT EXISTS idx_fact_games_datetime ON action.fact_games(dim_datetime);
CREATE INDEX IF NOT EXISTS idx_fact_games_teams ON action.fact_games(dim_home_team_actionid, dim_away_team_actionid);
CREATE INDEX IF NOT EXISTS idx_fact_games_status ON action.fact_games(game_status);
CREATE INDEX IF NOT EXISTS idx_fact_games_season ON action.fact_games(season, season_type);

-- Comments for documentation
COMMENT ON TABLE action.fact_games IS 'Fact table for MLB games from Action Network with dimensional links';
COMMENT ON COLUMN action.fact_games.id_action IS 'Action Network unique game identifier';
COMMENT ON COLUMN action.fact_games.id_mlbstatsapi IS 'MLB Stats API game identifier for linking to official data';
COMMENT ON COLUMN action.fact_games.dim_home_team_actionid IS 'Foreign key to home team in dim_teams';
COMMENT ON COLUMN action.fact_games.dim_away_team_actionid IS 'Foreign key to away team in dim_teams';
COMMENT ON COLUMN action.fact_games.dim_date IS 'Game date (local time)';
COMMENT ON COLUMN action.fact_games.dim_time IS 'Game start time (local time)';
COMMENT ON COLUMN action.fact_games.dim_datetime IS 'Game start timestamp with timezone';
COMMENT ON COLUMN action.fact_games.game_status IS 'Current status of the game';
COMMENT ON COLUMN action.fact_games.venue_name IS 'Stadium/venue where game is played';

-- Function to upsert game data
CREATE OR REPLACE FUNCTION action.upsert_game(
    p_id_action INTEGER,
    p_id_mlbstatsapi INTEGER,
    p_dim_home_team_actionid INTEGER,
    p_dim_away_team_actionid INTEGER,
    p_dim_date DATE,
    p_dim_time TIME,
    p_dim_datetime TIMESTAMP WITH TIME ZONE,
    p_game_status VARCHAR(20),
    p_venue_name VARCHAR(200),
    p_season INTEGER,
    p_season_type VARCHAR(20),
    p_game_number INTEGER,
    p_weather_conditions TEXT DEFAULT NULL,
    p_temperature INTEGER DEFAULT NULL,
    p_wind_speed INTEGER DEFAULT NULL,
    p_wind_direction VARCHAR(10) DEFAULT NULL
)
RETURNS void AS $$
BEGIN
    INSERT INTO action.fact_games (
        id_action, id_mlbstatsapi, dim_home_team_actionid, dim_away_team_actionid,
        dim_date, dim_time, dim_datetime, game_status, venue_name,
        season, season_type, game_number,
        weather_conditions, temperature, wind_speed, wind_direction
    ) VALUES (
        p_id_action, p_id_mlbstatsapi, p_dim_home_team_actionid, p_dim_away_team_actionid,
        p_dim_date, p_dim_time, p_dim_datetime, p_game_status, p_venue_name,
        p_season, p_season_type, p_game_number,
        p_weather_conditions, p_temperature, p_wind_speed, p_wind_direction
    )
    ON CONFLICT (id_action) DO UPDATE SET
        id_mlbstatsapi = EXCLUDED.id_mlbstatsapi,
        dim_home_team_actionid = EXCLUDED.dim_home_team_actionid,
        dim_away_team_actionid = EXCLUDED.dim_away_team_actionid,
        dim_date = EXCLUDED.dim_date,
        dim_time = EXCLUDED.dim_time,
        dim_datetime = EXCLUDED.dim_datetime,
        game_status = EXCLUDED.game_status,
        venue_name = EXCLUDED.venue_name,
        season = EXCLUDED.season,
        season_type = EXCLUDED.season_type,
        game_number = EXCLUDED.game_number,
        weather_conditions = EXCLUDED.weather_conditions,
        temperature = EXCLUDED.temperature,
        wind_speed = EXCLUDED.wind_speed,
        wind_direction = EXCLUDED.wind_direction,
        updated_at = NOW();
END;
$$ LANGUAGE plpgsql;

-- View for quick team lookups with formatted colors
CREATE OR REPLACE VIEW action.teams_formatted AS
SELECT 
    team_id,
    full_name,
    display_name,
    short_name,
    location,
    abbr,
    logo,
    '#' || primary_color as primary_color_hex,
    '#' || secondary_color as secondary_color_hex,
    CASE conference_type 
        WHEN 'AL' THEN 'American League'
        WHEN 'NL' THEN 'National League'
    END as conference_name,
    conference_type || ' ' || division_type as division_full,
    url_slug,
    created_at,
    updated_at
FROM action.dim_teams
ORDER BY conference_type, division_type, full_name;

-- Function to normalize team names for joining with splits data
CREATE OR REPLACE FUNCTION action.normalize_team_name(team_name TEXT)
RETURNS TEXT AS $$
BEGIN
    -- Handle NULL or empty input
    IF team_name IS NULL OR TRIM(team_name) = '' THEN
        RETURN NULL;
    END IF;
    
    -- Convert to uppercase and trim
    team_name := UPPER(TRIM(team_name));
    
    -- Handle common variations and full names
    team_name := CASE team_name
        -- Full team names to abbreviations
        WHEN 'CUBS' THEN 'CHC'
        WHEN 'CHICAGO CUBS' THEN 'CHC'
        WHEN 'WHITE SOX' THEN 'CWS'
        WHEN 'CHICAGO WHITE SOX' THEN 'CWS'
        WHEN 'RED SOX' THEN 'BOS'
        WHEN 'BOSTON RED SOX' THEN 'BOS'
        WHEN 'YANKEES' THEN 'NYY'
        WHEN 'NEW YORK YANKEES' THEN 'NYY'
        WHEN 'METS' THEN 'NYM'
        WHEN 'NEW YORK METS' THEN 'NYM'
        WHEN 'DODGERS' THEN 'LAD'
        WHEN 'LOS ANGELES DODGERS' THEN 'LAD'
        WHEN 'GIANTS' THEN 'SF'
        WHEN 'SAN FRANCISCO GIANTS' THEN 'SF'
        WHEN 'PADRES' THEN 'SD'
        WHEN 'SAN DIEGO PADRES' THEN 'SD'
        WHEN 'ANGELS' THEN 'LAA'
        WHEN 'LOS ANGELES ANGELS' THEN 'LAA'
        WHEN 'ATHLETICS' THEN 'OAK'
        WHEN 'OAKLAND ATHLETICS' THEN 'OAK'
        WHEN 'MARINERS' THEN 'SEA'
        WHEN 'SEATTLE MARINERS' THEN 'SEA'
        WHEN 'RANGERS' THEN 'TEX'
        WHEN 'TEXAS RANGERS' THEN 'TEX'
        WHEN 'ASTROS' THEN 'HOU'
        WHEN 'HOUSTON ASTROS' THEN 'HOU'
        WHEN 'TWINS' THEN 'MIN'
        WHEN 'MINNESOTA TWINS' THEN 'MIN'
        WHEN 'ROYALS' THEN 'KC'
        WHEN 'KANSAS CITY ROYALS' THEN 'KC'
        WHEN 'TIGERS' THEN 'DET'
        WHEN 'DETROIT TIGERS' THEN 'DET'
        WHEN 'GUARDIANS' THEN 'CLE'
        WHEN 'CLEVELAND GUARDIANS' THEN 'CLE'
        WHEN 'BRAVES' THEN 'ATL'
        WHEN 'ATLANTA BRAVES' THEN 'ATL'
        WHEN 'MARLINS' THEN 'MIA'
        WHEN 'MIAMI MARLINS' THEN 'MIA'
        WHEN 'PHILLIES' THEN 'PHI'
        WHEN 'PHILADELPHIA PHILLIES' THEN 'PHI'
        WHEN 'NATIONALS' THEN 'WSH'
        WHEN 'WASHINGTON NATIONALS' THEN 'WSH'
        WHEN 'BREWERS' THEN 'MIL'
        WHEN 'MILWAUKEE BREWERS' THEN 'MIL'
        WHEN 'REDS' THEN 'CIN'
        WHEN 'CINCINNATI REDS' THEN 'CIN'
        WHEN 'PIRATES' THEN 'PIT'
        WHEN 'PITTSBURGH PIRATES' THEN 'PIT'
        WHEN 'CARDINALS' THEN 'STL'
        WHEN 'ST. LOUIS CARDINALS' THEN 'STL'
        WHEN 'DIAMONDBACKS' THEN 'ARI'
        WHEN 'ARIZONA DIAMONDBACKS' THEN 'ARI'
        WHEN 'ROCKIES' THEN 'COL'
        WHEN 'COLORADO ROCKIES' THEN 'COL'
        WHEN 'RAYS' THEN 'TB'
        WHEN 'TAMPA BAY RAYS' THEN 'TB'
        WHEN 'BLUE JAYS' THEN 'TOR'
        WHEN 'TORONTO BLUE JAYS' THEN 'TOR'
        WHEN 'ORIOLES' THEN 'BAL'
        WHEN 'BALTIMORE ORIOLES' THEN 'BAL'
        
        -- Alternative abbreviations
        WHEN 'ATH' THEN 'OAK'  -- Fix Athletics abbreviation
        WHEN 'CHA' THEN 'CWS'  -- Alternative White Sox
        WHEN 'CHW' THEN 'CWS'  -- Alternative White Sox
        WHEN 'TBR' THEN 'TB'   -- Alternative Rays
        WHEN 'TBD' THEN 'TB'   -- Alternative Rays
        WHEN 'WSN' THEN 'WSH'  -- Alternative Nationals
        WHEN 'WAS' THEN 'WSH'  -- Alternative Nationals
        WHEN 'ANA' THEN 'LAA'  -- Alternative Angels
        
        -- Return as-is if already standard abbreviation
        ELSE team_name
    END;
    
    RETURN team_name;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to get team_id from normalized team name
CREATE OR REPLACE FUNCTION action.get_team_id_by_name(team_name TEXT)
RETURNS INTEGER AS $$
DECLARE
    normalized_name TEXT;
    result_id INTEGER;
BEGIN
    -- Normalize the team name
    normalized_name := action.normalize_team_name(team_name);
    
    -- Find team_id by abbreviation
    SELECT team_id INTO result_id
    FROM action.dim_teams
    WHERE abbr = normalized_name;
    
    RETURN result_id;
END;
$$ LANGUAGE plpgsql STABLE;

-- View for games with team information
CREATE OR REPLACE VIEW action.games_with_teams AS
SELECT 
    fg.id,
    fg.id_action,
    fg.id_mlbstatsapi,
    fg.dim_date,
    fg.dim_time,
    fg.dim_datetime,
    fg.game_status,
    fg.venue_name,
    fg.season,
    fg.season_type,
    
    -- Home team info
    ht.full_name as home_team_name,
    ht.abbr as home_team_abbr,
    ht.url_slug as home_team_slug,
    ht.primary_color as home_team_primary_color,
    
    -- Away team info  
    at.full_name as away_team_name,
    at.abbr as away_team_abbr,
    at.url_slug as away_team_slug,
    at.primary_color as away_team_primary_color,
    
    -- Matchup info
    at.full_name || ' @ ' || ht.full_name as matchup,
    at.abbr || ' @ ' || ht.abbr as matchup_short,
    
    fg.created_at,
    fg.updated_at
FROM action.fact_games fg
JOIN action.dim_teams ht ON fg.dim_home_team_actionid = ht.team_id
JOIN action.dim_teams at ON fg.dim_away_team_actionid = at.team_id
ORDER BY fg.dim_datetime DESC;

-- Comprehensive view joining games with betting splits using normalization
CREATE OR REPLACE VIEW action.games_with_splits AS
SELECT 
    fg.id_action,
    fg.id_mlbstatsapi,
    fg.dim_date,
    fg.dim_datetime,
    fg.game_status,
    fg.venue_name,
    
    -- Home team info
    ht.full_name as home_team_name,
    ht.abbr as home_team_abbr,
    ht.url_slug as home_team_slug,
    
    -- Away team info  
    at.full_name as away_team_name,
    at.abbr as away_team_abbr,
    at.url_slug as away_team_slug,
    
    -- Matchup info
    at.abbr || ' @ ' || ht.abbr as matchup_abbr,
    at.full_name || ' @ ' || ht.full_name as matchup_full,
    
    -- Betting splits info
    s.id as split_id,
    s.split_type,
    s.book,
    s.source as split_source,
    s.home_or_over_bets,
    s.home_or_over_bets_percentage,
    s.home_or_over_stake_percentage,
    s.away_or_under_bets,
    s.away_or_under_bets_percentage,
    s.away_or_under_stake_percentage,
    s.sharp_action,
    s.line_movement,
    s.winning_team as split_winning_team,
    s.outcome as split_outcome,
    
    -- Normalization info for debugging
    s.home_team as original_home_team,
    s.away_team as original_away_team,
    action.normalize_team_name(s.home_team) as normalized_home_team,
    action.normalize_team_name(s.away_team) as normalized_away_team

FROM action.fact_games fg
JOIN action.dim_teams ht ON fg.dim_home_team_actionid = ht.team_id
JOIN action.dim_teams at ON fg.dim_away_team_actionid = at.team_id
LEFT JOIN splits.raw_mlb_betting_splits s ON (
    DATE(s.game_datetime) = fg.dim_date AND (
        -- Match home team using normalization
        ht.abbr = action.normalize_team_name(s.home_team) OR
        -- Match away team using normalization (for when teams are flipped in splits data)
        at.abbr = action.normalize_team_name(s.away_team) OR
        -- Additional flexible matching
        (ht.abbr = action.normalize_team_name(s.away_team) AND at.abbr = action.normalize_team_name(s.home_team))
    )
)
ORDER BY fg.dim_datetime DESC, s.created_at DESC;

COMMENT ON VIEW action.games_with_splits IS 'Comprehensive view joining Action Network games with betting splits using team name normalization'; 