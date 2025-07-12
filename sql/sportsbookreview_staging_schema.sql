-- Schema for SportsbookReview staging tables

CREATE TABLE IF NOT EXISTS sbr_raw_html (
    id SERIAL PRIMARY KEY,
    source_url TEXT NOT NULL UNIQUE,
    html_content TEXT NOT NULL,
    scraped_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status VARCHAR(20) NOT NULL DEFAULT 'new' -- e.g., new, processed, failed
);

CREATE INDEX IF NOT EXISTS idx_sbr_raw_html_status ON sbr_raw_html(status);

CREATE TABLE IF NOT EXISTS sbr_parsed_games (
    id SERIAL PRIMARY KEY,
    raw_html_id INTEGER NOT NULL REFERENCES sbr_raw_html(id) ON DELETE CASCADE,
    game_data JSONB NOT NULL,
    parsed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status VARCHAR(20) NOT NULL DEFAULT 'new' -- e.g., new, processed, failed, invalid
);

CREATE INDEX IF NOT EXISTS idx_sbr_parsed_games_status ON sbr_parsed_games(status); 