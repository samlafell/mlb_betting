-- Migration: Create Staging Bet Type Tables
-- Purpose: Create missing staging tables expected by staging zone processor
-- Creates: staging.moneylines, staging.spreads, staging.totals
-- Date: 2025-08-11

-- ================================
-- Create staging.moneylines table
-- ================================

CREATE TABLE IF NOT EXISTS staging.moneylines (
    id BIGSERIAL PRIMARY KEY,
    raw_moneylines_id BIGINT,
    game_id VARCHAR(255),
    sportsbook_id INTEGER,
    sportsbook_name VARCHAR(100),
    home_odds INTEGER,
    away_odds INTEGER,
    home_team_normalized VARCHAR(100),
    away_team_normalized VARCHAR(100),
    data_quality_score DECIMAL(3,2) DEFAULT 1.0,
    validation_status VARCHAR(20) DEFAULT 'pending' CHECK (validation_status IN ('pending', 'valid', 'invalid', 'warning')),
    validation_errors JSONB,
    processed_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ================================
-- Create staging.spreads table
-- ================================

CREATE TABLE IF NOT EXISTS staging.spreads (
    id BIGSERIAL PRIMARY KEY,
    raw_spreads_id BIGINT,
    game_id VARCHAR(255),
    sportsbook_id INTEGER,
    sportsbook_name VARCHAR(100),
    line_value DECIMAL(5,2),
    home_odds INTEGER,
    away_odds INTEGER,
    home_team_normalized VARCHAR(100),
    away_team_normalized VARCHAR(100),
    data_quality_score DECIMAL(3,2) DEFAULT 1.0,
    validation_status VARCHAR(20) DEFAULT 'pending' CHECK (validation_status IN ('pending', 'valid', 'invalid', 'warning')),
    validation_errors JSONB,
    processed_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ================================
-- Create staging.totals table
-- ================================

CREATE TABLE IF NOT EXISTS staging.totals (
    id BIGSERIAL PRIMARY KEY,
    raw_totals_id BIGINT,
    game_id VARCHAR(255),
    sportsbook_id INTEGER,
    sportsbook_name VARCHAR(100),
    line_value DECIMAL(5,2),
    over_odds INTEGER,
    under_odds INTEGER,
    data_quality_score DECIMAL(3,2) DEFAULT 1.0,
    validation_status VARCHAR(20) DEFAULT 'pending' CHECK (validation_status IN ('pending', 'valid', 'invalid', 'warning')),
    validation_errors JSONB,
    processed_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ================================
-- Create indexes for performance
-- ================================

-- Moneylines indexes
CREATE INDEX IF NOT EXISTS idx_staging_moneylines_game_id ON staging.moneylines(game_id);
CREATE INDEX IF NOT EXISTS idx_staging_moneylines_sportsbook ON staging.moneylines(sportsbook_id);
CREATE INDEX IF NOT EXISTS idx_staging_moneylines_processed_at ON staging.moneylines(processed_at);
CREATE INDEX IF NOT EXISTS idx_staging_moneylines_validation_status ON staging.moneylines(validation_status);

-- Spreads indexes
CREATE INDEX IF NOT EXISTS idx_staging_spreads_game_id ON staging.spreads(game_id);
CREATE INDEX IF NOT EXISTS idx_staging_spreads_sportsbook ON staging.spreads(sportsbook_id);
CREATE INDEX IF NOT EXISTS idx_staging_spreads_line_value ON staging.spreads(line_value);
CREATE INDEX IF NOT EXISTS idx_staging_spreads_processed_at ON staging.spreads(processed_at);
CREATE INDEX IF NOT EXISTS idx_staging_spreads_validation_status ON staging.spreads(validation_status);

-- Totals indexes
CREATE INDEX IF NOT EXISTS idx_staging_totals_game_id ON staging.totals(game_id);
CREATE INDEX IF NOT EXISTS idx_staging_totals_sportsbook ON staging.totals(sportsbook_id);
CREATE INDEX IF NOT EXISTS idx_staging_totals_line_value ON staging.totals(line_value);
CREATE INDEX IF NOT EXISTS idx_staging_totals_processed_at ON staging.totals(processed_at);
CREATE INDEX IF NOT EXISTS idx_staging_totals_validation_status ON staging.totals(validation_status);

-- ================================
-- Add foreign key relationships (optional)
-- ================================

-- Reference to staging.sportsbooks if needed
-- ALTER TABLE staging.moneylines ADD CONSTRAINT fk_moneylines_sportsbook 
--     FOREIGN KEY (sportsbook_id) REFERENCES staging.sportsbooks(id);
-- ALTER TABLE staging.spreads ADD CONSTRAINT fk_spreads_sportsbook 
--     FOREIGN KEY (sportsbook_id) REFERENCES staging.sportsbooks(id);
-- ALTER TABLE staging.totals ADD CONSTRAINT fk_totals_sportsbook 
--     FOREIGN KEY (sportsbook_id) REFERENCES staging.sportsbooks(id);

-- ================================
-- Comments and documentation
-- ================================

COMMENT ON TABLE staging.moneylines IS 'Staging table for normalized moneyline betting data';
COMMENT ON TABLE staging.spreads IS 'Staging table for normalized spread betting data with point spreads';
COMMENT ON TABLE staging.totals IS 'Staging table for normalized totals (over/under) betting data';

COMMENT ON COLUMN staging.moneylines.raw_moneylines_id IS 'Reference to source raw table record';
COMMENT ON COLUMN staging.spreads.raw_spreads_id IS 'Reference to source raw table record';
COMMENT ON COLUMN staging.totals.raw_totals_id IS 'Reference to source raw table record';

COMMENT ON COLUMN staging.moneylines.validation_status IS 'Data validation status: pending, valid, invalid, warning';
COMMENT ON COLUMN staging.spreads.validation_status IS 'Data validation status: pending, valid, invalid, warning';
COMMENT ON COLUMN staging.totals.validation_status IS 'Data validation status: pending, valid, invalid, warning';

COMMENT ON COLUMN staging.spreads.line_value IS 'Point spread value (positive for underdog, negative for favorite)';
COMMENT ON COLUMN staging.totals.line_value IS 'Total points line for over/under betting';