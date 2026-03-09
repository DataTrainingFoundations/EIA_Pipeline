-- ============================================================
-- EIA Electricity Pipeline — Warehouse Schema
-- Applied automatically on first postgres container startup
-- via /docker-entrypoint-initdb.d/
-- ============================================================

-- ── Dimensions ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_balancing_authority (
    ba_code     VARCHAR(20)  PRIMARY KEY,
    ba_name     VARCHAR(200) NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_fuel_type (
    fuel_code   VARCHAR(10)  PRIMARY KEY,
    fuel_name   VARCHAR(100) NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ── Fact Tables ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_generation_hourly (
    record_id       VARCHAR(64)   PRIMARY KEY,           -- MD5 of period+ba+fuel
    period_ts       TIMESTAMPTZ   NOT NULL,
    ba_code         VARCHAR(20)   NOT NULL,
    ba_name         VARCHAR(200),
    fuel_code       VARCHAR(10)   NOT NULL,
    fuel_name       VARCHAR(100),
    generation_gwh  NUMERIC(12,4),
    partition_date  DATE          NOT NULL,
    loaded_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fgh_period    ON fact_generation_hourly (period_ts);
CREATE INDEX IF NOT EXISTS idx_fgh_ba        ON fact_generation_hourly (ba_code);
CREATE INDEX IF NOT EXISTS idx_fgh_fuel      ON fact_generation_hourly (fuel_code);
CREATE INDEX IF NOT EXISTS idx_fgh_part_date ON fact_generation_hourly (partition_date);

CREATE TABLE IF NOT EXISTS fact_demand_hourly (
    record_id       VARCHAR(64)   PRIMARY KEY,           -- MD5 of period+ba
    period_ts       TIMESTAMPTZ   NOT NULL,
    ba_code         VARCHAR(20)   NOT NULL,
    ba_name         VARCHAR(200),
    demand_gwh      NUMERIC(12,4),
    forecast_gwh    NUMERIC(12,4),
    partition_date  DATE          NOT NULL,
    loaded_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fdh_period    ON fact_demand_hourly (period_ts);
CREATE INDEX IF NOT EXISTS idx_fdh_ba        ON fact_demand_hourly (ba_code);
CREATE INDEX IF NOT EXISTS idx_fdh_part_date ON fact_demand_hourly (partition_date);

-- ── Aggregation Tables (pre-computed for Streamlit app) ───────
CREATE TABLE IF NOT EXISTS agg_daily_generation (
    report_date     DATE          NOT NULL,
    fuel_code       VARCHAR(10)   NOT NULL,
    fuel_name       VARCHAR(100),
    total_gwh       NUMERIC(14,4),
    partition_date  DATE          NOT NULL,
    loaded_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (report_date, fuel_code)
);

CREATE INDEX IF NOT EXISTS idx_adg_date ON agg_daily_generation (report_date);
CREATE INDEX IF NOT EXISTS idx_adg_fuel ON agg_daily_generation (fuel_code);

CREATE TABLE IF NOT EXISTS agg_daily_demand_peak (
    report_date     DATE          NOT NULL,
    ba_code         VARCHAR(20)   NOT NULL,
    ba_name         VARCHAR(200),
    peak_gwh        NUMERIC(12,4),
    partition_date  DATE          NOT NULL,
    loaded_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (report_date, ba_code)
);

CREATE INDEX IF NOT EXISTS idx_addp_date ON agg_daily_demand_peak (report_date);
CREATE INDEX IF NOT EXISTS idx_addp_ba   ON agg_daily_demand_peak (ba_code);

-- ── Trigger: auto-update updated_at on dimensions ─────────────
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_dim_ba_updated_at
    BEFORE UPDATE ON dim_balancing_authority
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_dim_fuel_updated_at
    BEFORE UPDATE ON dim_fuel_type
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();