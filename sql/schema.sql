-- Schema definition for PostgreSQL API reconciliation benchmark
-- Tables for subscriptions, watermark tracking, sweeps, and unlogged staging.

DROP TABLE IF EXISTS unlogged_seen_ids CASCADE;
DROP TABLE IF EXISTS reconciliation_sweeps CASCADE;
DROP TABLE IF EXISTS destination_subscriptions CASCADE;
DROP TABLE IF EXISTS tenant_watermarks CASCADE;
DROP SEQUENCE IF EXISTS dest_ingestion_seq CASCADE;
DROP SEQUENCE IF EXISTS sweep_order_seq CASCADE;

CREATE SEQUENCE dest_ingestion_seq AS BIGINT START WITH 1 INCREMENT BY 1;

CREATE TABLE tenant_watermarks (
    tenant_id TEXT PRIMARY KEY,
    current_epoch BIGINT NOT NULL DEFAULT 1
);

INSERT INTO tenant_watermarks (tenant_id, current_epoch)
VALUES ('acme', 1), ('globex', 1);

CREATE TABLE destination_subscriptions (
    tenant_id TEXT NOT NULL,
    id BIGINT NOT NULL,
    mrr_cents INTEGER NOT NULL,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    source_created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    ingestion_seq BIGINT NOT NULL DEFAULT nextval('dest_ingestion_seq'),
    ingestion_epoch BIGINT NOT NULL DEFAULT 1,
    missing_streak INTEGER NOT NULL DEFAULT 0,
    last_missed_sweep_seq BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, id)
);

-- Trigger function enforcing tenant immutability, epoch watermarking,
-- and quarantine streak tracking during subscription ingestion.
CREATE OR REPLACE FUNCTION enforce_tenant_epoch_and_quarantine()
RETURNS TRIGGER AS $$
DECLARE
    v_epoch BIGINT;
BEGIN
    -- Tenant IDs are immutable once written
    IF TG_OP = 'UPDATE' AND OLD.tenant_id IS DISTINCT FROM NEW.tenant_id THEN
        RAISE EXCEPTION 'Immutable column violation: tenant_id cannot be modified from % to %',
            OLD.tenant_id, NEW.tenant_id;
    END IF;

    -- If this UPDATE is strictly for sweep quarantine maintenance,
    -- preserve the existing ingestion_epoch and streak counters.
    IF TG_OP = 'UPDATE' AND (
        (OLD.mrr_cents, OLD.is_deleted, OLD.source_created_at, OLD.updated_at, OLD.ingested_at, OLD.ingestion_seq)
        IS NOT DISTINCT FROM
        (NEW.mrr_cents, NEW.is_deleted, NEW.source_created_at, NEW.updated_at, NEW.ingested_at, NEW.ingestion_seq)
    ) THEN
        RETURN NEW;
    END IF;

    SELECT current_epoch INTO v_epoch
    FROM tenant_watermarks
    WHERE tenant_id = NEW.tenant_id
    FOR SHARE;

    IF NOT FOUND THEN
        INSERT INTO tenant_watermarks (tenant_id, current_epoch)
        VALUES (NEW.tenant_id, 1)
        ON CONFLICT (tenant_id) DO NOTHING;

        SELECT current_epoch INTO STRICT v_epoch
        FROM tenant_watermarks
        WHERE tenant_id = NEW.tenant_id
        FOR SHARE;
    END IF;

    NEW.ingestion_epoch := v_epoch;

    IF NEW.is_deleted = FALSE THEN
        NEW.missing_streak := 0;
        NEW.last_missed_sweep_seq := 0;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_enforce_tenant_epoch
BEFORE INSERT OR UPDATE
ON destination_subscriptions
FOR EACH ROW
EXECUTE FUNCTION enforce_tenant_epoch_and_quarantine();

CREATE INDEX idx_dest_subs_tenant_ingested
    ON destination_subscriptions (tenant_id, ingested_at)
    WHERE is_deleted = FALSE;

CREATE INDEX idx_dest_subs_tenant_seq
    ON destination_subscriptions (tenant_id, ingestion_seq)
    WHERE is_deleted = FALSE;

CREATE INDEX idx_dest_subs_tenant_epoch
    ON destination_subscriptions (tenant_id, ingestion_epoch)
    WHERE is_deleted = FALSE;

CREATE INDEX idx_dest_subs_quarantine
    ON destination_subscriptions (tenant_id, missing_streak)
    WHERE is_deleted = FALSE;

CREATE SEQUENCE sweep_order_seq AS BIGINT START WITH 1 INCREMENT BY 1;

CREATE TABLE reconciliation_sweeps (
    sweep_id UUID PRIMARY KEY,
    sweep_seq BIGINT NOT NULL DEFAULT nextval('sweep_order_seq'),
    tenant_id TEXT NOT NULL,
    strategy_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('STARTED', 'STAGED', 'COMPLETED', 'ABORTED')),
    pages_fetched INTEGER NOT NULL DEFAULT 0,
    records_seen INTEGER NOT NULL DEFAULT 0,
    sweep_started_at TIMESTAMPTZ NOT NULL,
    hwm_ingestion_seq BIGINT NOT NULL,
    sealed_epoch BIGINT NOT NULL DEFAULT 1,
    completed_at TIMESTAMPTZ
);

CREATE UNLOGGED TABLE unlogged_seen_ids (
    sweep_id UUID NOT NULL,
    tenant_id TEXT NOT NULL,
    id BIGINT NOT NULL,
    PRIMARY KEY (sweep_id, tenant_id, id)
);
