-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Data database schema for extenddb.
-- These tables live in the data database (separate from the catalog) so that
-- stream records and idempotency tokens can be written atomically with item
-- data within a single PostgreSQL transaction.

BEGIN;

-- Stream shards — fixed shards per table, assigned by partition key hash.
-- No FK to catalog tables (cross-database FKs are not possible).
-- Application-level integrity ensures table_id validity.
CREATE TABLE IF NOT EXISTS stream_shards (
    shard_id TEXT PRIMARY KEY,
    table_id TEXT NOT NULL,
    parent_shard_id TEXT,
    starting_sequence_number TEXT NOT NULL,
    ending_sequence_number TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stream_shards_table
    ON stream_shards (table_id);

-- Stream records — change data capture records.
CREATE TABLE IF NOT EXISTS stream_records (
    shard_id TEXT NOT NULL REFERENCES stream_shards(shard_id) ON DELETE CASCADE,
    sequence_number TEXT NOT NULL,
    table_id TEXT NOT NULL,
    event_name TEXT NOT NULL,
    record_data JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (shard_id, sequence_number)
);

CREATE INDEX IF NOT EXISTS idx_stream_records_created
    ON stream_records (created_at);

-- Active stream reader leases. Fixed reader slots and unique constraints
-- enforce DynamoDB's two-readers-per-shard quota across frontends.
CREATE TABLE IF NOT EXISTS stream_reader_leases (
    shard_id TEXT NOT NULL,
    reader_slot SMALLINT NOT NULL,
    reader_id TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (shard_id, reader_slot),
    UNIQUE (shard_id, reader_id)
);

CREATE INDEX IF NOT EXISTS idx_stream_reader_leases_expires
    ON stream_reader_leases (expires_at);

-- Monotonic sequence for stream record ordering.
-- Note: the idempotency check in run_data_migrations uses stream_shards
-- existence to decide whether to skip this entire migration. If stream_shards
-- is created manually without the sequence, stream_seq will not exist.
CREATE SEQUENCE IF NOT EXISTS stream_seq START 1;
SELECT setval('stream_seq', GREATEST(
    (EXTRACT(EPOCH FROM now()) * 1000000)::BIGINT,
    1
));

-- Idempotency token storage for TransactWriteItems.
CREATE TABLE IF NOT EXISTS idempotency_tokens (
    token       TEXT PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_idempotency_tokens_created
    ON idempotency_tokens (created_at);

COMMIT;
