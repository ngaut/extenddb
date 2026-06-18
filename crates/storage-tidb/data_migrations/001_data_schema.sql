-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Data database schema for extenddb.
-- These tables live in the data database (separate from the catalog) so that
-- stream records and idempotency tokens can be written atomically with item
-- data within a single TiDB transaction.

-- Stream records — change data capture records. TiDB derives fixed stream
-- shards from table_id. Rows are inserted atomically with item writes using a
-- TiDB transaction TSO plus an in-transaction ordinal as the user-visible
-- sequence number. This avoids shard counters and privileged MVCC inspection.
CREATE TABLE IF NOT EXISTS stream_records (
    record_id BIGINT NOT NULL AUTO_RANDOM(4),
    shard_id VARCHAR(128) NOT NULL,
    sequence_number VARCHAR(64) NOT NULL,
    commit_sequence_number VARCHAR(64),
    table_id VARCHAR(64) NOT NULL,
    event_name VARCHAR(32) NOT NULL,
    record_data JSON NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (record_id) CLUSTERED,
    UNIQUE KEY uk_stream_records_storage_sequence (shard_id, sequence_number),
    INDEX idx_stream_records_commit_sequence (shard_id, commit_sequence_number)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  PRE_SPLIT_REGIONS = 4
  TTL = `created_at` + INTERVAL 24 HOUR TTL_JOB_INTERVAL = '1h';

ALTER TABLE stream_records ATTRIBUTES 'merge_option=deny';

-- Active stream reader leases. DynamoDB allows at most two simultaneous
-- readers per shard. Fixed reader slots plus native unique keys make admission
-- a storage-enforced operation across frontends.
CREATE TABLE IF NOT EXISTS stream_reader_leases (
    shard_id VARCHAR(128) NOT NULL,
    reader_slot TINYINT UNSIGNED NOT NULL,
    reader_id VARCHAR(36) NOT NULL,
    expires_at TIMESTAMP(6) NOT NULL,
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (shard_id, reader_slot) CLUSTERED,
    UNIQUE KEY uk_stream_reader_leases_reader (shard_id, reader_id),
    INDEX idx_stream_reader_leases_expires (expires_at)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  PRE_SPLIT_REGIONS = 4
  TTL = `expires_at` + INTERVAL 0 SECOND TTL_JOB_INTERVAL = '10m';

ALTER TABLE stream_reader_leases ATTRIBUTES 'merge_option=deny';

-- Idempotency token storage for TransactWriteItems.
CREATE TABLE IF NOT EXISTS idempotency_tokens (
    token_id    BIGINT NOT NULL AUTO_RANDOM(4),
    token       VARCHAR(255) NOT NULL,
    token_hash  BIGINT UNSIGNED AS (CRC32(token)) STORED,
    fingerprint TEXT NOT NULL,
    claim_id    VARCHAR(36) NOT NULL,
    created_at  TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (token_id) CLUSTERED,
    UNIQUE KEY uk_idempotency_tokens_token ((TIDB_SHARD(token_hash)), token_hash, token),
    KEY idx_idempotency_tokens_token_lookup (token)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  PRE_SPLIT_REGIONS = 4
  TTL = `created_at` + INTERVAL 600 SECOND TTL_JOB_INTERVAL = '10m';

ALTER TABLE idempotency_tokens ATTRIBUTES 'merge_option=deny';
