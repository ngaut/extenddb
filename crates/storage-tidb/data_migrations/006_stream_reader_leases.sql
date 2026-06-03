-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0

-- Distributed stream reader admission for DynamoDB's two-readers-per-shard
-- quota. The table lives in the TiDB data database beside stream_records so
-- stream consumers are coordinated by TiDB constraints, not frontend-local
-- memory.
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
ALTER TABLE stream_reader_leases TTL_ENABLE = 'ON';
