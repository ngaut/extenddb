-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Use a plain token lookup index for the post-claim idempotency read.
--
-- TiDB 8.5.6 can panic in the planner when a prepared SELECT inside a
-- pessimistic transaction probes the TIDB_SHARD(token_hash) expression index
-- after an INSERT ... ON DUPLICATE KEY UPDATE. The sharded unique index remains
-- the native duplicate-detection path; this index gives the follow-up read a
-- stable, non-expression lookup shape.

ALTER TABLE idempotency_tokens
    ADD INDEX IF NOT EXISTS idx_idempotency_tokens_token_lookup (token);
