-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Durable desired-state marker for TiDB-native TTL DDL reconciliation.

ALTER TABLE tables
    ADD COLUMN IF NOT EXISTS ttl_pending_action VARCHAR(16);

UPDATE settings SET value = '0.0.11' WHERE `key` = 'catalog_version';
