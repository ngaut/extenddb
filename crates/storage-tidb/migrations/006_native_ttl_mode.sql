-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Legacy physical-mode flags for DynamoDB TTL. Later migrations collapse this
-- into ttl_status; current user-table TTL expires through ExtendDB writes.

ALTER TABLE tables
    ADD COLUMN IF NOT EXISTS ttl_native_enabled BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE settings SET value = '0.0.6' WHERE `key` = 'catalog_version';
