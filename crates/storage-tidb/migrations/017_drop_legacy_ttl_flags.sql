-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- User-table TTL state is represented by ttl_status. The old
-- ttl_index_ready/ttl_native_enabled booleans duplicated transition state and
-- could drift from the physical data-table artifacts.

ALTER TABLE tables
    DROP COLUMN IF EXISTS ttl_index_ready,
    DROP COLUMN IF EXISTS ttl_native_enabled;

UPDATE settings SET value = '0.0.17' WHERE `key` = 'catalog_version';
