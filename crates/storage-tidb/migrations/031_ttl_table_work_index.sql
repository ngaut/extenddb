-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0

CREATE INDEX IF NOT EXISTS idx_tables_ttl_work
    ON tables (ttl_status, table_id, table_status);

UPDATE settings SET value = '0.0.31' WHERE `key` = 'catalog_version';
