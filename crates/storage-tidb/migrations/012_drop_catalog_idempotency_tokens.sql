-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- TiDB stores TransactWriteItems idempotency tokens in the data database so
-- token writes commit atomically with item writes and stream records.

DROP TABLE IF EXISTS idempotency_tokens;

UPDATE settings SET value = '0.0.12' WHERE `key` = 'catalog_version';
