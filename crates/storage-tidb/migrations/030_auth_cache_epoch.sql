-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0

INSERT IGNORE INTO settings (`key`, value) VALUES ('auth_cache_epoch', '0');

UPDATE settings SET value = '0.0.30' WHERE `key` = 'catalog_version';
