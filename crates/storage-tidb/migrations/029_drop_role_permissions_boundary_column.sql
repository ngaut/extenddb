-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Role permissions boundaries live in iam_permissions_boundaries, not on iam_roles.

ALTER TABLE iam_roles DROP COLUMN IF EXISTS permissions_boundary_arn;

UPDATE settings SET value = '0.0.29' WHERE `key` = 'catalog_version';
