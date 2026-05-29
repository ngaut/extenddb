-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Stream shards and stream records live in the TiDB data database so item
-- writes and stream capture can commit in one transaction. Keep the catalog
-- database limited to control-plane metadata.

DROP TABLE IF EXISTS stream_records;
DROP TABLE IF EXISTS stream_shards;
DROP TABLE IF EXISTS stream_sequence;
