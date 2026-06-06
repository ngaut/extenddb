// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Transaction operation types for `DataEngine::transact_get_items` and
//! `DataEngine::transact_write_items`.

use extenddb_core::expression::{Expr, ExpressionMaps, UpdateAction};
use extenddb_core::types::{Item, ReturnValuesOnConditionCheckFailure, TableKeyInfo};

use crate::StreamCapture;

/// A single get operation within a transactional read.
pub struct TransactGetOp<'a> {
    /// Table metadata.
    pub key_info: &'a TableKeyInfo,
    /// Primary key to fetch.
    pub key: &'a Item,
}

/// A single write operation within a transactional write.
pub enum TransactWriteOp<'a> {
    /// Put an item (unconditional or with condition).
    Put {
        /// Table metadata.
        key_info: &'a TableKeyInfo,
        /// The full item to write.
        item: &'a Item,
        /// Optional condition expression.
        condition: Option<&'a Expr>,
        /// Expression maps for condition evaluation.
        maps: &'a ExpressionMaps,
        /// Whether to return the old item on condition failure.
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure,
        /// Stream capture parameters (inserted in the same transaction).
        stream: Option<StreamCapture>,
    },
    /// Delete an item by key (unconditional or with condition).
    Delete {
        /// Table metadata.
        key_info: &'a TableKeyInfo,
        /// Primary key to delete.
        key: &'a Item,
        /// Optional condition expression.
        condition: Option<&'a Expr>,
        /// Expression maps for condition evaluation.
        maps: &'a ExpressionMaps,
        /// Whether to return the old item on condition failure.
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure,
        /// Stream capture parameters (inserted in the same transaction).
        stream: Option<StreamCapture>,
    },
    /// Update an item by key with update actions.
    Update {
        /// Table metadata.
        key_info: &'a TableKeyInfo,
        /// Primary key of the item.
        key: &'a Item,
        /// Parsed update actions.
        actions: &'a [UpdateAction],
        /// Optional condition expression.
        condition: Option<&'a Expr>,
        /// Expression maps for condition and update evaluation.
        maps: &'a ExpressionMaps,
        /// Whether to return the old item on condition failure.
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure,
        /// Stream capture parameters (inserted in the same transaction).
        stream: Option<StreamCapture>,
    },
    /// Check a condition without mutating.
    ConditionCheck {
        /// Table metadata.
        key_info: &'a TableKeyInfo,
        /// Primary key of the item to check.
        key: &'a Item,
        /// Condition expression (required).
        condition: &'a Expr,
        /// Expression maps for condition evaluation.
        maps: &'a ExpressionMaps,
        /// Whether to return the old item on condition failure.
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure,
    },
}
