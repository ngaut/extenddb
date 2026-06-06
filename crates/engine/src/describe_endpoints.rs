// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
use serde_json::{Value, json};

use extenddb_core::error::DynamoDbError;

/// REQ-SDK-002: `DescribeEndpoints` returns the server's own address.
pub fn handle_describe_endpoints(server_addr: &str) -> Result<Value, DynamoDbError> {
    Ok(json!({
        "Endpoints": [{
            "Address": server_addr,
            "CachePeriodInMinutes": 10
        }]
    }))
}
