// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for building expression maps and parsing expressions.

use std::collections::HashMap;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{
    Expr, ExpressionKind, ExpressionMaps, PathElement, Token, parse_condition_with_depth_limit,
    parse_projection, tokenize_for, tokenize_for_with_limits, validate_no_reserved_words,
};
use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::{
    AttributeValue, ConditionalOperator, ExpectedAttributeValue, attribute_value_size,
};

use crate::expected::desugar_expected;

/// Tokenize a typed expression with size limits and DynamoDB-compatible labels.
pub fn tokenize_typed_expression(
    input: &str,
    limits: &LimitsConfig,
    kind: ExpressionKind,
) -> Result<Vec<Token>, DynamoDbError> {
    let tokens = tokenize_for_with_limits(
        input,
        limits.max_expression_tokens,
        limits.max_expression_bytes,
        kind,
    )?;
    if limits.enforce_reserved_keywords {
        validate_no_reserved_words(&tokens)?;
    }
    Ok(tokens)
}

/// Build `ExpressionMaps` from optional request fields.
///
/// Pre-parses all numeric placeholder values into `BigDecimal` so that
/// filter expressions comparing a placeholder against many items parse
/// the placeholder only once per request.
pub fn build_expression_maps(
    names: Option<&HashMap<String, String>>,
    values: Option<&HashMap<String, AttributeValue>>,
) -> ExpressionMaps {
    ExpressionMaps::new(
        names
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.strip_prefix('#').unwrap_or(k).to_owned(), v.clone()))
                    .collect()
            })
            .unwrap_or_default(),
        values
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.strip_prefix(':').unwrap_or(k).to_owned(), v.clone()))
                    .collect()
            })
            .unwrap_or_default(),
    )
}

/// Validate and build `ExpressionMaps` from optional request fields.
pub fn build_checked_expression_maps(
    names: Option<&HashMap<String, String>>,
    values: Option<&HashMap<String, AttributeValue>>,
    limits: &LimitsConfig,
) -> Result<ExpressionMaps, DynamoDbError> {
    validate_expression_attribute_maps(names, values, limits)?;
    Ok(build_expression_maps(names, values))
}

/// Validate DynamoDB aggregate expression attribute map limits.
pub fn validate_expression_attribute_maps(
    names: Option<&HashMap<String, String>>,
    values: Option<&HashMap<String, AttributeValue>>,
    limits: &LimitsConfig,
) -> Result<(), DynamoDbError> {
    if let Some(names) = names {
        let size = expression_attribute_names_size(names);
        if size > limits.max_expression_attribute_names_bytes {
            return Err(expression_attribute_map_size_error(
                "ExpressionAttributeNames",
                limits.max_expression_attribute_names_bytes,
            ));
        }
    }

    if let Some(values) = values {
        let size = expression_attribute_values_size(values);
        if size > limits.max_expression_attribute_values_bytes {
            return Err(expression_attribute_map_size_error(
                "ExpressionAttributeValues",
                limits.max_expression_attribute_values_bytes,
            ));
        }
    }

    Ok(())
}

fn expression_attribute_names_size(names: &HashMap<String, String>) -> usize {
    names.iter().map(|(k, v)| k.len() + v.len()).sum()
}

fn expression_attribute_values_size(values: &HashMap<String, AttributeValue>) -> usize {
    values
        .iter()
        .map(|(k, v)| k.len() + attribute_value_size(v))
        .sum()
}

fn expression_attribute_map_size_error(field: &str, max_bytes: usize) -> DynamoDbError {
    DynamoDbError::ValidationException(format!(
        "{field} size has exceeded the maximum allowed size; maximum: {max_bytes} bytes"
    ))
}

/// Parse an optional condition expression string into an AST.
///
/// Returns `None` if the input is `None` or empty.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` for syntax errors.
pub fn parse_optional_condition(
    expr: Option<&str>,
    limits: &LimitsConfig,
) -> Result<Option<Expr>, DynamoDbError> {
    match expr {
        Some(s) if !s.is_empty() => {
            let tokens = tokenize_typed_expression(s, limits, ExpressionKind::Condition)?;
            let ast = parse_condition_with_depth_limit(&tokens, limits.max_expression_depth)?;
            Ok(Some(ast))
        }
        _ => Ok(None),
    }
}

/// Parse an optional filter expression string into an AST.
///
/// `FilterExpression` uses the same grammar as `ConditionExpression`.
/// Returns `None` if the input is `None` or empty.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` for syntax errors.
pub fn parse_optional_filter(
    expr: Option<&str>,
    limits: &LimitsConfig,
) -> Result<Option<Expr>, DynamoDbError> {
    parse_optional_condition(expr, limits)
        .map_err(|e| prefix_expression_error(e, ExpressionKind::Filter))
}

/// Resolve a condition from either `ConditionExpression` or legacy `Expected`.
///
/// `DynamoDB` rejects requests that specify both. Returns the parsed condition
/// AST and the expression maps to use for evaluation.
///
/// # Errors
///
/// Returns `ValidationException` if both `ConditionExpression` and `Expected` are set,
/// or for any parsing/desugaring errors.
pub fn resolve_condition(
    condition_expression: Option<&str>,
    names: Option<&HashMap<String, String>>,
    values: Option<&HashMap<String, AttributeValue>>,
    expected: Option<&HashMap<String, ExpectedAttributeValue>>,
    conditional_operator: Option<ConditionalOperator>,
    limits: &LimitsConfig,
) -> Result<(Option<Expr>, ExpressionMaps), DynamoDbError> {
    validate_expression_attribute_maps(names, values, limits)?;

    let has_condition = condition_expression.is_some_and(|s| !s.is_empty());
    let has_expected = expected.is_some_and(|m| !m.is_empty());

    if has_condition && has_expected {
        return Err(DynamoDbError::ValidationException(
            "Can not use both expression and non-expression parameters in the same request: \
             Non-expression parameters: {Expected} Expression parameters: {ConditionExpression}"
                .to_owned(),
        ));
    }

    if let Some(exp) = expected.filter(|m| !m.is_empty()) {
        let (expr, mut maps) = desugar_expected(exp, conditional_operator.unwrap_or_default())?;
        // Merge request-level ExpressionAttributeNames/Values so UpdateExpression
        // placeholders still resolve when Expected is used for the condition.
        if let Some(n) = names {
            for (k, v) in n {
                maps.names
                    .entry(k.strip_prefix('#').unwrap_or(k).to_owned())
                    .or_insert_with(|| v.clone());
            }
        }
        if let Some(v) = values {
            for (k, val) in v {
                maps.values
                    .entry(k.strip_prefix(':').unwrap_or(k).to_owned())
                    .or_insert_with(|| val.clone());
            }
        }
        // Re-parse numerics after merging additional values.
        maps.pre_parse_numerics();
        return Ok((Some(expr), maps));
    }

    let maps = build_expression_maps(names, values);
    let condition = parse_optional_condition(condition_expression, limits)?;
    Ok((condition, maps))
}

/// Tokenize, reserved-word check, and parse a `ProjectionExpression`.
///
/// Errors carry the `ProjectionExpression` prefix, matching Amazon DynamoDB.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` for syntax or reserved-word errors.
pub fn parse_projection_expr(
    proj_str: &str,
    limits: &LimitsConfig,
) -> Result<Vec<Vec<PathElement>>, DynamoDbError> {
    let result = tokenize_for(
        proj_str,
        limits.max_expression_tokens,
        ExpressionKind::Projection,
    )
    .and_then(|tokens| {
        if limits.enforce_reserved_keywords {
            validate_no_reserved_words(&tokens)?;
        }
        parse_projection(&tokens)
    });
    result.map_err(|e| prefix_expression_error(e, ExpressionKind::Projection))
}

/// Prefix an expression error with the expression type, matching DynamoDB's format.
///
/// `FilterExpression` shares the condition parser, so its errors arrive labelled
/// `ConditionExpression`; those are relabelled to `expr_type`. Errors already
/// labelled with another expression type, or non-expression validation errors,
/// are returned unchanged.
pub fn prefix_expression_error(err: DynamoDbError, kind: ExpressionKind) -> DynamoDbError {
    match err {
        DynamoDbError::ValidationException(msg) => {
            if let Some(rest) = msg.strip_prefix("Invalid ConditionExpression:") {
                DynamoDbError::ValidationException(format!("Invalid {kind}:{rest}"))
            } else if let Some(rest) = msg.strip_prefix("Invalid expression:") {
                DynamoDbError::ValidationException(format!("Invalid {kind}:{rest}"))
            } else if msg.starts_with("Invalid ") || msg.starts_with("1 validation") {
                DynamoDbError::ValidationException(msg)
            } else {
                DynamoDbError::ValidationException(format!("Invalid {kind}: {msg}"))
            }
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use extenddb_core::limits::LimitsConfig;
    use extenddb_core::types::{AttributeValue, ExpectedAttributeValue};

    const CONDITION_REDUNDANT: &str =
        "Invalid ConditionExpression: The expression has redundant parentheses;";

    #[test]
    fn condition_redundant_parens_rejected_with_canonical_message() {
        let limits = LimitsConfig::default();
        for expr in [
            "((a = :v))",
            "(((a = :v)))",
            "((a = :v AND b = :v2))",
            "((NOT (a = :v)))",
        ] {
            let err = parse_optional_condition(Some(expr), &limits).unwrap_err();
            assert!(
                matches!(&err, DynamoDbError::ValidationException(msg) if msg == CONDITION_REDUNDANT),
                "expr {expr}: got {err:?}"
            );
        }
    }

    #[test]
    fn condition_valid_parens_accepted() {
        let limits = LimitsConfig::default();
        for expr in [
            "(a = :v)",
            "(a = :v) AND (b = :v2)",
            "((a = :v) AND (b = :v2))",
            "(NOT (a = :v))",
        ] {
            assert!(
                parse_optional_condition(Some(expr), &limits).is_ok(),
                "expr {expr} should parse"
            );
        }
    }

    #[test]
    fn filter_redundant_parens_rejected_with_filter_label() {
        let limits = LimitsConfig::default();
        let err = parse_optional_filter(Some("((a = :v))"), &limits).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg)
                if msg == "Invalid FilterExpression: The expression has redundant parentheses;"),
            "got {err:?}"
        );
    }

    #[test]
    fn filter_parser_errors_carry_filter_label() {
        let limits = LimitsConfig::default();
        let err = parse_optional_filter(Some("a"), &limits).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg)
                if msg.starts_with("Invalid FilterExpression:")),
            "got {err:?}"
        );
    }

    #[test]
    fn condition_expression_size_limit_is_enforced_before_parsing() {
        let limits = LimitsConfig {
            max_expression_bytes: 8,
            ..Default::default()
        };
        let err = parse_optional_condition(Some("very_long_attribute_name = :v"), &limits)
            .expect_err("oversized condition expression must fail");

        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg)
                if msg.contains("Expression size has exceeded")),
            "got {err:?}"
        );
    }

    #[test]
    fn filter_expression_size_limit_carries_filter_label() {
        let limits = LimitsConfig {
            max_expression_bytes: 8,
            ..Default::default()
        };
        let err = parse_optional_filter(Some("very_long_attribute_name = :v"), &limits)
            .expect_err("oversized filter expression must fail");

        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg)
                if msg.starts_with("Invalid FilterExpression:")
                    && msg.contains("Expression size has exceeded")),
            "got {err:?}"
        );
    }

    #[test]
    fn typed_expression_size_limit_carries_expression_label() {
        let limits = LimitsConfig {
            max_expression_bytes: 8,
            ..Default::default()
        };
        let err = tokenize_typed_expression(
            "very_long_attribute_name",
            &limits,
            ExpressionKind::Projection,
        )
        .expect_err("oversized typed expression must fail");

        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg)
                if msg.starts_with("Invalid ProjectionExpression:")
                    && msg.contains("Expression size has exceeded")),
            "got {err:?}"
        );
    }

    #[test]
    fn expression_attribute_names_aggregate_limit_is_enforced() {
        let limits = LimitsConfig {
            max_expression_attribute_names_bytes: 8,
            ..Default::default()
        };
        let names = HashMap::from([("#long".to_owned(), "attribute".to_owned())]);

        let err = build_checked_expression_maps(Some(&names), None, &limits)
            .expect_err("oversized ExpressionAttributeNames map must fail");

        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg)
                if msg.starts_with("ExpressionAttributeNames size has exceeded")),
            "got {err:?}"
        );
    }

    #[test]
    fn expression_attribute_values_aggregate_limit_is_enforced() {
        let limits = LimitsConfig {
            max_expression_attribute_values_bytes: 8,
            ..Default::default()
        };
        let values = HashMap::from([(
            ":long".to_owned(),
            AttributeValue::S("substitution".to_owned()),
        )]);

        let err = build_checked_expression_maps(None, Some(&values), &limits)
            .expect_err("oversized ExpressionAttributeValues map must fail");

        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg)
                if msg.starts_with("ExpressionAttributeValues size has exceeded")),
            "got {err:?}"
        );
    }

    #[test]
    fn expected_condition_branch_still_enforces_request_map_limit() {
        let limits = LimitsConfig {
            max_expression_attribute_values_bytes: 8,
            ..Default::default()
        };
        let expected = HashMap::from([(
            "pk".to_owned(),
            ExpectedAttributeValue {
                value: Some(AttributeValue::S("ok".to_owned())),
                exists: None,
                comparison_operator: None,
                attribute_value_list: None,
            },
        )]);
        let values = HashMap::from([(
            ":large".to_owned(),
            AttributeValue::S("substitution".to_owned()),
        )]);

        let err = resolve_condition(None, None, Some(&values), Some(&expected), None, &limits)
            .expect_err("Expected branch must validate request expression maps");

        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg)
                if msg.starts_with("ExpressionAttributeValues size has exceeded")),
            "got {err:?}"
        );
    }
}
