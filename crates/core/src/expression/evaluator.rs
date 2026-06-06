// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Condition expression evaluator.
//!
//! Evaluates a parsed condition expression AST against an item, resolving
//! attribute paths and value placeholders from the provided `ExpressionMaps`.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};

use bigdecimal::BigDecimal;

use crate::error::DynamoDbError;
use crate::types::AttributeValue;

use super::ast::{CompareOp, Expr, PathElement};
use super::resolver::{ExpressionMaps, resolve_path};

struct EvalContext<'a> {
    item: &'a BTreeMap<String, AttributeValue>,
    maps: &'a ExpressionMaps,
    parsed_path_numerics: HashMap<Vec<PathElement>, Option<BigDecimal>>,
}

impl<'a> EvalContext<'a> {
    fn new(item: &'a BTreeMap<String, AttributeValue>, maps: &'a ExpressionMaps) -> Self {
        Self {
            item,
            maps,
            parsed_path_numerics: HashMap::new(),
        }
    }

    fn parsed_path_numeric(&mut self, path: &[PathElement], value: &AttributeValue) -> NumericHint {
        let AttributeValue::N(raw) = value else {
            return NumericHint::Uncached;
        };
        let parsed = self
            .parsed_path_numerics
            .entry(path.to_vec())
            .or_insert_with(|| raw.parse::<BigDecimal>().ok());
        match parsed {
            Some(decimal) => NumericHint::Parsed(decimal.clone()),
            None => NumericHint::Invalid,
        }
    }
}

enum NumericHint {
    Parsed(BigDecimal),
    Invalid,
    Uncached,
}

/// Evaluate a condition expression against an item.
///
/// Returns `true` if the condition is satisfied, `false` otherwise.
///
/// # Errors
///
/// Returns `ValidationException` for unresolvable placeholders or type errors.
pub fn evaluate_condition(
    expr: &Expr,
    item: &BTreeMap<String, AttributeValue>,
    maps: &ExpressionMaps,
) -> Result<bool, DynamoDbError> {
    let mut ctx = EvalContext::new(item, maps);
    evaluate_condition_inner(expr, &mut ctx)
}

fn evaluate_condition_inner(expr: &Expr, ctx: &mut EvalContext<'_>) -> Result<bool, DynamoDbError> {
    match expr {
        Expr::Compare { left, op, right } => {
            let lv = resolve_to_value(left, ctx)?;
            let rv = resolve_to_value(right, ctx)?;
            match (&lv, &rv) {
                (Some(l), Some(r)) => {
                    let lpn = numeric_hint(left, l, ctx);
                    let rpn = numeric_hint(right, r, ctx);
                    Ok(compare_values(l, r, *op, lpn, rpn))
                }
                _ => Ok(*op == CompareOp::Ne),
            }
        }
        Expr::And(left, right) => {
            Ok(evaluate_condition_inner(left, ctx)? && evaluate_condition_inner(right, ctx)?)
        }
        Expr::Or(left, right) => {
            Ok(evaluate_condition_inner(left, ctx)? || evaluate_condition_inner(right, ctx)?)
        }
        Expr::Not(inner) => Ok(!evaluate_condition_inner(inner, ctx)?),
        Expr::Function { name, args } => evaluate_function(name, args, ctx),
        Expr::Between { operand, low, high } => {
            let val = resolve_to_value(operand, ctx)?;
            let lo = resolve_to_value(low, ctx)?;
            let hi = resolve_to_value(high, ctx)?;
            // DynamoDB validates bounds when both are literal placeholders
            if matches!(low.as_ref(), Expr::Placeholder(_))
                && matches!(high.as_ref(), Expr::Placeholder(_))
                && let (Some(l), Some(h)) = (lo.as_deref(), hi.as_deref())
            {
                let l_type = attribute_type_code(l);
                let h_type = attribute_type_code(h);
                if l_type != h_type {
                    return Err(DynamoDbError::ValidationException(format!(
                        "Invalid ConditionExpression: The BETWEEN operator requires same data type for lower and upper bounds; lower bound operand: AttributeValue: {{{}}}, upper bound operand: AttributeValue: {{{}}}",
                        format_attribute_value(l),
                        format_attribute_value(h)
                    )));
                }
                let lpn = numeric_hint(low, l, ctx);
                let hpn = numeric_hint(high, h, ctx);
                if compare_values(l, h, CompareOp::Gt, lpn, hpn) {
                    return Err(DynamoDbError::ValidationException(format!(
                        "Invalid ConditionExpression: The BETWEEN operator requires upper bound to be greater than or equal to lower bound; lower bound operand: AttributeValue: {{{}}}, upper bound operand: AttributeValue: {{{}}}",
                        format_attribute_value(l),
                        format_attribute_value(h)
                    )));
                }
            }
            match (&val, &lo, &hi) {
                (Some(v), Some(l), Some(h)) => {
                    let vpn = numeric_hint(operand, v, ctx);
                    let lpn = numeric_hint(low, l, ctx);
                    let hpn = numeric_hint(high, h, ctx);
                    Ok(compare_values(v, l, CompareOp::Ge, vpn, lpn)
                        && compare_values(v, h, CompareOp::Le, numeric_hint(operand, v, ctx), hpn))
                }
                _ => Ok(false),
            }
        }
        Expr::In { operand, list } => {
            let val = resolve_to_value(operand, ctx)?;
            let Some(ref v) = val else { return Ok(false) };
            for candidate in list {
                let cv = resolve_to_value(candidate, ctx)?;
                if let Some(ref c) = cv {
                    let vpn = numeric_hint(operand, v, ctx);
                    let cpn = numeric_hint(candidate, c, ctx);
                    if compare_values(v, c, CompareOp::Eq, vpn, cpn) {
                        return Ok(true);
                    }
                }
            }
            Ok(false)
        }
        _ => Err(DynamoDbError::ValidationException(
            "Invalid ConditionExpression: unexpected expression type".to_owned(),
        )),
    }
}

/// Resolve an expression to an `AttributeValue`.
///
/// Returns `None` if the path points to a missing attribute (not an error).
fn resolve_to_value<'a>(
    expr: &Expr,
    ctx: &EvalContext<'a>,
) -> Result<Option<Cow<'a, AttributeValue>>, DynamoDbError> {
    match expr {
        Expr::Path(elements) => Ok(resolve_path(elements, ctx.item, ctx.maps)?.map(Cow::Borrowed)),
        Expr::Placeholder(name) => Ok(Some(Cow::Borrowed(ctx.maps.resolve_value(name)?))),
        Expr::Function { name, args } if name == "size" => evaluate_size(args, ctx),
        _ => Err(DynamoDbError::ValidationException(
            "Invalid ConditionExpression: expected path or value".to_owned(),
        )),
    }
}

/// Look up a pre-parsed `BigDecimal` for a numeric expression operand.
///
/// Placeholder numerics are stored per request in `ExpressionMaps`; path numerics
/// are cached for this item evaluation.
fn numeric_hint(expr: &Expr, value: &AttributeValue, ctx: &mut EvalContext<'_>) -> NumericHint {
    let AttributeValue::N(raw) = value else {
        return NumericHint::Uncached;
    };
    match expr {
        Expr::Placeholder(name) => ctx.maps.get_parsed_numeric(name).cloned().map_or_else(
            || {
                raw.parse::<BigDecimal>()
                    .map_or(NumericHint::Invalid, NumericHint::Parsed)
            },
            NumericHint::Parsed,
        ),
        Expr::Path(path) => ctx.parsed_path_numeric(path, value),
        _ => raw
            .parse::<BigDecimal>()
            .map_or(NumericHint::Invalid, NumericHint::Parsed),
    }
}

/// Compare two `AttributeValue`s using the given operator.
///
/// `DynamoDB` comparison rules:
/// - Same-type comparisons only (except N vs N which is numeric)
/// - S: lexicographic UTF-8
/// - N: numeric comparison via `BigDecimal` (pre-parsed values used when available)
/// - B: lexicographic byte comparison
/// - BOOL, NULL, L, M, SS, NS, BS: only equality
fn compare_values(
    left: &AttributeValue,
    right: &AttributeValue,
    op: CompareOp,
    left_parsed: NumericHint,
    right_parsed: NumericHint,
) -> bool {
    match (left, right) {
        (AttributeValue::S(l), AttributeValue::S(r)) => apply_op(l.cmp(r), op),
        (AttributeValue::N(l), AttributeValue::N(r)) => {
            let Some(ld) = numeric_decimal(left_parsed, l) else {
                return false;
            };
            let Some(rd) = numeric_decimal(right_parsed, r) else {
                return false;
            };
            apply_op(ld.cmp(&rd), op)
        }
        (AttributeValue::B(l), AttributeValue::B(r)) => apply_op(l.cmp(r), op),
        // For non-orderable types, only equality is meaningful
        (l, r) if l == r => matches!(op, CompareOp::Eq | CompareOp::Le | CompareOp::Ge),
        _ => matches!(op, CompareOp::Ne),
    }
}

fn numeric_decimal(hint: NumericHint, raw: &str) -> Option<BigDecimal> {
    match hint {
        NumericHint::Parsed(decimal) => Some(decimal),
        NumericHint::Invalid => None,
        NumericHint::Uncached => raw.parse::<BigDecimal>().ok(),
    }
}

fn apply_op(ordering: Ordering, op: CompareOp) -> bool {
    match op {
        CompareOp::Eq => ordering == Ordering::Equal,
        CompareOp::Ne => ordering != Ordering::Equal,
        CompareOp::Lt => ordering == Ordering::Less,
        CompareOp::Le => ordering != Ordering::Greater,
        CompareOp::Gt => ordering == Ordering::Greater,
        CompareOp::Ge => ordering != Ordering::Less,
    }
}

/// Evaluate a built-in function.
fn evaluate_function(
    name: &str,
    args: &[Expr],
    ctx: &mut EvalContext<'_>,
) -> Result<bool, DynamoDbError> {
    match name {
        "attribute_exists" => {
            if args.len() != 1 {
                return Err(DynamoDbError::ValidationException(
                    "Invalid ConditionExpression: attribute_exists requires exactly one argument"
                        .to_owned(),
                ));
            }
            let val = resolve_to_value(&args[0], ctx)?;
            Ok(val.is_some())
        }
        "attribute_not_exists" => {
            if args.len() != 1 {
                return Err(DynamoDbError::ValidationException(
                    "Invalid ConditionExpression: attribute_not_exists requires exactly one argument"
                        .to_owned(),
                ));
            }
            let val = resolve_to_value(&args[0], ctx)?;
            Ok(val.is_none())
        }
        "attribute_type" => {
            if args.len() != 2 {
                return Err(DynamoDbError::ValidationException(
                    "Invalid ConditionExpression: attribute_type requires exactly two arguments"
                        .to_owned(),
                ));
            }
            let val = resolve_to_value(&args[0], ctx)?;
            let type_val = resolve_to_value(&args[1], ctx)?;
            let Some(ref v) = val else { return Ok(false) };
            let Some(ref tv) = type_val else {
                return Err(DynamoDbError::ValidationException(
                    "Invalid ConditionExpression: attribute_type second argument must be a string"
                        .to_owned(),
                ));
            };
            let AttributeValue::S(ref type_str) = **tv else {
                return Err(DynamoDbError::ValidationException(
                    "Invalid ConditionExpression: attribute_type second argument must be a string"
                        .to_owned(),
                ));
            };
            Ok(attribute_type_code(v) == type_str.as_str())
        }
        "begins_with" => {
            if args.len() != 2 {
                return Err(DynamoDbError::ValidationException(
                    "Invalid ConditionExpression: begins_with requires exactly two arguments"
                        .to_owned(),
                ));
            }
            let val = resolve_to_value(&args[0], ctx)?;
            let prefix = resolve_to_value(&args[1], ctx)?;
            // Reject invalid operand types — only S and B are allowed
            if let Some(ref p) = prefix.as_deref()
                && !matches!(p, AttributeValue::S(_) | AttributeValue::B(_))
            {
                let type_code = attribute_type_code(p);
                return Err(DynamoDbError::ValidationException(format!(
                    "Invalid ConditionExpression: Incorrect operand type for operator or function; operator or function: begins_with, operand type: {type_code}"
                )));
            }
            match (val.as_deref(), prefix.as_deref()) {
                (Some(AttributeValue::S(s)), Some(AttributeValue::S(p))) => {
                    Ok(s.starts_with(p.as_str()))
                }
                (Some(AttributeValue::B(s)), Some(AttributeValue::B(p))) => {
                    Ok(s.starts_with(p.as_slice()))
                }
                _ => Ok(false),
            }
        }
        "contains" => {
            if args.len() != 2 {
                return Err(DynamoDbError::ValidationException(
                    "Invalid ConditionExpression: contains requires exactly two arguments"
                        .to_owned(),
                ));
            }
            if args[0] == args[1] {
                let operand_str = match &args[0] {
                    Expr::Path(p) => {
                        let parts: Vec<String> = p
                            .iter()
                            .map(|e| match e {
                                PathElement::Attribute(a) => {
                                    if let Some(ref_name) = a.strip_prefix('#') {
                                        ctx.maps
                                            .names
                                            .get(ref_name)
                                            .cloned()
                                            .unwrap_or_else(|| a.clone())
                                    } else {
                                        a.clone()
                                    }
                                }
                                PathElement::Index(i) => format!("[{i}]"),
                            })
                            .collect();
                        format!("[{}]", parts.join("."))
                    }
                    _ => String::new(),
                };
                return Err(DynamoDbError::ValidationException(format!(
                    "Invalid ConditionExpression: The first operand must be distinct from the remaining operands for this operator or function; operator: contains, first operand: {operand_str}"
                )));
            }
            let val = resolve_to_value(&args[0], ctx)?;
            let operand = resolve_to_value(&args[1], ctx)?;
            match (val.as_deref(), operand.as_deref()) {
                (Some(v), Some(o)) => Ok(contains_check(v, o)),
                _ => Ok(false),
            }
        }
        "size" => {
            // size() used as a standalone condition is invalid — it returns a number.
            // It should only appear as an operand in a comparison.
            Err(DynamoDbError::ValidationException(
                "Invalid ConditionExpression: size function must be used in a comparison"
                    .to_owned(),
            ))
        }
        _ => Err(DynamoDbError::ValidationException(format!(
            "Invalid ConditionExpression: unknown function '{name}'"
        ))),
    }
}

/// Evaluate the `size()` function, returning the size as a number `AttributeValue`.
///
/// Returns `None` if the argument resolves to a missing attribute, matching
/// DynamoDB's behavior where `size(nonexistent)` causes the enclosing
/// comparison to evaluate to false (the item is skipped).
fn evaluate_size<'a>(
    args: &[Expr],
    ctx: &EvalContext<'a>,
) -> Result<Option<Cow<'a, AttributeValue>>, DynamoDbError> {
    if args.len() != 1 {
        return Err(DynamoDbError::ValidationException(
            "Invalid ConditionExpression: size requires exactly one argument".to_owned(),
        ));
    }
    let val = resolve_to_value(&args[0], ctx)?;
    let Some(ref v) = val else {
        // Attribute does not exist — propagate None so the comparison short-circuits.
        return Ok(None);
    };
    let sz = match v.as_ref() {
        AttributeValue::S(s) => s.encode_utf16().count(),
        AttributeValue::B(b) => b.len(),
        AttributeValue::N(n) => n.len(), // ASCII digits are 1 byte each, so len() == UTF-8 byte count
        AttributeValue::L(l) => l.len(),
        AttributeValue::M(m) => m.len(),
        AttributeValue::SS(s) | AttributeValue::NS(s) => s.len(),
        AttributeValue::BS(s) => s.len(),
        AttributeValue::Bool(_) | AttributeValue::Null => {
            return Err(DynamoDbError::ValidationException(
                "Invalid ConditionExpression: size is not supported for this type".to_owned(),
            ));
        }
    };
    Ok(Some(Cow::Owned(AttributeValue::N(sz.to_string()))))
}

/// Return the `DynamoDB` type code for an `AttributeValue`.
fn attribute_type_code(val: &AttributeValue) -> &'static str {
    match val {
        AttributeValue::S(_) => "S",
        AttributeValue::N(_) => "N",
        AttributeValue::B(_) => "B",
        AttributeValue::Bool(_) => "BOOL",
        AttributeValue::Null => "NULL",
        AttributeValue::L(_) => "L",
        AttributeValue::M(_) => "M",
        AttributeValue::SS(_) => "SS",
        AttributeValue::NS(_) => "NS",
        AttributeValue::BS(_) => "BS",
    }
}

/// Format an `AttributeValue` for error messages (e.g. `N:5`, `S:hello`).
fn format_attribute_value(val: &AttributeValue) -> String {
    match val {
        AttributeValue::S(s) => format!("S:{s}"),
        AttributeValue::N(n) => format!("N:{n}"),
        AttributeValue::B(_) => "B:<binary>".to_owned(),
        _ => format!("{}:<value>", attribute_type_code(val)),
    }
}

/// Check if a container contains an operand.
///
/// `DynamoDB` `contains` semantics:
/// - String contains substring
/// - Set (SS/NS/BS) contains element
/// - List (L) contains element
fn contains_check(container: &AttributeValue, operand: &AttributeValue) -> bool {
    match (container, operand) {
        (AttributeValue::S(s), AttributeValue::S(sub)) => s.contains(sub.as_str()),
        (AttributeValue::SS(set), AttributeValue::S(val))
        | (AttributeValue::NS(set), AttributeValue::N(val)) => set.contains(val),
        (AttributeValue::BS(set), AttributeValue::B(val)) => set.contains(val),
        (AttributeValue::L(list), val) => list.contains(val),
        _ => false,
    }
}

#[cfg(test)]
#[path = "evaluator_tests.rs"]
mod tests;
