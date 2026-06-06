// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Expression AST types for condition and update expressions.

/// A single element in a document path.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PathElement {
    /// Attribute name or resolved `#name` reference.
    Attribute(String),
    /// List index dereference: `[0]`.
    Index(usize),
}

/// Comparison operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// Arithmetic operators (used in SET expressions).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithOp {
    Add,
    Sub,
}

/// Expression AST node.
///
/// Used for both condition expressions and value expressions within
/// update actions. The parser produces this tree; the evaluator walks it.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Document path: `address.city`, `tags[0]`, `#n`.
    Path(Vec<PathElement>),
    /// Placeholder reference: `:val1`.
    Placeholder(String),
    /// Binary comparison: `price > :min`.
    Compare {
        left: Box<Expr>,
        op: CompareOp,
        right: Box<Expr>,
    },
    /// Logical AND.
    And(Box<Expr>, Box<Expr>),
    /// Logical OR.
    Or(Box<Expr>, Box<Expr>),
    /// Logical NOT.
    Not(Box<Expr>),
    /// Function call: `attribute_exists(path)`, `attribute_not_exists(path)`.
    Function { name: String, args: Vec<Expr> },
    /// Arithmetic: `price + :tax` (SET expressions only).
    Arithmetic {
        left: Box<Expr>,
        op: ArithOp,
        right: Box<Expr>,
    },
    /// `operand BETWEEN operand AND operand`.
    Between {
        operand: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
    },
    /// `operand IN (operand, operand, ...)`.
    In { operand: Box<Expr>, list: Vec<Expr> },
}

/// A single update action from an `UpdateExpression`.
#[derive(Debug, Clone, PartialEq)]
pub enum UpdateAction {
    /// `SET path = value_expr`
    Set { path: Vec<PathElement>, value: Expr },
    /// `REMOVE path`
    Remove { path: Vec<PathElement> },
    /// `ADD path value` — adds to a number or inserts into a set.
    Add { path: Vec<PathElement>, value: Expr },
    /// `DELETE path value` — removes elements from a set.
    Delete { path: Vec<PathElement>, value: Expr },
}
