// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Abstract syntax tree nodes for sqllogictest.

use std::fmt;

/// A location in a file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Location {
    pub file: String,
    pub line: usize,
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}:{}", self.file, self.line)
    }
}

/// The declared type of an output column in a query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Type {
    /// A text column. Indicated by `T`.
    Text,
    /// An integer column. Indicated by `I`.
    Integer,
    /// A "real" number column (i.e., floating point). Indicated by `R`.
    Real,
    // Please don't add new types to this enum, unless you are adding support
    // for a sqllogictest dialect that has already done so. These type
    // indicators are not meant to be assertions about the output type, but
    // rather instructions to the test runner about any necessary coercions.
    // For example, declaring a column as an `Integer` when the query returns
    // a floating-point will cause the runner to truncate the floating-point
    // bit.
    //
    // In other words, `Bool` and `Oid` are unfortunate additions, as either
    // can be replaced with `Text` wherever it appears.
}

/// Whether to apply sorting before checking the results of a query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Sort {
    /// Do not sort. Default. Indicated by the `nosort` query option.
    No,
    /// Sort each column in each row lexicographically. Indicated by the
    /// `rowsort` query option.
    Row,
    /// Sort each value as though they're in one big list. That is, values are
    /// sorted with no respect for column or row boundaries. Indicated by the
    /// `valuesort` query option.
    Value,
}

impl Sort {
    /// Returns true if any kind of sorting should happen.
    pub fn yes(&self) -> bool {
        use Sort::*;
        match self {
            No => false,
            Row | Value => true,
        }
    }
}

/// A specific assertion about the expected output of a query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Output {
    /// The query should produce the specified values. Note that the values may
    /// need to be sorted according to a [`Sort`] before comparison.
    Values(Vec<String>),
    /// There should be `num_values` results that hash to `md5`. As with
    /// `Output::Values`, the values may need to be sorted according to a
    /// [`Sort`] before hashing.
    Hashed { num_values: usize, md5: String },
}

impl std::fmt::Display for Output {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Output::Values(strings) if strings.len() == 1 => f.write_str(&strings[0]),
            _ => write!(f, "{:?}", self),
        }
    }
}

/// Instructions for assessing the output of a query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryOutput<'a> {
    pub types: Vec<Type>,
    pub sort: Sort,
    pub label: Option<&'a str>,
    pub column_names: Option<Vec<String>>,
    pub output: Output,
    pub output_str: &'a str,
}

/// A single directive in a sqllogictest file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Record<'a> {
    // A `statement` directive.
    Statement {
        location: Location,
        expected_error: Option<&'a str>,
        rows_affected: Option<u64>,
        sql: &'a str,
    },
    /// A `query` directive.
    Query {
        location: Location,
        sql: &'a str,
        output: Result<QueryOutput<'a>, &'a str>,
    },
    /// A `hash-threshold` directive.
    HashThreshold { threshold: u64 },
    /// A `halt` directive.
    Halt,
}
