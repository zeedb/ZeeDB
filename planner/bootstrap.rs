use std::collections::HashMap;

use ast::{Expr, Index};
use catalog_types::Catalog;
use context::Context;
use kernel::AnyArray;
use remote_execution::{RecordStream, RemoteExecution};
use rpc::TraceEvent;
use statistics::ColumnStatistics;
use zetasql::{function_enums::*, *};

use catalog_types::builtin_function_options;

pub struct BootstrapStatistics;

impl RemoteExecution for BootstrapStatistics {
    fn approx_cardinality(&self, _table_id: i64) -> f64 {
        1.0
    }

    fn column_statistics(&self, _table_id: i64, _column_name: &str) -> Option<ColumnStatistics> {
        None
    }

    fn submit(&self, _expr: Expr, _txn: i64) -> RecordStream {
        unimplemented!()
    }

    fn trace(&self, _events: Vec<TraceEvent>, _txn: i64, _stage: i32, _worker: i32) {
        todo!()
    }

    fn broadcast(
        &self,
        _expr: Expr,
        _variables: HashMap<String, AnyArray>,
        _txn: i64,
        _stage: i32,
    ) -> RecordStream {
        unimplemented!()
    }

    fn exchange(
        &self,
        _expr: Expr,
        _variables: HashMap<String, AnyArray>,
        _txn: i64,
        _stage: i32,
        _hash_column: String,
        _hash_bucket: i32,
    ) -> RecordStream {
        unimplemented!()
    }
}

#[derive(Clone)]
pub struct BootstrapCatalog;

impl Catalog for BootstrapCatalog {
    fn catalog(
        &self,
        _catalog_id: i64,
        _table_names: Vec<Vec<String>>,
        _txn: i64,
        _context: &Context,
    ) -> SimpleCatalogProto {
        SimpleCatalogProto {
            catalog: vec![bootstrap_metadata_catalog()],
            builtin_function_options: Some(builtin_function_options()),
            custom_function: vec![
                simple_function(
                    "get_var".to_string(),
                    vec![TypeKind::TypeString],
                    TypeKind::TypeInt64,
                ),
                simple_function(
                    "next_val".to_string(),
                    vec![TypeKind::TypeInt64],
                    TypeKind::TypeInt64,
                ),
                simple_function(
                    "is_empty".to_string(),
                    vec![TypeKind::TypeInt64],
                    TypeKind::TypeBool,
                ),
            ],
            procedure: vec![
                simple_procedure(
                    "set_var".to_string(),
                    vec![TypeKind::TypeString, TypeKind::TypeInt64],
                    TypeKind::TypeBool,
                ),
                simple_procedure(
                    "create_table".to_string(),
                    vec![TypeKind::TypeInt64],
                    TypeKind::TypeBool,
                ),
                simple_procedure(
                    "drop_table".to_string(),
                    vec![TypeKind::TypeInt64],
                    TypeKind::TypeBool,
                ),
                simple_procedure(
                    "create_index".to_string(),
                    vec![TypeKind::TypeInt64],
                    TypeKind::TypeBool,
                ),
                simple_procedure(
                    "drop_index".to_string(),
                    vec![TypeKind::TypeInt64],
                    TypeKind::TypeBool,
                ),
            ],
            ..Default::default()
        }
    }

    fn indexes(&self, _table_id: i64, _txn: i64, _context: &Context) -> Vec<Index> {
        vec![]
    }
}

pub fn bootstrap_metadata_catalog() -> SimpleCatalogProto {
    let mut count = 0;
    let mut table = |name: &str, columns: Vec<SimpleColumnProto>| -> SimpleTableProto {
        let serialization_id = count;
        count += 1;
        SimpleTableProto {
            name: Some(String::from(name)),
            column: columns,
            serialization_id: Some(serialization_id),
            ..Default::default()
        }
    };
    let column = |name: &str, kind: TypeKind| -> SimpleColumnProto {
        SimpleColumnProto {
            name: Some(String::from(name)),
            r#type: Some(TypeProto {
                type_kind: Some(kind as i32),
                ..Default::default()
            }),
            ..Default::default()
        }
    };
    SimpleCatalogProto {
        name: Some("metadata".to_string()),
        table: vec![
            table(
                "catalog",
                vec![
                    column("parent_catalog_id", TypeKind::TypeInt64),
                    column("catalog_id", TypeKind::TypeInt64),
                    column("catalog_name", TypeKind::TypeString),
                ],
            ),
            table(
                "table",
                vec![
                    column("catalog_id", TypeKind::TypeInt64),
                    column("table_id", TypeKind::TypeInt64),
                    column("table_name", TypeKind::TypeString),
                ],
            ),
            table(
                "column",
                vec![
                    column("table_id", TypeKind::TypeInt64),
                    column("column_id", TypeKind::TypeInt64),
                    column("column_name", TypeKind::TypeString),
                    column("column_type", TypeKind::TypeString),
                ],
            ),
            table(
                "index",
                vec![
                    column("catalog_id", TypeKind::TypeInt64),
                    column("index_id", TypeKind::TypeInt64),
                    column("table_id", TypeKind::TypeInt64),
                    column("index_name", TypeKind::TypeString),
                ],
            ),
            table(
                "index_column",
                vec![
                    column("index_id", TypeKind::TypeInt64),
                    column("column_id", TypeKind::TypeInt64),
                    column("index_order", TypeKind::TypeInt64),
                ],
            ),
            table(
                "sequence",
                vec![
                    column("sequence_id", TypeKind::TypeInt64),
                    column("sequence_name", TypeKind::TypeString),
                ],
            ),
        ],
        ..Default::default()
    }
}

fn simple_function(name: String, arguments: Vec<TypeKind>, returns: TypeKind) -> FunctionProto {
    FunctionProto {
        name_path: vec![name],
        group: Some("system".to_string()),
        signature: vec![simple_signature(arguments, returns)],
        mode: Some(Mode::Scalar as i32),
        ..Default::default()
    }
}

fn simple_procedure(name: String, arguments: Vec<TypeKind>, returns: TypeKind) -> ProcedureProto {
    ProcedureProto {
        name_path: vec![name],
        signature: Some(simple_signature(arguments, returns)),
        ..Default::default()
    }
}

fn simple_signature(mut arguments: Vec<TypeKind>, returns: TypeKind) -> FunctionSignatureProto {
    let argument_types = arguments.drain(..).map(simple_argument).collect();
    let return_type = simple_argument(returns);
    FunctionSignatureProto {
        argument: argument_types,
        return_type: Some(return_type),
        ..Default::default()
    }
}

fn simple_argument(argument_type: TypeKind) -> FunctionArgumentTypeProto {
    FunctionArgumentTypeProto {
        r#type: Some(TypeProto {
            type_kind: Some(argument_type as i32),
            ..Default::default()
        }),
        kind: Some(SignatureArgumentKind::ArgTypeFixed as i32),
        num_occurrences: Some(1),
        options: Some(FunctionArgumentTypeOptionsProto {
            cardinality: Some(ArgumentCardinality::Required as i32),
            ..Default::default()
        }),
        ..Default::default()
    }
}
