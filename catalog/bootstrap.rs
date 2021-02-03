use crate::catalog::*;
use ast::Index;
use context::Context;
use kernel::*;
use zetasql::{function_enums::*, *};

#[derive(Clone)]
pub struct BootstrapCatalog;

impl Catalog for BootstrapCatalog {
    fn catalog(
        &self,
        catalog_id: i64,
        table_names: Vec<Vec<String>>,
        txn: i64,
        context: &Context,
    ) -> SimpleCatalogProto {
        SimpleCatalogProto {
            catalog: vec![bootstrap_metadata_catalog()],
            builtin_function_options: Some(builtin_function_options()),
            ..Default::default()
        }
    }

    fn indexes(&self, table_id: i64, txn: i64, context: &Context) -> Vec<Index> {
        vec![]
    }
}

pub fn bootstrap_tables() -> Vec<(i64, RecordBatch)> {
    vec![(
        5,
        RecordBatch::new(vec![
            (
                "sequence_id".to_string(),
                AnyArray::I64(I64Array::from_values(vec![0, 1, 2])),
            ),
            (
                "sequence_name".to_string(),
                AnyArray::String(StringArray::from_values(vec!["catalog", "table", "index"])),
            ),
        ]),
    )]
}

pub fn bootstrap_sequences() -> Vec<(i64, i64)> {
    vec![(0, 100), (1, 100), (2, 100)]
}

pub fn bootstrap_statistics() -> Vec<(i64, Vec<(&'static str, DataType)>)> {
    vec![
        (
            0, // catalog
            vec![
                ("parent_catalog_id", DataType::I64),
                ("catalog_id", DataType::I64),
                ("catalog_name", DataType::String),
            ],
        ),
        (
            1, // table
            vec![
                ("catalog_id", DataType::I64),
                ("table_id", DataType::I64),
                ("table_name", DataType::String),
            ],
        ),
        (
            2, // column
            vec![
                ("table_id", DataType::I64),
                ("column_id", DataType::I64),
                ("column_name", DataType::String),
                ("column_type", DataType::String),
            ],
        ),
        (
            3, // index
            vec![
                ("catalog_id", DataType::I64),
                ("index_id", DataType::I64),
                ("table_id", DataType::I64),
                ("index_name", DataType::String),
            ],
        ),
        (
            4, // index_column
            vec![
                ("index_id", DataType::I64),
                ("column_id", DataType::I64),
                ("index_order", DataType::I64),
            ],
        ),
        (
            5, // sequence
            vec![
                ("sequence_id", DataType::I64),
                ("sequence_name", DataType::String),
            ],
        ),
    ]
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
        custom_function: metadata_custom_functions(),
        procedure: metadata_procedures(),
        // builtin_function_options: Some(builtin_function_options()),
        ..Default::default()
    }
}

fn metadata_custom_functions() -> Vec<FunctionProto> {
    vec![simple_function(
        "next_val".to_string(),
        vec![TypeKind::TypeInt64],
        TypeKind::TypeInt64,
    )]
}

fn metadata_procedures() -> Vec<ProcedureProto> {
    vec![
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
    ]
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
