use crate::catalog::*;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::*;
use std::collections::HashMap;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use zetasql::function_enums::*;
use zetasql::SimpleCatalogProto;
use zetasql::*;

pub const ROOT_CATALOG_PARENT_ID: i64 = -1;
pub const ROOT_CATALOG_ID: i64 = 0;

pub fn bootstrap() -> Catalog {
    Catalog {
        catalog_id: ROOT_CATALOG_ID,
        catalog: bootstrap_zetasql(),
        indexes: HashMap::new(),
    }
}

pub fn bootstrap_sequences() -> Vec<AtomicI64> {
    let table_id = AtomicI64::new(100);
    vec![table_id]
}

pub fn bootstrap_tables() -> HashMap<i64, RecordBatch> {
    let catalog = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("parent_catalog_id", DataType::Int64, true),
            Field::new("catalog_id", DataType::Int64, true),
            Field::new("catalog_name", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![ROOT_CATALOG_PARENT_ID])),
            Arc::new(Int64Array::from(vec![ROOT_CATALOG_ID])),
            Arc::new(StringArray::from(vec!["root"])),
        ],
    )
    .unwrap();
    let sequence = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("sequence_id", DataType::Int64, true),
            Field::new("sequence_name", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![0])),
            Arc::new(StringArray::from(vec!["table"])),
        ],
    )
    .unwrap();
    let mut tables = HashMap::new();
    tables.insert(0, catalog);
    tables.insert(5, sequence);
    tables
}

fn bootstrap_zetasql() -> SimpleCatalogProto {
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
        builtin_function_options: Some(ZetaSqlBuiltinFunctionOptionsProto {
            language_options: Some(LanguageOptionsProto {
                enabled_language_features: enabled_language_features(),
                supported_statement_kinds: supported_statement_kinds(),
                ..Default::default()
            }),
            include_function_ids: enabled_functions(),
            ..Default::default()
        }),
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
