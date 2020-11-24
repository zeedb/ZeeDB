use arrow::datatypes::{DataType, Field, Schema};
use zetasql::function_enums::*;
use zetasql::*;

pub const ROOT_CATALOG_ID: i64 = 0;

pub fn catalog() -> SimpleCatalogProto {
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
        ..Default::default()
    }
}

pub fn metadata_arrow() -> Vec<Schema> {
    let parent_catalog_id = Field::new("parent_catalog_id", DataType::Int64, false);
    let catalog_id = Field::new("catalog_id", DataType::Int64, false);
    let table_id = Field::new("table_id", DataType::Int64, false);
    let column_id = Field::new("column_id", DataType::Int64, false);
    let sequence_id = Field::new("sequence_id", DataType::Int64, false);
    let index_id = Field::new("index_id", DataType::Int64, false);
    let index_order = Field::new("index_order", DataType::Int64, false);
    let catalog_name = Field::new("catalog_name", DataType::Utf8, false);
    let table_name = Field::new("table_name", DataType::Utf8, false);
    let column_name = Field::new("column_name", DataType::Utf8, false);
    let column_type = Field::new("column_type", DataType::Utf8, false);
    let sequence_name = Field::new("sequence_name", DataType::Utf8, false);
    let index_name = Field::new("index_name", DataType::Utf8, false);
    let catalog = Schema::new(vec![
        parent_catalog_id.clone(),
        catalog_id.clone(),
        catalog_name.clone(),
    ]);
    let table = Schema::new(vec![
        catalog_id.clone(),
        table_id.clone(),
        table_name.clone(),
    ]);
    let column = Schema::new(vec![
        table_id.clone(),
        column_id.clone(),
        column_name.clone(),
        column_type.clone(),
    ]);
    let index = Schema::new(vec![
        catalog_id.clone(),
        index_id.clone(),
        table_id.clone(),
        index_name.clone(),
    ]);
    let index_column = Schema::new(vec![
        index_id.clone(),
        column_id.clone(),
        index_order.clone(),
    ]);
    let sequence = Schema::new(vec![sequence_id, sequence_name]);
    vec![catalog, table, column, index, index_column, sequence]
}

pub fn metadata_zetasql() -> SimpleCatalogProto {
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

pub fn enabled_language_features() -> Vec<i32> {
    vec![
        LanguageFeature::FeatureDmlUpdateWithJoin as i32,
        LanguageFeature::FeatureNumericType as i32,
        LanguageFeature::FeatureNamedArguments as i32,
        LanguageFeature::FeatureV11SelectStarExceptReplace as i32,
        LanguageFeature::FeatureV12CorrelatedRefsInNestedDml as i32,
        LanguageFeature::FeatureV12WeekWithWeekday as i32,
        LanguageFeature::FeatureV13OmitInsertColumnList as i32,
        LanguageFeature::FeatureV13NullsFirstLastInOrderBy as i32,
        LanguageFeature::FeatureV13ConcatMixedTypes as i32,
    ]
}

pub fn supported_statement_kinds() -> Vec<i32> {
    // TODO shrink down to things that are actually supported.
    vec![
        ResolvedNodeKind::ResolvedLiteral as i32,
        ResolvedNodeKind::ResolvedParameter as i32,
        ResolvedNodeKind::ResolvedColumnRef as i32,
        ResolvedNodeKind::ResolvedFunctionCall as i32,
        ResolvedNodeKind::ResolvedAggregateFunctionCall as i32,
        ResolvedNodeKind::ResolvedCast as i32,
        ResolvedNodeKind::ResolvedSubqueryExpr as i32,
        ResolvedNodeKind::ResolvedSingleRowScan as i32,
        ResolvedNodeKind::ResolvedTableScan as i32,
        ResolvedNodeKind::ResolvedJoinScan as i32,
        ResolvedNodeKind::ResolvedFilterScan as i32,
        ResolvedNodeKind::ResolvedAggregateScan as i32,
        ResolvedNodeKind::ResolvedSetOperationItem as i32,
        ResolvedNodeKind::ResolvedSetOperationScan as i32,
        ResolvedNodeKind::ResolvedOrderByScan as i32,
        ResolvedNodeKind::ResolvedLimitOffsetScan as i32,
        ResolvedNodeKind::ResolvedWithRefScan as i32,
        ResolvedNodeKind::ResolvedComputedColumn as i32,
        ResolvedNodeKind::ResolvedOrderByItem as i32,
        ResolvedNodeKind::ResolvedColumnAnnotations as i32,
        ResolvedNodeKind::ResolvedColumnDefinition as i32,
        ResolvedNodeKind::ResolvedOutputColumn as i32,
        ResolvedNodeKind::ResolvedProjectScan as i32,
        ResolvedNodeKind::ResolvedQueryStmt as i32,
        ResolvedNodeKind::ResolvedCreateDatabaseStmt as i32,
        ResolvedNodeKind::ResolvedIndexItem as i32,
        ResolvedNodeKind::ResolvedCreateIndexStmt as i32,
        ResolvedNodeKind::ResolvedCreateSchemaStmt as i32,
        ResolvedNodeKind::ResolvedCreateTableStmt as i32,
        ResolvedNodeKind::ResolvedDropStmt as i32,
        ResolvedNodeKind::ResolvedWithScan as i32,
        ResolvedNodeKind::ResolvedWithEntry as i32,
        ResolvedNodeKind::ResolvedDmlvalue as i32,
        ResolvedNodeKind::ResolvedInsertRow as i32,
        ResolvedNodeKind::ResolvedInsertStmt as i32,
        ResolvedNodeKind::ResolvedDeleteStmt as i32,
        ResolvedNodeKind::ResolvedUpdateItem as i32,
        ResolvedNodeKind::ResolvedUpdateStmt as i32,
        ResolvedNodeKind::ResolvedCallStmt as i32,
        ResolvedNodeKind::ResolvedAggregateHavingModifier as i32,
        ResolvedNodeKind::ResolvedSingleAssignmentStmt as i32,
    ]
}

fn enabled_functions() -> Vec<i32> {
    vec![
        FunctionSignatureId::FnAddDouble as i32,
        FunctionSignatureId::FnAddInt64 as i32,
        FunctionSignatureId::FnAddNumeric as i32,
        FunctionSignatureId::FnAnd as i32,
        FunctionSignatureId::FnCaseNoValue as i32,
        FunctionSignatureId::FnCaseWithValue as i32,
        FunctionSignatureId::FnDivideDouble as i32,
        FunctionSignatureId::FnDivideNumeric as i32,
        FunctionSignatureId::FnGreater as i32,
        FunctionSignatureId::FnGreaterOrEqual as i32,
        FunctionSignatureId::FnLess as i32,
        FunctionSignatureId::FnLessOrEqual as i32,
        FunctionSignatureId::FnEqual as i32,
        FunctionSignatureId::FnStringLike as i32,
        FunctionSignatureId::FnIn as i32,
        FunctionSignatureId::FnBetween as i32,
        FunctionSignatureId::FnIsNull as i32,
        FunctionSignatureId::FnMultiplyDouble as i32,
        FunctionSignatureId::FnMultiplyInt64 as i32,
        FunctionSignatureId::FnMultiplyNumeric as i32,
        FunctionSignatureId::FnNot as i32,
        FunctionSignatureId::FnNotEqual as i32,
        FunctionSignatureId::FnOr as i32,
        FunctionSignatureId::FnSubtractDouble as i32,
        FunctionSignatureId::FnSubtractInt64 as i32,
        FunctionSignatureId::FnSubtractNumeric as i32,
        FunctionSignatureId::FnUnaryMinusInt64 as i32,
        FunctionSignatureId::FnUnaryMinusDouble as i32,
        FunctionSignatureId::FnUnaryMinusNumeric as i32,
        // Bitwise unary operators.
        FunctionSignatureId::FnBitwiseNotInt64 as i32,
        // Bitwise binary operators.
        FunctionSignatureId::FnBitwiseOrInt64 as i32,
        FunctionSignatureId::FnBitwiseXorInt64 as i32,
        FunctionSignatureId::FnBitwiseAndInt64 as i32,
        // For all bitwise shift operators, the second argument has int64 type as i32.
        // Expected behavior of bitwise shift operations:
        // * Shifting by a negative offset is an error.
        // * Shifting right on signed values does not do sign extension.
        FunctionSignatureId::FnBitwiseLeftShiftInt64 as i32,
        FunctionSignatureId::FnBitwiseRightShiftInt64 as i32,
        // BITCOUNT functions.
        FunctionSignatureId::FnBitCountInt64 as i32,
        // String functions
        FunctionSignatureId::FnConcatString as i32,
        FunctionSignatureId::FnStrposString as i32,
        FunctionSignatureId::FnLowerString as i32,
        FunctionSignatureId::FnUpperString as i32,
        FunctionSignatureId::FnLengthString as i32,
        FunctionSignatureId::FnStartsWithString as i32,
        FunctionSignatureId::FnEndsWithString as i32,
        FunctionSignatureId::FnSubstrString as i32,
        FunctionSignatureId::FnTrimString as i32,
        FunctionSignatureId::FnLtrimString as i32,
        FunctionSignatureId::FnRtrimString as i32,
        FunctionSignatureId::FnReplaceString as i32,
        FunctionSignatureId::FnRegexpExtractString as i32,
        FunctionSignatureId::FnRegexpReplaceString as i32,
        FunctionSignatureId::FnRegexpExtractAllString as i32,
        FunctionSignatureId::FnByteLengthString as i32,
        FunctionSignatureId::FnCharLengthString as i32,
        FunctionSignatureId::FnSplitString as i32,
        FunctionSignatureId::FnRegexpContainsString as i32,
        FunctionSignatureId::FnToBase64 as i32,
        FunctionSignatureId::FnFromBase64 as i32,
        FunctionSignatureId::FnToHex as i32,
        FunctionSignatureId::FnFromHex as i32,
        FunctionSignatureId::FnLpadString as i32,
        FunctionSignatureId::FnRpadString as i32,
        FunctionSignatureId::FnRepeatString as i32,
        FunctionSignatureId::FnReverseString as i32,
        // Control flow functions
        FunctionSignatureId::FnIf as i32,
        // Coalesce is used to express the output join column in FULL JOIN.
        FunctionSignatureId::FnCoalesce as i32,
        FunctionSignatureId::FnIfnull as i32,
        // Time functions
        FunctionSignatureId::FnDateAddDate as i32,
        FunctionSignatureId::FnTimestampAdd as i32,
        FunctionSignatureId::FnDateDiffDate as i32,
        FunctionSignatureId::FnTimestampDiff as i32,
        FunctionSignatureId::FnDateSubDate as i32,
        FunctionSignatureId::FnTimestampSub as i32,
        FunctionSignatureId::FnDateTruncDate as i32,
        FunctionSignatureId::FnTimestampTrunc as i32,
        FunctionSignatureId::FnDateFromUnixDate as i32,
        FunctionSignatureId::FnTimestampFromInt64Seconds as i32,
        FunctionSignatureId::FnTimestampFromInt64Millis as i32,
        FunctionSignatureId::FnTimestampFromInt64Micros as i32,
        FunctionSignatureId::FnTimestampFromUnixSecondsInt64 as i32,
        FunctionSignatureId::FnTimestampFromUnixSecondsTimestamp as i32,
        FunctionSignatureId::FnTimestampFromUnixMillisInt64 as i32,
        FunctionSignatureId::FnTimestampFromUnixMillisTimestamp as i32,
        FunctionSignatureId::FnTimestampFromUnixMicrosInt64 as i32,
        FunctionSignatureId::FnTimestampFromUnixMicrosTimestamp as i32,
        FunctionSignatureId::FnUnixDate as i32,
        FunctionSignatureId::FnUnixSecondsFromTimestamp as i32,
        FunctionSignatureId::FnUnixMillisFromTimestamp as i32,
        FunctionSignatureId::FnUnixMicrosFromTimestamp as i32,
        FunctionSignatureId::FnDateFromYearMonthDay as i32,
        FunctionSignatureId::FnTimestampFromString as i32,
        // Signatures for extracting date parts, taking a date/timestam as i32p
        // and the target date part as arguments.
        FunctionSignatureId::FnExtractFromDate as i32,
        FunctionSignatureId::FnExtractFromTimestamp as i32,
        // TIMESTAMP.
        FunctionSignatureId::FnExtractDateFromTimestamp as i32,
        // Math functions
        FunctionSignatureId::FnAbsInt64 as i32,
        FunctionSignatureId::FnAbsDouble as i32,
        FunctionSignatureId::FnAbsNumeric as i32,
        FunctionSignatureId::FnSignInt64 as i32,
        FunctionSignatureId::FnSignDouble as i32,
        FunctionSignatureId::FnSignNumeric as i32,
        FunctionSignatureId::FnRoundDouble as i32,
        FunctionSignatureId::FnRoundNumeric as i32,
        FunctionSignatureId::FnRoundWithDigitsDouble as i32,
        FunctionSignatureId::FnRoundWithDigitsNumeric as i32,
        FunctionSignatureId::FnTruncDouble as i32,
        FunctionSignatureId::FnTruncNumeric as i32,
        FunctionSignatureId::FnTruncWithDigitsDouble as i32,
        FunctionSignatureId::FnTruncWithDigitsNumeric as i32,
        FunctionSignatureId::FnCeilDouble as i32,
        FunctionSignatureId::FnCeilNumeric as i32,
        FunctionSignatureId::FnFloorDouble as i32,
        FunctionSignatureId::FnFloorNumeric as i32,
        FunctionSignatureId::FnModInt64 as i32,
        FunctionSignatureId::FnModNumeric as i32,
        FunctionSignatureId::FnDivInt64 as i32,
        FunctionSignatureId::FnDivNumeric as i32,
        FunctionSignatureId::FnIsInf as i32,
        FunctionSignatureId::FnIsNan as i32,
        FunctionSignatureId::FnGreatest as i32,
        FunctionSignatureId::FnLeast as i32,
        FunctionSignatureId::FnSqrtDouble as i32,
        FunctionSignatureId::FnPowDouble as i32,
        FunctionSignatureId::FnPowNumeric as i32,
        FunctionSignatureId::FnExpDouble as i32,
        FunctionSignatureId::FnNaturalLogarithmDouble as i32,
        FunctionSignatureId::FnDecimalLogarithmDouble as i32,
        FunctionSignatureId::FnLogarithmDouble as i32,
        FunctionSignatureId::FnCosDouble as i32,
        FunctionSignatureId::FnCoshDouble as i32,
        FunctionSignatureId::FnAcosDouble as i32,
        FunctionSignatureId::FnAcoshDouble as i32,
        FunctionSignatureId::FnSinDouble as i32,
        FunctionSignatureId::FnSinhDouble as i32,
        FunctionSignatureId::FnAsinDouble as i32,
        FunctionSignatureId::FnAsinhDouble as i32,
        FunctionSignatureId::FnTanDouble as i32,
        FunctionSignatureId::FnTanhDouble as i32,
        FunctionSignatureId::FnAtanDouble as i32,
        FunctionSignatureId::FnAtanhDouble as i32,
        FunctionSignatureId::FnAtan2Double as i32,
        // Non-deterministic functions
        FunctionSignatureId::FnCurrentDate as i32,
        FunctionSignatureId::FnCurrentTimestamp as i32,
        FunctionSignatureId::FnRand as i32,
        // Aggregate functions
        // TODO these are still not implemented in compiler
        FunctionSignatureId::FnCountStar as i32,
        FunctionSignatureId::FnAnyValue as i32,
        FunctionSignatureId::FnArrayAgg as i32,
        FunctionSignatureId::FnArrayConcatAgg as i32,
        FunctionSignatureId::FnAvgInt64 as i32,
        FunctionSignatureId::FnAvgDouble as i32,
        FunctionSignatureId::FnAvgNumeric as i32,
        FunctionSignatureId::FnCount as i32,
        FunctionSignatureId::FnMax as i32,
        FunctionSignatureId::FnMin as i32,
        FunctionSignatureId::FnStringAggString as i32,
        FunctionSignatureId::FnStringAggDelimString as i32,
        FunctionSignatureId::FnStringAggBytes as i32,
        FunctionSignatureId::FnStringAggDelimBytes as i32,
        FunctionSignatureId::FnSumInt64 as i32,
        FunctionSignatureId::FnSumDouble as i32,
        FunctionSignatureId::FnSumNumeric as i32,
        FunctionSignatureId::FnBitAndInt64 as i32,
        FunctionSignatureId::FnBitOrInt64 as i32,
        FunctionSignatureId::FnBitXorInt64 as i32,
        FunctionSignatureId::FnLogicalAnd as i32,
        FunctionSignatureId::FnLogicalOr as i32,
    ]
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
