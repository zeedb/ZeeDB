use arrow::datatypes::{DataType, Field, Schema};
use zetasql::*;

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
    let catalog_id = Field::new("catalog_id", DataType::Int64, false);
    let table_id = Field::new("table_id", DataType::Int64, false);
    let column_id = Field::new("column_id", DataType::Int64, false);
    let catalog_name = Field::new("catalog_name", DataType::Utf8, false);
    let table_name = Field::new("table_name", DataType::Utf8, false);
    let column_name = Field::new("column_name", DataType::Utf8, false);
    let column_type = Field::new("column_type", DataType::Utf8, false);
    let partition_by = Field::new("partition_by", DataType::Int64, false);
    let cluster_by = Field::new("cluster_by", DataType::Int64, false);
    let primary_key = Field::new("primary_key", DataType::Int64, false);
    let catalog = Schema::new(vec![catalog_id.clone(), catalog_name.clone()]);
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
        partition_by.clone(),
        cluster_by.clone(),
        primary_key.clone(),
    ]);
    vec![catalog, table, column]
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
                    column("partition_by", TypeKind::TypeInt64),
                    column("cluster_by", TypeKind::TypeInt64),
                    column("primary_key", TypeKind::TypeInt64),
                ],
            ),
        ],
        ..Default::default()
    }
}

pub fn enabled_language_features() -> Vec<i32> {
    vec![
        LanguageFeature::FeatureDmlUpdateWithJoin as i32,
        LanguageFeature::FeatureCreateTablePartitionBy as i32,
        LanguageFeature::FeatureCreateTableClusterBy as i32,
        LanguageFeature::FeatureNumericType as i32,
        LanguageFeature::FeatureCreateTableFieldAnnotations as i32,
        LanguageFeature::FeatureCreateTableAsSelectColumnList as i32,
        LanguageFeature::FeatureDisallowNullPrimaryKeys as i32,
        LanguageFeature::FeatureDisallowPrimaryKeyUpdates as i32,
        LanguageFeature::FeatureParametersInGranteeList as i32,
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
        ResolvedNodeKind::ResolvedExpressionColumn as i32,
        ResolvedNodeKind::ResolvedColumnRef as i32,
        ResolvedNodeKind::ResolvedConstant as i32,
        ResolvedNodeKind::ResolvedSystemVariable as i32,
        ResolvedNodeKind::ResolvedInlineLambda as i32,
        ResolvedNodeKind::ResolvedFunctionCall as i32,
        ResolvedNodeKind::ResolvedAggregateFunctionCall as i32,
        ResolvedNodeKind::ResolvedAnalyticFunctionCall as i32,
        ResolvedNodeKind::ResolvedExtendedCastElement as i32,
        ResolvedNodeKind::ResolvedExtendedCast as i32,
        ResolvedNodeKind::ResolvedCast as i32,
        ResolvedNodeKind::ResolvedMakeStruct as i32,
        ResolvedNodeKind::ResolvedMakeProto as i32,
        ResolvedNodeKind::ResolvedMakeProtoField as i32,
        ResolvedNodeKind::ResolvedGetStructField as i32,
        ResolvedNodeKind::ResolvedGetProtoField as i32,
        ResolvedNodeKind::ResolvedFlatten as i32,
        ResolvedNodeKind::ResolvedFlattenedArg as i32,
        ResolvedNodeKind::ResolvedReplaceFieldItem as i32,
        ResolvedNodeKind::ResolvedReplaceField as i32,
        ResolvedNodeKind::ResolvedSubqueryExpr as i32,
        ResolvedNodeKind::ResolvedModel as i32,
        ResolvedNodeKind::ResolvedConnection as i32,
        ResolvedNodeKind::ResolvedDescriptor as i32,
        ResolvedNodeKind::ResolvedSingleRowScan as i32,
        ResolvedNodeKind::ResolvedTableScan as i32,
        ResolvedNodeKind::ResolvedJoinScan as i32,
        ResolvedNodeKind::ResolvedArrayScan as i32,
        ResolvedNodeKind::ResolvedColumnHolder as i32,
        ResolvedNodeKind::ResolvedFilterScan as i32,
        ResolvedNodeKind::ResolvedGroupingSet as i32,
        ResolvedNodeKind::ResolvedAggregateScan as i32,
        ResolvedNodeKind::ResolvedSetOperationItem as i32,
        ResolvedNodeKind::ResolvedSetOperationScan as i32,
        ResolvedNodeKind::ResolvedOrderByScan as i32,
        ResolvedNodeKind::ResolvedLimitOffsetScan as i32,
        ResolvedNodeKind::ResolvedWithRefScan as i32,
        ResolvedNodeKind::ResolvedAnalyticScan as i32,
        ResolvedNodeKind::ResolvedSampleScan as i32,
        ResolvedNodeKind::ResolvedComputedColumn as i32,
        ResolvedNodeKind::ResolvedOrderByItem as i32,
        ResolvedNodeKind::ResolvedColumnAnnotations as i32,
        ResolvedNodeKind::ResolvedGeneratedColumnInfo as i32,
        ResolvedNodeKind::ResolvedColumnDefinition as i32,
        ResolvedNodeKind::ResolvedPrimaryKey as i32,
        ResolvedNodeKind::ResolvedForeignKey as i32,
        ResolvedNodeKind::ResolvedCheckConstraint as i32,
        ResolvedNodeKind::ResolvedOutputColumn as i32,
        ResolvedNodeKind::ResolvedProjectScan as i32,
        ResolvedNodeKind::ResolvedTvfscan as i32,
        ResolvedNodeKind::ResolvedFunctionArgument as i32,
        ResolvedNodeKind::ResolvedExplainStmt as i32,
        ResolvedNodeKind::ResolvedQueryStmt as i32,
        ResolvedNodeKind::ResolvedCreateDatabaseStmt as i32,
        ResolvedNodeKind::ResolvedIndexItem as i32,
        ResolvedNodeKind::ResolvedUnnestItem as i32,
        ResolvedNodeKind::ResolvedCreateIndexStmt as i32,
        ResolvedNodeKind::ResolvedCreateSchemaStmt as i32,
        ResolvedNodeKind::ResolvedCreateTableStmt as i32,
        ResolvedNodeKind::ResolvedCreateTableAsSelectStmt as i32,
        ResolvedNodeKind::ResolvedCreateModelStmt as i32,
        ResolvedNodeKind::ResolvedCreateViewStmt as i32,
        ResolvedNodeKind::ResolvedWithPartitionColumns as i32,
        ResolvedNodeKind::ResolvedCreateExternalTableStmt as i32,
        ResolvedNodeKind::ResolvedExportModelStmt as i32,
        ResolvedNodeKind::ResolvedExportDataStmt as i32,
        ResolvedNodeKind::ResolvedDefineTableStmt as i32,
        ResolvedNodeKind::ResolvedDescribeStmt as i32,
        ResolvedNodeKind::ResolvedShowStmt as i32,
        ResolvedNodeKind::ResolvedBeginStmt as i32,
        ResolvedNodeKind::ResolvedSetTransactionStmt as i32,
        ResolvedNodeKind::ResolvedCommitStmt as i32,
        ResolvedNodeKind::ResolvedRollbackStmt as i32,
        ResolvedNodeKind::ResolvedStartBatchStmt as i32,
        ResolvedNodeKind::ResolvedRunBatchStmt as i32,
        ResolvedNodeKind::ResolvedAbortBatchStmt as i32,
        ResolvedNodeKind::ResolvedDropStmt as i32,
        ResolvedNodeKind::ResolvedDropMaterializedViewStmt as i32,
        ResolvedNodeKind::ResolvedRecursiveRefScan as i32,
        ResolvedNodeKind::ResolvedRecursiveScan as i32,
        ResolvedNodeKind::ResolvedWithScan as i32,
        ResolvedNodeKind::ResolvedWithEntry as i32,
        ResolvedNodeKind::ResolvedOption as i32,
        ResolvedNodeKind::ResolvedWindowPartitioning as i32,
        ResolvedNodeKind::ResolvedWindowOrdering as i32,
        ResolvedNodeKind::ResolvedWindowFrame as i32,
        ResolvedNodeKind::ResolvedAnalyticFunctionGroup as i32,
        ResolvedNodeKind::ResolvedWindowFrameExpr as i32,
        ResolvedNodeKind::ResolvedDmlvalue as i32,
        ResolvedNodeKind::ResolvedDmldefault as i32,
        ResolvedNodeKind::ResolvedAssertStmt as i32,
        ResolvedNodeKind::ResolvedAssertRowsModified as i32,
        ResolvedNodeKind::ResolvedInsertRow as i32,
        ResolvedNodeKind::ResolvedInsertStmt as i32,
        ResolvedNodeKind::ResolvedDeleteStmt as i32,
        ResolvedNodeKind::ResolvedUpdateItem as i32,
        ResolvedNodeKind::ResolvedUpdateArrayItem as i32,
        ResolvedNodeKind::ResolvedUpdateStmt as i32,
        ResolvedNodeKind::ResolvedMergeWhen as i32,
        ResolvedNodeKind::ResolvedMergeStmt as i32,
        ResolvedNodeKind::ResolvedTruncateStmt as i32,
        ResolvedNodeKind::ResolvedPrivilege as i32,
        ResolvedNodeKind::ResolvedGrantStmt as i32,
        ResolvedNodeKind::ResolvedRevokeStmt as i32,
        ResolvedNodeKind::ResolvedAlterDatabaseStmt as i32,
        ResolvedNodeKind::ResolvedAlterMaterializedViewStmt as i32,
        ResolvedNodeKind::ResolvedAlterTableStmt as i32,
        ResolvedNodeKind::ResolvedAlterViewStmt as i32,
        ResolvedNodeKind::ResolvedSetOptionsAction as i32,
        ResolvedNodeKind::ResolvedAddColumnAction as i32,
        ResolvedNodeKind::ResolvedDropColumnAction as i32,
        ResolvedNodeKind::ResolvedSetAsAction as i32,
        ResolvedNodeKind::ResolvedAlterTableSetOptionsStmt as i32,
        ResolvedNodeKind::ResolvedRenameStmt as i32,
        ResolvedNodeKind::ResolvedCreateRowAccessPolicyStmt as i32,
        ResolvedNodeKind::ResolvedDropRowAccessPolicyStmt as i32,
        ResolvedNodeKind::ResolvedGrantToAction as i32,
        ResolvedNodeKind::ResolvedFilterUsingAction as i32,
        ResolvedNodeKind::ResolvedRevokeFromAction as i32,
        ResolvedNodeKind::ResolvedRenameToAction as i32,
        ResolvedNodeKind::ResolvedAlterRowAccessPolicyStmt as i32,
        ResolvedNodeKind::ResolvedAlterAllRowAccessPoliciesStmt as i32,
        ResolvedNodeKind::ResolvedCreateConstantStmt as i32,
        ResolvedNodeKind::ResolvedCreateFunctionStmt as i32,
        ResolvedNodeKind::ResolvedArgumentDef as i32,
        ResolvedNodeKind::ResolvedArgumentRef as i32,
        ResolvedNodeKind::ResolvedCreateTableFunctionStmt as i32,
        ResolvedNodeKind::ResolvedRelationArgumentScan as i32,
        ResolvedNodeKind::ResolvedArgumentList as i32,
        ResolvedNodeKind::ResolvedFunctionSignatureHolder as i32,
        ResolvedNodeKind::ResolvedDropFunctionStmt as i32,
        ResolvedNodeKind::ResolvedCallStmt as i32,
        ResolvedNodeKind::ResolvedImportStmt as i32,
        ResolvedNodeKind::ResolvedModuleStmt as i32,
        ResolvedNodeKind::ResolvedAggregateHavingModifier as i32,
        ResolvedNodeKind::ResolvedCreateMaterializedViewStmt as i32,
        ResolvedNodeKind::ResolvedCreateProcedureStmt as i32,
        ResolvedNodeKind::ResolvedExecuteImmediateArgument as i32,
        ResolvedNodeKind::ResolvedExecuteImmediateStmt as i32,
        ResolvedNodeKind::ResolvedAssignmentStmt as i32,
        ResolvedNodeKind::ResolvedSingleAssignmentStmt as i32,
        ResolvedNodeKind::ResolvedCreateEntityStmt as i32,
        ResolvedNodeKind::ResolvedAlterEntityStmt as i32,
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
