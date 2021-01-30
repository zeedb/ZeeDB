use zetasql::*;

pub const ROOT_CATALOG_ID: i64 = 0;

pub trait CatalogProvider {
    fn catalog(&self, catalog_id: i64, table_names: Vec<Vec<String>>) -> SimpleCatalogProto;
}

pub fn default_catalog() -> SimpleCatalogProto {
    let mut cat = empty_catalog();
    cat.catalog.push(crate::bootstrap_metadata_catalog());
    cat
}

pub fn empty_catalog() -> SimpleCatalogProto {
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

pub fn enabled_language_features() -> Vec<i32> {
    vec![
        LanguageFeature::FeatureDisallowGroupByFloat as i32,
        LanguageFeature::FeatureDmlUpdateWithJoin as i32,
        LanguageFeature::FeatureNamedArguments as i32,
        LanguageFeature::FeatureV11SelectStarExceptReplace as i32,
        LanguageFeature::FeatureV12CorrelatedRefsInNestedDml as i32,
        LanguageFeature::FeatureV12WeekWithWeekday as i32,
        LanguageFeature::FeatureV13OmitInsertColumnList as i32,
        LanguageFeature::FeatureV13ConcatMixedTypes as i32,
    ]
}

pub fn supported_statement_kinds() -> Vec<i32> {
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
        ResolvedNodeKind::ResolvedExplainStmt as i32,
    ]
}

pub fn enabled_functions() -> Vec<i32> {
    vec![
        FunctionSignatureId::FnAddDouble as i32,            // $add
        FunctionSignatureId::FnAddInt64 as i32,             // $add
        FunctionSignatureId::FnAddInt64Date as i32,         // $add
        FunctionSignatureId::FnAnd as i32,                  // $and
        FunctionSignatureId::FnCaseNoValue as i32,          // $case_no_value
        FunctionSignatureId::FnCaseWithValue as i32,        // $case_with_value
        FunctionSignatureId::FnDivideDouble as i32,         // $divide
        FunctionSignatureId::FnGreater as i32,              // $greater
        FunctionSignatureId::FnGreaterOrEqual as i32,       // $greater_or_equal
        FunctionSignatureId::FnLess as i32,                 // $less
        FunctionSignatureId::FnLessOrEqual as i32,          // $less_or_equal
        FunctionSignatureId::FnEqual as i32,                // $equal
        FunctionSignatureId::FnStringLike as i32,           // $like
        FunctionSignatureId::FnIn as i32,                   // $in
        FunctionSignatureId::FnBetween as i32,              // $between
        FunctionSignatureId::FnIsNull as i32,               // $is_null
        FunctionSignatureId::FnIsTrue as i32,               // $is_true
        FunctionSignatureId::FnIsFalse as i32,              // $is_false
        FunctionSignatureId::FnMultiplyDouble as i32,       // $multiply
        FunctionSignatureId::FnMultiplyInt64 as i32,        // $multiply
        FunctionSignatureId::FnNot as i32,                  // $not
        FunctionSignatureId::FnNotEqual as i32,             // $not_equal
        FunctionSignatureId::FnOr as i32,                   // $or
        FunctionSignatureId::FnSubtractDouble as i32,       // $subtract
        FunctionSignatureId::FnSubtractInt64 as i32,        // $subtract
        FunctionSignatureId::FnUnaryMinusInt64 as i32,      // $unary_minus
        FunctionSignatureId::FnUnaryMinusDouble as i32,     // $unary_minus
        FunctionSignatureId::FnConcatString as i32,         // concat(repeated string) -> string
        FunctionSignatureId::FnConcatOpString as i32,       // concat(string, string) -> string
        FunctionSignatureId::FnStrposString as i32,         // strpos(string, string) -> int64
        FunctionSignatureId::FnLowerString as i32,          // lower(string) -> string
        FunctionSignatureId::FnUpperString as i32,          // upper(string) -> string
        FunctionSignatureId::FnLengthString as i32,         // length(string) -> int64
        FunctionSignatureId::FnStartsWithString as i32,     // starts_with(string, string) -> string
        FunctionSignatureId::FnEndsWithString as i32,       // ends_with(string, string) -> string
        FunctionSignatureId::FnSubstrString as i32, // substr(string, int64[, int64]) -> string
        FunctionSignatureId::FnTrimString as i32,   // trim(string[, string]) -> string
        FunctionSignatureId::FnLtrimString as i32,  // ltrim(string[, string]) -> string
        FunctionSignatureId::FnRtrimString as i32,  // rtrim(string[, string]) -> string
        FunctionSignatureId::FnReplaceString as i32, // replace(string, string, string) -> string
        FunctionSignatureId::FnRegexpExtractString as i32, // regexp_extract(string, string) -> string
        FunctionSignatureId::FnRegexpReplaceString as i32, // regexp_replace(string, string, string) -> string
        FunctionSignatureId::FnByteLengthString as i32,    // byte_length(string) -> int64
        FunctionSignatureId::FnCharLengthString as i32,    // char_length(string) -> int64
        FunctionSignatureId::FnRegexpContainsString as i32, // regexp_contains(string, string) -> bool
        FunctionSignatureId::FnLpadString as i32, // lpad(string, int64[, string]) -> string
        FunctionSignatureId::FnRpadString as i32, // rpad(string, int64[, string]) -> string
        FunctionSignatureId::FnLeftString as i32, // left(string, int64) -> string
        FunctionSignatureId::FnRightString as i32, // right(string, int64) -> string
        FunctionSignatureId::FnRepeatString as i32, // repeat(string, int64) -> string
        FunctionSignatureId::FnReverseString as i32, // reverse(string) -> string
        FunctionSignatureId::FnChrString as i32,  // chr(int64) -> string
        FunctionSignatureId::FnIf as i32,         // if
        FunctionSignatureId::FnCoalesce as i32,   // coalesce
        FunctionSignatureId::FnIfnull as i32,     // ifnull
        FunctionSignatureId::FnNullif as i32,     // nullif
        FunctionSignatureId::FnCurrentDate as i32, // current_date
        FunctionSignatureId::FnCurrentTimestamp as i32, // current_timestamp
        FunctionSignatureId::FnDateAddDate as i32, // date_add
        FunctionSignatureId::FnTimestampAdd as i32, // timestamp_add
        FunctionSignatureId::FnDateDiffDate as i32, // date_diff
        FunctionSignatureId::FnTimestampDiff as i32, // timestamp_diff
        FunctionSignatureId::FnDateSubDate as i32, // date_sub
        FunctionSignatureId::FnTimestampSub as i32, // timestamp_sub
        FunctionSignatureId::FnDateTruncDate as i32, // date_trunc
        FunctionSignatureId::FnTimestampTrunc as i32, // timestamp_trunc
        FunctionSignatureId::FnDateFromUnixDate as i32, // date_from_unix_date
        FunctionSignatureId::FnTimestampFromUnixMicrosInt64 as i32, // timestamp_from_unix_micros
        FunctionSignatureId::FnUnixDate as i32,   // unix_date
        FunctionSignatureId::FnUnixMicrosFromTimestamp as i32, // unix_micros
        FunctionSignatureId::FnDateFromTimestamp as i32, // date
        FunctionSignatureId::FnDateFromDate as i32, // date
        FunctionSignatureId::FnDateFromString as i32, // date
        FunctionSignatureId::FnDateFromYearMonthDay as i32, // date
        FunctionSignatureId::FnTimestampFromString as i32, // timestamp
        FunctionSignatureId::FnTimestampFromDate as i32, // timestamp
        FunctionSignatureId::FnStringFromDate as i32, // string
        FunctionSignatureId::FnStringFromTimestamp as i32, // string
        FunctionSignatureId::FnExtractFromDate as i32, // $extract
        FunctionSignatureId::FnExtractFromTimestamp as i32, // $extract
        FunctionSignatureId::FnExtractDateFromTimestamp as i32, // $extract_date
        FunctionSignatureId::FnFormatDate as i32, // format_date
        FunctionSignatureId::FnFormatTimestamp as i32, // format_timestamp
        FunctionSignatureId::FnParseDate as i32,  // parse_date
        FunctionSignatureId::FnParseTimestamp as i32, // parse_timestamp
        FunctionSignatureId::FnAbsInt64 as i32,   // abs
        FunctionSignatureId::FnAbsDouble as i32,  // abs
        FunctionSignatureId::FnSignInt64 as i32,  // sign
        FunctionSignatureId::FnSignDouble as i32, // sign
        FunctionSignatureId::FnRoundDouble as i32, // round(double) -> double
        FunctionSignatureId::FnRoundWithDigitsDouble as i32, // round(double, int64) -> double
        FunctionSignatureId::FnTruncDouble as i32, // trunc(double) -> double
        FunctionSignatureId::FnTruncWithDigitsDouble as i32, // trunc(double, int64) -> double
        FunctionSignatureId::FnCeilDouble as i32, // ceil(double) -> double
        FunctionSignatureId::FnFloorDouble as i32, // floor(double) -> double
        FunctionSignatureId::FnModInt64 as i32,   // mod(int64, int64) -> int64
        FunctionSignatureId::FnDivInt64 as i32,   // div(int64, int64) -> int64
        FunctionSignatureId::FnIsInf as i32,      // is_inf
        FunctionSignatureId::FnIsNan as i32,      // is_nan
        FunctionSignatureId::FnGreatest as i32,   // greatest
        FunctionSignatureId::FnLeast as i32,      // least
        FunctionSignatureId::FnSqrtDouble as i32, // sqrt
        FunctionSignatureId::FnPowDouble as i32,  // pow
        FunctionSignatureId::FnExpDouble as i32,  // exp
        FunctionSignatureId::FnNaturalLogarithmDouble as i32, // ln and log
        FunctionSignatureId::FnDecimalLogarithmDouble as i32, // log10
        FunctionSignatureId::FnLogarithmDouble as i32, // log
        FunctionSignatureId::FnCosDouble as i32,  // cos
        FunctionSignatureId::FnCoshDouble as i32, // cosh
        FunctionSignatureId::FnAcosDouble as i32, // acos
        FunctionSignatureId::FnAcoshDouble as i32, // acosh
        FunctionSignatureId::FnSinDouble as i32,  // sin
        FunctionSignatureId::FnSinhDouble as i32, // sinh
        FunctionSignatureId::FnAsinDouble as i32, // asin
        FunctionSignatureId::FnAsinhDouble as i32, // asinh
        FunctionSignatureId::FnTanDouble as i32,  // tan
        FunctionSignatureId::FnTanhDouble as i32, // tanh
        FunctionSignatureId::FnAtanDouble as i32, // atan
        FunctionSignatureId::FnAtanhDouble as i32, // atanh
        FunctionSignatureId::FnAtan2Double as i32, // atan2
        FunctionSignatureId::FnLogicalAnd as i32, // logical_and
        FunctionSignatureId::FnLogicalOr as i32,  // logical_or
        FunctionSignatureId::FnRand as i32,       // rand() -> double
        // Aggregate functions
        FunctionSignatureId::FnCountStar as i32,
        FunctionSignatureId::FnAnyValue as i32,
        FunctionSignatureId::FnAvgInt64 as i32,
        FunctionSignatureId::FnAvgDouble as i32,
        FunctionSignatureId::FnCount as i32,
        FunctionSignatureId::FnMax as i32,
        FunctionSignatureId::FnMin as i32,
        FunctionSignatureId::FnSumInt64 as i32,
        FunctionSignatureId::FnSumDouble as i32,
        FunctionSignatureId::FnLogicalAnd as i32,
        FunctionSignatureId::FnLogicalOr as i32,
    ]
}
